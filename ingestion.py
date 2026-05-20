"""Main ingestion orchestrator with dependency injection."""
import logging
import signal
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Optional
from dataclasses import dataclass

from config import AppConfig, load_config
from database import ClickHouseConnection, ClickHouseRepository
from api_client import VacinacaoAPIClient
from state_manager import StateManager
from transformer import DataTransformer

logger = logging.getLogger(__name__)


@dataclass
class IngestionServices:
    """Container for all injected services."""
    config: AppConfig
    connection: ClickHouseConnection
    repository: ClickHouseRepository
    api_client: VacinacaoAPIClient
    state_manager: StateManager
    transformer: DataTransformer


class ServiceContainer:
    """Dependency injection container for creating services."""

    _instance: Optional["ServiceContainer"] = None

    def __new__(cls) -> "ServiceContainer":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        self._config: Optional[AppConfig] = None
        self._services: Optional[IngestionServices] = None
        self._initialized = True

    def initialize(self, config: Optional[AppConfig] = None) -> IngestionServices:
        """Initialize all services with dependency injection.

        Args:
            config: Optional configuration. If not provided, loads from environment.

        Returns:
            Container with all initialized services
        """
        self._config = config or load_config()

        # Create singletons in order of dependencies
        connection = ClickHouseConnection(self._config.clickhouse)
        state_manager = StateManager(self._config.state_file)

        self._services = IngestionServices(
            config=self._config,
            connection=connection,
            repository=ClickHouseRepository(connection),
            api_client=VacinacaoAPIClient(self._config.api),
            state_manager=state_manager,
            transformer=DataTransformer(),
        )

        logger.info("All services initialized")
        return self._services

    @property
    def services(self) -> IngestionServices:
        """Get initialized services."""
        if self._services is None:
            raise RuntimeError("Services not initialized. Call initialize() first.")
        return self._services

    def cleanup(self) -> None:
        """Cleanup all services."""
        if self._services:
            self._services.api_client.close()
            self._services.connection.close()
            logger.info("All services cleaned up")


class IngestionOrchestrator:
    """Orchestrates the data ingestion process."""

    def __init__(self, services: IngestionServices) -> None:
        self._services = services
        self._should_stop = False
        self._progress_lock = Lock()
        self._max_offset_seen = 0
        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Setup graceful shutdown handlers."""
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum: int, frame) -> None:
        """Handle shutdown signal gracefully."""
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        self._should_stop = True

    def _process_offset(self, offset: int) -> tuple[int, int, bool]:
        """Fetch, transform, and insert a single offset. Returns (offset, inserted_count, has_more)."""
        api_client = self._services.api_client
        repository = self._services.repository

        response = api_client.fetch_batch(offset)

        if not response.records:
            return (offset, 0, response.has_more)

        transformed = self._services.transformer.transform_batch(response.records)

        if not transformed:
            logger.warning(f"All records failed transformation at offset {offset}")
            return (offset, 0, response.has_more)

        inserted = repository.insert_batch(
            transformed,
            max_retries=self._services.config.api.max_retries,
        )

        return (offset, inserted, response.has_more)

    def run(self, reset: bool = False) -> None:
        """Run the ingestion process with parallel workers.

        Args:
            reset: If True, resets state and starts from beginning
        """
        state_manager = self._services.state_manager
        num_workers = self._services.config.num_workers

        if reset:
            logger.info("Resetting state and starting fresh")
            state_manager.reset()

        start_offset = state_manager.get_resume_offset()

        if start_offset > 0:
            logger.info(f"Resuming from offset {start_offset}")

        logger.info(f"Starting ingestion process with {num_workers} workers")

        current_offset = start_offset
        reached_end = False

        try:
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                while not reached_end and not self._should_stop:
                    # Submit a batch of offsets to process in parallel
                    futures = {}
                    for i in range(num_workers):
                        if self._should_stop:
                            break
                        offset = current_offset + i
                        future = executor.submit(self._process_offset, offset)
                        futures[future] = offset

                    # Collect results
                    for future in as_completed(futures):
                        if self._should_stop:
                            break

                        offset, inserted, has_more = future.result()

                        with self._progress_lock:
                            state_manager.update_progress(
                                offset=offset + 1,
                                records_processed=inserted,
                            )

                        logger.info(
                            f"Progress: offset={offset}, "
                            f"batch={inserted}, "
                            f"total={state_manager.state.total_records_processed}"
                        )

                        if not has_more:
                            reached_end = True

                    current_offset += num_workers

            if not self._should_stop:
                state_manager.mark_completed()
                logger.info("Ingestion completed successfully")

        except Exception as e:
            error_msg = str(e)
            state_manager.mark_failed(error_msg)
            logger.error(f"Ingestion failed: {error_msg}")
            raise


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("ingestion.log"),
        ],
    )


def main(reset: bool = False) -> None:
    """Main entry point.

    Args:
        reset: If True, resets state and starts from beginning
    """
    container = ServiceContainer()

    try:
        config = load_config()
        setup_logging(config.log_level)

        logger.info("=" * 60)
        logger.info("Vaccination Data Ingestion Starting")
        logger.info("=" * 60)

        services = container.initialize(config)

        # Check database connection
        if not services.connection.health_check():
            logger.error("Failed to connect to ClickHouse. Please check credentials.")
            sys.exit(1)

        orchestrator = IngestionOrchestrator(services)
        orchestrator.run(reset=reset)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        container.cleanup()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ingest vaccination data into ClickHouse")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset state and start from beginning",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current ingestion status and exit",
    )
    parser.add_argument(
        "--create-schema",
        action="store_true",
        help="Create database and table schema before ingestion",
    )

    args = parser.parse_args()

    if args.status:
        state_manager = StateManager()
        state = state_manager.state
        print(f"Status: {state.status}")
        print(f"Last offset: {state.last_offset}")
        print(f"Total records processed: {state.total_records_processed}")
        print(f"Last updated: {state.last_updated}")
        if state.error_message:
            print(f"Last error: {state.error_message}")
    else:
        if args.create_schema:
            setup_logging("INFO")
            config = load_config()
            connection = ClickHouseConnection(config.clickhouse)
            repo = ClickHouseRepository(connection)
            repo.create_schema()
            print("Schema created successfully")
            connection.close()
            ClickHouseConnection.reset_instance()
        main(reset=args.reset)
