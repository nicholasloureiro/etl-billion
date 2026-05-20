"""Database module with singleton connection manager."""
import logging
import os
from threading import Lock
from typing import Optional, List, Dict, Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from config import ClickHouseConfig

logger = logging.getLogger(__name__)


class ClickHouseConnection:
    """Singleton ClickHouse connection manager with connection pooling."""

    _instance: Optional["ClickHouseConnection"] = None
    _lock: Lock = Lock()

    def __new__(cls, config: Optional[ClickHouseConfig] = None) -> "ClickHouseConnection":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    if config is None:
                        raise ValueError("Config must be provided for first instantiation")
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, config: Optional[ClickHouseConfig] = None) -> None:
        if self._initialized:
            return

        if config is None:
            raise ValueError("Config must be provided for initialization")

        self._config = config
        self._client: Optional[Client] = None
        self._initialized = True
        logger.info("ClickHouseConnection singleton initialized")

    def _create_client(self) -> Client:
        """Create a new ClickHouse client."""
        logger.info(f"Connecting to ClickHouse at {self._config.host}:{self._config.port}")
        return clickhouse_connect.get_client(
            host=self._config.host,
            port=self._config.port,
            username=self._config.user,
            password=self._config.password,
            database=self._config.database,
            secure=self._config.secure,
        )

    def get_client(self) -> Client:
        """Get the ClickHouse client, creating if necessary."""
        if self._client is None:
            self._client = self._create_client()
        return self._client

    def reconnect(self) -> Client:
        """Force reconnection to ClickHouse."""
        logger.info("Reconnecting to ClickHouse...")
        if self._client is not None:
            try:
                self._client.close()
            except Exception as e:
                logger.warning(f"Error closing existing connection: {e}")
        self._client = self._create_client()
        return self._client

    def health_check(self) -> bool:
        """Check if connection is healthy."""
        try:
            client = self.get_client()
            client.command("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False

    def close(self) -> None:
        """Close the connection."""
        if self._client is not None:
            try:
                self._client.close()
                self._client = None
                logger.info("ClickHouse connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close()
            cls._instance = None


class ClickHouseRepository:
    """Repository for ClickHouse operations with retry logic."""

    TABLE_NAME = "vacinas.events"

    def __init__(self, connection: ClickHouseConnection) -> None:
        self._connection = connection

    def insert_batch(self, records: List[Dict[str, Any]], max_retries: int = 3) -> int:
        """Insert a batch of records with retry logic.

        Returns the number of records inserted.
        """
        if not records:
            return 0

        # Prepare column names and data
        columns = list(records[0].keys())
        data = [[record.get(col) for col in columns] for record in records]

        last_error: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                client = self._connection.get_client()
                client.insert(
                    table=self.TABLE_NAME,
                    data=data,
                    column_names=columns,
                )
                logger.info(f"Successfully inserted {len(records)} records")
                return len(records)
            except Exception as e:
                last_error = e
                logger.warning(f"Insert attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    self._connection.reconnect()

        raise RuntimeError(f"Failed to insert batch after {max_retries} attempts: {last_error}")

    def create_schema(self, schema_path: Optional[str] = None) -> None:
        """Execute the create_schema.sql file to set up the database and table."""
        if schema_path is None:
            schema_path = os.path.join(os.path.dirname(__file__), "create_schema.sql")

        with open(schema_path, "r") as f:
            sql = f.read()

        client = self._connection.get_client()
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                logger.info(f"Executing: {statement[:80]}...")
                client.command(statement)

        logger.info("Schema created successfully")

    def count_records(self) -> int:
        """Get total record count."""
        try:
            client = self._connection.get_client()
            result = client.command(f"SELECT COUNT(*) FROM {self.TABLE_NAME}")
            return int(result)
        except Exception as e:
            logger.warning(f"Error counting records: {e}")
            return 0
