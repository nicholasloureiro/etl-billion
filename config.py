"""Configuration module with ClickHouse credentials placeholders."""
from dataclasses import dataclass
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse connection configuration."""
    host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    port: int = int(os.getenv("CLICKHOUSE_PORT", "9000"))
    user: str = os.getenv("CLICKHOUSE_USER", "default")
    password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    database: str = os.getenv("CLICKHOUSE_DATABASE", "vacinas")
    secure: bool = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"


@dataclass(frozen=True)
class APIConfig:
    """API configuration."""
    base_url: str = "https://apidadosabertos.saude.gov.br"
    endpoint: str = "/vacinacao/doses-aplicadas-pni-2025"
    batch_size: int = 100
    max_retries: int = 3
    retry_delay: float = 5.0
    request_timeout: int = 60


@dataclass(frozen=True)
class AppConfig:
    """Application configuration."""
    clickhouse: ClickHouseConfig
    api: APIConfig
    state_file: str = "ingestion_state.json"
    log_level: str = "INFO"
    num_workers: int = int(os.getenv("NUM_WORKERS", "5"))


def load_config() -> AppConfig:
    """Load application configuration from environment or defaults."""
    return AppConfig(
        clickhouse=ClickHouseConfig(),
        api=APIConfig(),
        state_file=os.getenv("STATE_FILE", "ingestion_state.json"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
