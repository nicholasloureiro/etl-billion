"""State management module for tracking ingestion progress."""
import json
import logging
import os
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class IngestionState:
    """State of the ingestion process."""
    last_offset: int = 0
    total_records_processed: int = 0
    last_updated: str = ""
    status: str = "idle"  # idle, running, completed, failed
    error_message: Optional[str] = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "IngestionState":
        """Create from dictionary."""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class StateManager:
    """Manages persistence of ingestion state for resume capability."""

    _instance: Optional["StateManager"] = None
    _lock: Lock = Lock()

    def __new__(cls, state_file: Optional[str] = None) -> "StateManager":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, state_file: Optional[str] = None) -> None:
        if self._initialized:
            return

        if state_file is None:
            state_file = "ingestion_state.json"

        self._state_file = Path(state_file)
        self._state = self._load_state()
        self._file_lock = Lock()
        self._initialized = True
        logger.info(f"StateManager initialized with file: {self._state_file}")

    def _load_state(self) -> IngestionState:
        """Load state from file or create new."""
        if self._state_file.exists():
            try:
                with open(self._state_file, "r") as f:
                    data = json.load(f)
                    state = IngestionState.from_dict(data)
                    logger.info(f"Loaded existing state: offset={state.last_offset}, "
                              f"records={state.total_records_processed}")
                    return state
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Failed to load state file: {e}. Starting fresh.")

        return IngestionState()

    def _save_state(self) -> None:
        """Save state to file."""
        with self._file_lock:
            self._state.last_updated = datetime.utcnow().isoformat()
            try:
                # Write to temp file first for atomicity
                temp_file = self._state_file.with_suffix(".tmp")
                with open(temp_file, "w") as f:
                    json.dump(self._state.to_dict(), f, indent=2)
                temp_file.replace(self._state_file)
            except OSError as e:
                logger.error(f"Failed to save state: {e}")
                raise

    @property
    def state(self) -> IngestionState:
        """Get current state."""
        return self._state

    def get_resume_offset(self) -> int:
        """Get the offset to resume from."""
        return self._state.last_offset

    def update_progress(self, offset: int, records_processed: int) -> None:
        """Update progress after successful batch insert."""
        self._state.last_offset = offset
        self._state.total_records_processed += records_processed
        self._state.status = "running"
        self._state.error_message = None
        self._save_state()
        logger.debug(f"Progress updated: offset={offset}, total={self._state.total_records_processed}")

    def mark_completed(self) -> None:
        """Mark ingestion as completed."""
        self._state.status = "completed"
        self._save_state()
        logger.info(f"Ingestion completed. Total records: {self._state.total_records_processed}")

    def mark_failed(self, error: str) -> None:
        """Mark ingestion as failed."""
        self._state.status = "failed"
        self._state.error_message = error
        self._save_state()
        logger.error(f"Ingestion failed: {error}")

    def reset(self) -> None:
        """Reset state to start fresh."""
        self._state = IngestionState()
        self._save_state()
        logger.info("State reset to initial values")

    @classmethod
    def reset_instance(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        with cls._lock:
            cls._instance = None
