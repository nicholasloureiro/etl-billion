"""API client module for fetching vaccination data."""
import logging
import time
from typing import List, Dict, Any, Optional, Iterator
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import APIConfig

logger = logging.getLogger(__name__)


@dataclass
class APIResponse:
    """API response wrapper."""
    records: List[Dict[str, Any]]
    offset: int
    limit: int
    has_more: bool


class VacinacaoAPIClient:
    """Client for the Brazilian vaccination API."""

    DATA_KEY = "doses_aplicadas_pni"

    def __init__(self, config: APIConfig) -> None:
        self._config = config
        self._session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()

        retry_strategy = Retry(
            total=self._config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        session.headers.update({
            "Accept": "application/json",
            "User-Agent": "VacinacaoDataIngestion/1.0",
        })

        return session

    def _build_url(self, offset: int, limit: int) -> str:
        """Build the API URL with query parameters."""
        return f"{self._config.base_url}{self._config.endpoint}?limit={limit}&offset={offset}"

    def fetch_batch(self, offset: int, limit: Optional[int] = None) -> APIResponse:
        """Fetch a single batch of records from the API.

        Args:
            offset: The page offset (0-indexed)
            limit: Number of records per page (default from config)

        Returns:
            APIResponse with records and pagination info
        """
        limit = limit or self._config.batch_size
        url = self._build_url(offset, limit)

        logger.info(f"Fetching batch: offset={offset}, limit={limit}")

        last_error: Optional[Exception] = None

        for attempt in range(self._config.max_retries):
            try:
                response = self._session.get(url, timeout=self._config.request_timeout)
                response.raise_for_status()

                data = response.json()
                records = data.get(self.DATA_KEY, [])

                logger.info(f"Fetched {len(records)} records from offset {offset}")

                return APIResponse(
                    records=records,
                    offset=offset,
                    limit=limit,
                    has_more=len(records) == limit,
                )

            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"Request attempt {attempt + 1}/{self._config.max_retries} failed: {e}")
                if attempt < self._config.max_retries - 1:
                    time.sleep(self._config.retry_delay * (attempt + 1))

        raise RuntimeError(f"Failed to fetch batch after {self._config.max_retries} attempts: {last_error}")

    def fetch_all(self, start_offset: int = 0) -> Iterator[APIResponse]:
        """Fetch all records starting from a given offset.

        Yields APIResponse objects for each batch.
        """
        offset = start_offset
        limit = self._config.batch_size

        while True:
            response = self.fetch_batch(offset, limit)
            yield response

            if not response.has_more:
                logger.info(f"Reached end of data at offset {offset}")
                break

            offset += 1  # Offset is page number, not record number

    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()
