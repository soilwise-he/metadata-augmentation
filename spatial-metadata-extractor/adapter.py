from __future__ import annotations
import csv
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Iterator, Optional

import requests
from requests.sessions import HTTPAdapter
from urllib3 import Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standardised record produced by every adapter
# ---------------------------------------------------------------------------

@dataclass
class SourceRecord:
    """
    Returns a single record with an identifier, a URL, and optional metadata.
    """
    identifier: str
    url: str
    lname: Optional[str] = None
    mediatype: Optional[str] = None
    skip_link_check: bool = False
    extra: dict = field(default_factory=dict)

# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class SourceAdapter(ABC):
    """
    Base class for all source adapters.
    Subclasses must implement `records()` which yields SourceRecord objects.
    """

    @abstractmethod
    def records(self) -> Iterator[SourceRecord]:
        """Yield one SourceRecord object at a time."""

    def __iter__(self) -> Iterator[SourceRecord]:
        return self.records()

    @staticmethod
    def parse_url_field(raw: str) -> list[tuple[str, Optional[str]]]:
        """
        Parse a URL field that may be a plain string or a JSON array.
        Returns a list of (url, mediatype) tuples.

        Handles:
          - plain URL string       → [("https://...", None)]
          - JSON array of strings  → [("https://a", None), ("https://b", None)]
          - JSON array of objects  → [("https://a", "image/tiff"), ...]
        """
        if not raw:
            return []

        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                parsed = [parsed]

            result = []
            for item in parsed:
                if isinstance(item, dict):
                    url       = item.get("url") or item.get("href")
                    mediatype = item.get("type") or item.get("mediatype")
                else:
                    url, mediatype = str(item), None

                if url:
                    result.append((url, mediatype))
            return result

        except (json.JSONDecodeError, TypeError):
            return [(raw.strip(), None)] if raw.strip() else []

# ---------------------------------------------------------------------------
# PostgreSQL adapter
# ---------------------------------------------------------------------------

class PostgreSQLAdapter(SourceAdapter):
    """
    Fetch records from a PostgreSQL database.
    """

    DEFAULT_QUERY = """
        SELECT identifier, spatial
        FROM metadata.records
        WHERE COALESCE(spatial, '') <> ''
          AND identifier NOT IN (
              SELECT record_id
              FROM metadata.augment_status
              WHERE process = 'spatial-locator'
          )
    """

    def __init__(
        self,
        db_config: dict,
        query: str = DEFAULT_QUERY,
        identifier_col: str = "identifier",
        links_col: str = "spatial",
    ):
        self.db_config      = db_config
        self.query          = query
        self.identifier_col = identifier_col
        self.links_col      = links_col

    def records(self) -> Iterator[SourceRecord]:
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(**self.db_config)
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(self.query)
                rows = cur.fetchall()

            logger.info(f"[PostgreSQL] Fetched {len(rows)} rows")

            for row in rows:
                identifier = row.get(self.identifier_col, "unknown")
                raw_links  = row.get(self.links_col, "")
                extra      = {
                    k: v for k, v in row.items()
                    if k not in (self.identifier_col, self.links_col)
                }

                for url, mediatype in self.parse_url_field(raw_links):
                    yield SourceRecord(
                        identifier=identifier,
                        url=url,
                        mediatype=mediatype,
                        skip_link_check=False,
                        extra=extra,
                    )
        finally:
            conn.close()

# ---------------------------------------------------------------------------
# CSV adapter
# ---------------------------------------------------------------------------

class CSVAdapter(SourceAdapter):
    """
    Read records from a CSV file.
    """

    def __init__(
        self,
        filepath: str,
        url_col: str = "url",
        encoding: str = "utf-8",
    ):
        self.filepath = filepath
        self.url_col  = url_col
        self.encoding = encoding

    def records(self) -> Iterator[SourceRecord]:
        with open(self.filepath, newline="", encoding=self.encoding) as fh:
            reader = csv.DictReader(fh)

            if self.url_col not in (reader.fieldnames or []):
                raise ValueError(
                    f"URL column '{self.url_col}' not found. "
                    f"Available: {reader.fieldnames}"
                )

            for i, row in enumerate(reader, 1):
                raw_url    = row.pop(self.url_col, "").strip()
                identifier = row.pop("identifier", f"row_{i}").strip() or f"row_{i}"
                mediatype  = row.pop("mediatype", None) or None

                urls = self.parse_url_field(raw_url)
                if not urls:
                    logger.debug(f"[CSV] Row {i} has no URL — skipped")
                    continue

                for url, discovered_mediatype in urls:
                    yield SourceRecord(
                        identifier=identifier,
                        url=url,
                        mediatype=discovered_mediatype or mediatype,
                        skip_link_check=False,
                        extra=row.copy(),
                    )

        logger.info(f"[CSV] Finished reading {self.filepath}")

# ---------------------------------------------------------------------------
# Zenodo adapter
# ---------------------------------------------------------------------------

class ZenodoAdapter(SourceAdapter):
    """
    Fetch records from the Zenodo REST API.

    Two modes
    ---------
    1. record_ids   — process a specific list of Zenodo IDs
    2. search_query — run a free-text search and process all matching deposits

    Every file inside a deposit becomes one SourceRecord.
    Zenodo provides each file's type and download URL directly, so
    skip_link_check is set to True.

    Parameters
    ----------
    record_ids   : explicit list of Zenodo deposit IDs to fetch
    search_query : free-text Zenodo search string (used when record_ids is empty)
    community    : optional Zenodo community slug to restrict the search
    access_token : optional personal access token for restricted records
    base_url     : override for non-production Zenodo instances (e.g. sandbox)
    page_size    : number of results per search page (max 25)
    max_records  : stop after this many deposits (0 = no limit)
    """

    API_BASE = "https://zenodo.org/api"
    SPATIAL_FORMATS = '(".geojson" OR ".shp" OR ".gpkg" OR ".tif" OR ".tiff" OR ".nc" OR ".gml" OR ".kml")'

    def __init__(
        self,
        record_ids: Optional[list[str]] = None,
        search_query: Optional[str] = None,
        community: Optional[str] = None,
        access_token: Optional[str] = None,
        base_url: Optional[str] = None,
        page_size: int = 25,
        max_records: int = 0,
    ):
        if not record_ids and not search_query:
            raise ValueError("Provide either record_ids or a search_query")

        self.record_ids   = record_ids or []
        self.search_query = search_query
        self.community    = community
        self.access_token = access_token
        self.base_url     = (base_url or self.API_BASE).rstrip("/")
        self.page_size    = min(page_size, 25)
        self.max_records  = max_records

        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        if self.access_token:
            self._session.headers.update({"Authorization": f"Bearer {self.access_token}"})
        # Add retry logic
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        self._session.mount('https://', HTTPAdapter(max_retries=retry))
    
    def records(self) -> Iterator[SourceRecord]:
        if self.record_ids:
            yield from self._records_by_id()
        else:
            yield from self._records_by_search()

    def _records_by_id(self) -> Iterator[SourceRecord]:
        for record_id in self.record_ids:
            try:
                data = self._get(f"{self.base_url}/records/{record_id}")
                yield from self._expand_deposit(data)
            except Exception as e:
                logger.error(f"[Zenodo] Failed to fetch deposit {record_id}: {e}")

    def _records_by_search(self) -> Iterator[SourceRecord]:
        params: dict = {
            'q': f'{self.search_query} AND {self.SPATIAL_FORMATS}',
            "size": self.page_size,
            "page": 1,
            "sort": "mostrecent",
        }
        if self.community:
            params["communities"] = self.community

        total_fetched = 0

        while True:
            data = self._get(f"{self.base_url}/records", params=params)
            hits = data.get("hits", {}).get("hits", [])

            if not hits:
                break

            for deposit in hits:
                if self.max_records and total_fetched >= self.max_records:
                    logger.info(f"[Zenodo] Reached max_records limit ({self.max_records})")
                    return
                yield from self._expand_deposit(deposit)
                total_fetched += 1

            total_pages = -(-data["hits"]["total"] // self.page_size)
            if params["page"] >= total_pages:
                break
            params["page"] += 1

        logger.info(f"[Zenodo] Search complete — {total_fetched} deposits processed")

    @staticmethod
    def _expand_deposit(deposit: dict) -> Iterator[SourceRecord]:
        zenodo_id = str(deposit.get("id", "unknown"))
        doi       = deposit.get("doi", "")
        title     = deposit.get("metadata", {}).get("title", "")
        files     = deposit.get("files", [])

        # Build deposit-level file list summary
        file_summary = [
            {
                "name":     f.get("key", ""),
                "mimetype": f.get("type") or f.get("mimetype"),
                "size":     f.get("size"),
            }
            for f in files
        ]

        for file_entry in files:
            url = (
                file_entry.get("links", {}).get("self")
                or file_entry.get("links", {}).get("download")
            )
            if not url:
                continue

            yield SourceRecord(
                identifier=f"zenodo:{zenodo_id}:{file_entry.get('key', '')}",
                url=url,
                mediatype=file_entry.get("type") or None,
                skip_link_check=True,
                extra={
                    "zenodo_id":    zenodo_id,
                    "doi":          doi,
                    "title":        title,
                    "filename":     file_entry.get("key", ""),
                    "filesize":     file_entry.get("size"),
                    "deposit_files": file_summary,  # ← all files in the deposit
                },
            )

    def _get(self, url: str, params: Optional[dict] = None) -> dict:
        response = self._session.get(url, params=params, timeout=50)
        response.raise_for_status()
        return response.json()

# ---------------------------------------------------------------------------
# Registry — convenience factory
# ---------------------------------------------------------------------------

ADAPTER_REGISTRY: dict[str, type[SourceAdapter]] = {
    "postgresql": PostgreSQLAdapter,
    "csv":        CSVAdapter,
    "zenodo":     ZenodoAdapter,
}

def get_adapter(source_type: str, **kwargs) -> SourceAdapter:
    """
    Factory helper — instantiate an adapter by name.

    Example
    -------
    adapter = get_adapter("zenodo", search_query="soil moisture", community="soilwise")
    adapter = get_adapter("postgresql", db_config={...})
    adapter = get_adapter("csv", filepath="records.csv")
    """
    adapter_cls = ADAPTER_REGISTRY.get(source_type.lower())
    if not adapter_cls:
        raise ValueError(
            f"Unknown source type '{source_type}'. "
            f"Available: {list(ADAPTER_REGISTRY)}"
        )
    return adapter_cls(**kwargs)