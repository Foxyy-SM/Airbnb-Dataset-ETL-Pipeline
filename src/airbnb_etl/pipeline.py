"""Download a city snapshot, project each file, and hand rows to a loader."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Iterable, Iterator, Protocol

from airbnb_etl.config import Market, Settings
from airbnb_etl.datasets import DATASETS, dataset_by_name
from airbnb_etl.download import DownloadResult, download_market
from airbnb_etl.parsing import (
    MapStats,
    iter_geometries,
    iter_records,
    map_calendars,
    map_geometries,
    map_listing_summaries,
    map_listings,
    map_neighbourhoods,
    map_review_dates,
    map_reviews,
)
from airbnb_etl.storage import ObjectStorage

logger = logging.getLogger(__name__)

_MAPPERS = {
    "listings_detail": map_listings,
    "calendar": map_calendars,
    "reviews_detail": map_reviews,
    "listings_summary": map_listing_summaries,
    "reviews_summary": map_review_dates,
    "neighbourhoods": map_neighbourhoods,
    "neighbourhood_geojson": map_geometries,
}


class Loader(Protocol):
    def load(self, table: str, rows: Iterable[tuple]) -> int: ...

    def finish_market(self, market: Market) -> None: ...

    def record_files(self, run_id: str | None, files: list[FileLoad]) -> None: ...

    def close(self) -> None: ...


@dataclass
class FileLoad:
    dataset: str
    url: str
    key: str
    byte_size: int
    sha256: str
    row_count: int
    rejected_count: int
    elapsed_seconds: float
    reused: bool

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class RunStats:
    market_code: str
    snapshot_date: str
    files: list[FileLoad] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    def as_dict(self) -> dict:
        payload = asdict(self)
        payload["files"] = [item.as_dict() for item in self.files]
        return payload


class StreamCounter:
    """Dry-run loader. Counts rows and discards them."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    def load(self, table: str, rows: Iterable[tuple]) -> int:
        count = sum(1 for _ in rows)
        self.counts[table] = count
        return count

    def finish_market(self, market: Market) -> None:
        return None

    def record_files(self, run_id: str | None, files: list[FileLoad]) -> None:
        return None

    def close(self) -> None:
        return None


def _rows_for(dataset_name: str, path, market: Market, stats: MapStats) -> Iterator[tuple]:
    dataset = dataset_by_name(dataset_name)
    records = iter_geometries(path) if dataset.kind == "geojson" else iter_records(path)
    mapper = _MAPPERS[dataset_name]
    return mapper(market, records, stats)


def _load_paths(
    market: Market,
    loader: Loader,
    downloaded_paths: list[tuple[object, DownloadResult, object]],
    run_id: str | None,
    started: float,
) -> RunStats:
    stats = RunStats(
        market_code=market.code,
        snapshot_date=market.snapshot_date.isoformat(),
    )
    for dataset, downloaded, path in downloaded_paths:
        map_stats = MapStats()
        logger.info("loading %s from %s", dataset.table, path.name)
        loaded = loader.load(dataset.table, _rows_for(dataset.name, path, market, map_stats))
        if map_stats.seen != loaded + map_stats.rejected:
            raise RuntimeError(
                f"{dataset.name} accounting mismatch: "
                f"seen={map_stats.seen} loaded={loaded} rejected={map_stats.rejected}"
            )
        stats.files.append(
            FileLoad(
                dataset=dataset.name,
                url=downloaded.url,
                key=downloaded.key,
                byte_size=downloaded.byte_size,
                sha256=downloaded.sha256,
                row_count=loaded,
                rejected_count=map_stats.rejected,
                elapsed_seconds=round(downloaded.elapsed_seconds, 3),
                reused=downloaded.reused,
            )
        )
        logger.info("%s rows=%s rejected=%s", dataset.name, loaded, map_stats.rejected)
    loader.finish_market(market)
    loader.record_files(run_id, stats.files)
    stats.elapsed_seconds = round(time.perf_counter() - started, 3)
    return stats


async def execute(
    market: Market,
    settings: Settings,
    storage: ObjectStorage,
    loader_factory: Callable[[], Loader],
    run_id: str | None = None,
) -> RunStats:
    """Download on the event loop, then parse and load on a worker thread."""
    started = time.perf_counter()
    if settings.storage_backend.strip().lower() == "azure":
        ensure = getattr(storage, "ensure_container", None)
        if ensure is not None:
            await ensure()
    downloads = await download_market(market, storage, settings)
    materialized = []
    for dataset in DATASETS:
        downloaded = next(item for item in downloads if item.dataset == dataset.name)
        path = await storage.materialize(downloaded.key)
        materialized.append((dataset, downloaded, path))

    def _run() -> RunStats:
        loader = loader_factory()
        try:
            return _load_paths(market, loader, materialized, run_id, started)
        finally:
            loader.close()

    return await asyncio.to_thread(_run)
