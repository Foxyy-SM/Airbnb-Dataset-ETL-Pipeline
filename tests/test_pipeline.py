from __future__ import annotations

import asyncio
import csv
import gzip
import json
import socket
from datetime import date
from decimal import Decimal
from pathlib import Path

from aiohttp import web

from airbnb_etl.config import Market, Settings, find_markets_file, load_catalog
from airbnb_etl.datasets import DATASETS, dataset_by_name
from airbnb_etl.ddl import TABLES
from airbnb_etl.pipeline import FileLoad, execute
from airbnb_etl.storage import LocalStorage


class Collecting:
    def __init__(self) -> None:
        self.tables: dict[str, list[tuple]] = {}

    def load(self, table: str, rows) -> int:
        stored = list(rows)
        self.tables[table] = stored
        return len(stored)

    def finish_market(self, market: Market) -> None:
        return None

    def record_files(self, run_id: str | None, files: list[FileLoad]) -> None:
        return None

    def close(self) -> None:
        return None


def test_catalog_builds_the_albany_snapshot_urls() -> None:
    catalog = load_catalog(find_markets_file())
    albany = catalog.get("albany")
    detail = dataset_by_name("listings_detail")
    assert albany.url_for(detail) == (
        "https://data.insideairbnb.com/united-states/ny/albany/2026-06-16/data/listings.csv.gz"
    )
    assert len(DATASETS) == 7
    assert [item.name for item in DATASETS if item.compressed] == [
        "listings_detail",
        "calendar",
        "reviews_detail",
    ]


def _write_snapshot(root: Path) -> None:
    data = root / "data"
    visual = root / "visualisations"
    data.mkdir(parents=True)
    visual.mkdir(parents=True)
    with gzip.open(data / "listings.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ["id", "name", "price", "host_is_superhost", "room_type", "neighbourhood_cleansed"]
        )
        writer.writerow(["10", "Loft", "$80.43", "t", "Entire home/apt", "CENTER"])
        writer.writerow(["", "skip", "", "", "", ""])
    with gzip.open(data / "calendar.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["listing_id", "date", "available", "minimum_nights", "maximum_nights"])
        writer.writerow(["10", "2026-06-16", "t", "2", "30"])
        writer.writerow(["10", "2026-06-17", "f", "2", "30"])
    with gzip.open(data / "reviews.csv.gz", "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["listing_id", "id", "date", "reviewer_id", "reviewer_name", "comments"])
        writer.writerow(["10", "99", "2026-01-02", "7", "Ada", "Nice, place"])
    with (visual / "listings.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "name", "price", "room_type", "neighbourhood"])
        writer.writerow(["10", "Loft", "80", "Entire home/apt", "CENTER"])
    with (visual / "reviews.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["listing_id", "date"])
        writer.writerow(["10", "2026-01-02"])
    with (visual / "neighbourhoods.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["neighbourhood_group", "neighbourhood"])
        writer.writerow(["", "CENTER"])
    (visual / "neighbourhoods.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"neighbourhood": "CENTER", "neighbourhood_group": None},
                        "geometry": {"type": "MultiPolygon", "coordinates": []},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_pipeline_projects_all_seven_files(tmp_path: Path) -> None:
    source = tmp_path / "source"
    _write_snapshot(source)

    async def scenario() -> Collecting:
        app = web.Application()
        app.router.add_static("/", source)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        runner = web.AppRunner(app)
        await runner.setup()
        await web.SockSite(runner, sock).start()
        collector = Collecting()
        market = Market(
            code="albany",
            name="Albany",
            country="united-states",
            region="ny",
            city="albany",
            snapshot_date=date(2026, 6, 16),
            base_url=f"http://127.0.0.1:{port}",
        )
        settings = Settings(
            data_dir=tmp_path / "raw",
            download_concurrency=7,
            reuse_downloads=False,
            download_retries=1,
        )
        try:
            stats = await execute(
                market,
                settings,
                LocalStorage(settings.data_dir),
                lambda: collector,
            )
        finally:
            await runner.cleanup()
        by_name = {item.dataset: item for item in stats.files}
        assert by_name["listings_detail"].row_count == 1
        assert by_name["listings_detail"].rejected_count == 1
        assert by_name["calendar"].row_count == 2
        assert by_name["reviews_detail"].row_count == 1
        assert by_name["listings_summary"].row_count == 1
        assert by_name["reviews_summary"].row_count == 1
        assert by_name["neighbourhoods"].row_count == 1
        assert by_name["neighbourhood_geojson"].row_count == 1
        return collector

    collector = asyncio.run(scenario())
    names = [column.name for column in TABLES["listings"].columns]
    row = dict(zip(names, collector.tables["listings"][0]))
    assert row["price"] == Decimal("80.43")
    assert row["host_is_superhost"] is True
    assert row["room_type"] == "Entire home/apt"
