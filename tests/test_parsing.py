from __future__ import annotations

import csv
import gzip
import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from airbnb_etl.config import Market
from airbnb_etl.ddl import TABLES
from airbnb_etl.parsing import (
    MapStats,
    as_bool,
    as_money,
    as_percent,
    iter_records,
    map_calendars,
    map_listings,
)


def _market() -> Market:
    return Market(
        code="albany",
        name="Albany",
        country="united-states",
        region="ny",
        city="albany",
        snapshot_date=date(2026, 6, 16),
    )


def test_money_percent_and_bool_converters() -> None:
    assert as_money("$1,234.50") == Decimal("1234.50")
    assert as_money("") is None
    assert as_money("N/A") is None
    assert as_percent("98%") == Decimal("98")
    assert as_percent("") is None
    assert as_bool("t") is True
    assert as_bool("f") is False
    assert as_bool("") is None


def test_listing_projection_and_rejections(tmp_path: Path) -> None:
    path = tmp_path / "listings.csv.gz"
    header = [
        "id",
        "name",
        "price",
        "host_is_superhost",
        "host_response_rate",
        "amenities",
        "room_type",
        "neighbourhood_cleansed",
        "latitude",
        "longitude",
    ]
    rows = [
        [
            "10",
            "Loft",
            "$1,234.50",
            "t",
            "98%",
            '["Wifi"]',
            "Entire home/apt",
            "CENTER",
            "42.65",
            "-73.75",
        ],
        ["", "missing", "", "", "", "", "", "", "", ""],
        ["10", "dup", "$10", "f", "", "", "Private room", "CENTER", "42.6", "-73.7"],
    ]
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)

    stats = MapStats()
    projected = list(map_listings(_market(), iter_records(path), stats))
    assert stats.seen == 3
    assert stats.rejected == 2
    assert len(projected) == 1
    names = [column.name for column in TABLES["listings"].columns]
    row = dict(zip(names, projected[0]))
    assert row["listing_id"] == 10
    assert row["price"] == Decimal("1234.50")
    assert row["host_is_superhost"] is True
    assert row["host_response_rate"] == Decimal("98")
    assert row["neighbourhood_cleansed"] == "CENTER"
    assert len(projected[0]) == len(TABLES["listings"].columns)


def test_calendar_dedupes_listing_and_date() -> None:
    stats = MapStats()
    records = [
        {
            "listing_id": "10",
            "date": "2026-06-16",
            "available": "t",
            "minimum_nights": "2",
            "maximum_nights": "30",
        },
        {
            "listing_id": "10",
            "date": "2026-06-16",
            "available": "f",
            "minimum_nights": "2",
            "maximum_nights": "30",
        },
        {
            "listing_id": "10",
            "date": "2026-06-17",
            "available": "f",
            "minimum_nights": "2",
            "maximum_nights": "30",
        },
    ]
    rows = list(map_calendars(_market(), records, stats))
    assert len(rows) == 2
    assert stats.rejected == 1
    names = [column.name for column in TABLES["calendars"].columns]
    first = dict(zip(names, rows[0]))
    assert first["stay_date"] == date(2026, 6, 16)
    assert first["available"] is True


def test_geojson_round_trip_shape(tmp_path: Path) -> None:
    path = tmp_path / "neighbourhoods.geojson"
    path.write_text(
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
    from airbnb_etl.parsing import iter_geometries, map_geometries

    stats = MapStats()
    rows = list(map_geometries(_market(), iter_geometries(path), stats))
    assert len(rows) == 1
    assert stats.rejected == 0
