"""Stream gzip and CSV into typed warehouse rows.

Converters accept the raw Inside Airbnb strings: `$80.43`, `t`/`f`, `98%`,
and empty fields. Rows that cannot supply a primary key are rejected and counted.
"""

from __future__ import annotations

import csv
import gzip
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Iterator

from airbnb_etl.config import Market
from airbnb_etl.ddl import TABLES, VARCHAR_LIMITS

csv.field_size_limit(10 * 1024 * 1024)

_INT_MAX = 2_147_483_647
_BIGINT_MAX = 9_223_372_036_854_775_807
_MISSING = {"", "N/A", "NA", "NULL", "NONE"}


@dataclass
class MapStats:
    seen: int = 0
    rejected: int = 0


def clip(kind: str, value: object) -> object:
    if not isinstance(value, str):
        return value
    limit = VARCHAR_LIMITS.get(kind)
    if limit is not None and len(value) > limit:
        return value[:limit]
    return value


def row_tuple(table: str, values: dict[str, object]) -> tuple[object, ...]:
    spec = TABLES[table]
    rendered = []
    for column in spec.columns:
        if column.name not in values:
            raise KeyError(f"{table}.{column.name}")
        rendered.append(clip(column.kind, values[column.name]))
    return tuple(rendered)


def as_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    if text.upper() in _MISSING:
        return None
    return text


def _decimal(value: str | None) -> Decimal | None:
    text = as_text(value)
    if text is None:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def as_money(value: str | None) -> Decimal | None:
    text = as_text(value)
    if text is None:
        return None
    return _decimal(text.replace("$", "").replace(",", ""))


def as_percent(value: str | None) -> Decimal | None:
    text = as_text(value)
    if text is None:
        return None
    return _decimal(text.replace("%", ""))


def as_int(value: str | None) -> int | None:
    number = _decimal(as_text(value))
    if number is None:
        return None
    rendered = int(number)
    if rendered > _INT_MAX or rendered < -_INT_MAX - 1:
        return None
    return rendered


def as_bigint(value: str | None) -> int | None:
    number = _decimal(as_text(value))
    if number is None:
        return None
    rendered = int(number)
    if rendered > _BIGINT_MAX or rendered < -_BIGINT_MAX - 1:
        return None
    return rendered


def as_bool(value: str | None) -> bool | None:
    text = as_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"t", "true", "1", "yes"}:
        return True
    if lowered in {"f", "false", "0", "no"}:
        return False
    return None


def as_date(value: str | None) -> date | None:
    text = as_text(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def iter_records(path: Path) -> Iterator[dict[str, str]]:
    opener = gzip.open if path.name.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8", newline="", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield {key: value for key, value in row.items() if key is not None}


def iter_geometries(path: Path) -> Iterator[dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    for feature in payload.get("features", []):
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry")
        yield {
            "neighbourhood": properties.get("neighbourhood"),
            "neighbourhood_group": properties.get("neighbourhood_group"),
            "geometry": json.dumps(geometry) if geometry else None,
        }


def _reject(stats: MapStats) -> None:
    stats.seen += 1
    stats.rejected += 1


def map_listings(
    market: Market, records: Iterable[dict[str, str]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    seen: set[int] = set()
    for record in records:
        listing_id = as_bigint(record.get("id"))
        if listing_id is None or listing_id in seen:
            _reject(stats)
            continue
        seen.add(listing_id)
        stats.seen += 1
        yield row_tuple(
            "listings",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "listing_id": listing_id,
                "name": as_text(record.get("name")),
                "host_id": as_bigint(record.get("host_id")),
                "host_name": as_text(record.get("host_name")),
                "host_since": as_date(record.get("host_since")),
                "host_is_superhost": as_bool(record.get("host_is_superhost")),
                "host_listings_count": as_int(record.get("host_listings_count")),
                "host_identity_verified": as_bool(record.get("host_identity_verified")),
                "host_response_rate": as_percent(record.get("host_response_rate")),
                "host_acceptance_rate": as_percent(record.get("host_acceptance_rate")),
                "neighbourhood": as_text(record.get("neighbourhood")),
                "neighbourhood_cleansed": as_text(record.get("neighbourhood_cleansed")),
                "neighbourhood_group": as_text(record.get("neighbourhood_group_cleansed")),
                "latitude": as_money(record.get("latitude")),
                "longitude": as_money(record.get("longitude")),
                "property_type": as_text(record.get("property_type")),
                "room_type": as_text(record.get("room_type")),
                "accommodates": as_int(record.get("accommodates")),
                "bathrooms": _decimal(record.get("bathrooms")),
                "bedrooms": as_int(record.get("bedrooms")),
                "beds": as_int(record.get("beds")),
                "amenities": as_text(record.get("amenities")),
                "price": as_money(record.get("price")),
                "price_quote_total": as_money(record.get("price_quote_total_price")),
                "price_quote_per_night": as_money(record.get("price_quote_price_per_night")),
                "minimum_nights": as_int(record.get("minimum_nights")),
                "maximum_nights": as_int(record.get("maximum_nights")),
                "has_availability": as_bool(record.get("has_availability")),
                "availability_30": as_int(record.get("availability_30")),
                "availability_365": as_int(record.get("availability_365")),
                "number_of_reviews": as_int(record.get("number_of_reviews")),
                "number_of_reviews_ltm": as_int(record.get("number_of_reviews_ltm")),
                "number_of_reviews_l30d": as_int(record.get("number_of_reviews_l30d")),
                "first_review": as_date(record.get("first_review")),
                "last_review": as_date(record.get("last_review")),
                "review_scores_rating": _decimal(record.get("review_scores_rating")),
                "review_scores_accuracy": _decimal(record.get("review_scores_accuracy")),
                "review_scores_cleanliness": _decimal(record.get("review_scores_cleanliness")),
                "review_scores_checkin": _decimal(record.get("review_scores_checkin")),
                "review_scores_communication": _decimal(
                    record.get("review_scores_communication")
                ),
                "review_scores_location": _decimal(record.get("review_scores_location")),
                "review_scores_value": _decimal(record.get("review_scores_value")),
                "estimated_occupancy_l365d": _decimal(record.get("estimated_occupancy_l365d")),
                "estimated_revenue_l365d": as_money(record.get("estimated_revenue_l365d")),
                "instant_bookable": as_bool(record.get("instant_bookable")),
                "reviews_per_month": _decimal(record.get("reviews_per_month")),
                "license": as_text(record.get("license")),
            },
        )


def map_listing_summaries(
    market: Market, records: Iterable[dict[str, str]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    seen: set[int] = set()
    for record in records:
        listing_id = as_bigint(record.get("id"))
        if listing_id is None or listing_id in seen:
            _reject(stats)
            continue
        seen.add(listing_id)
        stats.seen += 1
        yield row_tuple(
            "listing_summaries",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "listing_id": listing_id,
                "name": as_text(record.get("name")),
                "host_id": as_bigint(record.get("host_id")),
                "host_profile_id": as_text(record.get("host_profile_id")),
                "host_name": as_text(record.get("host_name")),
                "neighbourhood_group": as_text(record.get("neighbourhood_group")),
                "neighbourhood": as_text(record.get("neighbourhood")),
                "latitude": as_money(record.get("latitude")),
                "longitude": as_money(record.get("longitude")),
                "room_type": as_text(record.get("room_type")),
                "price": as_money(record.get("price")),
                "minimum_nights": as_int(record.get("minimum_nights")),
                "number_of_reviews": as_int(record.get("number_of_reviews")),
                "last_review": as_date(record.get("last_review")),
                "reviews_per_month": _decimal(record.get("reviews_per_month")),
                "calculated_host_listings_count": as_int(
                    record.get("calculated_host_listings_count")
                ),
                "availability_365": as_int(record.get("availability_365")),
                "number_of_reviews_ltm": as_int(record.get("number_of_reviews_ltm")),
                "license": as_text(record.get("license")),
            },
        )


def map_calendars(
    market: Market, records: Iterable[dict[str, str]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    seen: set[tuple[int, date]] = set()
    for record in records:
        listing_id = as_bigint(record.get("listing_id"))
        stay_date = as_date(record.get("date"))
        if listing_id is None or stay_date is None:
            _reject(stats)
            continue
        key = (listing_id, stay_date)
        if key in seen:
            _reject(stats)
            continue
        seen.add(key)
        stats.seen += 1
        yield row_tuple(
            "calendars",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "listing_id": listing_id,
                "stay_date": stay_date,
                "available": as_bool(record.get("available")),
                "minimum_nights": as_int(record.get("minimum_nights")),
                "maximum_nights": as_int(record.get("maximum_nights")),
            },
        )


def map_reviews(
    market: Market, records: Iterable[dict[str, str]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    seen: set[int] = set()
    for record in records:
        review_id = as_bigint(record.get("id"))
        listing_id = as_bigint(record.get("listing_id"))
        if review_id is None or listing_id is None or review_id in seen:
            _reject(stats)
            continue
        seen.add(review_id)
        stats.seen += 1
        comments = as_text(record.get("comments"))
        if comments is not None and len(comments) > 100_000:
            comments = comments[:100_000]
        yield row_tuple(
            "reviews",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "review_id": review_id,
                "listing_id": listing_id,
                "review_date": as_date(record.get("date")),
                "reviewer_id": as_bigint(record.get("reviewer_id")),
                "reviewer_name": as_text(record.get("reviewer_name")),
                "comments": comments,
            },
        )


def map_review_dates(
    market: Market, records: Iterable[dict[str, str]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    for source_row, record in enumerate(records, start=1):
        listing_id = as_bigint(record.get("listing_id"))
        review_date = as_date(record.get("date"))
        if listing_id is None or review_date is None:
            _reject(stats)
            continue
        stats.seen += 1
        yield row_tuple(
            "review_dates",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "source_row": source_row,
                "listing_id": listing_id,
                "review_date": review_date,
            },
        )


def map_neighbourhoods(
    market: Market, records: Iterable[dict[str, str]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    seen: set[str] = set()
    for record in records:
        name = as_text(record.get("neighbourhood"))
        if name is None or name in seen:
            _reject(stats)
            continue
        seen.add(name)
        stats.seen += 1
        yield row_tuple(
            "neighbourhoods",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "neighbourhood": name,
                "neighbourhood_group": as_text(record.get("neighbourhood_group")),
            },
        )


def map_geometries(
    market: Market, records: Iterable[dict[str, object]], stats: MapStats
) -> Iterator[tuple[object, ...]]:
    seen: set[str] = set()
    for record in records:
        raw_name = record.get("neighbourhood")
        name = as_text(raw_name if isinstance(raw_name, str) else None)
        geometry = record.get("geometry")
        if name is None or geometry is None or name in seen:
            _reject(stats)
            continue
        seen.add(name)
        stats.seen += 1
        group = record.get("neighbourhood_group")
        yield row_tuple(
            "neighbourhood_geometries",
            {
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "neighbourhood": name,
                "neighbourhood_group": as_text(group if isinstance(group, str) else None),
                "geometry": geometry,
            },
        )
