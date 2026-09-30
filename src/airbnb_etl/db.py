"""PostgreSQL schema bootstrap, COPY loader, run history, and read queries."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Iterable

import psycopg
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool

from airbnb_etl.config import Market, Settings
from airbnb_etl.ddl import (
    POSTGRES_DELETE_METRICS,
    POSTGRES_INSERT_METRICS,
    TABLES,
    render_schema,
    split_sql,
)
from airbnb_etl.pipeline import FileLoad

logger = logging.getLogger(__name__)

_IDENT = re.compile(r"^[a-z_][a-z0-9_]*$")

_LISTING_LIST = """
listing_id, name, host_id, host_name, host_is_superhost,
neighbourhood_cleansed, neighbourhood_group, room_type, price,
accommodates, bedrooms, number_of_reviews, review_scores_rating,
availability_365, estimated_revenue_l365d, instant_bookable
""".strip()

_LISTING_DETAIL = """
listing_id, name, host_id, host_name, host_since, host_is_superhost,
host_listings_count, host_identity_verified, host_response_rate, host_acceptance_rate,
neighbourhood, neighbourhood_cleansed, neighbourhood_group, latitude, longitude,
property_type, room_type, accommodates, bathrooms, bedrooms, beds, amenities,
price, price_quote_total, price_quote_per_night, minimum_nights, maximum_nights,
has_availability, availability_30, availability_365, number_of_reviews,
number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review,
review_scores_rating, review_scores_accuracy, review_scores_cleanliness,
review_scores_checkin, review_scores_communication, review_scores_location,
review_scores_value, estimated_occupancy_l365d, estimated_revenue_l365d,
instant_bookable, reviews_per_month, license
""".strip()


class MarketNotLoaded(Exception):
    def __init__(self, market_code: str) -> None:
        super().__init__(f"{market_code} has not been loaded")
        self.market_code = market_code


class MemoryRunStore:
    def __init__(self) -> None:
        self._runs: dict[str, dict] = {}
        self._order: list[str] = []

    def create(self, record: dict) -> dict:
        self._runs[record["run_id"]] = dict(record)
        self._order.append(record["run_id"])
        return dict(record)

    def update(self, run_id: str, **fields: object) -> dict:
        current = self._runs[run_id]
        current.update(fields)
        return dict(current)

    def get(self, run_id: str) -> dict | None:
        found = self._runs.get(run_id)
        return dict(found) if found else None

    def list(self, limit: int = 20) -> list[dict]:
        ids = list(reversed(self._order))[:limit]
        return [dict(self._runs[run_id]) for run_id in ids]


def apply_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        for statement in split_sql(render_schema("postgresql")):
            cur.execute(statement)


def fail_stale_runs(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        UPDATE pipeline_runs
        SET status = 'failed',
            error_message = 'interrupted before completion',
            finished_at = NOW()
        WHERE status IN ('queued', 'running')
          AND started_at < NOW() - INTERVAL '6 hours'
        """
    )


def connect_pool(settings: Settings) -> ConnectionPool:
    with psycopg.connect(settings.database_url, connect_timeout=3) as conn:
        apply_schema(conn)
        fail_stale_runs(conn)
        conn.commit()
    return ConnectionPool(
        settings.database_url,
        min_size=1,
        max_size=4,
        kwargs={"connect_timeout": 5},
        open=True,
    )


def try_database(settings: Settings) -> ConnectionPool | None:
    try:
        return connect_pool(settings)
    except Exception:
        logger.warning("PostgreSQL is unavailable", exc_info=True)
        return None


def _rows(cursor: psycopg.Cursor) -> list[dict]:
    columns = [desc.name for desc in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _json_value(value: object) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _run_from_row(row: dict) -> dict:
    row["stats"] = _json_value(row.get("stats"))
    return row


class PostgresRunStore:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def create(self, record: dict) -> dict:
        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs (
                  run_id, market_code, snapshot_date, status, dry_run, started_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    record["run_id"],
                    record["market_code"],
                    record["snapshot_date"],
                    record["status"],
                    record["dry_run"],
                    record.get("started_at"),
                ),
            )
        return dict(record)

    def update(self, run_id: str, **fields: object) -> dict:
        assignments = []
        values: list[object] = []
        for key in ("status", "started_at", "finished_at", "error_message", "stats"):
            if key not in fields:
                continue
            assignments.append(f"{key} = %s")
            value = fields[key]
            if key == "stats" and value is not None:
                value = Json(value)
            values.append(value)
        if not assignments:
            found = self.get(run_id)
            if found is None:
                raise KeyError(run_id)
            return found
        values.append(run_id)
        with self.pool.connection() as conn:
            conn.execute(
                f"UPDATE pipeline_runs SET {', '.join(assignments)} WHERE run_id = %s",
                values,
            )
        found = self.get(run_id)
        if found is None:
            raise KeyError(run_id)
        return found

    def get(self, run_id: str) -> dict | None:
        with self.pool.connection() as conn:
            cursor = conn.execute(
                """
                SELECT run_id, market_code, snapshot_date, status, dry_run,
                       started_at, finished_at, error_message, stats
                FROM pipeline_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            rows = _rows(cursor)
        if not rows:
            return None
        return _run_from_row(rows[0])

    def list(self, limit: int = 20) -> list[dict]:
        with self.pool.connection() as conn:
            cursor = conn.execute(
                """
                SELECT run_id, market_code, snapshot_date, status, dry_run,
                       started_at, finished_at, error_message, stats
                FROM pipeline_runs
                ORDER BY started_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [_run_from_row(row) for row in _rows(cursor)]


def _adapt_row(table: str, row: tuple) -> list[object]:
    spec = TABLES[table]
    adapted: list[object] = []
    for column, value in zip(spec.columns, row):
        if column.kind == "json" and value is not None:
            payload = json.loads(value) if isinstance(value, str) else value
            adapted.append(Json(payload))
        else:
            adapted.append(value)
    return adapted


class PostgresLoader:
    """COPY each snapshot into a temp table, then replace that market's rows."""

    def __init__(self, conn: psycopg.Connection, market: Market) -> None:
        self.conn = conn
        self.market = market

    def close(self) -> None:
        self.conn.close()

    def load(self, table: str, rows: Iterable[tuple]) -> int:
        if table not in TABLES or _IDENT.fullmatch(table) is None:
            raise KeyError(table)
        columns = [column.name for column in TABLES[table].columns]
        if any(_IDENT.fullmatch(name) is None for name in columns):
            raise KeyError(table)
        column_list = ", ".join(columns)
        stage = f"stage_{table}"
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {stage}")
                cur.execute(f"CREATE TEMP TABLE {stage} (LIKE {table} INCLUDING DEFAULTS)")
                copied = 0
                with cur.copy(f"COPY {stage} ({column_list}) FROM STDIN") as copy:
                    for row in rows:
                        copy.write_row(_adapt_row(table, row))
                        copied += 1
                cur.execute(
                    f"DELETE FROM {table} WHERE market_code = %s AND snapshot_date = %s",
                    (self.market.code, self.market.snapshot_date),
                )
                cur.execute(
                    f"INSERT INTO {table} ({column_list}) SELECT {column_list} FROM {stage}"
                )
                cur.execute(f"DROP TABLE IF EXISTS {stage}")
            self.conn.commit()
            return copied
        except Exception:
            self.conn.rollback()
            raise

    def finish_market(self, market: Market) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO markets (
                      market_code, snapshot_date, name, country, region, city, loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (market_code, snapshot_date) DO UPDATE
                    SET name = EXCLUDED.name,
                        country = EXCLUDED.country,
                        region = EXCLUDED.region,
                        city = EXCLUDED.city,
                        loaded_at = NOW()
                    """,
                    (
                        market.code,
                        market.snapshot_date,
                        market.name,
                        market.country,
                        market.region,
                        market.city,
                    ),
                )
                cur.execute(POSTGRES_DELETE_METRICS, (market.code, market.snapshot_date))
                cur.execute(POSTGRES_INSERT_METRICS, (market.code, market.snapshot_date))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def record_files(self, run_id: str | None, files: list[FileLoad]) -> None:
        if not run_id:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute("DELETE FROM dataset_files WHERE run_id = %s", (run_id,))
                for item in files:
                    cur.execute(
                        """
                        INSERT INTO dataset_files (
                          run_id, dataset_name, source_url, storage_key, byte_size,
                          sha256, row_count, rejected_count, elapsed_ms, reused
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            run_id,
                            item.dataset,
                            item.url,
                            item.key,
                            item.byte_size,
                            item.sha256,
                            item.row_count,
                            item.rejected_count,
                            int(item.elapsed_seconds * 1000),
                            item.reused,
                        ),
                    )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise


def postgres_loader_factory(settings: Settings, market: Market):
    def factory() -> PostgresLoader:
        conn = psycopg.connect(settings.database_url)
        return PostgresLoader(conn, market)

    return factory


class PostgresRepository:
    def __init__(self, pool: ConnectionPool) -> None:
        self.pool = pool

    def _snapshot(self, conn: psycopg.Connection, market_code: str, snapshot: date | None) -> date:
        if snapshot is not None:
            return snapshot
        row = conn.execute(
            """
            SELECT snapshot_date FROM markets
            WHERE market_code = %s
            ORDER BY snapshot_date DESC
            LIMIT 1
            """,
            (market_code,),
        ).fetchone()
        if row is None:
            raise MarketNotLoaded(market_code)
        return row[0]

    def list_listings(
        self,
        market_code: str,
        snapshot: date | None,
        room_type: str | None,
        neighbourhood: str | None,
        max_price: object,
        limit: int,
        offset: int,
    ) -> dict:
        filters = ["market_code = %s", "snapshot_date = %s"]
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            params: list[object] = [market_code, resolved]
            if room_type:
                filters.append("room_type = %s")
                params.append(room_type)
            if neighbourhood:
                filters.append("neighbourhood_cleansed = %s")
                params.append(neighbourhood)
            if max_price is not None:
                filters.append("price <= %s")
                params.append(max_price)
            where = " AND ".join(filters)
            total = conn.execute(
                f"SELECT COUNT(*) FROM listings WHERE {where}",
                params,
            ).fetchone()[0]
            cursor = conn.execute(
                f"""
                SELECT {_LISTING_LIST}
                FROM listings
                WHERE {where}
                ORDER BY number_of_reviews DESC NULLS LAST, listing_id
                LIMIT %s OFFSET %s
                """,
                [*params, limit, offset],
            )
            items = _rows(cursor)
        return {
            "market_code": market_code,
            "snapshot_date": resolved,
            "total": total,
            "limit": limit,
            "offset": offset,
            "items": items,
        }

    def get_listing(
        self, market_code: str, snapshot: date | None, listing_id: int
    ) -> dict | None:
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            cursor = conn.execute(
                f"""
                SELECT {_LISTING_DETAIL}
                FROM listings
                WHERE market_code = %s AND snapshot_date = %s AND listing_id = %s
                """,
                (market_code, resolved, listing_id),
            )
            rows = _rows(cursor)
        if not rows:
            return None
        rows[0]["snapshot_date"] = resolved
        rows[0]["market_code"] = market_code
        return rows[0]

    def list_reviews(
        self,
        market_code: str,
        snapshot: date | None,
        listing_id: int,
        limit: int,
        offset: int,
    ) -> dict:
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            cursor = conn.execute(
                """
                SELECT review_id, review_date, reviewer_id, reviewer_name, comments
                FROM reviews
                WHERE market_code = %s AND snapshot_date = %s AND listing_id = %s
                ORDER BY review_date DESC NULLS LAST, review_id DESC
                LIMIT %s OFFSET %s
                """,
                (market_code, resolved, listing_id, limit, offset),
            )
            items = _rows(cursor)
        return {
            "market_code": market_code,
            "snapshot_date": resolved,
            "listing_id": listing_id,
            "items": items,
        }

    def listing_calendar(
        self, market_code: str, snapshot: date | None, listing_id: int
    ) -> dict:
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            cursor = conn.execute(
                """
                SELECT stay_date, available, minimum_nights, maximum_nights
                FROM calendars
                WHERE market_code = %s AND snapshot_date = %s AND listing_id = %s
                ORDER BY stay_date
                """,
                (market_code, resolved, listing_id),
            )
            days = _rows(cursor)
        available = sum(1 for day in days if day["available"] is True)
        unavailable = sum(1 for day in days if day["available"] is False)
        return {
            "market_code": market_code,
            "snapshot_date": resolved,
            "listing_id": listing_id,
            "available_nights": available,
            "unavailable_nights": unavailable,
            "days": days,
        }

    def list_neighbourhoods(
        self,
        market_code: str,
        snapshot: date | None,
        include_geometry: bool,
    ) -> dict:
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            if include_geometry:
                cursor = conn.execute(
                    """
                    SELECT neighbourhoods.neighbourhood,
                           neighbourhoods.neighbourhood_group,
                           neighbourhood_geometries.geometry
                    FROM neighbourhoods
                    LEFT JOIN neighbourhood_geometries
                      ON neighbourhood_geometries.market_code = neighbourhoods.market_code
                     AND neighbourhood_geometries.snapshot_date = neighbourhoods.snapshot_date
                     AND neighbourhood_geometries.neighbourhood = neighbourhoods.neighbourhood
                    WHERE neighbourhoods.market_code = %s
                      AND neighbourhoods.snapshot_date = %s
                    ORDER BY neighbourhoods.neighbourhood
                    """,
                    (market_code, resolved),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT neighbourhood, neighbourhood_group
                    FROM neighbourhoods
                    WHERE market_code = %s AND snapshot_date = %s
                    ORDER BY neighbourhood
                    """,
                    (market_code, resolved),
                )
            items = _rows(cursor)
        for item in items:
            if "geometry" in item:
                item["geometry"] = _json_value(item["geometry"])
        return {"market_code": market_code, "snapshot_date": resolved, "items": items}

    def neighbourhood_metrics(self, market_code: str, snapshot: date | None) -> dict:
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            cursor = conn.execute(
                """
                SELECT neighbourhood, listings, median_price, avg_price, avg_rating,
                       superhost_share, avg_availability_365, estimated_revenue, avg_occupancy
                FROM neighbourhood_metrics
                WHERE market_code = %s AND snapshot_date = %s
                ORDER BY listings DESC, neighbourhood
                """,
                (market_code, resolved),
            )
            items = _rows(cursor)
        return {"market_code": market_code, "snapshot_date": resolved, "items": items}

    def market_metrics(self, market_code: str, snapshot: date | None) -> dict | None:
        with self.pool.connection() as conn:
            resolved = self._snapshot(conn, market_code, snapshot)
            cursor = conn.execute(
                """
                SELECT listings, avg_price, avg_rating, superhost_share,
                       estimated_revenue, avg_occupancy, avg_availability_365
                FROM market_metrics
                WHERE market_code = %s AND snapshot_date = %s
                """,
                (market_code, resolved),
            )
            rows = _rows(cursor)
        if not rows:
            return None
        rows[0]["market_code"] = market_code
        rows[0]["snapshot_date"] = resolved
        return rows[0]


def ping(pool: ConnectionPool) -> bool:
    try:
        with pool.connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:
        logger.warning("database ping failed", exc_info=True)
        return False


def utcnow() -> datetime:
    from datetime import timezone

    return datetime.now(timezone.utc)
