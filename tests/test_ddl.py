from __future__ import annotations

from airbnb_etl.config import project_root
from airbnb_etl.ddl import (
    DIALECTS,
    POSTGRES_INSERT_METRICS,
    TABLES,
    render_mart,
    render_schema,
    split_sql,
)


def test_checked_in_sql_matches_the_renderer() -> None:
    root = project_root() / "sql"
    for dialect in DIALECTS:
        assert (root / dialect / "schema.sql").read_text(encoding="utf-8") == render_schema(dialect)
        assert (root / dialect / "marts.sql").read_text(encoding="utf-8") == render_mart(dialect)


def test_postgres_script_is_executable_statement_by_statement() -> None:
    script = render_schema("postgresql")
    statements = split_sql(script)
    for table in TABLES:
        assert f"CREATE TABLE IF NOT EXISTS {table} (" in script
    assert all(statement.count("(") == statement.count(")") for statement in statements)
    assert any("market_metrics" in statement for statement in statements)


def test_every_dialect_builds_the_neighbourhood_mart() -> None:
    for dialect in DIALECTS:
        script = render_mart(dialect)
        assert "neighbourhood_metrics" in script
        assert "median_price" in script
    assert "PERCENTILE_CONT" in POSTGRES_INSERT_METRICS
    assert "PERCENTILE_CONT" in render_mart("azure_sql")
    assert "ROW_NUMBER()" in render_mart("mysql")


def test_calendar_stays_narrow() -> None:
    names = [column.name for column in TABLES["calendars"].columns]
    assert names == [
        "market_code",
        "snapshot_date",
        "listing_id",
        "stay_date",
        "available",
        "minimum_nights",
        "maximum_nights",
    ]
    assert "comments" in [column.name for column in TABLES["reviews"].columns]
    assert "comments" not in [column.name for column in TABLES["review_dates"].columns]
