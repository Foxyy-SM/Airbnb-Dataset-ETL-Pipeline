"""Command line entry points for a dry run, a database load, and the API."""

from __future__ import annotations

import argparse
import asyncio
import logging
from uuid import uuid4

from airbnb_etl.config import get_settings, load_catalog
from airbnb_etl.db import PostgresRunStore, connect_pool, postgres_loader_factory, utcnow
from airbnb_etl.pipeline import StreamCounter, execute
from airbnb_etl.storage import build_storage


def _print_stats(stats) -> None:
    for item in stats.files:
        print(
            f"{item.dataset:24} rows={item.row_count:<8} "
            f"rejected={item.rejected_count:<6} bytes={item.byte_size}"
        )
    print(f"elapsed_seconds={stats.elapsed_seconds}")


def _run(market_code: str | None, dry_run: bool) -> int:
    settings = get_settings()
    catalog = load_catalog(settings.markets_path)
    market = catalog.get(market_code or catalog.default_market)
    storage = build_storage(settings)
    pool = None
    store = None
    run_id = None
    if dry_run:
        factory = StreamCounter
    else:
        pool = connect_pool(settings)
        store = PostgresRunStore(pool)
        run_id = str(uuid4())
        store.create(
            {
                "run_id": run_id,
                "market_code": market.code,
                "snapshot_date": market.snapshot_date,
                "status": "running",
                "dry_run": False,
                "started_at": utcnow(),
                "finished_at": None,
                "error_message": None,
                "stats": None,
            }
        )
        factory = postgres_loader_factory(settings, market)
    try:
        stats = asyncio.run(
            execute(market, settings, storage, factory, None if dry_run else run_id)
        )
    except Exception as exc:
        if store is not None and run_id is not None:
            store.update(
                run_id,
                status="failed",
                error_message=str(exc)[:4000],
                finished_at=utcnow(),
            )
        if pool is not None:
            pool.close()
        raise
    if store is not None and run_id is not None:
        store.update(
            run_id,
            status="succeeded",
            stats=stats.as_dict(),
            finished_at=utcnow(),
            error_message=None,
        )
    if pool is not None:
        pool.close()
    _print_stats(stats)
    return 0


def _init_db() -> int:
    settings = get_settings()
    pool = connect_pool(settings)
    pool.close()
    print("schema ready")
    return 0


def _serve(host: str, port: int) -> int:
    import uvicorn

    uvicorn.run("airbnb_etl.api:app", host=host, port=port)
    return 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(prog="airbnb-etl")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="download a market and load it, or count rows")
    run.add_argument("--market", default=None, help="catalog code, default albany")
    run.add_argument(
        "--dry-run",
        action="store_true",
        help="download and parse, but do not write PostgreSQL",
    )

    serve = sub.add_parser("serve", help="start the FastAPI service")
    serve.add_argument("--host", default="0.0.0.0")
    serve.add_argument("--port", type=int, default=8000)

    sub.add_parser("init-db", help="create the PostgreSQL schema")

    args = parser.parse_args(argv)
    if args.command == "run":
        return _run(args.market, args.dry_run)
    if args.command == "serve":
        return _serve(args.host, args.port)
    if args.command == "init-db":
        return _init_db()
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
