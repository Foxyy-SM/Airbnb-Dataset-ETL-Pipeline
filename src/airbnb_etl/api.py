"""HTTP API for catalog, pipeline runs, and warehouse reads."""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date, datetime
from decimal import Decimal
from typing import Annotated
from uuid import uuid4

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from airbnb_etl import __version__
from airbnb_etl.config import Settings, get_settings, load_catalog
from airbnb_etl.datasets import DATASETS
from airbnb_etl.db import (
    MarketNotLoaded,
    MemoryRunStore,
    PostgresRepository,
    PostgresRunStore,
    ping,
    postgres_loader_factory,
    try_database,
    utcnow,
)
from airbnb_etl.pipeline import StreamCounter, execute
from airbnb_etl.storage import build_storage

logger = logging.getLogger(__name__)


class ListingOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    listing_id: int
    name: str | None = None
    host_id: int | None = None
    host_name: str | None = None
    host_since: date | None = None
    host_is_superhost: bool | None = None
    host_listings_count: int | None = None
    host_identity_verified: bool | None = None
    host_response_rate: Decimal | None = None
    host_acceptance_rate: Decimal | None = None
    neighbourhood: str | None = None
    neighbourhood_cleansed: str | None = None
    neighbourhood_group: str | None = None
    latitude: Decimal | None = None
    longitude: Decimal | None = None
    property_type: str | None = None
    room_type: str | None = None
    accommodates: int | None = None
    bathrooms: Decimal | None = None
    bedrooms: int | None = None
    beds: int | None = None
    amenities: str | None = None
    price: Decimal | None = None
    price_quote_total: Decimal | None = None
    price_quote_per_night: Decimal | None = None
    minimum_nights: int | None = None
    maximum_nights: int | None = None
    has_availability: bool | None = None
    availability_30: int | None = None
    availability_365: int | None = None
    number_of_reviews: int | None = None
    number_of_reviews_ltm: int | None = None
    number_of_reviews_l30d: int | None = None
    first_review: date | None = None
    last_review: date | None = None
    review_scores_rating: Decimal | None = None
    review_scores_accuracy: Decimal | None = None
    review_scores_cleanliness: Decimal | None = None
    review_scores_checkin: Decimal | None = None
    review_scores_communication: Decimal | None = None
    review_scores_location: Decimal | None = None
    review_scores_value: Decimal | None = None
    estimated_occupancy_l365d: Decimal | None = None
    estimated_revenue_l365d: Decimal | None = None
    instant_bookable: bool | None = None
    reviews_per_month: Decimal | None = None
    license: str | None = None


class ListingPage(BaseModel):
    market_code: str
    snapshot_date: date
    total: int
    limit: int
    offset: int
    items: list[ListingOut]


class ReviewOut(BaseModel):
    review_id: int
    review_date: date | None = None
    reviewer_id: int | None = None
    reviewer_name: str | None = None
    comments: str | None = None


class ReviewPage(BaseModel):
    market_code: str
    snapshot_date: date
    listing_id: int
    items: list[ReviewOut]


class CalendarDay(BaseModel):
    stay_date: date
    available: bool | None = None
    minimum_nights: int | None = None
    maximum_nights: int | None = None


class CalendarOut(BaseModel):
    market_code: str
    snapshot_date: date
    listing_id: int
    available_nights: int
    unavailable_nights: int
    days: list[CalendarDay]


class NeighbourhoodOut(BaseModel):
    neighbourhood: str
    neighbourhood_group: str | None = None
    geometry: dict | None = None


class NeighbourhoodPage(BaseModel):
    market_code: str
    snapshot_date: date
    items: list[NeighbourhoodOut]


class NeighbourhoodMetric(BaseModel):
    neighbourhood: str
    listings: int
    median_price: Decimal | None = None
    avg_price: Decimal | None = None
    avg_rating: Decimal | None = None
    superhost_share: Decimal | None = None
    avg_availability_365: Decimal | None = None
    estimated_revenue: Decimal | None = None
    avg_occupancy: Decimal | None = None


class MetricPage(BaseModel):
    market_code: str
    snapshot_date: date
    items: list[NeighbourhoodMetric]


class MarketMetric(BaseModel):
    market_code: str
    snapshot_date: date
    listings: int
    avg_price: Decimal | None = None
    avg_rating: Decimal | None = None
    superhost_share: Decimal | None = None
    estimated_revenue: Decimal | None = None
    avg_occupancy: Decimal | None = None
    avg_availability_365: Decimal | None = None


class RunRequest(BaseModel):
    market_code: str
    dry_run: bool = False


class RunOut(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: str
    market_code: str
    snapshot_date: date
    status: str
    dry_run: bool
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str | None = None
    stats: dict | None = None


class DatasetOut(BaseModel):
    name: str
    relative_path: str
    filename: str
    compressed: bool
    table: str
    kind: str


class MarketOut(BaseModel):
    code: str
    name: str
    country: str
    region: str
    city: str
    snapshot_date: date


def _known_market(request: Request, market_code: str):
    try:
        return request.app.state.catalog.get(market_code)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"unknown market: {market_code}") from exc


def _repo(request: Request):
    repo = request.app.state.repo
    if repo is None:
        raise HTTPException(status_code=503, detail="PostgreSQL is not connected")
    return repo


def create_app(
    settings: Settings | None = None,
    run_store=None,
    repo=None,
    connect_database: bool | None = None,
) -> FastAPI:
    settings = settings or get_settings()
    if connect_database is None:
        connect_database = run_store is None and repo is None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if app.state.connect_database:
            pool = try_database(app.state.settings)
            app.state.pool = pool
            if pool is not None:
                app.state.repo = PostgresRepository(pool)
                app.state.run_store = PostgresRunStore(pool)
                app.state.can_load = True
            else:
                app.state.repo = None
                app.state.run_store = MemoryRunStore()
                app.state.can_load = False
        yield
        pool = app.state.pool
        if pool is not None:
            pool.close()

    app = FastAPI(
        title="Airbnb ETL",
        version=__version__,
        summary="Load compressed Inside Airbnb snapshots and query the warehouse.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.catalog = load_catalog(settings.markets_path)
    app.state.locks = {}
    app.state.pool = None
    app.state.repo = repo
    app.state.run_store = run_store if run_store is not None else MemoryRunStore()
    app.state.connect_database = connect_database
    app.state.can_load = False

    async def executor(run_id: str) -> None:
        record = app.state.run_store.get(run_id)
        if record is None:
            return
        market = app.state.catalog.get(record["market_code"])
        lock = app.state.locks.setdefault(market.code, asyncio.Lock())
        async with lock:
            app.state.run_store.update(run_id, status="running")
            try:
                if record["dry_run"]:
                    factory = StreamCounter
                else:
                    factory = postgres_loader_factory(app.state.settings, market)
                stats = await execute(
                    market,
                    app.state.settings,
                    build_storage(app.state.settings),
                    factory,
                    None if record["dry_run"] else run_id,
                )
            except Exception as exc:
                logger.exception("run %s failed", run_id)
                app.state.run_store.update(
                    run_id,
                    status="failed",
                    error_message=str(exc)[:4000],
                    finished_at=utcnow(),
                )
                return
            app.state.run_store.update(
                run_id,
                status="succeeded",
                stats=stats.as_dict(),
                finished_at=utcnow(),
                error_message=None,
            )

    app.state.executor = executor

    @app.exception_handler(MarketNotLoaded)
    async def market_not_loaded(_request: Request, exc: MarketNotLoaded) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.get("/", include_in_schema=False)
    def root() -> dict:
        return {"service": "airbnb-etl", "docs": "/docs", "health": "/health"}

    @app.get("/health")
    def health() -> dict:
        pool = app.state.pool
        if pool is not None:
            database = "ok" if ping(pool) else "down"
        elif app.state.repo is not None:
            database = "ok"
        else:
            database = "unavailable"
        return {
            "status": "ok",
            "version": __version__,
            "storage_backend": app.state.settings.storage_backend,
            "database": database,
            "datasets": len(DATASETS),
        }

    @app.get("/datasets", response_model=list[DatasetOut])
    def datasets() -> list[DatasetOut]:
        return [
            DatasetOut(
                name=item.name,
                relative_path=item.relative_path,
                filename=item.filename,
                compressed=item.compressed,
                table=item.table,
                kind=item.kind,
            )
            for item in DATASETS
        ]

    @app.get("/markets", response_model=list[MarketOut])
    def markets() -> list[MarketOut]:
        return [
            MarketOut(
                code=item.code,
                name=item.name,
                country=item.country,
                region=item.region,
                city=item.city,
                snapshot_date=item.snapshot_date,
            )
            for item in app.state.catalog.list()
        ]

    @app.post("/pipeline/runs", response_model=RunOut, status_code=202)
    def start_run(body: RunRequest, background: BackgroundTasks, request: Request) -> dict:
        market = _known_market(request, body.market_code)
        if not body.dry_run and not request.app.state.can_load:
            raise HTTPException(
                status_code=503,
                detail="PostgreSQL is required for a load. Retry with dry_run=true.",
            )
        record = {
            "run_id": str(uuid4()),
            "market_code": market.code,
            "snapshot_date": market.snapshot_date,
            "status": "queued",
            "dry_run": body.dry_run,
            "started_at": utcnow(),
            "finished_at": None,
            "error_message": None,
            "stats": None,
        }
        request.app.state.run_store.create(record)
        background.add_task(request.app.state.executor, record["run_id"])
        return record

    @app.get("/pipeline/runs", response_model=list[RunOut])
    def list_runs(limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
        return app.state.run_store.list(limit)

    @app.get("/pipeline/runs/{run_id}", response_model=RunOut)
    def get_run(run_id: str) -> dict:
        record = app.state.run_store.get(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail="run not found")
        return record

    @app.get("/listings", response_model=ListingPage)
    def list_listings(
        request: Request,
        market: str,
        snapshot: date | None = None,
        room_type: str | None = None,
        neighbourhood: str | None = None,
        max_price: Decimal | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict:
        _known_market(request, market)
        return _repo(request).list_listings(
            market, snapshot, room_type, neighbourhood, max_price, limit, offset
        )

    @app.get("/listings/{listing_id}", response_model=ListingOut)
    def get_listing(
        listing_id: int,
        request: Request,
        market: str,
        snapshot: date | None = None,
    ) -> dict:
        _known_market(request, market)
        found = _repo(request).get_listing(market, snapshot, listing_id)
        if found is None:
            raise HTTPException(status_code=404, detail="listing not found")
        return found

    @app.get("/listings/{listing_id}/reviews", response_model=ReviewPage)
    def listing_reviews(
        listing_id: int,
        request: Request,
        market: str,
        snapshot: date | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0)] = 0,
    ) -> dict:
        _known_market(request, market)
        repo = _repo(request)
        if repo.get_listing(market, snapshot, listing_id) is None:
            raise HTTPException(status_code=404, detail="listing not found")
        return repo.list_reviews(market, snapshot, listing_id, limit, offset)

    @app.get("/listings/{listing_id}/calendar", response_model=CalendarOut)
    def listing_calendar(
        listing_id: int,
        request: Request,
        market: str,
        snapshot: date | None = None,
    ) -> dict:
        _known_market(request, market)
        repo = _repo(request)
        if repo.get_listing(market, snapshot, listing_id) is None:
            raise HTTPException(status_code=404, detail="listing not found")
        return repo.listing_calendar(market, snapshot, listing_id)

    @app.get("/neighbourhoods", response_model=NeighbourhoodPage)
    def neighbourhoods(
        request: Request,
        market: str,
        snapshot: date | None = None,
        include_geometry: bool = False,
    ) -> dict:
        _known_market(request, market)
        return _repo(request).list_neighbourhoods(market, snapshot, include_geometry)

    @app.get("/analytics/neighbourhoods", response_model=MetricPage)
    def neighbourhood_analytics(
        request: Request,
        market: str,
        snapshot: date | None = None,
    ) -> dict:
        _known_market(request, market)
        return _repo(request).neighbourhood_metrics(market, snapshot)

    @app.get("/analytics/markets", response_model=MarketMetric)
    def market_analytics(
        request: Request,
        market: str,
        snapshot: date | None = None,
    ) -> dict:
        _known_market(request, market)
        found = _repo(request).market_metrics(market, snapshot)
        if found is None:
            raise HTTPException(status_code=404, detail="market metrics are not available")
        return found

    return app


app = create_app()
