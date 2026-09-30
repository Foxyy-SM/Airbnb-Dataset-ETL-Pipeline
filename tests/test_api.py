from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi.testclient import TestClient

from airbnb_etl.api import create_app
from airbnb_etl.config import Settings
from airbnb_etl.db import MemoryRunStore, utcnow


class StubRepo:
    def list_listings(
        self, market_code, snapshot, room_type, neighbourhood, max_price, limit, offset
    ) -> dict:
        assert market_code == "albany"
        return {
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "total": 1,
            "limit": limit,
            "offset": offset,
            "items": [
                {
                    "listing_id": 10,
                    "name": "Loft",
                    "room_type": "Entire home/apt",
                    "price": Decimal("80.43"),
                    "number_of_reviews": 4,
                }
            ],
        }

    def get_listing(self, market_code, snapshot, listing_id):
        if listing_id != 10:
            return None
        return {
            "listing_id": 10,
            "name": "Loft",
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "amenities": '["Wifi"]',
        }

    def list_reviews(self, market_code, snapshot, listing_id, limit, offset) -> dict:
        return {
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "listing_id": listing_id,
            "items": [
                {
                    "review_id": 99,
                    "review_date": date(2026, 1, 2),
                    "reviewer_id": 7,
                    "reviewer_name": "Ada",
                    "comments": "Nice, place",
                }
            ],
        }

    def listing_calendar(self, market_code, snapshot, listing_id) -> dict:
        return {
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "listing_id": listing_id,
            "available_nights": 1,
            "unavailable_nights": 1,
            "days": [
                {
                    "stay_date": date(2026, 6, 16),
                    "available": True,
                    "minimum_nights": 2,
                    "maximum_nights": 30,
                }
            ],
        }

    def list_neighbourhoods(self, market_code, snapshot, include_geometry) -> dict:
        return {
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "items": [{"neighbourhood": "CENTER", "neighbourhood_group": None}],
        }

    def neighbourhood_metrics(self, market_code, snapshot) -> dict:
        return {
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "items": [
                {
                    "neighbourhood": "CENTER",
                    "listings": 1,
                    "median_price": Decimal("80.43"),
                }
            ],
        }

    def market_metrics(self, market_code, snapshot) -> dict:
        return {
            "market_code": market_code,
            "snapshot_date": date(2026, 6, 16),
            "listings": 1,
            "avg_price": Decimal("80.43"),
        }


def _client(tmp_path):
    store = MemoryRunStore()
    application = create_app(
        settings=Settings(data_dir=tmp_path, storage_backend="local"),
        run_store=store,
        repo=StubRepo(),
    )

    async def fake(run_id: str) -> None:
        application.state.run_store.update(
            run_id,
            status="succeeded",
            stats={"files": []},
            finished_at=utcnow(),
        )

    application.state.executor = fake
    return TestClient(application), store


def test_catalog_health_and_pipeline_contract(tmp_path) -> None:
    client, _store = _client(tmp_path)
    health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["datasets"] == 7
    assert health.json()["database"] == "ok"

    datasets = client.get("/datasets").json()
    assert len(datasets) == 7
    assert sum(1 for item in datasets if item["compressed"]) == 3

    markets = client.get("/markets").json()
    assert any(item["code"] == "albany" for item in markets)

    created = client.post("/pipeline/runs", json={"market_code": "albany", "dry_run": True})
    assert created.status_code == 202
    body = created.json()
    assert body["status"] == "queued"
    fetched = client.get(f"/pipeline/runs/{body['run_id']}")
    assert fetched.status_code == 200
    assert fetched.json()["status"] == "succeeded"

    refused = client.post("/pipeline/runs", json={"market_code": "albany", "dry_run": False})
    assert refused.status_code == 503
    assert client.post("/pipeline/runs", json={"market_code": "nope"}).status_code == 404
    assert client.get("/pipeline/runs/missing").status_code == 404


def test_query_endpoints(tmp_path) -> None:
    client, _store = _client(tmp_path)
    listings = client.get("/listings", params={"market": "albany", "max_price": "100"})
    assert listings.status_code == 200
    assert listings.json()["items"][0]["name"] == "Loft"
    assert client.get("/listings/10", params={"market": "albany"}).json()["amenities"] == '["Wifi"]'
    assert client.get("/listings/11", params={"market": "albany"}).status_code == 404
    reviews = client.get("/listings/10/reviews", params={"market": "albany"})
    assert reviews.json()["items"][0]["comments"] == "Nice, place"
    calendar = client.get("/listings/10/calendar", params={"market": "albany"})
    assert calendar.json()["available_nights"] == 1
    assert client.get("/neighbourhoods", params={"market": "albany"}).json()["items"][0][
        "neighbourhood"
    ] == "CENTER"
    metrics = client.get("/analytics/neighbourhoods", params={"market": "albany"})
    assert metrics.json()["items"][0]["median_price"] == "80.43"
    market = client.get("/analytics/markets", params={"market": "albany"})
    assert market.json()["listings"] == 1
    assert client.get("/listings", params={"market": "missing"}).status_code == 404


def test_queries_without_a_database_are_unavailable(tmp_path) -> None:
    application = create_app(
        settings=Settings(data_dir=tmp_path),
        run_store=MemoryRunStore(),
        repo=None,
    )
    client = TestClient(application)
    assert client.get("/health").json()["database"] == "unavailable"
    response = client.get("/listings", params={"market": "albany"})
    assert response.status_code == 503
