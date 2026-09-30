from __future__ import annotations

import asyncio
import socket
from datetime import date
from pathlib import Path

from aiohttp import web

from airbnb_etl.config import Market, Settings
from airbnb_etl.datasets import DATASETS
from airbnb_etl.download import download_market
from airbnb_etl.storage import AzureBlobStorage, LocalStorage


def _market(port: int) -> Market:
    return Market(
        code="albany",
        name="Albany",
        country="united-states",
        region="ny",
        city="albany",
        snapshot_date=date(2026, 6, 16),
        base_url=f"http://127.0.0.1:{port}",
    )


async def _start(app: web.Application) -> tuple[web.AppRunner, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    runner = web.AppRunner(app)
    await runner.setup()
    await web.SockSite(runner, sock).start()
    return runner, port


def test_downloads_overlap_and_can_be_reused(tmp_path: Path) -> None:
    async def scenario() -> None:
        state = {"current": 0, "peak": 0, "hits": 0}
        lock = asyncio.Lock()

        async def handle(_request: web.Request) -> web.Response:
            async with lock:
                state["hits"] += 1
                state["current"] += 1
                state["peak"] = max(state["peak"], state["current"])
            await asyncio.sleep(0.3)
            async with lock:
                state["current"] -= 1
            if _request.path.endswith(".geojson"):
                return web.json_response({"type": "FeatureCollection", "features": []})
            return web.Response(text="id\n")

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", handle)
        runner, port = await _start(app)
        storage = LocalStorage(tmp_path)
        market = _market(port)
        try:
            first = Settings(
                data_dir=tmp_path,
                download_concurrency=7,
                reuse_downloads=False,
                download_retries=1,
            )
            await download_market(market, storage, first)
            assert state["peak"] >= 4
            assert state["hits"] == len(DATASETS)
            second = Settings(
                data_dir=tmp_path,
                download_concurrency=7,
                reuse_downloads=True,
                download_retries=1,
            )
            await download_market(market, storage, second)
            assert state["hits"] == len(DATASETS)
        finally:
            await runner.cleanup()

    asyncio.run(scenario())


def test_local_storage_round_trip_and_rejects_parent_segments(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = LocalStorage(tmp_path)

        async def chunks():
            yield b"abc"
            yield b"def"

        await storage.write_chunks("albany/2026-06-16/listings_detail/listings.csv.gz", chunks())
        path = await storage.materialize("albany/2026-06-16/listings_detail/listings.csv.gz")
        assert path.read_bytes() == b"abcdef"
        await storage.write_text("albany/note.txt", "ok\n")
        assert await storage.read_text("albany/note.txt") == "ok\n"

    asyncio.run(scenario())
    storage = LocalStorage(tmp_path)
    try:
        asyncio.run(storage.exists("../secrets"))
    except ValueError:
        return
    raise AssertionError("parent segments should be rejected")


def test_azure_backend_requires_a_connection_string(tmp_path: Path) -> None:
    try:
        AzureBlobStorage("", "airbnb-raw", tmp_path)
    except ValueError as exc:
        assert "AZURE_STORAGE_CONNECTION_STRING" in str(exc)
        return
    raise AssertionError("missing connection string should fail")
