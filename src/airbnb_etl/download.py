"""Concurrent downloads. The seven files in a snapshot share one aiohttp session."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass

import aiohttp

from airbnb_etl.config import Market, Settings
from airbnb_etl.datasets import DATASETS, Dataset
from airbnb_etl.storage import ObjectStorage

logger = logging.getLogger(__name__)

USER_AGENT = "airbnb-etl/1.0 (+https://insideairbnb.com/get-the-data)"
CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True)
class DownloadResult:
    dataset: str
    url: str
    key: str
    byte_size: int
    sha256: str
    elapsed_seconds: float
    reused: bool


class DownloadError(RuntimeError):
    pass


def _sha_key(key: str) -> str:
    return f"{key}.sha256"


async def _download_once(
    session: aiohttp.ClientSession,
    storage: ObjectStorage,
    url: str,
    key: str,
) -> tuple[int, str]:
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=180)
    async with session.get(url, timeout=timeout) as response:
        if response.status == 404:
            raise DownloadError(f"dataset not found: {url}")
        if response.status >= 400 and response.status != 429 and response.status < 500:
            detail = (await response.text())[:200]
            raise DownloadError(f"{response.status} from {url}: {detail}")
        response.raise_for_status()
        hasher = hashlib.sha256()
        total = 0

        async def tracked():
            nonlocal total
            async for chunk in response.content.iter_chunked(CHUNK_BYTES):
                hasher.update(chunk)
                total += len(chunk)
                yield chunk

        await storage.write_chunks(key, tracked())
        return total, hasher.hexdigest()


async def download_dataset(
    session: aiohttp.ClientSession,
    storage: ObjectStorage,
    market: Market,
    dataset: Dataset,
    settings: Settings,
    semaphore: asyncio.Semaphore,
) -> DownloadResult:
    import time

    url = market.url_for(dataset)
    key = market.storage_key(dataset)
    started = time.perf_counter()
    async with semaphore:
        if settings.reuse_downloads and await storage.exists(key):
            existing = await storage.read_text(_sha_key(key))
            if existing:
                elapsed = time.perf_counter() - started
                logger.info("reusing %s (%s)", dataset.name, key)
                return DownloadResult(
                    dataset=dataset.name,
                    url=url,
                    key=key,
                    byte_size=await storage.size(key),
                    sha256=existing.strip(),
                    elapsed_seconds=elapsed,
                    reused=True,
                )
        last_error: Exception | None = None
        for attempt in range(1, settings.download_retries + 1):
            try:
                byte_size, digest = await _download_once(session, storage, url, key)
                await storage.write_text(_sha_key(key), digest + "\n")
                elapsed = time.perf_counter() - started
                logger.info(
                    "downloaded %s bytes=%s sha256=%s",
                    dataset.name,
                    byte_size,
                    digest[:12],
                )
                return DownloadResult(
                    dataset=dataset.name,
                    url=url,
                    key=key,
                    byte_size=byte_size,
                    sha256=digest,
                    elapsed_seconds=elapsed,
                    reused=False,
                )
            except DownloadError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
                logger.warning(
                    "download %s attempt %s failed: %s", dataset.name, attempt, exc
                )
                if attempt < settings.download_retries:
                    await asyncio.sleep(2 ** (attempt - 1))
        raise DownloadError(f"failed to download {url}") from last_error


async def download_market(
    market: Market,
    storage: ObjectStorage,
    settings: Settings,
    session: aiohttp.ClientSession | None = None,
) -> list[DownloadResult]:
    owns_session = session is None
    if session is None:
        session = aiohttp.ClientSession(headers={"User-Agent": USER_AGENT})
    semaphore = asyncio.Semaphore(max(1, settings.download_concurrency))
    try:
        results = await asyncio.gather(
            *(
                download_dataset(session, storage, market, dataset, settings, semaphore)
                for dataset in DATASETS
            )
        )
    finally:
        if owns_session:
            await session.close()
    by_name = {result.dataset: result for result in results}
    return [by_name[dataset.name] for dataset in DATASETS]
