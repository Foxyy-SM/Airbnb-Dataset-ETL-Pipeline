from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

from airbnb_etl.datasets import Dataset

_CODE = re.compile(r"[a-z0-9-]{1,64}")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://airbnb:airbnb@localhost:5432/airbnb"
    storage_backend: str = "local"
    data_dir: Path = Path("data")
    azure_storage_connection_string: str = ""
    azure_blob_container: str = "airbnb-raw"
    download_concurrency: int = 7
    download_retries: int = 3
    reuse_downloads: bool = True
    markets_file: Path | None = None

    @property
    def markets_path(self) -> Path:
        if self.markets_file is not None:
            return self.markets_file
        return find_markets_file()


@dataclass(frozen=True)
class Market:
    code: str
    name: str
    country: str
    region: str
    city: str
    snapshot_date: date
    base_url: str | None = None

    def url_for(self, dataset: Dataset) -> str:
        relative = dataset.relative_path
        if self.base_url:
            return f"{self.base_url.rstrip('/')}/{relative}"
        snapshot = self.snapshot_date.isoformat()
        return (
            "https://data.insideairbnb.com/"
            f"{self.country}/{self.region}/{self.city}/{snapshot}/{relative}"
        )

    def storage_key(self, dataset: Dataset) -> str:
        snapshot = self.snapshot_date.isoformat()
        return f"{self.code}/{snapshot}/{dataset.name}/{dataset.filename}"


@dataclass(frozen=True)
class Catalog:
    source: str
    retrieved: str
    default_market: str
    markets: dict[str, Market]

    def get(self, code: str) -> Market:
        try:
            return self.markets[code]
        except KeyError as exc:
            raise KeyError(code) from exc

    def list(self) -> list[Market]:
        return sorted(self.markets.values(), key=lambda market: market.code)


def find_markets_file() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "config" / "markets.json"
        if candidate.is_file():
            return candidate
    raise FileNotFoundError("config/markets.json not found")


def load_catalog(path: Path) -> Catalog:
    import json

    payload = json.loads(path.read_text(encoding="utf-8"))
    markets: dict[str, Market] = {}
    for raw in payload["markets"]:
        code = raw["code"]
        if not _CODE.fullmatch(code):
            raise ValueError(f"invalid market code: {code}")
        if code in markets:
            raise ValueError(f"duplicate market code: {code}")
        markets[code] = Market(
            code=code,
            name=raw["name"],
            country=raw["country"],
            region=raw["region"],
            city=raw["city"],
            snapshot_date=date.fromisoformat(raw["snapshot_date"]),
        )
    default_market = payload.get("default_market") or next(iter(markets))
    if default_market not in markets:
        raise ValueError(f"default market is not in the catalog: {default_market}")
    return Catalog(
        source=payload.get("source", ""),
        retrieved=payload.get("retrieved", ""),
        default_market=default_market,
        markets=markets,
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_catalog(settings: Settings | None = None) -> Catalog:
    active = settings or get_settings()
    return load_catalog(active.markets_path)


def project_root() -> Path:
    return find_markets_file().parent.parent
