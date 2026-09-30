"""Raw file storage. Local disk and Azure Blob use the same object key layout."""

from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator, Protocol


class ObjectStorage(Protocol):
    async def write_chunks(self, key: str, chunks: AsyncIterator[bytes]) -> None: ...

    async def materialize(self, key: str) -> Path: ...

    async def exists(self, key: str) -> bool: ...

    async def read_text(self, key: str) -> str | None: ...

    async def write_text(self, key: str, text: str) -> None: ...

    async def size(self, key: str) -> int: ...


def _safe_key(key: str) -> Path:
    parts = Path(key).parts
    if not key or key.startswith("/") or ".." in parts:
        raise ValueError(f"unsafe storage key: {key}")
    return Path(key)


class LocalStorage:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, key: str) -> Path:
        relative = _safe_key(key)
        path = (self.root / relative).resolve()
        root = self.root.resolve()
        if root != path and root not in path.parents:
            raise ValueError(f"unsafe storage key: {key}")
        return path

    async def write_chunks(self, key: str, chunks: AsyncIterator[bytes]) -> None:
        import aiofiles

        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(path.name + ".partial")
        async with aiofiles.open(partial, "wb") as handle:
            async for chunk in chunks:
                await handle.write(chunk)
        partial.replace(path)

    async def materialize(self, key: str) -> Path:
        path = self._path(key)
        if not path.is_file():
            raise FileNotFoundError(key)
        return path

    async def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    async def read_text(self, key: str) -> str | None:
        path = self._path(key)
        if not path.is_file():
            return None
        return path.read_text(encoding="utf-8")

    async def write_text(self, key: str, text: str) -> None:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    async def size(self, key: str) -> int:
        return self._path(key).stat().st_size


class AzureBlobStorage:
    """Streams the same keys to one container. Parses from a local cache file."""

    def __init__(self, connection_string: str, container: str, cache_dir: Path) -> None:
        if not connection_string.strip():
            raise ValueError(
                "AZURE_STORAGE_CONNECTION_STRING is required when STORAGE_BACKEND=azure"
            )
        if not container.strip():
            raise ValueError("AZURE_BLOB_CONTAINER is required when STORAGE_BACKEND=azure")
        self.connection_string = connection_string
        self.container = container
        self.cache_dir = cache_dir

    def _client(self):
        from azure.storage.blob.aio import BlobServiceClient

        return BlobServiceClient.from_connection_string(self.connection_string)

    async def ensure_container(self) -> None:
        async with self._client() as client:
            container = client.get_container_client(self.container)
            if not await container.exists():
                await container.create_container()

    async def write_chunks(self, key: str, chunks: AsyncIterator[bytes]) -> None:
        _safe_key(key)
        async with self._client() as client:
            blob = client.get_blob_client(self.container, key)
            await blob.upload_blob(chunks, overwrite=True)

    async def materialize(self, key: str) -> Path:
        _safe_key(key)
        destination = self.cache_dir / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        async with self._client() as client:
            blob = client.get_blob_client(self.container, key)
            stream = await blob.download_blob()
            with destination.open("wb") as handle:
                async for chunk in stream.chunks():
                    handle.write(chunk)
        return destination

    async def exists(self, key: str) -> bool:
        _safe_key(key)
        async with self._client() as client:
            blob = client.get_blob_client(self.container, key)
            return await blob.exists()

    async def read_text(self, key: str) -> str | None:
        if not await self.exists(key):
            return None
        path = await self.materialize(key)
        return path.read_text(encoding="utf-8")

    async def write_text(self, key: str, text: str) -> None:
        async def _chunks() -> AsyncIterator[bytes]:
            yield text.encode("utf-8")

        await self.write_chunks(key, _chunks())

    async def size(self, key: str) -> int:
        _safe_key(key)
        async with self._client() as client:
            blob = client.get_blob_client(self.container, key)
            properties = await blob.get_blob_properties()
            return int(properties.size)


def build_storage(settings) -> ObjectStorage:
    backend = settings.storage_backend.strip().lower()
    if backend == "local":
        return LocalStorage(settings.data_dir)
    if backend == "azure":
        return AzureBlobStorage(
            settings.azure_storage_connection_string,
            settings.azure_blob_container,
            settings.data_dir / "azure-cache",
        )
    raise ValueError(f"unsupported STORAGE_BACKEND: {settings.storage_backend}")
