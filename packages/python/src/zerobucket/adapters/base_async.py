"""Async storage backend interface -- the async counterpart to base.py.

Same design rule as base.py: implementations know nothing about images,
only rows of bytes + metadata columns.

Every method below corresponds 1:1 to something the sync StorageBackend
does. Every method accepts an optional `connection` (0.33.0), exactly as the
sync interface does: None (the default) means "use your own pool/connection,
commit on your own, retry transient failures"; an open async connection of
the driver this adapter uses (a psycopg AsyncConnection, an aiosqlite
Connection or an aiomysql Connection) means "run on THAT connection, never
commit, never retry" -- the caller owns the transaction. AsyncZeroBucket only
passes `connection=` when the caller gave one, so a custom backend that does
not support it keeps working until someone asks it to.
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from .base import StoredRecord, StoredRecordMetadata


class AsyncStorageBackend(ABC):
    """Abstract interface an async ZeroBucket storage adapter implements."""

    async def _delete_tiered_object(self, key: str | None) -> None:
        """Async counterpart of StorageBackend._delete_tiered_object() (0.29.0):
        remove a tiered image's object AFTER its row is already gone, swallow
        any failure, and report it as an "object_storage_delete" on_operation
        event. The blocking boto3 call runs in a worker thread so it never
        stalls the event loop. Adapters using this must have
        `_object_storage` and `_emit`."""
        store = getattr(self, "_object_storage", None)
        if store is None or key is None:
            return
        start = time.monotonic()
        try:
            await asyncio.to_thread(store.delete, key)
        except Exception as exc:  # noqa: BLE001
            self._emit("object_storage_delete", start, False, str(exc), 0)  # type: ignore[attr-defined]
        else:
            self._emit("object_storage_delete", start, True, None, 0)  # type: ignore[attr-defined]

    @abstractmethod
    async def put(
        self,
        *,
        data: bytes,
        mime_type: str,
        original_filename: str | None,
        size_bytes: int,
        width: int | None,
        height: int | None,
        checksum_sha256: str,
        connection: object | None = None,
    ) -> str:
        """Persist a record and return its generated id."""

    @abstractmethod
    async def put_many(
        self, rows: list[dict], *, connection: object | None = None
    ) -> list[str]:
        """Insert multiple already-prepared rows; returns ids in the same order as `rows`."""

    @abstractmethod
    async def get(
        self, image_id: str, *, connection: object | None = None
    ) -> StoredRecord | None:
        """Fetch a full record including bytes, or None if it doesn't exist."""

    @abstractmethod
    async def get_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[StoredRecord]:
        """Fetch multiple records; missing ids are simply absent, not an error."""

    @abstractmethod
    async def get_metadata(
        self, image_id: str, *, connection: object | None = None
    ) -> StoredRecordMetadata | None:
        """Fetch metadata only (no bytes), or None if it doesn't exist."""

    @abstractmethod
    async def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int,
        connection: object | None = None,
    ) -> AsyncIterator[bytes] | None:
        """Fetch a record's bytes as an async iterator of chunks, or None
        if the id doesn't exist. Unlike get_many/get/etc above, this is a
        coroutine that RETURNS an async iterator rather than being an
        async generator itself -- so the not-found check happens eagerly
        when this is awaited, not deferred to the first iteration. See
        AsyncZeroBucket.get_stream's docstring for the full contract
        (Python-side-only memory reduction, mid-stream-delete behavior --
        same honest limitations as the sync version's get_stream)."""

    @abstractmethod
    async def delete(self, image_id: str, *, connection: object | None = None) -> bool:
        """Delete a record. Returns True if a record was deleted, False if it didn't exist."""

    @abstractmethod
    async def delete_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[str]:
        """Delete multiple records; returns the ids that were actually deleted."""

    @abstractmethod
    async def exists(self, image_id: str, *, connection: object | None = None) -> bool:
        """Return whether a record with this id exists."""

    @abstractmethod
    async def close(self) -> None:
        """Release underlying connections/resources."""
