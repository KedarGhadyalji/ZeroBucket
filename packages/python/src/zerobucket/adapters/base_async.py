"""Async storage backend interface -- the async counterpart to base.py.

Same design rule as base.py: implementations know nothing about images,
only rows of bytes + metadata columns.

Scoped narrower than the sync StorageBackend for this first pass,
deliberately, not by oversight -- see AsyncZeroBucket's docstring in
async_client.py for the full list of what's NOT here yet (dedup mode,
connection= transaction participation, automatic retry, on_operation
metrics) and why each was left for a follow-up rather than attempted
here. Every method below corresponds 1:1 to something StorageBackend
already does in classic (non-dedup) mode with no caller-supplied
connection.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from .base import StoredRecord, StoredRecordMetadata


class AsyncStorageBackend(ABC):
    """Abstract interface an async ZeroBucket storage adapter implements."""

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
    ) -> str:
        """Persist a record and return its generated id."""

    @abstractmethod
    async def put_many(self, rows: list[dict]) -> list[str]:
        """Insert multiple already-prepared rows; returns ids in the same order as `rows`."""

    @abstractmethod
    async def get(self, image_id: str) -> StoredRecord | None:
        """Fetch a full record including bytes, or None if it doesn't exist."""

    @abstractmethod
    async def get_many(self, image_ids: list[str]) -> list[StoredRecord]:
        """Fetch multiple records; missing ids are simply absent, not an error."""

    @abstractmethod
    async def get_metadata(self, image_id: str) -> StoredRecordMetadata | None:
        """Fetch metadata only (no bytes), or None if it doesn't exist."""

    @abstractmethod
    async def get_stream(
        self, image_id: str, *, chunk_size: int
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
    async def delete(self, image_id: str) -> bool:
        """Delete a record. Returns True if a record was deleted, False if it didn't exist."""

    @abstractmethod
    async def delete_many(self, image_ids: list[str]) -> list[str]:
        """Delete multiple records; returns the ids that were actually deleted."""

    @abstractmethod
    async def exists(self, image_id: str) -> bool:
        """Return whether a record with this id exists."""

    @abstractmethod
    async def close(self) -> None:
        """Release underlying connections/resources."""
