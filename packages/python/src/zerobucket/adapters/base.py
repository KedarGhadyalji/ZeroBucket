"""Storage backend interface.

CRITICAL DESIGN RULE: implementations of this interface know nothing about
images. They store and retrieve rows of bytes + metadata columns. All
image-specific logic (validation, format detection, resizing) lives in
zerobucket.client, above this layer.

This separation is what makes a future object-storage backend a real
drop-in replacement rather than a rewrite.

Every method accepts an optional `connection`. When None (the default),
the backend uses its own internal pool -- each call commits independently
on its own connection, exactly as before. When a connection is provided,
the backend uses it directly and does NOT commit or roll it back -- that
becomes the caller's responsibility, which is what lets an operation
participate in the caller's own transaction (see client.py's put() docs
for why this matters and a worked example). `connection` is intentionally
typed as `object` here rather than a Postgres-specific type, since this
interface is meant to stay backend-agnostic; concrete adapters narrow the
type in their own implementation.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StoredRecord:
    """Raw record shape as persisted by a storage backend."""

    id: str
    data: bytes
    mime_type: str
    original_filename: str | None
    size_bytes: int
    width: int | None
    height: int | None
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class StoredRecordMetadata:
    """Same as StoredRecord but without `data`, for cheap existence/info checks."""

    id: str
    mime_type: str
    original_filename: str | None
    size_bytes: int
    width: int | None
    height: int | None
    checksum_sha256: str


class StorageBackend(ABC):
    """Abstract interface every ZeroBucket storage adapter must implement."""

    @abstractmethod
    def put(
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
    def put_many(
        self, rows: list[dict], *, connection: object | None = None
    ) -> list[str]:
        """Insert multiple already-prepared rows; returns ids in the same order as `rows`."""

    @abstractmethod
    def get(
        self, image_id: str, *, connection: object | None = None
    ) -> StoredRecord | None:
        """Fetch a full record including bytes, or None if it doesn't exist."""

    @abstractmethod
    def get_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[StoredRecord]:
        """Fetch multiple records; missing ids are simply absent, not an error."""

    @abstractmethod
    def get_metadata(
        self, image_id: str, *, connection: object | None = None
    ) -> StoredRecordMetadata | None:
        """Fetch metadata only (no bytes), or None if it doesn't exist."""

    @abstractmethod
    def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int,
        connection: object | None = None,
    ) -> Iterator[bytes] | None:
        """Fetch a record's bytes as an iterator of chunks, or None if the
        id doesn't exist.

        Unlike get(), this never materializes the full value in Python
        memory at once -- only one chunk is held at a time, which is the
        point (piping a large stored value straight to a socket/file
        without ever holding the whole thing). It does NOT reduce memory
        pressure inside Postgres itself -- the server still handles the
        full stored value per chunk request the same way it always does
        for a BYTEA column (see docs/OPERATIONS.md). This is a Python-side
        memory optimization, not a storage-engine one.

        Without a `connection=` spanning the whole read, each chunk is
        fetched in its own round trip and there is no snapshot isolation
        across chunks -- a concurrent delete() between chunks will raise
        rather than silently return a truncated stream (implementations
        must not swallow a "row no longer exists" mid-stream into an
        early, silent stop). Pass your own open `connection=` if you need
        a consistent read across the whole stream despite concurrent
        writers.
        """

    @abstractmethod
    def delete(self, image_id: str, *, connection: object | None = None) -> bool:
        """Delete a record. Returns True if a record was deleted, False if it didn't exist."""

    @abstractmethod
    def delete_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[str]:
        """Delete multiple records; returns the ids that were actually deleted."""

    @abstractmethod
    def exists(self, image_id: str, *, connection: object | None = None) -> bool:
        """Return whether a record with this id exists."""

    @abstractmethod
    def close(self) -> None:
        """Release underlying connections/resources."""
