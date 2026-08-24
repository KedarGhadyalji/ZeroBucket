"""Storage backend interface.

CRITICAL DESIGN RULE: implementations of this interface know nothing about
images. They store and retrieve rows of bytes + metadata columns. All
image-specific logic (validation, format detection, resizing) lives in
zerobucket.client, above this layer.

This separation is what makes a future object-storage backend a real
drop-in replacement rather than a rewrite.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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
    ) -> str:
        """Persist a record and return its generated id."""

    @abstractmethod
    def get(self, image_id: str) -> StoredRecord | None:
        """Fetch a full record including bytes, or None if it doesn't exist."""

    @abstractmethod
    def get_metadata(self, image_id: str) -> StoredRecordMetadata | None:
        """Fetch metadata only (no bytes), or None if it doesn't exist."""

    @abstractmethod
    def delete(self, image_id: str) -> bool:
        """Delete a record. Returns True if a record was deleted, False if it didn't exist."""

    @abstractmethod
    def exists(self, image_id: str) -> bool:
        """Return whether a record with this id exists."""

    @abstractmethod
    def close(self) -> None:
        """Release underlying connections/resources."""
