"""Public data types returned by the ZeroBucket SDK.

These are the only shapes callers should depend on. Internal storage
representation (BYTEA, column layout, etc.) is never exposed here.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Image:
    """A retrieved image, ready to be served or written to disk.

    `data` is raw bytes -- never Base64. Serving it from a web framework
    is just: Response(image.data, mimetype=image.mime_type)
    """

    data: bytes
    mime_type: str
    filename: str | None
    size_bytes: int
    width: int | None
    height: int | None
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class ImageMetadata:
    """Metadata about a stored image, without the pixel data itself.

    Useful for existence/info checks that shouldn't pull potentially
    multi-megabyte blobs over the wire.
    """

    image_id: str
    mime_type: str
    filename: str | None
    size_bytes: int
    width: int | None
    height: int | None
    checksum_sha256: str
