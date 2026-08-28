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


@dataclass(frozen=True, slots=True)
class BatchPutResult:
    """Outcome of one image in a put_many() call.

    Batch operations are best-effort, not all-or-nothing: one bad image
    in a batch of 1000 doesn't abort the other 999. Check `.success` (or
    `.error`) per item rather than assuming the whole batch succeeded.
    """

    index: int  # position in the input list, for correlating back to it
    image_id: str | None
    error: str | None

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass(frozen=True, slots=True)
class BatchGetResult:
    """Outcome of one image_id in a get_many() call.

    A missing image_id is NOT an error here (unlike get(), which raises
    ImageNotFoundError) -- it's an expected, normal outcome of asking for
    a batch of ids where some may not exist. Check `.success`.
    """

    image_id: str
    image: Image | None
    error: str | None

    @property
    def success(self) -> bool:
        return self.error is None and self.image is not None


@dataclass(frozen=True, slots=True)
class BatchDeleteResult:
    """Outcome of one image_id in a delete_many() call.

    `deleted=False` with no error means the id simply didn't exist --
    same as delete()'s return value for a single id, not a failure.
    """

    image_id: str
    deleted: bool
    error: str | None

    @property
    def success(self) -> bool:
        return self.error is None
