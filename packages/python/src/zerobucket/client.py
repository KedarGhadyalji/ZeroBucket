"""The public ZeroBucket SDK entry point.

    from zerobucket import ZeroBucket
    images = ZeroBucket(database_url="postgresql://...")
    image_id = images.put("avatar.jpg")
    image = images.get(image_id)

The developer never needs to think about BYTEA, checksums, or connection
pooling -- that's all handled below.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import BinaryIO, Union

from .adapters.base import StorageBackend
from .adapters.postgres import PostgresBackend
from .exceptions import ImageNotFoundError
from .optimization import optimize_image
from .types import Image, ImageMetadata
from .validation import DEFAULT_MAX_PIXELS, SUPPORTED_FORMATS, validate_image

# What put() accepts. Framework upload objects (e.g. Flask's FileStorage,
# FastAPI's UploadFile) are duck-typed against BinaryIO via .read().
ImageInput = Union[str, "os.PathLike[str]", bytes, BinaryIO]

# 8 MiB. Chosen as a practical ceiling for "small app" images (see README
# for rationale); pass max_bytes= to override per-instance.
DEFAULT_MAX_BYTES = 8 * 1024 * 1024


class ZeroBucket:
    """Database-native image storage.

    Args:
        database_url: PostgreSQL connection string.
        max_bytes: Maximum accepted image size in bytes. Defaults to 8 MiB.
            ZeroBucket stores full images in a database column; it is not
            designed for arbitrarily large files. See the README for why.
        max_pixels: Decoded-pixel ceiling used to reject decompression
            bombs, independent of compressed file size.
        allowed_formats: Which image formats to accept. Defaults to
            JPEG/PNG/WebP.
        backend: Advanced -- inject a custom StorageBackend instead of
            constructing a PostgresBackend from database_url.
    """

    def __init__(
        self,
        database_url: str | None = None,
        *,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_pixels: int = DEFAULT_MAX_PIXELS,
        allowed_formats: frozenset[str] = SUPPORTED_FORMATS,
        backend: StorageBackend | None = None,
    ) -> None:
        if backend is not None:
            self._backend = backend
        elif database_url is not None:
            self._backend = PostgresBackend(database_url)
        else:
            raise ValueError("Either database_url or backend must be provided")

        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._allowed_formats = allowed_formats

    def put(
        self,
        image: ImageInput,
        *,
        filename: str | None = None,
        optimize: bool = False,
        max_width: int | None = None,
        format: str | None = None,
        quality: int | None = None,
    ) -> str:
        """Validate, optionally optimize, and store an image. Returns its id.

        Accepts a file path (str or PathLike), raw bytes, or any
        file-like object with a .read() method.

        By default (optimize=False), the exact input bytes are stored
        unchanged -- what you put in is byte-for-byte what you get back.

        Args:
            optimize: If True, strips metadata (EXIF/GPS/ICC) and applies
                the max_width/format/quality options below. If False
                (default), all of those are ignored and the original
                bytes are stored as-is.
            max_width: Downscale if wider than this (aspect ratio
                preserved). Only applies when optimize=True.
            format: Re-encode target -- "jpeg", "png", "webp", or
                "heic"/"heif". None
                keeps the original format. Only applies when
                optimize=True. Note: quality has no effect when the
                target (or original) format is PNG -- PNG has no lossy
                quality setting.
            quality: 1-100, JPEG/WebP only. Defaults to values chosen to
                be visually lossless for typical photos (see
                zerobucket.optimization for the reasoning and
                tests/test_optimization.py for the SSIM regression test
                that enforces it). Only applies when optimize=True.
        """
        data, resolved_filename = _read_image_input(image, filename)

        validated = validate_image(
            data,
            max_bytes=self._max_bytes,
            max_pixels=self._max_pixels,
            allowed_formats=self._allowed_formats,
        )

        if optimize:
            result = optimize_image(
                data,
                max_width=max_width,
                target_format=format,
                quality=quality,
                max_bytes=self._max_bytes,
                max_pixels=self._max_pixels,
            )
            final_data = result.data
            mime_type = result.mime_type
            width = result.width
            height = result.height
            size_bytes = result.size_bytes
        else:
            final_data = data
            mime_type = validated.mime_type
            width = validated.width
            height = validated.height
            size_bytes = validated.size_bytes

        # Checksum is of the FINAL stored bytes, not the original input --
        # this matters once dedup is built, so two uploads that produce
        # identical stored bytes are recognized as identical.
        checksum = hashlib.sha256(final_data).hexdigest()

        return self._backend.put(
            data=final_data,
            mime_type=mime_type,
            original_filename=resolved_filename,
            size_bytes=size_bytes,
            width=width,
            height=height,
            checksum_sha256=checksum,
        )

    def get(self, image_id: str) -> Image:
        """Retrieve a full image, including bytes. Raises ImageNotFoundError if missing."""
        record = self._backend.get(image_id)
        if record is None:
            raise ImageNotFoundError(image_id)
        return Image(
            data=record.data,
            mime_type=record.mime_type,
            filename=record.original_filename,
            size_bytes=record.size_bytes,
            width=record.width,
            height=record.height,
            checksum_sha256=record.checksum_sha256,
        )

    def metadata(self, image_id: str) -> ImageMetadata:
        """Retrieve image metadata without pulling the (potentially large) bytes."""
        record = self._backend.get_metadata(image_id)
        if record is None:
            raise ImageNotFoundError(image_id)
        return ImageMetadata(
            image_id=record.id,
            mime_type=record.mime_type,
            filename=record.original_filename,
            size_bytes=record.size_bytes,
            width=record.width,
            height=record.height,
            checksum_sha256=record.checksum_sha256,
        )

    def exists(self, image_id: str) -> bool:
        """Return whether an image with this id exists."""
        return self._backend.exists(image_id)

    def delete(self, image_id: str) -> bool:
        """Delete an image. Returns True if it existed and was deleted, False otherwise."""
        return self._backend.delete(image_id)

    def close(self) -> None:
        """Release underlying database connections."""
        self._backend.close()

    def __enter__(self) -> ZeroBucket:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


def _read_image_input(
    image: ImageInput, filename: str | None
) -> tuple[bytes, str | None]:
    """Normalize any accepted input type into (bytes, filename)."""
    if isinstance(image, bytes):
        return image, filename
    if isinstance(image, (str, os.PathLike)):
        path = Path(image)
        data = path.read_bytes()
        return data, filename or path.name
    if hasattr(image, "read"):
        data = image.read()
        if isinstance(data, str):
            raise TypeError("File-like object must be opened in binary mode")
        raw_name = getattr(image, "filename", None) or getattr(image, "name", None)
        resolved_filename = filename or (
            os.path.basename(raw_name) if raw_name else None
        )
        return data, resolved_filename
    raise TypeError(
        f"Unsupported image input type: {type(image)!r}. "
        "Expected a file path, bytes, or a file-like object with .read()."
    )
