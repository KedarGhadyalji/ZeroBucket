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
from .content_types import ContentValidator
from .exceptions import ImageNotFoundError, ImageValidationError
from .optimization import optimize_image
from .types import (
    BatchDeleteResult,
    BatchGetResult,
    BatchPutResult,
    Image,
    ImageMetadata,
)
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
        max_retries: How many times to automatically retry a transient
            database error (connection drop, deadlock, serialization
            failure) before giving up. Defaults to 3. Set to 0 to
            disable automatic retry entirely. Only applies to calls that
            do NOT pass their own connection= -- see put()'s docstring
            for why passing your own connection disables automatic retry.
        retry_base_delay: Base delay in seconds for exponential backoff
            between retries (actual delay grows per attempt, with
            jitter, capped at 2 seconds). Defaults to 0.1.
        dedup: If True, use content-addressed storage: byte-identical
            uploads share one underlying stored copy, reference-counted,
            with the bytes actually deleted only when the last
            referencing id is deleted. Defaults to False (classic
            single-table behavior, unchanged since v0.1.0). Uses
            DIFFERENT database tables from classic mode (zerobucket_blobs
            / zerobucket_image_refs, not zerobucket_images) -- flipping
            this on does NOT retroactively deduplicate or migrate
            existing classic-mode data; see
            zerobucket.adapters.postgres.migrate_classic_to_dedup for
            that (a separate, explicit, non-destructive operation).
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
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
        dedup: bool = False,
        backend: StorageBackend | None = None,
    ) -> None:
        if backend is not None:
            self._backend = backend
        elif database_url is not None:
            self._backend = PostgresBackend(
                database_url,
                max_retries=max_retries,
                retry_base_delay=retry_base_delay,
                dedup=dedup,
            )
        else:
            raise ValueError("Either database_url or backend must be provided")

        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._allowed_formats = allowed_formats

    def _prepare_row(
        self,
        image: ImageInput,
        *,
        filename: str | None,
        optimize: bool,
        max_width: int | None,
        format: str | None,
        quality: int | None,
        validator: ContentValidator | None = None,
    ) -> dict:
        """Validate, optionally optimize, and checksum one image -- the
        shared per-item work behind both put() and put_many(). This is
        inherently per-item (Pillow decode/validate/re-encode can't be
        batched), which is why put_many() still does this work in a
        Python loop even though the actual DB insert is batched."""
        data, resolved_filename = _read_image_input(image, filename)

        if validator is not None:
            if optimize:
                raise ImageValidationError(
                    "optimize=True is not supported together with a custom "
                    "validator= -- the optimize pipeline (resize/re-encode) "
                    "is image-specific (Pillow-based) and assumes the "
                    "built-in image validator's output. Validate/transform "
                    "custom content types yourself before calling put()."
                )
            validated_content = validator.validate(data, max_bytes=self._max_bytes)
            final_data = data
            mime_type = validated_content.mime_type
            width = validated_content.width
            height = validated_content.height
            size_bytes = validated_content.size_bytes
        else:
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

        checksum = hashlib.sha256(final_data).hexdigest()

        return {
            "data": final_data,
            "mime_type": mime_type,
            "original_filename": resolved_filename,
            "size_bytes": size_bytes,
            "width": width,
            "height": height,
            "checksum_sha256": checksum,
        }

    def put(
        self,
        image: ImageInput,
        *,
        filename: str | None = None,
        optimize: bool = False,
        max_width: int | None = None,
        format: str | None = None,
        quality: int | None = None,
        connection: object | None = None,
        validator: ContentValidator | None = None,
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
            connection: Advanced -- pass your own open psycopg connection
                (one you're already using for other writes in the same
                transaction) to make this put() commit or roll back
                together with the rest of that transaction, instead of
                committing independently on ZeroBucket's own internal
                pool. Without this, put() ALWAYS commits on its own,
                regardless of what your application does afterward --
                see the README's "Transactions" section for a worked
                example and why this matters (e.g. "create a user record
                and store their avatar atomically" only works if you
                pass connection= here). NOTE: passing connection= also
                disables automatic retry for this call (see the
                ZeroBucket constructor's max_retries docs) -- retrying a
                statement on a connection you're managing yourself could
                silently corrupt your transaction's semantics.
            validator: Advanced -- pass a ContentValidator (e.g.
                zerobucket.validators.pdf.PDFValidator()) to store a
                content type ZeroBucket doesn't natively validate as an
                image. When given, the built-in image validation is
                skipped entirely in favor of validator.validate(); every
                other feature (connection=, retry, batch, get/delete/
                exists) works identically regardless of which validator
                produced the row. Incompatible with optimize=True (raises
                immediately if both are given) -- see ContentValidator's
                docstring for why.
        """
        row = self._prepare_row(
            image,
            filename=filename,
            optimize=optimize,
            max_width=max_width,
            format=format,
            quality=quality,
            validator=validator,
        )
        return self._backend.put(**row, connection=connection)

    def put_many(
        self,
        images: list[ImageInput],
        *,
        filenames: list[str | None] | None = None,
        optimize: bool = False,
        max_width: int | None = None,
        format: str | None = None,
        quality: int | None = None,
        connection: object | None = None,
        validator: ContentValidator | None = None,
    ) -> list[BatchPutResult]:
        """Store multiple images. Best-effort, not all-or-nothing: one
        bad image doesn't abort the rest of the batch -- check each
        result's `.success`/`.error` rather than assuming the whole
        batch succeeded.

        The same optimize/max_width/format/quality/validator settings
        apply to every item in the batch (no per-item overrides) -- call
        put() individually if different items need different settings.

        Per-item validation/optimization still happens in a Python loop
        (that work is inherently per-item), but the actual database
        inserts for everything that validated successfully are batched
        into one pipelined round trip via psycopg's executemany(), not
        one round trip per image.

        filenames, if given, must be the same length as images (use None
        for individual entries to fall back to the input's own filename,
        same as put()).
        """
        if filenames is not None and len(filenames) != len(images):
            raise ValueError("filenames must be the same length as images if provided")

        prepared_rows: list[dict] = []
        prepared_indices: list[int] = []
        results: list[BatchPutResult | None] = [None] * len(images)

        for i, image in enumerate(images):
            fname = filenames[i] if filenames is not None else None
            try:
                row = self._prepare_row(
                    image,
                    filename=fname,
                    optimize=optimize,
                    max_width=max_width,
                    format=format,
                    quality=quality,
                    validator=validator,
                )
                prepared_rows.append(row)
                prepared_indices.append(i)
            except Exception as exc:  # noqa: BLE001
                results[i] = BatchPutResult(index=i, image_id=None, error=str(exc))

        if prepared_rows:
            try:
                ids = self._backend.put_many(prepared_rows, connection=connection)
                for idx, image_id in zip(prepared_indices, ids, strict=True):
                    results[idx] = BatchPutResult(
                        index=idx, image_id=image_id, error=None
                    )
            except Exception as exc:  # noqa: BLE001
                # The whole DB batch failed (e.g. connection error) --
                # every successfully-validated item in this batch failed
                # too, since they shared one executemany() call.
                for idx in prepared_indices:
                    results[idx] = BatchPutResult(
                        index=idx, image_id=None, error=str(exc)
                    )

        return results  # type: ignore[return-value]

    def get(self, image_id: str, *, connection: object | None = None) -> Image:
        """Retrieve a full image, including bytes. Raises ImageNotFoundError if missing.

        connection: Advanced -- see put()'s docstring. Passing the same
            open transaction lets you read back a row you just wrote in
            that same transaction, before it's committed.
        """
        record = self._backend.get(image_id, connection=connection)
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

    def get_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[BatchGetResult]:
        """Retrieve multiple images in a single query.

        A missing id is NOT an error here (unlike get(), which raises
        ImageNotFoundError) -- it's a normal, expected outcome for a
        batch of ids where some may not exist; check `.success` per
        result. Results are returned in the same order as `image_ids`,
        including entries for ids that weren't found.
        """
        if not image_ids:
            return []
        records = self._backend.get_many(image_ids, connection=connection)
        records_by_id = {record.id: record for record in records}

        results = []
        for image_id in image_ids:
            record = records_by_id.get(image_id)
            if record is None:
                results.append(
                    BatchGetResult(image_id=image_id, image=None, error="not found")
                )
            else:
                results.append(
                    BatchGetResult(
                        image_id=image_id,
                        image=Image(
                            data=record.data,
                            mime_type=record.mime_type,
                            filename=record.original_filename,
                            size_bytes=record.size_bytes,
                            width=record.width,
                            height=record.height,
                            checksum_sha256=record.checksum_sha256,
                        ),
                        error=None,
                    )
                )
        return results

    def metadata(
        self, image_id: str, *, connection: object | None = None
    ) -> ImageMetadata:
        """Retrieve image metadata without pulling the (potentially large) bytes.

        connection: Advanced -- see put()'s docstring.
        """
        record = self._backend.get_metadata(image_id, connection=connection)
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

    def exists(self, image_id: str, *, connection: object | None = None) -> bool:
        """Return whether an image with this id exists.

        connection: Advanced -- see put()'s docstring.
        """
        return self._backend.exists(image_id, connection=connection)

    def delete(self, image_id: str, *, connection: object | None = None) -> bool:
        """Delete an image. Returns True if it existed and was deleted, False otherwise.

        connection: Advanced -- see put()'s docstring. Passing the same
            open transaction lets a delete() roll back together with the
            rest of that transaction (e.g. "delete a user and their
            avatar atomically").
        """
        return self._backend.delete(image_id, connection=connection)

    def delete_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[BatchDeleteResult]:
        """Delete multiple images in a single query. Results are returned
        in the same order as `image_ids`; a missing id gets
        `deleted=False`, not an error -- same semantics as delete()
        returning False for one missing id."""
        if not image_ids:
            return []
        deleted_ids = set(self._backend.delete_many(image_ids, connection=connection))
        return [
            BatchDeleteResult(
                image_id=image_id, deleted=image_id in deleted_ids, error=None
            )
            for image_id in image_ids
        ]

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
