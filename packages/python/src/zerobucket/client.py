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
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import BinaryIO, Union

from .adapters.base import StorageBackend
from .adapters.postgres import (
    DEFAULT_STREAM_CHUNK_SIZE,
    OperationEvent,
    PostgresBackend,
)
from .content_types import ContentValidator
from .exceptions import (
    AccessDeniedError,
    ImageNotFoundError,
    ImageTooLargeError,
    ImageValidationError,
)
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

# 64 KiB. Chunk size used when reading a file-like put() input, so an
# oversized upload is rejected after reading a bounded amount past the
# cap rather than being fully buffered into memory first. See
# _read_image_input's docstring for why this doesn't extend to true
# unbounded-size streaming ingestion.
_WRITE_READ_CHUNK_SIZE = 64 * 1024


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
        pool_min_size: Minimum connections kept open in the internal
            pool. Defaults to 1, unchanged from every prior version --
            this makes a previously-hardcoded value configurable, not a
            new default.
        pool_max_size: Maximum connections the internal pool can open.
            Defaults to 5, unchanged from every prior version. Raise
            this if you're seeing pool-timeout errors under real
            concurrent load; see docs/OPERATIONS.md.
        pool_timeout: Seconds to wait for a pooled connection to become
            available before giving up. Defaults to 10, unchanged from
            every prior version.
        on_operation: Optional callback, called with a
            zerobucket.adapters.postgres.OperationEvent (operation name,
            duration, success/failure, retry count) after every storage
            operation completes. Wire this to your own metrics backend
            (Prometheus, StatsD, logging -- whatever you use); ZeroBucket
            does not ship a specific integration. Exceptions raised
            inside your callback are caught and silently ignored, so a
            bug in your metrics code can never break a real image
            operation. NOTE the difference from before_get/before_put
            below: on_operation is fire-and-forget observability, so
            swallowing its exceptions is safe; before_get/before_put are
            security decisions, so their exceptions are deliberately
            NOT swallowed.
        before_get: Optional authorization hook, called as
            before_get(image_id, context) -> bool before get(),
            get_many() (once per id), get_stream(), stream_to(), and
            metadata() -- anything that returns bytes or per-image
            metadata for a specific id. Return False to deny; the call
            then raises AccessDeniedError (or, for get_many(), that id's
            result gets error="access denied" instead of aborting the
            whole batch, same as any other per-item batch failure).
            `context` is whatever you pass as context= to the call being
            checked (e.g. the requesting user/tenant) -- ZeroBucket never
            inspects it itself, it's purely yours to define and use.
            NOT called for exists() -- a bare existence check was
            considered out of scope for this hook; gate it yourself at
            the call site if that matters for your use case. If the hook
            raises instead of returning a bool, that exception propagates
            (or is captured per-item for get_many()) rather than being
            treated as an implicit allow -- see AccessDeniedError's
            docstring for why. Denied/erroring calls never reach the
            database, so they do NOT produce an on_operation event.
        before_put: Optional authorization hook, called as
            before_put(context) -> bool before put() and put_many().
            There's no image_id yet at this point (nothing has been
            validated or assigned an id), so the signature is
            deliberately narrower than before_get's -- context is the
            only input. For put_many(), the hook is evaluated ONCE for
            the whole batch (not once per item) since context represents
            the caller's identity for the call, not per-item data; a
            denial marks every item in that batch as
            error="access denied" rather than aborting only some.
            Same fail-closed exception behavior as before_get.
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
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        pool_timeout: float = 10,
        on_operation: Callable[[OperationEvent], None] | None = None,
        before_get: Callable[[str, dict | None], bool] | None = None,
        before_put: Callable[[dict | None], bool] | None = None,
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
                pool_min_size=pool_min_size,
                pool_max_size=pool_max_size,
                pool_timeout=pool_timeout,
                on_operation=on_operation,
            )
        else:
            raise ValueError("Either database_url or backend must be provided")

        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._allowed_formats = allowed_formats
        self._before_get = before_get
        self._before_put = before_put

    def _check_before_get(self, image_id: str, context: dict | None) -> None:
        """Raise AccessDeniedError if a before_get hook is configured and
        denies this image_id. No-op if no hook is configured. Does NOT
        catch exceptions raised by the hook itself -- see
        AccessDeniedError's docstring for why."""
        if self._before_get is None:
            return
        if not self._before_get(image_id, context):
            raise AccessDeniedError("get", image_id)

    def _check_before_put(self, context: dict | None) -> None:
        """Raise AccessDeniedError if a before_put hook is configured and
        denies this call. No-op if no hook is configured."""
        if self._before_put is None:
            return
        if not self._before_put(context):
            raise AccessDeniedError("put")

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
        data, resolved_filename = _read_image_input(
            image, filename, max_bytes=self._max_bytes
        )

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
        context: dict | None = None,
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
            context: Passed through to the before_put hook, if one is
                configured (see the constructor's docstring). Ignored
                entirely if no before_put hook is set. Raises
                AccessDeniedError before any validation/storage work
                happens if the hook denies the call.
        """
        self._check_before_put(context)
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
        context: dict | None = None,
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

        context: Passed through to the before_put hook, if one is
            configured, EVALUATED ONCE for the whole call (not once per
            item) -- context represents who's making this batch call,
            not per-item data, so one evaluation covers the whole batch.
            A denial marks every item's result as error="access denied"
            without touching any of them, rather than partially
            processing the batch.
        """
        if filenames is not None and len(filenames) != len(images):
            raise ValueError("filenames must be the same length as images if provided")

        if self._before_put is not None:
            try:
                allowed = self._before_put(context)
            except Exception as exc:  # noqa: BLE001
                return [
                    BatchPutResult(index=i, image_id=None, error=str(exc))
                    for i in range(len(images))
                ]
            if not allowed:
                return [
                    BatchPutResult(index=i, image_id=None, error="access denied")
                    for i in range(len(images))
                ]

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

    def get(
        self,
        image_id: str,
        *,
        connection: object | None = None,
        context: dict | None = None,
    ) -> Image:
        """Retrieve a full image, including bytes. Raises ImageNotFoundError if missing.

        connection: Advanced -- see put()'s docstring. Passing the same
            open transaction lets you read back a row you just wrote in
            that same transaction, before it's committed.
        context: Passed through to the before_get hook, if one is
            configured (see the constructor's docstring). Raises
            AccessDeniedError before any database round trip if the hook
            denies the call.
        """
        self._check_before_get(image_id, context)
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
        self,
        image_ids: list[str],
        *,
        connection: object | None = None,
        context: dict | None = None,
    ) -> list[BatchGetResult]:
        """Retrieve multiple images in a single query.

        A missing id is NOT an error here (unlike get(), which raises
        ImageNotFoundError) -- it's a normal, expected outcome for a
        batch of ids where some may not exist; check `.success` per
        result. Results are returned in the same order as `image_ids`,
        including entries for ids that weren't found.

        context: Passed through to the before_get hook, if one is
            configured, evaluated ONCE PER ID (unlike put_many's
            before_put, which is evaluated once for the whole call) --
            authorization for a read is naturally per-item, since
            different ids in the same batch may belong to different
            owners. Ids the hook denies (or errors on) are excluded from
            the underlying database query entirely -- ZeroBucket doesn't
            fetch bytes for something it's about to refuse to return --
            and come back with error="access denied" (or the hook's own
            error message), same as any other per-item batch failure.
        """
        if not image_ids:
            return []

        results: list[BatchGetResult | None] = [None] * len(image_ids)
        allowed_indices: list[int] = []

        if self._before_get is not None:
            for i, image_id in enumerate(image_ids):
                try:
                    allowed = self._before_get(image_id, context)
                except Exception as exc:  # noqa: BLE001
                    results[i] = BatchGetResult(
                        image_id=image_id, image=None, error=str(exc)
                    )
                    continue
                if not allowed:
                    results[i] = BatchGetResult(
                        image_id=image_id, image=None, error="access denied"
                    )
                    continue
                allowed_indices.append(i)
        else:
            allowed_indices = list(range(len(image_ids)))

        allowed_ids = [image_ids[i] for i in allowed_indices]
        if allowed_ids:
            records = self._backend.get_many(allowed_ids, connection=connection)
            records_by_id = {record.id: record for record in records}
            for i in allowed_indices:
                image_id = image_ids[i]
                record = records_by_id.get(image_id)
                if record is None:
                    results[i] = BatchGetResult(
                        image_id=image_id, image=None, error="not found"
                    )
                else:
                    results[i] = BatchGetResult(
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
        return results  # type: ignore[return-value]

    def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
        connection: object | None = None,
        context: dict | None = None,
    ) -> Iterator[bytes]:
        """Retrieve an image's bytes as an iterator of chunks, instead of
        one complete `bytes` object. Raises ImageNotFoundError if missing
        (checked eagerly, before any chunk is yielded -- you don't have
        to start iterating to find out the id doesn't exist).

        Use this instead of get() when you want to pipe a stored image
        straight to a socket/file/HTTP response without ever holding the
        whole thing in Python memory at once -- e.g. FastAPI's
        StreamingResponse, or writing directly to an open file:

            for chunk in images.get_stream(image_id):
                response.write(chunk)

        Or use stream_to() below, which does exactly that loop for you.

        chunk_size: bytes per chunk. Defaults to 1 MiB. Larger values
            mean fewer round trips but higher peak memory; smaller values
            are the opposite.
        connection: Advanced -- see put()'s docstring. Without this, each
            chunk is fetched in its own round trip with no snapshot
            isolation across chunks -- a concurrent delete() between
            chunks raises StorageError rather than silently truncating
            the stream. Pass your own open transaction here if you need
            a consistent read across the whole stream.

        Honest limitation: this reduces PYTHON-side memory pressure per
        read, not Postgres-side -- the server still handles the full
        stored value the way it always does for a BYTEA column (TOAST
        detoast, etc). It also doesn't reduce network bytes transferred
        (still the full image, just paced out in pieces) -- it's not an
        HTTP range/partial-content feature. See the README's streaming
        section for what this does and doesn't buy you.

        context: Passed through to the before_get hook, if one is
            configured, checked ONCE up front (not once per chunk) --
            raises AccessDeniedError immediately, before even the
            metadata lookup used to check existence/size, if denied.
        """
        self._check_before_get(image_id, context)
        stream = self._backend.get_stream(
            image_id, chunk_size=chunk_size, connection=connection
        )
        if stream is None:
            raise ImageNotFoundError(image_id)
        return stream

    def stream_to(
        self,
        image_id: str,
        destination: BinaryIO,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
        connection: object | None = None,
        context: dict | None = None,
    ) -> int:
        """Write an image's bytes directly to `destination` (anything
        with a .write(bytes) method -- an open file, an HTTP response
        object, etc.), chunk by chunk, without holding the full image in
        Python memory at once. Returns the total number of bytes written.

        Equivalent to (and implemented as) looping over get_stream() and
        calling destination.write() yourself -- provided because it's the
        common case and there's no reason to make every caller write the
        loop. Raises ImageNotFoundError if missing, same as get_stream().

        context: Passed straight through to get_stream() -- see its
            docstring.
        """
        total = 0
        for chunk in self.get_stream(
            image_id, chunk_size=chunk_size, connection=connection, context=context
        ):
            destination.write(chunk)
            total += len(chunk)
        return total

    def metadata(
        self,
        image_id: str,
        *,
        connection: object | None = None,
        context: dict | None = None,
    ) -> ImageMetadata:
        """Retrieve image metadata without pulling the (potentially large) bytes.

        connection: Advanced -- see put()'s docstring.
        context: Passed through to the before_get hook, if one is
            configured -- metadata() is gated by the same hook as get(),
            since it's still per-image information tied to a specific id.
        """
        self._check_before_get(image_id, context)
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

        NOT gated by a before_get hook, even if one is configured --
        deliberately out of scope for this hook (see the constructor's
        before_get docs). A bare existence check returns no image data
        or metadata, so it was judged low-sensitivity enough not to
        require authorization by default; gate it yourself at the call
        site if your use case needs that.
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
    image: ImageInput, filename: str | None, *, max_bytes: int
) -> tuple[bytes, str | None]:
    """Normalize any accepted input type into (bytes, filename).

    For a file-like input specifically, this reads in bounded
    _WRITE_READ_CHUNK_SIZE chunks and stops as soon as it has read one
    byte past max_bytes, rather than calling .read() with no limit and
    buffering the entire stream before validate_image()'s own max_bytes
    check gets a chance to reject it. This bounds peak memory for an
    oversized upload to roughly max_bytes (not the stream's full,
    possibly much larger, size) -- the same "fail fast, don't pay for
    work you're about to reject" principle behind checking Content-
    Length before accepting a request body, applied here since put()
    can't always rely on the caller having done that upstream.

    This is NOT unbounded-size streaming ingestion: for input that IS
    within max_bytes, the full bytes still end up in memory here,
    because checksum computation and image validation (Pillow decode)
    both need the complete content -- there is no way to validate "is
    this a real, undamaged JPEG" from a prefix of the bytes. See
    get_stream()/stream_to() for the read-side streaming story, which
    has no equivalent requirement and is the more complete feature.

    Raises ImageTooLargeError itself, before validate_image() gets a
    chance to, when the input is a file-like object -- in that case the
    reported size_bytes is a LOWER BOUND (max_bytes + 1, or wherever
    reading stopped), not the stream's true total size, since finding
    the true size would mean reading all of it, which is exactly the
    cost this is avoiding. For bytes/path inputs (already fully in
    memory or a single read_bytes() call), the exact size is known and
    reported as before -- this only changes file-like input.
    """
    if isinstance(image, bytes):
        return image, filename
    if isinstance(image, (str, os.PathLike)):
        path = Path(image)
        data = path.read_bytes()
        return data, filename or path.name
    if hasattr(image, "read"):
        limit = max_bytes + 1
        chunks: list[bytes] = []
        total = 0
        while total <= max_bytes:
            piece = image.read(min(_WRITE_READ_CHUNK_SIZE, limit - total))
            if not piece:
                break
            if isinstance(piece, str):
                raise TypeError("File-like object must be opened in binary mode")
            chunks.append(piece)
            total += len(piece)
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise ImageTooLargeError(len(data), max_bytes)
        raw_name = getattr(image, "filename", None) or getattr(image, "name", None)
        resolved_filename = filename or (
            os.path.basename(raw_name) if raw_name else None
        )
        return data, resolved_filename
    raise TypeError(
        f"Unsupported image input type: {type(image)!r}. "
        "Expected a file path, bytes, or a file-like object with .read()."
    )
