"""The async counterpart to client.py's ZeroBucket.

    from zerobucket import AsyncZeroBucket
    images = AsyncZeroBucket(database_url="postgresql://...")
    image_id = await images.put("avatar.jpg")
    image = await images.get(image_id)

Built on psycopg3's native async mode (see adapters/postgres_async.py's
module docstring for why this is NOT the third-party `asyncpg` package
despite that being the name on the roadmap) -- zero new dependencies
beyond what the sync client already requires.

WHAT'S DELIBERATELY NOT HERE YET, tracked honestly rather than implied
(this is a first pass, scoped to "core operations + streaming reads" on
purpose -- see the project's CHANGELOG for the scoping decision):

- before_get/before_put access-control hooks.
- optimize=True (resize/re-encode pipeline) and custom validator=
  support -- put() here only does the built-in image validation.
- connection= transaction participation.

(0.27.0 added the on_operation metrics hook and automatic retry/backoff
on transient errors, for all three async adapters. 0.30.0 added dedup=True
(content-addressed storage) and object_storage= tiering, with
tier_to_object_storage(), for all three.)

Every one of these exists on the sync ZeroBucket today; none is
architecturally blocked from being added to the async client later, they
were simply left out of this first pass to ship a genuinely useful async
core (the thing actually blocking FastAPI/async-Django adopters) without
a much larger, slower single change. If you need any of the above today,
use the sync ZeroBucket -- e.g. via `asyncio.to_thread(images.put, ...)`
from async code, at the cost of not getting a real non-blocking driver
underneath for that call.

Image validation (Pillow decode, format/size/decompression-bomb checks)
and file-like input reading are both CPU/blocking-I/O work -- both are
run via asyncio.to_thread() here so they don't block the event loop, at
the cost of consuming a thread from Python's default executor. Only the
actual database round trips get a genuinely non-blocking async driver
underneath; validation was never going to be "async" in a meaningful
sense (Pillow has no async API), the goal here is just "doesn't stall
every other coroutine in your process while it runs."
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Callable
from typing import BinaryIO

from .adapters.base_async import AsyncStorageBackend
from .adapters.postgres import DEFAULT_STREAM_CHUNK_SIZE, OperationEvent
from .adapters.postgres_async import AsyncPostgresBackend
from .client import DEFAULT_MAX_BYTES, ImageInput, _read_image_input
from .exceptions import ImageNotFoundError
from .object_storage import ObjectStorage
from .types import (
    BatchDeleteResult,
    BatchGetResult,
    BatchPutResult,
    Image,
    ImageMetadata,
)
from .validation import DEFAULT_MAX_PIXELS, SUPPORTED_FORMATS, validate_image


class AsyncZeroBucket:
    """Database-native image storage, async version. See this module's
    docstring for what's deliberately not included in this first pass.

    Args: same meaning as the sync ZeroBucket's constructor for
        max_bytes/max_pixels/allowed_formats/pool_min_size/
        pool_max_size/pool_timeout/max_retries/retry_base_delay/
        on_operation -- see its docstring for the full rationale on
        each; not repeated here. max_retries, retry_base_delay and
        on_operation only configure the PostgreSQL backend this class
        builds from `database_url`. With `backend=...` they are ignored:
        pass them to that backend's own constructor instead (every async
        adapter accepts the same three).

        dedup, object_storage (0.30.0): the same two options as the sync
        ZeroBucket -- dedup=True stores each distinct image once, and
        object_storage= enables tier_to_object_storage() and reads tiered
        images back from the bucket. They cannot be combined (ValueError).
        Like the retry options they configure only the PostgreSQL backend
        built from `database_url`; with `backend=...` pass them to that
        backend instead.
    """

    def __init__(
        self,
        database_url: str | None = None,
        *,
        max_bytes: int = DEFAULT_MAX_BYTES,
        max_pixels: int = DEFAULT_MAX_PIXELS,
        allowed_formats: frozenset[str] = SUPPORTED_FORMATS,
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        pool_timeout: float = 10,
        auto_migrate: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
        on_operation: Callable[[OperationEvent], None] | None = None,
        dedup: bool = False,
        object_storage: ObjectStorage | None = None,
        backend: AsyncStorageBackend | None = None,
    ) -> None:
        if backend is not None:
            self._backend = backend
        elif database_url is not None:
            self._backend = AsyncPostgresBackend(
                database_url,
                auto_migrate=auto_migrate,
                pool_min_size=pool_min_size,
                pool_max_size=pool_max_size,
                pool_timeout=pool_timeout,
                max_retries=max_retries,
                retry_base_delay=retry_base_delay,
                on_operation=on_operation,
                dedup=dedup,
                object_storage=object_storage,
            )
        else:
            raise ValueError("Either database_url or backend must be provided")

        self._max_bytes = max_bytes
        self._max_pixels = max_pixels
        self._allowed_formats = allowed_formats

    async def _prepare_row(self, image: ImageInput, *, filename: str | None) -> dict:
        """Async counterpart to client.py's _prepare_row -- narrower
        (no optimize=/validator= support in this pass, see module
        docstring). Reads/validates via asyncio.to_thread so the CPU/
        blocking-I/O work doesn't stall the event loop."""
        data, resolved_filename = await asyncio.to_thread(
            _read_image_input, image, filename, max_bytes=self._max_bytes
        )
        validated = await asyncio.to_thread(
            validate_image,
            data,
            max_bytes=self._max_bytes,
            max_pixels=self._max_pixels,
            allowed_formats=self._allowed_formats,
        )
        checksum = await asyncio.to_thread(lambda: hashlib.sha256(data).hexdigest())
        return {
            "data": data,
            "mime_type": validated.mime_type,
            "original_filename": resolved_filename,
            "size_bytes": validated.size_bytes,
            "width": validated.width,
            "height": validated.height,
            "checksum_sha256": checksum,
        }

    async def put(self, image: ImageInput, *, filename: str | None = None) -> str:
        """Validate and store an image. Returns its id. See the sync
        ZeroBucket.put()'s docstring for input types accepted and
        general behavior -- identical here except optimize=/format=/
        quality=/validator=/connection= are not yet supported (see this
        module's docstring)."""
        row = await self._prepare_row(image, filename=filename)
        return await self._backend.put(**row)

    async def put_many(
        self,
        images: list[ImageInput],
        *,
        filenames: list[str | None] | None = None,
    ) -> list[BatchPutResult]:
        """Store multiple images. Best-effort, not all-or-nothing -- same
        semantics as the sync put_many(). Unlike the sync version,
        per-item validation runs CONCURRENTLY (via asyncio.gather over
        the asyncio.to_thread calls in _prepare_row), not in a serial
        Python loop -- a real advantage of the async version for batches
        of any size, not just a port of the sync behavior."""
        if filenames is not None and len(filenames) != len(images):
            raise ValueError("filenames must be the same length as images if provided")

        tasks = [
            self._prepare_row(
                image, filename=(filenames[i] if filenames is not None else None)
            )
            for i, image in enumerate(images)
        ]
        prepared = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[BatchPutResult | None] = [None] * len(images)
        rows: list[dict] = []
        indices: list[int] = []
        for i, item in enumerate(prepared):
            if isinstance(item, BaseException):
                results[i] = BatchPutResult(index=i, image_id=None, error=str(item))
            else:
                rows.append(item)
                indices.append(i)

        if rows:
            try:
                ids = await self._backend.put_many(rows)
                for idx, image_id in zip(indices, ids, strict=True):
                    results[idx] = BatchPutResult(
                        index=idx, image_id=image_id, error=None
                    )
            except Exception as exc:  # noqa: BLE001
                for idx in indices:
                    results[idx] = BatchPutResult(
                        index=idx, image_id=None, error=str(exc)
                    )

        return results  # type: ignore[return-value]

    async def get(self, image_id: str) -> Image:
        """Retrieve a full image, including bytes. Raises
        ImageNotFoundError if missing."""
        record = await self._backend.get(image_id)
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

    async def get_many(self, image_ids: list[str]) -> list[BatchGetResult]:
        """Retrieve multiple images in a single query. Same not-an-error
        semantics for missing ids as the sync get_many()."""
        if not image_ids:
            return []
        records = await self._backend.get_many(image_ids)
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

    async def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes]:
        """Retrieve an image's bytes as an async iterator of chunks.

        IMPORTANT DIFFERENCE FROM THE SYNC CLIENT: this is a coroutine
        that returns an async iterator -- you must `await` it before
        iterating, unlike the sync get_stream() which returns an
        iterator directly:

            stream = await images.get_stream(image_id)
            async for chunk in stream:
                ...

        This is what lets the not-found check happen eagerly, the same
        moment you await this call, rather than being deferred to the
        first `async for` iteration the way it would be if this were
        itself an async generator function (Python async generators run
        no code, not even the code before their first yield, until
        first iterated -- this design avoids that surprise). Raises
        ImageNotFoundError if missing.

        Same honest limitations as the sync version's get_stream(): this
        reduces Python-side memory pressure per read, not Postgres-side;
        it's not HTTP range/partial-content support; and a concurrent
        delete() mid-stream raises StorageError rather than silently
        returning a short/truncated stream.
        """
        stream = await self._backend.get_stream(image_id, chunk_size=chunk_size)
        if stream is None:
            raise ImageNotFoundError(image_id)
        return stream

    async def stream_to(
        self,
        image_id: str,
        destination: BinaryIO,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> int:
        """Write an image's bytes directly to `destination` (anything
        with a plain, SYNCHRONOUS .write(bytes) method -- an open file,
        io.BytesIO, etc.), chunk by chunk. Returns total bytes written.

        `destination.write()` is called directly, NOT awaited -- this
        does not support async destinations (e.g. aiofiles). For an
        async destination, loop over `await get_stream()` yourself and
        `await destination.write(chunk)` in your own loop instead of
        using this convenience wrapper.
        """
        total = 0
        stream = await self.get_stream(image_id, chunk_size=chunk_size)
        async for chunk in stream:
            destination.write(chunk)
            total += len(chunk)
        return total

    async def metadata(self, image_id: str) -> ImageMetadata:
        """Retrieve image metadata without pulling the (potentially large) bytes."""
        record = await self._backend.get_metadata(image_id)
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

    async def exists(self, image_id: str) -> bool:
        """Return whether an image with this id exists."""
        return await self._backend.exists(image_id)

    async def delete(self, image_id: str) -> bool:
        """Delete an image. Returns True if it existed and was deleted."""
        return await self._backend.delete(image_id)

    async def delete_many(self, image_ids: list[str]) -> list[BatchDeleteResult]:
        """Delete multiple images in a single query. Same not-an-error
        semantics for missing ids as the sync delete_many()."""
        if not image_ids:
            return []
        deleted_ids = set(await self._backend.delete_many(image_ids))
        return [
            BatchDeleteResult(
                image_id=image_id, deleted=image_id in deleted_ids, error=None
            )
            for image_id in image_ids
        ]

    async def tier_to_object_storage(self, image_id: str) -> bool:
        """Move an image's bytes out of the database and into object storage
        (see the constructor's object_storage= parameter). Returns True if it
        was tiered by this call, False if it was already tiered (a safe
        no-op), and raises ImageNotFoundError if the id doesn't exist. Same
        contract and safety as the sync ZeroBucket.tier_to_object_storage():
        the upload happens inside the transaction that locked the row, so a
        failed upload leaves the row untouched."""
        # Not part of the generic AsyncStorageBackend interface, like the
        # sync client: a backend without tiering raises AttributeError.
        result = await self._backend.tier_to_object_storage(image_id)  # type: ignore[attr-defined]
        if result is None:
            raise ImageNotFoundError(image_id)
        return result

    async def close(self) -> None:
        """Release underlying database connections."""
        await self._backend.close()

    async def __aenter__(self) -> AsyncZeroBucket:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
