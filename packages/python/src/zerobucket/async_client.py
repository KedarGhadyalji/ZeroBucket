"""The async counterpart to client.py's ZeroBucket.

    from zerobucket import AsyncZeroBucket
    images = AsyncZeroBucket(database_url="postgresql://...")
    image_id = await images.put("avatar.jpg")
    image = await images.get(image_id)

Built on psycopg3's native async mode (see adapters/postgres_async.py's
module docstring for why this is NOT the third-party `asyncpg` package
despite that being the name on the roadmap) -- zero new dependencies
beyond what the sync client already requires.

TRANSACTIONS (connection=, 0.33.0): every method accepts an optional
`connection=` -- your own open async connection (a psycopg AsyncConnection for
PostgreSQL, an aiosqlite Connection for SQLite, an aiomysql Connection for
MySQL/MariaDB) -- so a put()/get()/delete() runs inside YOUR transaction and
commits or rolls back with the rest of it, exactly as on the sync client:

    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        await conn.execute("INSERT INTO users (...) VALUES (...)")
        avatar_id = await images.put(data, connection=conn)
        # commits both on leaving the block, rolls back both on an exception

With a connection the call runs once on it, never commits, and is never
retried (replaying part of a caller's transaction could silently half-apply
it); it still reports an on_operation event. Open the connection for the SAME
database the client points at. See the README's Transactions section for the
per-database details (autocommit off for MySQL, isolation_level=None for the
SQLite tier_to_object_storage()).

There is nothing left that the sync client has and this one lacks: 0.27.0
added the on_operation metrics hook and automatic retry/backoff on transient
errors; 0.30.0 added dedup=True (content-addressed storage) and
object_storage= tiering with tier_to_object_storage(); 0.32.0 added the
before_get / before_put access-control hooks (with context=), optimize=True
with max_width=/format=/quality=, and custom validator=; 0.33.0 added
connection=.

Image validation (Pillow decode, format/size/decompression-bomb checks),
optimization (resize/re-encode), custom validators and file-like input reading
are all CPU/blocking-I/O work -- all run via asyncio.to_thread() here so they
don't block the event loop, at the cost of consuming a thread from Python's
default executor. Only the actual database round trips get a genuinely
non-blocking async driver underneath; validation was never going to be "async" in a meaningful
sense (Pillow has no async API), the goal here is just "doesn't stall
every other coroutine in your process while it runs."
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import BinaryIO

from ._ids import is_valid_id
from .adapters.base_async import AsyncStorageBackend
from .adapters.postgres import DEFAULT_STREAM_CHUNK_SIZE, OperationEvent
from .adapters.postgres_async import AsyncPostgresBackend
from .client import DEFAULT_MAX_BYTES, ImageInput, prepare_row
from .content_types import ContentValidator
from .exceptions import AccessDeniedError, ImageNotFoundError
from .object_storage import ObjectStorage
from .types import (
    BatchDeleteResult,
    BatchGetResult,
    BatchPutResult,
    Image,
    ImageMetadata,
)
from .validation import DEFAULT_MAX_PIXELS, SUPPORTED_FORMATS


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

        before_get, before_put (0.32.0): the same access-control hooks as the
        sync ZeroBucket -- `before_get(image_id, context) -> bool` and
        `before_put(context) -> bool`; see its docstring for the full
        semantics (denial raises AccessDeniedError, and marks the item
        "access denied" in the batch methods; a malformed id is "not found"
        and never reaches the hook; hook exceptions are not swallowed in the
        single-item methods). Unlike the sync client, each hook may be a plain
        function OR an `async def` function, so it can consult your database
        or an auth service without blocking the event loop.
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
        before_get: Callable[[str, dict | None], bool | Awaitable[bool]] | None = None,
        before_put: Callable[[dict | None], bool | Awaitable[bool]] | None = None,
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
        self._before_get = before_get
        self._before_put = before_put

    @staticmethod
    def _conn_kwargs(connection: object | None) -> dict:
        """`connection=` is forwarded to the backend ONLY when the caller
        gave one, so a custom backend written before 0.33.0 (whose methods
        do not accept it) keeps working until someone actually passes a
        connection to it."""
        return {} if connection is None else {"connection": connection}

    @staticmethod
    async def _call_hook(hook: Callable, *args: object) -> object:
        """Call an access-control hook that may be a plain function or an
        `async def` one; await the result if it is awaitable."""
        result = hook(*args)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _check_before_get(self, image_id: str, context: dict | None) -> None:
        """Async twin of the sync client's _check_before_get(): a malformed
        id is "not found" and never reaches the hook; a hook that returns a
        falsy value raises AccessDeniedError; a hook that raises is not
        caught."""
        if not is_valid_id(image_id):
            raise ImageNotFoundError(image_id)
        if self._before_get is None:
            return
        if not await self._call_hook(self._before_get, image_id, context):
            raise AccessDeniedError("get", image_id)

    async def _check_before_put(self, context: dict | None) -> None:
        if self._before_put is None:
            return
        if not await self._call_hook(self._before_put, context):
            raise AccessDeniedError("put")

    async def _prepare_row(
        self,
        image: ImageInput,
        *,
        filename: str | None,
        optimize: bool = False,
        max_width: int | None = None,
        format: str | None = None,
        quality: int | None = None,
        validator: ContentValidator | None = None,
    ) -> dict:
        """Async counterpart to the sync client's _prepare_row: the very same
        function (client.prepare_row), run in a worker thread so the CPU and
        blocking-file-I/O work (read, Pillow decode, optimize, checksum,
        custom validator) doesn't stall the event loop."""
        return await asyncio.to_thread(
            prepare_row,
            image,
            filename=filename,
            optimize=optimize,
            max_width=max_width,
            format=format,
            quality=quality,
            validator=validator,
            max_bytes=self._max_bytes,
            max_pixels=self._max_pixels,
            allowed_formats=self._allowed_formats,
        )

    async def put(
        self,
        image: ImageInput,
        *,
        filename: str | None = None,
        optimize: bool = False,
        max_width: int | None = None,
        format: str | None = None,
        quality: int | None = None,
        validator: ContentValidator | None = None,
        context: dict | None = None,
        connection: object | None = None,
    ) -> str:
        """Validate, optionally optimize, and store an image. Returns its id.

        Identical to the sync ZeroBucket.put() (see its docstring for the
        input types accepted, and for what optimize=/max_width=/format=/
        quality=/validator=/context=/connection= do). Pass your own open
        async connection as connection= to store the image inside your own
        transaction: it is not committed here, and not retried (see this
        module's docstring). With optimize=False (the
        default) the exact input bytes are stored unchanged. optimize=True
        is incompatible with a custom validator= (ImageValidationError).
        A denying before_put hook raises AccessDeniedError before any
        validation or storage work happens.
        """
        await self._check_before_put(context)
        row = await self._prepare_row(
            image,
            filename=filename,
            optimize=optimize,
            max_width=max_width,
            format=format,
            quality=quality,
            validator=validator,
        )
        return await self._backend.put(**row, **self._conn_kwargs(connection))

    async def put_many(
        self,
        images: list[ImageInput],
        *,
        filenames: list[str | None] | None = None,
        optimize: bool = False,
        max_width: int | None = None,
        format: str | None = None,
        quality: int | None = None,
        validator: ContentValidator | None = None,
        context: dict | None = None,
        connection: object | None = None,
    ) -> list[BatchPutResult]:
        """Store multiple images. Best-effort, not all-or-nothing -- same
        semantics as the sync put_many(). Unlike the sync version,
        per-item validation runs CONCURRENTLY (via asyncio.gather over
        the asyncio.to_thread calls in _prepare_row), not in a serial
        Python loop -- a real advantage of the async version for batches
        of any size, not just a port of the sync behavior."""
        if filenames is not None and len(filenames) != len(images):
            raise ValueError("filenames must be the same length as images if provided")

        # Same as the sync put_many(): the before_put hook is evaluated ONCE
        # for the whole call, and a denial (or an error in the hook) marks
        # every item without touching any of them.
        if self._before_put is not None:
            try:
                allowed = await self._call_hook(self._before_put, context)
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

        tasks = [
            self._prepare_row(
                image,
                filename=(filenames[i] if filenames is not None else None),
                optimize=optimize,
                max_width=max_width,
                format=format,
                quality=quality,
                validator=validator,
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
                ids = await self._backend.put_many(
                    rows, **self._conn_kwargs(connection)
                )
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

    async def get(
        self,
        image_id: str,
        *,
        context: dict | None = None,
        connection: object | None = None,
    ) -> Image:
        """Retrieve a full image, including bytes. Raises
        ImageNotFoundError if missing. context is passed to the before_get
        hook, if one is configured; a denial raises AccessDeniedError before
        any database round trip."""
        await self._check_before_get(image_id, context)
        record = await self._backend.get(image_id, **self._conn_kwargs(connection))
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

    async def get_many(
        self,
        image_ids: list[str],
        *,
        context: dict | None = None,
        connection: object | None = None,
    ) -> list[BatchGetResult]:
        """Retrieve multiple images in a single query. Same not-an-error
        semantics for missing ids as the sync get_many(), and the same
        per-id before_get evaluation: ids the hook denies (or errors on) are
        left out of the database query and come back with error="access
        denied" (or the hook's own message); a malformed id is "not found"
        and never reaches the hook."""
        if not image_ids:
            return []

        results: list[BatchGetResult | None] = [None] * len(image_ids)
        allowed_indices: list[int] = []

        if self._before_get is not None:
            for i, image_id in enumerate(image_ids):
                if not is_valid_id(image_id):
                    results[i] = BatchGetResult(
                        image_id=image_id, image=None, error="not found"
                    )
                    continue
                try:
                    allowed = await self._call_hook(self._before_get, image_id, context)
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
            records = await self._backend.get_many(
                allowed_ids, **self._conn_kwargs(connection)
            )
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

    async def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
        context: dict | None = None,
        connection: object | None = None,
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

        context is passed to the before_get hook, if one is configured; a
        denial raises AccessDeniedError when you await this call, before any
        database round trip.
        """
        await self._check_before_get(image_id, context)
        stream = await self._backend.get_stream(
            image_id, chunk_size=chunk_size, **self._conn_kwargs(connection)
        )
        if stream is None:
            raise ImageNotFoundError(image_id)
        return stream

    async def stream_to(
        self,
        image_id: str,
        destination: BinaryIO,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
        context: dict | None = None,
        connection: object | None = None,
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
        stream = await self.get_stream(
            image_id, chunk_size=chunk_size, context=context, connection=connection
        )
        async for chunk in stream:
            destination.write(chunk)
            total += len(chunk)
        return total

    async def metadata(
        self,
        image_id: str,
        *,
        context: dict | None = None,
        connection: object | None = None,
    ) -> ImageMetadata:
        """Retrieve image metadata without pulling the (potentially large)
        bytes. context is passed to the before_get hook, as in get()."""
        await self._check_before_get(image_id, context)
        record = await self._backend.get_metadata(
            image_id, **self._conn_kwargs(connection)
        )
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

    async def exists(self, image_id: str, *, connection: object | None = None) -> bool:
        """Return whether an image with this id exists."""
        return await self._backend.exists(image_id, **self._conn_kwargs(connection))

    async def delete(self, image_id: str, *, connection: object | None = None) -> bool:
        """Delete an image. Returns True if it existed and was deleted."""
        return await self._backend.delete(image_id, **self._conn_kwargs(connection))

    async def delete_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[BatchDeleteResult]:
        """Delete multiple images in a single query. Same not-an-error
        semantics for missing ids as the sync delete_many()."""
        if not image_ids:
            return []
        deleted_ids = set(
            await self._backend.delete_many(image_ids, **self._conn_kwargs(connection))
        )
        return [
            BatchDeleteResult(
                image_id=image_id, deleted=image_id in deleted_ids, error=None
            )
            for image_id in image_ids
        ]

    async def tier_to_object_storage(
        self, image_id: str, *, connection: object | None = None
    ) -> bool:
        """Move an image's bytes out of the database and into object storage
        (see the constructor's object_storage= parameter). Returns True if it
        was tiered by this call, False if it was already tiered (a safe
        no-op), and raises ImageNotFoundError if the id doesn't exist. Same
        contract and safety as the sync ZeroBucket.tier_to_object_storage():
        the upload happens inside the transaction that locked the row, so a
        failed upload leaves the row untouched."""
        # Not part of the generic AsyncStorageBackend interface, like the
        # sync client: a backend without tiering raises AttributeError.
        result = await self._backend.tier_to_object_storage(  # type: ignore[attr-defined]
            image_id, **self._conn_kwargs(connection)
        )
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
