"""Async PostgreSQL storage adapter.

Built on psycopg3's OWN native async mode (AsyncConnection,
AsyncConnectionPool) -- NOT the third-party `asyncpg` package, despite
that being the name used on the roadmap. This was a deliberate technical
correction, not a rename: this library is built on psycopg3, which
already ships a real async driver mode using the exact same SQL,
schema, and connection string as the sync adapter. Adding `asyncpg` on
top would mean maintaining two different SQL layers against two
different drivers for the same feature, for no benefit to anyone -- an
async FastAPI/Django-async user gets the same "await zb.get(id)"
regardless of which driver library sits underneath. Zero new
dependencies were needed for this: psycopg[binary] and psycopg_pool are
already required by the sync adapter.

Reuses the exact same SQL query strings and schema DDL as postgres.py
(imported, not copy-pasted) -- one schema, one set of queries, two ways
of executing them.

Since 0.30.0 this supports dedup=True (content-addressed storage) and
object_storage= tiering, same as the sync adapter -- but not both at once
(same restriction, same ValueError). The object-storage SDK (boto3) is
blocking, so every call to it runs via asyncio.to_thread() and never stalls
the event loop.
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable

from psycopg_pool import AsyncConnectionPool

from .._ids import is_valid_id
from ..exceptions import StorageError
from ..object_storage import ObjectStorage
from .base import StoredRecord, StoredRecordMetadata
from .base_async import AsyncStorageBackend
from .postgres import (
    _DEDUP_DECREMENT_BLOB,
    _DEDUP_DELETE_EMPTY_BLOBS,
    _DEDUP_DELETE_REF,
    _DEDUP_EXISTS,
    _DEDUP_INSERT_REF,
    _DEDUP_SCHEMA,
    _DEDUP_SELECT_CHUNK,
    _DEDUP_SELECT_FULL,
    _DEDUP_SELECT_METADATA,
    _DEDUP_UPSERT_BLOB,
    _DELETE_RETURNING,
    _EXISTS,
    _INSERT,
    _SCHEMA,
    _SELECT_CHUNK,
    _SELECT_FOR_TIERING,
    _SELECT_FULL,
    _SELECT_METADATA,
    _SELECT_STREAM_INFO,
    _UPDATE_AFTER_TIERING,
    DEFAULT_STREAM_CHUNK_SIZE,
    OperationEvent,
    _backoff_delay,
    _is_retryable,
)

__all__ = ["AsyncPostgresBackend", "DEFAULT_STREAM_CHUNK_SIZE"]


def _windows_proactor_loop_error() -> StorageError:
    """Built to mirror psycopg's OWN check for this exact condition
    (psycopg.AsyncConnection.connect(), connection_async.py: `if
    sys.platform == "win32": ... isinstance(loop, asyncio.ProactorEventLoop)`)
    -- confirmed by reading the installed psycopg source directly, not
    guessed at. psycopg raises a clear, specific InterfaceError for this;
    the problem is psycopg_pool.AsyncConnectionPool doesn't propagate it
    -- its background connect worker catches it, logs a WARNING per
    retry attempt, and keeps retrying silently until the whole pool
    times out with a generic PoolTimeout ~10+ seconds later. That's a
    real, verified failure mode (not a hypothetical): 4 identical
    WARNING log lines followed by a `PoolTimeout` masking the actual,
    specific, actionable cause underneath. Checking for the same
    condition ourselves, BEFORE calling pool.open(), turns that into an
    immediate, specific error instead of a confusing multi-second stall.
    """
    if sys.version_info >= (3, 12):
        guidance = "asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)"
    else:
        guidance = (
            "asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy()) "
            "before calling asyncio.run(...)"
        )
    return StorageError(
        "AsyncZeroBucket cannot run under Windows' default ProactorEventLoop -- "
        "psycopg3's async mode requires a SelectorEventLoop on Windows. Fix: "
        f"{guidance}. See the README's Async support section for a full example. "
        "(This is a documented psycopg3 limitation on Windows, not specific to "
        "ZeroBucket -- confirmed directly against psycopg's own source.)"
    )


class AsyncPostgresBackend(AsyncStorageBackend):
    """Async storage backend for PostgreSQL using BYTEA columns.

    dedup / object_storage (added in 0.30.0): the same two options as the
    sync PostgresBackend, with the same meaning and the same restriction --
    they cannot be combined (ValueError). dedup=True stores each distinct
    image once and counts references; object_storage= lets
    tier_to_object_storage() move an image's bytes into S3-compatible
    storage and makes get()/get_many()/get_stream() read them back from
    there. Blocking object-storage calls run in worker threads.

    Lazy-initialized by design: __init__ cannot be a coroutine (Python
    has no async __init__), so the connection pool is constructed
    unopened here and opened -- along with running the schema migration,
    if auto_migrate=True -- on the FIRST actual async call, guarded by an
    asyncio.Lock so concurrent first-callers don't race to open/migrate
    twice. This means __init__ itself does no I/O and can never raise a
    connection error; the first real operation can, same as it always
    could for a bad database_url.

    max_retries/retry_base_delay/on_operation (added in 0.27.0): the same
    three knobs, defaults (3 / 0.1 / None), retry classification
    (`postgres._is_retryable`), backoff formula, `OperationEvent` shape
    and operation names as the sync PostgresBackend. A transient failure
    re-runs the WHOLE operation on a freshly acquired pooled connection,
    inside a new transaction, so a half-applied multi-statement
    operation is never continued. There is no `connection=` parameter on
    this adapter, so unlike the sync adapter there is no no-retry path.
    The callback is called synchronously on the event loop thread: keep
    it fast and non-blocking. An exception it raises is swallowed.

    Windows note: psycopg3's async mode cannot run under Windows'
    default ProactorEventLoop (a documented psycopg3 limitation, not
    something specific to this library). _ensure_ready() detects this
    up front and raises a clear, actionable StorageError immediately,
    rather than letting psycopg_pool's background connect worker retry
    silently and time out ~10+ seconds later with a generic PoolTimeout
    that buries the real cause -- that masking behavior was observed
    directly (not assumed) while getting this feature working on
    Windows for the first time. See _windows_proactor_loop_error()'s
    docstring for the full story.
    """

    def __init__(
        self,
        database_url: str,
        *,
        auto_migrate: bool = True,
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        pool_timeout: float = 10,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
        on_operation: Callable[[OperationEvent], None] | None = None,
        dedup: bool = False,
        object_storage: ObjectStorage | None = None,
    ) -> None:
        if object_storage is not None and dedup:
            raise ValueError(
                "object_storage= is not supported together with dedup=True "
                "in this first pass -- see PostgresBackend's docstring."
            )
        self._pool = AsyncConnectionPool(
            database_url,
            min_size=pool_min_size,
            max_size=pool_max_size,
            open=False,
            timeout=pool_timeout,
        )
        self._pool_timeout = pool_timeout
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._on_operation = on_operation
        self._auto_migrate = auto_migrate
        self._dedup = dedup
        self._object_storage = object_storage
        self._ready = False
        self._ready_lock = asyncio.Lock()

    async def _ensure_ready(self) -> None:
        if self._ready:
            return
        async with self._ready_lock:
            if self._ready:  # re-check: another task may have won the race
                return

            if sys.platform == "win32":
                loop = asyncio.get_running_loop()
                if isinstance(loop, asyncio.ProactorEventLoop):
                    raise _windows_proactor_loop_error()

            try:
                await self._pool.open(wait=True, timeout=self._pool_timeout)
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Could not connect to PostgreSQL: {exc}") from exc

            if self._auto_migrate:
                try:

                    schema = _DEDUP_SCHEMA if self._dedup else _SCHEMA

                    async def migrate_work(cur):
                        await cur.execute(schema)

                    await self._attempt(migrate_work, operation="migrate")
                except Exception as exc:  # noqa: BLE001
                    # Don't leak the pool's background resources if setup
                    # fails partway through -- mirrors the sync adapter's
                    # same concern in its own __init__.
                    await self._pool.close()
                    raise StorageError(f"Migration failed: {exc}") from exc

            self._ready = True

    def _emit(
        self,
        operation: str,
        start_time: float,
        success: bool,
        error: str | None,
        retry_count: int,
    ) -> None:
        if self._on_operation is None:
            return
        event = OperationEvent(
            operation=operation,
            duration_seconds=time.monotonic() - start_time,
            success=success,
            error=error,
            retry_count=retry_count,
        )
        try:
            self._on_operation(event)
        except Exception:  # noqa: BLE001
            pass  # never let a metrics callback break a real operation

    async def _attempt(self, work, *, operation: str, start_time: float | None = None):
        """The retry loop, without the lazy-init step (so migration,
        which runs inside `_ensure_ready()`, can use it too).

        Runs `work(cursor)` on a pooled connection; a transient failure
        (see `postgres._is_retryable`) is retried up to `max_retries`
        times with exponential backoff plus jitter, each retry re-running
        `work` in full on a freshly acquired connection and a new
        transaction. Non-transient errors propagate on the first attempt.
        Emits exactly one `OperationEvent` per call, after the final
        attempt; `duration_seconds` covers the whole call including
        backoff delays.
        """
        if start_time is None:
            start_time = time.monotonic()
        attempt = 0
        while True:
            try:
                async with self._pool.connection() as conn:
                    async with conn.cursor() as cur:
                        result = await work(cur)
                self._emit(operation, start_time, True, None, attempt)
                return result
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                if attempt > self._max_retries or not _is_retryable(exc):
                    self._emit(operation, start_time, False, str(exc), attempt - 1)
                    raise
                await asyncio.sleep(_backoff_delay(attempt, self._retry_base_delay))

    async def _run(self, work, *, operation: str):
        """Run `work(cursor)` (an async callable) on a pooled connection
        and return its result, with retry and metrics -- see `_attempt`.
        A failure to open the pool or migrate is reported as a failed
        event for this operation and is never retried."""
        start_time = time.monotonic()
        try:
            await self._ensure_ready()
        except Exception as exc:  # noqa: BLE001
            self._emit(operation, start_time, False, str(exc), 0)
            raise
        return await self._attempt(work, operation=operation, start_time=start_time)

    # ---- put ------------------------------------------------------------

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
        if self._dedup:

            async def work(cur):
                await cur.execute(
                    _DEDUP_UPSERT_BLOB,
                    (checksum_sha256, data, mime_type, size_bytes, width, height),
                )
                await cur.execute(
                    _DEDUP_INSERT_REF, (checksum_sha256, original_filename)
                )
                row = await cur.fetchone()
                return str(row[0])

        else:

            async def work(cur):
                await cur.execute(
                    _INSERT,
                    (
                        data,
                        mime_type,
                        original_filename,
                        size_bytes,
                        width,
                        height,
                        checksum_sha256,
                    ),
                )
                row = await cur.fetchone()
                return str(row[0])

        try:
            return await self._run(work, operation="put")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    async def put_many(self, rows: list[dict]) -> list[str]:
        """See PostgresBackend.put_many's docstring for the ordering
        guarantee (executemany(returning=True), not a multi-row VALUES
        list) -- identical reasoning applies here, same query, same
        driver family, just awaited. In dedup mode blobs are upserted in
        sorted checksum order (a single global lock order, so overlapping
        batches cannot deadlock) and references are inserted in the
        caller's order, so the returned ids line up with `rows`."""
        if not rows:
            return []

        if self._dedup:

            async def work(cur):
                blob_params = sorted(
                    (
                        (
                            row["checksum_sha256"],
                            row["data"],
                            row["mime_type"],
                            row["size_bytes"],
                            row["width"],
                            row["height"],
                        )
                        for row in rows
                    ),
                    key=lambda params: params[0],
                )
                await cur.executemany(_DEDUP_UPSERT_BLOB, blob_params)
                ref_params = [
                    (row["checksum_sha256"], row["original_filename"]) for row in rows
                ]
                await cur.executemany(_DEDUP_INSERT_REF, ref_params, returning=True)
                ids = [str((await cur.fetchone())[0])]
                while cur.nextset():
                    ids.append(str((await cur.fetchone())[0]))
                return ids

        else:

            async def work(cur):
                params_seq = [
                    (
                        row["data"],
                        row["mime_type"],
                        row["original_filename"],
                        row["size_bytes"],
                        row["width"],
                        row["height"],
                        row["checksum_sha256"],
                    )
                    for row in rows
                ]
                await cur.executemany(_INSERT, params_seq, returning=True)
                ids = [str((await cur.fetchone())[0])]
                while cur.nextset():
                    ids.append(str((await cur.fetchone())[0]))
                return ids

        try:
            return await self._run(work, operation="put_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    # ---- get --------------------------------------------------------------

    async def _fetch_tiered_bytes(
        self, object_storage_key: str, *, image_id: str
    ) -> bytes:
        """The row says this image lives in object storage: fetch the bytes.
        Same error as the sync adapter if no ObjectStorage was configured.
        The boto3 download is blocking, so it runs in a worker thread. Like
        the sync adapter, this round trip is not covered by retry or by its
        own OperationEvent."""
        if self._object_storage is None:
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) but this AsyncPostgresBackend was "
                "constructed without object_storage=... -- configure it "
                "with the same bucket/credentials used to tier this image."
            )
        return await asyncio.to_thread(
            self._object_storage.download, object_storage_key
        )

    async def get(self, image_id: str) -> StoredRecord | None:
        if not is_valid_id(image_id):
            return None  # can never exist; no database round trip
        select_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL

        async def work(cur):
            await cur.execute(select_sql, (image_id,))
            return await cur.fetchone()

        try:
            row = await self._run(work, operation="get")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image: {exc}") from exc
        if row is None:
            return None
        data = row[1]
        if not self._dedup and row[8] == "object_storage":
            data = await self._fetch_tiered_bytes(row[10], image_id=image_id)
        return StoredRecord(
            id=str(row[0]),
            data=bytes(data),
            mime_type=row[2],
            original_filename=row[3],
            size_bytes=row[4],
            width=row[5],
            height=row[6],
            checksum_sha256=row[7],
        )

    async def get_many(self, image_ids: list[str]) -> list[StoredRecord]:
        """Order of results is NOT guaranteed to match `image_ids` -- same
        caveat as the sync adapter; async_client.py re-correlates by id.
        Tiered rows are fetched from object storage one at a time after the
        single database query, as in the sync adapter."""
        # Malformed ids can never exist: drop them rather than let the whole
        # batch fail with an invalid-uuid error.
        image_ids = [i for i in image_ids if is_valid_id(i)]
        if not image_ids:
            return []

        if self._dedup:
            select_many_sql = _DEDUP_SELECT_FULL.replace(
                "WHERE r.id = %s", "WHERE r.id = ANY(%s)"
            )
        else:
            select_many_sql = _SELECT_FULL.replace(
                "WHERE id = %s", "WHERE id = ANY(%s)"
            )

        async def work(cur):
            await cur.execute(select_many_sql, (image_ids,))
            return await cur.fetchall()

        try:
            rows = await self._run(work, operation="get_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image batch: {exc}") from exc
        records = []
        for row in rows:
            data = row[1]
            if not self._dedup and row[8] == "object_storage":
                data = await self._fetch_tiered_bytes(row[10], image_id=str(row[0]))
            records.append(
                StoredRecord(
                    id=str(row[0]),
                    data=bytes(data),
                    mime_type=row[2],
                    original_filename=row[3],
                    size_bytes=row[4],
                    width=row[5],
                    height=row[6],
                    checksum_sha256=row[7],
                )
            )
        return records

    async def get_metadata(self, image_id: str) -> StoredRecordMetadata | None:
        if not is_valid_id(image_id):
            return None
        select_sql = _DEDUP_SELECT_METADATA if self._dedup else _SELECT_METADATA

        async def work(cur):
            await cur.execute(select_sql, (image_id,))
            return await cur.fetchone()

        try:
            row = await self._run(work, operation="get_metadata")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
        if row is None:
            return None
        return StoredRecordMetadata(
            id=str(row[0]),
            mime_type=row[1],
            original_filename=row[2],
            size_bytes=row[3],
            width=row[4],
            height=row[5],
            checksum_sha256=row[6],
        )

    # ---- get_stream -----------------------------------------------------

    async def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
    ) -> AsyncIterator[bytes] | None:
        """See AsyncStorageBackend.get_stream and the sync adapter's
        get_stream for the full contract -- same semantics, awaited:
        one metadata lookup to learn size_bytes (also the not-found
        check), then repeated ranged substring() queries. A row that
        disappears mid-stream (concurrent delete, no held transaction)
        raises StorageError rather than silently truncating.

        A TIERED row is streamed from object storage with real HTTP byte-
        range requests instead (see ObjectStorage.download_stream()); each
        blocking chunk read runs in a worker thread."""
        if self._dedup:
            metadata = await self.get_metadata(image_id)
            if metadata is None:
                return None
            total_size = metadata.size_bytes
            select_chunk_sql = _DEDUP_SELECT_CHUNK
        else:
            if not is_valid_id(image_id):
                return None

            async def info_work(cur):
                await cur.execute(_SELECT_STREAM_INFO, (image_id,))
                return await cur.fetchone()

            try:
                info_row = await self._run(info_work, operation="get_metadata")
            except StorageError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
            if info_row is None:
                return None
            total_size, storage_backend, object_storage_key = info_row

            if storage_backend == "object_storage":
                if self._object_storage is None:
                    raise StorageError(
                        f"Image {image_id!r} is stored in object storage (key="
                        f"{object_storage_key!r}) but this backend was "
                        "constructed without object_storage=... -- configure "
                        "it with the same bucket/credentials used to tier "
                        "this image."
                    )
                return self._stream_tiered(object_storage_key, chunk_size)
            select_chunk_sql = _SELECT_CHUNK

        async def generator() -> AsyncIterator[bytes]:
            offset = 1  # substring() is 1-indexed
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                async def work(cur, offset=offset, length=length):
                    await cur.execute(select_chunk_sql, (offset, length, image_id))
                    return await cur.fetchone()

                try:
                    row = await self._run(work, operation="get_stream")
                except Exception as exc:  # noqa: BLE001
                    raise StorageError(f"Failed to stream image: {exc}") from exc

                if row is None:
                    raise StorageError(
                        f"Image {image_id!r} was deleted while streaming "
                        f"(delivered {delivered} of {total_size} bytes)."
                    )

                chunk = bytes(row[0])
                yield chunk
                offset += len(chunk)
                remaining -= len(chunk)
                delivered += len(chunk)

        return generator()

    async def _stream_tiered(
        self, object_storage_key: str, chunk_size: int
    ) -> AsyncIterator[bytes]:
        """Adapt ObjectStorage.download_stream() (a blocking generator) to an
        async iterator: each next() runs in a worker thread."""
        iterator = self._object_storage.download_stream(  # type: ignore[union-attr]
            object_storage_key, chunk_size=chunk_size
        )
        done = object()
        try:
            while True:
                chunk = await asyncio.to_thread(next, iterator, done)
                if chunk is done:
                    break
                yield chunk
        finally:
            iterator.close()

    # ---- tier_to_object_storage ------------------------------------------

    async def tier_to_object_storage(self, image_id: str) -> bool | None:
        """Move an image's bytes out of PostgreSQL into object storage.
        Same contract and same safety as PostgresBackend.tier_to_object_
        storage(): None if the id doesn't exist, False if it was already
        tiered (a safe no-op), True if tiered just now. The upload happens
        INSIDE the transaction that locked the row (SELECT ... FOR UPDATE),
        so a failed upload rolls everything back and the row is untouched.
        The same tradeoff applies: the row stays locked for the duration of
        the upload, so a concurrent delete() or tier of THE SAME image
        waits. Plain reads are not blocked. A retry re-uploads the same
        bytes to the same deterministic key, which is harmless."""
        if self._dedup:
            raise StorageError(
                "tier_to_object_storage() is not supported in dedup mode "
                "in this first pass -- see PostgresBackend's docstring."
            )
        if self._object_storage is None:
            raise StorageError(
                "tier_to_object_storage() requires this backend to be "
                "constructed with object_storage=... -- see ObjectStorage "
                "in object_storage.py."
            )
        if not is_valid_id(image_id):
            return None  # can never exist -> same as a missing id

        object_storage = self._object_storage

        async def work(cur):
            await cur.execute(_SELECT_FOR_TIERING, (image_id,))
            row = await cur.fetchone()
            if row is None:
                return None
            data, mime_type, _size_bytes, storage_backend = row
            if storage_backend != "postgres":
                return False
            key = str(image_id)
            await asyncio.to_thread(
                object_storage.upload, key, bytes(data), mime_type=mime_type
            )
            await cur.execute(
                _UPDATE_AFTER_TIERING, (object_storage.bucket, key, image_id)
            )
            return True

        try:
            return await self._run(work, operation="tier_to_object_storage")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to tier image to object storage: {exc}"
            ) from exc

    # ---- delete -------------------------------------------------------------

    async def delete(self, image_id: str) -> bool:
        if not is_valid_id(image_id):
            return False

        if self._dedup:

            async def work(cur):
                await cur.execute(_DEDUP_DELETE_REF, (image_id,))
                row = await cur.fetchone()
                if row is None:
                    return None  # id didn't exist -- nothing to decrement either
                checksum = row[0]
                await cur.execute(_DEDUP_DECREMENT_BLOB, (1, checksum))
                new_ref_count = (await cur.fetchone())[0]
                if new_ref_count <= 0:
                    await cur.execute(_DEDUP_DELETE_EMPTY_BLOBS, ([checksum],))
                return ("postgres", None)

        else:

            async def work(cur):
                await cur.execute(_DELETE_RETURNING, (image_id,))
                return await cur.fetchone()

        try:
            row = await self._run(work, operation="delete")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc
        if row is None:
            return False
        storage_backend, object_storage_key = row
        if storage_backend == "object_storage" and self._object_storage is not None:
            # After the row is gone, and a failure NEVER fails the delete
            # (see StorageBackend._delete_tiered_object, 0.29.0).
            await self._delete_tiered_object(object_storage_key)
        return True

    async def delete_many(self, image_ids: list[str]) -> list[str]:
        image_ids = [i for i in image_ids if is_valid_id(i)]
        if not image_ids:
            return []

        if self._dedup:
            return await self._delete_many_dedup(image_ids)

        async def work(cur):
            await cur.execute(
                "DELETE FROM zerobucket_images WHERE id = ANY(%s) "
                "RETURNING id, storage_backend, object_storage_key;",
                (image_ids,),
            )
            return await cur.fetchall()

        try:
            rows = await self._run(work, operation="delete_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc
        deleted_ids = []
        for image_id, storage_backend, object_storage_key in rows:
            deleted_ids.append(str(image_id))
            if storage_backend == "object_storage" and self._object_storage is not None:
                await self._delete_tiered_object(object_storage_key)
        return deleted_ids

    async def _delete_many_dedup(self, image_ids: list[str]) -> list[str]:
        async def work(cur):
            await cur.execute(
                "DELETE FROM zerobucket_image_refs WHERE id = ANY(%s) "
                "RETURNING id, checksum_sha256;",
                (image_ids,),
            )
            deleted_rows = await cur.fetchall()
            if not deleted_rows:
                return []
            deleted_ids = [str(row[0]) for row in deleted_rows]
            # Several deleted refs can share one blob: decrement each blob
            # by how many of its refs were deleted, in sorted checksum order
            # (the same lock order as put_many, so overlapping batches
            # cannot deadlock).
            checksum_counts = Counter(row[1] for row in deleted_rows)
            emptied = []
            for checksum, count in sorted(checksum_counts.items()):
                await cur.execute(_DEDUP_DECREMENT_BLOB, (count, checksum))
                if (await cur.fetchone())[0] <= 0:
                    emptied.append(checksum)
            if emptied:
                await cur.execute(_DEDUP_DELETE_EMPTY_BLOBS, (emptied,))
            return deleted_ids

        try:
            return await self._run(work, operation="delete_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    # ---- exists -------------------------------------------------------------

    async def exists(self, image_id: str) -> bool:
        if not is_valid_id(image_id):
            return False
        exists_sql = _DEDUP_EXISTS if self._dedup else _EXISTS

        async def work(cur):
            await cur.execute(exists_sql, (image_id,))
            return (await cur.fetchone()) is not None

        try:
            return await self._run(work, operation="exists")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    async def close(self) -> None:
        await self._pool.close()
