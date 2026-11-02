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
of executing them. This is CLASSIC MODE ONLY for this first pass (no
dedup=True support yet -- see AsyncZeroBucket's docstring for the full
list of what's deferred and why).
"""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator, Callable

from psycopg_pool import AsyncConnectionPool

from .._ids import is_valid_id
from ..exceptions import StorageError
from .base import StoredRecord, StoredRecordMetadata
from .base_async import AsyncStorageBackend
from .postgres import (
    _DELETE,
    _EXISTS,
    _INSERT,
    _SCHEMA,
    _SELECT_CHUNK,
    _SELECT_FULL,
    _SELECT_METADATA,
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
    """Async storage backend for PostgreSQL using BYTEA columns, classic
    (non-dedup) mode only.

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
    ) -> None:
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

                    async def migrate_work(cur):
                        await cur.execute(_SCHEMA)

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
        driver family, just awaited."""
        if not rows:
            return []

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

    async def get(self, image_id: str) -> StoredRecord | None:
        if not is_valid_id(image_id):
            return None  # can never exist; no database round trip

        async def work(cur):
            await cur.execute(_SELECT_FULL, (image_id,))
            return await cur.fetchone()

        try:
            row = await self._run(work, operation="get")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image: {exc}") from exc
        if row is None:
            return None
        return StoredRecord(
            id=str(row[0]),
            data=bytes(row[1]),
            mime_type=row[2],
            original_filename=row[3],
            size_bytes=row[4],
            width=row[5],
            height=row[6],
            checksum_sha256=row[7],
        )

    async def get_many(self, image_ids: list[str]) -> list[StoredRecord]:
        """Order of results is NOT guaranteed to match `image_ids` -- same
        caveat as the sync adapter; async_client.py re-correlates by id."""
        # Malformed ids can never exist: drop them rather than let the whole
        # batch fail with an invalid-uuid error.
        image_ids = [i for i in image_ids if is_valid_id(i)]
        if not image_ids:
            return []

        select_many_sql = _SELECT_FULL.replace("WHERE id = %s", "WHERE id = ANY(%s)")

        async def work(cur):
            await cur.execute(select_many_sql, (image_ids,))
            return await cur.fetchall()

        try:
            rows = await self._run(work, operation="get_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image batch: {exc}") from exc
        return [
            StoredRecord(
                id=str(row[0]),
                data=bytes(row[1]),
                mime_type=row[2],
                original_filename=row[3],
                size_bytes=row[4],
                width=row[5],
                height=row[6],
                checksum_sha256=row[7],
            )
            for row in rows
        ]

    async def get_metadata(self, image_id: str) -> StoredRecordMetadata | None:
        if not is_valid_id(image_id):
            return None

        async def work(cur):
            await cur.execute(_SELECT_METADATA, (image_id,))
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
        raises StorageError rather than silently truncating."""
        metadata = await self.get_metadata(image_id)
        if metadata is None:
            return None

        total_size = metadata.size_bytes

        async def generator() -> AsyncIterator[bytes]:
            offset = 1  # substring() is 1-indexed
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                async def work(cur, offset=offset, length=length):
                    await cur.execute(_SELECT_CHUNK, (offset, length, image_id))
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

    # ---- delete -------------------------------------------------------------

    async def delete(self, image_id: str) -> bool:
        if not is_valid_id(image_id):
            return False

        async def work(cur):
            await cur.execute(_DELETE, (image_id,))
            return cur.rowcount > 0

        try:
            return await self._run(work, operation="delete")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    async def delete_many(self, image_ids: list[str]) -> list[str]:
        image_ids = [i for i in image_ids if is_valid_id(i)]
        if not image_ids:
            return []

        async def work(cur):
            await cur.execute(
                "DELETE FROM zerobucket_images WHERE id = ANY(%s) RETURNING id;",
                (image_ids,),
            )
            return [str(row[0]) for row in await cur.fetchall()]

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

        async def work(cur):
            await cur.execute(_EXISTS, (image_id,))
            return (await cur.fetchone()) is not None

        try:
            return await self._run(work, operation="exists")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    async def close(self) -> None:
        await self._pool.close()
