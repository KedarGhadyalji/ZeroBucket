"""Async SQLite storage adapter, using `aiosqlite`.

Scope matches `AsyncPostgresBackend`'s: core CRUD + streaming, and --
since 0.30.0 -- `dedup=True` (content-addressed storage) and
`object_storage=` tiering with `tier_to_object_storage()`, same as the
sync adapter and, as there, not both at once. (The `on_operation` metrics
hook and busy-retry were added in 0.27.0.) The blocking object-storage SDK
runs in worker threads (`asyncio.to_thread`), never on the event loop.

ONE REAL, VERIFIED DRIVER CONSTRAINT THAT SHAPES THIS FILE'S DESIGN:
`aiosqlite.Connection` does NOT expose `blobopen()` -- confirmed
directly by inspecting `aiosqlite.Connection`'s actual method list, not
assumed to exist because the sync `sqlite3.Connection` has it. The sync
`SQLiteBackend.get_stream()` is built entirely on `blobopen()`'s
incremental-BLOB-I/O API; that approach is simply unavailable here.
Instead, this adapter's `get_stream()` uses SQLite's `substr()`
function in repeated ranged queries -- the SAME strategy
`AsyncPostgresBackend.get_stream()` already uses (via Postgres's
`substring()`), verified to work correctly and identically on SQLite
(1-indexed, clamps at the value's actual end) before relying on it.

A REAL, WORTH-KNOWING CONSEQUENCE OF THAT DIFFERENCE: this means async
and sync `get_stream()` behave DIFFERENTLY from each other on SQLite
for concurrent deletion mid-stream -- not just different from Postgres.
Sync `SQLiteBackend.get_stream()` holds one connection/blob handle open
for the whole stream and, thanks to WAL-mode snapshot isolation,
SURVIVES a concurrent delete (see that method's docstring). This async
version issues a SEPARATE query per chunk (like the Postgres async
adapter does), so a concurrent delete mid-stream causes the next
chunk's query to see no row and RAISE StorageError -- matching Postgres's
behavior, not sync SQLite's. Three different backends, two different
verified behaviors for the same scenario, not one universal answer --
stated directly rather than left to be discovered.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from ..exceptions import StorageError
from ..object_storage import ObjectStorage
from .base import StoredRecord, StoredRecordMetadata
from .base_async import AsyncStorageBackend
from .postgres import OperationEvent, _backoff_delay
from .sqlite import (
    _DEDUP_DECREMENT_BLOB,
    _DEDUP_DELETE_REF,
    _DEDUP_EXISTS,
    _DEDUP_INSERT_REF,
    _DEDUP_SCHEMA,
    _DEDUP_SELECT_FULL,
    _DEDUP_SELECT_METADATA,
    _DEDUP_UPSERT_BLOB,
    _DELETE_RETURNING,
    _SCHEMA,
    _SELECT_FOR_TIERING,
    _SELECT_FULL,
    _UPDATE_AFTER_TIERING,
    DEFAULT_STREAM_CHUNK_SIZE,
    _is_retryable,
)

if TYPE_CHECKING:
    import aiosqlite

__all__ = ["AsyncSQLiteBackend", "DEFAULT_STREAM_CHUNK_SIZE"]

_INSERT = """
INSERT INTO zerobucket_images
    (id, data, mime_type, original_filename, size_bytes, width, height,
     checksum_sha256, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = ?;
"""

_SELECT_STREAM_INFO = (
    "SELECT size_bytes, storage_backend, object_storage_key "
    "FROM zerobucket_images WHERE id = ?;"
)

_SELECT_CHUNK = "SELECT substr(data, ?, ?) FROM zerobucket_images WHERE id = ?;"

_DEDUP_SELECT_CHUNK = """
SELECT substr(b.data, ?, ?)
FROM zerobucket_image_refs r
JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256
WHERE r.id = ?;
"""

_EXISTS = "SELECT 1 FROM zerobucket_images WHERE id = ?;"

# 10 seconds. How long a connection will wait for a lock held by
# another connection before giving up with "database is locked" --
# see AsyncSQLiteBackend._connect()'s docstring for why this is set at
# two layers (connect()'s own timeout= AND an explicit PRAGMA), and
# why it matters more here than it might for the sync adapter (many
# concurrent connections, each on its own aiosqlite background thread,
# racing to open against the same file).
_BUSY_TIMEOUT_SECONDS = 10.0
_BUSY_TIMEOUT_MS = int(_BUSY_TIMEOUT_SECONDS * 1000)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class AsyncSQLiteBackend(AsyncStorageBackend):
    """Async storage backend for SQLite. See module docstring for exact
    scope and the two real driver-shaped design differences from both the
    sync SQLite adapter and the async Postgres adapter.

    dedup / object_storage (added in 0.30.0): the same two options as the
    sync SQLiteBackend, with the same meaning and restriction -- they
    cannot be combined (ValueError). tier_to_object_storage() takes
    SQLite's single write lock (BEGIN IMMEDIATE) for the whole upload, so
    other WRITERS to this database file wait until it finishes, exactly as
    in the sync adapter; readers are not blocked (WAL mode).

    Lazy-initialized the same way `AsyncPostgresBackend` is, and for the
    same underlying reason (`__init__` cannot be a coroutine): schema
    migration is deferred to the first real async call, guarded by an
    `asyncio.Lock` so concurrent first-callers can't race to migrate
    twice.

    Same "no connection pool" design as the sync adapter, for the same
    reason: a SQLite connection is a handle to a local file, not a
    network resource, so there's no pooling benefit to chase -- a fresh
    `aiosqlite` connection is opened and closed per operation.

    Every connection this backend opens sets an explicit busy timeout
    (`timeout=` on connect, plus `PRAGMA busy_timeout` as a second,
    belt-and-suspenders layer -- see `_connect()`'s docstring for why
    both). WAL mode is set exactly ONCE, in `_ensure_ready()`, NOT
    per-connection -- see that method's docstring for why re-issuing
    the WAL-mode pragma on every connection was itself the actual
    cause of a real, observed-on-Windows lock-contention bug (20
    concurrent first-call connections racing to perform that
    file-level conversion simultaneously), not just redundant work. An
    explicit busy_timeout alone was tried first and was NOT sufficient
    to fix it -- confirmed by a second real Windows test run showing
    the same failure, slower, not assumed fixed from reasoning alone.
    """

    def __init__(
        self,
        database_path: str,
        *,
        auto_migrate: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
        on_operation: Callable[[OperationEvent], None] | None = None,
        dedup: bool = False,
        object_storage: ObjectStorage | None = None,
    ) -> None:
        if object_storage is not None and dedup:
            raise ValueError(
                "object_storage= is not supported together with dedup=True "
                "in this first pass -- see SQLiteBackend's docstring."
            )
        try:
            import aiosqlite
        except ImportError as exc:
            raise ImportError(
                "Async SQLite support requires aiosqlite. Install it with "
                "`pip install zerobucket[sqlite-async]` (or `pip install "
                "aiosqlite` directly). Plain `import zerobucket` and the "
                "SYNC SQLiteBackend never require this -- only "
                "AsyncSQLiteBackend does."
            ) from exc
        self._aiosqlite = aiosqlite
        self._database_path = database_path
        self._auto_migrate = auto_migrate
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._on_operation = on_operation
        self._dedup = dedup
        self._object_storage = object_storage
        self._ready = False
        self._ready_lock = asyncio.Lock()

    async def _ensure_ready(self) -> None:
        """Also where WAL mode gets set -- ALWAYS, regardless of
        auto_migrate, not just when creating the schema. This is the
        real fix for the Windows lock-contention bug (see class
        docstring): WAL mode is a durable, file-level setting, not a
        per-connection one -- once set, every later connection opened
        against this file is already in WAL mode with no further work
        needed. The original design re-issued `PRAGMA journal_mode=WAL`
        on every single connection in `_connect()`, which is not just
        redundant but was the actual source of the contention: 20
        connections all racing to perform that conversion simultaneously
        against a brand-new file is a far worse contention pattern than
        ordinary reads/writes, and an explicit busy_timeout alone (the
        first fix attempted) was not sufficient to reliably resolve it
        on Windows -- confirmed by a second real Windows test run after
        that first fix, not assumed fixed from reasoning alone. Setting
        it exactly once, here, under the same lock that already
        serializes migration, removes that contention pattern entirely
        rather than just making it wait longer before failing.
        """
        if self._ready:
            return
        async with self._ready_lock:
            if self._ready:
                return

            async def migrate_attempt() -> None:
                async with self._aiosqlite.connect(
                    self._database_path, timeout=_BUSY_TIMEOUT_SECONDS
                ) as conn:
                    await conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS};")
                    await conn.execute("PRAGMA journal_mode=WAL;")
                    if self._auto_migrate:
                        # Idempotent, so safe to re-run after a retry.
                        await conn.executescript(
                            _DEDUP_SCHEMA if self._dedup else _SCHEMA
                        )
                    await conn.commit()

            try:
                await self._with_retry(migrate_attempt, operation="migrate")
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Migration failed: {exc}") from exc
            self._ready = True

    async def _connect(self) -> aiosqlite.Connection:
        """Busy timeout only, deliberately at two layers -- `timeout=`
        on connect() sets sqlite3's own Python-level busy handler (a
        retry loop it runs internally before giving up and raising
        `OperationalError: database is locked`); `PRAGMA busy_timeout`
        sets the equivalent at the SQLite C-library level. They should
        agree, but real-world SQLite driver/OS combinations don't always
        behave identically to how they behave on the platform a change
        was developed and tested on -- setting both is cheap insurance,
        not caution for its own sake.

        Does NOT set journal_mode here -- that's done exactly once, in
        `_ensure_ready()`, not per connection. See that method's
        docstring for why re-issuing it here was the actual bug, not
        just redundant work.
        """
        conn = await self._aiosqlite.connect(
            self._database_path, timeout=_BUSY_TIMEOUT_SECONDS
        )
        await conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS};")
        return conn

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

    async def _with_retry(
        self, attempt_fn, *, operation: str, start_time: float | None = None
    ):
        """Runs `attempt_fn()` (one complete attempt, including opening
        its own connection), retrying lock contention (see
        `sqlite._is_retryable`) up to max_retries times with exponential
        backoff plus jitter. Emits one OperationEvent after the final
        attempt. SQLite already waits up to 10 s per attempt inside the
        engine, so a genuinely stuck lock can take up to
        (max_retries + 1) x 10 s to fail; max_retries=0 keeps the old
        single wait."""
        if start_time is None:
            start_time = time.monotonic()
        attempt = 0
        while True:
            try:
                result = await attempt_fn()
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                if attempt > self._max_retries or not _is_retryable(exc):
                    self._emit(operation, start_time, False, str(exc), attempt - 1)
                    raise
                await asyncio.sleep(_backoff_delay(attempt, self._retry_base_delay))
            else:
                self._emit(operation, start_time, True, None, attempt)
                return result

    async def _run(self, work, *, operation: str, connection: object | None = None):
        """Run `work(conn)` and return its result.

        connection provided (0.33.0) -> run ONCE on that exact aiosqlite
        Connection, never commit, never retry: the caller owns the
        transaction. Still emits an OperationEvent (retry_count always 0).
        Same contract as the sync adapter's `_run()`.

        connection is None -> a fresh connection per attempt, committed on
        success, with lock-contention retry (see `_with_retry`).
        """
        start_time = time.monotonic()
        try:
            await self._ensure_ready()
        except Exception as exc:  # noqa: BLE001
            self._emit(operation, start_time, False, str(exc), 0)
            raise

        if connection is not None:
            try:
                result = await work(connection)
            except Exception as exc:  # noqa: BLE001
                self._emit(operation, start_time, False, str(exc), 0)
                raise
            self._emit(operation, start_time, True, None, 0)
            return result

        async def attempt_fn():
            conn = await self._connect()
            try:
                result = await work(conn)
                await conn.commit()
                return result
            except Exception:
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                raise
            finally:
                await conn.close()

        return await self._with_retry(
            attempt_fn, operation=operation, start_time=start_time
        )

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
        connection: object | None = None,
    ) -> str:
        image_id = str(uuid.uuid4())
        now = _now_iso()

        if self._dedup:

            async def work(conn):
                await conn.execute(
                    _DEDUP_UPSERT_BLOB,
                    (checksum_sha256, data, mime_type, size_bytes, width, height, now),
                )
                cur = await conn.execute(
                    _DEDUP_INSERT_REF,
                    (image_id, checksum_sha256, original_filename, now, now),
                )
                row = await cur.fetchone()  # drain: aiosqlite won't commit otherwise
                return str(row[0])

        else:

            async def work(conn):
                cur = await conn.execute(
                    _INSERT,
                    (
                        image_id,
                        data,
                        mime_type,
                        original_filename,
                        size_bytes,
                        width,
                        height,
                        checksum_sha256,
                        now,
                        now,
                    ),
                )
                row = await cur.fetchone()
                return str(row[0])

        try:
            return await self._run(work, connection=connection, operation="put")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    async def put_many(
        self, rows: list[dict], *, connection: object | None = None
    ) -> list[str]:
        if not rows:
            return []
        now = _now_iso()
        prepared = [(str(uuid.uuid4()), row) for row in rows]

        async def work(conn):
            ids = []
            for image_id, row in prepared:
                if self._dedup:
                    # Repeated identical checksums within one batch correctly
                    # accumulate ref_count: each upsert is a separate
                    # sequential statement in the same transaction.
                    await conn.execute(
                        _DEDUP_UPSERT_BLOB,
                        (
                            row["checksum_sha256"],
                            row["data"],
                            row["mime_type"],
                            row["size_bytes"],
                            row["width"],
                            row["height"],
                            now,
                        ),
                    )
                    cur = await conn.execute(
                        _DEDUP_INSERT_REF,
                        (
                            image_id,
                            row["checksum_sha256"],
                            row["original_filename"],
                            now,
                            now,
                        ),
                    )
                else:
                    cur = await conn.execute(
                        _INSERT,
                        (
                            image_id,
                            row["data"],
                            row["mime_type"],
                            row["original_filename"],
                            row["size_bytes"],
                            row["width"],
                            row["height"],
                            row["checksum_sha256"],
                            now,
                            now,
                        ),
                    )
                # aiosqlite (unlike the sync sqlite3 driver -- verified, not
                # assumed to match) refuses to commit() while a RETURNING
                # cursor's result set hasn't been drained, raising "cannot
                # commit transaction - SQL statements in progress".
                await cur.fetchone()
                ids.append(image_id)
            return ids

        try:
            return await self._run(work, connection=connection, operation="put_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    # ---- get --------------------------------------------------------------

    async def _fetch_tiered_bytes(
        self, object_storage_key: str, *, image_id: str
    ) -> bytes:
        """The row says this image lives in object storage: fetch the bytes
        (the blocking boto3 download runs in a worker thread)."""
        if self._object_storage is None:
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) but this backend was constructed "
                "without object_storage=... -- configure it with the same "
                "bucket/credentials used to tier this image."
            )
        return await asyncio.to_thread(
            self._object_storage.download, object_storage_key
        )

    async def get(
        self, image_id: str, *, connection: object | None = None
    ) -> StoredRecord | None:
        select_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL

        async def work(conn):
            cur = await conn.execute(select_sql, (image_id,))
            return await cur.fetchone()

        try:
            row = await self._run(work, connection=connection, operation="get")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image: {exc}") from exc
        if row is None:
            return None
        data = row[1]
        if not self._dedup and row[8] == "object_storage":
            data = await self._fetch_tiered_bytes(row[9], image_id=image_id)
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

    async def get_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[StoredRecord]:
        if not image_ids:
            return []
        placeholders = ",".join("?" * len(image_ids))
        base_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL
        where = "WHERE r.id = ?" if self._dedup else "WHERE id = ?"
        column = "r.id" if self._dedup else "id"
        sql = base_sql.replace(where, f"WHERE {column} IN ({placeholders})")

        async def work(conn):
            cur = await conn.execute(sql, image_ids)
            return await cur.fetchall()

        try:
            rows = await self._run(work, connection=connection, operation="get_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image batch: {exc}") from exc
        records = []
        for row in rows:
            data = row[1]
            if not self._dedup and row[8] == "object_storage":
                data = await self._fetch_tiered_bytes(row[9], image_id=str(row[0]))
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

    async def get_metadata(
        self, image_id: str, *, connection: object | None = None
    ) -> StoredRecordMetadata | None:
        select_sql = _DEDUP_SELECT_METADATA if self._dedup else _SELECT_METADATA

        async def work(conn):
            cur = await conn.execute(select_sql, (image_id,))
            return await cur.fetchone()

        try:
            row = await self._run(work, connection=connection, operation="get_metadata")
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
        connection: object | None = None,
    ) -> AsyncIterator[bytes] | None:
        """See module docstring: built on repeated `substr()` range
        queries (like AsyncPostgresBackend), NOT `blobopen()` (which
        aiosqlite doesn't expose) -- and as a real consequence, this
        does NOT share sync SQLiteBackend's "survives a concurrent
        delete mid-stream" guarantee. A row deleted between chunks here
        raises StorageError, matching AsyncPostgresBackend's behavior
        instead.

        A TIERED row is streamed from object storage with real HTTP byte-
        range requests (ObjectStorage.download_stream()); each blocking
        chunk read runs in a worker thread."""
        if self._dedup:
            info_sql = _DEDUP_SELECT_METADATA
            select_chunk_sql = _DEDUP_SELECT_CHUNK
        else:
            info_sql = _SELECT_STREAM_INFO
            select_chunk_sql = _SELECT_CHUNK

        async def info_work(conn):
            cur = await conn.execute(info_sql, (image_id,))
            return await cur.fetchone()

        try:
            info_row = await self._run(
                info_work, connection=connection, operation="get_stream"
            )
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
        if info_row is None:
            return None
        if self._dedup:
            total_size = info_row[3]  # _DEDUP_SELECT_METADATA: size_bytes is column 3
        else:
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

        async def generator() -> AsyncIterator[bytes]:
            offset = 1  # substr() is 1-indexed, same as Postgres's substring()
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                async def work(conn, offset=offset, length=length):
                    cur = await conn.execute(
                        select_chunk_sql, (offset, length, image_id)
                    )
                    return await cur.fetchone()

                try:
                    row = await self._run(
                        work, connection=connection, operation="get_stream"
                    )
                except Exception as exc:  # noqa: BLE001
                    raise StorageError(f"Failed to stream image: {exc}") from exc

                if row is None or row[0] is None:
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

    async def tier_to_object_storage(
        self, image_id: str, *, connection: object | None = None
    ) -> bool | None:
        """Move an image's bytes out of SQLite into object storage. Same
        contract and safety as SQLiteBackend.tier_to_object_storage(): None
        if the id doesn't exist, False if already tiered (a safe no-op),
        True if tiered just now. The upload happens INSIDE a BEGIN
        IMMEDIATE transaction, so a failed upload rolls back and the row is
        untouched. TRADEOFF, as in the sync adapter: BEGIN IMMEDIATE takes
        SQLite's single write lock for the whole upload, so every other
        WRITER to this database file waits; readers are not blocked. Like
        the sync adapter this opens its own connection (so it is not
        covered by retry or by an OperationEvent) unless you pass
        `connection=`: then it uses yours, which must have been opened with
        `isolation_level=None` (autocommit mode) so the explicit BEGIN
        IMMEDIATE / COMMIT below can be issued; it COMMITS (or rolls back)
        its own transaction on it, exactly as the sync adapter does."""
        if self._dedup:
            raise StorageError(
                "tier_to_object_storage() is not supported in dedup mode "
                "in this first pass -- see SQLiteBackend's docstring."
            )
        if self._object_storage is None:
            raise StorageError(
                "tier_to_object_storage() requires this backend to be "
                "constructed with object_storage=... -- see ObjectStorage "
                "in object_storage.py."
            )
        await self._ensure_ready()
        store = self._object_storage

        owns_connection = connection is None
        try:
            if owns_connection:
                # isolation_level=None: we issue BEGIN/COMMIT/ROLLBACK ourselves.
                conn = await self._aiosqlite.connect(
                    self._database_path,
                    timeout=_BUSY_TIMEOUT_SECONDS,
                    isolation_level=None,
                )
            else:
                conn = connection
            try:
                if owns_connection:
                    await conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS};")
                await conn.execute("BEGIN IMMEDIATE;")
                try:
                    cur = await conn.execute(_SELECT_FOR_TIERING, (image_id,))
                    row = await cur.fetchone()
                    if row is None:
                        await conn.execute("ROLLBACK;")
                        return None
                    data, mime_type, _size_bytes, storage_backend = row
                    if storage_backend != "sqlite":
                        await conn.execute("ROLLBACK;")
                        return False
                    key = str(image_id)
                    await asyncio.to_thread(
                        store.upload, key, bytes(data), mime_type=mime_type
                    )
                    await conn.execute(
                        _UPDATE_AFTER_TIERING, (store.bucket, key, _now_iso(), image_id)
                    )
                    await conn.execute("COMMIT;")
                    return True
                except Exception:
                    await conn.execute("ROLLBACK;")
                    raise
            finally:
                if owns_connection:
                    await conn.close()
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to tier image to object storage: {exc}"
            ) from exc

    # ---- delete -------------------------------------------------------------

    async def delete(self, image_id: str, *, connection: object | None = None) -> bool:
        if self._dedup:

            async def work(conn):
                cur = await conn.execute(_DEDUP_DELETE_REF, (image_id,))
                row = await cur.fetchone()
                if row is None:
                    return None  # id didn't exist -- nothing to decrement either
                checksum = row[0]
                cur = await conn.execute(_DEDUP_DECREMENT_BLOB, (1, checksum))
                new_ref_count = (await cur.fetchone())[0]
                if new_ref_count <= 0:
                    await conn.execute(
                        "DELETE FROM zerobucket_blobs "
                        "WHERE checksum_sha256 = ? AND ref_count <= 0;",
                        (checksum,),
                    )
                return ("sqlite", None)

        else:

            async def work(conn):
                cur = await conn.execute(_DELETE_RETURNING, (image_id,))
                return await cur.fetchone()

        try:
            row = await self._run(work, connection=connection, operation="delete")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc
        if row is None:
            return False
        storage_backend, object_storage_key = row
        if (
            connection is None
            and storage_backend == "object_storage"
            and self._object_storage is not None
        ):
            # After the row is gone, and a failure NEVER fails the delete
            # (see StorageBackend._delete_tiered_object, 0.29.0).
            await self._delete_tiered_object(object_storage_key)
        return True

    async def delete_many(
        self, image_ids: list[str], *, connection: object | None = None
    ) -> list[str]:
        if not image_ids:
            return []
        placeholders = ",".join("?" * len(image_ids))

        if self._dedup:

            async def work(conn):
                cur = await conn.execute(
                    f"DELETE FROM zerobucket_image_refs WHERE id IN ({placeholders}) "
                    "RETURNING id, checksum_sha256;",
                    image_ids,
                )
                deleted_rows = await cur.fetchall()
                if not deleted_rows:
                    return []
                # Several deleted refs can share one blob: decrement each blob
                # by how many of its refs were deleted.
                checksum_counts = Counter(row[1] for row in deleted_rows)
                emptied = []
                for checksum, count in checksum_counts.items():
                    cur = await conn.execute(_DEDUP_DECREMENT_BLOB, (count, checksum))
                    if (await cur.fetchone())[0] <= 0:
                        emptied.append(checksum)
                if emptied:
                    marks = ",".join("?" * len(emptied))
                    await conn.execute(
                        f"DELETE FROM zerobucket_blobs WHERE checksum_sha256 IN ({marks}) "
                        "AND ref_count <= 0;",
                        emptied,
                    )
                return [(str(row[0]), "sqlite", None) for row in deleted_rows]

        else:

            async def work(conn):
                cur = await conn.execute(
                    f"DELETE FROM zerobucket_images WHERE id IN ({placeholders}) "
                    "RETURNING id, storage_backend, object_storage_key;",
                    image_ids,
                )
                return [(str(r[0]), r[1], r[2]) for r in await cur.fetchall()]

        try:
            rows = await self._run(work, connection=connection, operation="delete_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc
        deleted_ids = []
        for image_id, storage_backend, object_storage_key in rows:
            deleted_ids.append(image_id)
            if (
                connection is None
                and storage_backend == "object_storage"
                and self._object_storage is not None
            ):
                await self._delete_tiered_object(object_storage_key)
        return deleted_ids

    # ---- exists -------------------------------------------------------------

    async def exists(self, image_id: str, *, connection: object | None = None) -> bool:
        exists_sql = _DEDUP_EXISTS if self._dedup else _EXISTS

        async def work(conn):
            cur = await conn.execute(exists_sql, (image_id,))
            return (await cur.fetchone()) is not None

        try:
            return await self._run(work, connection=connection, operation="exists")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    async def close(self) -> None:
        # No pool to release: a fresh connection is opened per operation.
        return None
