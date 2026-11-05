"""Async MySQL/MariaDB storage adapter, using `aiomysql`.

PHASE 4 of MySQL's multi-phase build (see mysql.py's own module
docstring for phases 1-3: core CRUD, streaming + tiering, dedup mode).
Scope matches `AsyncPostgresBackend`'s and `AsyncSQLiteBackend`'s
established scope EXACTLY, not by accident: core CRUD + streaming, and --
since 0.30.0 -- `dedup=True` (content-addressed storage) and
`object_storage=` tiering with `tier_to_object_storage()`, as in the sync
adapter and, as there, not both at once. NOT implemented, same as both
other async adapters: `connection=` transaction participation. The
blocking object-storage SDK (boto3) runs in worker threads
(`asyncio.to_thread`), never on the event loop.

**0.27.0 added the `on_operation` metrics hook and automatic
retry/backoff** (`max_retries`, `retry_base_delay`, `on_operation`),
with the same names, defaults, `OperationEvent` shape and operation
names as the sync `MySQLBackend`. See `_run()` for exactly what is
retried and why a retry always runs the WHOLE logical operation again
on a fresh connection, never one statement.

DRIVER CHOICE, stated directly: **aiomysql, not asyncmy**. asyncmy is
faster (Cython-accelerated) but ships as a COMPILED extension -- that
would break this project's consistent "no compiled dependency" choice
for every optional extra so far (PyMySQL itself was chosen over
mysqlclient for exactly this reason; aiosqlite and boto3 are both pure
Python too). aiomysql is pure Python, built directly on top of
PyMySQL's own protocol implementation -- confirmed directly, not
assumed: `pip show aiomysql` lists `PyMySQL` as its only dependency,
already required by the sync adapter, so this is zero new transitive
dependencies beyond aiomysql itself. aiomysql is the same KIND of
"asyncio wrapper around a synchronous-style driver's protocol" that
aiosqlite is for SQLite -- not psycopg3's situation, which ships a
genuinely native dual-mode driver needing no wrapper at all. Installed
via the `zerobucket[mysql-async]` optional extra, not bundled into
plain `zerobucket[mysql]` -- same reasoning as `sqlite-async` being
separate from the base SQLite support: the sync MySQLBackend must
never require this.

**A REAL, VERIFIED VERSION-COMPATIBILITY BUG, caught by actually
running this against a real database, not assumed safe from aiomysql's
description alone**: aiomysql 0.3.2 (its latest release; the project
has been effectively unmaintained since 2023) imports `escape_dict`,
`escape_bytes_prefixed`, and related helpers directly from
`pymysql.converters` -- internal names that were never public API and
that PyMySQL has been actively removing/repurposing: **PyMySQL 1.2.1
removed `escape_dict` outright** (aiomysql fails immediately with
`ImportError: cannot import name 'escape_dict'` the moment `import
aiomysql` runs), and **PyMySQL 1.2.2+ replaced it with a loud
poison-pill string**, `escape_dict = escape_bytes_prefixed = "DO NOT
IMPORT THIS!!!"`, specifically to catch this kind of internal-API
misuse (aiomysql then fails later, on the first `bytes`-binding query,
with `TypeError: 'str' object is not callable`). Both failure modes
were hit directly while building this, not assumed from reading
aiomysql's source: the first real test run against PyMySQL 1.2.3
failed with the TypeError; tightening the version pin to what seemed
like the obvious boundary (`<1.2.2`) then resolved to 1.2.1 in a fresh
install and hit the DIFFERENT ImportError instead -- both were root
-caused by inspecting `pymysql.converters` directly across versions
1.1.1 through 1.2.3, not guessed at from either error message alone.
Fix: the `zerobucket[mysql-async]` extra pins `PyMySQL<1.2.0`
explicitly (aiomysql itself declares no useful upper bound, and
`<1.2.2` alone isn't tight enough -- see above). `pip install
"zerobucket[mysql,mysql-async]"` together resolves to a PyMySQL version
satisfying both `>=1.1` and `<1.2.0` -- a real, stable, previous minor
release, not an ancient one. Confirmed working end to end afterward,
from a genuinely fresh venv install of the built wheel, against real
MariaDB.

Reuses the exact same SQL query strings and schema DDL as mysql.py
(imported, not copy-pasted) -- including `_migrate_tiering()`, so a
table this adapter creates/migrates is schema-compatible with the sync
`MySQLBackend` on the same database, even though this adapter's own
callers can't use tiering themselves (no `object_storage=` parameter
here -- see scope above).

**Tiered images (0.30.0):** `get()`/`get_many()`/`get_stream()` read an
image whose row says it lives in object storage back from the bucket, which
needs `object_storage=` on this backend; a clear StorageError says so if it
is missing. Before 0.30.0 such a read failed with an unhelpful
`bytes(None)`-shaped TypeError, because this adapter had no ObjectStorage.

`RETURNING` is NOT used, same reason as the sync adapter (real MySQL
8.0 doesn't support it on any statement -- see mysql.py's module
docstring): `put()`/`put_many()` generate the id in Python before the
`INSERT`, same as sync. `delete_many()` uses `SELECT ... FOR UPDATE`
then `DELETE` inside one transaction to learn exactly which ids
existed and were removed -- same shape as sync `MySQLBackend
.delete_many()`, purely to know which ids to report back without
RETURNING. Since 0.30.0 it also reports each deleted row's
`storage_backend`/`object_storage_key`, so a deleted TIERED image's object
is removed after the row is gone; a failed cleanup never fails the delete
(see `_delete_tiered_object`).

`get_stream()` uses the exact same `SUBSTRING(data FROM ... FOR ...)`
ranged-read strategy as the sync adapter -- confirmed to work
identically via aiomysql, not assumed, since aiomysql and PyMySQL are
both thin wrappers around the same underlying MySQL wire protocol.
Each chunk is its own round trip with no held snapshot, so a row
deleted mid-stream by a different connection causes the next chunk's
query to see no row and raise `StorageError` -- matching every other
async adapter's `get_stream()` behavior (and the sync MySQL adapter's
own non-tiered path), not sync SQLite's WAL-survives-it exception.

**Connection pooling** (pool_min_size/pool_max_size/pool_timeout,
same knobs and defaults as `PostgresBackend`/sync `MySQLBackend`) uses
aiomysql's own native pool, `aiomysql.create_pool()` -- no extra
dependency, unlike the sync adapter (PyMySQL ships no pool, so it needs
DBUtils; aiomysql does ship one). aiomysql's pool has no native
acquire-timeout either, but asyncio makes that trivial:
`pool_timeout` is `asyncio.wait_for()` around `pool.acquire()`. One
real difference from DBUtils' pool worth knowing: aiomysql's pool needs
an explicit `pool.release(conn)`; calling `conn.close()` on a pooled
connection would actually disconnect it, whereas DBUtils' wrapper
turns `close()` into "return to pool".
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING

from ..exceptions import StorageError
from ..object_storage import ObjectStorage
from .base import StoredRecord, StoredRecordMetadata
from .base_async import AsyncStorageBackend
from .mysql import (
    _DEDUP_EXISTS,
    _DEDUP_INSERT_REF,
    _DEDUP_SCHEMA_BLOBS,
    _DEDUP_SCHEMA_REFS,
    _DEDUP_SELECT_CHUNK,
    _DEDUP_SELECT_FULL,
    _DEDUP_SELECT_METADATA,
    _DEDUP_STREAM_INFO,
    _DEDUP_UPSERT_BLOB,
    _DELETE,
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
    _backoff_delay,
    _now,
    _parse_database_url,
)
from .postgres import OperationEvent

if TYPE_CHECKING:
    import aiomysql

__all__ = ["AsyncMySQLBackend", "DEFAULT_STREAM_CHUNK_SIZE"]


class AsyncMySQLBackend(AsyncStorageBackend):
    """Async storage backend for MySQL/MariaDB, classic mode only. See
    module docstring for exact scope, driver choice, and the inherited
    tiering-related limitation shared with AsyncPostgresBackend.

    Lazy-initialized the same way AsyncPostgresBackend/AsyncSQLiteBackend
    are, and for the same underlying reason (`__init__` cannot be a
    coroutine): pool creation and schema migration are deferred to the
    first real async call, guarded by an asyncio.Lock so concurrent
    first-callers can't race to create the pool or migrate twice.

    pool_min_size/pool_max_size/pool_timeout: same three knobs, same
    names, same defaults (1/5/10) as PostgresBackend/MySQLBackend.
    Uses aiomysql's own native pool (`aiomysql.create_pool`) -- no extra
    dependency needed on the async side, unlike sync MySQLBackend
    (PyMySQL ships no pool, so it needs DBUtils; aiomysql does ship
    one). aiomysql's pool, like DBUtils', has no native acquire-timeout,
    but asyncio makes this trivial: pool_timeout is enforced with
    `asyncio.wait_for()` around `pool.acquire()`, no polling loop
    needed here.

    max_retries/retry_base_delay/on_operation: the same three knobs as
    sync `MySQLBackend` (defaults 3 / 0.1 / None). A transient failure
    is retried up to `max_retries` times with exponential backoff and
    jitter, each retry re-running the WHOLE operation on a fresh pooled
    connection. `on_operation` receives one `OperationEvent` per call
    after the final attempt; an exception raised by the callback is
    swallowed, never allowed to break the real operation. There is no
    `connection=` parameter on this adapter, so unlike the sync
    adapter there is no no-retry path.
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
                "in this first pass -- see MySQLBackend's docstring."
            )
        self._connect_kwargs = _parse_database_url(database_url)
        self._auto_migrate = auto_migrate
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool_timeout = pool_timeout
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._on_operation = on_operation
        self._dedup = dedup
        self._object_storage = object_storage
        self._ready = False
        self._ready_lock = asyncio.Lock()
        self._aiomysql = None  # set in _ensure_ready, once import succeeds
        self._pool = None  # created in _ensure_ready

    async def _ensure_ready(self) -> None:
        if self._ready:
            return
        async with self._ready_lock:
            if self._ready:  # re-check: another task may have won the race
                return
            try:
                import aiomysql
            except ImportError as exc:
                raise StorageError(
                    "Async MySQL/MariaDB support requires aiomysql. Install "
                    "it with `pip install zerobucket[mysql-async]` (or "
                    "`pip install aiomysql` directly). Plain `import "
                    "zerobucket` and the SYNC MySQLBackend never require "
                    "this -- only AsyncMySQLBackend does."
                ) from exc
            self._aiomysql = aiomysql

            # aiomysql.create_pool() takes `db=`, not `database=` -- a
            # real, verified difference from both PyMySQL's own
            # connect() (which DOES accept `database=` as an alias) and
            # from this adapter's `_connect_kwargs` (reused as-is from
            # sync MySQLBackend's `_parse_database_url()`, which uses
            # the `database` key). Caught immediately by actually
            # running this against real MariaDB -- the first test run
            # failed with `connect() got an unexpected keyword argument
            # 'database'` -- not assumed compatible just because
            # aiomysql wraps PyMySQL's protocol.
            kwargs = dict(self._connect_kwargs)
            kwargs["db"] = kwargs.pop("database")
            try:
                self._pool = await aiomysql.create_pool(
                    minsize=self._pool_min_size,
                    maxsize=self._pool_max_size,
                    autocommit=False,
                    charset="utf8mb4",
                    **kwargs,
                )
            except Exception as exc:  # noqa: BLE001
                raise StorageError(
                    f"Could not connect to MySQL/MariaDB: {exc}"
                ) from exc

            if self._auto_migrate:
                conn = None
                migrate_start = time.monotonic()
                try:
                    conn = await self._acquire()
                    async with conn.cursor() as cur:
                        if self._dedup:
                            await cur.execute(_DEDUP_SCHEMA_BLOBS)
                            await cur.execute(_DEDUP_SCHEMA_REFS)
                        else:
                            await cur.execute(_SCHEMA)
                            await _migrate_tiering_async(cur)
                    await conn.commit()
                    self._emit("migrate", migrate_start, True, None, 0)
                except Exception as exc:  # noqa: BLE001
                    self._emit("migrate", migrate_start, False, str(exc), 0)
                    # Don't leak the pool if setup fails partway
                    # through -- close it before propagating, same
                    # reasoning as the sync adapter's equivalent.
                    if conn is not None:
                        self._pool.release(conn)
                        conn = None
                    self._pool.close()
                    await self._pool.wait_closed()
                    self._pool = None
                    raise StorageError(f"Migration failed: {exc}") from exc
                finally:
                    if conn is not None:
                        self._pool.release(conn)

            self._ready = True

    async def _acquire(self) -> aiomysql.Connection:
        """Acquires a pooled connection, waiting up to pool_timeout
        seconds if the pool is fully checked out. Does NOT call
        _ensure_ready() itself (unlike _run()) -- _ensure_ready() needs
        to acquire a connection for the migration step while it is
        still the one initializing the pool."""
        try:
            return await asyncio.wait_for(
                self._pool.acquire(), timeout=self._pool_timeout
            )
        except asyncio.TimeoutError as exc:
            raise StorageError(
                f"Timed out after {self._pool_timeout}s waiting for a "
                f"MySQL/MariaDB connection pool slot "
                f"(pool_max_size={self._pool_max_size} all in use)."
            ) from exc
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to connect to MySQL/MariaDB: {exc}") from exc

    def _is_retryable(self, exc: BaseException) -> bool:
        """Same classification as sync `MySQLBackend._is_retryable()`:
        PyMySQL (which aiomysql re-exports) raises `OperationalError`
        for connection-level failures AND for lock-wait-timeout (1205)
        and deadlock (1213) alike, and all of those are transient.
        Integrity/Programming/Data errors are not and are never
        retried."""
        return isinstance(exc, self._aiomysql.OperationalError)

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

    async def _run(self, work, *, operation: str):
        """Run `work(conn)` (an async callable) on a pooled connection,
        commit, and return its result.

        A transient failure (see `_is_retryable`) is retried up to
        `max_retries` times with exponential backoff plus jitter, EACH
        retry re-running `work` in full on a freshly acquired
        connection after a rollback. That is retry at the whole
        logical-operation boundary, never statement-by-statement
        replay, which is what makes it safe for multi-statement work
        such as `put_many()` and `delete_many()`.

        Pool-acquire failures (including `pool_timeout`) are never
        retried: `_acquire()` is already a bounded wait, and retrying it
        would only repeat the same wait. Emits exactly one
        `OperationEvent` per call, after the final attempt;
        `duration_seconds` covers the whole call including backoff.
        """
        start_time = time.monotonic()
        try:
            await self._ensure_ready()
        except Exception as exc:  # noqa: BLE001
            self._emit(operation, start_time, False, str(exc), 0)
            raise

        attempt = 0
        while True:
            try:
                conn = await self._acquire()
            except Exception as exc:  # noqa: BLE001
                self._emit(operation, start_time, False, str(exc), attempt)
                raise
            try:
                result = await work(conn)
                await conn.commit()
            except Exception as exc:  # noqa: BLE001
                try:
                    await conn.rollback()
                except Exception:  # noqa: BLE001
                    # The connection is probably already dead; drop it
                    # rather than hand a broken one back to the pool.
                    conn.close()
                # aiomysql's pool needs an explicit release() -- unlike
                # DBUtils (sync adapter), conn.close() would disconnect
                # rather than return the connection to the pool.
                self._pool.release(conn)
                attempt += 1
                if attempt > self._max_retries or not self._is_retryable(exc):
                    self._emit(operation, start_time, False, str(exc), attempt - 1)
                    raise
                await asyncio.sleep(_backoff_delay(attempt, self._retry_base_delay))
            else:
                self._pool.release(conn)
                self._emit(operation, start_time, True, None, attempt)
                return result

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
        image_id = str(uuid.uuid4())
        now = _now()

        if self._dedup:

            async def work(conn):
                async with conn.cursor() as cur:
                    await cur.execute(
                        _DEDUP_UPSERT_BLOB,
                        (
                            checksum_sha256,
                            data,
                            mime_type,
                            size_bytes,
                            width,
                            height,
                            now,
                        ),
                    )
                    await cur.execute(
                        _DEDUP_INSERT_REF,
                        (image_id, checksum_sha256, original_filename, now, now),
                    )
                return image_id

        else:

            async def work(conn):
                async with conn.cursor() as cur:
                    await cur.execute(
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
                return image_id

        try:
            return await self._run(work, operation="put")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    async def put_many(self, rows: list[dict]) -> list[str]:
        if not rows:
            return []
        now = _now()
        prepared = [(str(uuid.uuid4()), row) for row in rows]

        async def work(conn):
            async with conn.cursor() as cur:
                if self._dedup:
                    # Blobs in sorted checksum order (one global lock order, so
                    # overlapping batches cannot deadlock), refs in the caller's
                    # order so the returned ids line up with `rows`.
                    for row in sorted(rows, key=lambda r: r["checksum_sha256"]):
                        await cur.execute(
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
                    for image_id, row in prepared:
                        await cur.execute(
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
                    for image_id, row in prepared:
                        await cur.execute(
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
            return [image_id for image_id, _ in prepared]

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
        """The row says this image lives in object storage: fetch the bytes
        (the blocking boto3 download runs in a worker thread)."""
        if self._object_storage is None:
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) but this AsyncMySQLBackend was "
                "constructed without object_storage=... -- configure it "
                "with the same bucket/credentials used to tier this image."
            )
        return await asyncio.to_thread(
            self._object_storage.download, object_storage_key
        )

    @staticmethod
    def _record(row, data) -> StoredRecord:
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

    async def get(self, image_id: str) -> StoredRecord | None:
        select_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL

        async def work(conn):
            async with conn.cursor() as cur:
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
            data = await self._fetch_tiered_bytes(row[9], image_id=image_id)
        return self._record(row, data)

    async def get_many(self, image_ids: list[str]) -> list[StoredRecord]:
        if not image_ids:
            return []
        placeholders = ", ".join(["%s"] * len(image_ids))
        base_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL
        where = "WHERE r.id = %s" if self._dedup else "WHERE id = %s"
        column = "r.id" if self._dedup else "id"
        sql = base_sql.replace(where, f"WHERE {column} IN ({placeholders})")

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(sql, tuple(image_ids))
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
                data = await self._fetch_tiered_bytes(row[9], image_id=str(row[0]))
            records.append(self._record(row, data))
        return records

    async def get_metadata(self, image_id: str) -> StoredRecordMetadata | None:
        select_sql = _DEDUP_SELECT_METADATA if self._dedup else _SELECT_METADATA

        async def work(conn):
            async with conn.cursor() as cur:
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
        """One metadata lookup (the size, and the not-found check), then
        repeated ranged SUBSTRING reads -- same strategy as the sync adapter.
        A row deleted between chunks raises StorageError instead of silently
        truncating. A TIERED row is streamed from object storage with real
        HTTP byte-range requests (ObjectStorage.download_stream()); each
        blocking chunk read runs in a worker thread."""
        info_sql = _DEDUP_STREAM_INFO if self._dedup else _SELECT_STREAM_INFO
        select_chunk_sql = _DEDUP_SELECT_CHUNK if self._dedup else _SELECT_CHUNK

        async def info_work(conn):
            async with conn.cursor() as cur:
                await cur.execute(info_sql, (image_id,))
                return await cur.fetchone()

        try:
            info_row = await self._run(info_work, operation="get_stream")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
        if info_row is None:
            return None
        if self._dedup:
            total_size = info_row[0]
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
            offset = 1  # SUBSTRING is 1-indexed
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                async def work(conn, offset=offset, length=length):
                    async with conn.cursor() as cur:
                        await cur.execute(select_chunk_sql, (offset, length, image_id))
                        return await cur.fetchone()

                try:
                    row = await self._run(work, operation="get_stream")
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

    async def tier_to_object_storage(self, image_id: str) -> bool | None:
        """Move an image's bytes out of MySQL into object storage. Same
        contract and safety as MySQLBackend.tier_to_object_storage(): None if
        the id doesn't exist, False if already tiered (a safe no-op), True if
        tiered just now. The upload happens INSIDE the transaction that
        locked the row (SELECT ... FOR UPDATE), so a failed upload rolls
        everything back and the row is untouched. TRADEOFF, as in the sync
        adapter: the row stays locked for the whole upload, so a concurrent
        delete() or tier of THE SAME image waits; plain reads are not
        blocked. A retry re-uploads the same bytes to the same
        deterministic key, which is harmless."""
        if self._dedup:
            raise StorageError(
                "tier_to_object_storage() is not supported in dedup mode "
                "in this first pass -- see MySQLBackend's docstring."
            )
        if self._object_storage is None:
            raise StorageError(
                "tier_to_object_storage() requires this backend to be "
                "constructed with object_storage=... -- see ObjectStorage "
                "in object_storage.py."
            )
        store = self._object_storage

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(_SELECT_FOR_TIERING, (image_id,))
                row = await cur.fetchone()
                if row is None:
                    return None
                data, mime_type, _size_bytes, storage_backend = row
                if storage_backend != "mysql":
                    return False
                key = str(image_id)
                await asyncio.to_thread(
                    store.upload, key, bytes(data), mime_type=mime_type
                )
                await cur.execute(
                    _UPDATE_AFTER_TIERING, (store.bucket, key, _now(), image_id)
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
        if self._dedup:
            return await self._delete_dedup(image_id)

        async def work(conn):
            async with conn.cursor() as cur:
                # No RETURNING in MySQL: lock the row, read what the cleanup
                # needs, then delete -- all in one transaction.
                await cur.execute(
                    "SELECT storage_backend, object_storage_key "
                    "FROM zerobucket_images WHERE id = %s FOR UPDATE;",
                    (image_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    return None
                await cur.execute(_DELETE, (image_id,))
                return row

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

    async def _delete_dedup(self, image_id: str) -> bool:
        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT checksum_sha256 FROM zerobucket_image_refs "
                    "WHERE id = %s FOR UPDATE;",
                    (image_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    return False
                checksum = row[0]
                # Lock the blob row too, then drop the ref and decrement.
                await cur.execute(
                    "SELECT ref_count FROM zerobucket_blobs "
                    "WHERE checksum_sha256 = %s FOR UPDATE;",
                    (checksum,),
                )
                blob = await cur.fetchone()
                await cur.execute(
                    "DELETE FROM zerobucket_image_refs WHERE id = %s;", (image_id,)
                )
                if blob is not None:
                    await cur.execute(
                        "UPDATE zerobucket_blobs SET ref_count = ref_count - 1 "
                        "WHERE checksum_sha256 = %s;",
                        (checksum,),
                    )
                    if blob[0] - 1 <= 0:
                        await cur.execute(
                            "DELETE FROM zerobucket_blobs "
                            "WHERE checksum_sha256 = %s AND ref_count <= 0;",
                            (checksum,),
                        )
                return True

        try:
            return await self._run(work, operation="delete")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    async def delete_many(self, image_ids: list[str]) -> list[str]:
        if not image_ids:
            return []
        if self._dedup:
            return await self._delete_many_dedup(image_ids)
        placeholders = ", ".join(["%s"] * len(image_ids))

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, storage_backend, object_storage_key FROM zerobucket_images "
                    f"WHERE id IN ({placeholders}) FOR UPDATE;",
                    tuple(image_ids),
                )
                rows = await cur.fetchall()
                if not rows:
                    return []
                await cur.execute(
                    f"DELETE FROM zerobucket_images WHERE id IN ({placeholders});",
                    tuple(image_ids),
                )
                return [(str(r[0]), r[1], r[2]) for r in rows]

        try:
            rows = await self._run(work, operation="delete_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc
        deleted_ids = []
        for image_id, storage_backend, object_storage_key in rows:
            deleted_ids.append(image_id)
            if storage_backend == "object_storage" and self._object_storage is not None:
                await self._delete_tiered_object(object_storage_key)
        return deleted_ids

    async def _delete_many_dedup(self, image_ids: list[str]) -> list[str]:
        placeholders = ", ".join(["%s"] * len(image_ids))

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT id, checksum_sha256 FROM zerobucket_image_refs "
                    f"WHERE id IN ({placeholders}) FOR UPDATE;",
                    tuple(image_ids),
                )
                rows = await cur.fetchall()
                if not rows:
                    return []
                counts = Counter(row[1] for row in rows)
                # Lock the affected blobs in sorted checksum order (the same
                # lock order as put_many), then delete refs and decrement.
                emptied = []
                for checksum in sorted(counts):
                    await cur.execute(
                        "SELECT ref_count FROM zerobucket_blobs "
                        "WHERE checksum_sha256 = %s FOR UPDATE;",
                        (checksum,),
                    )
                    blob = await cur.fetchone()
                    if blob is not None and blob[0] - counts[checksum] <= 0:
                        emptied.append(checksum)
                await cur.execute(
                    f"DELETE FROM zerobucket_image_refs WHERE id IN ({placeholders});",
                    tuple(image_ids),
                )
                for checksum, count in sorted(counts.items()):
                    await cur.execute(
                        "UPDATE zerobucket_blobs SET ref_count = ref_count - %s "
                        "WHERE checksum_sha256 = %s;",
                        (count, checksum),
                    )
                if emptied:
                    marks = ", ".join(["%s"] * len(emptied))
                    await cur.execute(
                        f"DELETE FROM zerobucket_blobs WHERE checksum_sha256 IN ({marks}) "
                        "AND ref_count <= 0;",
                        tuple(emptied),
                    )
                return [str(row[0]) for row in rows]

        try:
            return await self._run(work, operation="delete_many")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    # ---- exists -------------------------------------------------------------

    async def exists(self, image_id: str) -> bool:
        exists_sql = _DEDUP_EXISTS if self._dedup else _EXISTS

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(exists_sql, (image_id,))
                return (await cur.fetchone()) is not None

        try:
            return await self._run(work, operation="exists")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    async def close(self) -> None:
        """Closes the connection pool if one was ever created (it's
        lazy -- a backend that never made a call has nothing to close).
        Matches AsyncPostgresBackend.close()."""
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            self._ready = False


async def _migrate_tiering_async(cur) -> None:
    """aiomysql cursors are awaited for execute() but otherwise behave
    like a normal DB-API cursor for fetch calls -- this wraps sync
    MySQLBackend's `_migrate_tiering()` logic with awaited execute()
    calls rather than importing and reusing it directly, since that
    function's execute() calls are synchronous (PyMySQL cursor) and
    can't be awaited. Kept in exact lockstep with `_migrate_tiering()`
    in mysql.py -- same columns, same constraint, same
    information_schema-based existence checks (see that function's
    docstring for the full reasoning); duplicated here only because
    async/await can't be sprinkled onto a sync function after the fact.
    """
    await cur.execute(
        "SELECT COLUMN_NAME, IS_NULLABLE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images';"
    )
    existing = {row[0]: row[1] for row in await cur.fetchall()}
    if "storage_backend" not in existing:
        await cur.execute(
            "ALTER TABLE zerobucket_images "
            "ADD COLUMN storage_backend VARCHAR(20) NOT NULL DEFAULT 'mysql';"
        )
    if "object_storage_bucket" not in existing:
        await cur.execute(
            "ALTER TABLE zerobucket_images ADD COLUMN object_storage_bucket VARCHAR(255);"
        )
    if "object_storage_key" not in existing:
        await cur.execute(
            "ALTER TABLE zerobucket_images ADD COLUMN object_storage_key VARCHAR(512);"
        )
    # Only when still NOT NULL -- see mysql.py's _migrate_tiering() for why
    # an unconditional ALTER on every startup can stall all traffic.
    if existing.get("data") == "NO":
        await cur.execute("ALTER TABLE zerobucket_images MODIFY data LONGBLOB NULL;")
    await cur.execute(
        "SELECT 1 FROM information_schema.TABLE_CONSTRAINTS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images' "
        "AND CONSTRAINT_NAME = 'chk_zerobucket_storage';"
    )
    if await cur.fetchone() is None:
        await cur.execute(
            "ALTER TABLE zerobucket_images ADD CONSTRAINT chk_zerobucket_storage CHECK ("
            "  (storage_backend = 'mysql' AND data IS NOT NULL "
            "     AND object_storage_bucket IS NULL AND object_storage_key IS NULL)"
            "  OR"
            "  (storage_backend = 'object_storage' AND data IS NULL "
            "     AND object_storage_bucket IS NOT NULL AND object_storage_key IS NOT NULL)"
            ");"
        )
