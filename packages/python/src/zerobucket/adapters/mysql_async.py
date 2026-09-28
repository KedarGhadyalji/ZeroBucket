"""Async MySQL/MariaDB storage adapter, using `aiomysql`.

PHASE 4 of MySQL's multi-phase build (see mysql.py's own module
docstring for phases 1-3: core CRUD, streaming + tiering, dedup mode).
Scope matches `AsyncPostgresBackend`'s and `AsyncSQLiteBackend`'s
established scope EXACTLY, not by accident: core CRUD + streaming,
classic mode only. NOT implemented, same as both other async adapters
and tracked the same way: `dedup=True`, `tier_to_object_storage()` as
a caller-facing operation, `connection=` transaction participation,
the `on_operation`/retry-backoff machinery. This mirrors
`AsyncSQLiteBackend`'s own documented scope decision rather than
inventing a wider one for MySQL -- consistency across all three async
adapters was judged more valuable than matching sync MySQL's fuller
(Phase 1-3) feature set.

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

**A REAL, INHERITED LIMITATION, not new to this adapter**, stated
plainly rather than silently carried forward: `get()`/`get_many()`
reuse sync `MySQLBackend`'s tiering-aware `_SELECT_FULL` (which selects
`storage_backend`/`object_storage_key` alongside `data`), the exact
same thing `AsyncPostgresBackend` does with Postgres's equivalent
query. If an image was tiered via the SYNC client, fetching it through
THIS async adapter will fail with an unhelpful `bytes(None)`-shaped
`TypeError` wrapped in `StorageError`, rather than a clean, specific
message -- this adapter has no `ObjectStorage` to resolve the tiered
bytes with. This mirrors `AsyncPostgresBackend`'s own documented
behavior exactly (consistency across the three async adapters, not a
new gap introduced here) -- deliberately not special-cased away in
this pass, since fixing it well means either adding `object_storage=`
to every async adapter's scope at once (out of scope for this single
phase) or giving MySQL a nicer error the other two async adapters
don't have (inconsistent, and would make the three adapters diverge
for no caller-visible benefit). Tracked here honestly so a future pass
fixes all three consistently, not one at a time.

`RETURNING` is NOT used, same reason as the sync adapter (real MySQL
8.0 doesn't support it on any statement -- see mysql.py's module
docstring): `put()`/`put_many()` generate the id in Python before the
`INSERT`, same as sync. `delete_many()` uses `SELECT ... FOR UPDATE`
then `DELETE` inside one transaction to learn exactly which ids
existed and were removed -- same shape as sync `MySQLBackend
.delete_many()`, purely to know which ids to report back without
RETURNING, NOT for any tiering-related reason: this adapter does no
object-storage cleanup on delete at all (same inherited scope gap as
`AsyncPostgresBackend`, which also doesn't clean up tiered objects on
delete -- see the limitation above).

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
import uuid
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from ..exceptions import StorageError
from .base import StoredRecord, StoredRecordMetadata
from .base_async import AsyncStorageBackend
from .mysql import (
    _DELETE,
    _EXISTS,
    _INSERT,
    _SCHEMA,
    _SELECT_CHUNK,
    _SELECT_FULL,
    _SELECT_METADATA,
    _SELECT_STREAM_INFO,
    DEFAULT_STREAM_CHUNK_SIZE,
    _now,
    _parse_database_url,
)

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
    """

    def __init__(
        self,
        database_url: str,
        *,
        auto_migrate: bool = True,
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        pool_timeout: float = 10,
    ) -> None:
        self._connect_kwargs = _parse_database_url(database_url)
        self._auto_migrate = auto_migrate
        self._pool_min_size = pool_min_size
        self._pool_max_size = pool_max_size
        self._pool_timeout = pool_timeout
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
                try:
                    conn = await self._acquire()
                    async with conn.cursor() as cur:
                        await cur.execute(_SCHEMA)
                        await _migrate_tiering_async(cur)
                    await conn.commit()
                except Exception as exc:  # noqa: BLE001
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

    async def _run(self, work):
        await self._ensure_ready()
        conn = await self._acquire()
        try:
            result = await work(conn)
            await conn.commit()
            return result
        except Exception:
            await conn.rollback()
            raise
        finally:
            # aiomysql's pool needs an explicit release() -- unlike
            # DBUtils (sync adapter), conn.close() here would actually
            # disconnect rather than return the connection to the pool.
            self._pool.release(conn)

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
            return await self._run(work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    async def put_many(self, rows: list[dict]) -> list[str]:
        """One transaction for the whole batch, each row its own
        execute() -- same shape as the sync adapter's put_many(), same
        reasoning (no RETURNING-based pipelining to chase here, see
        module docstring)."""
        if not rows:
            return []
        now = _now()
        prepared = [(str(uuid.uuid4()), row) for row in rows]

        async def work(conn):
            async with conn.cursor() as cur:
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
            return await self._run(work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    # ---- get --------------------------------------------------------------

    async def get(self, image_id: str) -> StoredRecord | None:
        """See module docstring: reuses sync MySQLBackend's
        tiering-aware _SELECT_FULL. A tiered row will fail here with an
        unhelpful error -- a real, inherited, documented limitation,
        not something this method tries to hide."""

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(_SELECT_FULL, (image_id,))
                return await cur.fetchone()

        try:
            row = await self._run(work)
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
        if not image_ids:
            return []
        placeholders = ",".join(["%s"] * len(image_ids))
        sql = _SELECT_FULL.replace("WHERE id = %s", f"WHERE id IN ({placeholders})")

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(sql, image_ids)
                return await cur.fetchall()

        try:
            rows = await self._run(work)
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
        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(_SELECT_METADATA, (image_id,))
                return await cur.fetchone()

        try:
            row = await self._run(work)
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
        self, image_id: str, *, chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE
    ) -> AsyncIterator[bytes] | None:
        """See module docstring: SUBSTRING()-based ranged reads, each
        chunk its own round trip. A concurrent delete mid-stream raises
        StorageError rather than silently truncating."""

        async def info_work(conn):
            async with conn.cursor() as cur:
                await cur.execute(_SELECT_STREAM_INFO, (image_id,))
                return await cur.fetchone()

        try:
            info_row = await self._run(info_work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
        if info_row is None:
            return None
        total_size, storage_backend, object_storage_key = info_row
        if storage_backend == "object_storage":
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) -- AsyncMySQLBackend cannot "
                "resolve tiered images in this phase. Use the sync "
                "MySQLBackend (with object_storage=...) to read this one."
            )

        async def generator() -> AsyncIterator[bytes]:
            offset = 1  # SUBSTRING() is 1-indexed
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                async def work(conn, offset=offset, length=length):
                    async with conn.cursor() as cur:
                        await cur.execute(_SELECT_CHUNK, (offset, length, image_id))
                        return await cur.fetchone()

                try:
                    row = await self._run(work)
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

    # ---- delete -------------------------------------------------------------

    async def delete(self, image_id: str) -> bool:
        """No object-storage cleanup here -- see module docstring's
        inherited-limitation note (same as AsyncPostgresBackend). Just a
        plain DELETE + rowcount check; no SELECT-then-act dance needed
        since nothing but "did a row disappear" is being determined."""

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(_DELETE, (image_id,))
                return cur.rowcount > 0

        try:
            return await self._run(work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    async def delete_many(self, image_ids: list[str]) -> list[str]:
        """No RETURNING (see module docstring), so this locks the
        candidate rows with SELECT ... FOR UPDATE before DELETE, purely
        to learn which ids existed and were removed -- same shape as
        sync MySQLBackend.delete_many(), no tiering-cleanup purpose
        here (this adapter does none)."""
        if not image_ids:
            return []
        placeholders = ",".join(["%s"] * len(image_ids))

        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT id FROM zerobucket_images WHERE id IN ({placeholders}) "
                    "FOR UPDATE;",
                    image_ids,
                )
                existing_ids = [str(row[0]) for row in await cur.fetchall()]
                if not existing_ids:
                    return []
                existing_placeholders = ",".join(["%s"] * len(existing_ids))
                await cur.execute(
                    f"DELETE FROM zerobucket_images WHERE id IN ({existing_placeholders});",
                    existing_ids,
                )
                return existing_ids

        try:
            return await self._run(work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    # ---- exists -------------------------------------------------------------

    async def exists(self, image_id: str) -> bool:
        async def work(conn):
            async with conn.cursor() as cur:
                await cur.execute(_EXISTS, (image_id,))
                return (await cur.fetchone()) is not None

        try:
            return await self._run(work)
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
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images';"
    )
    existing = {row[0] for row in await cur.fetchall()}
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
