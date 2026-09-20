"""SQLite storage adapter.

PHASE 2 OF A MULTI-PHASE BUILD, stated directly rather than implied.
Phase 1 shipped classic-mode core CRUD (put, put_many, get, get_many,
get_metadata, delete, delete_many, exists) -- see v0.16.0's CHANGELOG
entry. This phase adds get_stream()/tier_to_object_storage(). STILL NOT
implemented, tracked as explicit follow-up phases, not silent gaps:
dedup mode, async support (aiosqlite), and MySQL entirely.

Stores image bytes directly in a BLOB column. All queries are
parameterized; nothing is ever built via string concatenation.

WHY SQLite'S DESIGN DIFFERS FROM POSTGRES'S, stated explicitly rather
than left for someone to wonder about while reading unfamiliar code:

- No `gen_random_uuid()` equivalent guaranteed available across SQLite
  builds -- ids are generated in Python (`uuid.uuid4()`) and stored as
  TEXT, not left to the database.
- No native UUID/TIMESTAMPTZ types -- ids and timestamps are both TEXT
  (UUID string, ISO-8601 UTC string respectively). Sortable as text,
  same as their native-typed Postgres counterparts would be.
- No connection pool. Unlike a networked database, a SQLite "connection"
  is just a handle to a local file -- opening one is cheap (no network
  round trip, no auth handshake), so this backend opens a fresh
  connection per operation when connection=None, rather than
  maintaining a pool. This is a deliberate simplicity choice enabled by
  SQLite's local-file nature specifically, not a design that would make
  sense for a networked database -- don't copy this pattern into a
  hypothetical future networked-backend adapter without re-deriving
  whether it still makes sense there.
- WAL mode (`PRAGMA journal_mode=WAL`) is enabled on every connection
  opened by this backend -- SQLite's default rollback-journal mode
  serializes ALL readers behind a writer; WAL mode allows concurrent
  readers alongside a single writer, which matters even for a single
  local file being used by more than one process/thread.
- No `SELECT ... FOR UPDATE` -- SQLite has no per-row locking at all
  (it's fundamentally a single-writer database). `tier_to_object_storage()`
  below uses `BEGIN IMMEDIATE` instead -- SQLite's closest equivalent,
  but meaningfully COARSER: it locks the ENTIRE database file for
  writes, not just one row, for the duration of the object-storage
  upload. See that method's docstring for the full reasoning and what
  this actually costs you.
- `= ANY(array)` isn't SQLite syntax -- get_many()/delete_many() build
  a dynamically-sized `IN (?, ?, ...)` placeholder list instead.
- `RETURNING` IS supported (SQLite >= 3.35, released March 2021) and is
  used the same way the Postgres adapter uses it.
- get_stream() uses `sqlite3.Connection.blobopen()` (Python >= 3.11), a
  genuine incremental-BLOB-I/O API -- not a workaround built on top of
  substring()-style range queries the way the Postgres adapter's
  version is. This is arguably a more natural fit for streaming than
  the Postgres implementation, not a lesser one.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime

from ..exceptions import StorageError
from ..object_storage import ObjectStorage
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024

_SELECT_STREAM_INFO = """
SELECT rowid, size_bytes, storage_backend, object_storage_key
FROM zerobucket_images
WHERE id = ?;
"""

_SELECT_FOR_TIERING = """
SELECT data, mime_type, size_bytes, storage_backend
FROM zerobucket_images
WHERE id = ?;
"""

_UPDATE_AFTER_TIERING = """
UPDATE zerobucket_images
SET data = NULL, storage_backend = 'object_storage',
    object_storage_bucket = ?, object_storage_key = ?, updated_at = ?
WHERE id = ?;
"""

_SCHEMA = """
CREATE TABLE IF NOT EXISTS zerobucket_images (
    id                  TEXT PRIMARY KEY,
    data                BLOB,
    mime_type           TEXT NOT NULL,
    original_filename   TEXT,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    checksum_sha256     TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    storage_backend     TEXT NOT NULL DEFAULT 'sqlite',
    object_storage_bucket TEXT,
    object_storage_key  TEXT,
    CHECK (
        (storage_backend = 'sqlite'
            AND data IS NOT NULL
            AND object_storage_key IS NULL
            AND object_storage_bucket IS NULL)
        OR
        (storage_backend = 'object_storage'
            AND data IS NULL
            AND object_storage_key IS NOT NULL
            AND object_storage_bucket IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_checksum ON zerobucket_images (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_created_at ON zerobucket_images (created_at);
"""
# The storage_backend/object_storage_* columns exist from the start here
# (unlike Postgres, where they were added by a later ALTER TABLE
# migration onto an already-shipped schema) -- this adapter is new, so
# there's no pre-existing deployed schema to migrate; baking the final
# column shape in from day one avoids a pointless migration-then-
# immediately-alter sequence. tier_to_object_storage() itself is a
# later phase (see module docstring) -- the columns exist now, the
# feature using them doesn't yet.

# Dedup mode: separate blobs/refs tables, mirroring the Postgres
# adapter's dedup schema exactly in shape (see postgres.py's
# _DEDUP_SCHEMA) -- content-addressed storage where one blob can be
# shared by many ids via ref-counting. NO tiering columns here: dedup
# mode does not support object-storage tiering in this adapter, same
# restriction as PostgresBackend (rejected at construction below, not
# discovered later as a confusing runtime failure).
_DEDUP_SCHEMA = """
CREATE TABLE IF NOT EXISTS zerobucket_blobs (
    checksum_sha256     TEXT PRIMARY KEY,
    data                BLOB NOT NULL,
    mime_type           TEXT NOT NULL,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    ref_count           INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS zerobucket_image_refs (
    id                  TEXT PRIMARY KEY,
    checksum_sha256     TEXT NOT NULL REFERENCES zerobucket_blobs(checksum_sha256),
    original_filename   TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_image_refs_checksum
    ON zerobucket_image_refs (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_image_refs_created_at
    ON zerobucket_image_refs (created_at);
"""

# SQLite's UPSERT syntax (ON CONFLICT ... DO UPDATE, available since
# 3.24) -- the unqualified `ref_count` on the SET side refers to the
# existing row being updated, same semantics as Postgres's
# `zerobucket_blobs.ref_count + 1` form, just without needing the
# table-qualifier SQLite doesn't require here.
_DEDUP_UPSERT_BLOB = """
INSERT INTO zerobucket_blobs
    (checksum_sha256, data, mime_type, size_bytes, width, height, ref_count, created_at)
VALUES (?, ?, ?, ?, ?, ?, 1, ?)
ON CONFLICT (checksum_sha256) DO UPDATE SET ref_count = ref_count + 1;
"""

_DEDUP_INSERT_REF = """
INSERT INTO zerobucket_image_refs (id, checksum_sha256, original_filename, created_at, updated_at)
VALUES (?, ?, ?, ?, ?)
RETURNING id;
"""

_DEDUP_SELECT_FULL = """
SELECT r.id, b.data, b.mime_type, r.original_filename, b.size_bytes,
       b.width, b.height, r.checksum_sha256
FROM zerobucket_image_refs r
JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256
WHERE r.id = ?;
"""

_DEDUP_SELECT_METADATA = """
SELECT r.id, b.mime_type, r.original_filename, b.size_bytes, b.width, b.height, r.checksum_sha256
FROM zerobucket_image_refs r
JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256
WHERE r.id = ?;
"""

_DEDUP_STREAM_INFO = """
SELECT b.rowid, b.size_bytes
FROM zerobucket_image_refs r
JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256
WHERE r.id = ?;
"""

_DEDUP_EXISTS = "SELECT 1 FROM zerobucket_image_refs WHERE id = ?;"

_DEDUP_DELETE_REF = (
    "DELETE FROM zerobucket_image_refs WHERE id = ? RETURNING checksum_sha256;"
)

_DEDUP_DECREMENT_BLOB = """
UPDATE zerobucket_blobs SET ref_count = ref_count - ?
WHERE checksum_sha256 = ?
RETURNING ref_count;
"""

_INSERT = """
INSERT INTO zerobucket_images
    (id, data, mime_type, original_filename, size_bytes, width, height,
     checksum_sha256, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height,
       checksum_sha256, storage_backend, object_storage_key
FROM zerobucket_images
WHERE id = ?;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = ?;
"""

_DELETE_RETURNING = """
DELETE FROM zerobucket_images WHERE id = ?
RETURNING storage_backend, object_storage_key;
"""

_EXISTS = "SELECT 1 FROM zerobucket_images WHERE id = ?;"


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


class SQLiteBackend(StorageBackend):
    """Storage backend for SQLite. See module docstring for what's
    implemented in this phase and what isn't yet.

    `connection=` here means a `sqlite3.Connection` (not a Postgres
    connection) -- the interface in base.py types this as `object`
    specifically so each adapter can narrow it to its own driver's type,
    same pattern the Postgres adapter uses.
    """

    def __init__(
        self,
        database_path: str,
        *,
        auto_migrate: bool = True,
        dedup: bool = False,
        object_storage: ObjectStorage | None = None,
    ) -> None:
        if object_storage is not None and dedup:
            raise ValueError(
                "object_storage= is not supported together with dedup=True "
                "in this adapter -- same restriction as PostgresBackend, "
                "see this module's docstring."
            )
        self._database_path = database_path
        self._dedup = dedup
        self._object_storage = object_storage
        if auto_migrate:
            schema = _DEDUP_SCHEMA if dedup else _SCHEMA
            conn = self._connect()
            try:
                conn.executescript(schema)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Migration failed: {exc}") from exc
            finally:
                conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._database_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    def _run(self, connection, work):
        """Mirrors the Postgres adapter's `_run()` shape (a `work(conn)`
        callable, connection=None vs connection=provided) without the
        retry loop or on_operation metrics -- neither is implemented in
        this phase (see module docstring)."""
        if connection is not None:
            return work(connection)
        conn = self._connect()
        try:
            result = work(conn)
            conn.commit()
            return result
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ---- put ------------------------------------------------------------

    def put(
        self,
        *,
        data: bytes,
        mime_type: str,
        original_filename: str | None,
        size_bytes: int,
        width: int | None,
        height: int | None,
        checksum_sha256: str,
        connection: sqlite3.Connection | None = None,
    ) -> str:
        if self._dedup:
            return self._put_dedup(
                data=data,
                mime_type=mime_type,
                original_filename=original_filename,
                size_bytes=size_bytes,
                width=width,
                height=height,
                checksum_sha256=checksum_sha256,
                connection=connection,
            )

        image_id = str(uuid.uuid4())
        now = _now_iso()

        def work(conn):
            cur = conn.execute(
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
            row = cur.fetchone()
            return str(row[0])

        try:
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    def _put_dedup(
        self,
        *,
        data: bytes,
        mime_type: str,
        original_filename: str | None,
        size_bytes: int,
        width: int | None,
        height: int | None,
        checksum_sha256: str,
        connection: sqlite3.Connection | None,
    ) -> str:
        image_id = str(uuid.uuid4())
        now = _now_iso()

        def work(conn):
            conn.execute(
                _DEDUP_UPSERT_BLOB,
                (checksum_sha256, data, mime_type, size_bytes, width, height, now),
            )
            cur = conn.execute(
                _DEDUP_INSERT_REF,
                (image_id, checksum_sha256, original_filename, now, now),
            )
            row = cur.fetchone()
            return str(row[0])

        try:
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    def put_many(
        self, rows: list[dict], *, connection: sqlite3.Connection | None = None
    ) -> list[str]:
        """Unlike the Postgres adapter's executemany()-with-RETURNING
        approach (which pipelines everything into one round trip),
        there's no equivalent pipelining benefit to chase for a local
        file -- there's no network round trip to batch away. Each row
        gets id/timestamp generated in Python then inserted with its own
        execute() call, all within one transaction/connection (one
        _run() call), which is what actually matters here: one file
        write/fsync cycle for the whole batch, not one per row."""
        if not rows:
            return []

        if self._dedup:
            return self._put_many_dedup(rows, connection=connection)

        now = _now_iso()
        prepared = [(str(uuid.uuid4()), row) for row in rows]

        def work(conn):
            ids = []
            for image_id, row in prepared:
                conn.execute(
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
                ids.append(image_id)
            return ids

        try:
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    def _put_many_dedup(
        self, rows: list[dict], *, connection: sqlite3.Connection | None
    ) -> list[str]:
        """Repeated identical checksums within one batch correctly
        accumulate ref_count -- each row's upsert is a separate,
        sequential statement execution within the same transaction, so
        it sees the prior row's effect, same as the Postgres adapter's
        equivalent (verified there empirically; the underlying mechanism
        -- sequential statements within one transaction -- is identical
        here, so this inherits that guarantee rather than needing to
        re-derive it from scratch)."""
        now = _now_iso()
        prepared = [(str(uuid.uuid4()), row) for row in rows]

        def work(conn):
            ids = []
            for image_id, row in prepared:
                conn.execute(
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
                cur = conn.execute(
                    _DEDUP_INSERT_REF,
                    (
                        image_id,
                        row["checksum_sha256"],
                        row["original_filename"],
                        now,
                        now,
                    ),
                )
                ids.append(str(cur.fetchone()[0]))
            return ids

        try:
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    # ---- get --------------------------------------------------------------

    def get(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> StoredRecord | None:
        select_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL

        def work(conn):
            cur = conn.execute(select_sql, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image: {exc}") from exc
        if row is None:
            return None
        data = row[1]
        if not self._dedup and row[8] == "object_storage":
            data = self._fetch_tiered_bytes(row[9], image_id=image_id)
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

    def _fetch_tiered_bytes(self, object_storage_key: str, *, image_id: str) -> bytes:
        """Shared by get()/get_many() -- identical purpose and error
        message to PostgresBackend's own helper of the same name. Only
        ever called in classic mode -- dedup mode can never have a
        tiered row (rejected at construction)."""
        if self._object_storage is None:
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) but this backend was constructed "
                "without object_storage=... -- configure it with the same "
                "bucket/credentials used to tier this image."
            )
        return self._object_storage.download(object_storage_key)

    def get_many(
        self, image_ids: list[str], *, connection: sqlite3.Connection | None = None
    ) -> list[StoredRecord]:
        if not image_ids:
            return []
        placeholders = ",".join("?" * len(image_ids))
        if self._dedup:
            sql = _DEDUP_SELECT_FULL.replace(
                "WHERE r.id = ?", f"WHERE r.id IN ({placeholders})"
            )
        else:
            sql = _SELECT_FULL.replace("WHERE id = ?", f"WHERE id IN ({placeholders})")

        def work(conn):
            cur = conn.execute(sql, image_ids)
            return cur.fetchall()

        try:
            rows = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image batch: {exc}") from exc
        records = []
        for row in rows:
            data = row[1]
            if not self._dedup and row[8] == "object_storage":
                data = self._fetch_tiered_bytes(row[9], image_id=str(row[0]))
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

    def get_metadata(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> StoredRecordMetadata | None:
        select_sql = _DEDUP_SELECT_METADATA if self._dedup else _SELECT_METADATA

        def work(conn):
            cur = conn.execute(select_sql, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work)
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

    def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
        connection: sqlite3.Connection | None = None,
    ) -> Iterator[bytes] | None:
        """Uses `sqlite3.Connection.blobopen()` -- a real incremental-
        BLOB-read API, not a range-query workaround. Unlike the Postgres
        adapter (which does one `_run()` call PER CHUNK, each its own
        pooled connection), this holds ONE connection open for the
        WHOLE stream -- necessary because the Blob handle is tied to
        the connection that opened it. If `connection=None`, this method
        opens and owns that connection itself, closing it when the
        generator is exhausted (or garbage-collected/explicitly closed
        without being fully consumed -- the `finally` block below covers
        both).

        Same not-found contract as the Postgres adapter's version:
        returns None immediately if the id doesn't exist (checked
        eagerly, before any generator is even created).

        MID-STREAM CONCURRENT DELETION BEHAVES DIFFERENTLY FROM
        POSTGRES, verified directly rather than assumed to match: on
        Postgres, each chunk is its own round trip/query, so a row
        deleted by another connection mid-stream causes the NEXT chunk
        fetch to see no row and raise StorageError. On SQLite, this
        method holds ONE connection open for the whole stream, and
        `blobopen()`'s underlying read transaction gives it a
        consistent snapshot (in WAL mode) of the row as it was when
        streaming began -- a concurrent DELETE from another connection
        does NOT interrupt or truncate an in-progress stream here; the
        stream completes successfully with the complete original
        bytes. Neither behavior is a bug -- they're honest consequences
        of each database's actual isolation model, not a deliberate
        design choice to differ, and this difference should not be
        assumed away by code written against one backend and later
        pointed at the other.

        For a TIERED row, delegates to ObjectStorage.download_stream()
        exactly the way the Postgres adapter's version does -- same
        real HTTP byte-Range benefit, same requirement that this
        backend was constructed with object_storage=... to read it back.
        Once a row is tiered, streaming goes through S3, not SQLite, so
        the snapshot-isolation behavior above only applies to
        non-tiered rows.
        """
        owns_connection = connection is None
        conn = connection if connection is not None else self._connect()

        if self._dedup:
            try:
                cur = conn.execute(_DEDUP_STREAM_INFO, (image_id,))
                dedup_row = cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                if owns_connection:
                    conn.close()
                raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc
            if dedup_row is None:
                if owns_connection:
                    conn.close()
                return None
            rowid, total_size = dedup_row
            table = "zerobucket_blobs"
        else:
            try:
                cur = conn.execute(_SELECT_STREAM_INFO, (image_id,))
                info_row = cur.fetchone()
            except Exception as exc:  # noqa: BLE001
                if owns_connection:
                    conn.close()
                raise StorageError(f"Failed to retrieve image metadata: {exc}") from exc

            if info_row is None:
                if owns_connection:
                    conn.close()
                return None

            rowid, total_size, storage_backend, object_storage_key = info_row

            if storage_backend == "object_storage":
                if owns_connection:
                    conn.close()
                if self._object_storage is None:
                    raise StorageError(
                        f"Image {image_id!r} is stored in object storage (key="
                        f"{object_storage_key!r}) but this backend was "
                        "constructed without object_storage=... -- configure "
                        "it with the same bucket/credentials used to tier "
                        "this image."
                    )
                return self._object_storage.download_stream(
                    object_storage_key, chunk_size=chunk_size
                )
            table = "zerobucket_images"

        def generator() -> Iterator[bytes]:
            try:
                blob = conn.blobopen(table, "data", rowid, readonly=True)
            except Exception as exc:  # noqa: BLE001
                if owns_connection:
                    conn.close()
                raise StorageError(f"Failed to stream image: {exc}") from exc
            try:
                delivered = 0
                while delivered < total_size:
                    try:
                        chunk = blob.read(min(chunk_size, total_size - delivered))
                    except Exception as exc:  # noqa: BLE001
                        raise StorageError(f"Failed to stream image: {exc}") from exc
                    if not chunk:
                        raise StorageError(
                            f"Image {image_id!r} was deleted while streaming "
                            f"(delivered {delivered} of {total_size} bytes)."
                        )
                    yield chunk
                    delivered += len(chunk)
            finally:
                blob.close()
                if owns_connection:
                    conn.close()

        return generator()

    # ---- tier_to_object_storage ------------------------------------------

    def tier_to_object_storage(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> bool | None:
        """See PostgresBackend.tier_to_object_storage()'s docstring for
        the full contract (None = not found, False = already tiered
        no-op, True = tiered just now) -- identical return semantics.

        THE ONE REAL DIFFERENCE FROM POSTGRES, stated plainly: this uses
        `BEGIN IMMEDIATE` instead of `SELECT ... FOR UPDATE`, because
        SQLite has no per-row locking at all. `BEGIN IMMEDIATE` acquires
        SQLite's RESERVED lock, which blocks every OTHER write to the
        ENTIRE database file -- not just to this one row -- for as long
        as this transaction is open, i.e. for the full duration of the
        object-storage upload (a real network call). On Postgres, only
        the one row being tiered is locked; every other row is
        unaffected. On SQLite, tiering one image blocks ALL other writes
        to ALL images (reads are unaffected -- WAL mode allows concurrent
        readers alongside a writer) until the upload finishes.

        This is accepted as the honest cost of preserving the same
        safety guarantee (a failed upload leaves the row completely
        untouched, no window where bytes exist in neither location) with
        the locking primitive SQLite actually has -- not glossed over as
        equivalent to Postgres's behavior, because it isn't.

        Requires `isolation_level=None` (autocommit mode) on the
        connection so `BEGIN IMMEDIATE` can be issued explicitly without
        conflicting with Python sqlite3's own implicit transaction
        management. If `connection=None` (the common case), this method
        opens its own connection configured that way. If you pass your
        own `connection=`, set `conn.isolation_level = None` on it
        yourself first, or this will raise.
        """
        if self._object_storage is None:
            raise StorageError(
                "tier_to_object_storage() requires this backend to be "
                "constructed with object_storage=... -- see ObjectStorage "
                "in object_storage.py."
            )

        owns_connection = connection is None
        conn = connection
        if owns_connection:
            conn = sqlite3.connect(self._database_path, isolation_level=None)
            conn.execute("PRAGMA journal_mode=WAL;")

        try:
            conn.execute("BEGIN IMMEDIATE;")
            try:
                cur = conn.execute(_SELECT_FOR_TIERING, (image_id,))
                row = cur.fetchone()
                if row is None:
                    conn.execute("ROLLBACK;")
                    return None
                data, mime_type, size_bytes, storage_backend = row
                if storage_backend != "sqlite":
                    conn.execute("ROLLBACK;")
                    return False

                key = str(image_id)
                self._object_storage.upload(key, bytes(data), mime_type=mime_type)
                conn.execute(
                    _UPDATE_AFTER_TIERING,
                    (self._object_storage.bucket, key, _now_iso(), image_id),
                )
                conn.execute("COMMIT;")
                return True
            except Exception:
                conn.execute("ROLLBACK;")
                raise
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to tier image to object storage: {exc}"
            ) from exc
        finally:
            if owns_connection:
                conn.close()

    # ---- delete -------------------------------------------------------------

    def delete(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> bool:
        if self._dedup:
            return self._delete_dedup(image_id, connection=connection)

        def work(conn):
            cur = conn.execute(_DELETE_RETURNING, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc
        if row is None:
            return False
        storage_backend, object_storage_key = row
        if storage_backend == "object_storage" and self._object_storage is not None:
            # Same best-effort, after-the-fact ordering as PostgresBackend's
            # delete() -- see its docstring for the reasoning (a harmless
            # orphaned S3 object beats a row that claims to exist but
            # points at nothing).
            self._object_storage.delete(object_storage_key)
        return True

    def _delete_dedup(
        self, image_id: str, *, connection: sqlite3.Connection | None
    ) -> bool:
        def work(conn):
            cur = conn.execute(_DEDUP_DELETE_REF, (image_id,))
            row = cur.fetchone()
            if row is None:
                return False  # id didn't exist -- nothing to decrement either
            checksum = row[0]
            cur = conn.execute(_DEDUP_DECREMENT_BLOB, (1, checksum))
            new_ref_count = cur.fetchone()[0]
            if new_ref_count <= 0:
                conn.execute(
                    "DELETE FROM zerobucket_blobs WHERE checksum_sha256 = ? AND ref_count <= 0;",
                    (checksum,),
                )
            return True

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    def delete_many(
        self, image_ids: list[str], *, connection: sqlite3.Connection | None = None
    ) -> list[str]:
        if not image_ids:
            return []

        if self._dedup:
            return self._delete_many_dedup(image_ids, connection=connection)

        placeholders = ",".join("?" * len(image_ids))
        sql = (
            f"DELETE FROM zerobucket_images WHERE id IN ({placeholders}) "
            "RETURNING id, storage_backend, object_storage_key;"
        )

        def work(conn):
            cur = conn.execute(sql, image_ids)
            return cur.fetchall()

        try:
            rows = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc
        deleted_ids = []
        for row in rows:
            image_id, storage_backend, object_storage_key = row
            deleted_ids.append(str(image_id))
            if storage_backend == "object_storage" and self._object_storage is not None:
                self._object_storage.delete(object_storage_key)
        return deleted_ids

    def _delete_many_dedup(
        self, image_ids: list[str], *, connection: sqlite3.Connection | None
    ) -> list[str]:
        """Multiple deleted refs can share the same checksum (two ids
        both pointing at the same de-duplicated blob) -- counts
        occurrences per checksum so each blob's ref_count is decremented
        by the correct total, not by 1 per distinct checksum. Same
        reasoning as PostgresBackend's equivalent."""
        placeholders = ",".join("?" * len(image_ids))

        def work(conn):
            cur = conn.execute(
                f"DELETE FROM zerobucket_image_refs WHERE id IN ({placeholders}) "
                "RETURNING id, checksum_sha256;",
                image_ids,
            )
            deleted_rows = cur.fetchall()
            if not deleted_rows:
                return []

            deleted_ids = [str(row[0]) for row in deleted_rows]
            checksum_counts = Counter(row[1] for row in deleted_rows)

            empty_checksums = []
            for checksum, count in checksum_counts.items():
                cur = conn.execute(_DEDUP_DECREMENT_BLOB, (count, checksum))
                new_ref_count = cur.fetchone()[0]
                if new_ref_count <= 0:
                    empty_checksums.append(checksum)

            if empty_checksums:
                empty_placeholders = ",".join("?" * len(empty_checksums))
                conn.execute(
                    f"DELETE FROM zerobucket_blobs WHERE checksum_sha256 IN "
                    f"({empty_placeholders}) AND ref_count <= 0;",
                    empty_checksums,
                )
            return deleted_ids

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    # ---- exists -------------------------------------------------------------

    def exists(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> bool:
        exists_sql = _DEDUP_EXISTS if self._dedup else _EXISTS

        def work(conn):
            cur = conn.execute(exists_sql, (image_id,))
            return cur.fetchone() is not None

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    def close(self) -> None:
        """No-op: this backend holds no persistent connection/pool to
        release (see module docstring -- a fresh connection is opened
        and closed per operation). Exists only to satisfy
        StorageBackend's interface."""
