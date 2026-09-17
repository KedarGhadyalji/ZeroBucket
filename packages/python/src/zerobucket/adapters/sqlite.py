"""SQLite storage adapter.

PHASE 1 OF A MULTI-PHASE BUILD, stated directly rather than implied:
this file currently implements classic-mode core CRUD only (put,
put_many, get, get_many, get_metadata, delete, delete_many, exists).
NOT yet implemented, tracked as explicit follow-up phases, not silent
gaps: get_stream(), dedup mode, tier_to_object_storage(), async support
(aiosqlite), and the before_get/before_put hook interaction hasn't been
exercised against this backend yet either (the hooks live entirely in
client.py and are backend-agnostic by construction, so they're expected
to work unmodified -- but "expected to" isn't the same as "tested
against," and this docstring won't claim tested until it's been done).

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
  (it's fundamentally a single-writer database). Relevant once tiering
  is implemented in a later phase (see this module's docstring above);
  not relevant to anything implemented so far in this file, since
  nothing here yet needs a lock stronger than SQLite's normal implicit
  transaction behavior.
- `= ANY(array)` isn't SQLite syntax -- get_many()/delete_many() build
  a dynamically-sized `IN (?, ?, ...)` placeholder list instead.
- `RETURNING` IS supported (SQLite >= 3.35, released March 2021) and is
  used the same way the Postgres adapter uses it.
"""

from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

from ..exceptions import StorageError
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024

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

_INSERT = """
INSERT INTO zerobucket_images
    (id, data, mime_type, original_filename, size_bytes, width, height,
     checksum_sha256, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id;
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = ?;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = ?;
"""

_DELETE = "DELETE FROM zerobucket_images WHERE id = ?;"

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

    def __init__(self, database_path: str, *, auto_migrate: bool = True) -> None:
        self._database_path = database_path
        if auto_migrate:
            conn = self._connect()
            try:
                conn.executescript(_SCHEMA)
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

    # ---- get --------------------------------------------------------------

    def get(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> StoredRecord | None:
        def work(conn):
            cur = conn.execute(_SELECT_FULL, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work)
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

    def get_many(
        self, image_ids: list[str], *, connection: sqlite3.Connection | None = None
    ) -> list[StoredRecord]:
        if not image_ids:
            return []
        placeholders = ",".join("?" * len(image_ids))
        sql = _SELECT_FULL.replace("WHERE id = ?", f"WHERE id IN ({placeholders})")

        def work(conn):
            cur = conn.execute(sql, image_ids)
            return cur.fetchall()

        try:
            rows = self._run(connection, work)
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

    def get_metadata(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> StoredRecordMetadata | None:
        def work(conn):
            cur = conn.execute(_SELECT_METADATA, (image_id,))
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
        raise NotImplementedError(
            "get_stream() is not yet implemented for SQLiteBackend -- "
            "tracked as a follow-up phase, see adapters/sqlite.py's "
            "module docstring. Use get() instead for now."
        )

    # ---- delete -------------------------------------------------------------

    def delete(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> bool:
        def work(conn):
            cur = conn.execute(_DELETE, (image_id,))
            return cur.rowcount > 0

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    def delete_many(
        self, image_ids: list[str], *, connection: sqlite3.Connection | None = None
    ) -> list[str]:
        if not image_ids:
            return []
        placeholders = ",".join("?" * len(image_ids))
        sql = (
            f"DELETE FROM zerobucket_images WHERE id IN ({placeholders}) RETURNING id;"
        )

        def work(conn):
            cur = conn.execute(sql, image_ids)
            return [str(row[0]) for row in cur.fetchall()]

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    # ---- exists -------------------------------------------------------------

    def exists(
        self, image_id: str, *, connection: sqlite3.Connection | None = None
    ) -> bool:
        def work(conn):
            cur = conn.execute(_EXISTS, (image_id,))
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
