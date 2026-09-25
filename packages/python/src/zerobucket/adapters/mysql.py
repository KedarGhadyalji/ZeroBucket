"""MySQL / MariaDB storage adapter.

PHASE 2 OF A MULTI-PHASE BUILD, stated directly rather than implied,
same convention used for the SQLite adapter. Phase 1 (v0.20.0) shipped
classic-mode core CRUD. This phase adds streaming reads
(`get_stream()`) and object-storage tiering (`tier_to_object_storage()`,
`object_storage=`). NOT yet implemented, tracked as explicit follow-up
phases rather than a silent gap: dedup mode, async support, connection
pooling.

Tested against MariaDB 10.11 (this project's sandbox); written against
MySQL/MariaDB syntax common to both, and the specific divergences
between them are called out explicitly below rather than assumed away.

Uses PyMySQL (pure-Python, no compiled extension required) rather than
mysqlclient -- consistent with keeping the core library's install story
simple (same reasoning as boto3/aiosqlite being optional extras
elsewhere in this project). Installed via the `zerobucket[mysql]`
optional extra, not a hard dependency -- `import zerobucket` must not
require it, same rule as every other optional backend.

Stores image bytes directly in a LONGBLOB column. All queries are
parameterized; nothing is ever built via string concatenation (dynamic
`IN (%s, %s, ...)` placeholder lists for get_many()/delete_many() are
sized to the input and each element is still bound as its own
parameter, exactly the same pattern SQLite's adapter uses for the same
reason -- see that module's docstring).

WHY THIS ADAPTER'S DESIGN DIFFERS FROM POSTGRES'S AND SQLITE'S, stated
explicitly rather than left for someone to wonder about while reading
unfamiliar code:

- No `gen_random_uuid()` equivalent used -- same choice SQLite made,
  for the same reason (not guaranteed available/consistent across
  MySQL 8 vs. MariaDB versions): ids are generated in Python
  (`uuid.uuid4()`) and stored as CHAR(36), not left to the database.
- No native UUID type -- CHAR(36) storing the UUID's string form, same
  reasoning as SQLite's TEXT id column.
- `RETURNING` is NOT used. MariaDB 10.5+ supports `INSERT/DELETE ...
  RETURNING`, but real MySQL (8.0, as of this writing) does not support
  RETURNING on ANY statement -- confirmed directly, not assumed, since
  this adapter is meant to work against both, not just the MariaDB
  instance available in this sandbox. Concretely: put()/put_many()
  don't need it (the id is already known -- generated in Python before
  the INSERT), and delete()/delete_many()/tier_to_object_storage() use
  `SELECT ... FOR UPDATE` immediately before the statement that follows
  inside one transaction instead -- this closes the same race a
  RETURNING-based statement would close (the row lock guarantees a row
  selected as existing/untiered is still there, unchanged, when the
  next statement acts on it), just as two statements instead of one.
- No connection pool in this phase. Unlike SQLite (a local file, where
  "no pool" is a deliberate, durable design choice -- see that
  adapter's module docstring), MySQL is a genuinely networked database
  where connection setup (TCP handshake + auth) has a real, non-trivial
  cost. This phase still opens a fresh PyMySQL connection per operation
  when connection=None purely to keep scope focused on streaming and
  tiering correctness, not because that's judged to be the right
  long-term design -- pool_min_size/pool_max_size/pool_timeout
  (matching PostgresBackend's own knobs) are explicitly flagged as
  follow-up work, not silently absent.
- Tiering columns (storage_backend/object_storage_bucket/
  object_storage_key) were deliberately NOT baked into Phase 1's
  schema (see that phase's CHANGELOG entry for why -- it mirrors how
  PostgresBackend's own schema evolved historically, classic-mode
  first, tiering columns added later via an additive migration). This
  phase's `_migrate_tiering()` is that additive migration for MySQL.
  It does NOT rely on `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` --
  even though real MySQL 8.0 and MariaDB 10.0.2+ both do support that
  clause, this project would rather check column/constraint existence
  directly via `information_schema` than depend on a specific minimum
  version of either engine being confirmed correct for every
  reader's actual deployment. Slightly more code, no version-support
  guessing.
- The tiering CHECK constraint requires MySQL 8.0.16+ or MariaDB
  10.2.1+ to actually be ENFORCED (both engines parse but silently
  ignore CHECK constraints before those versions -- confirmed against
  MySQL's own reference documentation). Stated as a minimum-version
  requirement for tiering specifically, not silently assumed to work
  everywhere.
- Index creation is INLINE inside `CREATE TABLE IF NOT EXISTS`
  (`INDEX name (col)`), not a separate `CREATE INDEX IF NOT EXISTS ...`
  statement the way the Postgres/SQLite adapters both do it. This is a
  genuine, verified MySQL/MariaDB divergence, not a style preference:
  MariaDB supports `CREATE INDEX IF NOT EXISTS` (10.1.4+), but real
  MySQL 8.0 does NOT support `IF NOT EXISTS` on `CREATE INDEX` at all
  -- confirmed against MySQL's own reference documentation. Defining
  indexes inline sidesteps the incompatibility entirely: the whole
  table (columns + indexes) is created idempotently as one unit via
  `CREATE TABLE IF NOT EXISTS`, which both engines support identically.
- `get_stream()` for a non-tiered row uses `SUBSTRING(data FROM %s FOR
  %s)` -- the same SQL-standard ranged-read form Postgres's
  `substring()`-based streaming uses, and MySQL/MariaDB both support it
  identically (confirmed directly, not assumed from the Postgres
  adapter's success with the same syntax). For a TIERED row, this
  delegates to `ObjectStorage.download_stream()` instead -- real S3
  byte-Range requests, strictly better than the SUBSTRING approach,
  same as every other adapter's tiered `get_stream()` path.
- `tier_to_object_storage()`'s row lock is a genuine, real per-row
  `SELECT ... FOR UPDATE` via InnoDB -- unlike SQLite's `BEGIN
  IMMEDIATE` workaround (which locks the WHOLE database file because
  SQLite has no row-level locking at all), MySQL/MariaDB's InnoDB
  storage engine has real row-level locking, so this adapter's tiering
  safety guarantee matches Postgres's exactly: tiering one image never
  blocks writes to any other row. Confirmed directly with a dedicated
  test (a second connection can write to a DIFFERENT row while tiering
  is in progress on this one), not assumed just because InnoDB is
  "supposed to" support row locks.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

from ..exceptions import StorageError
from ..object_storage import ObjectStorage
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

if TYPE_CHECKING:
    import pymysql

DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024  # 1MB, same default as every other adapter

_SCHEMA = """
CREATE TABLE IF NOT EXISTS zerobucket_images (
    id                  CHAR(36) PRIMARY KEY,
    data                LONGBLOB NOT NULL,
    mime_type           VARCHAR(255) NOT NULL,
    original_filename   TEXT,
    size_bytes          INT NOT NULL,
    width               INT,
    height              INT,
    checksum_sha256     CHAR(64) NOT NULL,
    created_at          DATETIME(6) NOT NULL,
    updated_at          DATETIME(6) NOT NULL,
    INDEX idx_zerobucket_checksum (checksum_sha256),
    INDEX idx_zerobucket_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

_TIERING_CHECK_CONSTRAINT = "chk_zerobucket_storage"

_INSERT = """
INSERT INTO zerobucket_images
    (id, data, mime_type, original_filename, size_bytes, width, height,
     checksum_sha256, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height,
       checksum_sha256, storage_backend, object_storage_key
FROM zerobucket_images
WHERE id = %s;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = %s;
"""

_SELECT_STREAM_INFO = """
SELECT size_bytes, storage_backend, object_storage_key
FROM zerobucket_images
WHERE id = %s;
"""

# SUBSTRING() is 1-indexed and clamps `length` at the value's actual
# end, so the last chunk of a stream naturally comes back shorter
# without any special-casing here -- same behavior as Postgres's
# substring(), confirmed directly against MariaDB rather than assumed
# to carry over just because the SQL is spelled the same way.
_SELECT_CHUNK = """
SELECT SUBSTRING(data FROM %s FOR %s)
FROM zerobucket_images
WHERE id = %s;
"""

# Everything tier_to_object_storage() needs to perform the move -- see
# that method's docstring. Separate from _SELECT_FULL rather than
# reusing it, same reasoning as the Postgres adapter's equivalent
# constant: only ever called on a row already confirmed untiered, so
# it doesn't need the tiering columns back.
_SELECT_FOR_TIERING = """
SELECT data, mime_type, size_bytes, storage_backend
FROM zerobucket_images
WHERE id = %s
FOR UPDATE;
"""

_UPDATE_AFTER_TIERING = """
UPDATE zerobucket_images
SET data = NULL, storage_backend = 'object_storage',
    object_storage_bucket = %s, object_storage_key = %s, updated_at = %s
WHERE id = %s;
"""

_DELETE = "DELETE FROM zerobucket_images WHERE id = %s;"

_EXISTS = "SELECT 1 FROM zerobucket_images WHERE id = %s;"


def _table_columns(cur) -> set[str]:
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images';"
    )
    return {row[0] for row in cur.fetchall()}


def _constraint_exists(cur, name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.TABLE_CONSTRAINTS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images' "
        "AND CONSTRAINT_NAME = %s;",
        (name,),
    )
    return cur.fetchone() is not None


def _migrate_tiering(cur) -> None:
    """Additive, idempotent migration adding tiering support to an
    existing classic-mode (Phase 1) table. Existing rows are untouched
    -- they satisfy the new CHECK constraint via storage_backend's
    column default ('mysql') and NULL tiering columns, same pattern
    every other adapter's tiering migration uses. See module docstring
    for why this checks information_schema directly rather than
    relying on `ADD COLUMN IF NOT EXISTS`."""
    existing = _table_columns(cur)
    if "storage_backend" not in existing:
        cur.execute(
            "ALTER TABLE zerobucket_images "
            "ADD COLUMN storage_backend VARCHAR(20) NOT NULL DEFAULT 'mysql';"
        )
    if "object_storage_bucket" not in existing:
        cur.execute(
            "ALTER TABLE zerobucket_images ADD COLUMN object_storage_bucket VARCHAR(255);"
        )
    if "object_storage_key" not in existing:
        cur.execute(
            "ALTER TABLE zerobucket_images ADD COLUMN object_storage_key VARCHAR(512);"
        )
    # Idempotent by nature -- re-running MODIFY on an already-nullable
    # column is a harmless no-op, so no existence check needed here.
    cur.execute("ALTER TABLE zerobucket_images MODIFY data LONGBLOB NULL;")
    if not _constraint_exists(cur, _TIERING_CHECK_CONSTRAINT):
        cur.execute(
            f"ALTER TABLE zerobucket_images ADD CONSTRAINT {_TIERING_CHECK_CONSTRAINT} "
            "CHECK ("
            "  (storage_backend = 'mysql' AND data IS NOT NULL "
            "     AND object_storage_bucket IS NULL AND object_storage_key IS NULL)"
            "  OR"
            "  (storage_backend = 'object_storage' AND data IS NULL "
            "     AND object_storage_bucket IS NOT NULL AND object_storage_key IS NOT NULL)"
            ");"
        )


def _now() -> datetime:
    # Naive UTC datetime -- PyMySQL binds Python datetime objects
    # directly to DATETIME columns; storing everything as UTC (and
    # never relying on the server's own timezone setting) keeps this
    # consistent regardless of how a given MySQL/MariaDB instance is
    # configured.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parse_database_url(database_url: str) -> dict:
    """Parses a `mysql://user:pass@host:port/dbname` URL into PyMySQL
    `connect()` kwargs. Not using SQLAlchemy's URL parser or similar --
    this project keeps its core dependency footprint small (see module
    docstring), and the URL shape needed here is simple enough not to
    justify pulling one in."""
    parsed = urlparse(database_url)
    if parsed.scheme not in ("mysql", "mariadb"):
        raise StorageError(
            f"Unsupported database URL scheme {parsed.scheme!r} -- expected "
            "'mysql://' or 'mariadb://'."
        )
    return {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or 3306,
        "user": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else "",
        "database": parsed.path.lstrip("/") or None,
    }


class MySQLBackend(StorageBackend):
    """Storage backend for MySQL/MariaDB. See module docstring for what
    this phase implements and what it deliberately doesn't yet.

    object_storage: optional ObjectStorage instance (see
    object_storage.py), enabling tiering -- see
    tier_to_object_storage(). dedup=True is not yet supported for this
    backend (Phase 3), so there's no dedup+tiering interaction to guard
    against here yet, unlike PostgresBackend/SQLiteBackend.

    `connection=` here means a `pymysql.connections.Connection` (not a
    Postgres or SQLite connection) -- the interface in base.py types
    this as `object` specifically so each adapter can narrow it to its
    own driver's type, same pattern every other adapter uses.
    """

    def __init__(
        self,
        database_url: str,
        *,
        auto_migrate: bool = True,
        object_storage: ObjectStorage | None = None,
    ) -> None:
        try:
            import pymysql
            import pymysql.cursors
        except ImportError as exc:
            raise StorageError(
                "MySQL/MariaDB support requires PyMySQL. Install it with "
                "`pip install zerobucket[mysql]` (or `pip install pymysql` "
                "directly). Plain `import zerobucket` and every other "
                "backend work without it -- this is an optional extra, "
                "same as boto3 for object-storage tiering or aiosqlite "
                "for async SQLite."
            ) from exc
        self._pymysql = pymysql
        self._connect_kwargs = _parse_database_url(database_url)
        self._object_storage = object_storage
        if auto_migrate:
            conn = self._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(_SCHEMA)
                    _migrate_tiering(cur)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                raise StorageError(f"Migration failed: {exc}") from exc
            finally:
                conn.close()

    def _connect(self) -> pymysql.connections.Connection:
        try:
            return self._pymysql.connect(
                **self._connect_kwargs,
                autocommit=False,
                charset="utf8mb4",
                cursorclass=self._pymysql.cursors.Cursor,
            )
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to connect to MySQL/MariaDB: {exc}") from exc

    def _run(self, connection, work):
        """Mirrors every other adapter's `_run()` shape (a `work(conn)`
        callable, connection=None vs connection=provided). No retry
        loop, no on_operation metrics, no pool -- none are implemented
        in this phase (see module docstring)."""
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

    def _fetch_tiered_bytes(self, object_storage_key: str, *, image_id: str) -> bytes:
        """Shared by get()/get_many() for a row with
        storage_backend='object_storage'. Raises a clear StorageError
        (rather than a confusing downstream AttributeError on a None)
        if this backend wasn't configured with object_storage=..."""
        if self._object_storage is None:
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) but this backend was constructed "
                "without object_storage=... -- configure it with the same "
                "bucket/credentials used to tier this image."
            )
        return self._object_storage.download(object_storage_key)

    # ---- put ----------------------------------------------------------

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
        connection: pymysql.connections.Connection | None = None,
    ) -> str:
        image_id = str(uuid.uuid4())
        now = _now()

        def work(conn):
            with conn.cursor() as cur:
                cur.execute(
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
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    def put_many(
        self,
        rows: list[dict],
        *,
        connection: pymysql.connections.Connection | None = None,
    ) -> list[str]:
        """One transaction for the whole batch (one `_run()` call), each
        row still its own `execute()` -- same reasoning SQLite's adapter
        gives for the same shape: there's no pipelining win to chase
        here the way Postgres's executemany()+RETURNING gets one, but
        batching every row into a single commit is still what matters."""
        if not rows:
            return []

        now = _now()
        prepared = [(str(uuid.uuid4()), row) for row in rows]

        def work(conn):
            with conn.cursor() as cur:
                for image_id, row in prepared:
                    cur.execute(
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
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    # ---- get ------------------------------------------------------------

    def get(
        self, image_id: str, *, connection: pymysql.connections.Connection | None = None
    ) -> StoredRecord | None:
        def work(conn):
            with conn.cursor() as cur:
                cur.execute(_SELECT_FULL, (image_id,))
                return cur.fetchone()

        try:
            row = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image: {exc}") from exc
        if row is None:
            return None
        (
            row_id,
            data,
            mime_type,
            original_filename,
            size_bytes,
            width,
            height,
            checksum_sha256,
            storage_backend,
            object_storage_key,
        ) = row
        if storage_backend == "object_storage":
            data = self._fetch_tiered_bytes(object_storage_key, image_id=str(row_id))
        else:
            data = bytes(data)
        return StoredRecord(
            id=str(row_id),
            data=data,
            mime_type=mime_type,
            original_filename=original_filename,
            size_bytes=size_bytes,
            width=width,
            height=height,
            checksum_sha256=checksum_sha256,
        )

    def get_many(
        self,
        image_ids: list[str],
        *,
        connection: pymysql.connections.Connection | None = None,
    ) -> list[StoredRecord]:
        if not image_ids:
            return []
        placeholders = ",".join(["%s"] * len(image_ids))
        sql = _SELECT_FULL.replace("WHERE id = %s", f"WHERE id IN ({placeholders})")

        def work(conn):
            with conn.cursor() as cur:
                cur.execute(sql, image_ids)
                return cur.fetchall()

        try:
            rows = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to retrieve image batch: {exc}") from exc
        results = []
        for row in rows:
            (
                row_id,
                data,
                mime_type,
                original_filename,
                size_bytes,
                width,
                height,
                checksum_sha256,
                storage_backend,
                object_storage_key,
            ) = row
            if storage_backend == "object_storage":
                data = self._fetch_tiered_bytes(
                    object_storage_key, image_id=str(row_id)
                )
            else:
                data = bytes(data)
            results.append(
                StoredRecord(
                    id=str(row_id),
                    data=data,
                    mime_type=mime_type,
                    original_filename=original_filename,
                    size_bytes=size_bytes,
                    width=width,
                    height=height,
                    checksum_sha256=checksum_sha256,
                )
            )
        return results

    def get_metadata(
        self, image_id: str, *, connection: pymysql.connections.Connection | None = None
    ) -> StoredRecordMetadata | None:
        def work(conn):
            with conn.cursor() as cur:
                cur.execute(_SELECT_METADATA, (image_id,))
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
        connection: pymysql.connections.Connection | None = None,
    ) -> Iterator[bytes] | None:
        """See StorageBackend.get_stream for the contract. Implementation
        mirrors the Postgres adapter's: one metadata lookup (also the
        not-found check), then repeated `SUBSTRING(data FROM ... FOR
        ...)` queries walking forward through the value -- see module
        docstring for confirmation this syntax behaves identically on
        MySQL/MariaDB. For a TIERED row, delegates entirely to
        `ObjectStorage.download_stream()` instead (real S3 byte-Range
        requests). If the row disappears mid-stream (concurrent delete,
        no connection= holding a snapshot), the next chunk query returns
        no row and this raises StorageError rather than silently
        yielding a short read."""

        def info_work(conn):
            with conn.cursor() as cur:
                cur.execute(_SELECT_STREAM_INFO, (image_id,))
                return cur.fetchone()

        try:
            info_row = self._run(connection, info_work)
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
            return self._object_storage.download_stream(
                object_storage_key, chunk_size=chunk_size
            )

        def generator() -> Iterator[bytes]:
            offset = 1  # SUBSTRING() is 1-indexed
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                def work(conn, offset=offset, length=length):
                    with conn.cursor() as cur:
                        cur.execute(_SELECT_CHUNK, (offset, length, image_id))
                        return cur.fetchone()

                try:
                    row = self._run(connection, work)
                except Exception as exc:  # noqa: BLE001
                    raise StorageError(f"Failed to stream image: {exc}") from exc

                if row is None:
                    raise StorageError(
                        f"Image {image_id!r} was deleted while streaming "
                        f"(delivered {delivered} of {total_size} bytes). "
                        "Pass connection= with your own open transaction "
                        "if you need a consistent read across concurrent "
                        "writers."
                    )

                chunk = bytes(row[0])
                yield chunk
                offset += len(chunk)
                remaining -= len(chunk)
                delivered += len(chunk)

        return generator()

    # ---- tier_to_object_storage ------------------------------------------

    def tier_to_object_storage(
        self, image_id: str, *, connection: pymysql.connections.Connection | None = None
    ) -> bool | None:
        """Move an image's bytes out of MySQL/MariaDB and into object
        storage. Returns None if image_id doesn't exist, False if it
        exists but was ALREADY tiered (a safe no-op -- lets a backfill
        script re-run without choking on rows it already handled), True
        if it was actually tiered just now. Requires object_storage= to
        have been passed to this backend's constructor.

        SAFETY: the upload happens INSIDE the same transaction as the
        row lookup/lock (`SELECT ... FOR UPDATE`) and the subsequent
        UPDATE that flips storage_backend -- all as one `_run()` call.
        If the object-storage upload fails, the exception propagates,
        the whole transaction rolls back, and the row is left
        completely untouched -- exactly mirroring the Postgres
        adapter's guarantee (see that method's docstring for the full
        reasoning, identical here).

        A REAL, VERIFIED DIFFERENCE FROM SQLITE'S VERSION, worth stating
        directly: MySQL/MariaDB's InnoDB storage engine has genuine
        per-row locking via `SELECT ... FOR UPDATE`, unlike SQLite
        (which has no row-level locking at all and falls back to
        locking the whole database file via `BEGIN IMMEDIATE`). This
        method's lock -- like Postgres's -- only blocks a concurrent
        get()/delete()/tier_to_object_storage() call on this SAME
        image_id; every other row is completely unaffected. Confirmed
        directly with a dedicated test, not assumed just because InnoDB
        is documented to support row locks.
        """
        if self._object_storage is None:
            raise StorageError(
                "tier_to_object_storage() requires this backend to be "
                "constructed with object_storage=... -- see ObjectStorage "
                "in object_storage.py."
            )
        object_storage = self._object_storage  # narrow for closure below
        now = _now()

        def work(conn):
            with conn.cursor() as cur:
                cur.execute(_SELECT_FOR_TIERING, (image_id,))
                row = cur.fetchone()
                if row is None:
                    return None
                data, mime_type, size_bytes, storage_backend = row
                if storage_backend != "mysql":
                    return False
                key = str(image_id)
                object_storage.upload(key, bytes(data), mime_type=mime_type)
                cur.execute(
                    _UPDATE_AFTER_TIERING, (object_storage.bucket, key, now, image_id)
                )
                return True

        try:
            return self._run(connection, work)
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to tier image to object storage: {exc}"
            ) from exc

    # ---- delete -----------------------------------------------------------

    def delete(
        self, image_id: str, *, connection: pymysql.connections.Connection | None = None
    ) -> bool:
        """Locks the row with SELECT ... FOR UPDATE (needed to learn
        storage_backend/object_storage_key -- no RETURNING available,
        see module docstring) then deletes it. The object-storage
        cleanup happens AFTER this method's transaction has committed
        (i.e. after the MySQL row is already gone), same deliberate
        ordering as the Postgres adapter: the row being gone is what
        makes the image correctly "not found" from here on regardless
        of whether the S3 delete below succeeds; a failure here leaves
        a harmless orphaned object, not a data-integrity problem."""

        def work(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT storage_backend, object_storage_key "
                    "FROM zerobucket_images WHERE id = %s FOR UPDATE;",
                    (image_id,),
                )
                row = cur.fetchone()
                if row is None:
                    return None
                cur.execute(_DELETE, (image_id,))
                return row

        try:
            row = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc
        if row is None:
            return False
        storage_backend, object_storage_key = row
        if storage_backend == "object_storage" and self._object_storage is not None:
            self._object_storage.delete(object_storage_key)
        return True

    def delete_many(
        self,
        image_ids: list[str],
        *,
        connection: pymysql.connections.Connection | None = None,
    ) -> list[str]:
        """No RETURNING available (see module docstring), so this locks
        the candidate rows first with SELECT ... FOR UPDATE inside the
        same transaction as the DELETE that follows -- the row lock
        guarantees every id the SELECT found still exists, unchanged,
        by the time the DELETE removes it, closing the same
        concurrent-delete race a RETURNING-based statement would close,
        just as two statements sharing one transaction instead of one
        statement. Object-storage cleanup for any tiered rows happens
        one at a time, AFTER the transaction commits -- same ordering
        as delete()."""
        if not image_ids:
            return []
        placeholders = ",".join(["%s"] * len(image_ids))

        def work(conn):
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, storage_backend, object_storage_key "
                    f"FROM zerobucket_images WHERE id IN ({placeholders}) FOR UPDATE;",
                    image_ids,
                )
                rows = cur.fetchall()
                if not rows:
                    return []
                existing_ids = [str(row[0]) for row in rows]
                existing_placeholders = ",".join(["%s"] * len(existing_ids))
                cur.execute(
                    f"DELETE FROM zerobucket_images WHERE id IN ({existing_placeholders});",
                    existing_ids,
                )
                return rows

        try:
            rows = self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc
        deleted_ids = []
        for row_id, storage_backend, object_storage_key in rows:
            deleted_ids.append(str(row_id))
            if storage_backend == "object_storage" and self._object_storage is not None:
                self._object_storage.delete(object_storage_key)
        return deleted_ids

    # ---- exists -------------------------------------------------------------

    def exists(
        self, image_id: str, *, connection: pymysql.connections.Connection | None = None
    ) -> bool:
        def work(conn):
            with conn.cursor() as cur:
                cur.execute(_EXISTS, (image_id,))
                return cur.fetchone() is not None

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    def close(self) -> None:
        """No-op: this backend holds no persistent connection/pool to
        release in this phase (see module docstring -- a fresh
        connection is opened and closed per operation). Exists only to
        satisfy StorageBackend's interface."""
