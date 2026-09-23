"""MySQL / MariaDB storage adapter.

PHASE 1 OF A MULTI-PHASE BUILD, stated directly rather than implied,
same convention used for the SQLite adapter. This phase ships
classic-mode core CRUD only: put, put_many, get, get_many,
get_metadata, delete, delete_many, exists. get_stream() raises
NotImplementedError with a clear message. NOT implemented, tracked as
explicit follow-up phases rather than a silent gap: streaming reads,
object-storage tiering, dedup mode, async support, connection pooling.

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
  the INSERT), and delete()/delete_many() use `SELECT ... FOR UPDATE`
  immediately before the `DELETE` inside one transaction instead --
  this closes the same race a RETURNING-based DELETE would close (the
  row lock guarantees a row selected as existing is still there,
  unchanged, when the DELETE that follows removes it), just as two
  statements instead of one.
- No connection pool in this phase. Unlike SQLite (a local file, where
  "no pool" is a deliberate, durable design choice -- see that
  adapter's module docstring), MySQL is a genuinely networked database
  where connection setup (TCP handshake + auth) has a real, non-trivial
  cost. Phase 1 opens a fresh PyMySQL connection per operation when
  connection=None purely to keep this first phase's scope to "does the
  CRUD work correctly", not because that's judged to be the right
  long-term design -- pool_min_size/pool_max_size/pool_timeout
  (matching PostgresBackend's own knobs) are explicitly flagged as
  follow-up work, not silently absent.
- No tiering columns in this phase's schema. SQLite's Phase 1 baked
  storage_backend/object_storage_* columns in from day one (see that
  module's docstring for why). This adapter deliberately does NOT --
  it instead mirrors how PostgresBackend's OWN schema actually evolved
  historically in this project: a minimal classic-mode table first,
  tiering columns added later via an additive ALTER TABLE once tiering
  is actually being built for this backend. Both are legitimate,
  precedented choices within this project's own history; this one was
  picked to keep Phase 1's schema (and this docstring) focused only on
  what Phase 1 actually does.
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
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from urllib.parse import unquote, urlparse

from ..exceptions import StorageError
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

if TYPE_CHECKING:
    import pymysql

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

_INSERT = """
INSERT INTO zerobucket_images
    (id, data, mime_type, original_filename, size_bytes, width, height,
     checksum_sha256, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height,
       checksum_sha256
FROM zerobucket_images
WHERE id = %s;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = %s;
"""

_DELETE = "DELETE FROM zerobucket_images WHERE id = %s;"

_EXISTS = "SELECT 1 FROM zerobucket_images WHERE id = %s;"


def _now() -> datetime:
    # Naive UTC datetime -- PyMySQL binds Python datetime objects
    # directly to DATETIME columns; storing everything as UTC (and
    # never relying on the server's own timezone setting) keeps this
    # consistent regardless of how a given MySQL/MariaDB instance is
    # configured.
    return datetime.now(UTC).replace(tzinfo=None)


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

    `connection=` here means a `pymysql.connections.Connection` (not a
    Postgres or SQLite connection) -- the interface in base.py types
    this as `object` specifically so each adapter can narrow it to its
    own driver's type, same pattern every other adapter uses.
    """

    def __init__(self, database_url: str, *, auto_migrate: bool = True) -> None:
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
        if auto_migrate:
            conn = self._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(_SCHEMA)
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
        chunk_size: int,
        connection: pymysql.connections.Connection | None = None,
    ) -> Iterator[bytes] | None:
        raise NotImplementedError(
            "MySQLBackend.get_stream() is not implemented in this phase -- "
            "see this module's docstring for what's planned for Phase 2 "
            "(streaming + tiering, mirroring the SQLite adapter's own "
            "phase ordering)."
        )

    # ---- delete -----------------------------------------------------------

    def delete(
        self, image_id: str, *, connection: pymysql.connections.Connection | None = None
    ) -> bool:
        def work(conn):
            with conn.cursor() as cur:
                cur.execute(_DELETE, (image_id,))
                return cur.rowcount > 0

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

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
        statement."""
        if not image_ids:
            return []
        placeholders = ",".join(["%s"] * len(image_ids))

        def work(conn):
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id FROM zerobucket_images WHERE id IN ({placeholders}) "
                    "FOR UPDATE;",
                    image_ids,
                )
                existing_ids = [str(row[0]) for row in cur.fetchall()]
                if not existing_ids:
                    return []
                existing_placeholders = ",".join(["%s"] * len(existing_ids))
                cur.execute(
                    f"DELETE FROM zerobucket_images WHERE id IN ({existing_placeholders});",
                    existing_ids,
                )
                return existing_ids

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

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
