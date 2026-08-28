"""PostgreSQL storage adapter.

Stores image bytes directly in a BYTEA column. All queries are
parameterized; nothing is ever built via string concatenation.
"""

from __future__ import annotations

import random
import time

import psycopg
from psycopg_pool import ConnectionPool

from ..exceptions import StorageError
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

_SCHEMA = """
CREATE TABLE IF NOT EXISTS zerobucket_images (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    data                BYTEA NOT NULL,
    mime_type           TEXT NOT NULL,
    original_filename   TEXT,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    checksum_sha256     CHAR(64) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_checksum ON zerobucket_images (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_created_at ON zerobucket_images (created_at);
"""

_INSERT = """
INSERT INTO zerobucket_images
    (data, mime_type, original_filename, size_bytes, width, height, checksum_sha256)
VALUES (%s, %s, %s, %s, %s, %s, %s)
RETURNING id;
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height, checksum_sha256
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

# SQLSTATEs worth retrying automatically -- transient conditions where a
# second attempt has a real chance of succeeding, as opposed to errors
# that will fail identically every time (bad SQL, constraint violations,
# etc.), which must never be retried. Verified against real psycopg
# exception attributes during development, not assumed -- see
# docs/OPERATIONS.md for the empirical check.
_RETRYABLE_SQLSTATES = frozenset(
    {
        "40001",  # serialization_failure
        "40P01",  # deadlock_detected
        "08000",  # connection_exception
        "08003",  # connection_does_not_exist
        "08006",  # connection_failure
        "08001",  # sqlclient_unable_to_establish_sqlconnection
        "08004",  # sqlserver_rejected_establishment_of_sqlconnection
        "57P01",  # admin_shutdown
        "57P02",  # crash_shutdown
        "57P03",  # cannot_connect_now
        "53000",  # insufficient_resources
        "53300",  # too_many_connections
    }
)

# Cap on backoff delay so a misconfigured high retry count can't make a
# single failing call hang for an unreasonable amount of time.
_MAX_BACKOFF_SECONDS = 2.0


def _is_retryable(exc: BaseException) -> bool:
    """Classify whether an exception represents a transient condition
    worth retrying, vs. one that will fail identically every time.

    OperationalError covers connection-level failures (the connection
    never reached the server, so there's no SQLSTATE at all -- verified
    empirically: OperationalError.sqlstate is None for a real connection
    failure). Server-returned errors are classified by SQLSTATE instead.
    """
    if isinstance(exc, psycopg.OperationalError):
        return True
    sqlstate = getattr(exc, "sqlstate", None)
    return sqlstate in _RETRYABLE_SQLSTATES


def _backoff_delay(attempt: int, base_delay: float) -> float:
    """Exponential backoff with jitter, capped. `attempt` is 1-indexed
    (the delay before the 2nd try, 3rd try, etc.)."""
    exponential = base_delay * (2 ** (attempt - 1))
    jitter = random.uniform(0, base_delay)
    return min(exponential + jitter, _MAX_BACKOFF_SECONDS)


class PostgresBackend(StorageBackend):
    """Storage backend for PostgreSQL using BYTEA columns.

    Requires the pgcrypto extension (for gen_random_uuid()) on Postgres < 13.
    Postgres 13+ has gen_random_uuid() built in.

    Automatic retry (max_retries, retry_base_delay) applies ONLY to
    ZeroBucket's own internally-pooled connections (connection=None on
    every method). If you pass your own connection= to participate in
    your own transaction, that call is deliberately NEVER retried
    automatically -- retrying a statement on a connection you're
    managing yourself could silently corrupt your transaction's
    semantics (e.g. retrying after a serialization failure normally
    requires restarting the WHOLE transaction from your application's
    perspective, not just replaying one statement). That decision has to
    stay yours when you're the one holding the connection.
    """

    def __init__(
        self,
        database_url: str,
        *,
        auto_migrate: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
    ) -> None:
        try:
            self._pool = ConnectionPool(
                database_url, min_size=1, max_size=5, open=True, timeout=10
            )
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Could not connect to PostgreSQL: {exc}") from exc

        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay

        if auto_migrate:
            try:
                self.migrate()
            except Exception:
                # Don't leak the pool's background worker threads if setup
                # fails partway through -- close it before propagating so
                # callers (and test runners) don't hang on shutdown.
                self._pool.close()
                raise

    def migrate(self) -> None:
        """Create the zerobucket_images table and indexes if they don't exist."""
        try:
            self._run(None, lambda cur: cur.execute(_SCHEMA))
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Migration failed: {exc}") from exc

    def _run(self, connection: psycopg.Connection | None, work):
        """Run `work(cursor)` and return its result.

        connection provided -> run once, on that exact connection, no
        retry (see class docstring for why).

        connection is None -> use the internal pool; retry transient
        failures (see _is_retryable) up to max_retries times with
        exponential backoff + jitter. Non-transient errors propagate
        immediately on the first attempt, same as before this feature
        existed.
        """
        if connection is not None:
            with connection.cursor() as cur:
                return work(cur)

        attempt = 0
        while True:
            try:
                with self._pool.connection() as conn, conn.cursor() as cur:
                    return work(cur)
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                if attempt > self._max_retries or not _is_retryable(exc):
                    raise
                time.sleep(_backoff_delay(attempt, self._retry_base_delay))

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
        connection: psycopg.Connection | None = None,
    ) -> str:
        def work(cur):
            params = (
                data,
                mime_type,
                original_filename,
                size_bytes,
                width,
                height,
                checksum_sha256,
            )
            cur.execute(_INSERT, params)
            row = cur.fetchone()
            return str(row[0])

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image: {exc}") from exc

    def put_many(
        self,
        rows: list[dict],
        *,
        connection: psycopg.Connection | None = None,
    ) -> list[str]:
        """Insert multiple already-prepared rows in one pipelined batch.

        `rows` must be dicts with the same keys as put()'s kwargs (data,
        mime_type, original_filename, size_bytes, width, height,
        checksum_sha256) -- validation/optimization happens in client.py
        before this is called, since that work is inherently per-item and
        can't be batched at the SQL level.

        Uses psycopg's executemany(returning=True), which pipelines each
        row as its own statement execution (fewer network round trips
        than a naive loop) while GUARANTEEING result order matches input
        order -- verified empirically during development, not assumed;
        this is architectural (each row is a distinct statement
        execution under the hood), not an incidental implementation
        detail that could silently change. A hand-rolled multi-row
        `INSERT ... VALUES (...), (...) RETURNING id` was deliberately
        NOT used here, since Postgres does not formally guarantee
        RETURNING order matches VALUES order for that form.

        A retried batch (connection=None, transient failure) is safe
        from double-insertion: the whole batch lives in one connection
        scope that only commits on clean exit, so a transient failure
        mid-batch means NONE of it committed -- retrying from scratch
        cannot create duplicates.

        Returns ids in the same order as `rows`. Raises StorageError if
        any row fails -- callers wanting partial-success semantics
        should catch per-row validation errors before calling this (see
        client.py's put_many(), which does exactly that).
        """
        if not rows:
            return []

        def work(cur):
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
            cur.executemany(_INSERT, params_seq, returning=True)
            ids = [str(cur.fetchone()[0])]
            while cur.nextset():
                ids.append(str(cur.fetchone()[0]))
            return ids

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    def get(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> StoredRecord | None:
        def work(cur):
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
        self, image_ids: list[str], *, connection: psycopg.Connection | None = None
    ) -> list[StoredRecord]:
        """Fetch multiple records in a single query. Missing ids are
        simply absent from the result -- not an error, not a placeholder.
        Order of results is NOT guaranteed to match `image_ids` (a single
        WHERE id = ANY(...) query has no defined row order) -- client.py
        re-correlates by id, don't rely on this method's return order.
        """
        if not image_ids:
            return []

        def work(cur):
            cur.execute(
                _SELECT_FULL.replace("WHERE id = %s", "WHERE id = ANY(%s)"),
                (image_ids,),
            )
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
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> StoredRecordMetadata | None:
        def work(cur):
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

    def delete(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> bool:
        def work(cur):
            cur.execute(_DELETE, (image_id,))
            return cur.rowcount > 0

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    def delete_many(
        self, image_ids: list[str], *, connection: psycopg.Connection | None = None
    ) -> list[str]:
        """Delete multiple records in a single query. Returns the ids
        that were ACTUALLY deleted (a subset of the input if some ids
        didn't exist) -- not an error for missing ids, same semantics as
        delete() returning False for a missing id."""
        if not image_ids:
            return []

        def work(cur):
            cur.execute(
                "DELETE FROM zerobucket_images WHERE id = ANY(%s) RETURNING id;",
                (image_ids,),
            )
            return [str(row[0]) for row in cur.fetchall()]

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    def exists(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> bool:
        def work(cur):
            cur.execute(_EXISTS, (image_id,))
            return cur.fetchone() is not None

        try:
            return self._run(connection, work)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    def close(self) -> None:
        self._pool.close()
