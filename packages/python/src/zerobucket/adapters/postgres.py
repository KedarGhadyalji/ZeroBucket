"""PostgreSQL storage adapter.

Stores image bytes directly in a BYTEA column. All queries are
parameterized; nothing is ever built via string concatenation.

Two storage modes, selected once at construction (PostgresBackend(...,
dedup=True/False)) and never mixed within one instance:

- Classic (dedup=False, the default): one row per put() call, in
  zerobucket_images, exactly as it has worked since v0.1.0. Unchanged.

- Dedup (dedup=True): content-addressed. Bytes live in zerobucket_blobs,
  keyed by checksum, with a ref_count. Each put() call still gets its
  own fresh id in zerobucket_image_refs, referencing a blob -- multiple
  ids can reference the same blob if their content is byte-identical,
  and the bytes are stored exactly once regardless of how many ids
  reference them. A blob is only actually deleted when its last
  referencing id is deleted (ref_count reaches 0).

  Deliberately DIFFERENT table names from classic mode (zerobucket_blobs
  / zerobucket_image_refs, not zerobucket_images) -- this was a specific
  safety decision: reusing zerobucket_images for a completely different
  column shape would mean flipping dedup=True against a database that
  already has classic-mode data risks either silent schema mismatches
  or (with naive `IF NOT EXISTS`Boolean logic) doing nothing at all
  while every dedup query then fails confusingly. Separate tables make
  this impossible to get wrong: dedup mode simply cannot collide with or
  misinterpret classic-mode data. See migrate_classic_to_dedup() for the
  (non-destructive, opt-in, separately tested) path to move existing
  classic-mode data into dedup tables.

Pool sizing (pool_min_size/pool_max_size/pool_timeout) and an optional
on_operation observability callback are both exposed here -- see
OperationEvent and PostgresBackend's docstring below.
"""

from __future__ import annotations

import random
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass

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

# ---- Dedup-mode schema and queries -------------------------------------

_DEDUP_SCHEMA = """
CREATE TABLE IF NOT EXISTS zerobucket_blobs (
    checksum_sha256     CHAR(64) PRIMARY KEY,
    data                BYTEA NOT NULL,
    mime_type           TEXT NOT NULL,
    size_bytes          INTEGER NOT NULL,
    width               INTEGER,
    height              INTEGER,
    ref_count           INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS zerobucket_image_refs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    checksum_sha256     CHAR(64) NOT NULL REFERENCES zerobucket_blobs(checksum_sha256),
    original_filename   TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_zerobucket_image_refs_checksum
    ON zerobucket_image_refs (checksum_sha256);
CREATE INDEX IF NOT EXISTS idx_zerobucket_image_refs_created_at
    ON zerobucket_image_refs (created_at);
"""

# Race-safe by construction: verified empirically during development
# (20 concurrent threads upserting the same checksum produced ref_count
# == 20, not less) -- Postgres handles the atomicity at the row level,
# not this code.
_DEDUP_UPSERT_BLOB = """
INSERT INTO zerobucket_blobs
    (checksum_sha256, data, mime_type, size_bytes, width, height, ref_count)
VALUES (%s, %s, %s, %s, %s, %s, 1)
ON CONFLICT (checksum_sha256) DO UPDATE SET ref_count = zerobucket_blobs.ref_count + 1;
"""

_DEDUP_INSERT_REF = """
INSERT INTO zerobucket_image_refs (checksum_sha256, original_filename)
VALUES (%s, %s)
RETURNING id;
"""

_DEDUP_SELECT_FULL = """
SELECT r.id, b.data, b.mime_type, r.original_filename, b.size_bytes,
       b.width, b.height, r.checksum_sha256
FROM zerobucket_image_refs r
JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256
WHERE r.id = %s;
"""

_DEDUP_SELECT_METADATA = """
SELECT r.id, b.mime_type, r.original_filename, b.size_bytes, b.width, b.height, r.checksum_sha256
FROM zerobucket_image_refs r
JOIN zerobucket_blobs b ON r.checksum_sha256 = b.checksum_sha256
WHERE r.id = %s;
"""

_DEDUP_EXISTS = "SELECT 1 FROM zerobucket_image_refs WHERE id = %s;"

_DEDUP_DELETE_REF = (
    "DELETE FROM zerobucket_image_refs WHERE id = %s RETURNING checksum_sha256;"
)

_DEDUP_DECREMENT_BLOB = """
UPDATE zerobucket_blobs SET ref_count = ref_count - %s
WHERE checksum_sha256 = %s
RETURNING ref_count;
"""

_DEDUP_DELETE_EMPTY_BLOBS = """
DELETE FROM zerobucket_blobs WHERE checksum_sha256 = ANY(%s) AND ref_count <= 0;
"""

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


@dataclass(frozen=True, slots=True)
class OperationEvent:
    """Emitted to an on_operation callback after every storage operation
    completes (success or failure) -- the "measure" half of "you can't
    optimize what you can't measure."

    operation: one of "put", "put_many", "get", "get_many",
        "get_metadata", "delete", "delete_many", "exists", "migrate".
        Dedup-mode operations report the SAME operation name as their
        classic-mode counterpart (e.g. a dedup put() reports "put", not
        some dedup-specific name) -- from a metrics/tuning perspective,
        it's still logically the same operation regardless of storage
        mode underneath.
    duration_seconds: wall-clock time for the WHOLE call, including any
        retries and their backoff delays -- this is what actually
        matters for understanding real-world latency your application
        experiences, not just the final successful attempt in isolation.
    success: whether the operation ultimately succeeded.
    error: str(exception) if it failed, else None.
    retry_count: how many retries were attempted (0 if none, or if this
        call used connection= -- automatic retry never applies there,
        see PostgresBackend's docstring).
    """

    operation: str
    duration_seconds: float
    success: bool
    error: str | None
    retry_count: int


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

    dedup: see module docstring. False (default) is the classic,
    unchanged single-table behavior; True switches to content-addressed
    storage in separate tables. Not retroactive -- see
    migrate_classic_to_dedup() for moving existing classic-mode data.

    pool_min_size/pool_max_size/pool_timeout: tune the internal
    connection pool. Defaults (1/5/10) are unchanged from every prior
    version -- this is purely making previously-hardcoded values
    configurable, not changing any default behavior.

    on_operation: optional callback, called with an OperationEvent after
    every operation completes. Exceptions raised inside your callback
    are caught and silently ignored -- a bug in your metrics code can
    never break a real image operation, but this also means such bugs
    won't be visible to you unless you test the callback separately.
    Wire this to whatever you actually use (Prometheus client, StatsD,
    plain logging) -- ZeroBucket does not ship a specific metrics
    backend integration, deliberately, consistent with keeping the core
    library's dependency footprint small.
    """

    def __init__(
        self,
        database_url: str,
        *,
        auto_migrate: bool = True,
        max_retries: int = 3,
        retry_base_delay: float = 0.1,
        dedup: bool = False,
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        pool_timeout: float = 10,
        on_operation: Callable[[OperationEvent], None] | None = None,
    ) -> None:
        try:
            self._pool = ConnectionPool(
                database_url,
                min_size=pool_min_size,
                max_size=pool_max_size,
                open=True,
                timeout=pool_timeout,
            )
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Could not connect to PostgreSQL: {exc}") from exc

        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._dedup = dedup
        self._on_operation = on_operation

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
        """Create the schema for this instance's mode if it doesn't exist.

        dedup=False creates zerobucket_images (unchanged since v0.1.0).
        dedup=True creates zerobucket_blobs + zerobucket_image_refs.
        These never collide -- see module docstring for why that was a
        deliberate design choice, not an oversight.
        """
        schema = _DEDUP_SCHEMA if self._dedup else _SCHEMA
        try:
            self._run(None, lambda cur: cur.execute(schema), operation="migrate")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Migration failed: {exc}") from exc

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

    def _run(self, connection: psycopg.Connection | None, work, *, operation: str):
        """Run `work(cursor)` and return its result.

        connection provided -> run once, on that exact connection, no
        retry (see class docstring for why). Still emits an
        OperationEvent (retry_count always 0 on this path).

        connection is None -> use the internal pool; retry transient
        failures (see _is_retryable) up to max_retries times with
        exponential backoff + jitter. Non-transient errors propagate
        immediately on the first attempt, same as before this feature
        existed. Emits exactly one OperationEvent per call, after the
        final attempt (success or exhausted retries) -- duration_seconds
        covers the whole call including any backoff delays.
        """
        start_time = time.monotonic()

        if connection is not None:
            try:
                with connection.cursor() as cur:
                    result = work(cur)
                self._emit(operation, start_time, True, None, 0)
                return result
            except Exception as exc:  # noqa: BLE001
                self._emit(operation, start_time, False, str(exc), 0)
                raise

        attempt = 0
        while True:
            try:
                with self._pool.connection() as conn, conn.cursor() as cur:
                    result = work(cur)
                self._emit(operation, start_time, True, None, attempt)
                return result
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                if attempt > self._max_retries or not _is_retryable(exc):
                    self._emit(operation, start_time, False, str(exc), attempt - 1)
                    raise
                time.sleep(_backoff_delay(attempt, self._retry_base_delay))

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
        connection: psycopg.Connection | None = None,
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
            return self._run(connection, work, operation="put")
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
        connection: psycopg.Connection | None,
    ) -> str:
        def work(cur):
            cur.execute(
                _DEDUP_UPSERT_BLOB,
                (checksum_sha256, data, mime_type, size_bytes, width, height),
            )
            cur.execute(_DEDUP_INSERT_REF, (checksum_sha256, original_filename))
            row = cur.fetchone()
            return str(row[0])

        try:
            return self._run(connection, work, operation="put")
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

        In dedup mode: repeated identical checksums WITHIN one batch
        correctly accumulate ref_count (verified empirically -- 3
        pipelined upserts of the same checksum in one executemany call
        produced ref_count == 3, not 1), since each is a distinct
        sequential statement execution that sees the prior one's effect
        within the same transaction.

        Returns ids in the same order as `rows`. Raises StorageError if
        any row fails -- callers wanting partial-success semantics
        should catch per-row validation errors before calling this (see
        client.py's put_many(), which does exactly that).
        """
        if not rows:
            return []

        if self._dedup:
            return self._put_many_dedup(rows, connection=connection)

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
            return self._run(connection, work, operation="put_many")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    def _put_many_dedup(
        self, rows: list[dict], *, connection: psycopg.Connection | None
    ) -> list[str]:
        def work(cur):
            blob_params = [
                (
                    row["checksum_sha256"],
                    row["data"],
                    row["mime_type"],
                    row["size_bytes"],
                    row["width"],
                    row["height"],
                )
                for row in rows
            ]
            cur.executemany(_DEDUP_UPSERT_BLOB, blob_params)

            ref_params = [
                (row["checksum_sha256"], row["original_filename"]) for row in rows
            ]
            cur.executemany(_DEDUP_INSERT_REF, ref_params, returning=True)
            ids = [str(cur.fetchone()[0])]
            while cur.nextset():
                ids.append(str(cur.fetchone()[0]))
            return ids

        try:
            return self._run(connection, work, operation="put_many")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to store image batch: {exc}") from exc

    # ---- get --------------------------------------------------------------

    def get(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> StoredRecord | None:
        select_sql = _DEDUP_SELECT_FULL if self._dedup else _SELECT_FULL

        def work(cur):
            cur.execute(select_sql, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work, operation="get")
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

        if self._dedup:
            select_many_sql = _DEDUP_SELECT_FULL.replace(
                "WHERE r.id = %s", "WHERE r.id = ANY(%s)"
            )
        else:
            select_many_sql = _SELECT_FULL.replace(
                "WHERE id = %s", "WHERE id = ANY(%s)"
            )

        def work(cur):
            cur.execute(select_many_sql, (image_ids,))
            return cur.fetchall()

        try:
            rows = self._run(connection, work, operation="get_many")
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
        select_sql = _DEDUP_SELECT_METADATA if self._dedup else _SELECT_METADATA

        def work(cur):
            cur.execute(select_sql, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work, operation="get_metadata")
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

    # ---- delete -------------------------------------------------------------

    def delete(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> bool:
        if self._dedup:
            return self._delete_dedup(image_id, connection=connection)

        def work(cur):
            cur.execute(_DELETE, (image_id,))
            return cur.rowcount > 0

        try:
            return self._run(connection, work, operation="delete")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc

    def _delete_dedup(
        self, image_id: str, *, connection: psycopg.Connection | None
    ) -> bool:
        def work(cur):
            cur.execute(_DEDUP_DELETE_REF, (image_id,))
            row = cur.fetchone()
            if row is None:
                return False  # id didn't exist -- nothing to decrement either
            checksum = row[0]
            cur.execute(_DEDUP_DECREMENT_BLOB, (1, checksum))
            new_ref_count = cur.fetchone()[0]
            if new_ref_count <= 0:
                cur.execute(_DEDUP_DELETE_EMPTY_BLOBS, ([checksum],))
            return True

        try:
            return self._run(connection, work, operation="delete")
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

        if self._dedup:
            return self._delete_many_dedup(image_ids, connection=connection)

        def work(cur):
            cur.execute(
                "DELETE FROM zerobucket_images WHERE id = ANY(%s) RETURNING id;",
                (image_ids,),
            )
            return [str(row[0]) for row in cur.fetchall()]

        try:
            return self._run(connection, work, operation="delete_many")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    def _delete_many_dedup(
        self, image_ids: list[str], *, connection: psycopg.Connection | None
    ) -> list[str]:
        def work(cur):
            cur.execute(
                "DELETE FROM zerobucket_image_refs WHERE id = ANY(%s) "
                "RETURNING id, checksum_sha256;",
                (image_ids,),
            )
            deleted_rows = cur.fetchall()
            if not deleted_rows:
                return []

            deleted_ids = [str(row[0]) for row in deleted_rows]
            # Multiple deleted refs can share the same checksum (e.g. two
            # ids both pointed at the same de-duplicated blob) -- count
            # occurrences per checksum so each blob's ref_count is
            # decremented by the correct amount, not just by 1 per
            # distinct checksum.
            checksum_counts = Counter(row[1] for row in deleted_rows)

            checksums_to_check_for_deletion = []
            for checksum, count in checksum_counts.items():
                cur.execute(_DEDUP_DECREMENT_BLOB, (count, checksum))
                new_ref_count = cur.fetchone()[0]
                if new_ref_count <= 0:
                    checksums_to_check_for_deletion.append(checksum)

            if checksums_to_check_for_deletion:
                cur.execute(
                    _DEDUP_DELETE_EMPTY_BLOBS, (checksums_to_check_for_deletion,)
                )

            return deleted_ids

        try:
            return self._run(connection, work, operation="delete_many")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc

    # ---- exists -------------------------------------------------------------

    def exists(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> bool:
        exists_sql = _DEDUP_EXISTS if self._dedup else _EXISTS

        def work(cur):
            cur.execute(exists_sql, (image_id,))
            return cur.fetchone() is not None

        try:
            return self._run(connection, work, operation="exists")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to check image existence: {exc}") from exc

    def close(self) -> None:
        self._pool.close()


def migrate_classic_to_dedup(dedup_backend: PostgresBackend) -> dict:
    """One-time, NON-DESTRUCTIVE migration: copy every row from the
    classic zerobucket_images table into `dedup_backend`'s dedup tables
    (zerobucket_blobs / zerobucket_image_refs), preserving every
    existing id exactly (so any external references to those ids keep
    working) and correctly deduplicating identical content found along
    the way.

    Does NOT modify, truncate, or delete the original zerobucket_images
    table. Safe to run, inspect the results, and only clean up the old
    table yourself once you're confident -- this is deliberately
    conservative: an in-place ALTER/destructive migration risks data
    loss if anything goes wrong partway through, and this function has
    no way to test against a specific deployment's real production data
    ahead of time.

    `dedup_backend` must already be a PostgresBackend constructed with
    dedup=True (so zerobucket_blobs/zerobucket_image_refs already
    exist) and pointed at the SAME database that has the classic
    zerobucket_images table.

    Runs as one transaction (all rows migrated, or none) -- known
    limitation: loads all classic-table rows into memory at once, same
    memory-pressure principle as everything else in this project's BYTEA
    approach (see docs/OPERATIONS.md). Fine for typical small/medium
    datasets consistent with ZeroBucket's own stated positioning; not
    designed as a streaming migration tool for huge tables.

    Returns {"images_migrated": int, "distinct_blobs_created": int,
    "duplicate_references_found": int}.
    """
    with dedup_backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute("SELECT to_regclass('zerobucket_images');")
        if cur.fetchone()[0] is None:
            raise StorageError(
                "No classic zerobucket_images table found in this database -- "
                "nothing to migrate."
            )

        cur.execute(
            "SELECT id, data, mime_type, original_filename, size_bytes, "
            "width, height, checksum_sha256, created_at FROM zerobucket_images "
            "ORDER BY created_at;"
        )
        classic_rows = cur.fetchall()

        if not classic_rows:
            return {
                "images_migrated": 0,
                "distinct_blobs_created": 0,
                "duplicate_references_found": 0,
            }

        seen_checksums: set[str] = set()
        for row in classic_rows:
            (
                image_id,
                data,
                mime_type,
                original_filename,
                size_bytes,
                width,
                height,
                checksum,
                created_at,
            ) = row

            cur.execute(
                _DEDUP_UPSERT_BLOB,
                (checksum, data, mime_type, size_bytes, width, height),
            )
            seen_checksums.add(checksum)

            # Preserve the ORIGINAL id and created_at exactly -- this is
            # what makes the migration safe for anything that already
            # references these ids (URLs, foreign keys in the caller's
            # own app tables, etc.).
            cur.execute(
                "INSERT INTO zerobucket_image_refs "
                "(id, checksum_sha256, original_filename, created_at) "
                "VALUES (%s, %s, %s, %s);",
                (image_id, checksum, original_filename, created_at),
            )

        conn.commit()

    return {
        "images_migrated": len(classic_rows),
        "distinct_blobs_created": len(seen_checksums),
        "duplicate_references_found": len(classic_rows) - len(seen_checksums),
    }
