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
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import psycopg
from psycopg_pool import ConnectionPool

from ..exceptions import StorageError
from ..object_storage import ObjectStorage
from .base import StorageBackend, StoredRecord, StoredRecordMetadata

# 1 MiB. Default chunk size for get_stream() -- small enough to keep
# Python-side peak memory low, large enough to keep the per-chunk
# round-trip count reasonable for typical (few-MB) stored images.
DEFAULT_STREAM_CHUNK_SIZE = 1024 * 1024

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

-- Object-storage tiering (Stage 5). Additive to the schema above, safe
-- to run against a database that already has rows in it -- every
-- existing row gets storage_backend='postgres' via the column default,
-- object_storage_key/object_storage_bucket NULL, and the CHECK
-- constraint below is satisfied by every one of them without needing to
-- touch a single existing row. `data` has to become nullable because a
-- tiered row's bytes live in object storage, not in this column -- the
-- CHECK constraint is what keeps that from silently becoming "NULL data
-- and nobody notices": exactly one of "postgres row with data" or
-- "tiered row with a pointer" is allowed to be true, never both, never
-- neither.
ALTER TABLE zerobucket_images ALTER COLUMN data DROP NOT NULL;
ALTER TABLE zerobucket_images
    ADD COLUMN IF NOT EXISTS storage_backend TEXT NOT NULL DEFAULT 'postgres';
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS object_storage_bucket TEXT;
ALTER TABLE zerobucket_images ADD COLUMN IF NOT EXISTS object_storage_key TEXT;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'zerobucket_storage_location_check'
    ) THEN
        ALTER TABLE zerobucket_images ADD CONSTRAINT zerobucket_storage_location_check
        CHECK (
            (storage_backend = 'postgres'
                AND data IS NOT NULL
                AND object_storage_key IS NULL
                AND object_storage_bucket IS NULL)
            OR
            (storage_backend = 'object_storage'
                AND data IS NULL
                AND object_storage_key IS NOT NULL
                AND object_storage_bucket IS NOT NULL)
        );
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_zerobucket_storage_backend
    ON zerobucket_images (storage_backend) WHERE storage_backend != 'postgres';
"""

_INSERT = """
INSERT INTO zerobucket_images
    (data, mime_type, original_filename, size_bytes, width, height, checksum_sha256)
VALUES (%s, %s, %s, %s, %s, %s, %s)
RETURNING id;
"""

_SELECT_FULL = """
SELECT id, data, mime_type, original_filename, size_bytes, width, height,
       checksum_sha256, storage_backend, object_storage_bucket, object_storage_key
FROM zerobucket_images
WHERE id = %s;
"""

_SELECT_METADATA = """
SELECT id, mime_type, original_filename, size_bytes, width, height, checksum_sha256
FROM zerobucket_images
WHERE id = %s;
"""

# Everything tier_to_object_storage() needs to perform the move: the raw
# bytes (to upload) plus mime_type (for the object's Content-Type) and
# size_bytes (to sanity-check the upload). Separate from _SELECT_FULL
# rather than reusing it -- this one is only ever called on a row already
# confirmed to be storage_backend='postgres' (see tier_to_object_storage's
# docstring), so it doesn't need the tiering columns back.
_SELECT_FOR_TIERING = """
SELECT data, mime_type, size_bytes, storage_backend
FROM zerobucket_images
WHERE id = %s
FOR UPDATE;
"""

_UPDATE_AFTER_TIERING = """
UPDATE zerobucket_images
SET data = NULL, storage_backend = 'object_storage',
    object_storage_bucket = %s, object_storage_key = %s, updated_at = now()
WHERE id = %s;
"""

# substring() is 1-indexed and clamps `length` at the value's actual end,
# so the last chunk of a stream naturally comes back shorter without any
# special-casing here.
_SELECT_CHUNK = """
SELECT substring(data FROM %s FOR %s)
FROM zerobucket_images
WHERE id = %s;
"""

_SELECT_STREAM_INFO = """
SELECT size_bytes, storage_backend, object_storage_key
FROM zerobucket_images
WHERE id = %s;
"""

_DELETE = "DELETE FROM zerobucket_images WHERE id = %s;"

_DELETE_RETURNING = """
DELETE FROM zerobucket_images WHERE id = %s
RETURNING storage_backend, object_storage_key;
"""

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

_DEDUP_SELECT_CHUNK = """
SELECT substring(b.data FROM %s FOR %s)
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

    operation: one of "put", "put_many", "get", "get_stream",
        "get_many", "get_metadata", "delete", "delete_many", "exists",
        "migrate", "tier_to_object_storage". A single get_stream() call reports one event per
        chunk fetched (plus one for the initial metadata lookup used to
        learn the total size), not one event for the whole stream --
        each is a genuinely separate round trip, so this is consistent
        with every other operation being measured per round trip, not
        per logical call. Dedup-mode operations report the SAME operation name as their
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

    object_storage: optional ObjectStorage instance (see
    object_storage.py), enabling tiering -- see tier_to_object_storage().
    None (default) means tiering is unavailable: rows can never be
    tiered from this instance, and if this instance ever encounters an
    already-tiered row (tiered by a DIFFERENT, object_storage-configured
    instance pointed at the same database), get()/get_many()/
    get_stream() raise a clear StorageError rather than silently
    returning nothing or corrupted data. NOT available in dedup mode in
    this first pass -- combining content-addressed storage (where one
    blob can be referenced by many ids) with tiering (where a specific
    blob's bytes might live in Postgres or in object storage) was judged
    a meaningfully bigger, riskier design problem than tiering classic-
    mode rows, and out of scope for this pass. Passing object_storage=
    together with dedup=True raises ValueError immediately at
    construction, rather than failing confusingly later.

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
        object_storage: ObjectStorage | None = None,
    ) -> None:
        if object_storage is not None and dedup:
            raise ValueError(
                "object_storage= is not supported together with dedup=True "
                "in this first pass -- see PostgresBackend's docstring."
            )
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
        self._object_storage = object_storage

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
        data = row[1]
        if not self._dedup and row[8] == "object_storage":
            data = self._fetch_tiered_bytes(row[10], image_id=image_id)
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
        """Shared by get()/get_many() -- both need "the row says this
        image lives in object storage, go get the actual bytes" and both
        need the identical error if this backend wasn't configured with
        an ObjectStorage to do that with. Deliberately NOT wrapped in
        _run()/retried/given its own OperationEvent in this first pass --
        object-storage round trips aren't yet covered by on_operation,
        only Postgres ones are. Stated as a real gap, not silently
        unmeasured by omission."""
        if self._object_storage is None:
            raise StorageError(
                f"Image {image_id!r} is stored in object storage (key="
                f"{object_storage_key!r}) but this ZeroBucket/PostgresBackend "
                "was constructed without object_storage=... -- configure it "
                "with the same bucket/credentials used to tier this image."
            )
        return self._object_storage.download(object_storage_key)

    def get_many(
        self, image_ids: list[str], *, connection: psycopg.Connection | None = None
    ) -> list[StoredRecord]:
        """Fetch multiple records in a single query. Missing ids are
        simply absent from the result -- not an error, not a placeholder.
        Order of results is NOT guaranteed to match `image_ids` (a single
        WHERE id = ANY(...) query has no defined row order) -- client.py
        re-correlates by id, don't rely on this method's return order.

        Tiered rows are fetched from object storage ONE AT A TIME, after
        the single Postgres query returns -- not concurrently, not
        batched into fewer object-storage requests. A get_many() call
        touching many tiered images will be noticeably slower than one
        touching only Postgres-resident images. Stated directly rather
        than left to be discovered: this was left unoptimized in this
        first pass rather than adding real complexity (e.g. thread-pool
        fan-out) for a path that's expected to be the less-common case
        (most of a typical dataset stays in Postgres; only large/old
        images get explicitly tiered).
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
        records = []
        for row in rows:
            data = row[1]
            if not self._dedup and row[8] == "object_storage":
                data = self._fetch_tiered_bytes(row[10], image_id=str(row[0]))
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

    # ---- get_stream -----------------------------------------------------

    def get_stream(
        self,
        image_id: str,
        *,
        chunk_size: int = DEFAULT_STREAM_CHUNK_SIZE,
        connection: psycopg.Connection | None = None,
    ) -> Iterator[bytes] | None:
        """See StorageBackend.get_stream for the contract.

        Implementation: one metadata lookup to learn size_bytes (reused
        as the not-found check -- same semantics as get()/metadata()),
        then repeated `substring(data FROM offset FOR length)` queries
        walking forward through the value. Each chunk is its own `_run`
        call (its own OperationEvent, its own retry behavior if
        connection=None), exactly like every other operation here.

        If the row disappears between chunks (concurrent delete, no
        connection= holding a snapshot), the next chunk query returns no
        row and this raises StorageError rather than silently yielding a
        short read -- a truncated image passed off as complete would be
        a much worse failure mode than a loud one.

        For a TIERED row (storage_backend='object_storage'), this
        delegates entirely to ObjectStorage.download_stream() instead of
        the substring() approach below -- which, worth stating directly,
        is actually a strictly BETTER streaming implementation: it uses
        real HTTP byte-Range requests against S3, not just a Python-side
        memory optimization on top of transferring the full value every
        time (see object_storage.py's download_stream() docstring). Not
        available in dedup mode (dedup mode doesn't support tiering at
        all yet -- see PostgresBackend's docstring) or if this backend
        wasn't constructed with object_storage=... configured.
        """
        if self._dedup:
            metadata = self.get_metadata(image_id, connection=connection)
            if metadata is None:
                return None
            total_size = metadata.size_bytes
            select_chunk_sql = _DEDUP_SELECT_CHUNK
        else:

            def info_work(cur):
                cur.execute(_SELECT_STREAM_INFO, (image_id,))
                return cur.fetchone()

            try:
                info_row = self._run(connection, info_work, operation="get_metadata")
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
            select_chunk_sql = _SELECT_CHUNK

        def generator() -> Iterator[bytes]:
            offset = 1  # substring() is 1-indexed
            remaining = total_size
            delivered = 0
            while remaining > 0:
                length = min(chunk_size, remaining)

                def work(cur, offset=offset, length=length):
                    cur.execute(select_chunk_sql, (offset, length, image_id))
                    return cur.fetchone()

                try:
                    row = self._run(connection, work, operation="get_stream")
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
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> bool | None:
        """Move an image's bytes out of Postgres and into object storage,
        replacing this row's `data` with a pointer (storage_backend,
        object_storage_bucket, object_storage_key). Returns None if
        `image_id` doesn't exist (mirrors get()/get_metadata()'s
        not-found-returns-None convention -- client.py raises
        ImageNotFoundError from that None, same as everywhere else),
        False if it exists but was ALREADY tiered (a safe no-op, not an
        error -- lets you re-run a backfill script without it choking on
        rows it already handled), True if it was actually tiered just
        now.

        Requires object_storage= to have been passed to this backend's
        constructor -- raises StorageError immediately, before touching
        the database at all, if it wasn't.

        SAFETY: the upload to object storage happens INSIDE the same
        database transaction as the row lookup/lock (SELECT ... FOR
        UPDATE) and the subsequent UPDATE that flips storage_backend --
        all as one `_run()` call. If the object-storage upload fails for
        any reason (network error, bad credentials, bucket doesn't
        exist), the exception propagates, the whole transaction rolls
        back, and the row is left completely untouched -- still fully in
        Postgres, exactly as if tiering had never been attempted. There
        is no window where an image's bytes exist in neither location,
        and no window where a row claims to be tiered but the object-
        storage upload never actually completed.

        TRADEOFF, stated directly: this holds a Postgres row-level lock
        (via SELECT ... FOR UPDATE) for the entire duration of the
        object-storage upload -- a real network call, potentially slow
        for a large image. A concurrent get()/delete()/tier_to_object_storage()
        call on this SAME image_id will block until this finishes; every
        OTHER row is completely unaffected. This is a deliberate
        simplicity/safety tradeoff for what's expected to be an
        infrequent, explicitly-triggered maintenance operation, not
        something called on a hot request path.

        On retry (connection=None, a transient error mid-operation):
        _run() replays the whole `work(cur)` callable, including the
        object-storage upload. This is safe specifically because the
        upload key is deterministic (str(image_id)) and S3's PutObject
        semantics overwrite silently -- re-uploading the same bytes to
        the same key on retry is a harmless no-op, not a correctness
        risk, and the row lock ensures no other writer can have changed
        things in between attempts.
        """
        if self._dedup:
            raise StorageError(
                "tier_to_object_storage() is not supported in dedup mode "
                "in this first pass -- see PostgresBackend's docstring."
            )
        if self._object_storage is None:
            raise StorageError(
                "tier_to_object_storage() requires this backend to be "
                "constructed with object_storage=... -- see ObjectStorage "
                "in object_storage.py."
            )

        object_storage = self._object_storage  # narrow for closures below

        def work(cur):
            cur.execute(_SELECT_FOR_TIERING, (image_id,))
            row = cur.fetchone()
            if row is None:
                return None
            data, mime_type, size_bytes, storage_backend = row
            if storage_backend != "postgres":
                return False
            key = str(image_id)
            object_storage.upload(key, bytes(data), mime_type=mime_type)
            cur.execute(_UPDATE_AFTER_TIERING, (object_storage.bucket, key, image_id))
            return True

        try:
            return self._run(connection, work, operation="tier_to_object_storage")
        except StorageError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise StorageError(
                f"Failed to tier image to object storage: {exc}"
            ) from exc

    # ---- delete -------------------------------------------------------------

    def delete(
        self, image_id: str, *, connection: psycopg.Connection | None = None
    ) -> bool:
        if self._dedup:
            return self._delete_dedup(image_id, connection=connection)

        def work(cur):
            cur.execute(_DELETE_RETURNING, (image_id,))
            return cur.fetchone()

        try:
            row = self._run(connection, work, operation="delete")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image: {exc}") from exc
        if row is None:
            return False
        storage_backend, object_storage_key = row
        if storage_backend == "object_storage" and self._object_storage is not None:
            # Best-effort, AFTER the Postgres row is already gone -- a
            # deliberate ordering choice, not an oversight. The row being
            # gone is what makes the image correctly "not found" to every
            # caller from here on, regardless of whether this S3 delete
            # below succeeds; if it fails, the result is a harmless
            # orphaned object costing a few cents, not a data-integrity
            # problem (contrast: deleting from S3 FIRST and having the
            # Postgres DELETE fail afterward would leave a row that
            # claims to exist but points at nothing -- a worse failure
            # mode). If this backend has no object_storage configured at
            # all, the orphan is simply left behind with no attempt --
            # documented as a stated limitation (see class docstring),
            # not silently swallowed.
            self._object_storage.delete(object_storage_key)
        return True

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
                "DELETE FROM zerobucket_images WHERE id = ANY(%s) "
                "RETURNING id, storage_backend, object_storage_key;",
                (image_ids,),
            )
            return cur.fetchall()

        try:
            rows = self._run(connection, work, operation="delete_many")
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed to delete image batch: {exc}") from exc
        deleted_ids = []
        for row in rows:
            image_id, storage_backend, object_storage_key = row
            deleted_ids.append(str(image_id))
            if storage_backend == "object_storage" and self._object_storage is not None:
                # Same best-effort, after-the-fact ordering as delete()
                # above, one at a time -- not batched into a single S3
                # DeleteObjects call in this first pass. See delete()'s
                # docstring for the ordering reasoning.
                self._object_storage.delete(object_storage_key)
        return deleted_ids

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
