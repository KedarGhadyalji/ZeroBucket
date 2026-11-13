"""Tests for `on_operation` events and busy-retry on SQLiteBackend and
AsyncSQLiteBackend (0.27.0). No external database is needed.

Injected `sqlite3.OperationalError`s drive the retry loop
deterministically. The "real contention" tests shrink the module's busy
timeout so a genuine `database is locked` surfaces in milliseconds, hold
a write lock from a separate raw connection, and release it mid-retry.
"""

from __future__ import annotations

import contextlib
import gc
import io
import os
import sqlite3
import tempfile
import threading
import time

import pytest
from PIL import Image as PILImage

from zerobucket import AsyncSQLiteBackend, AsyncZeroBucket, SQLiteBackend, ZeroBucket
from zerobucket.adapters import sqlite as sqlite_mod
from zerobucket.adapters import sqlite_async as sqlite_async_mod
from zerobucket.adapters.sqlite import _is_retryable
from zerobucket.exceptions import StorageError

NIL = "00000000-0000-0000-0000-000000000000"


def _jpeg(size=(64, 48)) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", size, color=(10, 20, 30)).save(buf, format="JPEG")
    return buf.getvalue()


def _noisy_jpeg(size=(200, 200)) -> bytes:
    img = PILImage.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def _row(data: bytes) -> dict:
    return {
        "data": data,
        "mime_type": "image/jpeg",
        "original_filename": None,
        "size_bytes": len(data),
        "width": 1,
        "height": 1,
        "checksum_sha256": "0" * 64,
    }


def _remove_db_files(p: str) -> None:
    """Delete a SQLite file and its WAL/SHM companions. Windows refuses to
    delete a file while ANY handle is open, and releases a just-closed
    connection's handle a moment later, so collect garbage and retry
    briefly before giving up. A handle that is genuinely leaked still
    fails the test, as it should."""
    gc.collect()
    for ext in ("", "-wal", "-shm"):
        target = p + ext
        for attempt in range(10):
            try:
                if os.path.exists(target):
                    os.remove(target)
                break
            except PermissionError:
                if attempt == 9:
                    raise
                time.sleep(0.1)
                gc.collect()


@pytest.fixture
def path():
    p = tempfile.mktemp(suffix=".db")
    yield p
    _remove_db_files(p)


@pytest.fixture
def short_busy_timeout(monkeypatch):
    """Make a real lock conflict fail in ~50 ms instead of 10 s."""
    for mod in (sqlite_mod, sqlite_async_mod):
        monkeypatch.setattr(mod, "_BUSY_TIMEOUT_SECONDS", 0.05)
        monkeypatch.setattr(mod, "_BUSY_TIMEOUT_MS", 50)


# ---- classification ---------------------------------------------------


def test_is_retryable_classification():
    op = sqlite3.OperationalError
    assert _is_retryable(op("database is locked")) is True
    assert _is_retryable(op("database table is locked")) is True
    assert _is_retryable(op("no such table: zerobucket_images")) is False
    assert _is_retryable(op("unable to open database file")) is False
    assert _is_retryable(op("attempt to write a readonly database")) is False
    assert _is_retryable(sqlite3.IntegrityError("UNIQUE constraint failed")) is False
    assert _is_retryable(ValueError("database is locked")) is False


def test_is_retryable_uses_error_code_when_available(path, short_busy_timeout):
    """A REAL lock error, whichever way this Python reports it."""
    SQLiteBackend(path)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    other = sqlite3.connect(path, timeout=0.05)
    try:
        with pytest.raises(sqlite3.OperationalError) as info:
            other.execute("UPDATE zerobucket_images SET original_filename = 'x';")
        assert _is_retryable(info.value) is True
    finally:
        other.close()
        holder.close()


# ---- sync: retry loop with injected failures ------------------------------


def test_sync_retry_recovers_from_injected_lock_error(path):
    events = []
    backend = SQLiteBackend(path, retry_base_delay=0.01, on_operation=events.append)
    attempts = []

    def flaky(conn):
        attempts.append(1)
        if len(attempts) < 3:
            raise sqlite3.OperationalError("database is locked")
        return conn.execute("SELECT 1;").fetchone()

    events.clear()
    assert backend._run(None, flaky, operation="test_op") == (1,)  # noqa: SLF001
    assert len(attempts) == 3
    assert len(events) == 1
    assert events[0].success is True
    assert events[0].retry_count == 2


def test_sync_retry_exhausts_after_max_retries(path):
    events = []
    backend = SQLiteBackend(
        path, max_retries=2, retry_base_delay=0.01, on_operation=events.append
    )
    attempts = []

    def always_locked(conn):
        attempts.append(1)
        raise sqlite3.OperationalError("database is locked")

    events.clear()
    with pytest.raises(sqlite3.OperationalError):
        backend._run(None, always_locked, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 3
    assert events[0].success is False
    assert events[0].retry_count == 2


def test_sync_max_retries_zero_disables_retry(path):
    backend = SQLiteBackend(path, max_retries=0)
    attempts = []

    def locked(conn):
        attempts.append(1)
        raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError):
        backend._run(None, locked, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 1


def test_sync_non_retryable_error_is_not_retried(path):
    backend = SQLiteBackend(path, retry_base_delay=0.01)
    attempts = []

    def bad(conn):
        attempts.append(1)
        raise sqlite3.OperationalError("no such table: nope")

    with pytest.raises(sqlite3.OperationalError):
        backend._run(None, bad, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 1


def test_sync_caller_supplied_connection_is_never_retried(path):
    events = []
    backend = SQLiteBackend(path, on_operation=events.append)
    conn = backend._connect()  # noqa: SLF001
    attempts = []

    def locked(c):
        attempts.append(1)
        raise sqlite3.OperationalError("database is locked")

    try:
        events.clear()
        with pytest.raises(sqlite3.OperationalError):
            backend._run(conn, locked, operation="test_op")  # noqa: SLF001
        assert len(attempts) == 1
        assert events[0].retry_count == 0
        assert events[0].success is False
    finally:
        conn.close()


def test_sync_retry_reruns_whole_batch_without_duplicating_rows(path):
    backend = SQLiteBackend(path, retry_base_delay=0.01)
    real_run = backend._run  # noqa: SLF001
    state = {"failed": False}

    def run_with_one_late_failure(connection, work, *, operation):
        def wrapped(conn):
            result = work(conn)  # rows are inserted here...
            if not state["failed"]:
                state["failed"] = True
                raise sqlite3.OperationalError("database is locked")
            return result

        return real_run(connection, wrapped, operation=operation)

    backend._run = run_with_one_late_failure  # noqa: SLF001
    try:
        ids = backend.put_many([_row(_jpeg()), _row(_jpeg())])
    finally:
        backend._run = real_run  # noqa: SLF001
    assert state["failed"] is True
    assert len(ids) == 2
    with contextlib.closing(sqlite3.connect(path)) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM zerobucket_images;").fetchone()[0] == 2
        )


def test_sync_retry_recovers_from_real_lock_contention(path, short_busy_timeout):
    backend = SQLiteBackend(path, retry_base_delay=0.1, max_retries=5)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    timer = threading.Timer(0.5, lambda: holder.execute("COMMIT;"))
    timer.start()
    events = []
    backend._on_operation = events.append  # noqa: SLF001
    try:
        image_id = backend.put(**_row(_jpeg()))  # blocked by the held write lock
        assert backend.exists(image_id) is True
        put_events = [e for e in events if e.operation == "put"]
        assert put_events[0].success is True
        assert (
            put_events[0].retry_count >= 1
        ), "the first attempt should have hit the lock"
    finally:
        timer.cancel()
        timer.join()
        holder.close()


def test_sync_stuck_lock_fails_after_retries_exhausted(path, short_busy_timeout):
    backend = SQLiteBackend(path, retry_base_delay=0.01, max_retries=2)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    events = []
    backend._on_operation = events.append  # noqa: SLF001
    try:
        with pytest.raises(StorageError, match="locked"):
            backend.put(**_row(_jpeg()))
        assert events[-1].success is False
        assert events[-1].retry_count == 2
    finally:
        holder.execute("ROLLBACK;")
        holder.close()


# ---- sync: events --------------------------------------------------------


def test_sync_migrate_emits_event_on_construction(path):
    events = []
    SQLiteBackend(path, on_operation=events.append)
    assert [e.operation for e in events] == ["migrate"]
    assert events[0].success is True


def test_sync_no_migrate_event_when_auto_migrate_is_off(path):
    SQLiteBackend(path)  # create schema
    events = []
    SQLiteBackend(path, auto_migrate=False, on_operation=events.append)
    assert events == []


def test_sync_on_operation_receives_success_events(path):
    events = []
    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=events.append))
    events.clear()
    image_id = zb.put(_jpeg())
    zb.get(image_id)
    zb.metadata(image_id)
    zb.exists(image_id)
    zb.delete(image_id)
    by_op = {e.operation: e for e in events}
    assert set(by_op) == {"put", "get", "get_metadata", "exists", "delete"}
    for e in by_op.values():
        assert e.success is True and e.error is None and e.retry_count == 0
        assert e.duration_seconds >= 0


def test_sync_on_operation_receives_failure_events(path):
    events = []
    backend = SQLiteBackend(path, on_operation=events.append)
    events.clear()
    with contextlib.closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("DROP TABLE zerobucket_images;")
    with pytest.raises(StorageError):
        backend.exists(NIL)
    assert len(events) == 1
    assert events[0].operation == "exists"
    assert events[0].success is False
    assert "no such table" in events[0].error
    assert events[0].retry_count == 0  # not retryable, so not retried


def test_sync_dedup_reports_same_operation_names_as_classic(path):
    events = []
    zb = ZeroBucket(backend=SQLiteBackend(path, dedup=True, on_operation=events.append))
    events.clear()
    image_id = zb.put(_jpeg())
    zb.get(image_id)
    zb.delete(image_id)
    assert {e.operation for e in events} == {"put", "get", "delete"}


def test_sync_callback_exception_does_not_break_operation(path):
    def bad(event):
        raise RuntimeError("boom")

    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=bad))
    image_id = zb.put(_jpeg())
    assert zb.exists(image_id) is True


def test_sync_get_stream_is_not_instrumented(path):
    """Documented: sync SQLite streams over ONE connection it opens
    itself, outside `_run()`, so it emits no events and has no retry (a
    WAL read does not block on writers). Async SQLite, which does go
    through `_run()` per chunk, is covered below."""
    events = []
    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=events.append))
    image_id = zb.put(_noisy_jpeg())
    events.clear()
    chunks = list(zb.get_stream(image_id, chunk_size=1000))
    assert len(chunks) > 1
    assert events == []


# ---- async -----------------------------------------------------------------


async def _ready_async(path, **kwargs) -> AsyncSQLiteBackend:
    backend = AsyncSQLiteBackend(path, **kwargs)
    await backend._ensure_ready()  # noqa: SLF001
    return backend


async def test_async_retry_recovers_from_injected_lock_error(path):
    events = []
    backend = await _ready_async(
        path, retry_base_delay=0.01, on_operation=events.append
    )
    attempts = []

    async def flaky(conn):
        attempts.append(1)
        if len(attempts) < 3:
            raise sqlite3.OperationalError("database is locked")
        cur = await conn.execute("SELECT 1;")
        return await cur.fetchone()

    events.clear()
    assert tuple(await backend._run(flaky, operation="test_op")) == (1,)  # noqa: SLF001
    assert len(attempts) == 3
    assert events[0].success is True
    assert events[0].retry_count == 2


async def test_async_retry_exhausts_after_max_retries(path):
    events = []
    backend = await _ready_async(
        path, max_retries=2, retry_base_delay=0.01, on_operation=events.append
    )
    attempts = []

    async def locked(conn):
        attempts.append(1)
        raise sqlite3.OperationalError("database is locked")

    events.clear()
    with pytest.raises(sqlite3.OperationalError):
        await backend._run(locked, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 3
    assert events[0].success is False and events[0].retry_count == 2


async def test_async_max_retries_zero_and_non_retryable(path):
    backend = await _ready_async(path, max_retries=0)
    attempts = []

    async def locked(conn):
        attempts.append(1)
        raise sqlite3.OperationalError("database is locked")

    with pytest.raises(sqlite3.OperationalError):
        await backend._run(locked, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 1

    backend2 = await _ready_async(path, retry_base_delay=0.01)
    attempts2 = []

    async def bad(conn):
        attempts2.append(1)
        raise sqlite3.OperationalError("no such table: nope")

    with pytest.raises(sqlite3.OperationalError):
        await backend2._run(bad, operation="test_op")  # noqa: SLF001
    assert len(attempts2) == 1


async def test_async_retry_reruns_whole_batch_without_duplicating_rows(path):
    backend = await _ready_async(path, retry_base_delay=0.01)
    real_run = backend._run  # noqa: SLF001
    state = {"failed": False}

    async def run_with_one_late_failure(work, *, operation, connection=None):
        async def wrapped(conn):
            result = await work(conn)
            if not state["failed"]:
                state["failed"] = True
                raise sqlite3.OperationalError("database is locked")
            return result

        return await real_run(wrapped, operation=operation, connection=connection)

    backend._run = run_with_one_late_failure  # noqa: SLF001
    try:
        ids = await backend.put_many([_row(_jpeg()), _row(_jpeg())])
    finally:
        backend._run = real_run  # noqa: SLF001
    assert state["failed"] is True and len(ids) == 2
    with contextlib.closing(sqlite3.connect(path)) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM zerobucket_images;").fetchone()[0] == 2
        )


async def test_async_retry_recovers_from_real_lock_contention(path, short_busy_timeout):
    backend = await _ready_async(path, retry_base_delay=0.1, max_retries=5)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    timer = threading.Timer(0.5, lambda: holder.execute("COMMIT;"))
    timer.start()
    events = []
    backend._on_operation = events.append  # noqa: SLF001
    try:
        image_id = await backend.put(**_row(_jpeg()))
        assert await backend.exists(image_id) is True
        put_events = [e for e in events if e.operation == "put"]
        assert put_events[0].success is True
        assert put_events[0].retry_count >= 1
    finally:
        timer.cancel()
        timer.join()
        holder.close()


async def test_async_migrate_event_and_success_events(path):
    events = []
    zb = AsyncZeroBucket(backend=AsyncSQLiteBackend(path, on_operation=events.append))
    try:
        assert events == []  # lazy
        image_id = await zb.put(_jpeg())
        assert [e.operation for e in events] == ["migrate", "put"]
        events.clear()
        await zb.get(image_id)
        await zb.metadata(image_id)
        await zb.exists(image_id)
        await zb.delete(image_id)
        assert [e.operation for e in events] == [
            "get",
            "get_metadata",
            "exists",
            "delete",
        ]
        assert all(e.success and e.retry_count == 0 for e in events)
    finally:
        await zb.close()


async def test_async_failure_event_and_callback_exception(path):
    events = []
    backend = AsyncSQLiteBackend(path, on_operation=events.append)
    await backend._ensure_ready()  # noqa: SLF001
    with contextlib.closing(sqlite3.connect(path)) as conn:
        conn.execute("DROP TABLE zerobucket_images;")
        conn.commit()
    events.clear()
    with pytest.raises(StorageError):
        await backend.exists(NIL)
    assert events[0].operation == "exists" and events[0].success is False
    assert events[0].retry_count == 0

    def bad(event):
        raise RuntimeError("boom")

    zb = AsyncZeroBucket(backend=AsyncSQLiteBackend(path + "2", on_operation=bad))
    try:
        image_id = await zb.put(_jpeg())  # must not raise
        assert await zb.exists(image_id) is True
    finally:
        await zb.close()
        _remove_db_files(path + "2")


async def test_async_get_stream_emits_info_lookup_plus_one_event_per_chunk(path):
    events = []
    zb = AsyncZeroBucket(backend=AsyncSQLiteBackend(path, on_operation=events.append))
    try:
        image_id = await zb.put(_noisy_jpeg())
        events.clear()
        stream = await zb.get_stream(image_id, chunk_size=1000)
        chunks = [c async for c in stream]
        assert len(chunks) > 1
        assert len([e for e in events if e.operation == "get_stream"]) == 1 + len(
            chunks
        )
    finally:
        await zb.close()
