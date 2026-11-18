"""Events and retry for the SQLite operations that had none before 0.35.0: sync
`get_stream()`, sync `tier_to_object_storage()` and async `tier_to_object_storage()`.

No external database is needed (SQLite files plus moto's S3 emulator). Streaming
scenarios run through BOTH read implementations (blobopen() and the 3.10 substr()
fallback) by switching the module flag, as in test_sqlite_stream_fallback.py.
"""

from __future__ import annotations

import gc
import io
import os
import sqlite3
import tempfile
import threading
import time

import pytest
from moto import mock_aws
from PIL import Image as PILImage

from zerobucket import AsyncSQLiteBackend, AsyncZeroBucket, SQLiteBackend, ZeroBucket
from zerobucket.adapters import sqlite as sqlite_mod
from zerobucket.adapters import sqlite_async as sqlite_async_mod
from zerobucket.exceptions import StorageError
from zerobucket.object_storage import ObjectStorage


def _noisy_jpeg(size=(200, 200)) -> bytes:
    img = PILImage.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def _remove_db_files(p: str) -> None:
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


@pytest.fixture(params=["blobopen", "substr"])
def impl(request, monkeypatch):
    if request.param == "blobopen" and not hasattr(sqlite3.Connection, "blobopen"):
        pytest.skip(
            "this Python has no Connection.blobopen (3.10): only the fallback exists"
        )
    monkeypatch.setattr(sqlite_mod, "_HAS_BLOBOPEN", request.param == "blobopen")
    return request.param


@pytest.fixture
def object_store():
    with mock_aws():
        import boto3

        bucket = "zerobucket-sqlite-instrumentation-test"
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
        yield ObjectStorage(bucket, region_name="us-east-1")


def _ops(events, name):
    return [e for e in events if e.operation == name]


def _storage_backend(path: str, image_id: str) -> str:
    conn = sqlite3.connect(path)
    try:
        return conn.execute(
            "SELECT storage_backend FROM zerobucket_images WHERE id = ?;", (image_id,)
        ).fetchone()[0]
    finally:
        conn.close()


# =====================================================================
# sync get_stream()
# =====================================================================


@pytest.fixture(params=["classic", "dedup"])
def stream_zb(request, path):
    events = []
    backend = SQLiteBackend(
        path, dedup=(request.param == "dedup"), on_operation=events.append
    )
    zb = ZeroBucket(backend=backend)
    yield zb, backend, events
    zb.close()


def test_get_stream_emits_lookup_plus_one_event_per_chunk(impl, stream_zb):
    zb, _backend, events = stream_zb
    data = _noisy_jpeg()
    image_id = zb.put(data)
    events.clear()
    chunks = list(zb.get_stream(image_id, chunk_size=1000))
    assert b"".join(chunks) == data
    assert len(chunks) > 1
    stream_events = _ops(events, "get_stream")
    assert len(stream_events) == 1 + len(chunks)
    assert len(events) == len(stream_events)  # nothing else was reported
    for e in stream_events:
        assert e.success is True and e.error is None and e.retry_count == 0
        assert e.duration_seconds >= 0


def test_get_stream_not_found_reports_only_the_lookup(stream_zb):
    zb, backend, events = stream_zb
    events.clear()
    assert backend.get_stream("00000000-0000-0000-0000-000000000000") is None
    assert [e.operation for e in events] == ["get_stream"]
    assert events[0].success is True


def test_get_stream_event_time_excludes_the_consumers_pauses(impl, stream_zb):
    zb, _backend, events = stream_zb
    image_id = zb.put(_noisy_jpeg())
    events.clear()
    count = 0
    for _chunk in zb.get_stream(image_id, chunk_size=2000):
        count += 1
        time.sleep(0.15)  # a slow consumer
    assert count > 1
    assert all(e.duration_seconds < 0.1 for e in events), [
        e.duration_seconds for e in events
    ]


def test_get_stream_chunk_failure_reports_a_failure_event(impl, path):
    """A stored size larger than the real bytes makes the read run dry: the same
    StorageError as a mid-stream delete, now also reported."""
    events = []
    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=events.append))
    data = _noisy_jpeg()
    image_id = zb.put(data)
    conn = sqlite3.connect(path)
    conn.execute("UPDATE zerobucket_images SET size_bytes = ?;", (len(data) + 5000,))
    conn.commit()
    conn.close()
    events.clear()
    with pytest.raises(StorageError, match="deleted while streaming"):
        list(zb.get_stream(image_id, chunk_size=len(data)))
    last = events[-1]
    assert last.operation == "get_stream"
    assert last.success is False
    assert "deleted while streaming" in last.error
    assert [e.success for e in events[:-1]] == [True] * (len(events) - 1)
    zb.close()


def test_get_stream_lookup_is_retried_on_lock_contention(impl, path, monkeypatch):
    events = []
    backend = SQLiteBackend(path, retry_base_delay=0.01, on_operation=events.append)
    zb = ZeroBucket(backend=backend)
    data = _noisy_jpeg()
    image_id = zb.put(data)
    real_connect = backend._connect  # noqa: SLF001
    failures = []

    def flaky_connect(*args, **kwargs):
        if len(failures) < 2:
            failures.append(1)
            raise sqlite3.OperationalError("database is locked")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(backend, "_connect", flaky_connect)
    events.clear()
    assert b"".join(zb.get_stream(image_id, chunk_size=1000)) == data
    lookup = events[0]
    assert lookup.operation == "get_stream" and lookup.success is True
    assert lookup.retry_count == 2
    assert all(e.retry_count == 0 for e in events[1:])  # chunk reads are never retried
    zb.close()


def test_get_stream_lookup_with_max_retries_zero_is_not_retried(path, monkeypatch):
    events = []
    backend = SQLiteBackend(path, max_retries=0, on_operation=events.append)
    calls = []

    def always_locked(*args, **kwargs):
        calls.append(1)
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(backend, "_connect", always_locked)
    events.clear()
    with pytest.raises(StorageError, match="locked"):
        backend.get_stream("00000000-0000-0000-0000-000000000000")
    assert len(calls) == 1
    assert (
        len(events) == 1 and events[0].success is False and events[0].retry_count == 0
    )


def test_get_stream_non_retryable_lookup_failure_is_reported_once(path):
    events = []
    backend = SQLiteBackend(path, on_operation=events.append)
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE zerobucket_images;")
    conn.commit()
    conn.close()
    events.clear()
    with pytest.raises(StorageError, match="no such table"):
        backend.get_stream("00000000-0000-0000-0000-000000000000")
    assert len(events) == 1
    assert events[0].success is False and events[0].retry_count == 0


def test_get_stream_connect_failure_is_a_storage_error(path):
    """Before 0.35.0 a failure to open the database leaked the raw sqlite3 error."""
    backend = SQLiteBackend(path)
    backend._database_path = os.path.join(path + "-missing-dir", "x.db")  # noqa: SLF001
    with pytest.raises(StorageError, match="Failed to retrieve image metadata"):
        backend.get_stream("00000000-0000-0000-0000-000000000000")


def test_get_stream_with_caller_connection_reports_but_never_retries(impl, path):
    events = []
    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=events.append))
    data = _noisy_jpeg()
    image_id = zb.put(data)
    events.clear()
    conn = sqlite3.connect(path)
    try:
        chunks = list(zb.get_stream(image_id, chunk_size=1000, connection=conn))
        assert b"".join(chunks) == data
        assert len(_ops(events, "get_stream")) == 1 + len(chunks)
        assert all(e.retry_count == 0 for e in events)
        conn.execute("SELECT 1;")  # the caller's connection is still open
    finally:
        conn.close()
    zb.close()


def test_get_stream_tiered_row_reports_only_the_lookup(path, object_store):
    events = []
    backend = SQLiteBackend(
        path, object_storage=object_store, on_operation=events.append
    )
    zb = ZeroBucket(backend=backend)
    data = _noisy_jpeg()
    image_id = zb.put(data)
    assert backend.tier_to_object_storage(image_id) is True
    events.clear()
    assert b"".join(zb.get_stream(image_id, chunk_size=1000)) == data
    assert [e.operation for e in events] == [
        "get_stream"
    ]  # the S3 reads are not SQLite
    zb.close()


def test_get_stream_abandoned_stream_adds_no_events_and_frees_the_file(impl, path):
    events = []
    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=events.append))
    image_id = zb.put(_noisy_jpeg())
    events.clear()
    stream = zb.get_stream(image_id, chunk_size=1000)
    next(stream)
    stream.close()
    assert len(events) == 2  # lookup + the one chunk read
    zb.close()
    _remove_db_files(path)  # fails on Windows if the connection leaked


def test_get_stream_callback_exception_does_not_break_the_stream(impl, path):
    def bad(event):
        raise RuntimeError("boom")

    zb = ZeroBucket(backend=SQLiteBackend(path, on_operation=bad))
    data = _noisy_jpeg()
    image_id = zb.put(data)
    assert b"".join(zb.get_stream(image_id, chunk_size=1000)) == data
    zb.close()


# =====================================================================
# sync tier_to_object_storage()
# =====================================================================


@pytest.fixture
def tier_env(path, object_store):
    events = []
    backend = SQLiteBackend(
        path,
        object_storage=object_store,
        retry_base_delay=0.01,
        on_operation=events.append,
    )
    zb = ZeroBucket(backend=backend)
    image_id = zb.put(_noisy_jpeg())
    events.clear()
    yield backend, zb, image_id, events, path
    zb.close()


def test_tier_reports_one_event_for_each_outcome(tier_env):
    backend, _zb, image_id, events, _path = tier_env
    assert backend.tier_to_object_storage(image_id) is True
    assert backend.tier_to_object_storage(image_id) is False
    assert (
        backend.tier_to_object_storage("00000000-0000-0000-0000-000000000000") is None
    )
    assert [e.operation for e in events] == ["tier_to_object_storage"] * 3
    assert all(e.success and e.error is None and e.retry_count == 0 for e in events)


def test_tier_failed_upload_is_reported_not_retried_and_leaves_the_row(
    tier_env, object_store
):
    backend, _zb, image_id, events, path = tier_env
    uploads = []

    def broken_upload(*args, **kwargs):
        uploads.append(1)
        raise RuntimeError("simulated network failure")

    object_store.upload = broken_upload
    with pytest.raises(StorageError, match="Failed to tier"):
        backend.tier_to_object_storage(image_id)
    assert len(uploads) == 1
    assert len(events) == 1
    assert events[0].success is False and events[0].retry_count == 0
    assert "simulated network failure" in events[0].error
    assert _storage_backend(path, image_id) == "sqlite"


def test_tier_retries_the_whole_transaction_including_the_upload(
    tier_env, object_store, monkeypatch
):
    """A lock error after the upload (injected where the UPDATE's arguments are built)
    rolls back and replays everything. The upload key is deterministic, so the replay
    overwrites the same object."""
    backend, zb, image_id, events, path = tier_env
    uploads = []
    real_upload = object_store.upload

    def counting_upload(*args, **kwargs):
        uploads.append(1)
        return real_upload(*args, **kwargs)

    object_store.upload = counting_upload
    real_now = sqlite_mod._now_iso  # noqa: SLF001
    state = {"failed": False}

    def flaky_now():
        if not state["failed"]:
            state["failed"] = True
            raise sqlite3.OperationalError("database is locked")
        return real_now()

    monkeypatch.setattr(sqlite_mod, "_now_iso", flaky_now)
    assert backend.tier_to_object_storage(image_id) is True
    assert len(uploads) == 2
    assert len(events) == 1 and events[0].success is True and events[0].retry_count == 1
    assert _storage_backend(path, image_id) == "object_storage"
    assert zb.get(image_id).data  # readable through S3 afterwards


def test_tier_retries_real_lock_contention_and_succeeds(tier_env, short_busy_timeout):
    backend, _zb, image_id, events, path = tier_env
    backend._max_retries = 8  # noqa: SLF001
    backend._retry_base_delay = 0.1  # noqa: SLF001
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    timer = threading.Timer(0.5, lambda: holder.execute("COMMIT;"))
    timer.start()
    try:
        assert backend.tier_to_object_storage(image_id) is True
    finally:
        timer.cancel()
        timer.join()
        holder.close()
    assert len(events) == 1 and events[0].success is True
    assert events[0].retry_count >= 1
    assert _storage_backend(path, image_id) == "object_storage"


def test_tier_stuck_lock_fails_after_retries_without_uploading(
    tier_env, object_store, short_busy_timeout
):
    backend, _zb, image_id, events, path = tier_env
    backend._max_retries = 2  # noqa: SLF001
    uploads = []
    object_store.upload = lambda *a, **k: uploads.append(1)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    try:
        with pytest.raises(StorageError, match="locked"):
            backend.tier_to_object_storage(image_id)
    finally:
        holder.execute("ROLLBACK;")
        holder.close()
    assert uploads == []  # never got the write lock, so never uploaded
    assert (
        len(events) == 1 and events[0].success is False and events[0].retry_count == 2
    )
    assert _storage_backend(path, image_id) == "sqlite"


def test_tier_max_retries_zero_is_a_single_attempt(tier_env, short_busy_timeout):
    backend, _zb, image_id, events, path = tier_env
    backend._max_retries = 0  # noqa: SLF001
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    try:
        with pytest.raises(StorageError, match="locked"):
            backend.tier_to_object_storage(image_id)
    finally:
        holder.execute("ROLLBACK;")
        holder.close()
    assert len(events) == 1 and events[0].retry_count == 0


def test_tier_with_caller_connection_reports_but_never_retries(
    tier_env, short_busy_timeout
):
    backend, _zb, image_id, events, path = tier_env
    mine = sqlite3.connect(path, isolation_level=None, timeout=0.05)
    try:
        assert backend.tier_to_object_storage(image_id, connection=mine) is True
        assert len(events) == 1 and events[0].success and events[0].retry_count == 0
        mine.execute("SELECT 1;")  # not closed by the backend
    finally:
        mine.close()


def test_tier_with_caller_connection_does_not_retry_a_lock_error(
    tier_env, short_busy_timeout
):
    backend, _zb, image_id, events, path = tier_env
    mine = sqlite3.connect(path, isolation_level=None, timeout=0.05)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    try:
        with pytest.raises(StorageError, match="locked"):
            backend.tier_to_object_storage(image_id, connection=mine)
    finally:
        holder.execute("ROLLBACK;")
        holder.close()
        mine.close()
    assert (
        len(events) == 1 and events[0].success is False and events[0].retry_count == 0
    )


def test_tier_without_object_storage_raises_before_any_event(path):
    events = []
    backend = SQLiteBackend(path, on_operation=events.append)
    events.clear()
    with pytest.raises(StorageError, match="object_storage"):
        backend.tier_to_object_storage("00000000-0000-0000-0000-000000000000")
    assert events == []


def test_tier_leaves_no_open_transaction_or_leaked_file_handle(tier_env):
    backend, zb, image_id, _events, path = tier_env
    assert backend.tier_to_object_storage(image_id) is True
    other = sqlite3.connect(path, timeout=0.5)
    try:
        other.execute("BEGIN IMMEDIATE;")  # would fail if tiering left its lock behind
        other.execute("ROLLBACK;")
    finally:
        other.close()


# =====================================================================
# async tier_to_object_storage()
# =====================================================================


@pytest.fixture
async def async_tier_env(path, object_store):
    events = []
    backend = AsyncSQLiteBackend(
        path,
        object_storage=object_store,
        retry_base_delay=0.01,
        on_operation=events.append,
    )
    zb = AsyncZeroBucket(backend=backend)
    image_id = await zb.put(_noisy_jpeg())
    events.clear()
    yield backend, zb, image_id, events, path
    await zb.close()


async def test_async_tier_reports_one_event_for_each_outcome(async_tier_env):
    backend, _zb, image_id, events, _path = async_tier_env
    assert await backend.tier_to_object_storage(image_id) is True
    assert await backend.tier_to_object_storage(image_id) is False
    assert (
        await backend.tier_to_object_storage("00000000-0000-0000-0000-000000000000")
        is None
    )
    assert [e.operation for e in events] == ["tier_to_object_storage"] * 3
    assert all(e.success and e.error is None and e.retry_count == 0 for e in events)


async def test_async_tier_failed_upload_is_reported_not_retried(
    async_tier_env, object_store
):
    backend, _zb, image_id, events, path = async_tier_env
    uploads = []

    def broken_upload(*args, **kwargs):
        uploads.append(1)
        raise RuntimeError("simulated network failure")

    object_store.upload = broken_upload
    with pytest.raises(StorageError, match="Failed to tier"):
        await backend.tier_to_object_storage(image_id)
    assert len(uploads) == 1
    assert (
        len(events) == 1 and events[0].success is False and events[0].retry_count == 0
    )
    assert _storage_backend(path, image_id) == "sqlite"


async def test_async_tier_retries_the_whole_transaction(
    async_tier_env, object_store, monkeypatch
):
    backend, zb, image_id, events, path = async_tier_env
    uploads = []
    real_upload = object_store.upload

    def counting_upload(*args, **kwargs):
        uploads.append(1)
        return real_upload(*args, **kwargs)

    object_store.upload = counting_upload
    real_now = sqlite_async_mod._now_iso  # noqa: SLF001
    state = {"failed": False}

    def flaky_now():
        if not state["failed"]:
            state["failed"] = True
            raise sqlite3.OperationalError("database is locked")
        return real_now()

    monkeypatch.setattr(sqlite_async_mod, "_now_iso", flaky_now)
    assert await backend.tier_to_object_storage(image_id) is True
    assert len(uploads) == 2
    assert len(events) == 1 and events[0].success is True and events[0].retry_count == 1
    assert _storage_backend(path, image_id) == "object_storage"
    assert (await zb.get(image_id)).data


async def test_async_tier_retries_real_lock_contention(
    async_tier_env, short_busy_timeout
):
    backend, _zb, image_id, events, path = async_tier_env
    backend._max_retries = 8  # noqa: SLF001
    backend._retry_base_delay = 0.1  # noqa: SLF001
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    timer = threading.Timer(0.5, lambda: holder.execute("COMMIT;"))
    timer.start()
    try:
        assert await backend.tier_to_object_storage(image_id) is True
    finally:
        timer.cancel()
        timer.join()
        holder.close()
    assert len(events) == 1 and events[0].success is True and events[0].retry_count >= 1
    assert _storage_backend(path, image_id) == "object_storage"


async def test_async_tier_stuck_lock_fails_without_uploading(
    async_tier_env, object_store, short_busy_timeout
):
    backend, _zb, image_id, events, path = async_tier_env
    backend._max_retries = 2  # noqa: SLF001
    uploads = []
    object_store.upload = lambda *a, **k: uploads.append(1)
    holder = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    holder.execute("BEGIN IMMEDIATE;")
    try:
        with pytest.raises(StorageError, match="locked"):
            await backend.tier_to_object_storage(image_id)
    finally:
        holder.execute("ROLLBACK;")
        holder.close()
    assert uploads == []
    assert (
        len(events) == 1 and events[0].success is False and events[0].retry_count == 2
    )


async def test_async_tier_with_caller_connection_reports_once(async_tier_env):
    import aiosqlite

    backend, _zb, image_id, events, path = async_tier_env
    async with aiosqlite.connect(path, isolation_level=None) as mine:
        assert await backend.tier_to_object_storage(image_id, connection=mine) is True
        await mine.execute("SELECT 1;")  # still open
    assert len(events) == 1 and events[0].success and events[0].retry_count == 0


async def test_async_tier_dedup_and_missing_object_storage_still_raise_without_events(
    path,
):
    events = []
    backend = AsyncSQLiteBackend(path, dedup=True, on_operation=events.append)
    await backend._ensure_ready()  # noqa: SLF001
    events.clear()
    with pytest.raises(StorageError):
        await backend.tier_to_object_storage("00000000-0000-0000-0000-000000000000")
    assert events == []
