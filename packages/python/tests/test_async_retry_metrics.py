"""Tests for AsyncPostgresBackend's `on_operation` metrics hook and
automatic retry/backoff (0.27.0), the async counterpart of the sync
PostgresBackend behaviour covered in test_retry.py and
test_pool_and_metrics.py.

The retry loop is driven with injected psycopg exceptions so it is
deterministic, and ONE test forces a REAL PostgreSQL deadlock (40P01)
between two concurrent transactions, so the loop is also proven against
a genuine server-side failure and not only against synthetic ones.
"""

from __future__ import annotations

import asyncio
import io
import os

import psycopg
import pytest
from PIL import Image as PILImage

from zerobucket import AsyncPostgresBackend, AsyncZeroBucket
from zerobucket.exceptions import StorageError

from .conftest import TEST_DATABASE_URL

NIL = "00000000-0000-0000-0000-000000000000"


def _noisy_jpeg_bytes(size=(200, 200)) -> bytes:
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


@pytest.fixture(autouse=True)
def _clean_table(_db_available):
    """Create the REAL schema (via the sync backend's own migration), then
    empty it, so these tests work on a brand-new database too."""
    from zerobucket import ZeroBucket

    zb = ZeroBucket(database_url=TEST_DATABASE_URL)
    try:
        with zb._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
            cur.execute("TRUNCATE TABLE zerobucket_images;")
    finally:
        zb.close()
    yield


@pytest.fixture
async def make_backend():
    created: list[AsyncPostgresBackend] = []

    def factory(**kwargs) -> AsyncPostgresBackend:
        kwargs.setdefault("retry_base_delay", 0.01)
        backend = AsyncPostgresBackend(TEST_DATABASE_URL, **kwargs)
        created.append(backend)
        return backend

    yield factory
    for backend in created:
        await backend._pool.close()  # noqa: SLF001


# ---- retry loop, driven with injected failures ---------------------------


async def test_retry_recovers_from_injected_operational_error(make_backend):
    events = []
    backend = make_backend(on_operation=events.append)
    attempts = []

    async def flaky_work(cur):
        attempts.append(1)
        if len(attempts) < 3:
            raise psycopg.OperationalError("simulated connection failure")
        await cur.execute("SELECT 1;")
        return await cur.fetchone()

    await backend._ensure_ready()  # noqa: SLF001
    events.clear()
    result = await backend._run(flaky_work, operation="test_op")  # noqa: SLF001
    assert result == (1,)
    assert len(attempts) == 3
    assert len(events) == 1
    assert events[0].operation == "test_op"
    assert events[0].success is True
    assert events[0].retry_count == 2
    assert events[0].error is None


async def test_serialization_failure_is_retried(make_backend):
    backend = make_backend()
    attempts = []

    async def work(cur):
        attempts.append(1)
        if len(attempts) == 1:
            raise psycopg.errors.SerializationFailure("simulated")
        return "ok"

    assert await backend._run(work, operation="test_op") == "ok"  # noqa: SLF001
    assert len(attempts) == 2


async def test_retry_exhausts_and_raises_after_max_retries(make_backend):
    events = []
    backend = make_backend(max_retries=2, on_operation=events.append)
    attempts = []

    async def always_flaky(cur):
        attempts.append(1)
        raise psycopg.OperationalError("still down")

    await backend._ensure_ready()  # noqa: SLF001
    events.clear()
    with pytest.raises(psycopg.OperationalError):
        await backend._run(always_flaky, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 3  # initial + 2 retries
    assert len(events) == 1
    assert events[0].success is False
    assert events[0].retry_count == 2
    assert "still down" in events[0].error


async def test_max_retries_zero_disables_retry(make_backend):
    backend = make_backend(max_retries=0)
    attempts = []

    async def flaky(cur):
        attempts.append(1)
        raise psycopg.OperationalError("down")

    with pytest.raises(psycopg.OperationalError):
        await backend._run(flaky, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 1


async def test_non_retryable_error_is_not_retried(make_backend):
    events = []
    backend = make_backend(on_operation=events.append)
    attempts = []

    async def bad(cur):
        attempts.append(1)
        raise psycopg.errors.UniqueViolation("simulated")

    await backend._ensure_ready()  # noqa: SLF001
    events.clear()
    with pytest.raises(psycopg.errors.UniqueViolation):
        await backend._run(bad, operation="test_op")  # noqa: SLF001
    assert len(attempts) == 1
    assert events[0].success is False
    assert events[0].retry_count == 0


async def test_failed_attempts_do_not_leak_pool_connections(make_backend):
    backend = make_backend(pool_min_size=1, pool_max_size=1, pool_timeout=2)

    async def always_flaky(cur):
        raise psycopg.OperationalError("down")

    with pytest.raises(psycopg.OperationalError):
        await backend._run(always_flaky, operation="test_op")  # noqa: SLF001
    assert await backend.exists(NIL) is False  # a leaked slot would time out here


async def test_retry_reruns_whole_batch_without_duplicating_rows(make_backend, jpeg_bytes):
    """A put_many() that fails AFTER inserting its rows must roll them
    back before the retry, so the batch lands exactly once."""
    backend = make_backend()
    real_run = backend._run  # noqa: SLF001
    state = {"failed": False}

    async def run_with_one_late_failure(work, *, operation):
        async def wrapped(cur):
            result = await work(cur)  # the rows are inserted here...
            if not state["failed"]:
                state["failed"] = True
                raise psycopg.OperationalError("lost after inserting")
            return result

        return await real_run(wrapped, operation=operation)

    backend._run = run_with_one_late_failure  # noqa: SLF001
    try:
        ids = await backend.put_many([_row(jpeg_bytes), _row(jpeg_bytes)])
    finally:
        backend._run = real_run  # noqa: SLF001

    assert state["failed"] is True
    assert len(ids) == 2

    async def count(cur):
        await cur.execute("SELECT COUNT(*) FROM zerobucket_images;")
        return (await cur.fetchone())[0]

    # not 4: the first attempt's rows were rolled back, not committed
    assert await backend._run(count, operation="count") == 2  # noqa: SLF001


async def test_retry_recovers_from_a_real_deadlock(make_backend, jpeg_bytes):
    """End to end, not synthetic: two concurrent transactions lock two
    rows in opposite order. PostgreSQL aborts one with a REAL
    DeadlockDetected (40P01); the retry loop re-runs it and BOTH
    operations finish."""
    events = []
    backend = make_backend(pool_max_size=4, on_operation=events.append)
    id_a = await backend.put(**_row(jpeg_bytes))
    id_b = await backend.put(**_row(jpeg_bytes))
    events.clear()

    a_holds_first = asyncio.Event()
    b_holds_first = asyncio.Event()
    attempts = {"a": 0, "b": 0}

    async def lock(cur, image_id):
        await cur.execute(
            "UPDATE zerobucket_images SET original_filename = %s WHERE id = %s;",
            ("deadlock-test", image_id),
        )

    async def work_a(cur):
        attempts["a"] += 1
        await lock(cur, id_a)
        a_holds_first.set()
        await asyncio.wait_for(b_holds_first.wait(), 10)
        await lock(cur, id_b)

    async def work_b(cur):
        attempts["b"] += 1
        await lock(cur, id_b)
        b_holds_first.set()
        await asyncio.wait_for(a_holds_first.wait(), 10)
        await lock(cur, id_a)

    await asyncio.gather(
        backend._run(work_a, operation="deadlock_a"),  # noqa: SLF001
        backend._run(work_b, operation="deadlock_b"),  # noqa: SLF001
    )
    assert attempts["a"] + attempts["b"] >= 3, "one side should have been the deadlock victim"
    assert len(events) == 2
    assert all(e.success for e in events)
    assert max(e.retry_count for e in events) >= 1


# ---- on_operation through the public async client ---------------------


async def test_on_operation_receives_success_events(_db_available, jpeg_bytes):
    events = []
    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    try:
        await zb.put(jpeg_bytes)  # triggers lazy init
        events.clear()
        image_id = await zb.put(jpeg_bytes)
        await zb.get(image_id)
        await zb.metadata(image_id)
        await zb.exists(image_id)
        await zb.delete(image_id)

        by_op = {e.operation: e for e in events}
        assert set(by_op) == {"put", "get", "get_metadata", "exists", "delete"}
        for event in by_op.values():
            assert event.success is True
            assert event.error is None
            assert event.retry_count == 0
            assert event.duration_seconds >= 0
    finally:
        await zb.close()


async def test_async_client_passes_retry_settings_to_backend(_db_available):
    zb = AsyncZeroBucket(
        database_url=TEST_DATABASE_URL, max_retries=7, retry_base_delay=0.5
    )
    try:
        assert zb._backend._max_retries == 7  # noqa: SLF001
        assert zb._backend._retry_base_delay == 0.5  # noqa: SLF001
    finally:
        await zb.close()


async def test_on_operation_receives_failure_events(make_backend):
    events = []
    backend = make_backend(on_operation=events.append)
    row = _row(b"x")
    row["checksum_sha256"] = "x" * 200  # too long for CHAR(64) -> DataError
    with pytest.raises(StorageError):
        await backend.put(**row)
    put_events = [e for e in events if e.operation == "put"]
    assert len(put_events) == 1
    assert put_events[0].success is False
    assert put_events[0].error is not None
    assert put_events[0].retry_count == 0


async def test_on_operation_exception_does_not_break_real_operation(
    _db_available, jpeg_bytes
):
    def bad_callback(event):
        raise RuntimeError("boom")

    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL, on_operation=bad_callback)
    try:
        image_id = await zb.put(jpeg_bytes)  # must not raise despite the callback failing
        assert await zb.exists(image_id) is True
    finally:
        await zb.close()


async def test_migrate_emits_operation_event_once(make_backend):
    events = []
    backend = make_backend(on_operation=events.append)
    assert events == []  # lazy: constructing does no I/O
    await asyncio.gather(*(backend.exists(NIL) for _ in range(5)))
    migrate_events = [e for e in events if e.operation == "migrate"]
    assert len(migrate_events) == 1
    assert migrate_events[0].success is True
    assert migrate_events[0].retry_count == 0


async def test_no_migrate_event_when_auto_migrate_is_off(make_backend):
    events = []
    backend = make_backend(auto_migrate=False, on_operation=events.append)
    await backend.exists(NIL)
    assert [e.operation for e in events] == ["exists"]


async def test_get_stream_emits_metadata_lookup_plus_one_event_per_chunk(_db_available):
    events = []
    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    try:
        image_id = await zb.put(_noisy_jpeg_bytes())
        events.clear()
        stream = await zb.get_stream(image_id, chunk_size=1000)
        chunks = [c async for c in stream]
        assert len(chunks) > 1
        assert len([e for e in events if e.operation == "get_metadata"]) == 1
        assert len([e for e in events if e.operation == "get_stream"]) == len(chunks)
        assert all(e.success for e in events)
    finally:
        await zb.close()


async def test_connect_failure_is_reported_once_and_not_retried():
    events = []
    backend = AsyncPostgresBackend(
        "postgresql://nobody:nothing@127.0.0.1:1/none",
        pool_timeout=1,
        on_operation=events.append,
    )
    try:
        with pytest.raises(StorageError):
            await backend.exists(NIL)
        assert len(events) == 1
        assert events[0].success is False
        assert events[0].retry_count == 0
    finally:
        await backend._pool.close()  # noqa: SLF001
