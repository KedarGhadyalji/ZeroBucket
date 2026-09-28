"""Tests for AsyncMySQLBackend -- Phase 4, closing the MySQL async gap
(see the v0.23.0 CHANGELOG entry). Scope matches AsyncPostgresBackend/
AsyncSQLiteBackend exactly: core CRUD + streaming, classic mode only --
no dedup, no tiering as a caller-facing operation. See
adapters/mysql_async.py's module docstring for why, the driver choice
(aiomysql, not asyncmy), and the inherited tiering-related limitation
shared with AsyncPostgresBackend.

Runs against a REAL MySQL/MariaDB instance -- set
ZEROBUCKET_TEST_MYSQL_URL, same convention as test_mysql_adapter.py.
All tests in this file are skipped cleanly (not failed) if no such
database is reachable.
"""

from __future__ import annotations

import asyncio
import io
import os

import pytest
from PIL import Image as PILImage

from zerobucket import AsyncZeroBucket, ImageNotFoundError
from zerobucket.adapters.mysql import MySQLBackend
from zerobucket.adapters.mysql_async import AsyncMySQLBackend
from zerobucket.exceptions import StorageError

TEST_MYSQL_URL = os.environ.get(
    "ZEROBUCKET_TEST_MYSQL_URL",
    "mysql://zerobucket:zerobucket@127.0.0.1:3306/zerobucket_test",
)


def _jpeg_bytes(size=(300, 200), color=(40, 80, 120)) -> bytes:
    img = PILImage.new("RGB", size, color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


@pytest.fixture(scope="session")
def _mysql_available():
    try:
        backend = MySQLBackend(TEST_MYSQL_URL)
        backend.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"No reachable test MySQL/MariaDB database ({TEST_MYSQL_URL}): {exc}"
        )


@pytest.fixture(autouse=True)
def _clean_table(_mysql_available):
    """Truncates via the SYNC backend (plain, synchronous setup) before
    each test -- simpler than an async fixture for what's just table
    cleanup, same approach test_mysql_adapter.py's own fixtures use."""
    backend = MySQLBackend(TEST_MYSQL_URL)
    conn = backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE zerobucket_images;")
        conn.commit()
    finally:
        conn.close()
    backend.close()
    yield


@pytest.fixture
async def async_mysql_images():
    zb = AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL))
    yield zb
    await zb.close()


# ---- put / get round trip ---------------------------------------------


async def test_put_get_round_trip(async_mysql_images):
    data = _jpeg_bytes()
    image_id = await async_mysql_images.put(data)
    image = await async_mysql_images.get(image_id)
    assert image.data == data
    assert image.width == 300
    assert image.height == 200


async def test_get_not_found_raises(async_mysql_images):
    with pytest.raises(ImageNotFoundError):
        await async_mysql_images.get("00000000-0000-0000-0000-000000000000")


async def test_ids_are_real_uuids_and_unique(async_mysql_images):
    import uuid

    id_a = await async_mysql_images.put(_jpeg_bytes())
    id_b = await async_mysql_images.put(_jpeg_bytes())
    uuid.UUID(id_a)
    uuid.UUID(id_b)
    assert id_a != id_b


async def test_metadata_without_bytes(async_mysql_images):
    data = _jpeg_bytes()
    image_id = await async_mysql_images.put(data)
    meta = await async_mysql_images.metadata(image_id)
    assert meta.size_bytes == len(data)


async def test_exists_true_then_false(async_mysql_images):
    image_id = await async_mysql_images.put(_jpeg_bytes())
    assert await async_mysql_images.exists(image_id) is True
    await async_mysql_images.delete(image_id)
    assert await async_mysql_images.exists(image_id) is False


async def test_delete_true_then_false(async_mysql_images):
    image_id = await async_mysql_images.put(_jpeg_bytes())
    assert await async_mysql_images.delete(image_id) is True
    assert await async_mysql_images.delete(image_id) is False


# ---- batch operations ----------------------------------------------------


async def test_put_many_all_succeed(async_mysql_images):
    data = [_jpeg_bytes(), _jpeg_bytes((10, 10)), _jpeg_bytes((500, 500))]
    results = await async_mysql_images.put_many(data)
    assert all(r.success for r in results)
    assert len({r.image_id for r in results}) == 3


async def test_put_many_partial_failure_is_best_effort(async_mysql_images):
    good = _jpeg_bytes()
    results = await async_mysql_images.put_many([good, b"not an image", good])
    assert results[0].success
    assert not results[1].success
    assert results[2].success


async def test_get_many_mixed_found_and_missing(async_mysql_images):
    data = _jpeg_bytes()
    image_id = await async_mysql_images.put(data)
    results = await async_mysql_images.get_many(
        [image_id, "00000000-0000-0000-0000-000000000000"]
    )
    by_id = {r.image_id: r for r in results}
    assert by_id[image_id].success
    assert not by_id["00000000-0000-0000-0000-000000000000"].success


async def test_get_many_empty_list(async_mysql_images):
    assert await async_mysql_images.get_many([]) == []


async def test_delete_many_mixed(async_mysql_images):
    id_a = await async_mysql_images.put(_jpeg_bytes())
    id_b = await async_mysql_images.put(_jpeg_bytes())
    results = await async_mysql_images.delete_many(
        [id_a, "00000000-0000-0000-0000-000000000000"]
    )
    by_id = {r.image_id: r for r in results}
    assert by_id[id_a].deleted is True
    assert by_id["00000000-0000-0000-0000-000000000000"].deleted is False
    assert await async_mysql_images.exists(id_b) is True


async def test_delete_many_large_batch_builds_correct_in_clause(async_mysql_images):
    """Same dynamically-sized IN (%s, %s, ...) concern as the sync
    adapter's equivalent test -- exercised here via the SELECT ... FOR
    UPDATE path delete_many() uses in the absence of RETURNING."""
    ids = [await async_mysql_images.put(_jpeg_bytes()) for _ in range(25)]
    results = await async_mysql_images.delete_many(ids)
    assert all(r.deleted for r in results)
    for image_id in ids:
        assert await async_mysql_images.exists(image_id) is False


# ---- get_stream ---------------------------------------------------------


async def test_get_stream_reconstructs_exact_bytes(async_mysql_images):
    data = _jpeg_bytes((800, 600))
    image_id = await async_mysql_images.put(data)
    stream = await async_mysql_images.get_stream(image_id, chunk_size=500)
    chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == data
    assert len(chunks) > 1


async def test_get_stream_not_found_raises_on_await(async_mysql_images):
    with pytest.raises(ImageNotFoundError):
        await async_mysql_images.get_stream("00000000-0000-0000-0000-000000000000")


async def test_stream_to_writes_full_content(async_mysql_images):
    data = _jpeg_bytes()
    image_id = await async_mysql_images.put(data)
    dest = io.BytesIO()
    total = await async_mysql_images.stream_to(image_id, dest, chunk_size=333)
    assert total == len(data)
    assert dest.getvalue() == data


async def test_get_stream_raises_on_concurrent_delete_mid_stream(_mysql_available):
    """Each chunk is its own round trip with no held snapshot -- a row
    deleted between chunks by a different connection is observed and
    raises, matching AsyncPostgresBackend's behavior (and sync MySQL's
    own non-tiered get_stream(), see that method's docstring)."""
    zb = AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL))
    data = _jpeg_bytes((800, 600))
    image_id = await zb.put(data)

    stream = await zb.get_stream(image_id, chunk_size=50)
    first_chunk = await stream.__anext__()
    assert first_chunk

    sync_backend = MySQLBackend(TEST_MYSQL_URL)
    sync_backend.delete(image_id)
    sync_backend.close()

    with pytest.raises(StorageError, match="deleted while streaming"):
        async for _ in stream:
            pass

    await zb.close()


# ---- lazy connect / migrate-once under concurrency ------------------------


async def test_concurrent_first_calls_only_migrate_once(_mysql_available):
    zb = AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL))
    try:
        results = await asyncio.gather(
            *(zb.exists("00000000-0000-0000-0000-000000000000") for _ in range(20))
        )
        assert results == [False] * 20
    finally:
        await zb.close()


# ---- schema compatibility with the sync adapter ----------------------------


async def test_migrated_table_is_schema_compatible_with_sync_backend(_mysql_available):
    """AsyncMySQLBackend's own migration (_ensure_ready) applies BOTH
    the base schema AND the tiering migration (same as sync
    MySQLBackend's __init__ does), so a table this adapter creates is
    immediately usable by the sync backend too -- including its
    tiering columns, even though this async adapter's own callers
    can't tier anything themselves. Verified directly: after an
    async-only migration, the sync backend can read/write against the
    same table without needing its own separate migration pass."""
    zb = AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL))
    await zb.exists("00000000-0000-0000-0000-000000000000")  # forces migration
    await zb.close()

    sync_backend = MySQLBackend(TEST_MYSQL_URL, auto_migrate=False)
    try:
        image_id = sync_backend.put(
            data=_jpeg_bytes(),
            mime_type="image/jpeg",
            original_filename=None,
            size_bytes=10,
            width=1,
            height=1,
            checksum_sha256="0" * 64,
        )
        assert sync_backend.exists(image_id) is True
    finally:
        sync_backend.close()


# ---- inherited tiering limitation (documented, not silently different) ----


async def test_get_stream_on_tiered_row_raises_clear_error(_mysql_available):
    """A tiered row (set up directly via SQL rather than needing a real
    S3/moto dependency in this test file) must raise a CLEAR,
    specific StorageError from get_stream() -- not an obscure
    bytes(None)-shaped crash. get()/get_many() share the same
    underlying inherited limitation (see module docstring) but are not
    given the same clear-error treatment, consistent with
    AsyncPostgresBackend's own behavior -- only get_stream() checks
    explicitly here because its metadata lookup already reads
    storage_backend for a different reason (the not-found check)."""
    sync_backend = MySQLBackend(TEST_MYSQL_URL)
    image_id = sync_backend.put(
        data=_jpeg_bytes(),
        mime_type="image/jpeg",
        original_filename=None,
        size_bytes=10,
        width=1,
        height=1,
        checksum_sha256="0" * 64,
    )
    conn = sync_backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE zerobucket_images SET data = NULL, "
                "storage_backend = 'object_storage', "
                "object_storage_bucket = 'fake-bucket', "
                "object_storage_key = %s WHERE id = %s;",
                (image_id, image_id),
            )
        conn.commit()
    finally:
        conn.close()
    sync_backend.close()

    zb = AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL))
    try:
        with pytest.raises(StorageError, match="object storage"):
            await zb.get_stream(image_id)
    finally:
        await zb.close()


# ---- context manager -------------------------------------------------------


async def test_async_context_manager(_mysql_available):
    async with AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL)) as zb:
        image_id = await zb.put(_jpeg_bytes())
        assert await zb.exists(image_id) is True


# ---- connection pooling (Phase 5) ---------------------------------------


def _server_connection_count() -> int:
    backend = MySQLBackend(TEST_MYSQL_URL)
    conn = backend._connect()  # noqa: SLF001
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT VARIABLE_VALUE FROM information_schema.GLOBAL_STATUS "
                "WHERE VARIABLE_NAME = 'CONNECTIONS';"
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()
        backend.close()


async def test_pool_reuses_connections_instead_of_opening_one_per_operation(
    _mysql_available,
):
    zb = AsyncZeroBucket(backend=AsyncMySQLBackend(TEST_MYSQL_URL, pool_max_size=2))
    try:
        await zb.exists("00000000-0000-0000-0000-000000000000")  # pool + migration
        before = _server_connection_count()
        for _ in range(20):
            image_id = await zb.put(_jpeg_bytes())
            await zb.get(image_id)
        opened = _server_connection_count() - before
        # +1 for the counter query's own connection.
        assert opened <= 2 + 1, f"opened {opened} connections for 40 operations"
    finally:
        await zb.close()


async def test_pool_exhaustion_times_out_with_clear_error(_mysql_available):
    import time

    backend = AsyncMySQLBackend(
        TEST_MYSQL_URL, pool_min_size=1, pool_max_size=1, pool_timeout=0.5
    )
    zb = AsyncZeroBucket(backend=backend)
    try:
        await zb.exists("00000000-0000-0000-0000-000000000000")  # creates the pool
        held = await backend._acquire()  # noqa: SLF001
        try:
            start = time.monotonic()
            with pytest.raises(StorageError, match="Timed out"):
                await zb.exists("00000000-0000-0000-0000-000000000000")
            assert 0.4 <= time.monotonic() - start < 3.0
        finally:
            backend._pool.release(held)  # noqa: SLF001
    finally:
        await zb.close()


async def test_pool_handles_more_tasks_than_pool_slots(_mysql_available):
    zb = AsyncZeroBucket(
        backend=AsyncMySQLBackend(TEST_MYSQL_URL, pool_max_size=3, pool_timeout=20)
    )
    try:
        ids = await asyncio.gather(*(zb.put(_jpeg_bytes()) for _ in range(30)))
        assert len(set(ids)) == 30
    finally:
        await zb.close()


async def test_failed_operation_does_not_poison_the_pool(_mysql_available):
    """Single-slot pool: a failed put() (oversized checksum for
    CHAR(64)) must roll back and release its connection, so the very
    next call still works instead of deadlocking on a lost slot."""
    backend = AsyncMySQLBackend(
        TEST_MYSQL_URL, pool_min_size=1, pool_max_size=1, pool_timeout=3
    )
    try:
        with pytest.raises(StorageError):
            await backend.put(
                data=_jpeg_bytes(),
                mime_type="image/jpeg",
                original_filename=None,
                size_bytes=10,
                width=1,
                height=1,
                checksum_sha256="x" * 200,
            )
        assert await backend.exists("00000000-0000-0000-0000-000000000000") is False
    finally:
        await backend.close()


async def test_unreachable_server_raises_storage_error(_mysql_available):
    backend = AsyncMySQLBackend(
        "mysql://nobody:nothing@127.0.0.1:1/none", pool_timeout=1
    )
    with pytest.raises(StorageError, match="Could not connect"):
        await backend.exists("00000000-0000-0000-0000-000000000000")


async def test_close_is_safe_before_first_use_and_twice(_mysql_available):
    backend = AsyncMySQLBackend(TEST_MYSQL_URL)
    await backend.close()  # never used: nothing to close
    await backend.exists("00000000-0000-0000-0000-000000000000")
    await backend.close()
    await backend.close()  # idempotent
