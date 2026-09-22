"""Tests for AsyncSQLiteBackend -- closes the last SQLite gap (async
support). Scope matches AsyncPostgresBackend exactly: core CRUD +
streaming, classic mode only -- no dedup, no tiering. See
adapters/sqlite_async.py's module docstring for why, and for the two
real, verified driver-shaped design differences (no blobopen() in
aiosqlite, meaning async get_stream() behaves differently from SYNC
SQLite's get_stream() for concurrent-delete-mid-stream, not just
differently from Postgres).

Runs against a real SQLite file on disk via tempfile, same as the sync
adapter's own test suite.
"""

from __future__ import annotations

import asyncio
import io
import os
import tempfile

import pytest
from PIL import Image as PILImage

from zerobucket import AsyncZeroBucket, ImageNotFoundError
from zerobucket.adapters.sqlite_async import AsyncSQLiteBackend
from zerobucket.exceptions import StorageError


def _jpeg_bytes(size=(300, 200), color=(40, 80, 120)) -> bytes:
    img = PILImage.new("RGB", size, color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


@pytest.fixture
def sqlite_path():
    path = tempfile.mktemp(suffix=".db")
    yield path
    for ext in ("", "-wal", "-shm"):
        if os.path.exists(path + ext):
            os.remove(path + ext)


@pytest.fixture
async def async_sqlite_images(sqlite_path):
    zb = AsyncZeroBucket(backend=AsyncSQLiteBackend(sqlite_path))
    yield zb
    await zb.close()


# ---- put / get round trip ---------------------------------------------


async def test_put_get_round_trip(async_sqlite_images):
    data = _jpeg_bytes()
    image_id = await async_sqlite_images.put(data)
    image = await async_sqlite_images.get(image_id)
    assert image.data == data
    assert image.width == 300
    assert image.height == 200


async def test_get_not_found_raises(async_sqlite_images):
    with pytest.raises(ImageNotFoundError):
        await async_sqlite_images.get("00000000-0000-0000-0000-000000000000")


async def test_ids_are_real_uuids_and_unique(async_sqlite_images):
    import uuid

    id_a = await async_sqlite_images.put(_jpeg_bytes())
    id_b = await async_sqlite_images.put(_jpeg_bytes())
    uuid.UUID(id_a)
    uuid.UUID(id_b)
    assert id_a != id_b


async def test_metadata_without_bytes(async_sqlite_images):
    data = _jpeg_bytes()
    image_id = await async_sqlite_images.put(data)
    meta = await async_sqlite_images.metadata(image_id)
    assert meta.size_bytes == len(data)


async def test_exists_true_then_false(async_sqlite_images):
    image_id = await async_sqlite_images.put(_jpeg_bytes())
    assert await async_sqlite_images.exists(image_id) is True
    await async_sqlite_images.delete(image_id)
    assert await async_sqlite_images.exists(image_id) is False


async def test_delete_true_then_false(async_sqlite_images):
    image_id = await async_sqlite_images.put(_jpeg_bytes())
    assert await async_sqlite_images.delete(image_id) is True
    assert await async_sqlite_images.delete(image_id) is False


# ---- batch operations ----------------------------------------------------


async def test_put_many_all_succeed(async_sqlite_images):
    data = [_jpeg_bytes(), _jpeg_bytes((10, 10)), _jpeg_bytes((500, 500))]
    results = await async_sqlite_images.put_many(data)
    assert all(r.success for r in results)
    assert len({r.image_id for r in results}) == 3


async def test_put_many_partial_failure_is_best_effort(async_sqlite_images):
    good = _jpeg_bytes()
    results = await async_sqlite_images.put_many([good, b"not an image", good])
    assert results[0].success
    assert not results[1].success
    assert results[2].success


async def test_get_many_mixed_found_and_missing(async_sqlite_images):
    data = _jpeg_bytes()
    image_id = await async_sqlite_images.put(data)
    results = await async_sqlite_images.get_many(
        [image_id, "00000000-0000-0000-0000-000000000000"]
    )
    by_id = {r.image_id: r for r in results}
    assert by_id[image_id].success
    assert not by_id["00000000-0000-0000-0000-000000000000"].success


async def test_delete_many_mixed(async_sqlite_images):
    id_a = await async_sqlite_images.put(_jpeg_bytes())
    id_b = await async_sqlite_images.put(_jpeg_bytes())
    results = await async_sqlite_images.delete_many(
        [id_a, "00000000-0000-0000-0000-000000000000"]
    )
    by_id = {r.image_id: r for r in results}
    assert by_id[id_a].deleted is True
    assert by_id["00000000-0000-0000-0000-000000000000"].deleted is False
    assert await async_sqlite_images.exists(id_b) is True


# ---- get_stream / stream_to ------------------------------------------


async def test_get_stream_reconstructs_exact_bytes(async_sqlite_images):
    data = _jpeg_bytes((800, 600))
    image_id = await async_sqlite_images.put(data)
    stream = await async_sqlite_images.get_stream(image_id, chunk_size=500)
    chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == data
    assert len(chunks) > 1


async def test_get_stream_not_found_raises_on_await(async_sqlite_images):
    with pytest.raises(ImageNotFoundError):
        await async_sqlite_images.get_stream("00000000-0000-0000-0000-000000000000")


async def test_stream_to_writes_full_content(async_sqlite_images):
    data = _jpeg_bytes()
    image_id = await async_sqlite_images.put(data)
    dest = io.BytesIO()
    total = await async_sqlite_images.stream_to(image_id, dest, chunk_size=333)
    assert total == len(data)
    assert dest.getvalue() == data


async def test_get_stream_raises_on_concurrent_delete_mid_stream(sqlite_path):
    """VERIFIED, DIFFERENT-FROM-SYNC-SQLITE behavior, not a bug: unlike
    sync SQLiteBackend.get_stream() (which survives a concurrent delete
    thanks to blobopen()'s WAL-mode snapshot isolation -- see that
    method's docstring), this async version issues a separate substr()
    query per chunk (aiosqlite has no blobopen() to hold a snapshot
    open with), so a concurrent delete IS observed and DOES raise --
    matching AsyncPostgresBackend's behavior instead. Two different
    SQLite backends in this same project, two different verified
    behaviors for the identical scenario -- confirmed directly, not
    assumed to be consistent just because they're both "SQLite"."""
    import sqlite3

    zb = AsyncZeroBucket(backend=AsyncSQLiteBackend(sqlite_path))
    data = _jpeg_bytes((800, 600))
    image_id = await zb.put(data)

    stream = await zb.get_stream(image_id, chunk_size=50)
    first_chunk = await stream.__anext__()
    assert first_chunk

    conn = sqlite3.connect(sqlite_path)
    conn.execute("DELETE FROM zerobucket_images WHERE id = ?;", (image_id,))
    conn.commit()
    conn.close()

    with pytest.raises(StorageError, match="deleted while streaming"):
        async for _ in stream:
            pass

    await zb.close()


# ---- lazy pool-open / migrate-once under concurrency ----------------------


async def test_concurrent_first_calls_only_migrate_once(sqlite_path):
    zb = AsyncZeroBucket(backend=AsyncSQLiteBackend(sqlite_path))
    try:
        results = await asyncio.gather(
            *(zb.exists("00000000-0000-0000-0000-000000000000") for _ in range(20))
        )
        assert results == [False] * 20
    finally:
        await zb.close()


# ---- context manager -------------------------------------------------------


async def test_async_context_manager(sqlite_path):
    async with AsyncZeroBucket(backend=AsyncSQLiteBackend(sqlite_path)) as zb:
        image_id = await zb.put(_jpeg_bytes())
        assert await zb.exists(image_id) is True
