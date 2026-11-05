"""0.30.0: dedup=True and object_storage= tiering on the async adapters.

Every scenario runs on AsyncSQLiteBackend, AsyncPostgresBackend and
AsyncMySQLBackend. SQLite needs no server; PostgreSQL and MySQL tests skip
when unreachable. S3 is moto's in-process mock.

Two kinds of cross-checks matter most:
- the async adapters share their tables with the SYNC adapters, so an image
  written (or tiered) by one must be readable by the other;
- the blocking boto3 calls must run in worker threads, so a slow upload can
  never stall the event loop.
"""

# ruff: noqa: F401, F811
from __future__ import annotations

import asyncio
import io
import os
import time

import boto3
import pytest
from PIL import Image as PILImage

from zerobucket import AsyncZeroBucket, ImageNotFoundError, ZeroBucket
from zerobucket.adapters.mysql_async import AsyncMySQLBackend
from zerobucket.adapters.postgres_async import AsyncPostgresBackend
from zerobucket.adapters.sqlite_async import AsyncSQLiteBackend
from zerobucket.exceptions import StorageError
from zerobucket.object_storage import ObjectStorage

from .test_cli_backends import target  # the sqlite/postgres/mysql fixture

NIL = "00000000-0000-0000-0000-000000000000"


def _jpeg(color) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (40, 30), color=color).save(buf, format="JPEG")
    return buf.getvalue()


def _noisy_jpeg(size=(200, 200)) -> bytes:
    img = PILImage.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def _async_backend(target, **kwargs):
    if target.kind == "sqlite":
        return AsyncSQLiteBackend(target.path, **kwargs)
    if target.kind == "postgres":
        return AsyncPostgresBackend(target.url, **kwargs)
    return AsyncMySQLBackend(target.url, **kwargs)


@pytest.fixture
async def make(target):
    """Factory for async backends on this database; all are closed afterwards."""
    created = []

    def factory(**kwargs):
        backend = _async_backend(target, retry_base_delay=0.01, **kwargs)
        created.append(backend)
        return backend

    yield factory
    for backend in created:
        await backend.close()


def _blob_count(target, dedup=True) -> int:
    """Distinct blobs / rows as the SYNC adapter sees them (an independent view)."""
    backend = target.backend(dedup)
    try:
        info = backend.get_info()
    finally:
        backend.close()
    if info is None:
        return 0
    return info.dedup.blobs if dedup else info.count


# ---- dedup -----------------------------------------------------------------------------


async def test_dedup_stores_identical_images_once(make, target):
    zb = AsyncZeroBucket(backend=make(dedup=True))
    red = _jpeg((200, 0, 0))
    a = await zb.put(red)
    b = await zb.put(red)
    c = await zb.put(_jpeg((0, 0, 200)))
    assert len({a, b, c}) == 3  # three distinct ids...
    assert _blob_count(target) == 2  # ...but two stored blobs

    for image_id, expected in ((a, red), (b, red)):
        assert (await zb.get(image_id)).data == expected
    assert await zb.exists(a) and await zb.exists(c)
    assert (await zb.metadata(c)).mime_type == "image/jpeg"


async def test_dedup_delete_keeps_a_shared_blob_until_the_last_reference(make, target):
    zb = AsyncZeroBucket(backend=make(dedup=True))
    red = _jpeg((200, 0, 0))
    a, b = await zb.put(red), await zb.put(red)

    assert await zb.delete(a) is True
    assert await zb.exists(a) is False
    assert (await zb.get(b)).data == red  # the shared blob survived
    assert _blob_count(target) == 1

    assert await zb.delete(b) is True
    assert _blob_count(target) == 0  # last reference gone: blob gone
    assert await zb.delete(b) is False  # already deleted


async def test_dedup_put_many_and_delete_many_count_references(make, target):
    zb = AsyncZeroBucket(backend=make(dedup=True))
    red, blue = _jpeg((200, 0, 0)), _jpeg((0, 0, 200))
    put_results = await zb.put_many([red, blue, red, red])
    assert all(r.success for r in put_results)
    ids = [r.image_id for r in put_results]
    assert len(set(ids)) == 4
    assert _blob_count(target) == 2

    got = await zb.get_many(ids)  # results line up with the ids passed in
    assert [r.image.data for r in got] == [red, blue, red, red]

    results = await zb.delete_many([ids[0], ids[2]])  # two of the three red refs
    assert [r.deleted for r in results] == [True, True]
    assert _blob_count(target) == 2  # one red reference still holds its blob
    assert (await zb.get(ids[3])).data == red

    await zb.delete_many([ids[3], ids[1]])
    assert _blob_count(target) == 0


async def test_dedup_streaming_in_small_chunks_reassembles_the_image(make):
    zb = AsyncZeroBucket(backend=make(dedup=True))
    data = _noisy_jpeg()
    image_id = await zb.put(data)
    stream = await zb.get_stream(image_id, chunk_size=1000)
    chunks = [c async for c in stream]
    assert len(chunks) > 1
    assert b"".join(chunks) == data


async def test_dedup_not_found_paths(make):
    backend = make(dedup=True)
    assert await backend.get(NIL) is None
    assert await backend.get_metadata(NIL) is None
    assert await backend.get_stream(NIL) is None
    assert await backend.exists(NIL) is False
    assert await backend.delete(NIL) is False
    assert await backend.delete_many([NIL]) == []


async def test_sync_and_async_dedup_share_the_same_tables(make, target):
    red = _jpeg((200, 0, 0))
    sync_zb = ZeroBucket(backend=target.backend(True))
    try:
        sync_id = sync_zb.put(red)
        async_zb = AsyncZeroBucket(backend=make(dedup=True))
        async_id = await async_zb.put(red)  # same bytes: must share ONE blob
        assert _blob_count(target) == 1
        assert (await async_zb.get(sync_id)).data == red
        assert sync_zb.get(async_id).data == red
        await async_zb.delete(sync_id)
        assert (
            sync_zb.get(async_id).data == red
        )  # the async delete kept the shared blob
    finally:
        sync_zb.close()


async def test_dedup_and_object_storage_cannot_be_combined(target, s3_bucket):
    store = ObjectStorage(s3_bucket, region_name="us-east-1")
    with pytest.raises(ValueError, match="object_storage"):
        _async_backend(target, dedup=True, object_storage=store)


async def test_tiering_is_refused_in_dedup_mode(make):
    backend = make(dedup=True)
    with pytest.raises(StorageError, match="dedup"):
        await backend.tier_to_object_storage(NIL)


# ---- tiering ------------------------------------------------------------------------------------


@pytest.fixture
def s3(s3_bucket):
    return boto3.client("s3", region_name="us-east-1"), s3_bucket


def _store(bucket):
    return ObjectStorage(bucket, region_name="us-east-1")


async def test_tier_moves_bytes_to_s3_and_reads_come_back_identical(make, s3):
    client, bucket = s3
    zb = AsyncZeroBucket(backend=make(object_storage=_store(bucket)))
    data = _noisy_jpeg()
    image_id = await zb.put(data)

    assert await zb.tier_to_object_storage(image_id) is True
    assert client.list_objects_v2(Bucket=bucket)["KeyCount"] == 1
    assert await zb.tier_to_object_storage(image_id) is False  # already tiered: a no-op
    with pytest.raises(ImageNotFoundError):
        await zb.tier_to_object_storage(NIL)

    assert (await zb.get(image_id)).data == data  # served from the bucket
    metadata = await zb.metadata(image_id)
    assert metadata.size_bytes == len(data)  # metadata never needed the bytes
    stream = await zb.get_stream(image_id, chunk_size=1000)
    chunks = [c async for c in stream]
    assert len(chunks) > 1 and b"".join(chunks) == data  # real ranged reads


async def test_get_many_mixes_tiered_and_untiered_images(make, s3):
    _, bucket = s3
    zb = AsyncZeroBucket(backend=make(object_storage=_store(bucket)))
    datas = [_jpeg((1, 1, 1)), _jpeg((50, 60, 70)), _jpeg((90, 10, 10))]
    ids = [await zb.put(d) for d in datas]
    await zb.tier_to_object_storage(ids[1])
    got = await zb.get_many(ids)
    assert [r.image.data for r in got] == datas


async def test_delete_removes_the_tiered_object_and_reports_it(make, s3):
    client, bucket = s3
    events = []
    zb = AsyncZeroBucket(
        backend=make(object_storage=_store(bucket), on_operation=events.append)
    )
    ids = [await zb.put(_jpeg((i, i, i))) for i in (1, 2, 3)]
    for image_id in ids:
        await zb.tier_to_object_storage(image_id)
    events.clear()

    assert await zb.delete(ids[0]) is True
    assert client.list_objects_v2(Bucket=bucket)["KeyCount"] == 2
    reported = [e for e in events if e.operation == "object_storage_delete"]
    assert len(reported) == 1 and reported[0].success is True

    results = await zb.delete_many(ids[1:])
    assert [r.deleted for r in results] == [True, True]
    assert client.list_objects_v2(Bucket=bucket).get("KeyCount", 0) == 0


async def test_a_failed_object_cleanup_never_fails_an_async_delete(make, s3):
    client, bucket = s3
    events = []
    backend = make(object_storage=_store(bucket), on_operation=events.append)
    zb = AsyncZeroBucket(backend=backend)
    image_id = await zb.put(_jpeg((5, 5, 5)))
    await zb.tier_to_object_storage(image_id)
    events.clear()

    def boom(key):
        raise StorageError("boom: S3 is unavailable")

    backend._object_storage.delete = boom
    assert await zb.delete(image_id) is True  # no exception
    assert await zb.exists(image_id) is False
    assert client.list_objects_v2(Bucket=bucket)["KeyCount"] == 1  # orphan left behind
    failed = [e for e in events if e.operation == "object_storage_delete"]
    assert len(failed) == 1 and failed[0].success is False and "boom" in failed[0].error


async def test_a_failed_upload_rolls_back_and_leaves_the_image_in_the_database(
    make, s3, target
):
    _, bucket = s3
    backend = make(object_storage=_store(bucket))
    zb = AsyncZeroBucket(backend=backend)
    data = _jpeg((9, 9, 9))
    image_id = await zb.put(data)

    def boom(*args, **kwargs):
        raise StorageError("boom: upload failed")

    backend._object_storage.upload = boom
    with pytest.raises(StorageError, match="boom"):
        await zb.tier_to_object_storage(image_id)

    assert (await zb.get(image_id)).data == data  # still readable, from the database
    sync_backend = target.backend()
    try:
        assert sync_backend.get_info().tiered.count == 0
    finally:
        sync_backend.close()


async def test_reading_a_tiered_image_without_object_storage_explains_itself(make, s3):
    _, bucket = s3
    tiering = AsyncZeroBucket(backend=make(object_storage=_store(bucket)))
    image_id = await tiering.put(_noisy_jpeg())
    await tiering.tier_to_object_storage(image_id)

    plain = make()  # no object_storage=
    for call in (
        lambda: plain.get(image_id),
        lambda: plain.get_many([image_id]),
        lambda: plain.get_stream(image_id),
    ):
        with pytest.raises(StorageError, match="object_storage"):
            await call()


async def test_tiering_requires_object_storage(make):
    with pytest.raises(StorageError, match="object_storage"):
        await make().tier_to_object_storage(NIL)


async def test_sync_and_async_tiering_interoperate(make, s3, target):
    _, bucket = s3
    store = _store(bucket)
    sync_zb = ZeroBucket(backend=target.backend(False, object_storage=store))
    try:
        async_zb = AsyncZeroBucket(backend=make(object_storage=store))
        a_data, b_data = _jpeg((11, 11, 11)), _jpeg((22, 22, 22))
        sync_id = sync_zb.put(a_data)
        async_id = await async_zb.put(b_data)

        assert sync_zb.tier_to_object_storage(sync_id) is True  # tiered by SYNC...
        assert (await async_zb.get(sync_id)).data == a_data  # ...read by ASYNC
        assert (
            await async_zb.tier_to_object_storage(async_id) is True
        )  # tiered by ASYNC...
        assert sync_zb.get(async_id).data == b_data  # ...read by SYNC
        assert await async_zb.tier_to_object_storage(sync_id) is False  # already tiered
    finally:
        sync_zb.close()


async def test_a_slow_upload_does_not_block_the_event_loop(make, s3):
    """The boto3 upload is blocking. If it ran on the event loop, a ticker
    task could not run at all during it; in a worker thread it keeps ticking."""
    _, bucket = s3
    store = _store(bucket)
    backend = make(object_storage=store)
    zb = AsyncZeroBucket(backend=backend)
    image_id = await zb.put(_jpeg((7, 7, 7)))

    real_upload = store.upload

    def slow_upload(*args, **kwargs):
        time.sleep(0.6)  # a blocking call, like a slow network
        return real_upload(*args, **kwargs)

    store.upload = slow_upload
    ticks = 0

    async def ticker():
        nonlocal ticks
        while True:
            await asyncio.sleep(0.05)
            ticks += 1

    task = asyncio.create_task(ticker())
    try:
        assert await zb.tier_to_object_storage(image_id) is True
    finally:
        task.cancel()
    assert (
        ticks >= 5
    ), f"the event loop was blocked during the upload (only {ticks} ticks)"


# ---- the client ------------------------------------------------------------------------------


async def test_async_client_database_url_accepts_dedup_and_object_storage(
    _db_available, s3
):
    from .conftest import TEST_DATABASE_URL
    from .test_cli_backends import Target, _drop_everything

    pg = Target("postgres")
    _drop_everything(pg)
    _, bucket = s3
    dedup_zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL, dedup=True)
    try:
        red = _jpeg((200, 0, 0))
        a, b = await dedup_zb.put(red), await dedup_zb.put(red)
        assert (await dedup_zb.get(b)).data == red and a != b
    finally:
        await dedup_zb.close()
    _drop_everything(pg)

    tier_zb = AsyncZeroBucket(
        database_url=TEST_DATABASE_URL, object_storage=_store(bucket)
    )
    try:
        image_id = await tier_zb.put(_jpeg((1, 2, 3)))
        assert await tier_zb.tier_to_object_storage(image_id) is True
        assert (await tier_zb.get(image_id)).data == _jpeg((1, 2, 3))
    finally:
        await tier_zb.close()
        _drop_everything(pg)
