"""Tests for ZeroBucketStorage's async API (asave, aopen, aexists, adelete, asize, aclose) and
the per-event-loop async client cache behind it.

Needs the same real PostgreSQL as the rest of the suite. Async tests run through pytest-asyncio
(asyncio_mode = "auto", see pyproject.toml).
"""

from __future__ import annotations

import asyncio
import io
import warnings

import pytest
from django.core.exceptions import SuspiciousFileOperation
from django.core.files.base import ContentFile
from django.core.files.uploadedfile import SimpleUploadedFile
from PIL import Image as PILImage
from zerobucket import (
    AccessDeniedError,
    ImageNotFoundError,
    ImageTooLargeError,
    ImageValidationError,
)

from django_zerobucket import storage as storage_module
from django_zerobucket.storage import ZeroBucketStorage, aclose_async_clients
from tests.django_settings import ZEROBUCKET_DATABASE_URL

MISSING = "00000000-0000-0000-0000-000000000000"


def _jpeg(color=(30, 60, 90)) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (60, 40), color=color).save(buf, format="JPEG")
    return buf.getvalue()


# ---- save / open round trip ---------------------------------------------


async def test_asave_and_aopen_round_trip(astorage, jpeg_bytes):
    name = await astorage.asave("photo.jpg", ContentFile(jpeg_bytes))
    f = await astorage.aopen(name)
    assert f.read() == jpeg_bytes


async def test_asave_returns_an_id_and_keeps_the_filename_in_metadata(
    astorage, zb_client, jpeg_bytes
):
    name = await astorage.asave("my_original_filename.jpg", ContentFile(jpeg_bytes))
    assert name != "my_original_filename.jpg"
    assert zb_client.metadata(name).filename == "my_original_filename.jpg"


async def test_asave_with_no_name_uses_the_uploaded_files_own_name(
    astorage, zb_client, jpeg_bytes
):
    name = await astorage.asave(None, SimpleUploadedFile("uploaded.jpg", jpeg_bytes))
    assert zb_client.metadata(name).filename == "uploaded.jpg"


async def test_asave_wraps_a_plain_file_like_object(astorage, jpeg_bytes):
    name = await astorage.asave("raw.jpg", io.BytesIO(jpeg_bytes))
    assert (await astorage.aopen(name)).read() == jpeg_bytes


async def test_asave_rejects_a_name_that_climbs_out_of_the_directory(
    astorage, jpeg_bytes
):
    with pytest.raises(SuspiciousFileOperation):
        await astorage.asave("../evil.jpg", ContentFile(jpeg_bytes))


async def test_asave_rejects_something_that_is_not_an_image(astorage):
    with pytest.raises(ImageValidationError):
        await astorage.asave("fake.jpg", ContentFile(b"this is not an image"))


async def test_options_reach_the_async_client(zb_client, jpeg_bytes):
    storage = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL, max_bytes=100)
    try:
        with pytest.raises(ImageTooLargeError):
            await storage.asave("big.jpg", ContentFile(jpeg_bytes))
    finally:
        await storage.aclose()


# ---- exists / delete / size ---------------------------------------------


async def test_aexists_adelete_and_asize(astorage, jpeg_bytes):
    name = await astorage.asave("x.jpg", ContentFile(jpeg_bytes))
    assert await astorage.aexists(name) is True
    assert await astorage.asize(name) == len(jpeg_bytes)
    await astorage.adelete(name)
    assert await astorage.aexists(name) is False


async def test_adelete_of_a_missing_image_is_silent_like_delete(astorage):
    await astorage.adelete(MISSING)


async def test_aopen_and_asize_of_a_missing_image_raise_not_found(astorage):
    with pytest.raises(ImageNotFoundError):
        await astorage.aopen(MISSING)
    with pytest.raises(ImageNotFoundError):
        await astorage.asize(MISSING)


async def test_aexists_is_false_for_something_that_is_not_an_id(astorage):
    assert await astorage.aexists("not-an-id") is False


# ---- async and sync see the same data ------------------------------------


async def test_what_asave_stores_the_sync_api_reads_and_the_other_way_round(
    astorage, jpeg_bytes
):
    async_name = await astorage.asave("a.jpg", ContentFile(jpeg_bytes))
    assert astorage.exists(async_name)
    assert astorage.open(async_name).read() == jpeg_bytes

    sync_name = astorage.save("b.jpg", ContentFile(jpeg_bytes))
    assert await astorage.aexists(sync_name)
    assert (await astorage.aopen(sync_name)).read() == jpeg_bytes


async def test_many_concurrent_asaves_all_land(astorage):
    images = [_jpeg((i * 10, 20, 200 - i * 5)) for i in range(20)]
    names = await asyncio.gather(
        *(astorage.asave(f"{i}.jpg", ContentFile(b)) for i, b in enumerate(images))
    )
    assert len(set(names)) == 20
    for name, data in zip(names, images, strict=True):
        assert (await astorage.aopen(name)).read() == data


# ---- the per-event-loop client cache --------------------------------------


async def test_storages_with_the_same_settings_share_one_client_on_a_loop(astorage):
    other = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)
    assert other._aclient() is astorage._aclient()  # noqa: SLF001


async def test_different_options_get_a_different_client(astorage):
    other = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL, max_bytes=5_000_000)
    assert other._aclient() is not astorage._aclient()  # noqa: SLF001


async def test_aclose_closes_the_pool_and_a_later_call_builds_a_fresh_client(
    astorage, jpeg_bytes
):
    await astorage.asave("x.jpg", ContentFile(jpeg_bytes))
    first = astorage._aclient()  # noqa: SLF001
    await astorage.aclose()
    assert first._backend._pool.closed  # noqa: SLF001
    assert not storage_module._async_client_cache
    name = await astorage.asave("y.jpg", ContentFile(jpeg_bytes))
    assert astorage._aclient() is not first  # noqa: SLF001
    assert await astorage.aexists(name)


async def test_aclose_with_nothing_open_is_a_no_op(astorage):
    await astorage.aclose()
    await astorage.aclose()
    await aclose_async_clients()


async def test_aclose_async_clients_closes_every_client_of_this_loop(
    zb_client, jpeg_bytes
):
    a = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)
    b = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL, max_bytes=5_000_000)
    await a.asave("a.jpg", ContentFile(jpeg_bytes))
    await b.asave("b.jpg", ContentFile(jpeg_bytes))
    clients = [a._aclient(), b._aclient()]  # noqa: SLF001
    await aclose_async_clients()
    assert all(c._backend._pool.closed for c in clients)  # noqa: SLF001
    assert not storage_module._async_client_cache


def test_the_async_client_needs_a_running_event_loop(zb_client):
    storage = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)
    with pytest.raises(RuntimeError, match="no running event loop"):
        storage._aclient()  # noqa: SLF001


def test_each_event_loop_gets_its_own_client(zb_client):
    storage = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)
    seen = []

    async def grab():
        seen.append(storage._aclient())  # noqa: SLF001
        await storage.aclose()

    asyncio.run(grab())
    asyncio.run(grab())
    assert seen[0] is not seen[1]


def test_a_new_loop_after_an_unclosed_one_warns_once_that_this_looks_like_wsgi(
    zb_client,
):
    storage = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)

    async def use_without_closing():
        storage._aclient()  # noqa: SLF001 (creating the client opens no connection)

    asyncio.run(use_without_closing())
    with pytest.warns(RuntimeWarning, match="WSGI"):
        asyncio.run(use_without_closing())
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        asyncio.run(
            use_without_closing()
        )  # the second warning is suppressed: once per process


def test_closing_before_each_loop_ends_never_warns(zb_client):
    storage = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)

    async def use_and_close():
        storage._aclient()  # noqa: SLF001
        await storage.aclose()

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        for _ in range(3):
            asyncio.run(use_and_close())


# ---- access-control hooks ------------------------------------------------


async def test_an_async_before_get_hook_is_enforced_on_both_the_async_and_the_sync_path(
    zb_client, jpeg_bytes
):
    """One OPTIONS dict configures both clients. The sync client must not treat the coroutine an
    `async def` hook returns as a truthy answer (which would allow every read)."""

    async def deny_everything(image_id, context):
        return False

    storage = ZeroBucketStorage(
        database_url=ZEROBUCKET_DATABASE_URL, before_get=deny_everything
    )
    try:
        name = zb_client.put(jpeg_bytes)
        with pytest.raises(AccessDeniedError):
            await storage.aopen(name)
        with pytest.raises(AccessDeniedError):
            await asyncio.to_thread(storage.open, name)
    finally:
        await storage.aclose()


async def test_an_async_before_get_hook_that_allows_lets_both_paths_read(
    zb_client, jpeg_bytes
):
    calls = []

    async def allow_and_record(image_id, context):
        calls.append(image_id)
        return True

    storage = ZeroBucketStorage(
        database_url=ZEROBUCKET_DATABASE_URL, before_get=allow_and_record
    )
    try:
        name = zb_client.put(jpeg_bytes)
        assert (await storage.aopen(name)).read() == jpeg_bytes
        assert (await asyncio.to_thread(storage.open, name)).read() == jpeg_bytes
        assert calls == [name, name]
    finally:
        await storage.aclose()
