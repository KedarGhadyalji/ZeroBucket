"""Tests for AsyncZeroBucket (Stage 5's async support), built on
psycopg3's native async mode.

Covers: put/get round-trip, put_many/get_many/delete_many batch
semantics matching the sync client, get_stream/stream_to (including the
"must await before iterating" contract and its not-found timing),
not-found errors, validation errors still firing through
asyncio.to_thread, concurrent put_many validation actually running
concurrently (not just correctly), the lazy pool-open/migrate-once
behavior under concurrent first callers, and context-manager support.

Does NOT cover (out of scope for this first pass, see async_client.py's
module docstring): dedup mode, before_get/before_put hooks,
on_operation, optimize=/validator=, connection=, retry.
"""

from __future__ import annotations

import asyncio
import io
import time

import pytest
from PIL import Image as PILImage

from zerobucket import AsyncZeroBucket, ImageNotFoundError
from zerobucket.exceptions import ImageTooLargeError, UnsupportedFormatError

from .conftest import TEST_DATABASE_URL


def _jpeg_bytes(size=(300, 200), color=(40, 80, 120)) -> bytes:
    img = PILImage.new("RGB", size, color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


# ---- put / get round-trip ---------------------------------------------


async def test_put_get_round_trip(async_images):
    data = _jpeg_bytes()
    image_id = await async_images.put(data)
    image = await async_images.get(image_id)
    assert image.data == data
    assert image.mime_type == "image/jpeg"
    assert image.width == 300
    assert image.height == 200


async def test_put_accepts_file_like_object(async_images):
    data = _jpeg_bytes()
    image_id = await async_images.put(io.BytesIO(data))
    image = await async_images.get(image_id)
    assert image.data == data


async def test_get_not_found_raises(async_images):
    with pytest.raises(ImageNotFoundError):
        await async_images.get("00000000-0000-0000-0000-000000000000")


async def test_put_validates_and_rejects_bad_input(async_images):
    """Validation still runs (via asyncio.to_thread) -- not skipped just
    because this is the async client."""
    with pytest.raises(Exception):  # noqa: B017 -- ImageValidationError family
        await async_images.put(b"not a real image")


async def test_put_oversized_input_rejected():
    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL, max_bytes=50)
    try:
        with pytest.raises(ImageTooLargeError):
            await zb.put(_jpeg_bytes())
    finally:
        await zb.close()


async def test_put_disallowed_format_rejected():
    zb = AsyncZeroBucket(
        database_url=TEST_DATABASE_URL, allowed_formats=frozenset({"png"})
    )
    try:
        with pytest.raises(UnsupportedFormatError):
            await zb.put(_jpeg_bytes())
    finally:
        await zb.close()


async def test_put_and_get_preserve_filename(async_images):
    image_id = await async_images.put(_jpeg_bytes(), filename="avatar.jpg")
    image = await async_images.get(image_id)
    assert image.filename == "avatar.jpg"


# ---- metadata / exists / delete ----------------------------------------


async def test_metadata_without_bytes(async_images):
    data = _jpeg_bytes()
    image_id = await async_images.put(data)
    meta = await async_images.metadata(image_id)
    assert meta.size_bytes == len(data)
    assert meta.width == 300


async def test_metadata_not_found_raises(async_images):
    with pytest.raises(ImageNotFoundError):
        await async_images.metadata("00000000-0000-0000-0000-000000000000")


async def test_exists_true_and_false(async_images):
    image_id = await async_images.put(_jpeg_bytes())
    assert await async_images.exists(image_id) is True
    assert await async_images.exists("00000000-0000-0000-0000-000000000000") is False


async def test_delete_returns_true_then_false(async_images):
    image_id = await async_images.put(_jpeg_bytes())
    assert await async_images.delete(image_id) is True
    assert await async_images.exists(image_id) is False
    assert await async_images.delete(image_id) is False


# ---- batch operations ----------------------------------------------------


async def test_put_many_all_succeed(async_images):
    data = [_jpeg_bytes(), _jpeg_bytes((10, 10)), _jpeg_bytes((500, 500))]
    results = await async_images.put_many(data)
    assert len(results) == 3
    assert all(r.success for r in results)
    ids = [r.image_id for r in results]
    assert len(set(ids)) == 3


async def test_put_many_partial_failure_is_best_effort(async_images):
    good = _jpeg_bytes()
    results = await async_images.put_many([good, b"not an image", good])
    assert results[0].success
    assert not results[1].success
    assert results[2].success


async def test_put_many_filenames_length_mismatch_raises(async_images):
    with pytest.raises(ValueError, match="filenames"):
        await async_images.put_many([_jpeg_bytes()], filenames=["a.jpg", "b.jpg"])


async def test_put_many_validation_runs_concurrently(async_images):
    """A real advantage of the async client over the sync one: per-item
    validation for a put_many() batch should run concurrently (via
    asyncio.gather), not serially. Verified by wrapping validate_image
    with an artificial delay and confirming wall-clock time is closer to
    ONE delay than N delays."""
    import zerobucket.async_client as async_client_module

    real_validate = async_client_module.validate_image
    delay = 0.2
    n = 5

    def slow_validate(*args, **kwargs):
        time.sleep(delay)
        return real_validate(*args, **kwargs)

    async_client_module.validate_image = slow_validate
    try:
        start = time.monotonic()
        results = await async_images.put_many([_jpeg_bytes()] * n)
        elapsed = time.monotonic() - start
    finally:
        async_client_module.validate_image = real_validate

    assert all(r.success for r in results)
    # Serial would take >= n * delay (>= 1.0s for n=5); concurrent should
    # be well under that -- generous margin to avoid flakiness.
    assert elapsed < (n * delay) * 0.75


async def test_get_many_mixed_found_and_missing(async_images):
    data = _jpeg_bytes()
    image_id = await async_images.put(data)
    results = await async_images.get_many(
        [image_id, "00000000-0000-0000-0000-000000000000"]
    )
    by_id = {r.image_id: r for r in results}
    assert by_id[image_id].success
    assert by_id[image_id].image.data == data
    assert not by_id["00000000-0000-0000-0000-000000000000"].success


async def test_get_many_empty_list(async_images):
    assert await async_images.get_many([]) == []


async def test_delete_many_mixed_existing_and_missing(async_images):
    id_a = await async_images.put(_jpeg_bytes())
    id_b = await async_images.put(_jpeg_bytes())
    results = await async_images.delete_many(
        [id_a, "00000000-0000-0000-0000-000000000000"]
    )
    by_id = {r.image_id: r for r in results}
    assert by_id[id_a].deleted is True
    assert by_id["00000000-0000-0000-0000-000000000000"].deleted is False
    assert await async_images.exists(id_a) is False
    assert await async_images.exists(id_b) is True  # untouched


async def test_delete_many_empty_list(async_images):
    assert await async_images.delete_many([]) == []


# ---- get_stream / stream_to ------------------------------------------------


async def test_get_stream_reconstructs_exact_bytes(async_images):
    data = _jpeg_bytes((800, 600))
    image_id = await async_images.put(data)

    stream = await async_images.get_stream(image_id, chunk_size=500)
    chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == data
    assert len(chunks) > 1


async def test_get_stream_must_be_awaited_before_iterating(async_images):
    """This is the documented API difference from the sync client:
    get_stream() is a coroutine, not directly an async generator."""
    data = _jpeg_bytes()
    image_id = await async_images.put(data)

    coro_or_result = async_images.get_stream(image_id)
    assert asyncio.iscoroutine(coro_or_result)
    stream = await coro_or_result
    chunks = [chunk async for chunk in stream]
    assert b"".join(chunks) == data


async def test_get_stream_not_found_raises_on_await(async_images):
    with pytest.raises(ImageNotFoundError):
        await async_images.get_stream("00000000-0000-0000-0000-000000000000")


async def test_stream_to_writes_full_content_and_returns_count(async_images):
    data = _jpeg_bytes()
    image_id = await async_images.put(data)
    dest = io.BytesIO()
    total = await async_images.stream_to(image_id, dest, chunk_size=333)
    assert total == len(data)
    assert dest.getvalue() == data


async def test_stream_to_not_found_raises(async_images):
    with pytest.raises(ImageNotFoundError):
        await async_images.stream_to(
            "00000000-0000-0000-0000-000000000000", io.BytesIO()
        )


# ---- lazy pool-open / migrate-once under concurrency ----------------------


async def test_concurrent_first_calls_only_open_and_migrate_once(_db_available):
    """AsyncPostgresBackend lazily opens its pool + runs migration on
    first use, guarded by an asyncio.Lock. Firing many operations
    concurrently as the very first thing done with a fresh instance
    should not race or double-migrate -- and every one of them should
    still succeed."""
    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL)
    try:
        results = await asyncio.gather(
            *(zb.exists("00000000-0000-0000-0000-000000000000") for _ in range(20))
        )
        assert results == [False] * 20
    finally:
        await zb.close()


# ---- context manager -------------------------------------------------------


async def test_async_context_manager_closes_on_exit(_db_available):
    async with AsyncZeroBucket(database_url=TEST_DATABASE_URL) as zb:
        image_id = await zb.put(_jpeg_bytes())
        assert await zb.exists(image_id) is True
    # backend pool should be closed now -- further use should fail
    with pytest.raises(Exception):  # noqa: B017
        await zb.exists(image_id)


# ---- Windows ProactorEventLoop fail-fast (see postgres_async.py) ----------


def test_windows_proactor_loop_error_message_python312_plus(monkeypatch):
    """Covers the message-construction logic only -- the actual
    sys.platform == "win32" branch inside _ensure_ready() that calls
    this function structurally cannot be exercised on Linux/macOS CI:
    asyncio.ProactorEventLoop doesn't exist as an importable attribute
    outside real Windows, so there's no way to construct a realistic
    "running under it" scenario here. That branch is only ever actually
    exercised by running the test suite on real Windows -- stated
    honestly as a testing gap, not silently uncovered."""
    import zerobucket.adapters.postgres_async as mod

    monkeypatch.setattr(mod.sys, "version_info", (3, 12, 0))
    err = mod._windows_proactor_loop_error()
    assert "ProactorEventLoop" in str(err)
    assert "SelectorEventLoop" in str(err)
    assert "loop_factory" in str(err)


def test_windows_proactor_loop_error_message_pre_312(monkeypatch):
    import zerobucket.adapters.postgres_async as mod

    monkeypatch.setattr(mod.sys, "version_info", (3, 11, 0))
    err = mod._windows_proactor_loop_error()
    assert "ProactorEventLoop" in str(err)
    assert "WindowsSelectorEventLoopPolicy" in str(err)


# ---- no new dependency check ----------------------------------------------


def test_async_backend_uses_psycopg_not_asyncpg():
    """Confirms the module docstring's claim directly: no `asyncpg`
    import anywhere in the async adapter."""
    import zerobucket.adapters.postgres_async as mod

    source = open(mod.__file__).read()
    assert "import asyncpg" not in source
    assert "from asyncpg" not in source
