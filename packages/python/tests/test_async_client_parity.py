"""0.32.0: AsyncZeroBucket gets the sync client's before_get / before_put hooks (with
context=), optimize=True (max_width=/format=/quality=) and custom validator=.

The cases mirror tests/test_access_control.py, test_optimization.py and
test_custom_validators.py one for one, so the async client is held to exactly the rules the
sync one is. Every scenario runs on AsyncSQLiteBackend, AsyncPostgresBackend and
AsyncMySQLBackend (SQLite needs no server; the others skip when unreachable).
"""

# ruff: noqa: F401, F811
from __future__ import annotations

import asyncio
import io

import pytest
from PIL import Image as PILImage

from zerobucket import (
    AccessDeniedError,
    AsyncZeroBucket,
    ContentValidationError,
    ImageNotFoundError,
    ImageValidationError,
    ZeroBucket,
)
from zerobucket.adapters.mysql_async import AsyncMySQLBackend
from zerobucket.adapters.postgres_async import AsyncPostgresBackend
from zerobucket.adapters.sqlite_async import AsyncSQLiteBackend
from zerobucket.validators.pdf import PDFValidator

from .test_cli_backends import target  # the sqlite/postgres/mysql fixture

NIL = "00000000-0000-0000-0000-000000000000"


def _jpeg(color=(200, 0, 0), size=(40, 30)) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", size, color=color).save(buf, format="JPEG")
    return buf.getvalue()


def _png(color=(0, 200, 0), size=(40, 30)) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", size, color=color).save(buf, format="PNG")
    return buf.getvalue()


def _fake_pdf() -> bytes:
    return b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF"


def _async_backend(target, **kwargs):
    if target.kind == "sqlite":
        return AsyncSQLiteBackend(target.path, **kwargs)
    if target.kind == "postgres":
        return AsyncPostgresBackend(target.url, **kwargs)
    return AsyncMySQLBackend(target.url, **kwargs)


@pytest.fixture
async def make(target):
    """Factory for AsyncZeroBucket clients on this database; all closed afterwards."""
    created = []

    def factory(**client_kwargs):
        backend = _async_backend(target, retry_base_delay=0.01)
        zb = AsyncZeroBucket(backend=backend, **client_kwargs)
        created.append(zb)
        return zb

    yield factory
    for zb in created:
        await zb.close()


async def _seed(make, data=None) -> str:
    """Store one image through an unguarded client and return its id."""
    return await make().put(data or _jpeg())


class Recorder:
    """A hook that records its calls and answers with a fixed verdict."""

    def __init__(self, verdict=True):
        self.verdict = verdict
        self.calls: list[tuple] = []

    def get(self, image_id, context):
        self.calls.append((image_id, context))
        return self.verdict

    def put(self, context):
        self.calls.append((context,))
        return self.verdict


# ---- before_get ------------------------------------------------------------------------------


async def test_get_allowed_when_hook_returns_true(make):
    image_id = await _seed(make)
    zb = make(before_get=Recorder(True).get)
    assert (await zb.get(image_id)).mime_type == "image/jpeg"


async def test_get_denied_when_hook_returns_false(make):
    image_id = await _seed(make)
    zb = make(before_get=Recorder(False).get)
    with pytest.raises(AccessDeniedError):
        await zb.get(image_id)


@pytest.mark.parametrize("method", ["get_stream", "stream_to", "metadata"])
async def test_every_gated_read_is_denied_when_the_hook_says_no(make, method):
    image_id = await _seed(make)
    zb = make(before_get=Recorder(False).get)
    with pytest.raises(AccessDeniedError):
        if method == "stream_to":
            await zb.stream_to(image_id, io.BytesIO())
        else:
            await getattr(zb, method)(image_id)


async def test_exists_is_not_gated_even_when_the_hook_would_deny(make):
    image_id = await _seed(make)
    hook = Recorder(False)
    zb = make(before_get=hook.get)
    assert await zb.exists(image_id) is True
    assert (
        hook.calls == []
    )  # same as the sync client: a bare existence check is not gated


async def test_a_denied_get_never_reaches_the_database(make):
    class Spy:
        """Counts every awaited call on the real backend."""

        def __init__(self, backend):
            self._backend = backend
            self.calls: list[str] = []

        def __getattr__(self, name):
            attr = getattr(self._backend, name)
            if not callable(attr):
                return attr

            async def wrapper(*a, **kw):
                self.calls.append(name)
                return await attr(*a, **kw)

            return wrapper

    image_id = await _seed(make)
    backend = Spy(make()._backend)
    zb = AsyncZeroBucket(backend=backend, before_get=Recorder(False).get)
    with pytest.raises(AccessDeniedError):
        await zb.get(image_id)
    assert backend.calls == []


async def test_context_is_passed_through_to_the_hook(make):
    image_id = await _seed(make)
    hook = Recorder(True)
    zb = make(before_get=hook.get)
    await zb.get(image_id, context={"user": "ada"})
    await zb.metadata(image_id, context={"user": "bob"})
    assert hook.calls == [(image_id, {"user": "ada"}), (image_id, {"user": "bob"})]


async def test_context_defaults_to_none(make):
    image_id = await _seed(make)
    hook = Recorder(True)
    await make(before_get=hook.get).get(image_id)
    assert hook.calls == [(image_id, None)]


async def test_a_hook_exception_propagates_and_is_not_swallowed(make):
    image_id = await _seed(make)

    def broken(image_id, context):
        raise RuntimeError("auth service down")

    with pytest.raises(RuntimeError, match="auth service down"):
        await make(before_get=broken).get(image_id)


async def test_a_malformed_id_is_not_found_and_never_reaches_the_hook(make):
    hook = Recorder(True)
    zb = make(before_get=hook.get)
    with pytest.raises(ImageNotFoundError):
        await zb.get("not-a-uuid")
    with pytest.raises(ImageNotFoundError):
        await zb.get("1'; DROP TABLE zerobucket_images; --")
    assert hook.calls == []


async def test_async_hooks_are_awaited(make):
    """Unlike the sync client, a hook may be an `async def` (it can query your database)."""
    image_id = await _seed(make)
    seen = []

    async def allow(image_id, context):
        await asyncio.sleep(0)  # a real suspension point
        seen.append(context)
        return True

    async def deny(image_id, context):
        await asyncio.sleep(0)
        return False

    assert (await make(before_get=allow).get(image_id, context={"k": 1})).size_bytes > 0
    assert seen == [{"k": 1}]
    with pytest.raises(AccessDeniedError):
        await make(before_get=deny).get(image_id)


# ---- before_get on get_many ------------------------------------------------------------------


async def test_get_many_allows_and_denies_per_id(make):
    a = await _seed(make, _jpeg((1, 2, 3)))
    b = await _seed(make, _png())

    def only_a(image_id, context):
        return image_id == a

    results = await make(before_get=only_a).get_many([a, b])
    assert results[0].image is not None and results[0].error is None
    assert results[1].image is None and results[1].error == "access denied"


async def test_get_many_calls_the_hook_once_per_id_and_keeps_the_order(make):
    a = await _seed(make, _jpeg((1, 2, 3)))
    b = await _seed(make, _png())
    hook = Recorder(True)
    results = await make(before_get=hook.get).get_many([b, a, NIL], context={"u": 1})
    assert [r.image_id for r in results] == [b, a, NIL]
    assert [r.error for r in results] == [None, None, "not found"]
    assert hook.calls == [(b, {"u": 1}), (a, {"u": 1}), (NIL, {"u": 1})]


async def test_get_many_a_denied_id_is_left_out_of_the_database_query(make):
    a = await _seed(make, _jpeg((1, 2, 3)))
    b = await _seed(make, _png())
    zb = make(before_get=lambda image_id, context: image_id == a)
    queried: list[list[str]] = []
    real = zb._backend.get_many

    async def spy(ids):
        queried.append(list(ids))
        return await real(ids)

    zb._backend.get_many = spy
    await zb.get_many([a, b])
    assert queried == [[a]]


async def test_get_many_a_hook_exception_is_captured_per_item(make):
    a = await _seed(make, _jpeg((1, 2, 3)))
    b = await _seed(make, _png())

    def flaky(image_id, context):
        if image_id == b:
            raise RuntimeError("lookup failed")
        return True

    results = await make(before_get=flaky).get_many([a, b])
    assert results[0].error is None
    assert results[1].image is None and "lookup failed" in results[1].error


async def test_get_many_malformed_ids_are_not_found_and_skip_the_hook(make):
    a = await _seed(make)
    hook = Recorder(True)
    results = await make(before_get=hook.get).get_many([a, "not-a-uuid"])
    assert results[1].error == "not found"
    assert [c[0] for c in hook.calls] == [a]


async def test_get_many_empty_list_short_circuits_without_calling_the_hook(make):
    hook = Recorder(True)
    assert await make(before_get=hook.get).get_many([]) == []
    assert hook.calls == []


# ---- before_put ------------------------------------------------------------------------------


async def test_put_allowed_when_the_hook_returns_true(make):
    image_id = await make(before_put=Recorder(True).put).put(_jpeg())
    assert await make().exists(image_id)


async def test_put_denied_when_the_hook_returns_false_and_nothing_is_stored(
    make, target
):
    zb = make(before_put=Recorder(False).put)
    with pytest.raises(AccessDeniedError):
        await zb.put(_jpeg())
    sync = ZeroBucket(backend=target.backend())
    try:
        info = sync._backend.get_info()
        assert info is None or info.count == 0
    finally:
        sync.close()


async def test_put_denial_happens_before_any_validation_work(make):
    zb = make(before_put=Recorder(False).put)
    # Not an image at all: a denial must win over the validation error.
    with pytest.raises(AccessDeniedError):
        await zb.put(b"this is not an image")


async def test_put_hook_exception_propagates_and_is_not_swallowed(make):
    def broken(context):
        raise RuntimeError("auth service down")

    with pytest.raises(RuntimeError, match="auth service down"):
        await make(before_put=broken).put(_jpeg())


async def test_put_context_is_passed_through(make):
    hook = Recorder(True)
    await make(before_put=hook.put).put(_jpeg(), context={"tenant": 7})
    assert hook.calls == [({"tenant": 7},)]


async def test_an_async_before_put_hook_is_awaited(make):
    async def deny(context):
        await asyncio.sleep(0)
        return False

    with pytest.raises(AccessDeniedError):
        await make(before_put=deny).put(_jpeg())


async def test_put_many_allowed_when_the_hook_returns_true(make):
    results = await make(before_put=Recorder(True).put).put_many([_jpeg(), _png()])
    assert [r.error for r in results] == [None, None]


async def test_put_many_denied_marks_every_item_and_stores_none(make, target):
    results = await make(before_put=Recorder(False).put).put_many([_jpeg(), _png()])
    assert [r.error for r in results] == ["access denied", "access denied"]
    assert all(r.image_id is None for r in results)
    sync = ZeroBucket(backend=target.backend())
    try:
        info = sync._backend.get_info()
        assert info is None or info.count == 0
    finally:
        sync.close()


async def test_put_many_calls_the_hook_exactly_once_with_the_context(make):
    hook = Recorder(True)
    await make(before_put=hook.put).put_many(
        [_jpeg(), _png(), _jpeg((5, 5, 5))], context={"u": 1}
    )
    assert hook.calls == [({"u": 1},)]


async def test_put_many_a_hook_exception_marks_every_item(make):
    def broken(context):
        raise RuntimeError("auth service down")

    results = await make(before_put=broken).put_many([_jpeg(), _png()])
    assert all("auth service down" in r.error for r in results)


async def test_with_no_hooks_everything_works_as_before(make):
    zb = make()
    image_id = await zb.put(_jpeg())
    assert (await zb.get(image_id)).size_bytes > 0
    assert [r.error for r in await zb.put_many([_png()])] == [None]
    assert [r.error for r in await zb.get_many([image_id])] == [None]


# ---- optimize --------------------------------------------------------------------------------


async def test_without_optimize_the_exact_bytes_are_stored(make):
    zb = make()
    data = _jpeg(size=(400, 300))
    image_id = await zb.put(
        data, max_width=100, quality=10
    )  # ignored: optimize is False
    assert (await zb.get(image_id)).data == data


async def test_optimize_downscales_to_max_width_and_keeps_the_aspect_ratio(make):
    zb = make()
    image_id = await zb.put(_jpeg(size=(400, 300)), optimize=True, max_width=100)
    stored = await zb.get(image_id)
    assert (stored.width, stored.height) == (100, 75)
    assert PILImage.open(io.BytesIO(stored.data)).size == (100, 75)
    assert (
        stored.checksum_sha256 == __import__("hashlib").sha256(stored.data).hexdigest()
    )


async def test_optimize_can_convert_the_format(make):
    zb = make()
    image_id = await zb.put(_jpeg(size=(80, 60)), optimize=True, format="png")
    stored = await zb.get(image_id)
    assert stored.mime_type == "image/png"
    assert PILImage.open(io.BytesIO(stored.data)).format == "PNG"


async def test_optimize_strips_metadata(make):
    img = PILImage.new("RGB", (60, 40), (10, 20, 30))
    exif = img.getexif()
    exif[0x010F] = "SecretCameraMaker"
    buf = io.BytesIO()
    img.save(buf, format="JPEG", exif=exif)
    assert b"SecretCameraMaker" in buf.getvalue()

    zb = make()
    image_id = await zb.put(buf.getvalue(), optimize=True)
    assert b"SecretCameraMaker" not in (await zb.get(image_id)).data


async def test_the_async_and_sync_clients_store_identical_bytes_when_optimizing(
    make, target
):
    data = _jpeg((90, 40, 200), size=(300, 200))
    async_id = await make().put(data, optimize=True, max_width=120, quality=80)
    sync = ZeroBucket(backend=target.backend())
    try:
        sync_id = sync.put(data, optimize=True, max_width=120, quality=80)
        assert sync.get(async_id).data == sync.get(sync_id).data
    finally:
        sync.close()


async def test_put_many_applies_the_optimize_settings_to_every_item(make):
    zb = make()
    results = await zb.put_many(
        [_jpeg(size=(400, 300)), _png(size=(500, 100))], optimize=True, max_width=100
    )
    assert [r.error for r in results] == [None, None]
    widths = [(await zb.get(r.image_id)).width for r in results]
    assert widths == [100, 100]


async def test_put_many_one_bad_item_does_not_abort_the_rest_when_optimizing(make):
    results = await make().put_many([_jpeg(), b"not an image"], optimize=True)
    assert results[0].error is None
    assert results[1].image_id is None and results[1].error


# ---- custom validator ------------------------------------------------------------------------


async def test_a_custom_validator_stores_a_pdf_unchanged(make):
    zb = make()
    pdf = _fake_pdf()
    image_id = await zb.put(pdf, validator=PDFValidator(), filename="doc.pdf")
    stored = await zb.get(image_id)
    assert stored.data == pdf
    assert stored.mime_type == "application/pdf"
    assert stored.filename == "doc.pdf"


async def test_without_the_validator_a_pdf_is_rejected_as_an_image(make):
    with pytest.raises(ImageValidationError):
        await make().put(_fake_pdf())


async def test_a_custom_validator_rejection_surfaces(make):
    with pytest.raises(ContentValidationError):
        await make().put(b"definitely not a pdf", validator=PDFValidator())


async def test_optimize_together_with_a_validator_is_refused(make):
    with pytest.raises(ImageValidationError, match="optimize=True is not supported"):
        await make().put(_fake_pdf(), validator=PDFValidator(), optimize=True)


async def test_put_many_with_a_validator(make):
    results = await make().put_many([_fake_pdf(), b"nope"], validator=PDFValidator())
    assert results[0].error is None
    assert results[1].image_id is None and results[1].error


# ---- the event loop stays free ---------------------------------------------------------------


async def test_optimizing_does_not_block_the_event_loop(make):
    """The CPU-heavy re-encode runs in a worker thread, so other coroutines keep running."""
    zb = make()
    big = _jpeg(size=(3000, 2000))
    ticks = 0
    stop = False

    async def ticker():
        nonlocal ticks
        while not stop:
            ticks += 1
            await asyncio.sleep(0.001)

    task = asyncio.create_task(ticker())
    await zb.put(big, optimize=True, max_width=300)
    stop = True
    await task
    assert ticks >= 5  # the loop kept turning while Pillow worked
