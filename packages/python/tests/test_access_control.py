"""Tests for the Stage 4 access-control hook: before_get(image_id,
context) -> bool and before_put(context) -> bool, passed to the
ZeroBucket constructor.

Covers: allow/deny for every gated method (get, get_many, get_stream,
stream_to, metadata, put, put_many), that exists() is deliberately NOT
gated, that a hook exception propagates rather than being treated as an
implicit allow (fail closed), that denied calls never reach the database
(no on_operation event, no wasted validation work), that context is
passed through unmodified, and that put_many's hook is evaluated once
per call while get_many's is evaluated once per id.
"""

from __future__ import annotations

import io

import pytest

from zerobucket import OperationEvent, ZeroBucket
from zerobucket.exceptions import AccessDeniedError

from .conftest import TEST_DATABASE_URL


def _zb(**kwargs) -> ZeroBucket:
    return ZeroBucket(database_url=TEST_DATABASE_URL, **kwargs)


# ---- before_get: single-item methods --------------------------------------


def test_get_allowed_when_hook_returns_true(jpeg_bytes):
    zb = _zb(before_get=lambda image_id, context: True)
    try:
        image_id = zb.put(jpeg_bytes)
        image = zb.get(image_id)
        assert image.data == jpeg_bytes
    finally:
        zb.close()


def test_get_denied_when_hook_returns_false(jpeg_bytes):
    zb = _zb(before_get=lambda image_id, context: False)
    try:
        image_id = zb.put(jpeg_bytes)
        with pytest.raises(AccessDeniedError):
            zb.get(image_id)
    finally:
        zb.close()


def test_get_stream_denied_when_hook_returns_false(jpeg_bytes):
    zb = _zb(before_get=lambda image_id, context: False)
    try:
        image_id = zb.put(jpeg_bytes)
        with pytest.raises(AccessDeniedError):
            zb.get_stream(image_id)
    finally:
        zb.close()


def test_stream_to_denied_when_hook_returns_false(jpeg_bytes):
    zb = _zb(before_get=lambda image_id, context: False)
    try:
        image_id = zb.put(jpeg_bytes)
        with pytest.raises(AccessDeniedError):
            zb.stream_to(image_id, io.BytesIO())
    finally:
        zb.close()


def test_metadata_denied_when_hook_returns_false(jpeg_bytes):
    zb = _zb(before_get=lambda image_id, context: False)
    try:
        image_id = zb.put(jpeg_bytes)
        with pytest.raises(AccessDeniedError):
            zb.metadata(image_id)
    finally:
        zb.close()


def test_exists_is_not_gated_even_when_hook_would_deny(jpeg_bytes):
    """Deliberate scope decision, documented in the constructor's
    docstring: exists() is never gated, even with a before_get hook that
    denies everything."""
    zb = _zb(before_get=lambda image_id, context: False)
    try:
        image_id = zb.put(jpeg_bytes)
        assert zb.exists(image_id) is True
        assert zb.exists("00000000-0000-0000-0000-000000000000") is False
    finally:
        zb.close()


def test_get_denial_happens_before_db_round_trip(jpeg_bytes):
    """A denied get() should never reach the backend at all -- verified
    directly by swapping in a backend whose get() raises if called."""
    zb = _zb(before_get=lambda image_id, context: False)
    try:
        image_id = zb.put(jpeg_bytes)

        def _boom(*args, **kwargs):
            raise AssertionError("backend.get() should not have been called")

        zb._backend.get = _boom  # noqa: SLF001
        with pytest.raises(AccessDeniedError):
            zb.get(image_id)
    finally:
        zb.close()


def test_denied_get_does_not_emit_on_operation_event(jpeg_bytes):
    events: list[OperationEvent] = []
    zb = _zb(before_get=lambda image_id, context: False, on_operation=events.append)
    try:
        image_id_allow = ZeroBucket(database_url=TEST_DATABASE_URL).put(jpeg_bytes)
        events.clear()
        with pytest.raises(AccessDeniedError):
            zb.get(image_id_allow)
        assert events == []
    finally:
        zb.close()


# ---- before_get: context is passed through unmodified ---------------------


def test_get_passes_context_through_to_hook(jpeg_bytes):
    received = []

    def hook(image_id, context):
        received.append((image_id, context))
        return True

    zb = _zb(before_get=hook)
    try:
        image_id = zb.put(jpeg_bytes)
        zb.get(image_id, context={"user_id": "alice"})
        assert received == [(image_id, {"user_id": "alice"})]
    finally:
        zb.close()


def test_get_context_defaults_to_none(jpeg_bytes):
    received = []

    def hook(image_id, context):
        received.append(context)
        return True

    zb = _zb(before_get=hook)
    try:
        image_id = zb.put(jpeg_bytes)
        zb.get(image_id)
        assert received == [None]
    finally:
        zb.close()


# ---- before_get: hook exceptions propagate (fail closed) ------------------


def test_get_hook_exception_propagates_not_swallowed(jpeg_bytes):
    """A hook that raises must fail the call, not be treated as an
    implicit allow -- this is the core fail-closed security guarantee."""

    class MyAuthError(Exception):
        pass

    def broken_hook(image_id, context):
        raise MyAuthError("auth service unreachable")

    zb = _zb(before_get=broken_hook)
    try:
        image_id = zb.put(jpeg_bytes)
        with pytest.raises(MyAuthError):
            zb.get(image_id)
    finally:
        zb.close()


# ---- before_get: get_many, evaluated once per id ---------------------------


def test_get_many_allows_and_denies_per_id(jpeg_bytes, png_bytes):
    def hook(image_id, context):
        return image_id == allowed_id

    zb = _zb()
    try:
        allowed_id = zb.put(jpeg_bytes)
        denied_id = zb.put(png_bytes)
    finally:
        zb.close()

    zb2 = _zb(before_get=hook)
    try:
        results = {r.image_id: r for r in zb2.get_many([allowed_id, denied_id])}
        assert results[allowed_id].success
        assert results[allowed_id].image.data == jpeg_bytes
        assert not results[denied_id].success
        assert results[denied_id].error == "access denied"
    finally:
        zb2.close()


def test_get_many_hook_called_once_per_id(jpeg_bytes, png_bytes):
    calls = []

    def hook(image_id, context):
        calls.append(image_id)
        return True

    zb = _zb()
    try:
        id_a = zb.put(jpeg_bytes)
        id_b = zb.put(png_bytes)
    finally:
        zb.close()

    zb2 = _zb(before_get=hook)
    try:
        zb2.get_many([id_a, id_b])
        assert sorted(calls) == sorted([id_a, id_b])
    finally:
        zb2.close()


def test_get_many_denied_id_excluded_from_db_query(jpeg_bytes):
    """Denied ids should never reach the backend's get_many() call --
    verified by swapping in a backend whose get_many() asserts on what
    it was actually asked for."""
    zb = _zb()
    try:
        allowed_id = zb.put(jpeg_bytes)
        denied_id = zb.put(jpeg_bytes)
    finally:
        zb.close()

    real_get_many = None

    def hook(image_id, context):
        return image_id == allowed_id

    zb2 = _zb(before_get=hook)
    try:
        real_get_many = zb2._backend.get_many  # noqa: SLF001

        def spy(ids, **kwargs):
            assert denied_id not in ids
            return real_get_many(ids, **kwargs)

        zb2._backend.get_many = spy  # noqa: SLF001
        results = zb2.get_many([allowed_id, denied_id])
        by_id = {r.image_id: r for r in results}
        assert by_id[allowed_id].success
        assert by_id[denied_id].error == "access denied"
    finally:
        zb2.close()


def test_get_many_hook_exception_captured_per_item_not_raised(jpeg_bytes, png_bytes):
    """Unlike single-item get(), get_many is a batch call: a hook
    exception for one id is captured into that item's error, it does not
    abort the whole call."""

    def hook(image_id, context):
        if image_id == boom_id:
            raise RuntimeError("boom")
        return True

    zb = _zb()
    try:
        boom_id = zb.put(jpeg_bytes)
        fine_id = zb.put(png_bytes)
    finally:
        zb.close()

    zb2 = _zb(before_get=hook)
    try:
        results = {r.image_id: r for r in zb2.get_many([boom_id, fine_id])}
        assert results[boom_id].error == "boom"
        assert results[fine_id].success
    finally:
        zb2.close()


def test_get_many_empty_list_short_circuits_without_calling_hook():
    calls = []
    zb = _zb(before_get=lambda image_id, context: calls.append(image_id) or True)
    try:
        assert zb.get_many([]) == []
        assert calls == []
    finally:
        zb.close()


# ---- before_put: put() -----------------------------------------------------


def test_put_allowed_when_hook_returns_true(jpeg_bytes):
    zb = _zb(before_put=lambda context: True)
    try:
        image_id = zb.put(jpeg_bytes)
        assert zb.get(image_id).data == jpeg_bytes
    finally:
        zb.close()


def test_put_denied_when_hook_returns_false(jpeg_bytes):
    zb = _zb(before_put=lambda context: False)
    try:
        with pytest.raises(AccessDeniedError):
            zb.put(jpeg_bytes)
    finally:
        zb.close()


def test_put_denial_happens_before_validation_work(jpeg_bytes):
    """A denied put() should be rejected before any validation/checksum
    work happens -- verified by passing deliberately-corrupt bytes that
    would otherwise fail image validation with a DIFFERENT exception; if
    AccessDeniedError is what's raised, the hook ran first."""
    zb = _zb(before_put=lambda context: False)
    try:
        with pytest.raises(AccessDeniedError):
            zb.put(b"not a real image at all")
    finally:
        zb.close()


def test_put_hook_exception_propagates_not_swallowed(jpeg_bytes):
    class MyAuthError(Exception):
        pass

    def broken_hook(context):
        raise MyAuthError("auth service unreachable")

    zb = _zb(before_put=broken_hook)
    try:
        with pytest.raises(MyAuthError):
            zb.put(jpeg_bytes)
    finally:
        zb.close()


def test_put_passes_context_through_to_hook(jpeg_bytes):
    received = []

    def hook(context):
        received.append(context)
        return True

    zb = _zb(before_put=hook)
    try:
        zb.put(jpeg_bytes, context={"tenant": "acme"})
        assert received == [{"tenant": "acme"}]
    finally:
        zb.close()


# ---- before_put: put_many, evaluated once for the whole call --------------


def test_put_many_allowed_when_hook_returns_true(jpeg_bytes, png_bytes):
    zb = _zb(before_put=lambda context: True)
    try:
        results = zb.put_many([jpeg_bytes, png_bytes])
        assert all(r.success for r in results)
    finally:
        zb.close()


def test_put_many_denied_marks_every_item_without_processing_any(jpeg_bytes, png_bytes):
    zb = _zb(before_put=lambda context: False)
    try:
        results = zb.put_many([jpeg_bytes, png_bytes])
        assert len(results) == 2
        assert all(not r.success for r in results)
        assert all(r.error == "access denied" for r in results)
        assert all(r.image_id is None for r in results)
    finally:
        zb.close()


def test_put_many_hook_called_exactly_once_not_per_item(jpeg_bytes, png_bytes):
    call_count = 0

    def hook(context):
        nonlocal call_count
        call_count += 1
        return True

    zb = _zb(before_put=hook)
    try:
        zb.put_many([jpeg_bytes, png_bytes, jpeg_bytes])
        assert call_count == 1
    finally:
        zb.close()


def test_put_many_hook_exception_marks_every_item(jpeg_bytes, png_bytes):
    def broken_hook(context):
        raise RuntimeError("boom")

    zb = _zb(before_put=broken_hook)
    try:
        results = zb.put_many([jpeg_bytes, png_bytes])
        assert len(results) == 2
        assert all(r.error == "boom" for r in results)
    finally:
        zb.close()


def test_put_many_context_passed_through_once(jpeg_bytes, png_bytes):
    received = []

    def hook(context):
        received.append(context)
        return True

    zb = _zb(before_put=hook)
    try:
        zb.put_many([jpeg_bytes, png_bytes], context={"tenant": "acme"})
        assert received == [{"tenant": "acme"}]
    finally:
        zb.close()


# ---- no hooks configured: fully backward compatible ------------------------


def test_no_hooks_configured_everything_works_as_before(jpeg_bytes, png_bytes):
    zb = _zb()
    try:
        image_id = zb.put(jpeg_bytes)
        assert zb.get(image_id).data == jpeg_bytes
        assert zb.metadata(image_id).size_bytes > 0
        assert zb.exists(image_id) is True
        assert b"".join(zb.get_stream(image_id)) == jpeg_bytes

        results = zb.get_many([image_id])
        assert results[0].success

        ids = [r.image_id for r in zb.put_many([jpeg_bytes, png_bytes])]
        assert all(ids)
    finally:
        zb.close()
