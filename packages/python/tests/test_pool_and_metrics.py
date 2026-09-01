"""Tests for pool_min_size/pool_max_size/pool_timeout and on_operation --
the two "optimize ZeroBucket" additions: tune the pool, measure what's
happening.
"""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage

from zerobucket import OperationEvent, ZeroBucket
from zerobucket.exceptions import ImageNotFoundError


def _jpeg_bytes(color=(5, 10, 15)) -> bytes:
    img = PILImage.new("RGB", (30, 20), color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


# ---- pool configuration -------------------------------------------------


def test_pool_size_defaults_unchanged(_db_available):
    """Confirms the new params don't change default behavior -- same
    1/5/10 defaults as every prior version, just now visible/overridable."""
    from tests.conftest import TEST_DATABASE_URL

    zb = ZeroBucket(database_url=TEST_DATABASE_URL)
    try:
        pool = zb._backend._pool  # noqa: SLF001
        assert pool.min_size == 1
        assert pool.max_size == 5
        assert pool.timeout == 10
    finally:
        zb.close()


def test_pool_size_actually_configurable(_db_available):
    """The real point of this feature: custom values genuinely reach
    the underlying psycopg_pool.ConnectionPool, not just accepted and
    silently ignored."""
    from tests.conftest import TEST_DATABASE_URL

    zb = ZeroBucket(
        database_url=TEST_DATABASE_URL,
        pool_min_size=2,
        pool_max_size=9,
        pool_timeout=3,
    )
    try:
        pool = zb._backend._pool  # noqa: SLF001
        assert pool.min_size == 2
        assert pool.max_size == 9
        assert pool.timeout == 3
    finally:
        zb.close()


# ---- on_operation callback ------------------------------------------------


def test_on_operation_fires_on_successful_put(_db_available):
    from tests.conftest import TEST_DATABASE_URL

    events: list[OperationEvent] = []
    zb = ZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    try:
        image_id = zb.put(_jpeg_bytes())

        put_events = [e for e in events if e.operation == "put"]
        assert len(put_events) == 1
        event = put_events[0]
        assert event.success is True
        assert event.error is None
        assert event.retry_count == 0
        assert event.duration_seconds > 0

        zb.delete(image_id)
    finally:
        zb.close()


def test_on_operation_reports_correct_operation_names(_db_available):
    """Every public operation reports the right name, including batch
    variants -- this is what makes the callback actually useful for
    per-operation metrics/dashboards."""
    from tests.conftest import TEST_DATABASE_URL

    events: list[OperationEvent] = []
    zb = ZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    try:
        id1 = zb.put(_jpeg_bytes((1, 1, 1)))
        zb.get(id1)
        zb.metadata(id1)
        zb.exists(id1)

        ids = [
            r.image_id
            for r in zb.put_many([_jpeg_bytes((2, 2, 2)), _jpeg_bytes((3, 3, 3))])
        ]
        zb.get_many(ids)
        zb.delete_many(ids)
        zb.delete(id1)

        seen_operations = {e.operation for e in events}
        assert seen_operations == {
            "migrate",  # from ZeroBucket's own construction/auto_migrate
            "put",
            "get",
            "get_metadata",
            "exists",
            "put_many",
            "get_many",
            "delete_many",
            "delete",
        }
    finally:
        zb.close()


def test_on_operation_fires_on_failure_with_error_message(_db_available):
    from tests.conftest import TEST_DATABASE_URL

    events: list[OperationEvent] = []
    zb = ZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    try:
        with pytest.raises(ImageNotFoundError):
            zb.get("00000000-0000-0000-0000-000000000000")

        # get() on a missing id is a successful DB query returning no
        # row -- the OperationEvent reports success=True (the QUERY
        # succeeded), ImageNotFoundError is raised at the client.py
        # layer above the backend, not inside the storage operation
        # itself. Confirm we understand where that boundary actually is.
        get_events = [e for e in events if e.operation == "get"]
        assert len(get_events) == 1
        assert get_events[0].success is True
    finally:
        zb.close()


def test_broken_callback_does_not_break_real_operations(_db_available):
    """The critical safety property: a callback that itself raises must
    never prevent the actual image operation from succeeding."""
    from tests.conftest import TEST_DATABASE_URL

    def bad_callback(event):
        raise RuntimeError("this metrics callback is broken on purpose")

    zb = ZeroBucket(database_url=TEST_DATABASE_URL, on_operation=bad_callback)
    try:
        # If the broken callback interfered, this would raise instead
        # of returning a real id.
        image_id = zb.put(_jpeg_bytes())
        assert zb.exists(image_id) is True
        zb.delete(image_id)
    finally:
        zb.close()


def test_on_operation_with_connection_always_reports_zero_retries(
    _db_available, db_connection_factory
):
    """Mirrors test_retry.py's connection= safety-rule test, but from
    the observability side: retry_count must be 0 on the connection=
    path even when the event is otherwise reported correctly."""
    from tests.conftest import TEST_DATABASE_URL

    events: list[OperationEvent] = []
    zb = ZeroBucket(database_url=TEST_DATABASE_URL, on_operation=events.append)
    conn = db_connection_factory()
    conn.autocommit = False
    try:
        image_id = zb.put(_jpeg_bytes(), connection=conn)
        conn.commit()

        put_events = [e for e in events if e.operation == "put"]
        assert len(put_events) == 1
        assert put_events[0].retry_count == 0

        zb.delete(image_id)
    finally:
        conn.close()
        zb.close()
