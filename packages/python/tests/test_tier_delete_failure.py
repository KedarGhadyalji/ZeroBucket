"""0.29.0: a failed object-storage cleanup never fails a delete.

Once the database row is gone the image is "not found" to everyone, so a
failed S3 delete leaves only an orphaned object. Raising used to tell the
caller the image still existed when it did not. The failure is now reported
to `on_operation` as an "object_storage_delete" event instead.

Every scenario runs on SQLite, PostgreSQL and MySQL. S3 is moto's in-process
mock; the failure is injected, and one test causes a REAL failure by removing
the bucket after the image was tiered.
"""

from __future__ import annotations

import contextlib
import io
import tempfile

import boto3
import pytest
from PIL import Image as PILImage

from zerobucket import ZeroBucket
from zerobucket.exceptions import StorageError
from zerobucket.object_storage import ObjectStorage

from .test_cli_backends import (
    TEST_MYSQL_URL,
    Target,
    _drop_everything,
    _remove_db_files,
)

KINDS = ["sqlite", "postgres", "mysql"]


def _jpeg(color) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (40, 30), color=color).save(buf, format="JPEG")
    return buf.getvalue()


@pytest.fixture(params=KINDS)
def target(request):
    kind = request.param
    if kind == "postgres":
        request.getfixturevalue("_db_available")
    elif kind == "mysql":
        import pymysql

        from zerobucket.adapters.mysql import _parse_database_url

        try:
            pymysql.connect(**_parse_database_url(TEST_MYSQL_URL), connect_timeout=3).close()
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"MySQL/MariaDB not reachable: {exc}")
    path = tempfile.mktemp(suffix=".db") if kind == "sqlite" else None
    t = Target(kind, path)
    _drop_everything(t)
    yield t
    _drop_everything(t)
    if path:
        _remove_db_files(path)


@pytest.fixture
def events():
    return []


@pytest.fixture
def tiered(target, s3_bucket, events):
    """(backend, ids, s3 client): three images, all tiered to the bucket."""
    store = ObjectStorage(s3_bucket, region_name="us-east-1")
    backend = target.backend(object_storage=store, on_operation=events.append)
    zb = ZeroBucket(backend=backend)
    ids = [zb.put(_jpeg(c)) for c in ((1, 1, 1), (50, 60, 70), (90, 10, 10))]
    for image_id in ids:
        assert backend.tier_to_object_storage(image_id) is True
    events.clear()
    yield zb, backend, ids, boto3.client("s3", region_name="us-east-1"), s3_bucket
    backend.close()


def _object_count(s3, bucket) -> int:
    return s3.list_objects_v2(Bucket=bucket).get("KeyCount", 0)


def _failing_delete(backend, fail_times=None):
    """Make the backend's object deletes raise StorageError, optionally only
    the first `fail_times` calls (the rest go through to real S3)."""
    real = backend._object_storage.delete
    calls = {"n": 0}

    def delete(key):
        calls["n"] += 1
        if fail_times is None or calls["n"] <= fail_times:
            raise StorageError("boom: S3 is unavailable")
        return real(key)

    backend._object_storage.delete = delete


def _object_events(events):
    return [e for e in events if e.operation == "object_storage_delete"]


def test_delete_succeeds_when_object_cleanup_fails(tiered, events):
    zb, backend, ids, s3, bucket = tiered
    _failing_delete(backend)

    assert zb.delete(ids[0]) is True  # no exception: the delete itself worked
    assert zb.exists(ids[0]) is False  # and the image really is gone
    assert _object_count(s3, bucket) == 3  # the object is left behind as an orphan

    assert [e.success for e in events if e.operation == "delete"] == [True]
    failed = _object_events(events)
    assert len(failed) == 1
    assert failed[0].success is False
    assert "boom" in failed[0].error
    assert failed[0].retry_count == 0


def test_delete_many_survives_one_failed_object_and_cleans_the_rest(tiered, events):
    zb, backend, ids, s3, bucket = tiered
    _failing_delete(backend, fail_times=1)  # only the first object delete fails

    results = zb.delete_many(ids)
    assert [r.deleted for r in results] == [True, True, True]
    for image_id in ids:
        assert zb.exists(image_id) is False

    outcome = [e.success for e in _object_events(events)]
    assert sorted(outcome) == [False, True, True]  # 1 orphan, 2 cleaned
    assert _object_count(s3, bucket) == 1


def test_successful_cleanup_is_reported_too(tiered, events):
    zb, _, ids, s3, bucket = tiered
    assert zb.delete(ids[0]) is True
    assert _object_count(s3, bucket) == 2
    reported = _object_events(events)
    assert len(reported) == 1
    assert reported[0].success is True and reported[0].error is None


def test_a_real_s3_failure_is_swallowed_and_reported(tiered, events):
    """No injection: remove the bucket out from under the backend, so the
    cleanup fails for real (NoSuchBucket)."""
    zb, _, ids, s3, bucket = tiered
    for obj in s3.list_objects_v2(Bucket=bucket)["Contents"]:
        s3.delete_object(Bucket=bucket, Key=obj["Key"])
    s3.delete_bucket(Bucket=bucket)

    assert zb.delete(ids[1]) is True
    assert zb.exists(ids[1]) is False
    failed = _object_events(events)
    assert len(failed) == 1 and failed[0].success is False
    assert failed[0].error


def test_an_untiered_image_has_no_object_to_clean_up(target, s3_bucket, events):
    store = ObjectStorage(s3_bucket, region_name="us-east-1")
    backend = target.backend(object_storage=store, on_operation=events.append)
    try:
        zb = ZeroBucket(backend=backend)
        image_id = zb.put(_jpeg((7, 7, 7)))
        events.clear()
        assert zb.delete(image_id) is True
        assert _object_events(events) == []
    finally:
        backend.close()


def test_a_missing_id_reports_nothing(tiered, events):
    zb, _, _, _, _ = tiered
    assert zb.delete("00000000-0000-0000-0000-000000000000") is False
    assert _object_events(events) == []


def test_a_callback_that_raises_cannot_break_the_cleanup(tiered):
    zb, backend, ids, s3, bucket = tiered

    def bad_callback(event):
        raise RuntimeError("boom")

    backend._on_operation = bad_callback
    assert zb.delete(ids[0]) is True
    assert _object_count(s3, bucket) == 2  # cleanup still ran


def test_caller_owned_transaction_still_skips_cleanup_and_events(sqlite_connection_case):
    """With connection= the caller owns the transaction, so the object must
    stay until they commit; nothing is attempted and nothing is reported."""
    backend, events, image_id, s3, bucket = sqlite_connection_case
    conn = backend._connect()  # noqa: SLF001
    try:
        assert backend.delete(image_id, connection=conn) is True
        conn.commit()
    finally:
        conn.close()
    assert _object_events(events) == []
    assert _object_count(s3, bucket) == 1


@pytest.fixture
def sqlite_connection_case(s3_bucket, events):
    import os

    from zerobucket.adapters.sqlite import SQLiteBackend

    path = tempfile.mktemp(suffix=".db")
    store = ObjectStorage(s3_bucket, region_name="us-east-1")
    backend = SQLiteBackend(path, object_storage=store, on_operation=events.append)
    try:
        image_id = ZeroBucket(backend=backend).put(_jpeg((3, 3, 3)))
        assert backend.tier_to_object_storage(image_id) is True
        events.clear()
        yield backend, events, image_id, boto3.client("s3", region_name="us-east-1"), s3_bucket
    finally:
        with contextlib.suppress(Exception):
            backend.close()
        if os.path.exists(path):
            _remove_db_files(path)
