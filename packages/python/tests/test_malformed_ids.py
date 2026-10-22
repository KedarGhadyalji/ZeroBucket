"""Malformed ids are "not found" everywhere, and never reach a before_get hook.

An id that is not a UUID can never exist. Previously the PostgreSQL backends
raised StorageError("invalid input syntax for type uuid") for it on EVERY
operation (including exists() and delete()), and a before_get hook received the
raw string: a hook that used the id in SQL turned /images/garbage into a 500, a
deny-all hook into a 403. Now every backend answers "not found" / False without
a database round trip, and the hook only ever sees well-formed UUIDs. This is
the same contract as the npm package (0.9.0).
"""

from __future__ import annotations

import io
import os
import uuid

import psycopg
import pytest

from tests.conftest import TEST_DATABASE_URL
from zerobucket import AsyncZeroBucket, MySQLBackend, SQLiteBackend, ZeroBucket
from zerobucket._ids import is_valid_id
from zerobucket.exceptions import AccessDeniedError, ImageNotFoundError

MALFORMED = [
    "not-a-uuid",
    "",
    "' OR 1=1 --",
    "../../etc/passwd",
    "00000000-0000-4000-8000-00000000000",  # one character short
    "00000000-0000-4000-8000-0000000000000",  # one character long
    "g0000000-0000-4000-8000-000000000000",  # non-hex
    "00000000-0000-4000-8000-000000000000\n",  # trailing newline
]
WELL_FORMED_UNKNOWN = "00000000-0000-4000-8000-000000000000"


@pytest.fixture(params=["postgres", "postgres-dedup", "sqlite", "mysql"])
def make_zb(request, tmp_path):
    """Build a ZeroBucket on each backend; closes everything it made."""
    made: list[ZeroBucket] = []

    def make(**kwargs) -> ZeroBucket:
        if request.param == "postgres":
            zb = ZeroBucket(database_url=TEST_DATABASE_URL, **kwargs)
        elif request.param == "postgres-dedup":
            zb = ZeroBucket(database_url=TEST_DATABASE_URL, dedup=True, **kwargs)
        elif request.param == "sqlite":
            zb = ZeroBucket(backend=SQLiteBackend(str(tmp_path / "t.db")), **kwargs)
        else:
            url = os.environ.get("ZEROBUCKET_TEST_MYSQL_URL")
            if not url:
                pytest.skip("ZEROBUCKET_TEST_MYSQL_URL not set")
            zb = ZeroBucket(backend=MySQLBackend(url), **kwargs)
        made.append(zb)
        return zb

    yield make
    for zb in made:
        zb.close()


# ---- the helper itself ------------------------------------------------------


@pytest.mark.parametrize(
    "good",
    [
        "00000000-0000-4000-8000-000000000000",
        "123e4567-e89b-12d3-a456-426614174000",
        "123E4567-E89B-12D3-A456-426614174000",  # upper case: PostgreSQL's uuid is case-insensitive
        uuid.UUID(
            "123e4567-e89b-12d3-a456-426614174000"
        ),  # objects are valid by construction
    ],
)
def test_is_valid_id_accepts_uuids(good):
    assert is_valid_id(good) is True


@pytest.mark.parametrize(
    "bad",
    MALFORMED
    + [
        "{123e4567-e89b-12d3-a456-426614174000}",  # braces: Postgres accepts, ids never look so
        "123e4567e89b12d3a456426614174000",  # no hyphens
        " 123e4567-e89b-12d3-a456-426614174000",
        None,
        123,
        b"123e4567-e89b-12d3-a456-426614174000",
    ],
)
def test_is_valid_id_rejects_everything_else(bad):
    assert is_valid_id(bad) is False


def test_uuid_objects_still_work_end_to_end(jpeg_bytes):
    """REGRESSION guard: Django's <uuid:image_id> URL converter hands the client a uuid.UUID
    object, not a str, and psycopg accepts it. A str-only id check would 404 every image
    served through django-zerobucket."""
    zb = ZeroBucket(database_url=TEST_DATABASE_URL)
    try:
        image_id = zb.put(jpeg_bytes)
        as_object = uuid.UUID(image_id)
        assert zb.get(as_object).data == jpeg_bytes
        assert zb.metadata(as_object).size_bytes == len(jpeg_bytes)
        assert zb.exists(as_object) is True
        assert b"".join(zb.get_stream(as_object)) == jpeg_bytes
        assert zb.delete(as_object) is True
    finally:
        zb.close()


def test_a_hook_receives_a_uuid_object_unchanged(jpeg_bytes):
    seen: list[object] = []
    zb = ZeroBucket(
        database_url=TEST_DATABASE_URL,
        before_get=lambda image_id, context: (seen.append(image_id), True)[1],
    )
    try:
        image_id = zb.put(jpeg_bytes)
        as_object = uuid.UUID(image_id)
        zb.get(as_object)
        assert seen == [as_object]  # passed through as given, not stringified
        zb.delete(image_id)
    finally:
        zb.close()


# ---- every operation, every backend ----------------------------------------


def test_every_operation_treats_a_malformed_id_as_not_found(make_zb):
    zb = make_zb()
    for bad in MALFORMED:
        with pytest.raises(ImageNotFoundError):
            zb.get(bad)
        with pytest.raises(ImageNotFoundError):
            zb.metadata(bad)
        with pytest.raises(ImageNotFoundError):
            zb.get_stream(bad)
        with pytest.raises(ImageNotFoundError):
            zb.stream_to(bad, io.BytesIO())
        assert zb.exists(bad) is False
        assert zb.delete(bad) is False
    many = zb.get_many(MALFORMED)
    assert [(r.image_id, r.success, r.error) for r in many] == [
        (bad, False, "not found") for bad in MALFORMED
    ]
    gone = zb.delete_many(MALFORMED)
    assert [(r.image_id, r.deleted) for r in gone] == [
        (bad, False) for bad in MALFORMED
    ]


def test_one_malformed_id_does_not_fail_a_whole_batch(make_zb, jpeg_bytes):
    zb = make_zb()
    good = zb.put(jpeg_bytes)
    try:
        many = zb.get_many(["not-a-uuid", good])
        assert [(r.success, r.error) for r in many] == [
            (False, "not found"),
            (True, None),
        ]
        assert many[1].image.data == jpeg_bytes
        deleted = zb.delete_many(["not-a-uuid", good])
        assert [(r.image_id, r.deleted) for r in deleted] == [
            ("not-a-uuid", False),
            (good, True),
        ]
    finally:
        zb.delete(good)


# ---- the before_get hook never sees a malformed id --------------------------


def test_before_get_is_never_called_with_a_malformed_id(make_zb):
    seen: list[str] = []

    def hook(image_id, context):
        seen.append(image_id)
        return True

    zb = make_zb(before_get=hook)
    for bad in MALFORMED:
        for call in (
            lambda b=bad: zb.get(b),
            lambda b=bad: zb.metadata(b),
            lambda b=bad: zb.get_stream(b),
            lambda b=bad: zb.stream_to(b, io.BytesIO()),
        ):
            with pytest.raises(ImageNotFoundError):
                call()
    assert seen == []


def test_get_many_hook_only_sees_the_well_formed_ids(make_zb):
    seen: list[str] = []
    zb = make_zb(before_get=lambda image_id, context: (seen.append(image_id), True)[1])
    res = zb.get_many(["not-a-uuid", WELL_FORMED_UNKNOWN, "", "' OR 1=1 --"])
    assert [(r.image_id, r.success, r.error) for r in res] == [
        ("not-a-uuid", False, "not found"),
        (WELL_FORMED_UNKNOWN, False, "not found"),
        ("", False, "not found"),
        ("' OR 1=1 --", False, "not found"),
    ]
    assert seen == [WELL_FORMED_UNKNOWN]


def test_a_denying_or_throwing_hook_cannot_change_the_answer_for_a_malformed_id(
    make_zb,
):
    def boom(image_id, context):
        raise RuntimeError("hook blew up")

    for hook in (lambda i, c: False, boom):
        zb = make_zb(before_get=hook)
        with pytest.raises(ImageNotFoundError):
            zb.get("not-a-uuid")  # not AccessDeniedError, not RuntimeError


def test_well_formed_ids_are_unchanged_hook_still_called_and_denial_still_wins(make_zb):
    seen: list[str] = []
    zb = make_zb(before_get=lambda image_id, context: (seen.append(image_id), False)[1])
    upper = WELL_FORMED_UNKNOWN.replace("0", "A")
    with pytest.raises(AccessDeniedError):
        zb.get(WELL_FORMED_UNKNOWN)
    with pytest.raises(AccessDeniedError):
        zb.get(upper)
    assert seen == [WELL_FORMED_UNKNOWN, upper]
    assert zb.get_many([WELL_FORMED_UNKNOWN])[0].error == "access denied"


def test_the_typical_sql_hook_no_longer_turns_garbage_into_a_database_error():
    """A hook that casts the id to a uuid raises on junk. It must never see any."""
    with psycopg.connect(TEST_DATABASE_URL, autocommit=True) as conn:

        def hook(image_id, context):
            return conn.execute("SELECT %s::uuid", (image_id,)).fetchone() is not None

        zb = ZeroBucket(database_url=TEST_DATABASE_URL, before_get=hook)
        try:
            for bad in MALFORMED:
                with pytest.raises(ImageNotFoundError):
                    zb.get(bad)
        finally:
            zb.close()


# ---- tiering and the async client -------------------------------------------


def test_tiering_a_malformed_id_is_not_found(tiered_images):
    with pytest.raises(ImageNotFoundError):
        tiered_images.tier_to_object_storage("not-a-uuid")


async def test_async_postgres_client_treats_malformed_ids_as_not_found(jpeg_bytes):
    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL)
    try:
        for bad in MALFORMED:
            with pytest.raises(ImageNotFoundError):
                await zb.get(bad)
            with pytest.raises(ImageNotFoundError):
                await zb.metadata(bad)
            with pytest.raises(ImageNotFoundError):
                await zb.get_stream(bad)
            assert await zb.exists(bad) is False
            assert await zb.delete(bad) is False
        good = await zb.put(jpeg_bytes)
        try:
            many = await zb.get_many(["not-a-uuid", good])
            assert [(r.success, r.error) for r in many] == [
                (False, "not found"),
                (True, None),
            ]
        finally:
            await zb.delete(good)
        gone = await zb.delete_many(MALFORMED)
        assert [(r.image_id, r.deleted) for r in gone] == [
            (b, False) for b in MALFORMED
        ]
    finally:
        await zb.close()
