"""Shared pytest fixtures.

Integration tests require a real PostgreSQL instance. Set
ZEROBUCKET_TEST_DATABASE_URL to point at a throwaway database, e.g.:

    export ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/zerobucket_test

Tests truncate the zerobucket_images table between runs rather than
dropping the database, so they're safe to run repeatedly.

WINDOWS: psycopg3's async mode cannot run under the default
ProactorEventLoop, only a SelectorEventLoop -- a documented psycopg3
limitation (verified directly against psycopg's own source, not
assumed), not something specific to this project. Without the policy
set below, every async test would fail with a confusing ~10-second
PoolTimeout that buries the real cause (see
adapters/postgres_async.py's _windows_proactor_loop_error() for the
full story of how that masking happens). This has to be set at import
time, before pytest-asyncio creates its event loop for the session --
setting it inside a fixture would be too late.
"""

from __future__ import annotations

import io
import os
import sys

import pytest
import pytest_asyncio
from PIL import Image as PILImage

if sys.platform == "win32":
    import asyncio

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from zerobucket import AsyncZeroBucket, ZeroBucket
from zerobucket.adapters.postgres import PostgresBackend

TEST_DATABASE_URL = os.environ.get(
    "ZEROBUCKET_TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/zerobucket_test",
)


def _make_image_bytes(*, size=(64, 48), color=(255, 0, 0), fmt="JPEG") -> bytes:
    img = PILImage.new("RGB", size, color=color)
    buf = io.BytesIO()
    img.save(buf, format=fmt)
    return buf.getvalue()


@pytest.fixture(scope="session")
def _db_available():
    """Skip all integration tests cleanly if no test database is reachable."""
    try:
        backend = PostgresBackend(TEST_DATABASE_URL)
        backend.close()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"No reachable test database ({TEST_DATABASE_URL}): {exc}")


@pytest.fixture
def images(_db_available):
    """A ZeroBucket instance backed by a clean test table for each test."""
    zb = ZeroBucket(database_url=TEST_DATABASE_URL)
    # Ensure a clean slate per test rather than per session.
    with zb._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute("TRUNCATE TABLE zerobucket_images;")
    yield zb
    zb.close()


@pytest.fixture
def s3_bucket():
    """A moto-mocked S3 bucket, in-process. Uses moto's `mock_aws()`
    context manager (patches botocore's HTTP layer directly), NOT a
    standalone `moto_server` subprocess -- a standalone server was tried
    first and proved unreliable to background/tear down cleanly in this
    project's sandbox (backgrounded processes didn't reliably survive
    between tool invocations during development); `mock_aws()` is also
    simply the standard, widely-used way most projects test boto3-based
    code, not a compromise made only for this environment. Yields the
    bucket name; the bucket is created (moto doesn't require this
    up-front the way real S3 workflows do, but ObjectStorage's own
    docstring is explicit that it does NOT create buckets itself, so
    tests shouldn't rely on that either)."""
    from moto import mock_aws

    with mock_aws():
        import boto3

        bucket = "zerobucket-test-tier"
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=bucket)
        yield bucket


@pytest.fixture
def object_store(s3_bucket):
    """A real ObjectStorage instance pointed at the moto-mocked bucket."""
    from zerobucket.object_storage import ObjectStorage

    return ObjectStorage(s3_bucket, region_name="us-east-1")


@pytest.fixture
def tiered_images(_db_available, object_store):
    """A ZeroBucket instance constructed WITH object_storage=... --
    tiering is actually usable from this instance, unlike the plain
    `images` fixture. Independently truncates the shared
    zerobucket_images table (same table classic mode always uses) rather
    than assuming `images`' truncate already ran -- keeps this fixture
    correct standalone, regardless of fixture ordering in any given
    test."""
    zb = ZeroBucket(database_url=TEST_DATABASE_URL, object_storage=object_store)
    with zb._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute("TRUNCATE TABLE zerobucket_images;")
    yield zb
    zb.close()


@pytest.fixture
def jpeg_bytes() -> bytes:
    return _make_image_bytes(fmt="JPEG")


@pytest.fixture
def png_bytes() -> bytes:
    return _make_image_bytes(fmt="PNG")


@pytest.fixture
def webp_bytes() -> bytes:
    return _make_image_bytes(fmt="WEBP")


@pytest.fixture
def make_image_bytes():
    """Factory fixture for tests that need custom size/color/format."""
    return _make_image_bytes


@pytest.fixture
def db_connection_factory(_db_available):
    """Factory for a raw psycopg connection to the test database --
    used by tests that need their OWN connection (transaction tests,
    connection= tests) separate from anything ZeroBucket's `images`
    fixture manages internally."""
    import psycopg

    return lambda: psycopg.connect(TEST_DATABASE_URL)


@pytest.fixture
def dedup_images(_db_available):
    """A ZeroBucket instance with dedup=True, backed by clean dedup
    tables (zerobucket_blobs / zerobucket_image_refs) for each test.
    Separate tables from the `images` fixture's classic-mode table --
    see postgres.py's module docstring for why that's a deliberate
    design choice."""
    zb = ZeroBucket(database_url=TEST_DATABASE_URL, dedup=True)
    with zb._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        # Must truncate both together in one statement -- Postgres
        # refuses to truncate a referenced table alone (even if empty)
        # while a foreign key constraint against it exists, unless done
        # jointly or with CASCADE. Discovered by running this for real,
        # not assumed.
        cur.execute("TRUNCATE TABLE zerobucket_image_refs, zerobucket_blobs;")
    yield zb
    zb.close()


@pytest_asyncio.fixture
async def async_images(_db_available):
    """An AsyncZeroBucket instance backed by a clean test table for each
    test. Truncation itself uses a plain SYNC psycopg connection (not
    async) -- there's no async-specific behavior being tested by the
    truncate step itself, and it's simpler to reuse the same sync
    connect-and-truncate pattern the `images` fixture already uses than
    to open a second async connection just for setup."""
    import psycopg

    zb = AsyncZeroBucket(database_url=TEST_DATABASE_URL)
    # Trigger lazy pool-open + migration before the test body runs, so
    # every test starts from a known-ready state rather than each test
    # separately paying (and potentially racing on) first-call setup.
    await zb.exists("00000000-0000-0000-0000-000000000000")
    with psycopg.connect(TEST_DATABASE_URL) as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE zerobucket_images;")
    yield zb
    await zb.close()
