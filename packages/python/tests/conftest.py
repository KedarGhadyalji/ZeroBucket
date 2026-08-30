"""Shared pytest fixtures.

Integration tests require a real PostgreSQL instance. Set
ZEROBUCKET_TEST_DATABASE_URL to point at a throwaway database, e.g.:

    export ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/zerobucket_test

Tests truncate the zerobucket_images table between runs rather than
dropping the database, so they're safe to run repeatedly.
"""

from __future__ import annotations

import io
import os

import pytest
from PIL import Image as PILImage

from zerobucket import ZeroBucket
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
