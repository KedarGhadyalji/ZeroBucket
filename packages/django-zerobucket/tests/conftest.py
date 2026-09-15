"""Fixtures for django-zerobucket's test suite.

Requires a real Postgres instance for BOTH roles simultaneously (see
django_settings.py's module docstring): Django's own DATABASES
('zerobucket_test' via the 'default' Django DB alias) and ZeroBucket's
own storage (ZEROBUCKET_DATABASE_URL, same physical server in this test
setup, separate table). pytest-django's `db`/`django_db` fixture
handles the former; the `zb_client` fixture below handles the latter,
truncating between tests the same way the main zerobucket package's own
test suite does.
"""

from __future__ import annotations

import io

import pytest
from PIL import Image as PILImage

from django_zerobucket.storage import _client_cache
from tests.django_settings import ZEROBUCKET_DATABASE_URL


@pytest.fixture(autouse=True)
def _clear_client_cache():
    """The shared-client cache (see storage.py) is module-level global
    state -- without clearing it between tests, a ZeroBucketStorage
    constructed in one test would silently reuse a client (and its
    already-open connection pool) left over from a previous test.
    Cleared before AND after each test so neither direction leaks."""
    _client_cache.clear()
    yield
    for client in _client_cache.values():
        client.close()
    _client_cache.clear()


@pytest.fixture
def zb_client(_clear_client_cache):
    """A raw ZeroBucket client (not going through Django at all) for
    setup/assertions -- truncates the shared image table before and
    hands back a client tests can use to inspect what
    ZeroBucketStorage/the view actually did, independent of the Django
    layer under test."""
    from zerobucket import ZeroBucket

    client = ZeroBucket(database_url=ZEROBUCKET_DATABASE_URL)
    with client._backend._pool.connection() as conn, conn.cursor() as cur:  # noqa: SLF001
        cur.execute("TRUNCATE TABLE zerobucket_images;")
    yield client
    client.close()


@pytest.fixture
def jpeg_bytes() -> bytes:
    img = PILImage.new("RGB", (120, 80), color=(30, 60, 90))
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()
