"""Tests for AsyncServeImageView and the urls_async route.

Under ASGI they go through Django's AsyncClient (a real ASGIRequest and the ASGI response path).
Under WSGI they go through the ordinary test Client, where the view must NOT create an async
connection pool. See views.py's module docstring.
"""

from __future__ import annotations

import io
import os
import warnings

import pytest
from django.core.files.base import ContentFile
from django.test import AsyncClient, AsyncRequestFactory, Client
from PIL import Image as PILImage
from zerobucket import AsyncZeroBucket, ImageNotFoundError
from zerobucket.adapters.postgres import DEFAULT_STREAM_CHUNK_SIZE

from django_zerobucket import storage as storage_module
from django_zerobucket.storage import ZeroBucketStorage
from django_zerobucket.views import AsyncServeImageView
from tests.django_settings import ZEROBUCKET_DATABASE_URL

MISSING = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(autouse=True)
def _use_async_urls(settings):
    settings.ROOT_URLCONF = "tests.urls_async"


async def _body(response) -> bytes:
    return b"".join([chunk async for chunk in response.streaming_content])


def _save(name: str, data: bytes) -> str:
    return ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).save(
        name, ContentFile(data)
    )


# ---- under ASGI -----------------------------------------------------------


async def test_serves_the_right_bytes_and_headers_over_asgi(
    zb_client, jpeg_bytes, monkeypatch
):
    name = _save("x.jpg", jpeg_bytes)

    def must_not_be_used(self, request, image_id):
        raise AssertionError("the WSGI fallback was used under ASGI")

    monkeypatch.setattr(AsyncServeImageView, "_get_sync", must_not_be_used)
    try:
        response = await AsyncClient().get(f"/zerobucket-images/{name}/")
        assert response.status_code == 200
        assert response["Content-Type"] == "image/jpeg"
        assert response["Content-Length"] == str(len(jpeg_bytes))
        assert await _body(response) == jpeg_bytes
        assert (
            len(storage_module._async_client_cache) == 1
        )  # the async client did the work
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_content_disposition_carries_the_original_filename(zb_client, jpeg_bytes):
    name = _save("my_photo.jpg", jpeg_bytes)
    try:
        response = await AsyncClient().get(f"/zerobucket-images/{name}/")
        assert response["Content-Disposition"] == 'inline; filename="my_photo.jpg"'
        await _body(response)
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_a_filename_with_a_quote_or_non_ascii_characters_gives_a_valid_header(
    zb_client, jpeg_bytes
):
    quoted = _save('a"b.jpg', jpeg_bytes)
    accented = _save("café.jpg", jpeg_bytes)
    try:
        r1 = await AsyncClient().get(f"/zerobucket-images/{quoted}/")
        r2 = await AsyncClient().get(f"/zerobucket-images/{accented}/")
        assert r1["Content-Disposition"] == 'inline; filename="a\\"b.jpg"'
        assert r2["Content-Disposition"] == "inline; filename*=utf-8''caf%C3%A9.jpg"
        await _body(r1)
        await _body(r2)
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_404_for_a_missing_image(zb_client):
    try:
        response = await AsyncClient().get(f"/zerobucket-images/{MISSING}/")
        assert response.status_code == 404
        assert "Content-Disposition" not in response
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_a_malformed_id_never_reaches_the_view(zb_client):
    response = await AsyncClient().get("/zerobucket-images/not-a-uuid-at-all/")
    assert response.status_code == 404
    assert not storage_module._async_client_cache


async def test_an_image_deleted_between_the_two_calls_is_a_404_not_a_500(
    zb_client, jpeg_bytes, monkeypatch
):
    name = _save("x.jpg", jpeg_bytes)

    async def vanished(self, image_id, **kwargs):
        raise ImageNotFoundError(image_id)

    monkeypatch.setattr(AsyncZeroBucket, "get_stream", vanished)
    try:
        response = await AsyncClient().get(f"/zerobucket-images/{name}/")
        assert response.status_code == 404
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_an_image_larger_than_one_chunk_streams_in_several_chunks(zb_client):
    """More than one chunk, so the async iterator is really consumed by the response."""
    side = 700
    img = PILImage.frombytes("RGB", (side, side), os.urandom(side * side * 3))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()
    assert len(data) > DEFAULT_STREAM_CHUNK_SIZE
    name = _save("big.png", data)
    try:
        response = await AsyncClient().get(f"/zerobucket-images/{name}/")
        chunks = [chunk async for chunk in response.streaming_content]
        assert len(chunks) > 1
        assert b"".join(chunks) == data
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_url_of_a_saved_image_resolves_to_the_async_route(zb_client, jpeg_bytes):
    name = _save("x.jpg", jpeg_bytes)
    url = ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).url(name)
    assert url == f"/zerobucket-images/{name}/"
    try:
        response = await AsyncClient().get(url)
        assert response.status_code == 200
        await _body(response)
    finally:
        await ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL).aclose()


async def test_a_subclass_can_point_the_view_at_a_differently_configured_storage(
    zb_client, jpeg_bytes
):
    class Capped(AsyncServeImageView):
        storage_class = staticmethod(
            lambda: ZeroBucketStorage(
                database_url=ZEROBUCKET_DATABASE_URL, max_bytes=5_000_000
            )
        )

    name = _save("x.jpg", jpeg_bytes)
    request = AsyncRequestFactory().get("/")
    try:
        response = await Capped.as_view()(request, image_id=name)
        assert response.status_code == 200
        assert b"".join([c async for c in response.streaming_content]) == jpeg_bytes
    finally:
        await Capped().storage.aclose()


# ---- under WSGI -------------------------------------------------------------


def test_under_wsgi_it_serves_with_the_sync_client_and_creates_no_async_pool(
    zb_client, jpeg_bytes
):
    name = _save("x.jpg", jpeg_bytes)
    response = Client().get(f"/zerobucket-images/{name}/")
    assert response.status_code == 200
    assert response["Content-Type"] == "image/jpeg"
    assert response["Content-Length"] == str(len(jpeg_bytes))
    assert b"".join(response.streaming_content) == jpeg_bytes
    assert not storage_module._async_client_cache


def test_under_wsgi_a_missing_image_is_a_404_and_still_no_async_pool(zb_client):
    response = Client().get(f"/zerobucket-images/{MISSING}/")
    assert response.status_code == 404
    assert not storage_module._async_client_cache


def test_repeated_wsgi_requests_never_warn_about_throwaway_loops(zb_client, jpeg_bytes):
    name = _save("x.jpg", jpeg_bytes)
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        for _ in range(3):
            response = Client().get(f"/zerobucket-images/{name}/")
            assert response.status_code == 200
            b"".join(response.streaming_content)
