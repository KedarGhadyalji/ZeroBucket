"""Tests for ServeImageView, using Django's real test client -- through
actual HTTP request/response machinery, not calling the view function
directly."""

from __future__ import annotations

import pytest
from django.core.files.base import ContentFile
from django.test import Client

from django_zerobucket.storage import ZeroBucketStorage
from tests.django_settings import ZEROBUCKET_DATABASE_URL


def _storage() -> ZeroBucketStorage:
    return ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)


def test_serves_correct_bytes_and_content_type(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("x.jpg", ContentFile(jpeg_bytes))

    response = Client().get(f"/zerobucket-images/{name}/")
    assert response.status_code == 200
    assert response["Content-Type"] == "image/jpeg"
    assert response["Content-Length"] == str(len(jpeg_bytes))
    assert b"".join(response.streaming_content) == jpeg_bytes


def test_content_disposition_includes_original_filename(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("my_photo.jpg", ContentFile(jpeg_bytes))

    response = Client().get(f"/zerobucket-images/{name}/")
    assert 'filename="my_photo.jpg"' in response["Content-Disposition"]


def test_404_for_missing_image(zb_client):
    response = Client().get("/zerobucket-images/00000000-0000-0000-0000-000000000000/")
    assert response.status_code == 404


def test_404_response_has_no_content_disposition_header(zb_client):
    """Sanity check that the 404 path doesn't accidentally try to read
    metadata that doesn't exist and blow up with a 500 instead."""
    response = Client().get("/zerobucket-images/00000000-0000-0000-0000-000000000000/")
    assert "Content-Disposition" not in response


def test_malformed_id_does_not_reach_the_view(zb_client):
    """The <uuid:image_id> URL converter should 404 a non-UUID path
    segment before the view (and therefore before any database query)
    ever runs -- confirmed by using something that clearly isn't a
    UUID."""
    response = Client().get("/zerobucket-images/not-a-uuid-at-all/")
    assert response.status_code == 404


@pytest.mark.django_db
def test_model_photo_url_is_actually_servable(zb_client, jpeg_bytes):
    """The full loop: model -> ImageField -> ZeroBucketStorage.url() ->
    ServeImageView, through Django's real URL resolution end to end."""
    from tests.testapp.models import Product

    product = Product(name="Widget")
    product.photo.save("widget.jpg", ContentFile(jpeg_bytes), save=True)

    response = Client().get(product.photo.url)
    assert response.status_code == 200
    assert b"".join(response.streaming_content) == jpeg_bytes
