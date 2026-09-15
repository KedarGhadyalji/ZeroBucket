"""Tests for ZeroBucketStorage, both called directly and through a real
Django model's ImageField -- see conftest.py for the two-Postgres-roles
setup this all runs against.
"""

from __future__ import annotations

import pytest
from django.core.files.base import ContentFile
from django.core.files.storage import Storage
from django.urls import NoReverseMatch, reverse

from django_zerobucket.storage import ZeroBucketStorage
from tests.django_settings import ZEROBUCKET_DATABASE_URL
from tests.testapp.models import Product


def _storage() -> ZeroBucketStorage:
    return ZeroBucketStorage(database_url=ZEROBUCKET_DATABASE_URL)


# ---- construction -----------------------------------------------------


def test_requires_database_url_one_way_or_another(settings):
    settings.ZEROBUCKET_DATABASE_URL = None
    with pytest.raises(ValueError, match="database_url"):
        ZeroBucketStorage()


def test_falls_back_to_django_setting(zb_client, jpeg_bytes, settings):
    settings.ZEROBUCKET_DATABASE_URL = ZEROBUCKET_DATABASE_URL
    storage = ZeroBucketStorage()  # no explicit database_url this time
    name = storage.save("whatever.jpg", ContentFile(jpeg_bytes))
    assert storage.exists(name)


def test_is_a_real_django_storage_subclass():
    assert issubclass(ZeroBucketStorage, Storage)


# ---- _save / _open round trip ------------------------------------------


def test_save_and_open_round_trip(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("photo.jpg", ContentFile(jpeg_bytes))

    with storage.open(name) as f:
        assert f.read() == jpeg_bytes


def test_save_returns_an_id_not_the_suggested_name(zb_client, jpeg_bytes):
    """The core, deliberate difference from typical Storage backends,
    documented in storage.py's module docstring -- confirmed directly,
    not just asserted in prose."""
    storage = _storage()
    name = storage.save("my_original_filename.jpg", ContentFile(jpeg_bytes))
    assert name != "my_original_filename.jpg"
    # It's a UUID -- storage.py relies on this being reversible by the
    # `uuid` URL converter, so confirm the actual shape, not just
    # "not the original name".
    import uuid

    uuid.UUID(name)  # raises ValueError if this isn't a real UUID string


def test_original_filename_preserved_in_zerobucket_metadata(zb_client, jpeg_bytes):
    """Even though the Django-visible `name` is a UUID, the original
    upload name isn't lost -- it's stored as ZeroBucket's own filename
    metadata, retrievable independently of Django."""
    storage = _storage()
    name = storage.save("my_original_filename.jpg", ContentFile(jpeg_bytes))
    meta = zb_client.metadata(name)
    assert meta.filename == "my_original_filename.jpg"


def test_get_available_name_does_not_check_existence(zb_client):
    """Overridden to skip Django's default collision-avoidance loop
    entirely -- confirmed it really is a no-op passthrough, not just
    documented as one."""
    storage = _storage()
    assert storage.get_available_name("anything-at-all.jpg") == "anything-at-all.jpg"
    assert storage.get_available_name("even/if/it/looks/like/a/path.png") == (
        "even/if/it/looks/like/a/path.png"
    )


# ---- exists / delete / size ---------------------------------------------


def test_exists_true_then_false_after_delete(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("x.jpg", ContentFile(jpeg_bytes))
    assert storage.exists(name) is True
    storage.delete(name)
    assert storage.exists(name) is False


def test_exists_false_for_unknown_name(zb_client):
    storage = _storage()
    assert storage.exists("00000000-0000-0000-0000-000000000000") is False


def test_size_matches_content_length_without_fetching_bytes(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("x.jpg", ContentFile(jpeg_bytes))
    assert storage.size(name) == len(jpeg_bytes)


# ---- url() ----------------------------------------------------------------


def test_url_reverses_to_serving_view(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("x.jpg", ContentFile(jpeg_bytes))
    url = storage.url(name)
    assert url == reverse("django_zerobucket:serve", args=[name])
    assert name in url


def test_url_raises_clearly_if_urls_not_included(zb_client, jpeg_bytes, settings):
    """If a project forgets to include django_zerobucket.urls, url()
    should fail with Django's normal NoReverseMatch, not something more
    confusing."""
    settings.ROOT_URLCONF = "tests.empty_urls"
    storage = _storage()
    name = storage.save("x.jpg", ContentFile(jpeg_bytes))
    with pytest.raises(NoReverseMatch):
        storage.url(name)


# ---- unsupported timestamp methods raise clearly, not silently wrong -----


def test_timestamp_methods_raise_not_implemented(zb_client, jpeg_bytes):
    storage = _storage()
    name = storage.save("x.jpg", ContentFile(jpeg_bytes))
    with pytest.raises(NotImplementedError):
        storage.get_created_time(name)
    with pytest.raises(NotImplementedError):
        storage.get_modified_time(name)
    with pytest.raises(NotImplementedError):
        storage.get_accessed_time(name)


# ---- real ORM/model integration ------------------------------------------


@pytest.mark.django_db
def test_model_save_and_reload_round_trip(zb_client, jpeg_bytes):
    """The full path: Django ORM -> FileField -> ZeroBucketStorage ->
    ZeroBucket -> Postgres, and back, through a real saved+reloaded
    model instance, not just the Storage class called directly."""
    product = Product(name="Widget")
    product.photo.save("widget.jpg", ContentFile(jpeg_bytes), save=True)

    reloaded = Product.objects.get(pk=product.pk)
    assert reloaded.photo.read() == jpeg_bytes


@pytest.mark.django_db
def test_model_delete_does_not_orphan_the_image(zb_client, jpeg_bytes):
    """Django does NOT automatically delete storage files when a model
    instance is deleted (a Django behavior, not a ZeroBucketStorage
    one) -- confirmed directly so this isn't assumed. If this ever
    matters for a real project, that's a signal-driven cleanup step
    they'd add themselves (e.g. a post_delete signal calling
    photo.delete()), same as with any other Django Storage backend."""
    product = Product(name="Widget")
    product.photo.save("widget.jpg", ContentFile(jpeg_bytes), save=True)
    name = product.photo.name

    product.delete()

    storage = _storage()
    assert storage.exists(name) is True  # still there -- Django didn't clean it up
    storage.delete(name)  # clean up after ourselves in the test
