"""ZeroBucketStorage: a Django Storage backend adapter, plugging
ZeroBucket into Django's existing FileField/ImageField via the
STORAGES setting -- no new field type to learn, works with existing
forms/admin/migrations the same way django-storages' S3/GCS backends do
for their targets.

    # settings.py
    STORAGES = {
        "default": {
            "BACKEND": "django_zerobucket.storage.ZeroBucketStorage",
            "OPTIONS": {"database_url": "postgresql://..."},
        },
    }

    # models.py
    class Product(models.Model):
        photo = models.ImageField(upload_to="ignored")  # see below

A DELIBERATE, HONEST DIFFERENCE from typical Django Storage backends,
worth understanding before adopting this: the `name` your FileField/
ImageField stores in its database column is the ZeroBucket image_id (a
UUID), NOT a filesystem-style path built from `upload_to=`. ZeroBucket
is id-addressed storage, not path-addressed storage -- there's no
"directory" for an uploaded file to live in, so `upload_to=` is
effectively ignored (Django still requires the kwarg be present on the
field, but this backend never uses it to build the stored name). If
your code reads `instance.photo.name` expecting something like
"uploads/2024/photo.jpg", it will instead see a UUID. `instance.photo.url`
and `instance.photo.open()` work exactly like any other Storage backend
regardless -- this only affects code that inspects the raw name string.
"""

from __future__ import annotations

from typing import Any

from django.conf import settings
from django.core.files import File
from django.core.files.base import ContentFile
from django.core.files.storage import Storage
from django.urls import reverse
from django.utils.deconstruct import deconstructible
from zerobucket import ZeroBucket

_client_cache: dict[tuple, ZeroBucket] = {}


def _get_shared_client(database_url: str, options: dict) -> ZeroBucket:
    """ZeroBucket instances hold a real connection pool -- constructing
    a new one per Storage instantiation (which Django's own storage
    registry mostly avoids via its own caching, but nothing guarantees
    every caller goes through that registry) would mean leaking pools.
    Cached here, keyed on (database_url, frozen options), so repeated
    ZeroBucketStorage(**same_options) calls -- from Django's registry or
    otherwise, including the serving view constructing its own instance
    (see views.py) -- share one underlying client/pool rather than each
    opening a separate one. Not thread-safety-hardened beyond Python
    dict operations already being atomic under the GIL -- a documented,
    accepted simplicity tradeoff, not a claimed guarantee under free-
    threaded Python.
    """
    key = (database_url, tuple(sorted(options.items())))
    if key not in _client_cache:
        _client_cache[key] = ZeroBucket(database_url=database_url, **options)
    return _client_cache[key]


@deconstructible
class ZeroBucketStorage(Storage):
    """See module docstring for the id-vs-path naming difference from
    typical Storage backends before using this.

    OPTIONS (all optional except database_url, which can instead come
    from the ZEROBUCKET_DATABASE_URL Django setting):

        database_url: falls back to settings.ZEROBUCKET_DATABASE_URL if
            omitted. Required one way or the other.
        Any other keyword ZeroBucket's own constructor accepts
            (max_bytes, max_pixels, allowed_formats, dedup,
            pool_min_size, pool_max_size, pool_timeout, object_storage,
            before_get, before_put, on_operation) is forwarded as-is --
            this class does not re-document or restrict them.

    @deconstructible is required for Django migrations to be able to
    serialize a FileField(storage=ZeroBucketStorage(...)) reference --
    without it, `makemigrations` fails on this class specifically. Not
    needed if you only ever configure this via the STORAGES setting
    (Django constructs the instance itself in that case), but included
    unconditionally since supporting the direct storage=... kwarg too
    costs nothing extra.
    """

    def __init__(self, database_url: str | None = None, **options: Any) -> None:
        self._database_url = database_url or getattr(
            settings, "ZEROBUCKET_DATABASE_URL", None
        )
        if not self._database_url:
            raise ValueError(
                "ZeroBucketStorage requires database_url (via STORAGES' "
                "OPTIONS) or the ZEROBUCKET_DATABASE_URL Django setting -- "
                "neither was provided."
            )
        self._options = options
        self._client = _get_shared_client(self._database_url, options)

    # ---- the required Storage overrides ------------------------------

    def _save(self, name: str, content) -> str:
        """Ignores `name` as a target location (see module docstring) --
        ZeroBucket always generates its own id via put(), which becomes
        the returned name. `name` IS still passed through as the
        `filename=` ZeroBucket stores in its own metadata (so
        `image.filename`/ImageMetadata.filename still reflects the
        original upload name, even though the Django-visible `name`
        doesn't)."""
        if not hasattr(content, "read"):
            content = File(content)
        data = content.read()
        return self._client.put(data, filename=name)

    def _open(self, name: str, mode: str = "rb"):
        image = self._client.get(name)
        return ContentFile(image.data, name=name)

    def get_available_name(self, name: str, max_length: int | None = None) -> str:
        """Overridden to skip Django's default collision-avoidance loop
        entirely (which calls exists() repeatedly, appending suffixes
        until free) -- meaningless here, since _save() above ignores
        whatever name is passed in and always gets a fresh id from
        put() regardless. Returning `name` unchanged avoids a wasted
        exists() round trip for a check that could never fail to be
        "available" in any way that matters."""
        return name

    def exists(self, name: str) -> bool:
        return self._client.exists(name)

    def delete(self, name: str) -> None:
        self._client.delete(name)

    def size(self, name: str) -> int:
        """Uses metadata() rather than get() -- doesn't pull the
        (potentially large) image bytes just to answer "how big is
        this," the same efficiency reasoning as everywhere else in
        ZeroBucket that offers a metadata-only path."""
        return self._client.metadata(name).size_bytes

    def url(self, name: str) -> str:
        """Reverses to this package's own serving view (see views.py) --
        requires `django_zerobucket.urls` to be included in your
        project's urlconf (see that module's docstring), or this raises
        Django's normal NoReverseMatch. Override this method in a
        subclass if you want images served some other way instead (e.g.
        a CDN in front of tiered S3 objects, or S3 presigned URLs
        generated directly for images you've tiered via
        tier_to_object_storage() -- this class has no special knowledge
        of tiering one way or the other, since transparent reads mean
        it doesn't need any)."""
        return reverse("django_zerobucket:serve", args=[name])

    # ---- niceties, not required by Storage but cheap to provide -------

    def get_accessed_time(self, name: str):
        raise NotImplementedError("ZeroBucket does not track last-accessed time.")

    def get_created_time(self, name: str):
        # ImageMetadata doesn't currently expose created_at -- documented
        # gap, not silently wrong. Raising (Django's own convention for
        # "this storage doesn't support this") rather than returning a
        # fabricated value.
        raise NotImplementedError(
            "ZeroBucket's metadata() does not currently expose created_at."
        )

    def get_modified_time(self, name: str):
        raise NotImplementedError(
            "ZeroBucket's metadata() does not currently expose an "
            "updated-at timestamp through the client API."
        )
