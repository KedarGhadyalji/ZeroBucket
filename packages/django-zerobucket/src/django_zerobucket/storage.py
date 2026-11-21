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

ASYNC (0.2.0): besides the usual synchronous Storage API, ZeroBucketStorage has
async counterparts built on the core library's AsyncZeroBucket --

    name = await storage.asave("photo.jpg", content)
    f = await storage.aopen(name)
    await storage.aexists(name)
    await storage.asize(name)
    await storage.adelete(name)

(`asave`, `aopen`, `aexists`, `asize`, `adelete`; Django's own Storage has no
async API, so these names are this package's.) They use an async connection
pool, and a psycopg async pool belongs to the event loop it was first used on
and cannot move to another. So the async client is cached PER EVENT LOOP, not
globally like the sync one. That is exactly right under ASGI (one loop for the
life of the process: one pool). It is wasteful under WSGI, where Django runs
each async call in a throwaway loop of its own: the first time this module sees
a new loop after an earlier one was closed without `aclose()`, it emits a
RuntimeWarning that says so. See `aclose()` and `aclose_async_clients()`.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
import warnings
from typing import Any

from asgiref.sync import async_to_sync
from django.conf import settings
from django.core.files import File
from django.core.files.base import ContentFile
from django.core.files.storage import Storage
from django.core.files.utils import validate_file_name
from django.urls import reverse
from django.utils.deconstruct import deconstructible
from zerobucket import AsyncZeroBucket, ZeroBucket

_client_cache: dict[tuple, ZeroBucket] = {}


def _sync_options(options: dict) -> dict:
    """The options for the SYNC client. One OPTIONS dict configures both clients, and the async
    client accepts `async def` access-control hooks (before_get / before_put). The sync client
    would call such a hook, get a coroutine object back, and treat that as True: every read and
    write allowed, silently. So an async hook is run through async_to_sync for the sync client,
    and both clients enforce the same rule."""
    out = dict(options)
    for name in ("before_get", "before_put"):
        hook = out.get(name)
        if hook is not None and inspect.iscoroutinefunction(hook):
            out[name] = async_to_sync(hook)
    return out


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
        _client_cache[key] = ZeroBucket(
            database_url=database_url, **_sync_options(options)
        )
    return _client_cache[key]


# Async clients, one per (event loop, database_url, options). The loop itself is kept in the entry
# (not just id(loop)) so a recycled id can never hand one loop another loop's pool, and so closed
# loops can be found and dropped. Guarded by a lock: unlike the sync cache above, entries are
# added, looked up and pruned from whichever thread a loop happens to run on.
_async_client_cache: dict[tuple, tuple[asyncio.AbstractEventLoop, AsyncZeroBucket]] = {}
_async_cache_lock = threading.Lock()
_warned_about_throwaway_loops = False


def _prune_closed_loops() -> bool:
    """Drops cache entries whose event loop has been closed; returns whether there were any.

    Their pools cannot be closed any more (closing one is a coroutine, and its loop is gone), so
    they are only released for garbage collection. Caller holds `_async_cache_lock`."""
    stale = [
        key for key, (loop, _client) in _async_client_cache.items() if loop.is_closed()
    ]
    for key in stale:
        del _async_client_cache[key]
    return bool(stale)


def _get_async_client(database_url: str, options: dict) -> AsyncZeroBucket:
    """The AsyncZeroBucket for the RUNNING event loop (raises RuntimeError outside one)."""
    global _warned_about_throwaway_loops
    loop = asyncio.get_running_loop()
    key = (id(loop), database_url, tuple(sorted(options.items())))
    with _async_cache_lock:
        entry = _async_client_cache.get(key)
        if entry is not None and entry[0] is loop:
            return entry[1]
        if _prune_closed_loops() and not _warned_about_throwaway_loops:
            _warned_about_throwaway_loops = True
            warnings.warn(
                "ZeroBucketStorage's async methods are running on a new event loop after an "
                "earlier loop was closed without aclose(): each loop gets its own connection "
                "pool, so the earlier pool was abandoned. This usually means Django is running "
                "under WSGI, where every async call gets a short-lived loop. Run under ASGI, "
                "use the synchronous methods, or call `await storage.aclose()` before a loop "
                "ends. (Shown once per process.)",
                RuntimeWarning,
                stacklevel=4,
            )
        client = AsyncZeroBucket(database_url=database_url, **options)
        _async_client_cache[key] = (loop, client)
        return client


async def aclose_async_clients() -> None:
    """Closes and forgets every async client that belongs to the running event loop.

    Call it when an event loop is about to end (for example at ASGI shutdown, or at the end of a
    test) so its connection pools are closed properly instead of being abandoned. Safe to call
    when there is nothing to close. Clients of other loops are left alone."""
    loop = asyncio.get_running_loop()
    with _async_cache_lock:
        mine = [key for key, (lp, _c) in _async_client_cache.items() if lp is loop]
        clients = [_async_client_cache.pop(key)[1] for key in mine]
    for client in clients:
        await client.close()


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
            this class does not re-document or restrict them. The two
            access-control hooks may be plain functions or `async def`
            functions: the async client awaits an async hook, and the sync
            client runs it through asgiref's async_to_sync, so the same rule
            applies on both paths.

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

    # ---- async API (0.2.0), on the core library's AsyncZeroBucket -----------

    def _aclient(self) -> AsyncZeroBucket:
        """The async client for the running event loop (see the module docstring)."""
        return _get_async_client(self._database_url, self._options)

    async def asave(
        self, name: str | None, content, max_length: int | None = None
    ) -> str:
        """Async counterpart of Storage.save(): same name handling and validation, same return
        value (the new image id), but the database work runs on the async client."""
        if name is None:
            name = content.name
        if not hasattr(content, "chunks"):
            content = File(content, name)
        # The same three checks Storage.save() makes (see Django's comment there).
        validate_file_name(name, allow_relative_path=True)
        name = self.get_available_name(name, max_length=max_length)
        validate_file_name(name, allow_relative_path=True)
        name = await self._asave(name, content)
        validate_file_name(name, allow_relative_path=True)
        return name

    async def _asave(self, name: str, content) -> str:
        if not hasattr(content, "read"):
            content = File(content)
        # Reading an uploaded file can touch the disk: keep it off the event loop.
        data = await asyncio.to_thread(content.read)
        return await self._aclient().put(data, filename=name)

    async def aopen(self, name: str, mode: str = "rb"):
        """Async counterpart of Storage.open(). Reads the whole image, like open()."""
        image = await self._aclient().get(name)
        return ContentFile(image.data, name=name)

    async def aexists(self, name: str) -> bool:
        return await self._aclient().exists(name)

    async def adelete(self, name: str) -> None:
        await self._aclient().delete(name)

    async def asize(self, name: str) -> int:
        """metadata() rather than get(), as in size(): the bytes are never fetched."""
        return (await self._aclient().metadata(name)).size_bytes

    async def aclose(self) -> None:
        """Closes this storage's async client for the running event loop, if there is one.

        Not needed under ASGI, where the loop lives as long as the process. Use it when you
        run your own short-lived loop (a script, a test) so the pool is closed, not abandoned.
        """
        loop = asyncio.get_running_loop()
        key = (id(loop), self._database_url, tuple(sorted(self._options.items())))
        with _async_cache_lock:
            entry = _async_client_cache.get(key)
            if entry is not None and entry[0] is loop:
                del _async_client_cache[key]
            else:
                entry = None
        if entry is not None:
            await entry[1].close()

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
