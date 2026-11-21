"""The serving half of ZeroBucketStorage.url() -- a class-based view
that streams a stored image's bytes back over HTTP.

Include this app's urls in your project for ZeroBucketStorage.url() to
resolve:

    # project urls.py
    from django.urls import include, path
    urlpatterns = [
        ...,
        path("zerobucket-images/", include("django_zerobucket.urls")),
    ]

No built-in access control. This is a deliberate first-pass scope
decision, not an oversight: ZeroBucket's own before_get/before_put hooks
(see the core library's README) exist for exactly this, but wiring a
context= through Django's request/auth system into those hooks is a
real design surface of its own (whose context? the request? the user?
something else?) that wasn't part of what was asked for this round. If
you need access control on served images today, wrap this view (or a
subclass of it) with Django's own decorators the normal way:

    from django.contrib.auth.decorators import login_required
    from django_zerobucket.views import ServeImageView

    urlpatterns = [
        path("images/<uuid:image_id>/",
             login_required(ServeImageView.as_view()),
             name="my_protected_image"),
    ]

(and skip including django_zerobucket.urls' own unprotected route, or
your images will still be reachable there too.)

ASYNC (0.2.0): AsyncServeImageView is the same view on the async client, for
projects running under ASGI. To make ZeroBucketStorage.url() point at it, include
"django_zerobucket.urls_async" INSTEAD of "django_zerobucket.urls" (both define the
same route name, so use one or the other), or mount the view yourself. Under ASGI
it never blocks the event loop and streams the response asynchronously. Under WSGI
Django runs an async view in a throwaway event loop per request, and the async
client's connection pool cannot survive that (see storage.py), so there the view
serves the request with the synchronous client in a worker thread instead: same
response, no pool per request. It tells the two apart by whether the request is an
ASGIRequest (it has a `scope`).
"""

from __future__ import annotations

from asgiref.sync import sync_to_async
from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseNotFound,
    StreamingHttpResponse,
)
from django.utils.http import content_disposition_header
from django.views import View
from zerobucket import ImageNotFoundError

from .storage import ZeroBucketStorage


def _apply_image_headers(response: StreamingHttpResponse, metadata) -> None:
    """Content-Length and Content-Disposition, shared by both views.

    Content-Disposition is built by Django's own helper (RFC 6266), so a filename with a double
    quote or non-ASCII characters is escaped or encoded properly instead of producing a
    malformed header."""
    response["Content-Length"] = str(metadata.size_bytes)
    if metadata.filename:
        response["Content-Disposition"] = content_disposition_header(
            False, metadata.filename
        )


class ServeImageView(View):
    """`storage_class` is instantiated ONCE, at class-definition/import
    time (a class attribute, not per-request) -- see storage.py's
    `_get_shared_client` for why that doesn't mean a separate connection
    pool from whatever ZeroBucketStorage instance your FileField/
    ImageField actually uses: both share ZeroBucket instances keyed by
    (database_url, options), so as long as this view's implicit
    `ZeroBucketStorage()` (using default OPTIONS -- i.e.
    settings.ZEROBUCKET_DATABASE_URL, no extra kwargs) matches how your
    model field's storage is configured, they resolve to the exact same
    underlying client. If your model field's storage is configured with
    OPTIONS beyond a bare database_url, override `storage_class` (or
    the `storage` property below) in a subclass to match, or this view
    will open a second, differently-configured client instead of
    sharing the first.

    Streams via get_stream(), not get() -- a real use of the core
    library's own streaming feature, not just a demonstration of it:
    avoids holding a full image in this process's memory during a
    request, the exact same benefit ZeroBucket's own README describes
    for get_stream() generally.
    """

    storage_class = ZeroBucketStorage

    @property
    def storage(self) -> ZeroBucketStorage:
        if not hasattr(self, "_storage"):
            self._storage = self.storage_class()
        return self._storage

    def get(self, request: HttpRequest, image_id: str) -> HttpResponse:
        """Two round trips per request, stated plainly rather than left
        for someone to notice under profiling: one explicit metadata()
        call here (to get mime_type/size/filename for the response
        headers before any bytes are sent), and one implicit one inside
        get_stream() itself (its own existence/size check -- see the
        core library's get_stream() docstring). Both are metadata-only
        queries (no image bytes fetched twice), so this costs two small
        round trips, not two full-image reads -- but it is two, not
        one, and get_stream()'s public API doesn't currently offer a
        way to skip its internal check when the caller already knows
        the row exists."""
        client = self.storage._client  # noqa: SLF001 -- same package, deliberate reuse
        try:
            metadata = client.metadata(image_id)
        except ImageNotFoundError:
            return HttpResponseNotFound()

        stream = client.get_stream(image_id)
        response = StreamingHttpResponse(stream, content_type=metadata.mime_type)
        _apply_image_headers(response, metadata)
        return response


class AsyncServeImageView(View):
    """ServeImageView on the async client (see this module's docstring for when to use it).

    `storage_class` works exactly as on ServeImageView: it is instantiated once, and if your
    model field's storage has OPTIONS beyond a bare database_url, override it in a subclass so
    this view uses an identically configured client.

    Two round trips per request, as in the sync view (metadata for the headers, then
    get_stream()'s own existence check). The response body is an async iterator, which Django's
    ASGI handler streams without blocking; it is read after this coroutine returns, on the same
    event loop. If the image is deleted between the two calls the answer is a 404, not a 500.
    """

    storage_class = ZeroBucketStorage

    @property
    def storage(self) -> ZeroBucketStorage:
        if not hasattr(self, "_storage"):
            self._storage = self.storage_class()
        return self._storage

    async def get(self, request: HttpRequest, image_id: str) -> HttpResponse:
        if not hasattr(request, "scope"):
            # Not an ASGIRequest: Django is running this async view under WSGI, in a loop that
            # ends with the request. Use the synchronous path (see the module docstring).
            return await sync_to_async(self._get_sync)(request, image_id)

        client = (
            self.storage._aclient()
        )  # noqa: SLF001 -- same package, deliberate reuse
        try:
            metadata = await client.metadata(image_id)
            stream = await client.get_stream(image_id)
        except ImageNotFoundError:
            return HttpResponseNotFound()

        response = StreamingHttpResponse(stream, content_type=metadata.mime_type)
        _apply_image_headers(response, metadata)
        return response

    def _get_sync(self, request: HttpRequest, image_id: str) -> HttpResponse:
        view = ServeImageView()
        view.storage_class = self.storage_class
        return view.get(request, image_id)
