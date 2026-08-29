"""Serving views with proper HTTP caching.

The highest-value, lowest-effort item in this whole integration: the
SHA-256 checksum is already computed and stored on every image AND
document row (ZeroBucket does this for images already; documents.py
does the same for PDFs). That checksum IS a perfect ETag -- content-
addressed, changes if and only if the bytes change. Wiring it up costs
almost nothing and gives real conditional-GET/304 behavior: a browser
that already has an image cached sends `If-None-Match`, and if it
matches, we skip re-sending the bytes entirely.

Framework-agnostic in principle -- this example uses Flask since the
rest of the zerobucket README examples do, but the same pattern
(ETag = checksum, honor If-None-Match, set Cache-Control) applies
identically in Django, FastAPI, or anything else.
"""

from __future__ import annotations

from flask import Blueprint, Response, request

from .client import images
from .db import app_pool
from .documents import DocumentNotFoundError, get_document
from zerobucket import ImageNotFoundError

bp = Blueprint("zerobucket_serving", __name__)


@bp.route("/images/<image_id>")
def serve_image(image_id: str):
    try:
        image = images.get(image_id)
    except ImageNotFoundError:
        return Response("Not found", status=404)

    etag = image.checksum_sha256

    # Honor conditional GET: if the client's cached copy matches, tell
    # them so without re-sending the bytes at all.
    if request.if_none_match and etag in request.if_none_match:
        return Response(status=304)

    response = Response(image.data, mimetype=image.mime_type)
    response.headers["ETag"] = etag
    # immutable is safe here specifically BECAUSE the URL is content-
    # addressed by id and the id never changes meaning once created --
    # if your app ever allows editing an image in place under the same
    # id, remove `immutable` and lower max-age accordingly.
    response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return response


@bp.route("/documents/<document_id>")
def serve_document(document_id: str):
    try:
        document = get_document(app_pool, document_id)
    except DocumentNotFoundError:
        return Response("Not found", status=404)

    etag = document.checksum_sha256

    if request.if_none_match and etag in request.if_none_match:
        return Response(status=304)

    response = Response(document.data, mimetype="application/pdf")
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    if document.original_filename:
        response.headers["Content-Disposition"] = (
            f'inline; filename="{document.original_filename}"'
        )
    return response
