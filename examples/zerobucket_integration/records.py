"""Example: atomic writes across a parent record, an image (ZeroBucket),
and a PDF (documents.py) -- all in one transaction, all using the SAME
connection via connection=.

This is the Stage 1.2 decision in code: atomic when there's a parent
record write happening alongside the upload (nothing to orphan against
otherwise). See upload_standalone_image() at the bottom for the
non-atomic case, used when there's no parent write to protect.
"""

from __future__ import annotations

from .client import images
from .db import app_pool
from .documents import store_document

_POSTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS posts (
    id              SERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    image_id        UUID,
    document_id     UUID,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def init_posts_table() -> None:
    with app_pool.connection() as conn, conn.cursor() as cur:
        cur.execute(_POSTS_SCHEMA)


def create_post_with_attachments(
    title: str,
    *,
    image_file=None,
    document_file: bytes | None = None,
    document_filename: str | None = None,
) -> int:
    """Create a post, optionally with an attached image and/or PDF, all
    atomically -- if ANYTHING fails partway through, nothing commits:
    not the post, not the image, not the document. This is the actual
    payoff of connection=: no "post exists but its image upload silently
    failed" class of bug.
    """
    conn = app_pool.getconn()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO posts (title) VALUES (%s) RETURNING id;", (title,))
            post_id = cur.fetchone()[0]

        image_id = None
        if image_file is not None:
            # Same connection as the post insert above -- this is what
            # makes it atomic with the post, not a separate transaction.
            image_id = images.put(image_file, connection=conn)

        document_id = None
        if document_file is not None:
            document_id = store_document(
                app_pool, document_file, filename=document_filename, connection=conn
            )

        if image_id or document_id:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE posts SET image_id = %s, document_id = %s WHERE id = %s;",
                    (image_id, document_id, post_id),
                )

        conn.commit()
        return post_id
    except Exception:
        conn.rollback()
        raise
    finally:
        app_pool.putconn(conn)


def upload_standalone_image(image_file) -> str:
    """No parent record involved -- nothing to orphan against, so no
    connection= needed here. This commits independently on ZeroBucket's
    own internal pool, same as calling images.put() directly anywhere
    else in the app."""
    return images.put(image_file)
