import io
import sys

from PIL import Image as PILImage

sys.path.insert(0, "/home/claude")

from zerobucket_integration.client import images
from zerobucket_integration.db import app_pool
from zerobucket_integration.documents import (
    DocumentValidationError,
    delete_document,
    init_documents_table,
    store_document,
)
from zerobucket_integration.records import (
    create_post_with_attachments,
    init_posts_table,
    upload_standalone_image,
)
from zerobucket_integration.serving import bp

from flask import Flask


def jpeg_bytes(color=(10, 20, 30)):
    img = PILImage.new("RGB", (40, 30), color=color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def fake_pdf_bytes():
    return b"%PDF-1.4\n%fake but valid-looking header for testing\n%%EOF"


def cleanup_posts_table():
    with app_pool.connection() as conn, conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS posts;")
        cur.execute("TRUNCATE app_documents;")


print("=== Setup ===")
init_documents_table(app_pool)
init_posts_table()
cleanup_posts_table()
init_posts_table()
print("Tables ready.")

print()
print("=== Test 1: atomic success (post + image + document all commit together) ===")
post_id = create_post_with_attachments(
    "My first post",
    image_file=jpeg_bytes((255, 0, 0)),
    document_file=fake_pdf_bytes(),
    document_filename="report.pdf",
)
print(f"Created post_id={post_id}")

with app_pool.connection() as conn, conn.cursor() as cur:
    cur.execute(
        "SELECT title, image_id, document_id FROM posts WHERE id = %s;", (post_id,)
    )
    title, image_id, document_id = cur.fetchone()
    print(f"  post.title={title!r} image_id={image_id} document_id={document_id}")
    assert title == "My first post"
    assert image_id is not None
    assert document_id is not None

assert images.exists(str(image_id)) is True
print("  Image genuinely exists in zerobucket_images: OK")

from zerobucket_integration.documents import get_document

doc = get_document(app_pool, str(document_id))
assert doc.original_filename == "report.pdf"
print("  Document genuinely exists in app_documents: OK")

print()
print(
    "=== Test 2: atomic ROLLBACK (bad document should roll back post AND image together) ==="
)
posts_before = None
with app_pool.connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM posts;")
    posts_before = cur.fetchone()[0]
images_before = None
with app_pool.connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM zerobucket_images;")
    images_before = cur.fetchone()[0]

try:
    create_post_with_attachments(
        "This post should NOT exist after rollback",
        image_file=jpeg_bytes((0, 255, 0)),  # valid image
        document_file=b"NOT A REAL PDF AT ALL",  # invalid -- will raise DocumentValidationError
        document_filename="bad.pdf",
    )
    print("  ERROR: expected DocumentValidationError, none was raised!")
    sys.exit(1)
except DocumentValidationError as e:
    print(f"  Correctly raised DocumentValidationError: {e}")

with app_pool.connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM posts;")
    posts_after = cur.fetchone()[0]
with app_pool.connection() as conn, conn.cursor() as cur:
    cur.execute("SELECT count(*) FROM zerobucket_images;")
    images_after = cur.fetchone()[0]

print(f"  posts count: before={posts_before} after={posts_after}")
print(f"  zerobucket_images count: before={images_before} after={images_after}")
assert posts_after == posts_before, "Post row leaked despite rollback!"
assert (
    images_after == images_before
), "Image row leaked despite rollback! (orphaned image bug)"
print(
    "  Confirmed: NEITHER the post NOR the image committed. True atomicity across post+image+document."
)

print()
print(
    "=== Test 3: standalone image upload (no parent record, no connection= needed) ==="
)
standalone_id = upload_standalone_image(jpeg_bytes((0, 0, 255)))
assert images.exists(standalone_id) is True
print(f"  Standalone image {standalone_id} stored independently: OK")

print()
print("=== Test 4: document validation rejects non-PDF content directly ===")
try:
    store_document(app_pool, b"just some random text, not a pdf")
    print("  ERROR: expected DocumentValidationError")
    sys.exit(1)
except DocumentValidationError as e:
    print(f"  Correctly rejected: {e}")

print()
print("=== Test 5: ETag / Cache-Control / 304 behavior (real Flask test client) ===")
app = Flask(__name__)
app.register_blueprint(bp)
client = app.test_client()

resp1 = client.get(f"/images/{standalone_id}")
print(
    f"  First request: status={resp1.status_code} ETag={resp1.headers.get('ETag')} Cache-Control={resp1.headers.get('Cache-Control')}"
)
assert resp1.status_code == 200
assert resp1.headers.get("ETag") is not None
assert resp1.headers.get("Cache-Control") == "public, max-age=31536000, immutable"
etag = resp1.headers["ETag"]

# Second request with If-None-Match matching -- should get 304, no body
resp2 = client.get(f"/images/{standalone_id}", headers={"If-None-Match": etag})
print(
    f"  Conditional request (matching ETag): status={resp2.status_code} body_length={len(resp2.data)}"
)
assert resp2.status_code == 304
assert len(resp2.data) == 0

# Missing image -> 404
resp3 = client.get("/images/00000000-0000-0000-0000-000000000000")
print(f"  Missing image request: status={resp3.status_code}")
assert resp3.status_code == 404

# Document serving too
resp4 = client.get(f"/documents/{document_id}")
print(
    f"  Document request: status={resp4.status_code} ETag={resp4.headers.get('ETag')} Content-Disposition={resp4.headers.get('Content-Disposition')}"
)
assert resp4.status_code == 200
assert resp4.headers.get("ETag") is not None
assert "report.pdf" in resp4.headers.get("Content-Disposition", "")

print()
print("=== ALL INTEGRATION TESTS PASSED ===")

# cleanup
images.delete(str(image_id))
images.delete(standalone_id)
delete_document(app_pool, str(document_id))
with app_pool.connection() as conn, conn.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS posts;")
    cur.execute("TRUNCATE app_documents;")
print("Cleaned up test data.")

# Explicitly close both connection pools before exiting -- without this,
# their background worker threads linger past interpreter shutdown and
# print "couldn't stop thread" warnings (same class of issue fixed in
# the library itself back in v0.1.1: pools need an explicit close(),
# don't rely on garbage collection timing).
images.close()
app_pool.close()
print("Closed both connection pools cleanly.")
