import os
from zerobucket import ZeroBucket, ImageValidationError, ImageNotFoundError

DATABASE_URL = os.environ.get(
    "ZEROBUCKET_TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/zerobucket_test",
)

images = ZeroBucket(database_url=DATABASE_URL)

# --- put() ---
image_id = images.put("test.png")
print("put() -> id:", image_id)

# --- get() ---
result = images.get(image_id)
print("get() -> mime_type:", result.mime_type)
print("get() -> size_bytes:", result.size_bytes)
print("get() -> dimensions:", result.width, "x", result.height)
print("get() -> checksum:", result.checksum_sha256)

# Write the retrieved bytes back out so you can open and eyeball it
with open("retrieved.png", "wb") as f:
    f.write(result.data)
print("Wrote retrieved.png -- open it and compare to test.png")

# --- metadata() ---
meta = images.metadata(image_id)
print("metadata() -> filename:", meta.filename)

# --- exists() ---
print("exists() ->", images.exists(image_id))

# --- delete() ---
print("delete() ->", images.delete(image_id))
print("exists() after delete ->", images.exists(image_id))

# --- confirm get() now raises correctly ---
try:
    images.get(image_id)
except ImageNotFoundError:
    print("get() after delete correctly raised ImageNotFoundError")

images.close()
