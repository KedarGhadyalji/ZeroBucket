"""Shared app-level Postgres connection pool.

This is deliberately SEPARATE from ZeroBucket's own internal pool.
ZeroBucket doesn't currently expose its internal pool for reuse by other
tables (that's Stage 4 item 14 on the roadmap -- not built yet), so this
app needs its own small pool for two things:

  1. Writing to your own application tables (users, posts, whatever owns
     the images/documents) -- you almost certainly already have
     something like this in a real app; this file exists mainly so the
     rest of this example is runnable standalone.
  2. The `app_documents` table (see documents.py) -- PDFs stored as
     BYTEA in the same database, following ZeroBucket's own philosophy
     without going through the image-specific library.

In a real app, replace this with whatever connection pool your
framework already manages (Flask-SQLAlchemy's engine, Django's own
connection handling, etc.) -- don't run a THIRD pool alongside your
framework's and this one.
"""

from __future__ import annotations

import os

from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get(
    "ZEROBUCKET_TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/zerobucket_test",
)

# Small pool -- this example app is not high-concurrency. Tune to your
# real app's needs.
app_pool = ConnectionPool(DATABASE_URL, min_size=1, max_size=5, open=True)
