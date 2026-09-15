"""Minimal Django settings for testing django-zerobucket against a real
Postgres instance -- NOT the same Postgres database ZeroBucket stores
images in (that's a separate, plain connection string,
ZEROBUCKET_DATABASE_URL) versus Django's own DATABASES setting (used
here only for Django's own internal tables -- sessions, migrations
bookkeeping, and this test app's dummy model -- which is intentionally
a *different* thing from where ZeroBucket stores image bytes, even
though in this test setup they happen to point at the same physical
Postgres server for convenience).
"""

from __future__ import annotations

import os

SECRET_KEY = "test-secret-key-not-for-production"
DEBUG = True
ALLOWED_HOSTS = ["*"]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "zerobucket_test",
        "USER": "postgres",
        "PASSWORD": "postgres",
        "HOST": "localhost",
        "PORT": "5432",
    }
}

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_zerobucket",
    "tests.testapp",
]

ROOT_URLCONF = "tests.urls"

USE_TZ = True

ZEROBUCKET_DATABASE_URL = os.environ.get(
    "ZEROBUCKET_TEST_DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/zerobucket_test",
)

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
