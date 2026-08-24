"""Tests for error handling that don't require a healthy database connection."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from zerobucket import ZeroBucket
from zerobucket.client import ZeroBucket as ZB
from zerobucket.exceptions import StorageError


def test_unreachable_database_raises_storage_error():
    with pytest.raises(StorageError):
        ZeroBucket(database_url="postgresql://baduser:badpass@localhost:1/nonexistent_db")


def test_zerobucket_requires_database_url_or_backend():
    with pytest.raises(ValueError):
        ZeroBucket()


def test_put_rejects_unsupported_input_type():
    zb = ZB(backend=MagicMock())
    with pytest.raises(TypeError):
        zb.put(12345)  # not a path, bytes, or file-like object
