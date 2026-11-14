"""0.33.1: an incompatible aiomysql + PyMySQL pair fails at start-up, with a clear message.

aiomysql 0.3.2 imports internal PyMySQL names that PyMySQL 1.2 removed (1.2.1) or replaced
with a poison-pill string (1.2.2+). Before 0.33.1 the first case was reported as "aiomysql is
not installed" (the wrong advice: it is installed) and the second only surfaced as a cryptic
`TypeError: 'str' object is not callable` on the first image upload.

None of these tests need a database: the check runs before any connection is made.
"""

from __future__ import annotations

import sys
import textwrap
from importlib import invalidate_caches

import pymysql.converters
import pytest

from zerobucket import StorageError
from zerobucket.adapters.mysql_async import AsyncMySQLBackend, _pymysql_breaks_aiomysql

URL = "mysql://nobody:nothing@127.0.0.1:1/never_connected"
NIL = "00000000-0000-0000-0000-000000000000"


def test_the_poison_pill_is_detected_and_a_real_function_is_not(monkeypatch):
    monkeypatch.setattr(
        pymysql.converters, "escape_dict", "DO NOT IMPORT THIS!!!", raising=False
    )
    assert _pymysql_breaks_aiomysql() is True
    monkeypatch.setattr(pymysql.converters, "escape_dict", lambda *a: {}, raising=False)
    assert _pymysql_breaks_aiomysql() is False


async def test_a_poisoned_pymysql_fails_at_start_up_not_at_the_first_upload(
    monkeypatch,
):
    pytest.importorskip("aiomysql")
    monkeypatch.setattr(
        pymysql.converters, "escape_dict", "DO NOT IMPORT THIS!!!", raising=False
    )
    backend = AsyncMySQLBackend(URL)
    with pytest.raises(StorageError, match=r"PyMySQL<1\.2\.0") as caught:
        await backend.exists(NIL)
    assert "cannot work with PyMySQL" in str(caught.value)


async def test_an_installed_aiomysql_that_fails_to_import_is_not_reported_as_missing(
    monkeypatch, tmp_path
):
    """The PyMySQL 1.2.1 case: `import aiomysql` itself raises ImportError. 'Install aiomysql' would
    be exactly the wrong advice -- it is installed."""
    package = tmp_path / "aiomysql"
    package.mkdir()
    (package / "__init__.py").write_text(textwrap.dedent("""
            raise ImportError("cannot import name 'escape_dict' from 'pymysql.converters'")
            """))
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "aiomysql", raising=False)
    invalidate_caches()

    backend = AsyncMySQLBackend(URL)
    with pytest.raises(StorageError) as caught:
        await backend.exists(NIL)
    message = str(caught.value)
    assert "cannot work with PyMySQL" in message and "PyMySQL<1.2.0" in message
    assert "requires aiomysql" not in message


async def test_a_really_missing_aiomysql_still_says_to_install_it(monkeypatch):
    monkeypatch.setitem(
        sys.modules, "aiomysql", None
    )  # makes `import aiomysql` raise ImportError
    backend = AsyncMySQLBackend(URL)
    with pytest.raises(StorageError, match="requires aiomysql"):
        await backend.exists(NIL)
