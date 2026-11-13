"""0.33.0: connection= on AsyncZeroBucket and the three async adapters.

The async counterpart of tests/test_transactions.py, on all three databases and with each
database's own async driver (psycopg AsyncConnection, aiosqlite Connection, aiomysql
Connection): does a put()/get()/delete() with connection= take part in the CALLER's transaction,
and does the default (no connection) stay independently committing, exactly as it always was?

The rules under test are the sync client's:
- with connection= the call runs once on that connection, never commits, never retries, and
  leaves the connection open and usable;
- without it, nothing changes: the call commits on its own whatever the caller's own
  transaction does afterwards;
- a tiered object is never deleted from object storage while the caller still owns the
  transaction (a rollback would leave a row pointing at nothing).

PostgreSQL and MySQL tests skip when no server is reachable; SQLite needs none.
"""

# ruff: noqa: F401, F811
from __future__ import annotations

import io
import os

import boto3
import pytest
from PIL import Image as PILImage

from zerobucket import AsyncZeroBucket, ImageNotFoundError, StorageError
from zerobucket.adapters.mysql_async import AsyncMySQLBackend
from zerobucket.adapters.postgres_async import AsyncPostgresBackend
from zerobucket.adapters.sqlite_async import AsyncSQLiteBackend
from zerobucket.object_storage import ObjectStorage

from .test_cli_backends import target  # the sqlite/postgres/mysql fixture

SCRATCH = "_zb_txn_scratch"


def _jpeg(color=(10, 20, 30)) -> bytes:
    buf = io.BytesIO()
    PILImage.new("RGB", (40, 30), color=color).save(buf, format="JPEG")
    return buf.getvalue()


def _noisy_jpeg(size=(200, 200)) -> bytes:
    img = PILImage.frombytes("RGB", size, os.urandom(size[0] * size[1] * 3))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    return buf.getvalue()


def _async_backend(target, **kwargs):
    if target.kind == "sqlite":
        return AsyncSQLiteBackend(target.path, **kwargs)
    if target.kind == "postgres":
        return AsyncPostgresBackend(target.url, **kwargs)
    return AsyncMySQLBackend(target.url, **kwargs)


class Session:
    """The CALLER's own open transaction, on this database's own async driver."""

    def __init__(self, kind: str, conn):
        self.kind = kind
        self.conn = conn
        self.ph = "?" if kind == "sqlite" else "%s"

    @classmethod
    async def open(cls, target, *, autocommit_mode: bool = False) -> Session:
        if target.kind == "postgres":
            import psycopg

            conn = await psycopg.AsyncConnection.connect(target.url)  # autocommit off
        elif target.kind == "sqlite":
            import aiosqlite

            # autocommit_mode (isolation_level=None) is what tier_to_object_storage() needs.
            conn = await aiosqlite.connect(
                target.path,
                timeout=10.0,
                **({"isolation_level": None} if autocommit_mode else {}),
            )
        else:
            import aiomysql

            from zerobucket.adapters.mysql import _parse_database_url

            p = _parse_database_url(target.url)
            conn = await aiomysql.connect(
                host=p["host"],
                port=p["port"],
                user=p["user"],
                password=p["password"],
                db=p["database"],
                autocommit=False,
            )
        return cls(target.kind, conn)

    async def execute(self, sql: str, params=()) -> None:
        if self.kind == "mysql":
            cur = await self.conn.cursor()
            try:
                await cur.execute(sql, params)
            finally:
                await cur.close()
        else:
            await self.conn.execute(sql, params)

    async def scalar(self, sql: str, params=()):
        if self.kind == "mysql":
            cur = await self.conn.cursor()
            try:
                await cur.execute(sql, params)
                row = await cur.fetchone()
            finally:
                await cur.close()
            return None if row is None else row[0]
        cur = await self.conn.execute(sql, params)
        row = await cur.fetchone()
        return None if row is None else row[0]

    async def commit(self) -> None:
        await self.conn.commit()

    async def rollback(self) -> None:
        await self.conn.rollback()

    async def close(self) -> None:
        if self.kind == "mysql":
            self.conn.close()  # aiomysql's close() is synchronous
        else:
            await self.conn.close()


@pytest.fixture
async def env(target):
    """(client factory, session factory, scratch table) -- everything closed afterwards."""
    backends, sessions = [], []

    def client(**kw):
        backend = _async_backend(target, retry_base_delay=0.01, **kw)
        backends.append(backend)
        return AsyncZeroBucket(backend=backend)

    async def session(**kw) -> Session:
        s = await Session.open(target, **kw)
        sessions.append(s)
        return s

    # Make sure the zerobucket tables exist before any caller transaction touches them.
    first = client()
    await first.exists("00000000-0000-0000-0000-000000000000")

    setup = await session()
    await setup.execute(f"DROP TABLE IF EXISTS {SCRATCH};")
    await setup.execute(f"CREATE TABLE {SCRATCH} (id INTEGER);")
    await setup.commit()

    class Env:
        pass

    e = Env()
    e.client, e.session, e.target = client, session, target
    yield e

    for s in sessions:
        try:
            await s.rollback()
        except Exception:  # noqa: BLE001, S110
            pass  # already closed by the test
    cleanup = await Session.open(target)
    await cleanup.execute(f"DROP TABLE IF EXISTS {SCRATCH};")
    await cleanup.commit()
    await cleanup.close()
    for s in sessions:
        try:
            await s.close()
        except Exception:  # noqa: BLE001, S110
            pass
    for b in backends:
        await b.close()


async def _scratch_count(env) -> int:
    s = await env.session()
    n = await s.scalar(f"SELECT COUNT(*) FROM {SCRATCH};")
    await s.rollback()
    return n


# ---- the default is unchanged ------------------------------------------------------------


async def test_put_without_connection_commits_independently_of_the_callers_transaction(
    env,
):
    """No connection= -> the put commits on its own, whatever the caller's own transaction
    does afterwards. This is the long-standing behaviour; the test exists so it cannot change
    silently."""
    zb = env.client()
    app = await env.session()
    if env.target.kind != "sqlite":
        await app.execute(f"INSERT INTO {SCRATCH} (id) VALUES (1);")  # uncommitted

    image_id = await zb.put(_jpeg())  # no connection=

    if env.target.kind == "sqlite":
        # SQLite has ONE writer: a caller's open write transaction would simply make the
        # independent put wait ("database is locked"), so the caller writes after it instead.
        await app.execute(f"INSERT INTO {SCRATCH} (id) VALUES (1);")
    await app.rollback()

    assert await _scratch_count(env) == 0  # the caller's own insert was rolled back...
    assert await zb.exists(image_id) is True  # ...but the image was not


# ---- put ---------------------------------------------------------------------------------


async def test_put_with_connection_rolls_back_with_the_callers_transaction(env):
    zb = env.client()
    app = await env.session()
    image_id = await zb.put(_jpeg(), connection=app.conn)
    await app.rollback()
    assert await zb.exists(image_id) is False


async def test_put_with_connection_commits_with_the_callers_transaction(env):
    zb = env.client()
    app = await env.session()
    image_id = await zb.put(_jpeg(), connection=app.conn)
    assert await zb.exists(image_id) is False  # not visible to others before the commit
    await app.commit()
    assert await zb.exists(image_id) is True


async def test_the_image_and_the_callers_own_write_are_atomic(env):
    """The reason connection= exists: 'create a record and store its image' lands as one unit."""
    zb = env.client()
    app = await env.session()
    await app.execute(f"INSERT INTO {SCRATCH} (id) VALUES (7);")
    image_id = await zb.put(_jpeg((1, 2, 3)), connection=app.conn)
    await app.rollback()
    assert await _scratch_count(env) == 0
    assert await zb.exists(image_id) is False

    app = await env.session()
    await app.execute(f"INSERT INTO {SCRATCH} (id) VALUES (8);")
    image_id = await zb.put(_jpeg((4, 5, 6)), connection=app.conn)
    await app.commit()
    assert await _scratch_count(env) == 1
    assert await zb.exists(image_id) is True


async def test_put_many_with_connection_rolls_back_and_commits_as_a_unit(env):
    zb = env.client()
    app = await env.session()
    results = await zb.put_many(
        [_jpeg((1, 1, 1)), _jpeg((2, 2, 2))], connection=app.conn
    )
    ids = [r.image_id for r in results]
    assert all(r.error is None for r in results)
    await app.rollback()
    assert [await zb.exists(i) for i in ids] == [False, False]

    app = await env.session()
    results = await zb.put_many(
        [_jpeg((3, 3, 3)), _jpeg((4, 4, 4))], connection=app.conn
    )
    ids = [r.image_id for r in results]
    await app.commit()
    assert [await zb.exists(i) for i in ids] == [True, True]


# ---- reads see the caller's own uncommitted work -----------------------------------------


async def test_reads_with_connection_see_uncommitted_writes_of_the_same_transaction(
    env,
):
    zb = env.client()
    app = await env.session()
    data = _jpeg((9, 8, 7))
    image_id = await zb.put(data, connection=app.conn)

    assert (await zb.get(image_id, connection=app.conn)).data == data
    assert (await zb.metadata(image_id, connection=app.conn)).mime_type == "image/jpeg"
    assert await zb.exists(image_id, connection=app.conn) is True
    many = await zb.get_many([image_id], connection=app.conn)
    assert many[0].image is not None and many[0].error is None
    stream = await zb.get_stream(image_id, connection=app.conn)
    assert b"".join([c async for c in stream]) == data
    sink = io.BytesIO()
    assert await zb.stream_to(image_id, sink, connection=app.conn) == len(data)
    assert sink.getvalue() == data

    # ...while everyone else still sees nothing.
    assert await zb.exists(image_id) is False
    with pytest.raises(ImageNotFoundError):
        await zb.get(image_id)
    await app.rollback()


# ---- delete ------------------------------------------------------------------------------


async def test_delete_with_connection_rolls_back_with_the_callers_transaction(env):
    zb = env.client()
    image_id = await zb.put(_jpeg())
    app = await env.session()
    assert await zb.delete(image_id, connection=app.conn) is True
    assert (
        await zb.exists(image_id, connection=app.conn) is False
    )  # gone inside the transaction
    assert await zb.exists(image_id) is True  # still there for everyone else
    await app.rollback()
    assert await zb.exists(image_id) is True


async def test_delete_with_connection_commits_with_the_callers_transaction(env):
    zb = env.client()
    image_id = await zb.put(_jpeg())
    app = await env.session()
    await zb.delete(image_id, connection=app.conn)
    await app.commit()
    assert await zb.exists(image_id) is False


async def test_delete_many_with_connection_rolls_back_with_the_callers_transaction(env):
    zb = env.client()
    ids = [await zb.put(_jpeg((i, i, i))) for i in (1, 2, 3)]
    app = await env.session()
    results = await zb.delete_many(ids, connection=app.conn)
    assert [r.deleted for r in results] == [True, True, True]
    await app.rollback()
    assert [await zb.exists(i) for i in ids] == [True, True, True]


# ---- dedup -------------------------------------------------------------------------------


async def test_dedup_puts_with_connection_leave_no_blob_behind_on_rollback(env):
    zb = env.client(dedup=True)
    red = _jpeg((200, 0, 0))
    app = await env.session()
    await zb.put(red, connection=app.conn)
    await zb.put(
        red, connection=app.conn
    )  # same bytes: one blob, ref_count 2, inside the tx
    assert await app.scalar("SELECT ref_count FROM zerobucket_blobs;") == 2
    await app.rollback()

    check = await env.session()
    assert await check.scalar("SELECT COUNT(*) FROM zerobucket_blobs;") == 0
    assert await check.scalar("SELECT COUNT(*) FROM zerobucket_image_refs;") == 0
    await check.rollback()


async def test_dedup_puts_with_connection_commit_together(env):
    zb = env.client(dedup=True)
    red = _jpeg((200, 0, 0))
    app = await env.session()
    a = await zb.put(red, connection=app.conn)
    b = await zb.put(red, connection=app.conn)
    await app.commit()
    assert await app.scalar("SELECT ref_count FROM zerobucket_blobs;") == 2
    assert await zb.exists(a) and await zb.exists(b)


# ---- never retried, still observable -----------------------------------------------------


async def test_a_connection_call_is_reported_once_and_never_retried(env):
    events = []
    zb = env.client(on_operation=events.append)
    app = await env.session()
    await zb.put(_jpeg(), connection=app.conn)
    put_events = [e for e in events if e.operation == "put"]
    assert len(put_events) == 1
    assert put_events[0].success is True and put_events[0].retry_count == 0
    await app.rollback()


async def test_a_failing_connection_call_fails_once_without_a_retry(env):
    """A dead connection is exactly the kind of transient error the default path retries; with
    connection= the caller owns the transaction, so it must surface at once, one attempt only.
    """
    events = []
    zb = env.client(on_operation=events.append)
    app = await env.session()
    await app.close()  # now unusable

    # Every driver's own error (sqlite: ValueError, psycopg: OperationalError, aiomysql:
    # InterfaceError) comes back wrapped in the library's StorageError.
    with pytest.raises(StorageError):
        await zb.put(_jpeg(), connection=app.conn)

    put_events = [e for e in events if e.operation == "put"]
    assert len(put_events) == 1
    assert put_events[0].success is False and put_events[0].retry_count == 0


async def test_the_callers_connection_stays_open_and_usable(env):
    zb = env.client()
    app = await env.session()
    await zb.put(_jpeg(), connection=app.conn)
    await zb.exists("00000000-0000-0000-0000-000000000000", connection=app.conn)
    assert await app.scalar("SELECT 1;") == 1  # we did not close or commit it
    await app.rollback()


# ---- tiering -----------------------------------------------------------------------------


@pytest.fixture
def s3(s3_bucket):
    return boto3.client("s3", region_name="us-east-1"), s3_bucket


def _store(bucket):
    return ObjectStorage(bucket, region_name="us-east-1")


async def test_tier_with_connection_follows_the_databases_transaction_rules(env, s3):
    client, bucket = s3
    zb = env.client(object_storage=_store(bucket))
    image_id = await zb.put(_noisy_jpeg())

    if env.target.kind == "sqlite":
        # Like the sync adapter, SQLite tiering runs its OWN transaction (BEGIN IMMEDIATE ...
        # COMMIT) on the connection, which therefore must be in autocommit mode.
        app = await env.session(autocommit_mode=True)
        assert await zb.tier_to_object_storage(image_id, connection=app.conn) is True
        assert await app.scalar("SELECT 1;") == 1  # the connection was not closed
        assert (
            await zb.tier_to_object_storage(image_id) is False
        )  # committed: already tiered
        return

    app = await env.session()
    assert await zb.tier_to_object_storage(image_id, connection=app.conn) is True
    await app.rollback()
    # Rolled back with the caller's transaction: the row is untouched, so tiering still works.
    assert await zb.tier_to_object_storage(image_id) is True

    other = await zb.put(_noisy_jpeg())
    app = await env.session()
    assert await zb.tier_to_object_storage(other, connection=app.conn) is True
    await app.commit()
    assert (
        await zb.tier_to_object_storage(other) is False
    )  # committed with the caller's work


async def test_deleting_a_tiered_image_with_connection_leaves_the_object_alone(env, s3):
    """The caller still owns the transaction: a rollback must not leave a row pointing at an
    object we already deleted. Without connection=, the object is removed (tested elsewhere).
    """
    client, bucket = s3
    zb = env.client(object_storage=_store(bucket))
    image_id = await zb.put(_noisy_jpeg())
    await zb.tier_to_object_storage(image_id)
    assert client.list_objects_v2(Bucket=bucket)["KeyCount"] == 1

    app = await env.session()
    assert await zb.delete(image_id, connection=app.conn) is True
    await app.rollback()
    assert client.list_objects_v2(Bucket=bucket)["KeyCount"] == 1
    assert (
        await zb.exists(image_id) is True
    )  # and the row is back, still pointing at it

    app = await env.session()
    await zb.delete(image_id, connection=app.conn)
    await app.commit()
    # Committed, still never touched by us: removing the orphan is the caller's job.
    assert client.list_objects_v2(Bucket=bucket)["KeyCount"] == 1


# ---- custom backends written before 0.33.0 keep working ----------------------------------


class _OldBackend:
    """A backend written before 0.33.0: its methods have no connection= parameter."""

    def __init__(self, inner):
        self._inner = inner

    async def put(
        self,
        *,
        data,
        mime_type,
        original_filename,
        size_bytes,
        width,
        height,
        checksum_sha256,
    ):
        return await self._inner.put(
            data=data,
            mime_type=mime_type,
            original_filename=original_filename,
            size_bytes=size_bytes,
            width=width,
            height=height,
            checksum_sha256=checksum_sha256,
        )

    async def exists(self, image_id):
        return await self._inner.exists(image_id)

    async def close(self):
        await self._inner.close()


async def test_a_backend_without_connection_support_still_works_until_one_is_passed(
    env,
):
    inner = _async_backend(env.target, retry_base_delay=0.01)
    zb = AsyncZeroBucket(backend=_OldBackend(inner))
    try:
        image_id = await zb.put(
            _jpeg()
        )  # no connection= -> the client does not pass one
        assert await zb.exists(image_id) is True
        with pytest.raises(TypeError):  # asking for one is the only thing that fails
            await zb.put(_jpeg(), connection=object())
    finally:
        await zb.close()
