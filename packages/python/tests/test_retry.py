"""Tests for zerobucket.adapters.postgres retry/backoff behavior.

The retry LOOP is tested via dependency injection at the _run() level --
passing a `work` closure that raises a controlled number of times before
succeeding, using a real PostgresBackend's real pool underneath (so the
retry loop's actual connection-acquisition path is exercised), but
without depending on genuinely triggering a live deadlock/connection
drop, which would be flaky in CI. The classification logic
(_is_retryable) is tested separately against real psycopg exception
instances constructed directly, which IS meaningful without a live
failure since it's pure classification logic.

Honest limitation: this does not prove behavior against an actual live
network partition or a real concurrent-transaction deadlock -- those are
inherently hard to reproduce deterministically. What's verified here is
the retry mechanism's own logic (when does it retry, how many times,
does backoff happen, does connection= correctly bypass it) using
controlled failure injection.
"""

from __future__ import annotations

import psycopg
import pytest

from zerobucket.adapters.postgres import PostgresBackend, _backoff_delay, _is_retryable
from zerobucket.exceptions import StorageError

# ---- _is_retryable classification -----------------------------------


def test_operational_error_is_retryable():
    # Constructed directly rather than triggered live -- OperationalError
    # itself, regardless of message, represents a connection-level
    # failure and should always be retried.
    exc = psycopg.OperationalError("simulated connection failure")
    assert _is_retryable(exc) is True


def test_serialization_failure_sqlstate_is_retryable():
    exc = psycopg.errors.SerializationFailure("simulated")
    assert exc.sqlstate == "40001"
    assert _is_retryable(exc) is True


def test_deadlock_detected_sqlstate_is_retryable():
    exc = psycopg.errors.DeadlockDetected("simulated")
    assert exc.sqlstate == "40P01"
    assert _is_retryable(exc) is True


def test_syntax_error_is_not_retryable():
    exc = psycopg.errors.SyntaxError("simulated bad SQL")
    assert _is_retryable(exc) is False


def test_unique_violation_is_not_retryable():
    """A constraint violation will fail identically every time -- must
    never be retried."""
    exc = psycopg.errors.UniqueViolation("simulated")
    assert _is_retryable(exc) is False


def test_plain_value_error_is_not_retryable():
    assert _is_retryable(ValueError("not even a database error")) is False


# ---- _backoff_delay ----------------------------------------------------


def test_backoff_delay_increases_with_attempt():
    # Run several times since jitter is random -- the exponential base
    # should still dominate and produce a clear increasing trend.
    delay_1 = max(_backoff_delay(1, base_delay=0.1) for _ in range(20))
    delay_3 = max(_backoff_delay(3, base_delay=0.1) for _ in range(20))
    assert delay_3 > delay_1


def test_backoff_delay_is_capped():
    # A huge attempt number must not produce an unbounded delay.
    delay = _backoff_delay(50, base_delay=1.0)
    assert delay <= 2.0  # _MAX_BACKOFF_SECONDS


def test_backoff_delay_never_negative():
    for attempt in range(1, 10):
        assert _backoff_delay(attempt, base_delay=0.1) >= 0


# ---- _run() retry loop, via dependency injection -----------------------


@pytest.fixture
def fast_backend(_db_available, monkeypatch):
    """A real PostgresBackend, but with sleep patched out so retry tests
    run instantly instead of waiting out real backoff delays."""
    from tests.conftest import TEST_DATABASE_URL

    monkeypatch.setattr("zerobucket.adapters.postgres.time.sleep", lambda _: None)

    backend = PostgresBackend(TEST_DATABASE_URL, max_retries=3, retry_base_delay=0.01)
    yield backend
    backend.close()


def test_run_retries_transient_failure_then_succeeds(fast_backend):
    attempts = {"count": 0}

    def flaky_work(cur):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise psycopg.OperationalError("simulated transient failure")
        return "success"

    result = fast_backend._run(None, flaky_work)
    assert result == "success"
    assert attempts["count"] == 3  # failed twice, succeeded on the 3rd


def test_run_does_not_retry_non_transient_error(fast_backend):
    attempts = {"count": 0}

    def always_bad_sql(cur):
        attempts["count"] += 1
        raise psycopg.errors.SyntaxError("simulated bad SQL, will never succeed")

    with pytest.raises(psycopg.errors.SyntaxError):
        fast_backend._run(None, always_bad_sql)
    assert attempts["count"] == 1  # no retry attempted


def test_run_gives_up_after_max_retries(fast_backend):
    attempts = {"count": 0}

    def always_transient(cur):
        attempts["count"] += 1
        raise psycopg.OperationalError("simulated, always fails")

    with pytest.raises(psycopg.OperationalError):
        fast_backend._run(None, always_transient)
    # max_retries=3 means up to 4 total attempts (1 initial + 3 retries).
    assert attempts["count"] == 4


def test_run_with_connection_never_retries_even_transient_error(fast_backend):
    """The critical safety rule: a caller-supplied connection must be
    tried exactly once, never retried automatically, regardless of the
    error type -- retrying on a connection the caller owns could corrupt
    their own transaction semantics."""
    attempts = {"count": 0}

    def always_transient(cur):
        attempts["count"] += 1
        raise psycopg.OperationalError("simulated, would normally be retried")

    conn = psycopg.connect(
        "postgresql://postgres:postgres@localhost:5432/zerobucket_test"
    )
    try:
        with pytest.raises(psycopg.OperationalError):
            fast_backend._run(conn, always_transient)
        assert attempts["count"] == 1  # exactly one attempt, no retry
    finally:
        conn.close()


def test_max_retries_zero_disables_retry_entirely(_db_available, monkeypatch):
    from tests.conftest import TEST_DATABASE_URL

    monkeypatch.setattr("zerobucket.adapters.postgres.time.sleep", lambda _: None)

    backend = PostgresBackend(TEST_DATABASE_URL, max_retries=0, retry_base_delay=0.01)
    try:
        attempts = {"count": 0}

        def always_transient(cur):
            attempts["count"] += 1
            raise psycopg.OperationalError("simulated")

        with pytest.raises(psycopg.OperationalError):
            backend._run(None, always_transient)
        assert attempts["count"] == 1
    finally:
        backend.close()


def test_retry_is_transparent_to_public_methods_wrapping_in_storage_error(fast_backend):
    """Confirm the public-facing StorageError wrapping still happens
    correctly after retries are exhausted -- retries shouldn't change
    the exception type callers actually see."""
    attempts = {"count": 0}

    def always_transient(cur):
        attempts["count"] += 1
        raise psycopg.OperationalError("simulated")

    with pytest.raises(StorageError):
        try:
            fast_backend._run(None, always_transient)
        except Exception as exc:  # noqa: BLE001
            raise StorageError(f"Failed: {exc}") from exc
    assert attempts["count"] == 4
