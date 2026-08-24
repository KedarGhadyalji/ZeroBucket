# Contributing to ZeroBucket

Thanks for considering a contribution. This project intentionally stays
small -- please read the scope notes below before proposing a large
feature.

## Scope

ZeroBucket targets small applications, prototypes, and internal tools
that want to avoid standing up a separate object-storage service. It
deliberately does **not** try to compete with S3 for large-scale media
infrastructure. Features that only make sense at that scale (streaming,
CDN integration, multi-region replication) are out of scope.

## Development setup

```bash
git clone https://github.com/zerobucket/zerobucket
cd zerobucket/packages/python
pip install -e ".[dev]"
```

Integration tests need a real PostgreSQL database:

```bash
createdb zerobucket_test
export ZEROBUCKET_TEST_DATABASE_URL=postgresql://localhost/zerobucket_test
pytest
```

Unit tests (`tests/test_validation.py`, `tests/test_errors.py`) run
without a database and are skipped-safe if one isn't configured.

## Before submitting a PR

- `pytest` passes, including integration tests against a real Postgres
- `ruff check src/ tests/` is clean
- New behavior has a test; bug fixes have a regression test
- No claims of performance or storage savings without numbers in
  `benchmarks/RESULTS.md` to back them

## Reporting security issues

See [SECURITY.md](SECURITY.md) -- please do not open a public issue for
a security vulnerability.
