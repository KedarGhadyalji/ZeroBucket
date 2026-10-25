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

### npm package (`packages/typescript`)

```bash
cd packages/typescript
npm ci --ignore-scripts      # or: npm install
npm test
```

Create a `.env` at the repo root from `.env.example` and put your real database URLs in it. `npm test` reads it
automatically (and `.env.local`, and the same two files inside `packages/typescript/`), so there is nothing to `set` or
`export` in each new terminal. Things to know:

- A value in `.env` **wins over a variable already set in your shell**, because a stale one left by an old `set`/`setx` is the
  usual cause of "password authentication failed" after you change a password. The run prints what it used, with passwords
  hidden, and says when it replaced a different shell value. For one run with the shell in charge, set
  `ZEROBUCKET_IGNORE_DOTENV=1`.
- A backend whose variable is not set is **skipped, not failed**, so check the skipped count in the summary. A green run that
  skipped everything proves nothing.
- Only `ZEROBUCKET_*` and `AWS_*` keys are read. CI has no `.env` and passes its settings through the workflow environment.
- **If a test fails with an authentication error, or is skipped when it should not be, run `npm run test:env`** (in
  `packages/typescript`). It lists which `.env` files it found and how they were decoded, shows every setting with passwords
  hidden, and then really connects to PostgreSQL, MySQL/MariaDB and the S3 endpoint and prints what each one answered. Paste its
  output when asking for help.
- On Windows, create `.env` in an editor and save it as UTF-8. The loader also reads UTF-16, which PowerShell's `echo ... > .env`
  writes, and it warns if the file is really called `.env.txt` (Notepad adds that silently; turn on "File name extensions" in
  Explorer to see it).
- The Python tests do not read `.env` yet; set the variables in your shell for those.

## Before submitting a PR

- `pytest` passes, including integration tests against a real Postgres
- `ruff check src/ tests/` is clean
- New behavior has a test; bug fixes have a regression test
- No claims of performance or storage savings without numbers in
  `benchmarks/RESULTS.md` to back them

## Reporting security issues

See [SECURITY.md](SECURITY.md) -- please do not open a public issue for
a security vulnerability.
