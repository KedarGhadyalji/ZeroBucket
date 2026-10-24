# zerobucket npm benchmarks

Measures the [`zerobucket`](https://www.npmjs.com/package/zerobucket) npm package: `put()` and `get()` latency,
memory, how long the event loop stalls, throughput under concurrency, and how well `optimize` compresses. Results from
the maintainer's runs are in [RESULTS.md](RESULTS.md) and [COMPRESSION_RESULTS.md](COMPRESSION_RESULTS.md); raw numbers
are in `results/`.

This is a separate project, like `examples/node-integration`, that installs `zerobucket` as a dependency, so it measures
what users actually install. Nothing here ships in the npm package.

## Run it

```bash
cd benchmarks/node
npm install          # not `npm ci`: there is deliberately no lockfile (see .npmrc)
```

`better-sqlite3` and `mysql2` are optional dependencies: if one fails to install, that backend is skipped with a message.
The project has no lockfile on purpose: with one, npm runs `node-gyp rebuild` for `better-sqlite3`, which needs C++ build tools
and otherwise fails silently.

PostgreSQL and MySQL/MariaDB are opt-in through environment variables. SQLite needs nothing (it uses a temporary file).
On Windows use `set NAME=value` in the same window you run the benchmark in.

```bash
export ZEROBUCKET_BENCH_DATABASE_URL=postgresql://user:pass@localhost:5432/mydb   # or ZEROBUCKET_TEST_DATABASE_URL
export ZEROBUCKET_BENCH_MYSQL_URL=mysql://user:pass@127.0.0.1:3306/mydb           # or ZEROBUCKET_TEST_MYSQL_URL

npm run bench                 # full run: six sizes, 7 measured runs each, writes results/
npm run bench:quick           # smoke run (two small sizes), writes nothing
npm run bench -- --backend postgres --sizes 100KB,1MB --repeats 9
npm run compression           # compression and SSIM sweep
npm run validation-cost       # where validation time goes (see RESULTS.md)
```

Options for `npm run bench`: `--backend postgres,sqlite,mysql`, `--sizes 10KB,100KB,...`, `--repeats N`, `--warmup N`,
`--concurrency 1,5,20`, `--concurrency-ops N`, `--sqlite-path FILE`, `--out DIR`, `--no-write`, `--quick`.

The benchmark only touches the `zerobucket_images` table and only the rows it creates: it never truncates or drops
anything, and it deletes its own rows when it finishes. It creates the table if it does not exist, so point it at a
scratch database rather than one holding data you care about.

To benchmark an unreleased build, pack it and install the tarball: `(cd ../../packages/typescript && npm pack)`, then
`npm install --no-save ../../packages/typescript/zerobucket-*.tgz`.

## What is measured

| Column     | Meaning                                                                                             |
| ---------- | --------------------------------------------------------------------------------------------------- |
| Validate   | `validateImage()` alone (format check, header read, full decode)                                    |
| Put        | `put()`: validation, checksum and the database write                                                |
| Get        | `get()`: the query and building the result                                                          |
| Stored     | bytes the database reports for the data column (`pg_column_size` on PostgreSQL, `length` elsewhere) |
| Peak mem   | largest growth of resident memory during the call                                                   |
| Loop stall | longest gap between event-loop ticks during the call, beyond the 1 ms sampling interval             |

Run with `npm run bench` (which passes `--expose-gc`) for steadier memory numbers; the other columns do not depend on it.
Memory is resident-memory growth, not a heap profile, and the allocator can reuse memory freed by an earlier call, so small
or zero values on later calls are a lower bound.

## Comparing with the Python package

The Python harness is `benchmarks/run_benchmark.py` in the repo root. Run it against the same database on the same machine,
or the numbers say nothing about the packages. For compression, `npm run compression -- --dump ./fixtures` writes the
original images and `python compare_python.py ./fixtures` runs Pillow on those exact files.

## Files

```
src/run.ts               latency, memory, event-loop and throughput runner
src/compression.ts       compression and SSIM sweep
src/validation-cost.ts   timing of each stage of validation
src/images.ts            seeded test-image generators
src/ssim.ts              SSIM, matching scikit-image's defaults
compare_python.py        Pillow on the same originals, for the compression comparison
results/                 results.csv, concurrency.csv, environment.json from the maintainer's run
```
