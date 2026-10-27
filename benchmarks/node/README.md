# zerobucket npm benchmarks

Measures the [`zerobucket`](https://www.npmjs.com/package/zerobucket) npm package: `put()` and `get()` latency,
memory, how long the event loop stalls, throughput under concurrency, and how well `optimize` compresses. Results from
the maintainer's runs are in [RESULTS.md](RESULTS.md) and [COMPRESSION_RESULTS.md](COMPRESSION_RESULTS.md); raw numbers
are in `results/<zerobucket version>/` (one folder per version, so before and after comparisons keep both).

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
npm run validation-cost       # time of each validation step, old and new (see RESULTS.md)
npm run read-cost             # PostgreSQL: time and fresh-process memory of get() and a full stream, per size
npm run compare-builds -- --a ../a --b ../b   # two installed builds, reads interleaved: the fair way to compare them
```

Options for `npm run bench`: `--backend postgres,sqlite,mysql`, `--sizes 10KB,100KB,...`, `--repeats N`, `--warmup N`,
`--concurrency 1,5,20`, `--concurrency-ops N`, `--sqlite-path FILE`, `--out DIR` (default `results/<version>`), `--no-write`, `--quick`.

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

## Comparing two builds of zerobucket

Timing one build and then the other, minutes apart, lets machine noise pose as a difference: on identical images the two
measurements disagreed by up to 25 percentage points. `compare-builds` loads both builds in one process and alternates their
reads, so drift affects both equally.

```bash
mkdir ..\a ..\b                  # (or mkdir -p ../a ../b)
cd ../a && npm init -y && npm install zerobucket@0.10.0 pg sharp && cd ../node
cd ../b && npm init -y && npm install zerobucket@0.10.1 pg sharp && cd ../node
npm run compare-builds -- --a ../a --b ../b --rounds 41 --sizes 100KB,1MB,5MB,10MB
```

Sizes can be a preset (`1MB`) or any number with a unit (`700KB`, `1.5MB`). For memory, use `read-cost`: it measures each read in a
fresh process started by a tiny launcher, because on Linux a child process begins with its parent's current size as its peak-memory
counter, which would hide the read entirely if the parent were large.

## Comparing with the Python package

The Python harness is `benchmarks/run_benchmark.py` in the repo root. Run it against the same database on the same machine,
or the numbers say nothing about the packages. For compression, `npm run compression -- --dump ./fixtures` writes the
original images and `python compare_python.py ./fixtures` runs Pillow on those exact files.

## Files

```
src/run.ts               latency, memory, event-loop and throughput runner
src/compression.ts       compression and SSIM sweep
src/validation-cost.ts   timing of each validation step (old stats(), bare decode, the new pipeline)
src/read-cost.ts         PostgreSQL read time and peak memory (each in a fresh process)
src/compare-builds.ts    two builds in one process, reads alternated round by round
src/images.ts            seeded test-image generators
src/ssim.ts              SSIM, matching scikit-image's defaults
compare_python.py        Pillow on the same originals, for the compression comparison
results/<version>/       results.csv, concurrency.csv, environment.json, postgres-reads.csv from the maintainer's runs (0.9.0 = baseline)
```
