# ZeroBucket npm Benchmark Results

**Package measured:** `zerobucket` 0.9.0 (0.9.1 is documentation-only, same code), installed from the packed tarball.

**Environment:** one container, **1 vCPU** (Intel Xeon), 4 GB RAM, Linux, Node 22.22.2, sharp 0.35.5 (libvips 8.18.7),
PostgreSQL 16.15, SQLite 3.53.4 (temp file), MariaDB 10.11.14. Databases on the same machine over localhost, so there
is no real network hop. Test images are random-noise JPEGs at quality 90 (hard to compress, like a real photo), built
by the same recipe as the Python harness (`benchmarks/run_benchmark.py`).

**Method:** per size, 2 warm-up runs, then 7 measured runs; medians are shown (min/max are in `results/results.csv`).
`Validate` is `validateImage()` alone; `Put` includes validation, the checksum and the database write; `Stored` is
what the database reports for the data column. Memory is the largest growth of the process's resident memory during
the call. `Loop stall` is the longest gap between event-loop ticks during the call, beyond the 1 ms sampling interval;
**with nothing running the same monitor reads 0.3 ms**.

Run it yourself: see [README.md](README.md). Raw numbers: `results/`.

> **Directional, not authoritative.** One shared single-core container is not production hardware. Absolute times will
> be lower on a real multi-core host; the _shape_ (what dominates, what scales with what) is what to take from this.

## PostgreSQL 16

| Size  | Actual | Stored | Validate | Put      | Get     | Put peak mem | Get peak mem | Put loop stall | Get loop stall |
| ----- | ------ | ------ | -------- | -------- | ------- | ------------ | ------------ | -------------- | -------------- |
| 10KB  | 10KB   | 10KB   | 7.3ms    | 9.4ms    | 0.8ms   | 0.0MB        | 0.0MB        | 0.3ms          | 0.0ms          |
| 100KB | 88KB   | 88KB   | 15.0ms   | 18.0ms   | 1.3ms   | 0.0MB        | 0.0MB        | 0.9ms          | 0.0ms          |
| 500KB | 516KB  | 516KB  | 49.5ms   | 59.2ms   | 3.5ms   | 0.0MB        | 1.0MB        | 3.1ms          | 0.0ms          |
| 1MB   | 1.3MB  | 1.3MB  | 132.7ms  | 151.9ms  | 9.5ms   | 0.0MB        | 2.5MB        | 4.4ms          | 1.6ms          |
| 5MB   | 5.5MB  | 5.5MB  | 533.6ms  | 635.5ms  | 45.2ms  | 58.0MB       | 11.0MB       | 8.5ms          | 6.8ms          |
| 10MB  | 11.5MB | 11.5MB | 1208.3ms | 1383.3ms | 121.8ms | 130.4MB      | 45.9MB       | 12.5ms         | 7.5ms          |

## SQLite 3

| Size  | Actual | Stored | Validate | Put      | Get   | Put peak mem | Get peak mem | Put loop stall | Get loop stall |
| ----- | ------ | ------ | -------- | -------- | ----- | ------------ | ------------ | -------------- | -------------- |
| 10KB  | 10KB   | 10KB   | 8.5ms    | 8.1ms    | 0.2ms | 0.0MB        | 0.0MB        | 0.3ms          | 0.0ms          |
| 100KB | 88KB   | 88KB   | 17.5ms   | 15.5ms   | 0.2ms | 0.0MB        | 0.0MB        | 1.3ms          | 0.0ms          |
| 500KB | 516KB  | 516KB  | 51.4ms   | 52.0ms   | 0.3ms | 0.0MB        | 0.0MB        | 1.5ms          | 0.0ms          |
| 1MB   | 1.3MB  | 1.3MB  | 129.2ms  | 135.4ms  | 0.5ms | 0.0MB        | 0.0MB        | 3.0ms          | 0.0ms          |
| 5MB   | 5.5MB  | 5.5MB  | 533.3ms  | 570.0ms  | 1.8ms | 57.9MB       | 0.0MB        | 4.1ms          | 0.0ms          |
| 10MB  | 11.5MB | 11.5MB | 1215.5ms | 1273.4ms | 4.0ms | 130.4MB      | 0.0MB        | 3.5ms          | 0.0ms          |

## MariaDB 10.11 (MySQL adapter)

| Size  | Actual | Stored | Validate | Put      | Get    | Put peak mem | Get peak mem | Put loop stall | Get loop stall |
| ----- | ------ | ------ | -------- | -------- | ------ | ------------ | ------------ | -------------- | -------------- |
| 10KB  | 10KB   | 10KB   | 7.5ms    | 9.0ms    | 0.7ms  | 0.0MB        | 0.0MB        | 0.3ms          | 0.0ms          |
| 100KB | 88KB   | 88KB   | 14.8ms   | 16.9ms   | 0.9ms  | 0.0MB        | 0.0MB        | 1.3ms          | 0.0ms          |
| 500KB | 516KB  | 516KB  | 49.6ms   | 52.1ms   | 1.1ms  | 0.0MB        | 0.0MB        | 3.6ms          | 0.0ms          |
| 1MB   | 1.3MB  | 1.3MB  | 128.5ms  | 133.3ms  | 1.9ms  | 0.0MB        | 0.0MB        | 3.5ms          | 0.0ms          |
| 5MB   | 5.5MB  | 5.5MB  | 530.4ms  | 554.9ms  | 6.7ms  | 58.0MB       | 0.0MB        | 10.7ms         | 1.4ms          |
| 10MB  | 11.5MB | 11.5MB | 1206.7ms | 1222.4ms | 14.6ms | 130.4MB      | 0.0MB        | 16.2ms         | 2.3ms          |

## Throughput (100KB image, 200 operations, library-default pool of 5)

| Backend    | Concurrency | put ops/s | put p50 / p95   | get ops/s | get p50 / p95 |
| ---------- | ----------- | --------- | --------------- | --------- | ------------- |
| PostgreSQL | 1           | 59        | 16.8 / 18.5ms   | 847       | 0.8 / 3.5ms   |
| PostgreSQL | 5           | 60        | 81.3 / 114.3ms  | 772       | 5.2 / 11.4ms  |
| PostgreSQL | 20          | 60        | 329.4 / 378.2ms | 846       | 23.0 / 30.4ms |
| SQLite     | 1           | 69        | 14.0 / 15.2ms   | 9962      | 0.0 / 0.1ms   |
| SQLite     | 5           | 69        | 68.6 / 104.6ms  | 10222     | 0.2 / 0.4ms   |
| SQLite     | 20          | 71        | 279.4 / 304.8ms | 12732     | 1.1 / 6.4ms   |
| MariaDB    | 1           | 66        | 14.9 / 16.0ms   | 1848      | 0.4 / 0.8ms   |
| MariaDB    | 5           | 69        | 69.7 / 103.7ms  | 1796      | 1.7 / 7.3ms   |
| MariaDB    | 20          | 67        | 295.8 / 332.7ms | 2103      | 7.1 / 21.9ms  |

## What this tells us

**Stored size equals actual size, exactly, on every backend.** JPEG bytes are already near-maximum entropy, so
PostgreSQL's TOAST compression saves nothing. Same finding as the Python package: the pitch is "no second service",
not "smaller than a file".

**`put()` time is almost entirely validation.** Validation is roughly 80-99% of `put()` from 100KB upward (on SQLite the two are within noise of each other), so the
database write is a minor share on all three backends. That makes `put()` latency track the image's pixel count, not
the database you chose.

**The event loop stays responsive.** The decode runs on libuv's thread pool, and the longest stall during a `put()`
was 0.3-4.4ms up to 1MB and 3.5-16ms at 10MB, against 0.3ms idle. Reads stall it by at most 8ms. A request handler
awaiting `put()` does not freeze other requests, but it does still occupy CPU (see throughput).

**Memory scales with pixels, not file size.** Putting an 11.5MB JPEG (about 45 megapixels) grew resident memory by about
130MB, roughly 11x the file, and a 5.5MB one by 58MB. That is the transient decoded image. Several large uploads at once
multiply it, so cap `maxBytes` and `maxPixels` for public endpoints. Treat the `Get peak mem` columns as a lower
bound: after a large `put()` the allocator keeps memory it already has, so later calls in the same process often show
0.0MB (the SQLite and MariaDB `get` columns). PostgreSQL's 45.9MB get at 11.5MB is a real signal about the driver's
buffering; I did not investigate its cause.

**Throughput is CPU-bound here, so do not read scaling from it.** `put()` stays at about 60-70 per second at every
concurrency level because there is one core and each put is a decode. Whether more cores raise it (libuv's default pool is
4 threads) was not tested. `get()` is limited by the database: about 800/s on PostgreSQL, about 2,000/s on MariaDB and
10,000+/s on SQLite, all over localhost with no network.

**Reads are cheap relative to writes**, as in Python: 10MB comes back in 122ms (PostgreSQL), 15ms (MariaDB) or 4ms
(SQLite), against 1.2-1.4s to store it. PostgreSQL is the slowest reader at the large sizes; the cause was not
investigated.

## Against the Python package, on the same machine

The Python harness (`benchmarks/run_benchmark.py`, 0.26.0) was run on this same container and database, with PostgreSQL:

| Size  | Python validate | Python put | Python get | npm validate | npm put  | npm get |
| ----- | --------------- | ---------- | ---------- | ------------ | -------- | ------- |
| 10KB  | 0.3ms           | 1.3ms      | 0.4ms      | 7.3ms        | 9.4ms    | 0.8ms   |
| 100KB | 1.5ms           | 4.7ms      | 0.8ms      | 15.0ms       | 18.0ms   | 1.3ms   |
| 500KB | 34.9ms          | 47.6ms     | 2.2ms      | 49.5ms       | 59.2ms   | 3.5ms   |
| 1MB   | 14.0ms          | 34.0ms     | 6.1ms      | 132.7ms      | 151.9ms  | 9.5ms   |
| 5MB   | 74.2ms          | 156.4ms    | 22.5ms     | 533.6ms      | 635.5ms  | 45.2ms  |
| 10MB  | 161.2ms         | 361.4ms    | 56.3ms     | 1208.3ms     | 1383.3ms | 121.8ms |

The two harnesses generate their own images, so sizes differ slightly (Python's "1MB" is 1.1MB, npm's is 1.3MB) and
the rows are comparable in trend only. **On the npm side, `put()` is several times slower than Python's.** The next section
shows why, on identical files.

## Why npm validation is slower: it is one function call

`validateImage()` reads the header with `metadata()`, then forces a full pixel decode with `sharp(...).stats()` so that
truncated files are rejected. `stats()` does more than decode: it also computes per-channel statistics, entropy,
sharpness and the dominant colour, none of which validation uses. Timed on the same bytes (median of 5, one core):

| Image  | Pixels | `metadata()` | `stats()` (current) | bare decode `raw().toBuffer()` | Pillow `verify()` + `load()` |
| ------ | ------ | ------------ | ------------------- | ------------------------------ | ---------------------------- |
| 10KB   | 0.0MP  | 0.2ms        | 6.2ms               | 0.8ms                          | 0.3ms                        |
| 88KB   | 0.3MP  | 0.2ms        | 13.4ms              | 2.1ms                          | 1.6ms                        |
| 516KB  | 1.7MP  | 0.1ms        | 46.3ms              | 7.7ms                          | 6.6ms                        |
| 1.3MB  | 5.1MP  | 0.1ms        | 123.6ms             | 22.8ms                         | 17.5ms                       |
| 5.5MB  | 20.3MP | 0.1ms        | 475.0ms             | 94.7ms                         | 105.8ms                      |
| 11.5MB | 45.6MP | 0.2ms        | 1129.8ms            | 206.5ms                        | 200.4ms                      |

A bare sharp decode matches Pillow, so **sharp is not slow; the extra `stats()` work is**: about 5-6x the cost of the decode.
A `resize(1,1)` pipeline, sharp's cheapest way to force a full decode, was faster still (90ms at 45.6MP).

**This is a possible follow-up, not a change made here.** A small check suggests the cheaper paths do not weaken
validation: on 12 cases (JPEG, PNG and WebP, each intact, truncated at 60%, truncated at 99%, and with 200 corrupted
bytes in the middle), `stats()`, the bare decode and the `resize(1,1)` pipeline accepted and rejected exactly the same
ones. That is a small sample and not proof. Changing it would alter library behaviour on edge cases, so it should come with
its own tests (including the HEIC path, which uses a different validator) and be released deliberately. Not measured: peak
memory of the alternatives.

## What this means for size guidance

On this machine, with the package as published:

| Image size   | Behaviour                                                                                                                |
| ------------ | ------------------------------------------------------------------------------------------------------------------------ |
| up to ~500KB | `put()` under 60ms, `get()` under 5ms. Feels like any other write.                                                       |
| 0.5-1.5MB    | `put()` roughly 60-160ms. Fine for uploads; be careful in a tight loop.                                                  |
| about 5MB    | `put()` about 0.6s and about 58MB of transient memory per upload.                                                        |
| about 10MB   | `put()` about 1.2-1.4s and about 130MB of transient memory. Measured at 11.5MB; the default `maxBytes` ceiling is 8 MiB. |

These are more conservative than the Python table for the same sizes, because of the validation cost above.

## Caveats

- One single-core container; no network between the app and the database. A managed database adds a fixed per-query
  delay on top of every number.
- `put()` throughput under concurrency is bound by this machine's single core and says nothing about multi-core scaling.
- Noise-JPEG test images stand in for hard-to-compress photos. Real photos with the same file size usually have fewer
  pixels, so decode time and memory will differ.
- Only MariaDB was measured for the MySQL adapter, not MySQL 8.
- Windows was not measured.
- Memory numbers are resident-memory growth (see above), not heap profiles.
