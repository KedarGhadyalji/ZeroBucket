# ZeroBucket npm Benchmark Results

**Package measured:** `zerobucket` **0.10.0** for writes, validation and the SQLite and MariaDB tables, and **0.10.1** for reads from
PostgreSQL (see [Reading from PostgreSQL](#reading-from-postgresql-0100-to-0101)), installed from the packed tarball. The 0.9.0 baseline (the version before
the validation change described below) is kept in `results/0.9.0/` so the before and after can be reproduced.

**Environment:** one container, **1 vCPU** (Intel Xeon), 4 GB RAM, Linux, Node 22.22.2, sharp 0.35.5 (libvips 8.18.7),
PostgreSQL 16.15, SQLite 3.53.4 (temp file), MariaDB 10.11.14. Databases on the same machine over localhost, so there
is no real network hop. Test images are random-noise JPEGs at quality 90 (hard to compress, like a real photo), built
by the same recipe as the Python harness (`benchmarks/run_benchmark.py`).

**Method:** per size, 2 warm-up runs, then 7 measured runs; medians are shown (min/max are in `results/0.10.0/results.csv`).
`Validate` is `validateImage()` alone; `Put` includes validation, the checksum and the database write; `Stored` is
what the database reports for the data column. Memory is the largest growth of the process's resident memory during
the call. `Loop stall` is the longest gap between event-loop ticks during the call, beyond the 1 ms sampling interval;
**with nothing running the same monitor reads 0.3 ms**.

Run it yourself: see [README.md](README.md). Raw numbers: `results/`.

> **Directional, not authoritative.** One shared single-core container is not production hardware. Absolute times will
> be lower on a real multi-core host; the _shape_ (what dominates, what scales with what) is what to take from this.

## What changed in 0.10.0, and what it did

The 0.9.1 benchmarks found that `put()` was several times slower than Python's, almost entirely in validation: after reading
the header, `validateImage()` forced a full pixel decode with sharp's `stats()`, which also computes entropy, sharpness and
the dominant colour (none of it used) and keeps the whole decoded image in memory. 0.10.0 forces the same decode by streaming
every scanline through a one-pixel-wide extract and discarding it.

**PostgreSQL, 0.9.0 to 0.10.0 (medians):**

| Size  | Validate before | Validate after | `put()` before | `put()` after | `put()` speed-up |
| ----- | --------------- | -------------- | -------------- | ------------- | ---------------- |
| 10KB  | 7.3ms           | 1.6ms          | 9.4ms          | 3.8ms         | 2.5x             |
| 100KB | 15.0ms          | 2.9ms          | 18.0ms         | 5.7ms         | 3.2x             |
| 500KB | 49.5ms          | 8.4ms          | 59.2ms         | 17.9ms        | 3.3x             |
| 1MB   | 132.7ms         | 19.6ms         | 151.9ms        | 41.9ms        | 3.6x             |
| 5MB   | 533.6ms         | 72.4ms         | 635.5ms        | 170.3ms       | 3.7x             |
| 10MB  | 1208.3ms        | 155.5ms        | 1383.3ms       | 353.3ms       | 3.9x             |

SQLite and MariaDB improve the same way: `put()` at 10MB went from 1273ms to 210ms (SQLite) and from 1222ms to 203ms
(MariaDB). `put()` throughput at 100KB rose from about 60-70 per second to about **190-230 (PostgreSQL), 340-375 (SQLite) and
210-340 (MariaDB)**. The gain is larger on faster databases because validation was nearly all of the old cost.

**Memory.** Measured in a fresh process per case, using the operating system's peak-memory counter (so allocator reuse
cannot hide anything), peak growth during validation was:

| Image                         | `stats()` (0.9.x) | New (0.10.0) |
| ----------------------------- | ----------------- | ------------ |
| 45.6-megapixel JPEG, 11.5MB   | 148MB             | 11MB         |
| 20.3-megapixel JPEG, 5.5MB    | 68MB              | 8MB          |
| 12-megapixel PNG              | 43MB              | 7MB          |
| 12-megapixel progressive JPEG | 78MB              | 41MB         |
| 12-megapixel interlaced PNG   | 38MB              | 35MB         |
| 12-megapixel WebP             | 93MB              | 93MB         |

The gain depends on the format: baseline JPEG and non-interlaced PNG now stream, while progressive JPEG, interlaced PNG and
WebP still need the whole image in memory because of how those formats are encoded (libwebp decodes into a full buffer
regardless). The in-process `Put peak mem` column in the tables below shows about 0MB where 0.9.0 showed 58MB at 5MB and
130MB at 10MB; the fresh-process figures above are the more reliable ones.

**Stage by stage, on identical files** (median of 5, one core), `npm run validation-cost`:

| Image  | Pixels | `stats()` (0.9.x) | bare decode `raw().toBuffer()` | `resize(1,1)` (rejected) | column extract (0.10.0) | Pillow `verify()`+`load()` |
| ------ | ------ | ----------------- | ------------------------------ | ------------------------ | ----------------------- | -------------------------- |
| 10KB   | 0.0MP  | 6.4ms             | 0.8ms                          | 1.7ms                    | 0.8ms                   | 0.2ms                      |
| 88KB   | 0.3MP  | 14.2ms            | 2.3ms                          | 2.3ms                    | 1.9ms                   | 1.6ms                      |
| 516KB  | 1.7MP  | 47.5ms            | 8.1ms                          | 5.6ms                    | 7.3ms                   | 6.4ms                      |
| 1.3MB  | 5.1MP  | 129.9ms           | 20.3ms                         | 12.0ms                   | 17.8ms                  | 16.9ms                     |
| 5.5MB  | 20.3MP | 555.7ms           | 99.8ms                         | 43.4ms                   | 67.5ms                  | 104.4ms                    |
| 11.5MB | 45.6MP | 1146.5ms          | 212.7ms                        | 91.7ms                   | 147.1ms                 | 199.9ms                    |

The new step is as fast as Pillow's full decode up to about 1MB and faster above that, because it never builds the decoded
image. For tiny images Pillow still wins on fixed per-call cost (0.2ms against 0.8ms), which does not matter in practice.

### Why not `resize(1, 1)`, which is faster still

It is, and it was my first candidate. It was rejected because it is **weaker**: JPEG and WebP shrink-on-load decodes at reduced
scale and then does not notice a file that is only missing its last few bytes, which is exactly what an interrupted upload
produces. A fuzz run over 2,462 damaged files found 9 JPEGs, missing their last 1-100 bytes, that the old check rejected and
`resize(1, 1)` accepted. The column extract had zero such cases.

### How the change was verified

- **Equivalence on 2,462 damaged files.** Twenty valid base images (baseline, progressive, 4:4:4, greyscale and CMYK JPEG; RGB,
  RGBA, greyscale, palette, 16-bit and interlaced PNG; lossy, lossless, alpha and near-lossless WebP; at 256x192 and 1200x900),
  each truncated, tail-clipped, bit-flipped, byte-overwritten, zero-filled, spliced and padded. The new check was **never weaker**
  than the old one (0 files). Where both rejected, the error text was identical in all 1,250 cases.
- **123 files the old check accepted are now rejected: 122 interlaced PNGs and one bit-flipped palette PNG.** See below.
- **The tests fail on the bad versions.** `tests/validation.test.ts` (22 tests) was run against the old `stats()` version (3
  fail: the oracle comparison, the interlaced-PNG case and memory at 118MB against a 40MB limit) and against the `resize(1,1)`
  version (2 fail: the oracle comparison and the interrupted-upload test), and passes on the new one.
- The full suite passes: **395 tests** (0.10.1) on PostgreSQL 16, MariaDB 10.11 and an S3 emulator.

### A hole this closed, and a behaviour change

**sharp's `stats()` accepted a truncated interlaced PNG.** An interlaced PNG cut to 30% of its size passed validation in 0.9.x
and was stored; a plain full decode rejects it, and so does the Python package. The old check was blind to damage in interlaced
PNGs generally: in the first fuzz run it rejected none of 123 damaged variants of one interlaced PNG, while a full decode
rejected 119 of them. In 0.10.0 those are rejected with a normal "truncated or corrupted"
error. This is a behaviour change: a damaged interlaced PNG that 0.9.x stored will now fail `put()`. Images already stored are
not re-validated (`zerobucket verify` re-checksums bytes), so nothing existing is affected.

**Unchanged on purpose:** a JPEG missing only its final end-of-image marker (its last 1-2 bytes, with all pixels intact) is
still rejected by npm, as before. The Python package accepts it. That is a real difference between the packages, recorded in
the README's Differences table, not something this release alters.

## Reading from PostgreSQL: 0.10.0 to 0.10.1

The 0.10.0 tables below showed PostgreSQL reads as the slowest of the three backends (136ms to read a 11.5MB image, against 13ms on
MariaDB and 5ms on SQLite) and I had not found out why. 0.10.1 is the result of finding out.

**Why it is slow.** PostgreSQL sends a `bytea` column as **hex text**, two characters per byte, so an 11.5MB image crosses the wire as
about 24MB, is built into one 24MB string by node-postgres, and is decoded back. The cost is roughly linear (7 to 11ms per MB, from 1MB
to 46MB) and about a third of it is PostgreSQL itself formatting the hex (38ms of 104ms for this image, measured with a query that
returns only the length of the encoded value). Things I expected to matter and measured not to:

- **Decoding the hex** is not the cost: reading it and not decoding it took as long.
- **A quadratic buffer copy** in the protocol parser: it grows its buffer by doubling.
- **node-postgres's binary mode** (`binary: true`) is not an option. It converts every field to a UTF-8 string before any column parser
  runs, so a `bytea` comes back **corrupted** (wrong bytes, not just slow), and it was slower too (279ms against 111ms).

**What changed.** Reads of whole images and of ranges now ask PostgreSQL for `encode(data, 'base64')` and decode it with
`Buffer.from(..., 'base64')`: 33% fewer bytes on the wire, and byte-for-byte identical results (9 new tests insert arbitrary bytes
directly and demand an exact match from `get`, `getMany`, ranged `getStream` and dedup mode, at every length around base64 padding and
PostgreSQL's 76-character line wrapping).

**Speed**, both builds loaded in one process and read alternately (41 rounds, medians), PostgreSQL on the same machine:

| Size   | `get()` 0.10.0 | `get()` 0.10.1 | change | full stream 0.10.0 | full stream 0.10.1 | change |
| ------ | -------------- | -------------- | ------ | ------------------ | ------------------ | ------ |
| 88KB   | 1.0ms          | 0.8ms          | -17%   | 1.2ms              | 1.0ms              | -16%   |
| 516KB  | 2.5ms          | 2.8ms          | +13%   | 4.1ms              | 3.1ms              | -23%   |
| 1.3MB  | 7.8ms          | 8.8ms          | +13%   | 7.9ms              | 9.0ms              | +14%   |
| 2.2MB  | 16.8ms         | 13.1ms         | -22%   | 18.7ms             | 13.0ms             | -30%   |
| 5.5MB  | 47.4ms         | 33.3ms         | -30%   | 36.7ms             | 29.5ms             | -20%   |
| 11.5MB | 103.4ms        | 72.3ms         | -30%   | 81.4ms             | 65.2ms             | -20%   |

"Full stream" is how `createImageHandler` serves an image: `metadata()` then 1MB chunk reads. The gain is solid from about 2MB up
(20 to 30%) and small below 300KB. **In the middle, roughly 0.5MB to 1.3MB, there is no reliable gain and probably a small cost.** The
rows for 516KB and 1.3MB changed sign between runs, so I repeated just those two with 201 interleaved rounds: 516KB `get()` 2.4 to
2.8ms (+15%) and streaming 2.9 to 3.1ms (+8%); 1.3MB `get()` 8.6 to 8.0ms (-7%) and streaming 6.8 to 7.5ms (+10%). That is a cost of
under a millisecond, I do not know its cause, and it is on localhost, where fewer bytes on the wire cost nothing. Over a real network
between the application and the database 33% fewer bytes should help at every size, but that is reasoning, not something I measured.

**Memory**, each case in a fresh process using the operating system's peak-memory counter (so allocator reuse cannot hide anything),
median of 5, real JPEGs from `npm run read-cost`:

| Size   | `get()` 0.10.0 | `get()` 0.10.1 | full stream 0.10.0 | full stream 0.10.1 |
| ------ | -------------- | -------------- | ------------------ | ------------------ |
| 516KB  | 7MB            | 4MB            | 7MB                | 4MB                |
| 1.3MB  | 18MB           | 10MB           | 15MB               | 9MB                |
| 5.5MB  | 52MB           | 43MB           | 21MB               | 23MB               |
| 11.5MB | 110MB          | 66MB           | 26MB               | 43MB               |

`get()` needs 37 to 47% less memory at every size. A one-off check at larger sizes (random bytes inserted directly, not a script in
this repository) shows the same: 12MB 116 to 61MB, 24MB 264 to 169MB, 48MB 528 to 339MB, 96MB **1027 to 646MB**. Note what the old
figure means: reading an image with `get()` needed about **ten times the image's size** in memory; it is now about six and a half.
A real fix for very large images is to stream them, which `createImageHandler` already does.

**Streaming memory is bounded either way and I am not claiming a saving.** At 12 to 96MB it peaked at 27 to 76MB on 0.10.0 and 44 to
61MB on 0.10.1, higher for 0.10.1 at 12 and 24MB and lower at 48 and 96MB. That is garbage-collection timing, not a cost that grows
with the image.

**How to reproduce:** `npm run read-cost` times and measures one build; `npm run compare-builds -- --a <folder> --b <folder>` loads
two installed builds in one process and alternates their reads, which is the fair way to compare them on a noisy machine (timing one
and then the other, minutes apart, produced differences of up to 25 percentage points on identical images).

## PostgreSQL 16 (0.10.0 numbers; its Get columns are superseded by the section above)

| Size  | Actual | Stored | Validate | Put     | Get     | Put peak mem | Get peak mem | Put loop stall | Get loop stall |
| ----- | ------ | ------ | -------- | ------- | ------- | ------------ | ------------ | -------------- | -------------- |
| 10KB  | 10KB   | 10KB   | 1.6ms    | 3.8ms   | 1.1ms   | 0.0MB        | 0.0MB        | 0.4ms          | 0.0ms          |
| 100KB | 88KB   | 88KB   | 2.9ms    | 5.7ms   | 1.6ms   | 0.0MB        | 0.0MB        | 0.5ms          | 0.0ms          |
| 500KB | 516KB  | 516KB  | 8.4ms    | 17.9ms  | 4.2ms   | 0.0MB        | 1.0MB        | 3.8ms          | 0.0ms          |
| 1MB   | 1.3MB  | 1.3MB  | 19.6ms   | 41.9ms  | 9.1ms   | 0.0MB        | 2.5MB        | 2.8ms          | 1.8ms          |
| 5MB   | 5.5MB  | 5.5MB  | 72.4ms   | 170.3ms | 48.4ms  | 0.3MB        | 11.0MB       | 7.8ms          | 7.9ms          |
| 10MB  | 11.5MB | 11.5MB | 155.5ms  | 353.3ms | 136.4ms | 0.1MB        | 45.9MB       | 12.0ms         | 13.3ms         |

## SQLite 3

| Size  | Actual | Stored | Validate | Put     | Get   | Put peak mem | Get peak mem | Put loop stall | Get loop stall |
| ----- | ------ | ------ | -------- | ------- | ----- | ------------ | ------------ | -------------- | -------------- |
| 10KB  | 10KB   | 10KB   | 1.6ms    | 1.8ms   | 0.2ms | 0.0MB        | 0.0MB        | 0.0ms          | 0.0ms          |
| 100KB | 88KB   | 88KB   | 2.9ms    | 3.2ms   | 0.2ms | 0.0MB        | 0.0MB        | 0.1ms          | 0.0ms          |
| 500KB | 516KB  | 516KB  | 8.3ms    | 9.6ms   | 0.4ms | 0.0MB        | 0.0MB        | 1.8ms          | 0.0ms          |
| 1MB   | 1.3MB  | 1.3MB  | 19.5ms   | 22.5ms  | 0.5ms | 0.0MB        | 0.0MB        | 0.3ms          | 0.0ms          |
| 5MB   | 5.5MB  | 5.5MB  | 72.1ms   | 103.0ms | 2.3ms | 0.3MB        | 0.0MB        | 0.4ms          | 0.0ms          |
| 10MB  | 11.5MB | 11.5MB | 154.2ms  | 210.1ms | 4.8ms | 0.1MB        | 0.0MB        | 3.8ms          | 0.0ms          |

## MariaDB 10.11 (MySQL adapter)

| Size  | Actual | Stored | Validate | Put     | Get    | Put peak mem | Get peak mem | Put loop stall | Get loop stall |
| ----- | ------ | ------ | -------- | ------- | ------ | ------------ | ------------ | -------------- | -------------- |
| 10KB  | 10KB   | 10KB   | 1.5ms    | 3.1ms   | 0.7ms  | 0.0MB        | 0.0MB        | 0.0ms          | 0.0ms          |
| 100KB | 88KB   | 88KB   | 2.7ms    | 5.2ms   | 0.9ms  | 0.0MB        | 0.0MB        | 0.7ms          | 0.0ms          |
| 500KB | 516KB  | 516KB  | 8.3ms    | 11.5ms  | 1.2ms  | 0.0MB        | 0.0MB        | 1.4ms          | 0.0ms          |
| 1MB   | 1.3MB  | 1.3MB  | 19.3ms   | 25.3ms  | 1.9ms  | 0.0MB        | 0.0MB        | 3.2ms          | 0.0ms          |
| 5MB   | 5.5MB  | 5.5MB  | 72.1ms   | 91.0ms  | 7.0ms  | 0.0MB        | 0.0MB        | 7.7ms          | 1.5ms          |
| 10MB  | 11.5MB | 11.5MB | 154.7ms  | 203.2ms | 12.6ms | 0.0MB        | 0.0MB        | 14.1ms         | 1.8ms          |

## Throughput (100KB image, 200 operations, library-default pool of 5)

| Backend    | Concurrency | put ops/s | put p50 / p95  | get ops/s | get p50 / p95 |
| ---------- | ----------- | --------- | -------------- | --------- | ------------- |
| PostgreSQL | 1           | 192       | 5.1 / 6.2ms    | 713       | 0.8 / 4.7ms   |
| PostgreSQL | 5           | 219       | 21.2 / 34.4ms  | 800       | 4.6 / 14.1ms  |
| PostgreSQL | 20          | 226       | 88.6 / 115.5ms | 813       | 21.7 / 32.6ms |
| SQLite     | 1           | 342       | 2.6 / 3.1ms    | 12140     | 0.0 / 0.1ms   |
| SQLite     | 5           | 357       | 12.5 / 25.8ms  | 9032      | 0.3 / 0.6ms   |
| SQLite     | 20          | 375       | 53.6 / 76.1ms  | 16958     | 1.1 / 1.9ms   |
| MariaDB    | 1           | 207       | 3.8 / 5.4ms    | 2188      | 0.3 / 1.1ms   |
| MariaDB    | 5           | 330       | 13.8 / 24.6ms  | 2298      | 1.3 / 4.5ms   |
| MariaDB    | 20          | 342       | 57.4 / 79.3ms  | 2305      | 4.9 / 25.9ms  |

## What this tells us

**Stored size equals actual size, exactly, on every backend.** JPEG bytes are already near-maximum entropy, so PostgreSQL's
TOAST compression saves nothing. Same finding as the Python package: the pitch is "no second service", not "smaller than a file".

**Validation is no longer most of `put()`.** It is now about 40-55% of `put()` on PostgreSQL (155ms of 353ms at 10MB), about 70-90%
on SQLite and about 50-80% on MariaDB, so the database write is a real share and `put()` latency now depends on the backend you chose.

**The event loop stays responsive.** The decode runs on libuv's thread pool, and the longest stall during a `put()` was
0.0-3.8ms up to 1MB and 3.8-14ms at 10MB, against 0.3ms idle. Reads stall it by at most about 13ms.

**Memory.** See the fresh-process table above. After this change an 11.5MB, 45-megapixel JPEG costs about 11MB of transient
memory to validate instead of about 148MB. Formats that cannot stream (WebP, progressive JPEG, interlaced PNG) still cost
roughly the decoded image, so cap `maxBytes` and `maxPixels` on public endpoints regardless. Treat the `Get peak mem` columns as
a lower bound: the allocator keeps memory it already has, so later calls often show 0.0MB. The PostgreSQL `Get peak mem` of 45.9MB
at 11.5MB understates what happened: measured properly in a fresh process it was 110MB, and 0.10.1 reduces it to 66MB (see above).

**Reads are cheap relative to writes**, as in Python: 10MB comes back in 136ms (PostgreSQL, 0.10.0; about 70ms in 0.10.1),
13ms (MariaDB) or 5ms (SQLite). PostgreSQL is the slowest reader at the large sizes because it sends the bytes as hex text; see above.

**Throughput is still CPU-bound here, so do not read scaling from it.** There is one core, so `put()` barely rises with
concurrency (192 to 226 per second on PostgreSQL). Whether more cores help (libuv's pool is 4 threads) was not tested.

## Against the Python package, on the same machine

The Python harness (`benchmarks/run_benchmark.py`, 0.26.0) was run on this same container and database, with PostgreSQL:

| Size  | Python validate | Python put | Python get | npm validate | npm put | npm get |
| ----- | --------------- | ---------- | ---------- | ------------ | ------- | ------- |
| 10KB  | 0.3ms           | 1.4ms      | 0.4ms      | 1.6ms        | 3.8ms   | 1.1ms   |
| 100KB | 2.0ms           | 4.9ms      | 1.0ms      | 2.9ms        | 5.7ms   | 1.6ms   |
| 500KB | 45.3ms          | 59.0ms     | 2.9ms      | 8.4ms        | 17.9ms  | 4.2ms   |
| 1MB   | 16.5ms          | 36.1ms     | 5.5ms      | 19.6ms       | 41.9ms  | 9.1ms   |
| 5MB   | 63.2ms          | 149.1ms    | 23.5ms     | 72.4ms       | 170.3ms | 48.4ms  |
| 10MB  | 166.3ms         | 373.2ms    | 62.4ms     | 155.5ms      | 353.3ms | 136.4ms |

The two harnesses generate their own images, so sizes differ slightly (Python's "1MB" is 1.1MB, npm's is 1.3MB, and Python's
500KB row validates more slowly than its 1MB row, for reasons of the generated image that I did not investigate) and the rows are comparable in trend only. **The two packages are now on par for `put()`**
at 1MB and above. For very small images Python is still faster by a few milliseconds, and its `get()` is faster, most visibly
against PostgreSQL.

## What this means for size guidance

On this machine, with the package as published:

| Image size   | Behaviour                                                                                                               |
| ------------ | ----------------------------------------------------------------------------------------------------------------------- |
| up to ~500KB | `put()` under 20ms, `get()` under 5ms. Feels like any other write.                                                      |
| 0.5-1.5MB    | `put()` roughly 20-45ms. Fine for uploads, including in a loop.                                                         |
| about 5MB    | `put()` about 0.1-0.17s, and a few MB of transient memory for baseline JPEG or PNG.                                     |
| about 10MB   | `put()` about 0.2-0.35s. Measured at 11.5MB; the default `maxBytes` ceiling is 8 MiB. Memory depends on format (above). |

## Caveats

- One single-core container; no network between the app and the database. A managed database adds a fixed per-query
  delay on top of every number.
- `put()` throughput under concurrency is bound by this machine's single core and says nothing about multi-core scaling.
- Noise-JPEG test images stand in for hard-to-compress photos. Real photos with the same file size usually have fewer
  pixels, so decode time and memory will differ.
- The equivalence test covers JPEG, PNG and WebP in the variants listed. It is a large sample, not a proof; the checked-in
  test (`tests/validation.test.ts`) repeats the comparison on 400 damaged files on every run.
- Only MariaDB was measured for the MySQL adapter, not MySQL 8. Windows was not measured.
- Memory in the in-process tables is resident-memory growth, not a heap profile.
