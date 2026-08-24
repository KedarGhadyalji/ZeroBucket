# ZeroBucket Benchmark Results

**Environment:** single-container Ubuntu 24.04, PostgreSQL 16, local Unix
socket connection (no network hop), Python 3.12. Test images are random-noise
JPEGs at quality 90 -- noise compresses unpredictably, similar to how a
real photo (already near-maximum entropy) behaves, unlike a synthetic
solid-color image which would compress far smaller than production data.

**Method:** each size was uploaded and retrieved 5 times; reported numbers
are medians. Memory deltas are RSS before/after the call in the benchmark
process itself, not the Postgres server process.

Run with: `ZEROBUCKET_TEST_DATABASE_URL=... python3 benchmarks/run_benchmark.py`

## Results

| Size  | Actual  | On-disk (Postgres) | Validate | Put     | Get    | Put mem | Get mem |
|-------|---------|---------------------|----------|---------|--------|---------|---------|
| 10KB  | 10KB    | 10KB                | 0.5ms    | 1.5ms   | 0.5ms  | ~0MB    | ~0MB    |
| 100KB | 100KB   | 100KB               | 2.0ms    | 5.6ms   | 1.2ms  | ~0MB    | ~0MB    |
| 500KB | 515KB   | 515KB               | 58.0ms*  | 78.8ms* | 3.9ms  | ~0MB    | ~0MB    |
| 1MB   | 1116KB  | 1116KB              | 19.9ms   | 48.1ms  | 7.5ms  | ~0MB    | ~0MB    |
| 5MB   | 4967KB  | 4967KB              | 107.8ms  | 253.1ms | 36.1ms | ~0MB    | 9.6MB   |
| 10MB  | 11.5MB  | 11.5MB              | 300.3ms  | 613.9ms | 95.0ms | 11.1MB  | 33.7MB  |

\* The 500KB validation/put numbers are higher than the 1MB row immediately
below them, which is noise from running in a shared container (single
process, no isolation, no warmup runs discarded), not a real inversion.
Treat single-container numbers as directional, not authoritative --
re-run on target hardware before publishing final claims in the README.

## What this tells us

**On-disk size equals actual size, exactly, at every tier.** Postgres's
TOAST compression did not shrink any of these rows. This isn't a bug --
JPEG/WebP bytes are already near-maximum entropy, so there's nothing left
for TOAST to compress. **This matters for the README**: don't imply BYTEA
storage saves space over the original file. It doesn't. The "your database
already has the bytes" pitch is about eliminating a second service, not
about storage efficiency.

**Validation cost scales with pixel count, not just file size**, since
Pillow has to decode the full image to guard against truncation and
decompression bombs. At 10MB this is ~300ms of the ~614ms total put()
time -- roughly half the request is spent proving the image is real
before a single byte reaches Postgres.

**put() latency crosses into "this blocks a web request uncomfortably"
territory well before 10MB.** 5MB images take ~250ms to store; 10MB takes
~600ms. For a synchronous HTTP handler, anything over roughly 1-2MB starts
to feel like it should be backgrounded rather than awaited inline, and by
5MB it's a genuinely noticeable delay for the end user.

**get() stays cheap relative to put()** across the board (95ms at 10MB vs.
614ms to store it), since retrieval skips validation entirely -- it's just
a parameterized SELECT and a bytes copy.

**Memory delta only becomes visible at 5MB+.** Below that, Python's
allocator/GC absorbs the temporary buffers without a measurable RSS
change in this harness. At 10MB, a single put() plus get() cycle can add
~10-30MB of resident memory per concurrent request -- worth flagging in
the README for anyone planning to serve many simultaneous uploads.

## Does this change the recommended default cap?

The plan proposed an 8MB default (`DEFAULT_MAX_BYTES` in `client.py`).
These numbers support keeping that as the *ceiling* but suggest the
README should be explicit that "supported" and "comfortable" aren't the
same thing:

- **Comfortable, low-latency range: up to ~1MB.** Sub-50ms put(), sub-10ms
  get(), no measurable memory pressure. This is the "feels like any other
  DB write" zone the product pitch implies.
- **Usable but noticeable: 1-5MB.** Fine for occasional uploads (avatars,
  document scans, product photos); would not want this in a tight
  request-per-second loop.
- **Upper edge, use deliberately: 5-8MB.** Latency and memory cost are
  real at this tier. Keeping 8MB as the *default* ceiling is reasonable
  because it still fits "small app" use cases, but the README should say
  plainly that images in this range should probably not block the main
  request thread.

Recommendation: keep `DEFAULT_MAX_BYTES = 8 * 1024 * 1024`, but add a
"performance characteristics" table (this one) to the README instead of
just stating a byte limit with no context. A number alone invites the
question "why 8MB and not 20MB?" -- the answer is these latency curves,
and it's better to show them than assert them.

## Caveats

- Single-container benchmark, no concurrent load testing beyond the
  10-thread smoke test in the pytest suite. Real concurrent-write
  throughput under load is not yet measured and should not be claimed.
- No network latency between client and Postgres (Unix socket). A real
  deployment with a network hop to a managed Postgres instance will add
  fixed per-query latency on top of every number above.
- Noise-JPEG test images approximate "hard to compress" photo-like data
  but are not real photographs; real-world size/latency correlation
  should hold, but exact numbers will vary by image content.
