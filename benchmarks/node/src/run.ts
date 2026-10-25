/**
 * Latency, memory and event-loop benchmark for the zerobucket npm package.
 *
 * Mirrors benchmarks/run_benchmark.py (the Python package's harness): six target sizes from 10KB to 10MB, and for each
 * size the validation time, put() time, get() time, peak memory growth and the stored size in the database. It adds
 * what matters on Node: how long the event loop is stalled during an operation, and throughput under concurrency.
 *
 * Backends are opt-in by environment variable, and only rows this script creates are ever touched (it never truncates
 * or drops anything):
 *   PostgreSQL  ZEROBUCKET_BENCH_DATABASE_URL  (falls back to ZEROBUCKET_TEST_DATABASE_URL)
 *   MySQL       ZEROBUCKET_BENCH_MYSQL_URL     (falls back to ZEROBUCKET_TEST_MYSQL_URL)
 *   SQLite      always available; a temporary file unless --sqlite-path is given
 *
 *   npm run bench                         full run, writes results/
 *   npm run bench:quick                   smoke run (two small sizes), writes nothing
 *   npm run bench -- --backend postgres --sizes 100KB,1MB --repeats 9
 */
import { createRequire } from "node:module";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import sharp from "sharp";
import { ZeroBucket, validateImage } from "zerobucket";
import { TARGET_SIZES, makeImageOfSize } from "./images.js";

const require = createRequire(import.meta.url);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const MAX_BYTES = 20 * 1024 * 1024; // same ceiling the Python harness uses, so 10MB images are accepted
const MB = 1024 * 1024;

// --------------------------------------------------------------------------------------------------------------
// Arguments
// --------------------------------------------------------------------------------------------------------------

const argv = process.argv.slice(2);
const flag = (name: string) => argv.includes(`--${name}`);
const opt = (name: string): string | undefined => {
  const i = argv.indexOf(`--${name}`);
  return i >= 0 ? argv[i + 1] : undefined;
};

const QUICK = flag("quick");
const WRITE = !flag("no-write") && !QUICK;
const REPEATS = Number(opt("repeats") ?? (QUICK ? 2 : 7));
const WARMUP = Number(opt("warmup") ?? (QUICK ? 1 : 2));
const SIZE_LABELS = (
  opt("sizes") ?? (QUICK ? "10KB,100KB" : Object.keys(TARGET_SIZES).join(","))
).split(",");
const CONC_OPS = Number(opt("concurrency-ops") ?? (QUICK ? 20 : 200));
const CONC_LEVELS = (opt("concurrency") ?? (QUICK ? "1,5" : "1,5,20"))
  .split(",")
  .map(Number);
const WANT = (opt("backend") ?? "postgres,sqlite,mysql").split(",");

for (const l of SIZE_LABELS)
  if (!(l in TARGET_SIZES))
    throw new Error(
      `Unknown size "${l}". Use: ${Object.keys(TARGET_SIZES).join(", ")}`,
    );

// --------------------------------------------------------------------------------------------------------------
// Backends
// --------------------------------------------------------------------------------------------------------------

interface Harness {
  name: "postgres" | "sqlite" | "mysql";
  serverVersion: string;
  make(): ZeroBucket;
  /** Bytes the database reports for one stored image's data column. */
  storedBytes(id: string): Promise<number>;
  close(): Promise<void>;
}

async function postgresHarness(): Promise<Harness | string> {
  const url =
    process.env.ZEROBUCKET_BENCH_DATABASE_URL ??
    process.env.ZEROBUCKET_TEST_DATABASE_URL;
  if (!url)
    return "set ZEROBUCKET_BENCH_DATABASE_URL (or ZEROBUCKET_TEST_DATABASE_URL)";
  let pg: typeof import("pg");
  try {
    pg = (await import("pg")).default as unknown as typeof import("pg");
  } catch {
    return "the pg package is not installed";
  }
  const side = new pg.Client({
    connectionString: url,
    connectionTimeoutMillis: 5000,
  });
  try {
    await side.connect();
  } catch (e) {
    return `cannot connect: ${(e as Error).message}`;
  }
  const version = (
    (await side.query("SHOW server_version")).rows[0] as {
      server_version: string;
    }
  ).server_version;
  return {
    name: "postgres",
    serverVersion: `PostgreSQL ${version}`,
    make: () => new ZeroBucket({ connectionString: url, maxBytes: MAX_BYTES }),
    storedBytes: async (id) => {
      const r = await side.query(
        "SELECT pg_column_size(data) AS n FROM zerobucket_images WHERE id = $1",
        [id],
      );
      return Number((r.rows[0] as { n: string }).n);
    },
    close: () => side.end(),
  };
}

async function sqliteHarness(): Promise<Harness | string> {
  let Database: typeof import("better-sqlite3");
  try {
    Database = (await import("better-sqlite3"))
      .default as unknown as typeof import("better-sqlite3");
  } catch {
    return "better-sqlite3 is not installed (it is an optional dependency here)";
  }
  const file =
    opt("sqlite-path") ??
    path.join(os.tmpdir(), `zerobucket-bench-${process.pid}.db`);
  const own = opt("sqlite-path") === undefined;
  // Create the schema through ZeroBucket first so the side connection can query it.
  const probe = new ZeroBucket({ sqlite: file, maxBytes: MAX_BYTES });
  await probe.close();
  const side = new Database(file);
  const version = (
    side.prepare("SELECT sqlite_version() AS v").get() as { v: string }
  ).v;
  return {
    name: "sqlite",
    serverVersion: `SQLite ${version} (${own ? "temp file in " + os.tmpdir() : file})`,
    make: () => new ZeroBucket({ sqlite: file, maxBytes: MAX_BYTES }),
    storedBytes: async (id) =>
      (
        side
          .prepare(
            "SELECT length(data) AS n FROM zerobucket_images WHERE id = ?",
          )
          .get(id) as { n: number }
      ).n,
    close: async () => {
      side.close();
      if (own)
        for (const suffix of ["", "-wal", "-shm"])
          fs.rmSync(file + suffix, { force: true });
    },
  };
}

async function mysqlHarness(): Promise<Harness | string> {
  const url =
    process.env.ZEROBUCKET_BENCH_MYSQL_URL ??
    process.env.ZEROBUCKET_TEST_MYSQL_URL;
  if (!url)
    return "set ZEROBUCKET_BENCH_MYSQL_URL (or ZEROBUCKET_TEST_MYSQL_URL)";
  let mysql: typeof import("mysql2/promise");
  try {
    mysql = await import("mysql2/promise");
  } catch {
    return "mysql2 is not installed (it is an optional dependency here)";
  }
  let side: Awaited<ReturnType<typeof mysql.createConnection>>;
  try {
    side = await mysql.createConnection(url);
  } catch (e) {
    return `cannot connect: ${(e as Error).message}`;
  }
  const [rows] = await side.query("SELECT VERSION() AS v");
  const version = (rows as { v: string }[])[0]!.v;
  return {
    name: "mysql",
    serverVersion: `MySQL/MariaDB ${version}`,
    make: () => new ZeroBucket({ mysql: url, maxBytes: MAX_BYTES }),
    storedBytes: async (id) => {
      const [r] = await side.query(
        "SELECT LENGTH(data) AS n FROM zerobucket_images WHERE id = ?",
        [id],
      );
      return Number((r as { n: string }[])[0]!.n);
    },
    close: () => side.end(),
  };
}

// --------------------------------------------------------------------------------------------------------------
// Measuring
// --------------------------------------------------------------------------------------------------------------

interface Sample {
  ms: number;
  /** Largest growth of resident memory above the pre-call level, in MB. */
  peakRssMb: number;
  /** Longest gap, in ms, between event-loop ticks beyond the 1 ms sampling interval. */
  stallMs: number;
}

const gc = (globalThis as { gc?: () => void }).gc;

async function measure<T>(
  fn: () => Promise<T>,
): Promise<{ value: T } & Sample> {
  gc?.();
  const base = process.memoryUsage.rss();
  let peak = base;
  const sampler = setInterval(() => {
    const r = process.memoryUsage.rss();
    if (r > peak) peak = r;
  }, 2);
  const loop = monitorEventLoopDelay({ resolution: 1 });
  loop.enable();
  const t0 = performance.now();
  let value!: T;
  let ms = 0;
  try {
    value = await fn();
  } finally {
    ms = performance.now() - t0;
    clearInterval(sampler);
    loop.disable();
  }
  peak = Math.max(peak, process.memoryUsage.rss());
  const stallMs = loop.count > 0 ? Math.max(0, loop.max / 1e6 - 1) : 0;
  return { value, ms, peakRssMb: Math.max(0, peak - base) / MB, stallMs };
}

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  const m = s.length >> 1;
  return s.length % 2 ? s[m]! : (s[m - 1]! + s[m]!) / 2;
};
const pct = (xs: number[], p: number) => {
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.min(s.length - 1, Math.ceil((p / 100) * s.length) - 1)]!;
};

/**
 * What the event-loop monitor reports when nothing is happening, so stalls can be read against it: the median of the
 * worst gap in five 200ms idle windows, taken after a garbage collection.
 */
async function idleControl(): Promise<number> {
  gc?.();
  const worst: number[] = [];
  for (let i = 0; i < 5; i++) {
    const loop = monitorEventLoopDelay({ resolution: 1 });
    loop.enable();
    await new Promise((r) => setTimeout(r, 200));
    loop.disable();
    worst.push(loop.count > 0 ? Math.max(0, loop.max / 1e6 - 1) : 0);
  }
  return median(worst);
}

// --------------------------------------------------------------------------------------------------------------
// One backend
// --------------------------------------------------------------------------------------------------------------

interface SizeRow {
  backend: string;
  size: string;
  actualKb: number;
  storedKb: number;
  validateMs: number;
  putMs: number;
  putMinMs: number;
  putMaxMs: number;
  getMs: number;
  getMinMs: number;
  getMaxMs: number;
  putPeakMb: number;
  getPeakMb: number;
  putStallMs: number;
  getStallMs: number;
}
interface ConcRow {
  backend: string;
  concurrency: number;
  ops: number;
  putPerSec: number;
  putP50: number;
  putP95: number;
  getPerSec: number;
  getP50: number;
  getP95: number;
}

async function runBackend(h: Harness, images: Map<string, Buffer>) {
  const bucket = h.make();
  const rows: SizeRow[] = [];
  const conc: ConcRow[] = [];
  const cleanup: string[] = [];
  try {
    for (const [label, data] of images) {
      process.stderr.write(`  [${h.name}] ${label}\n`);
      for (let w = 0; w < WARMUP; w++) {
        const id = await bucket.put(data);
        await bucket.get(id);
        await bucket.delete(id);
      }
      const val: number[] = [];
      const put: Sample[] = [];
      const get: Sample[] = [];
      const ids: string[] = [];
      for (let r = 0; r < REPEATS; r++) {
        val.push(
          (await measure(() => validateImage(data, { maxBytes: MAX_BYTES })))
            .ms,
        );
        const p = await measure(() => bucket.put(data));
        ids.push(p.value);
        cleanup.push(p.value);
        put.push(p);
        get.push(await measure(() => bucket.get(p.value)));
      }
      const stored = await h.storedBytes(ids[ids.length - 1]!);
      for (const id of ids) await bucket.delete(id);
      cleanup.length = 0;
      const col = (xs: Sample[], k: keyof Sample) => xs.map((s) => s[k]);
      rows.push({
        backend: h.name,
        size: label,
        actualKb: data.length / 1024,
        storedKb: stored / 1024,
        validateMs: median(val),
        putMs: median(col(put, "ms")),
        putMinMs: Math.min(...col(put, "ms")),
        putMaxMs: Math.max(...col(put, "ms")),
        getMs: median(col(get, "ms")),
        getMinMs: Math.min(...col(get, "ms")),
        getMaxMs: Math.max(...col(get, "ms")),
        putPeakMb: median(col(put, "peakRssMb")),
        getPeakMb: median(col(get, "peakRssMb")),
        putStallMs: median(col(put, "stallMs")),
        getStallMs: median(col(get, "stallMs")),
      });
    }

    // Throughput: a 100KB image, N operations spread over `c` concurrent workers.
    const small = await makeImageOfSize(TARGET_SIZES["100KB"]!);
    for (const c of CONC_LEVELS) {
      process.stderr.write(`  [${h.name}] concurrency ${c}\n`);
      const run = async (n: number, op: (i: number) => Promise<void>) => {
        const lat: number[] = [];
        let next = 0;
        const t0 = performance.now();
        await Promise.all(
          Array.from({ length: c }, async () => {
            while (next < n) {
              const i = next++;
              const s = performance.now();
              await op(i);
              lat.push(performance.now() - s);
            }
          }),
        );
        return {
          perSec: (n / (performance.now() - t0)) * 1000,
          p50: median(lat),
          p95: pct(lat, 95),
        };
      };
      const ids: string[] = new Array(CONC_OPS);
      const p = await run(CONC_OPS, async (i) => {
        ids[i] = await bucket.put(small);
        cleanup.push(ids[i]!);
      });
      const g = await run(
        CONC_OPS,
        async (i) => void (await bucket.get(ids[i]!)),
      );
      for (const id of ids) await bucket.delete(id);
      cleanup.length = 0;
      conc.push({
        backend: h.name,
        concurrency: c,
        ops: CONC_OPS,
        putPerSec: p.perSec,
        putP50: p.p50,
        putP95: p.p95,
        getPerSec: g.perSec,
        getP50: g.p50,
        getP95: g.p95,
      });
    }
  } finally {
    // Best-effort removal of anything left behind by a failure part-way through.
    for (const id of cleanup) await bucket.delete(id).catch(() => undefined);
    await bucket.close();
  }
  return { rows, conc };
}

// --------------------------------------------------------------------------------------------------------------
// Output
// --------------------------------------------------------------------------------------------------------------

const f1 = (n: number) => n.toFixed(1);
const kb = (n: number) =>
  n >= 1024 ? `${(n / 1024).toFixed(1)}MB` : `${Math.round(n)}KB`;

function sizeTable(rows: SizeRow[]): string {
  const head =
    "| Size | Actual | Stored | Validate | Put | Get | Put peak mem | Get peak mem | Put loop stall | Get loop stall |";
  const sep = "|---|---|---|---|---|---|---|---|---|---|";
  const body = rows.map(
    (r) =>
      `| ${r.size} | ${kb(r.actualKb)} | ${kb(r.storedKb)} | ${f1(r.validateMs)}ms | ${f1(r.putMs)}ms | ${f1(r.getMs)}ms | ${f1(r.putPeakMb)}MB | ${f1(r.getPeakMb)}MB | ${f1(r.putStallMs)}ms | ${f1(r.getStallMs)}ms |`,
  );
  return [head, sep, ...body].join("\n");
}

function concTable(rows: ConcRow[]): string {
  const head =
    "| Concurrency | put ops/s | put p50 / p95 | get ops/s | get p50 / p95 |";
  const sep = "|---|---|---|---|---|";
  const body = rows.map(
    (r) =>
      `| ${r.concurrency} | ${Math.round(r.putPerSec)} | ${f1(r.putP50)} / ${f1(r.putP95)}ms | ${Math.round(r.getPerSec)} | ${f1(r.getP50)} / ${f1(r.getP95)}ms |`,
  );
  return [head, sep, ...body].join("\n");
}

async function main() {
  if (!gc)
    console.error(
      "note: run with `npm run bench` (node --expose-gc) for steadier memory numbers; none of the others are affected.",
    );
  const zbVersion = (require("zerobucket/package.json") as { version: string })
    .version;
  const makers: Record<string, () => Promise<Harness | string>> = {
    postgres: postgresHarness,
    sqlite: sqliteHarness,
    mysql: mysqlHarness,
  };
  for (const w of WANT)
    if (!makers[w])
      throw new Error(`Unknown backend "${w}". Use: postgres, sqlite, mysql`);

  const harnesses: Harness[] = [];
  for (const name of WANT) {
    const h = await makers[name]!();
    if (typeof h === "string") console.error(`skipping ${name}: ${h}`);
    else harnesses.push(h);
  }
  if (harnesses.length === 0)
    throw new Error(
      "No backend is available. Set ZEROBUCKET_BENCH_DATABASE_URL, or use --backend sqlite.",
    );

  console.error(`Generating test images (${SIZE_LABELS.join(", ")})...`);
  const images = new Map<string, Buffer>();
  for (const label of SIZE_LABELS)
    images.set(label, await makeImageOfSize(TARGET_SIZES[label]!));

  const idle = await idleControl();
  const env = {
    zerobucket: zbVersion,
    node: process.version,
    sharp: sharp.versions.sharp,
    libvips: sharp.versions.vips,
    os: `${os.type()} ${os.release()} (${os.arch()})`,
    cpu: `${os.cpus()[0]?.model.trim()} x${os.cpus().length}`,
    memoryGb: Math.round(os.totalmem() / 1024 ** 3),
    repeats: REPEATS,
    warmup: WARMUP,
    idleLoopStallMs: idle,
    backends: harnesses.map((h) => h.serverVersion),
  };

  const allRows: SizeRow[] = [];
  const allConc: ConcRow[] = [];
  for (const h of harnesses) {
    console.error(`Benchmarking ${h.serverVersion}`);
    try {
      const { rows, conc } = await runBackend(h, images);
      allRows.push(...rows);
      allConc.push(...conc);
    } finally {
      await h.close();
    }
  }

  console.log(
    `zerobucket ${env.zerobucket} on Node ${env.node}, sharp ${env.sharp} (libvips ${env.libvips})`,
  );
  console.log(
    `${env.cpu}, ${env.memoryGb} GB, ${env.os}; ${REPEATS} measured runs per size after ${WARMUP} warm-up runs, medians shown.`,
  );
  console.log(`Idle event-loop control: ${f1(idle)}ms.\n`);
  for (const h of harnesses) {
    console.log(`### ${h.serverVersion}\n`);
    console.log(sizeTable(allRows.filter((r) => r.backend === h.name)));
    console.log(
      `\nThroughput, 100KB image, ${CONC_OPS} operations, library-default pool:\n`,
    );
    console.log(concTable(allConc.filter((r) => r.backend === h.name)));
    console.log();
  }

  if (WRITE) {
    // One folder per zerobucket version, so a before/after comparison keeps both sets of raw numbers.
    const OUT_DIR = path.resolve(
      opt("out") ?? path.join(HERE, "..", "results", zbVersion),
    );
    fs.mkdirSync(OUT_DIR, { recursive: true });
    const csv = (rows: object[]) => {
      const keys = Object.keys(rows[0]!);
      return (
        [
          keys.join(","),
          ...rows.map((r) =>
            keys.map((k) => (r as Record<string, unknown>)[k]).join(","),
          ),
        ].join("\n") + "\n"
      );
    };
    fs.writeFileSync(path.join(OUT_DIR, "results.csv"), csv(allRows));
    fs.writeFileSync(path.join(OUT_DIR, "concurrency.csv"), csv(allConc));
    fs.writeFileSync(
      path.join(OUT_DIR, "environment.json"),
      JSON.stringify(env, null, 2) + "\n",
    );
    console.error(`Wrote ${OUT_DIR}`);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
