/**
 * What does reading an image back cost on PostgreSQL?
 *
 * Times `get()` and a full `getStream()` read (the path createImageHandler uses to serve an image, in 1 MB chunks) at each
 * size, and measures the peak memory each one needs in a FRESH process (the operating system's peak-memory counter, so the
 * allocator reusing memory cannot hide anything). Written to compare 0.10.0, which read bytea as hex text, with 0.10.1, which
 * reads it as base64: install each build in turn and run this.
 *
 *   npm run read-cost
 *   npm run read-cost -- --sizes 1MB,10MB --repeats 15 --no-write
 *
 * Needs ZEROBUCKET_BENCH_DATABASE_URL (or ZEROBUCKET_TEST_DATABASE_URL). Only rows it creates are touched.
 */
import { spawn } from "node:child_process";
import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { ZeroBucket } from "zerobucket";
import { TARGET_SIZES, makeImageOfSize } from "./images.js";

const require = createRequire(import.meta.url);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const URL_ =
  process.env.ZEROBUCKET_BENCH_DATABASE_URL ??
  process.env.ZEROBUCKET_TEST_DATABASE_URL;
const MAX_BYTES = 64 * 1024 * 1024;
const argv = process.argv.slice(2);
const opt = (n: string) =>
  argv.includes(`--${n}`) ? argv[argv.indexOf(`--${n}`) + 1] : undefined;

if (!URL_) {
  console.error(
    "Set ZEROBUCKET_BENCH_DATABASE_URL (or ZEROBUCKET_TEST_DATABASE_URL).",
  );
  process.exit(1);
}

async function readOnce(zb: ZeroBucket, op: string, id: string): Promise<void> {
  if (op === "get") await zb.get(id);
  else for await (const _ of await zb.getStream(id, {})) void _;
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1]!;
const REPEATS = Number(opt("repeats") ?? 9);
const MEM_RUNS = Number(opt("mem-runs") ?? 3);
const LABELS = (opt("sizes") ?? Object.keys(TARGET_SIZES).join(",")).split(",");
for (const l of LABELS)
  if (!(l in TARGET_SIZES)) throw new Error(`Unknown size "${l}"`);

/**
 * Runs one read in a separate plain-Node process (a tiny script that imports the same installed zerobucket) and returns how far
 * that process's peak resident memory rose during the read, using the operating system's own peak counter (maxRSS), which
 * sees brief spikes that sampling would miss.
 *
 * One trap: on Linux a new process starts with its PARENT's current memory size as its peak, and this script has just built
 * very large test images, which would hide the read entirely (every figure came out as zero). So the measuring process is
 * started by a small launcher process instead, whose own size is tiny.
 */
function childPeak(op: string, id: string): Promise<number> {
  const measure = `
    import { ZeroBucket } from ${JSON.stringify(import.meta.resolve("zerobucket"))};
    const [op, id] = process.argv.slice(1);
    const zb = new ZeroBucket({ connectionString: process.env.ZB_URL, maxBytes: ${MAX_BYTES} });
    await zb.metadata(id);
    globalThis.gc?.();
    const before = process.resourceUsage().maxRSS;
    if (op === "get") await zb.get(id); else for await (const _ of await zb.getStream(id, {})) void _;
    console.log(JSON.stringify({ growthMB: (process.resourceUsage().maxRSS - before) / 1024 }));
    await zb.close();`;
  const launcher = `
    import { spawnSync } from "node:child_process";
    const [script, op, id] = process.argv.slice(1);
    const r = spawnSync(process.execPath, ["--expose-gc", "--input-type=module", "-e", script, op, id], { stdio: ["ignore", "inherit", "inherit"], env: process.env });
    process.exit(r.status ?? 1);`;
  const env: NodeJS.ProcessEnv = { ...process.env, ZB_URL: URL_ };
  delete env.NODE_OPTIONS; // tsx puts its loaders here; the children must be bare Node
  return new Promise((resolve, reject) => {
    const c = spawn(
      process.execPath,
      ["--input-type=module", "-e", launcher, measure, op, id],
      { env },
    );
    let out = "";
    let err = "";
    c.stdout.on("data", (d) => (out += d));
    c.stderr.on("data", (d) => (err += d));
    c.on("error", reject);
    c.on("exit", (code) =>
      code === 0
        ? resolve(JSON.parse(out.trim().split("\n").pop()!).growthMB)
        : reject(new Error(`child exited ${code}: ${err}`)),
    );
  });
}

const version = (require("zerobucket/package.json") as { version: string })
  .version;
const zb = new ZeroBucket({ connectionString: URL_, maxBytes: MAX_BYTES });
const rows: Record<string, string | number>[] = [];
console.error(`zerobucket ${version}, PostgreSQL reads`);
for (const label of LABELS) {
  process.stderr.write(`  ${label}\n`);
  const data = await makeImageOfSize(TARGET_SIZES[label]!);
  const id = await zb.put(data);
  try {
    const time = async (op: string) => {
      for (let i = 0; i < 2; i++) await readOnce(zb, op, id);
      const t: number[] = [];
      for (let i = 0; i < REPEATS; i++) {
        const t0 = performance.now();
        await readOnce(zb, op, id);
        t.push(performance.now() - t0);
      }
      return median(t);
    };
    const peak = async (op: string) => {
      const samples: number[] = [];
      for (let i = 0; i < MEM_RUNS; i++) samples.push(await childPeak(op, id));
      if (argv.includes("--verbose"))
        process.stderr.write(
          `    ${op} peak samples (MB): ${samples.map((x) => x.toFixed(0)).join(" ")}\n`,
        );
      return median(samples);
    };
    rows.push({
      size: label,
      actualKb: Math.round(data.length / 1024),
      getMs: +(await time("get")).toFixed(1),
      streamMs: +(await time("stream")).toFixed(1),
      getPeakMb: Math.round(await peak("get")),
      streamPeakMb: Math.round(await peak("stream")),
    });
  } finally {
    await zb.delete(id).catch(() => undefined);
  }
}
await zb.close();

const kb = (n: number) => (n >= 1024 ? `${(n / 1024).toFixed(1)}MB` : `${n}KB`);
console.log(
  `zerobucket ${version}, PostgreSQL; medians of ${REPEATS} runs; memory = peak growth of a fresh process\n`,
);
console.log(
  "| Size | Actual | get() | Full stream (1MB chunks) | get() peak mem | Stream peak mem |",
);
console.log("|---|---|---|---|---|---|");
for (const r of rows)
  console.log(
    `| ${r.size} | ${kb(r.actualKb as number)} | ${r.getMs}ms | ${r.streamMs}ms | ${r.getPeakMb}MB | ${r.streamPeakMb}MB |`,
  );

if (!argv.includes("--no-write")) {
  const dir = path.join(HERE, "..", "results", version);
  fs.mkdirSync(dir, { recursive: true });
  const keys = Object.keys(rows[0]!);
  fs.writeFileSync(
    path.join(dir, "postgres-reads.csv"),
    [keys.join(","), ...rows.map((r) => keys.map((k) => r[k]).join(","))].join(
      "\n",
    ) + "\n",
  );
  console.error(`Wrote results/${version}/postgres-reads.csv`);
}
