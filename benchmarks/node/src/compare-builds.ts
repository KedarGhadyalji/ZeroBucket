/**
 * Compare two installed builds of zerobucket on PostgreSQL, fairly.
 *
 * Timing one build and then the other, minutes apart, lets machine noise pose as a difference, especially for small
 * images. This loads BOTH builds in one process and alternates their reads round by round, so any drift hits both equally.
 * Each folder must contain node_modules/zerobucket (and pg, sharp):
 *
 *   mkdir a b && (cd a && npm init -y && npm i zerobucket@0.10.0 pg sharp) && (cd b && npm init -y && npm i zerobucket@0.10.1 pg sharp)
 *   npm run compare-builds -- --a ../a --b ../b
 *   npm run compare-builds -- --a ../a --b ../b --sizes 1MB,10MB --rounds 25
 */
import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";
import { performance } from "node:perf_hooks";
import { TARGET_SIZES, makeImageOfSize } from "./images.js";

const argv = process.argv.slice(2);
const opt = (n: string) =>
  argv.includes(`--${n}`) ? argv[argv.indexOf(`--${n}`) + 1] : undefined;
const URL_ =
  process.env.ZEROBUCKET_BENCH_DATABASE_URL ??
  process.env.ZEROBUCKET_TEST_DATABASE_URL;
if (!URL_ || !opt("a") || !opt("b")) {
  console.error(
    "Usage: npm run compare-builds -- --a <folder> --b <folder>   (and set ZEROBUCKET_BENCH_DATABASE_URL)",
  );
  process.exit(1);
}
const ROUNDS = Number(opt("rounds") ?? 15);
const LABELS = (opt("sizes") ?? Object.keys(TARGET_SIZES).join(",")).split(",");
/** A preset name (100KB, 1MB, ...) or any number with a unit, such as 700KB or 1.5MB. */
function targetBytes(label: string): number {
  if (label in TARGET_SIZES) return TARGET_SIZES[label]!;
  const m = /^(\d+(?:\.\d+)?)(KB|MB)$/i.exec(label);
  if (!m)
    throw new Error(
      `Unknown size "${label}": use a name like 1MB, or a number with KB or MB such as 700KB`,
    );
  return Math.round(
    Number(m[1]) * (m[2]!.toUpperCase() === "MB" ? 1048576 : 1024),
  );
}
const MAX_BYTES = 64 * 1024 * 1024;

async function load(dir: string) {
  const root = path.resolve(dir, "node_modules", "zerobucket");
  const version = (
    createRequire(path.join(root, "x.js"))("./package.json") as {
      version: string;
    }
  ).version;
  const mod = await import(
    pathToFileURL(path.join(root, "dist", "index.js")).href
  );
  return {
    version,
    zb: new mod.ZeroBucket({
      connectionString: URL_,
      maxBytes: MAX_BYTES,
    }) as import("zerobucket").ZeroBucket,
  };
}

const a = await load(opt("a")!);
const b = await load(opt("b")!);
const median = (xs: number[]) => [...xs].sort((x, y) => x - y)[xs.length >> 1]!;
const read = async (
  zb: import("zerobucket").ZeroBucket,
  op: string,
  id: string,
) => {
  if (op === "get") await zb.get(id);
  else for await (const _ of await zb.getStream(id, {})) void _;
};

console.log(
  `${a.version} (A) against ${b.version} (B), PostgreSQL, ${ROUNDS} interleaved rounds, medians\n`,
);
console.log(
  `| Size | get() A | get() B | change | stream A | stream B | change |`,
);
console.log(`|---|---|---|---|---|---|---|`);
for (const label of LABELS) {
  const data = await makeImageOfSize(targetBytes(label));
  const id = await a.zb.put(data);
  try {
    const t: Record<string, number[]> = {
      "get-a": [],
      "get-b": [],
      "stream-a": [],
      "stream-b": [],
    };
    for (const op of ["get", "stream"])
      for (let i = 0; i < 3; i++) {
        await read(a.zb, op, id);
        await read(b.zb, op, id);
      }
    for (let r = 0; r < ROUNDS; r++) {
      for (const op of ["get", "stream"]) {
        for (const who of r % 2
          ? (["b", "a"] as const)
          : (["a", "b"] as const)) {
          const t0 = performance.now();
          await read(who === "a" ? a.zb : b.zb, op, id);
          t[`${op}-${who}`]!.push(performance.now() - t0);
        }
      }
    }
    const f = (x: number) => `${x.toFixed(1)}ms`;
    const pct = (x: number, y: number) =>
      `${y <= x ? "" : "+"}${(((y - x) / x) * 100).toFixed(0)}%`;
    const [ga, gb, sa, sb] = [
      median(t["get-a"]!),
      median(t["get-b"]!),
      median(t["stream-a"]!),
      median(t["stream-b"]!),
    ];
    console.log(
      `| ${label} (${Math.round(data.length / 1024)}KB) | ${f(ga)} | ${f(gb)} | ${pct(ga, gb)} | ${f(sa)} | ${f(sb)} | ${pct(sa, sb)} |`,
    );
  } finally {
    await a.zb.delete(id).catch(() => undefined);
  }
}
await a.zb.close();
await b.zb.close();
