/**
 * Compression quality sweep for optimizeImage(), the npm counterpart of the Python package's
 * benchmarks/COMPRESSION_RESULTS.md. For each of four photo-like fixtures it reports size saved and SSIM (the
 * standard structural-similarity proxy for "would a person notice") at the default quality, then sweeps JPEG quality.
 *
 *   npm run compression
 *   npm run compression -- --dump ./fixtures     also write the originals, for compare_python.py
 */
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { optimizeImage } from "zerobucket";
import { FIXTURES } from "./images.js";
import { decodeRgb, ssim } from "./ssim.js";

const argv = process.argv.slice(2);
const dump = argv.includes("--dump")
  ? argv[argv.indexOf("--dump") + 1]
  : undefined;
const MAX_BYTES = 64 * 1024 * 1024;
const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1]!;
const kb = (n: number) => `${Math.round(n / 1024)}KB`;
const saved = (orig: number, now: number) => {
  const p = Math.round((1 - now / orig) * 100);
  return p < 0 ? `**${p}% (grew)**` : `${p}%`;
};

async function score(original: Buffer, optimized: Buffer): Promise<number> {
  return ssim(await decodeRgb(original), await decodeRgb(optimized));
}

const originals = new Map<string, Buffer>();
for (const [name, make] of Object.entries(FIXTURES)) {
  const data = await make();
  originals.set(name, data);
  if (dump) {
    fs.mkdirSync(dump, { recursive: true });
    fs.writeFileSync(
      path.join(
        dump,
        `${name}.orig.${name === "flat_graphic" ? "png" : "jpg"}`,
      ),
      data,
    );
  }
}

console.log("### Defaults (JPEG quality 90, WebP quality 88)\n");
console.log(
  "| Content type | Format | Original | Compressed | Saved | SSIM | Time |",
);
console.log("|---|---|---|---|---|---|---|");
for (const [name, original] of originals) {
  for (const format of ["jpeg", "webp"]) {
    const run = () => optimizeImage(original, { format, maxBytes: MAX_BYTES });
    const result = await run();
    const times: number[] = [];
    for (let i = 0; i < 3; i++) {
      const t0 = performance.now();
      await run();
      times.push(performance.now() - t0);
    }
    console.log(
      `| ${name} | ${format.toUpperCase()} | ${kb(original.length)} | ${kb(result.sizeBytes)} | ${saved(original.length, result.sizeBytes)} | ${(await score(original, result.data)).toFixed(3)} | ${median(times).toFixed(0)}ms |`,
    );
  }
}

const QUALITIES = [50, 70, 80, 90, 95, 98];
console.log("\n### JPEG quality sweep: SSIM (size)\n");
console.log(`| Content type | ${QUALITIES.map((q) => `q${q}`).join(" | ")} |`);
console.log(`|---|${QUALITIES.map(() => "---").join("|")}|`);
for (const [name, original] of originals) {
  const cells: string[] = [];
  for (const quality of QUALITIES) {
    const r = await optimizeImage(original, {
      format: "jpeg",
      quality,
      maxBytes: MAX_BYTES,
    });
    cells.push(
      `${(await score(original, r.data)).toFixed(3)} (${kb(r.sizeBytes)})`,
    );
  }
  console.log(`| ${name} | ${cells.join(" | ")} |`);
}
