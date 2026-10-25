/**
 * Where does validation time go?
 *
 * validateImage() reads the header with metadata(), then forces a full pixel decode to catch truncated files. Before
 * 0.10.0 it did that with stats(); since 0.10.0 it streams every scanline through a one-pixel-wide extract. This times
 * each option on the same bytes, next to a bare decode, so the cost of the "full decode" step can be compared. Optionally writes the images out (--dump DIR) so another tool, such as
 * Pillow, can be timed on exactly the same files.
 *
 *   npm run validation-cost
 *   npm run validation-cost -- --dump ./images
 */
import fs from "node:fs";
import path from "node:path";
import { performance } from "node:perf_hooks";
import sharp from "sharp";
import { TARGET_SIZES, makeImageOfSize } from "./images.js";

const argv = process.argv.slice(2);
const dump = argv.includes("--dump")
  ? argv[argv.indexOf("--dump") + 1]
  : undefined;
const RUNS = 5;

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1]!;
const make = (d: Buffer) => sharp(d, { failOn: "error" });

async function time(fn: () => Promise<unknown>): Promise<number> {
  await fn(); // warm-up
  const out: number[] = [];
  for (let i = 0; i < RUNS; i++) {
    const t0 = performance.now();
    await fn();
    out.push(performance.now() - t0);
  }
  return median(out);
}

const stages: [string, (d: Buffer, h: number) => Promise<unknown>][] = [
  ["metadata()", (d) => make(d).metadata()],
  ["stats() (validateImage before 0.10.0)", (d) => make(d).stats()],
  ["bare decode: raw().toBuffer()", (d) => make(d).raw().toBuffer()],
  [
    "resize(1,1) (faster, but misses truncated JPEG tails: rejected)",
    (d) =>
      sharp(d, { failOn: "error", sequentialRead: true })
        .resize(1, 1, { fastShrinkOnLoad: false })
        .raw()
        .toBuffer(),
  ],
  [
    "column extract (validateImage since 0.10.0)",
    (d, h) =>
      sharp(d, { failOn: "error", sequentialRead: true })
        .extract({ left: 0, top: 0, width: 1, height: h })
        .raw()
        .toBuffer(),
  ],
];

console.log(`| Size | Pixels | ${stages.map(([n]) => n).join(" | ")} |`);
console.log(`|---|---|${stages.map(() => "---").join("|")}|`);
for (const label of Object.keys(TARGET_SIZES)) {
  const data = await makeImageOfSize(TARGET_SIZES[label]!);
  if (dump) {
    fs.mkdirSync(dump, { recursive: true });
    fs.writeFileSync(path.join(dump, `${label}.jpg`), data);
  }
  const meta = await sharp(data).metadata();
  const cells: string[] = [];
  for (const [, fn] of stages)
    cells.push(`${(await time(() => fn(data, meta.height!))).toFixed(1)}ms`);
  console.log(
    `| ${label} (${(data.length / 1024).toFixed(0)}KB) | ${((meta.width! * meta.height!) / 1e6).toFixed(1)}MP | ${cells.join(" | ")} |`,
  );
}
