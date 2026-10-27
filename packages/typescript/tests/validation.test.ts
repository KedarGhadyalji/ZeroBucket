/**
 * validateImage(): the full-decode step.
 *
 * Since 0.10.0 the decode is forced by streaming every scanline through a one-pixel-wide extract instead of sharp's
 * stats() (see the comment in validation.ts). These tests pin what must never change about it: damaged files are
 * rejected, intact ones are accepted, and memory stays flat for large images.
 */
import { describe, expect, it } from "vitest";
import sharp from "sharp";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import {
  CorruptedImageError,
  ImageTooLargeError,
  validateImage,
} from "../src/index.js";

const MAX = 64 * 1024 * 1024;

/** Photo-like pixels (a gradient plus noise) so every codec has real work to do. */
function pixels(w: number, h: number, channels: number, seed = 7): Buffer {
  const b = Buffer.alloc(w * h * channels);
  let s = seed >>> 0;
  for (let y = 0; y < h; y++)
    for (let x = 0; x < w; x++)
      for (let c = 0; c < channels; c++) {
        s = (s * 1664525 + 1013904223) >>> 0;
        b[(y * w + x) * channels + c] = Math.min(
          255,
          (x * 255) / w + (y * 90) / h + c * 40 + ((s >>> 24) % 40),
        );
      }
  return b;
}
const image = (w: number, h: number, channels: 1 | 3 | 4 = 3) =>
  sharp(pixels(w, h, channels), { raw: { width: w, height: h, channels } });

const SMALL: [number, number] = [256, 192];
const BIG: [number, number] = [1200, 900];

const FLAVOURS: Record<string, () => Promise<Buffer>> = {
  "jpeg 4:2:0": () =>
    image(...SMALL)
      .jpeg({ quality: 85 })
      .toBuffer(),
  "jpeg 4:4:4": () =>
    image(...SMALL)
      .jpeg({ quality: 92, chromaSubsampling: "4:4:4" })
      .toBuffer(),
  "jpeg progressive": () =>
    image(...SMALL)
      .jpeg({ quality: 80, progressive: true })
      .toBuffer(),
  "jpeg greyscale": () =>
    image(...SMALL, 1)
      .jpeg({ quality: 85 })
      .toBuffer(),
  "jpeg CMYK": () =>
    image(...SMALL)
      .toColourspace("cmyk")
      .jpeg({ quality: 85 })
      .toBuffer(),
  "jpeg large": () =>
    image(...BIG)
      .jpeg({ quality: 85 })
      .toBuffer(),
  "jpeg large progressive": () =>
    image(...BIG)
      .jpeg({ quality: 85, progressive: true })
      .toBuffer(),
  "png rgb": () =>
    image(...SMALL)
      .png()
      .toBuffer(),
  "png rgba": () =>
    image(...SMALL, 4)
      .png()
      .toBuffer(),
  "png greyscale": () =>
    image(...SMALL, 1)
      .png()
      .toBuffer(),
  "png palette": () =>
    image(...SMALL)
      .png({ palette: true, colours: 64 })
      .toBuffer(),
  "png interlaced": () =>
    image(...SMALL)
      .png({ progressive: true })
      .toBuffer(),
  "png 16-bit": () =>
    image(...SMALL)
      .toColourspace("rgb16")
      .png()
      .toBuffer(),
  "webp lossy": () =>
    image(...SMALL)
      .webp({ quality: 80 })
      .toBuffer(),
  "webp lossless": () =>
    image(...SMALL)
      .webp({ lossless: true })
      .toBuffer(),
  "webp alpha": () =>
    image(...SMALL, 4)
      .webp({ quality: 80 })
      .toBuffer(),
};

const rejects = (data: Buffer) =>
  validateImage(data, { maxBytes: MAX }).then(
    () => false,
    () => true,
  );

/** The oracle: would a plain, whole-image decode accept this file? (Formats outside jpeg/png/webp are refused anyway.) */
async function plainDecodeRejects(data: Buffer): Promise<boolean> {
  try {
    const meta = await sharp(data, { failOn: "error" }).metadata();
    if (!meta.format || !["jpeg", "png", "webp"].includes(meta.format))
      return true;
    await sharp(data, { failOn: "error" }).raw().toBuffer();
    return false;
  } catch {
    return true;
  }
}

describe("validateImage: intact images", () => {
  for (const [name, make] of Object.entries(FLAVOURS)) {
    it(`accepts an intact ${name}`, async () => {
      const data = await make();
      const meta = await sharp(data).metadata();
      const v = await validateImage(data, { maxBytes: MAX });
      expect(v.mimeType).toBe(`image/${meta.format}`);
      expect(v.width).toBe(meta.width);
      expect(v.height).toBe(meta.height);
      expect(v.sizeBytes).toBe(data.length);
    });
  }
});

describe("validateImage: damaged images are rejected exactly as a full decode rejects them", () => {
  it("agrees with a plain full decode on 400 truncated, flipped and zeroed files", async () => {
    let s = 99;
    const rand = () => (s = (s * 1664525 + 1013904223) >>> 0) / 4294967296;
    let rejected = 0;
    let accepted = 0;
    const disagreements: string[] = [];
    for (const [name, make] of Object.entries(FLAVOURS)) {
      const base = await make();
      const cases: [string, Buffer][] = [];
      for (const f of [0.1, 0.5, 0.9, 0.99])
        cases.push([
          `truncate ${f}`,
          base.subarray(0, Math.floor(base.length * f)),
        ]);
      for (const n of [1, 2, 5, 10, 100])
        cases.push([`drop last ${n}`, base.subarray(0, base.length - n)]);
      for (let i = 0; i < 12; i++) {
        const c = Buffer.from(base);
        const p = Math.floor(rand() * c.length);
        c[p] = (c[p]! ^ (1 << Math.floor(rand() * 8))) & 0xff;
        cases.push([`flip bit @${p}`, c]);
      }
      for (let i = 0; i < 4; i++) {
        const c = Buffer.from(base);
        const p = Math.floor(rand() * Math.max(1, c.length - 64));
        c.fill(0, p, p + 64);
        cases.push([`zero 64 bytes @${p}`, c]);
      }
      for (const [what, data] of cases) {
        const [mine, oracle] = [
          await rejects(data),
          await plainDecodeRejects(data),
        ];
        mine ? rejected++ : accepted++;
        if (mine !== oracle)
          disagreements.push(
            `${name} / ${what}: validateImage ${mine ? "rejected" : "accepted"}, full decode ${oracle ? "rejected" : "accepted"}`,
          );
      }
    }
    expect(disagreements).toEqual([]);
    // Not vacuous: the corpus must contain plenty of both outcomes.
    expect(rejected).toBeGreaterThan(200);
    expect(accepted).toBeGreaterThan(100);
  }, 120_000);

  it("rejects a JPEG that is only missing its last few bytes (an interrupted upload)", async () => {
    // Regression: a cheaper `resize(1, 1)` decode accepted these because JPEG shrink-on-load skips the check.
    const big = await FLAVOURS["jpeg large"]!();
    const progressive = await FLAVOURS["jpeg large progressive"]!();
    for (const jpeg of [big, progressive]) {
      for (const n of [1, 2, 10, 100]) {
        const cut = jpeg.subarray(0, jpeg.length - n);
        expect(await plainDecodeRejects(cut), `precondition, drop ${n}`).toBe(
          true,
        );
        await expect(
          validateImage(cut, { maxBytes: MAX }),
        ).rejects.toBeInstanceOf(CorruptedImageError);
      }
    }
  });

  it("rejects a truncated interlaced PNG, which sharp's stats() used to accept", async () => {
    const png = await FLAVOURS["png interlaced"]!();
    const cut = png.subarray(0, Math.floor(png.length * 0.3));
    const err = await validateImage(cut, { maxBytes: MAX }).catch((e) => e);
    expect(err).toBeInstanceOf(CorruptedImageError);
    expect(err.message).toMatch(/truncated or corrupted: \S/);
    expect(err.message).not.toMatch(/: Unknown error$/); // the cryptic libvips text is replaced by a sentence
  });

  it("still reports a pixel-limit breach as too large, not corrupt", async () => {
    const data = await FLAVOURS["jpeg large"]!();
    await expect(
      validateImage(data, { maxBytes: MAX, maxPixels: 100_000 }),
    ).rejects.toBeInstanceOf(ImageTooLargeError);
  });

  it("keeps rejecting empty, oversized-by-bytes and garbage input", async () => {
    await expect(
      validateImage(Buffer.alloc(0), { maxBytes: MAX }),
    ).rejects.toBeInstanceOf(CorruptedImageError);
    await expect(
      validateImage(Buffer.from("not an image at all"), { maxBytes: MAX }),
    ).rejects.toBeInstanceOf(CorruptedImageError);
    await expect(
      validateImage(await FLAVOURS["png rgb"]!(), { maxBytes: 100 }),
    ).rejects.toBeInstanceOf(ImageTooLargeError);
  });
});

describe("validateImage: memory", () => {
  it("does not hold the decoded image in memory (a 36-megapixel JPEG grows the process by well under its 108 MB decoded size)", async () => {
    const { spawn } = await import("node:child_process");
    const { mkdtempSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    // A large image: a small random tile scaled up, so it is cheap to build but still decodes to 36 megapixels.
    const side = 6000;
    const tileSide = 375;
    const tile = pixels(tileSide, tileSide, 3, 3);
    const big = await sharp(tile, {
      raw: { width: tileSide, height: tileSide, channels: 3 },
    })
      .resize(side, side, { kernel: "nearest" })
      .jpeg({ quality: 85 })
      .toBuffer();
    const dir = mkdtempSync(join(tmpdir(), "zb-mem-"));
    const imageFile = join(dir, "big.jpg");
    writeFileSync(imageFile, big);
    const warmFile = join(dir, "warm.jpg");
    writeFileSync(warmFile, await image(16, 16).jpeg().toBuffer());
    const script = `import { readFileSync } from "node:fs";
      import { validateImage } from ${JSON.stringify(pathToFileURL(join(process.cwd(), "src/index.ts")).href)};
      const data = readFileSync(${JSON.stringify(imageFile)});
      // Warm up sharp itself first so the measurement is the decode, not loading the library.
      await validateImage(readFileSync(${JSON.stringify(warmFile)}), { maxBytes: 1 << 20 });
      const before = process.resourceUsage().maxRSS;
      const v = await validateImage(data, { maxBytes: ${MAX} });
      const after = process.resourceUsage().maxRSS;
      console.log("RESULT " + v.width + "x" + v.height + " growthMB=" + Math.round((after - before) / 1024));`;
    const file = join(dir, "mem-check.mts");
    writeFileSync(file, script);
    // The measuring process is started by a tiny launcher, not directly by this (large) test worker: on Linux a child
    // starts with its parent's current size as its peak-memory counter, so a big parent would hide the very growth being
    // measured and the test could pass without detecting anything.
    const launcher = `import { spawnSync } from "node:child_process";
      const r = spawnSync(process.execPath, ["--import", "tsx", process.argv[1]], { stdio: ["ignore", "inherit", "inherit"] });
      process.exit(r.status ?? 1);`;
    const r = await new Promise<{
      code: number | null;
      out: string;
      err: string;
    }>((resolve) => {
      const c = spawn(
        process.execPath,
        ["--input-type=module", "-e", launcher, file],
        { cwd: process.cwd() },
      );
      let out = "";
      let err = "";
      c.stdout.on("data", (d) => (out += d));
      c.stderr.on("data", (d) => (err += d));
      const killer = setTimeout(() => c.kill("SIGKILL"), 90_000);
      c.on(
        "exit",
        (code) => (clearTimeout(killer), resolve({ code, out, err })),
      );
    });
    expect(r.err).toBe("");
    expect(r.out).toContain("RESULT 6000x6000");
    const growth = Number(/growthMB=(-?\d+)/.exec(r.out)![1]);
    // sharp's stats() grew the process by about 110 MB here; the streaming decode stays in the low tens of MB.
    expect(growth).toBeLessThan(40);
    expect(r.code).toBe(0);
  }, 120_000);
});
