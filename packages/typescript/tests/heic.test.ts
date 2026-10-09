import { readFileSync } from "node:fs";
import { join } from "node:path";
import sharp from "sharp";
import { afterEach, describe, expect, it } from "vitest";
import {
  CorruptedImageError,
  ImageTooLargeError,
  ImageValidationError,
  StorageError,
  UnsupportedFormatError,
  ZeroBucket,
  optimizeImage,
  validateImage,
} from "../src/index.js";
import { createImageHandler } from "../src/http.js";
import { _testing, decodeHeic, looksLikeHeic } from "../src/heic.js";
import { DATABASE_URL, makeImage, newBucket } from "./helpers.js";

const fixture = (n: string) =>
  readFileSync(join(process.cwd(), "tests/fixtures", n));
const small = fixture("small.heic"); // 160x120
const tall = fixture("tall.heic"); //   90x160
const large = fixture("large_smooth.heic"); // 4000x3000 (12 MP), 12 KB on disk
const MAX = 8 * 1024 * 1024;
const nodeMajor = Number(process.versions.node.split(".")[0]);

afterEach(() => _testing.setResolver(undefined)); // never leak a test's resolver override into the next test

describe("HEIC content sniffing", () => {
  it("recognises the HEIC/HEIF brands (same list as the Python package) and nothing else", async () => {
    for (const brand of [
      "heic",
      "heix",
      "hevc",
      "heim",
      "heis",
      "mif1",
      "msf1",
    ]) {
      expect(
        looksLikeHeic(
          Buffer.concat([
            Buffer.from([0, 0, 0, 24]),
            Buffer.from("ftyp" + brand),
          ]),
        ),
      ).toBe(true);
    }
    expect(looksLikeHeic(small)).toBe(true);
    expect(
      looksLikeHeic(
        Buffer.concat([Buffer.from([0, 0, 0, 24]), Buffer.from("ftypmp42")]),
      ),
    ).toBe(false); // an MP4, not an image
    expect(looksLikeHeic(await makeImage("png"))).toBe(false);
    expect(looksLikeHeic(await makeImage("jpeg"))).toBe(false);
    expect(looksLikeHeic(Buffer.from("ftypheic"))).toBe(false); // too short / wrong offset
    expect(looksLikeHeic(Buffer.alloc(0))).toBe(false);
  });
});

describe("HEIC validation", () => {
  it("accepts real HEIC files and reports mime and dimensions (identical to what Python's pillow-heif reports)", async () => {
    expect(await validateImage(small, { maxBytes: MAX })).toEqual({
      mimeType: "image/heic",
      width: 160,
      height: 120,
      sizeBytes: small.length,
    });
    expect(await validateImage(tall, { maxBytes: MAX })).toMatchObject({
      mimeType: "image/heic",
      width: 90,
      height: 160,
    });
  });

  it("rejects truncated and garbage HEIC as CORRUPTED (not as unsupported, not as a server fault)", async () => {
    const truncated = small.subarray(0, Math.floor(small.length * 0.6));
    await expect(
      validateImage(truncated, { maxBytes: MAX }),
    ).rejects.toBeInstanceOf(CorruptedImageError);
    const headerOnly = Buffer.concat([
      Buffer.from([0, 0, 0, 24]),
      Buffer.from("ftypheic"),
      Buffer.alloc(64, 7),
    ]);
    await expect(
      validateImage(headerOnly, { maxBytes: MAX }),
    ).rejects.toBeInstanceOf(CorruptedImageError);
    await expect(
      validateImage(small.subarray(0, 40), { maxBytes: MAX }),
    ).rejects.toBeInstanceOf(CorruptedImageError);
  });

  it("explains WHY a HEIC is corrupt, and does not spray library internals onto the host's stderr", async () => {
    const written: string[] = [];
    const orig = process.stderr.write.bind(process.stderr);
    process.stderr.write = ((chunk: string | Uint8Array) => (
      written.push(String(chunk)),
      true
    )) as typeof process.stderr.write;
    let err: any;
    try {
      err = await validateImage(
        small.subarray(0, Math.floor(small.length * 0.6)),
        { maxBytes: MAX },
      ).catch((e) => e);
      await new Promise((r) => setTimeout(r, 100)); // worker stdio is forwarded asynchronously
    } finally {
      process.stderr.write = orig;
    }
    expect(err).toBeInstanceOf(CorruptedImageError);
    expect(err.message).toMatch(
      /Unexpected end of file|outside of file bounds/,
    ); // the real diagnosis survives
    expect(written.join("")).not.toMatch(/subcode|iloc|HeifError/);
  });

  it("refuses a pixel bomb from the HEADER, before decoding a single pixel", async () => {
    const before = { ..._testing.stats };
    await expect(
      validateImage(small, { maxBytes: MAX, maxPixels: 1000 }),
    ).rejects.toBeInstanceOf(ImageTooLargeError);
    expect(_testing.stats.info).toBe(before.info + 1);
    expect(_testing.stats.validate).toBe(before.validate); // the expensive full decode never ran
  });

  it("honours allowedFormats", async () => {
    await expect(
      validateImage(small, { maxBytes: MAX, allowedFormats: ["png", "jpeg"] }),
    ).rejects.toBeInstanceOf(UnsupportedFormatError);
    expect(
      (await validateImage(small, { maxBytes: MAX, allowedFormats: ["heic"] }))
        .mimeType,
    ).toBe("image/heic");
    await expect(
      validateImage(await makeImage("png"), {
        maxBytes: MAX,
        allowedFormats: ["heic"],
      }),
    ).rejects.toBeInstanceOf(UnsupportedFormatError);
  });

  it("without libheif-js installed, says exactly how to fix it (never 'corrupted')", async () => {
    _testing.setResolver(() => {
      throw new Error("Cannot find module");
    });
    const err = await validateImage(small, { maxBytes: MAX }).catch((e) => e);
    expect(err).toBeInstanceOf(ImageValidationError);
    expect(err).not.toBeInstanceOf(CorruptedImageError);
    expect(err.message).toContain("npm install libheif-js");
    const opt = await optimizeImage(small, {
      maxBytes: MAX,
      format: "jpeg",
    }).catch((e) => e);
    expect(opt.message).toContain("npm install libheif-js");
    // other formats are completely unaffected
    expect(
      (await validateImage(await makeImage("png", 20, 20), { maxBytes: MAX }))
        .mimeType,
    ).toBe("image/png");
  });
});

describe("HEIC decoding runs OFF the event loop", () => {
  it("a 12-megapixel HEIC does not freeze the process while it decodes", async () => {
    let ticks = 0;
    const timer = setInterval(() => ticks++, 10);
    const t0 = performance.now();
    const v = await validateImage(large, { maxBytes: MAX });
    const elapsed = performance.now() - t0;
    clearInterval(timer);
    expect(v).toMatchObject({
      mimeType: "image/heic",
      width: 4000,
      height: 3000,
    });
    expect(elapsed).toBeGreaterThan(150); // it really was heavy work...
    expect(ticks).toBeGreaterThan(elapsed / 10 / 3); // ...and the loop kept ticking throughout (blocking decode would give ~0)
  });

  it("reuses one worker, and recovers if it is killed (in flight or idle)", async () => {
    await validateImage(small, { maxBytes: MAX });
    const started = _testing.stats.workersStarted;
    await validateImage(tall, { maxBytes: MAX });
    expect(_testing.stats.workersStarted).toBe(started); // reused, not respawned per image

    const inFlight = validateImage(large, { maxBytes: MAX });
    await new Promise((r) => setTimeout(r, 60));
    await _testing.terminateWorker();
    const err = await inFlight.catch((e) => e);
    expect(err).toBeInstanceOf(StorageError); // an infrastructure fault is not blamed on the user's image
    expect(err).not.toBeInstanceOf(CorruptedImageError);

    expect((await validateImage(small, { maxBytes: MAX })).width).toBe(160); // a fresh worker takes over
    await _testing.terminateWorker();
    expect((await validateImage(tall, { maxBytes: MAX })).height).toBe(160); // also after an idle kill
  });
});

describe("HEIC worker lifecycle", () => {
  it("never keeps the host process alive: a script that validates a HEIC exits by itself, with the right result", async () => {
    const { spawn } = await import("node:child_process");
    const { mkdtempSync, writeFileSync } = await import("node:fs");
    const { tmpdir } = await import("node:os");
    const script = `import { readFileSync } from "node:fs";
      import { validateImage } from ${JSON.stringify(join(process.cwd(), "src/index.ts"))};
      const d = readFileSync(${JSON.stringify(join(process.cwd(), "tests/fixtures/small.heic"))});
      const v = await validateImage(d, { maxBytes: 8388608 });
      console.log("RESULT " + v.mimeType + " " + v.width + "x" + v.height);
      await validateImage(d.subarray(0, 300), { maxBytes: 8388608 }).catch(() => console.log("CORRUPT REJECTED"));`;
    const file = join(
      mkdtempSync(join(tmpdir(), "zb-heic-")),
      "exit-check.mts",
    );
    writeFileSync(file, script);
    const r = await new Promise<{
      code: number | null;
      out: string;
      err: string;
      ms: number;
    }>((resolve) => {
      const t0 = Date.now();
      const c = spawn(process.execPath, ["--import", "tsx", file], {
        cwd: process.cwd(),
      });
      let out = "";
      let err = "";
      c.stdout.on("data", (d) => (out += d));
      c.stderr.on("data", (d) => (err += d));
      const killer = setTimeout(() => c.kill("SIGKILL"), 20_000); // a hang would show as a kill, not exit 0
      c.on(
        "exit",
        (code) => (
          clearTimeout(killer),
          resolve({ code, out, err, ms: Date.now() - t0 })
        ),
      );
    });
    expect(r.err).toBe(""); // nothing leaks to stderr, not even for the corrupt file
    expect(r.out).toContain("RESULT image/heic 160x120");
    expect(r.out).toContain("CORRUPT REJECTED");
    expect(r.code).toBe(0); // 13 would mean the process left mid-decode; null/SIGKILL would mean it never left
  }, 30_000);

  it("a dying worker's late exit event never fails the REPLACEMENT worker's jobs (regression)", async () => {
    await validateImage(small, { maxBytes: MAX }); // worker #1 exists
    _testing.setResolver(undefined); // detaches #1 and terminates it asynchronously: its 'exit' event is still on its way
    // These start worker #2 immediately; #1's 'exit' arrives while they are in flight and must not touch them.
    const [a, b] = await Promise.all([
      validateImage(large, { maxBytes: MAX }),
      validateImage(small, { maxBytes: MAX }),
    ]);
    expect([a.width, b.width]).toEqual([4000, 160]);
  });
});

describe("optimize: converting FROM HEIC", () => {
  it("converts to jpeg, webp and png with the right dimensions and real pixel content", async () => {
    const src = await decodeHeic(small);
    const mean = [0, 1, 2].map((c) => {
      let sum = 0;
      for (let i = c; i < src.rgba.length; i += 4) sum += src.rgba[i]!;
      return sum / (src.rgba.length / 4);
    });
    for (const [format, mime] of [
      ["jpeg", "image/jpeg"],
      ["webp", "image/webp"],
      ["png", "image/png"],
    ] as const) {
      const r = await optimizeImage(small, { maxBytes: MAX, format });
      expect(r).toMatchObject({ mimeType: mime, width: 160, height: 120 });
      // channel means match the HEIC's own decode: guards against swapped channels / wrong alpha handling
      const stats = await sharp(r.data).stats();
      [0, 1, 2].forEach((c) =>
        expect(Math.abs(stats.channels[c]!.mean - mean[c]!)).toBeLessThan(8),
      );
    }
  });
  it("resizes (never upscales) and re-validates its own output", async () => {
    expect(
      await optimizeImage(small, {
        maxBytes: MAX,
        format: "webp",
        maxWidth: 80,
      }),
    ).toMatchObject({ width: 80, height: 60 });
    expect(
      await optimizeImage(small, {
        maxBytes: MAX,
        format: "jpeg",
        maxWidth: 1000,
      }),
    ).toMatchObject({ width: 160, height: 120 });
  });
  it("requires an explicit format (it cannot silently re-encode to HEIC like Python does)", async () => {
    const err = await optimizeImage(small, { maxBytes: MAX }).catch((e) => e);
    expect(err).toBeInstanceOf(ImageValidationError);
    expect(err.message).toMatch(/explicit output format/);
    expect(err.message).toMatch(/jpeg/);
  });
  it("refuses to ENCODE heic from any source, instead of silently writing a PNG", async () => {
    for (const source of [small, await makeImage("png", 30, 30)]) {
      for (const format of ["heic", "heif", "HEIC"]) {
        const err = await optimizeImage(source, {
          maxBytes: MAX,
          format,
        }).catch((e) => e);
        expect(err).toBeInstanceOf(ImageValidationError);
        expect(err.message).toMatch(/Encoding to HEIC is not available/);
      }
    }
  });
  it("did not change how non-HEIC optimize works", async () => {
    const png = await makeImage("png", 200, 100, { seed: 4 });
    expect(
      await optimizeImage(png, {
        maxBytes: MAX,
        format: "webp",
        maxWidth: 100,
      }),
    ).toMatchObject({ mimeType: "image/webp", width: 100, height: 50 });
    expect(await optimizeImage(png, { maxBytes: MAX })).toMatchObject({
      mimeType: "image/png",
    });
    await expect(
      optimizeImage(png, { maxBytes: MAX, format: "gif" }),
    ).rejects.toBeInstanceOf(ImageValidationError);
  });
});

describe.skipIf(!DATABASE_URL)("HEIC through ZeroBucket (Postgres)", () => {
  it("stores the HEIC bytes exactly as uploaded, and serves them (get, stream, range, HTTP)", async () => {
    const zb = newBucket();
    const id = await zb.put(small, { filename: "IMG_0001.HEIC" });
    const img = await zb.get(id);
    expect(img.data.equals(small)).toBe(true); // stored as-is, not converted
    expect(img).toMatchObject({
      mimeType: "image/heic",
      width: 160,
      height: 120,
      filename: "IMG_0001.HEIC",
    });
    const parts: Buffer[] = [];
    for await (const c of await zb.getStream(id, { chunkSize: 1000 }))
      parts.push(c);
    expect(Buffer.concat(parts).equals(small)).toBe(true);
    const res = await createImageHandler(zb)(
      new Request(`http://x/i/${id}`, { headers: { Range: "bytes=0-11" } }),
    );
    expect(res.status).toBe(206);
    expect(res.headers.get("content-type")).toBe("image/heic");
    expect(
      Buffer.from(await res.arrayBuffer())
        .subarray(4, 12)
        .toString(),
    ).toBe("ftypheic");
    await zb.close();
  });
  it("optimize converts on the way in; putMany handles HEIC alongside other formats and bad files", async () => {
    const zb = newBucket();
    const id = await zb.put(small, {
      optimize: { format: "webp", maxWidth: 80 },
    });
    expect(await zb.get(id)).toMatchObject({
      mimeType: "image/webp",
      width: 80,
      height: 60,
    });
    await expect(zb.put(small, { optimize: true })).rejects.toThrow(
      /explicit output format/,
    );
    const res = await zb.putMany([
      small,
      await makeImage("png", 12, 12),
      small.subarray(0, 100),
      tall,
    ]);
    expect(res.map((r) => r.success)).toEqual([true, true, false, true]);
    expect((await zb.get(res[3]!.imageId!)).height).toBe(160);
    await zb.close();
  });
});

describe.skipIf(nodeMajor < 22)("HEIC through ZeroBucket (SQLite)", () => {
  it("round-trips HEIC bytes unchanged", async () => {
    const zb = new ZeroBucket({ sqlite: ":memory:" });
    const id = await zb.put(tall);
    const img = await zb.get(id);
    expect(img.data.equals(tall)).toBe(true);
    expect(img).toMatchObject({
      mimeType: "image/heic",
      width: 90,
      height: 160,
    });
    await zb.close();
  });
});
