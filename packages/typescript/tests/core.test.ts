import { createHash } from "node:crypto";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import pg from "pg";
import sharp from "sharp";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  ContentValidationError,
  CorruptedImageError,
  ImageNotFoundError,
  ImageTooLargeError,
  ImageValidationError,
  PDFValidator,
  StorageError,
  UnsupportedFormatError,
  type ZeroBucket,
} from "../src/index.js";
import { DATABASE_URL, makeGif, makeImage, newBucket } from "./helpers.js";

const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");

describe.skipIf(!DATABASE_URL)("core CRUD against real Postgres", () => {
  let zb: ZeroBucket;
  let raw: pg.Pool;
  beforeAll(async () => {
    zb = newBucket();
    await zb.ready();
    raw = new pg.Pool({ connectionString: DATABASE_URL });
  });
  afterAll(async () => {
    await zb.close();
    await raw.end();
  });

  it("round-trips bytes, checksum and metadata for every supported format", async () => {
    for (const [fmt, mime] of [
      ["jpeg", "image/jpeg"],
      ["png", "image/png"],
      ["webp", "image/webp"],
    ] as const) {
      const data = await makeImage(fmt, 40, 30);
      const id = await zb.put(data, { filename: `x.${fmt}` });
      expect(id).toMatch(/^[0-9a-f-]{36}$/);
      const img = await zb.get(id);
      expect(img.data.equals(data)).toBe(true);
      expect(img.mimeType).toBe(mime);
      expect(img.width).toBe(40);
      expect(img.height).toBe(30);
      expect(img.sizeBytes).toBe(data.length);
      expect(img.filename).toBe(`x.${fmt}`);
      expect(img.checksumSha256).toBe(sha(data));
      const meta = await zb.metadata(id);
      expect(meta).toMatchObject({
        imageId: id,
        mimeType: mime,
        checksumSha256: sha(data),
        width: 40,
        height: 30,
      });
    }
  });

  it("detects the format from bytes, ignoring the filename", async () => {
    const png = await makeImage("png");
    const id = await zb.put(png, { filename: "liar.jpg" });
    expect((await zb.get(id)).mimeType).toBe("image/png");
  });

  it("accepts file paths, Blobs/Files and streams, and takes the filename from them", async () => {
    const data = await makeImage("jpeg");
    const dir = await mkdtemp(join(tmpdir(), "zb-"));
    const path = join(dir, "from-disk.jpg");
    await writeFile(path, data);
    expect((await zb.get(await zb.put(path))).filename).toBe("from-disk.jpg");
    expect(
      (
        await zb.get(
          await zb.put(new File([new Uint8Array(data)], "browser.jpg")),
        )
      ).filename,
    ).toBe("browser.jpg");
    const viaStream = await zb.put(
      Readable.from([data.subarray(0, 10), data.subarray(10)]),
    );
    expect((await zb.get(viaStream)).data.equals(data)).toBe(true);
    expect(
      (await zb.get(await zb.put(new Uint8Array(data)))).data.equals(data),
    ).toBe(true);
  });

  it("rejects bad content with the right error class", async () => {
    await expect(zb.put(await makeGif())).rejects.toBeInstanceOf(
      UnsupportedFormatError,
    );
    await expect(
      zb.put(Buffer.from("definitely not an image")),
    ).rejects.toBeInstanceOf(CorruptedImageError);
    await expect(zb.put(Buffer.alloc(0))).rejects.toBeInstanceOf(
      CorruptedImageError,
    );
    const jpeg = await makeImage("jpeg", 200, 200);
    await expect(
      zb.put(jpeg.subarray(0, Math.floor(jpeg.length * 0.6))),
    ).rejects.toBeInstanceOf(CorruptedImageError);
    // every validation failure is also a ContentValidationError / ImageValidationError
    await expect(zb.put(Buffer.from("nope"))).rejects.toBeInstanceOf(
      ContentValidationError,
    );
  });

  it("enforces maxBytes for buffers, paths, Blobs, and streams", async () => {
    const data = await makeImage("png", 100, 100);
    const small = newBucket({ maxBytes: 100 });
    await expect(small.put(data)).rejects.toBeInstanceOf(ImageTooLargeError);
    const dir = await mkdtemp(join(tmpdir(), "zb-"));
    const path = join(dir, "big.png");
    await writeFile(path, data);
    await expect(small.put(path)).rejects.toBeInstanceOf(ImageTooLargeError);
    await expect(
      small.put(new Blob([new Uint8Array(data)])),
    ).rejects.toBeInstanceOf(ImageTooLargeError);
    // a stream is rejected after reading only a bounded amount past the cap
    let produced = 0;
    async function* endless() {
      for (;;) {
        produced += 1024;
        yield Buffer.alloc(1024);
        if (produced > 10_000_000) return;
      }
    }
    await expect(small.put(endless())).rejects.toBeInstanceOf(
      ImageTooLargeError,
    );
    expect(produced).toBeLessThan(10_000);
    await small.close();
  });

  it("rejects decompression bombs by pixel count, not file size", async () => {
    const bomb = await sharp({
      create: { width: 3000, height: 3000, channels: 3, background: "#000" },
    })
      .png()
      .toBuffer();
    expect(bomb.length).toBeLessThan(100_000); // tiny on disk
    const strict = newBucket({ maxPixels: 1_000_000 });
    await expect(strict.put(bomb)).rejects.toBeInstanceOf(ImageTooLargeError);
    await strict.close();
  });

  it("allowedFormats narrows what is accepted", async () => {
    const pngOnly = newBucket({ allowedFormats: ["png"] });
    await expect(pngOnly.put(await makeImage("jpeg"))).rejects.toBeInstanceOf(
      UnsupportedFormatError,
    );
    await expect(pngOnly.put(await makeImage("png"))).resolves.toBeTypeOf(
      "string",
    );
    await pngOnly.close();
  });

  it("throws ImageNotFoundError for missing and malformed ids (no DB error leaks)", async () => {
    const missing = "00000000-0000-4000-8000-000000000000";
    await expect(zb.get(missing)).rejects.toBeInstanceOf(ImageNotFoundError);
    await expect(zb.metadata(missing)).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    await expect(zb.getStream(missing)).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    await expect(zb.get("not-a-uuid")).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    expect(await zb.exists("not-a-uuid")).toBe(false);
    expect(await zb.delete("not-a-uuid")).toBe(false);
  });

  it("exists / delete", async () => {
    const id = await zb.put(await makeImage());
    expect(await zb.exists(id)).toBe(true);
    expect(await zb.delete(id)).toBe(true);
    expect(await zb.exists(id)).toBe(false);
    expect(await zb.delete(id)).toBe(false);
  });

  it("putMany is best-effort and keeps result order", async () => {
    const a = await makeImage("png", 10, 10, { seed: 1 });
    const c = await makeImage("jpeg", 12, 12, { seed: 3 });
    const results = await zb.putMany([a, Buffer.from("garbage"), c], {
      filenames: ["a.png", null, "c.jpg"],
    });
    expect(results.map((r) => r.success)).toEqual([true, false, true]);
    expect(results.map((r) => r.index)).toEqual([0, 1, 2]);
    expect(results[1]!.error).toBeTruthy();
    expect((await zb.get(results[0]!.imageId!)).data.equals(a)).toBe(true);
    expect((await zb.get(results[2]!.imageId!)).filename).toBe("c.jpg");
    await expect(zb.putMany([a], { filenames: [] })).rejects.toBeInstanceOf(
      RangeError,
    );
    expect(await zb.putMany([])).toEqual([]);
  });

  it("putMany does not trigger pg's concurrent-query deprecation (breaks in pg@9)", async () => {
    const warnings: Error[] = [];
    const onWarning = (w: Error) => warnings.push(w);
    process.on("warning", onWarning);
    const imgs = await Promise.all(
      [1, 2, 3].map((n) => makeImage("png", 8, 8, { seed: n })),
    );
    await zb.putMany(imgs);
    await new Promise((r) => setTimeout(r, 50)); // warnings are emitted on a later tick
    process.off("warning", onWarning);
    expect(warnings.map((w) => w.message)).toEqual([]);
  });

  it("putMany with many items preserves id <-> input mapping", async () => {
    const imgs = await Promise.all(
      Array.from({ length: 25 }, (_, i) =>
        makeImage("png", 8 + i, 8, { seed: i + 1 }),
      ),
    );
    const results = await zb.putMany(imgs);
    expect(results.every((r) => r.success)).toBe(true);
    for (let i = 0; i < imgs.length; i++) {
      expect((await zb.get(results[i]!.imageId!)).data.equals(imgs[i]!)).toBe(
        true,
      );
    }
  });

  it("getMany returns input order and reports missing ids without throwing", async () => {
    const [x, y] = [
      await zb.put(await makeImage("png", 9, 9)),
      await zb.put(await makeImage("png", 11, 11)),
    ];
    const missing = "00000000-0000-4000-8000-000000000001";
    const res = await zb.getMany([y, missing, x, "garbage-id", y]);
    expect(res.map((r) => r.imageId)).toEqual([y, missing, x, "garbage-id", y]);
    expect(res.map((r) => r.success)).toEqual([true, false, true, false, true]);
    expect(res[1]!.error).toBe("not found");
    expect(res[0]!.image!.width).toBe(11);
    expect(res[2]!.image!.width).toBe(9);
    expect(await zb.getMany([])).toEqual([]);
  });

  it("deleteMany reports per-id results in input order", async () => {
    const [x, y] = [
      await zb.put(await makeImage("png", 9, 9)),
      await zb.put(await makeImage("png", 9, 10)),
    ];
    const missing = "00000000-0000-4000-8000-000000000002";
    const res = await zb.deleteMany([x, missing, y]);
    expect(res.map((r) => r.deleted)).toEqual([true, false, true]);
    expect(await zb.exists(x)).toBe(false);
    expect(await zb.deleteMany([])).toEqual([]);
  });

  describe("custom validators", () => {
    const pdf = Buffer.from(
      "%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF",
    );
    it("stores a PDF through the same machinery", async () => {
      const id = await zb.put(pdf, {
        validator: new PDFValidator(),
        filename: "doc.pdf",
      });
      const doc = await zb.get(id);
      expect(doc.mimeType).toBe("application/pdf");
      expect(doc.width).toBeNull();
      expect(doc.data.equals(pdf)).toBe(true);
    });
    it("rejects non-PDFs, and optimize+validator", async () => {
      await expect(
        zb.put(Buffer.from("hello"), { validator: new PDFValidator() }),
      ).rejects.toBeInstanceOf(ContentValidationError);
      await expect(
        zb.put(pdf, { validator: new PDFValidator(), optimize: true }),
      ).rejects.toBeInstanceOf(ImageValidationError);
    });
    it("rejects PDFs over the validator's own ceiling", async () => {
      await expect(
        zb.put(pdf, { validator: new PDFValidator({ maxBytes: 10 }) }),
      ).rejects.toBeInstanceOf(ContentValidationError);
    });
  });

  describe("optimize", () => {
    it("downscales, re-encodes, and keeps aspect ratio", async () => {
      const big = await makeImage("png", 800, 400);
      const id = await zb.put(big, {
        optimize: { maxWidth: 200, format: "webp", quality: 70 },
      });
      const img = await zb.get(id);
      expect(img.mimeType).toBe("image/webp");
      expect(img.width).toBe(200);
      expect(img.height).toBe(100);
      expect(img.checksumSha256).toBe(sha(img.data));
    });
    it("never upscales", async () => {
      const id = await zb.put(await makeImage("png", 50, 50), {
        optimize: { maxWidth: 500 },
      });
      expect((await zb.get(id)).width).toBe(50);
    });
    it("strips EXIF but applies orientation first", async () => {
      // 60x20 landscape tagged orientation 6 (rotate 90 CW) should become 20x60 portrait.
      const withExif = await sharp({
        create: { width: 60, height: 20, channels: 3, background: "#369" },
      })
        .withMetadata({
          orientation: 6,
          exif: { IFD0: { Copyright: "secret" } },
        })
        .jpeg()
        .toBuffer();
      expect((await sharp(withExif).metadata()).exif).toBeTruthy();
      const img = await zb.get(await zb.put(withExif, { optimize: true }));
      const out = await sharp(img.data).metadata();
      expect(out.exif).toBeUndefined();
      expect([img.width, img.height]).toEqual([20, 60]);
    });
    it("flattens transparency onto white when converting to JPEG", async () => {
      const transparent = await sharp({
        create: {
          width: 8,
          height: 8,
          channels: 4,
          background: { r: 0, g: 0, b: 0, alpha: 0 },
        },
      })
        .png()
        .toBuffer();
      const img = await zb.get(
        await zb.put(transparent, { optimize: { format: "jpeg" } }),
      );
      const { data } = await sharp(img.data)
        .raw()
        .toBuffer({ resolveWithObject: true });
      expect(data[0]).toBeGreaterThan(240); // white, not black
    });
    it("rejects an unsupported target format", async () => {
      await expect(
        zb.put(await makeImage("png"), { optimize: { format: "gif" } }),
      ).rejects.toBeInstanceOf(ImageValidationError);
    });
  });

  describe("transactions via connection", () => {
    it("a rolled-back transaction leaves no row (put participates in the caller's tx)", async () => {
      const client = await raw.connect();
      let id = "";
      try {
        await client.query("BEGIN");
        id = await zb.put(await makeImage("png", 7, 7), { connection: client });
        // visible inside the transaction...
        expect((await zb.get(id, { connection: client })).width).toBe(7);
        await client.query("ROLLBACK");
      } finally {
        client.release();
      }
      // ...gone after rollback
      expect(await zb.exists(id)).toBe(false);
    });
    it("a committed transaction persists; delete joins the tx too", async () => {
      const client = await raw.connect();
      let id = "";
      try {
        await client.query("BEGIN");
        id = await zb.put(await makeImage("png", 7, 8), { connection: client });
        await client.query("COMMIT");
        await client.query("BEGIN");
        expect(await zb.delete(id, { connection: client })).toBe(true);
        await client.query("ROLLBACK");
      } finally {
        client.release();
      }
      expect(await zb.exists(id)).toBe(true);
    });
  });

  it("shares the exact schema with the Python package (tiered rows are recognised, not misread)", async () => {
    const id = (
      await raw.query(
        `INSERT INTO zerobucket_images (data, mime_type, size_bytes, checksum_sha256, storage_backend, object_storage_bucket, object_storage_key)
       VALUES (NULL, 'image/png', 10, $1, 'object_storage', 'b', 'k') RETURNING id`,
        ["a".repeat(64)],
      )
    ).rows[0].id as string;
    await expect(zb.get(id)).rejects.toBeInstanceOf(StorageError);
    await expect(zb.getStream(id)).rejects.toBeInstanceOf(StorageError);
    expect((await zb.metadata(id)).mimeType).toBe("image/png"); // metadata is storage-agnostic
    await raw.query("DELETE FROM zerobucket_images WHERE id=$1", [id]);
  });
});
