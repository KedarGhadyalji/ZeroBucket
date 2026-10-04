/**
 * Cross-language conformance, Node half.
 *   tsx conformance/node_side.ts write  <dir>   -> builds corpus, validates + stores it, writes node_*.json
 *   tsx conformance/node_side.ts read   <dir>   -> reads rows the PYTHON package wrote and verifies them
 */
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import sharp from "sharp";
import pg from "pg";
import { ObjectStorage, ZeroBucket, validateImage } from "../src/index.js";

const [mode, dir] = process.argv.slice(2) as [string, string];
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
// ZEROBUCKET_CONF_DEDUP=1 runs the whole flow with BOTH packages in dedup mode (tiering is not supported there).
const DEDUP = process.env.ZEROBUCKET_CONF_DEDUP === "1";
const S3_BUCKET = DEDUP ? undefined : process.env.ZEROBUCKET_TEST_S3_BUCKET;
const S3_ENDPOINT = process.env.ZEROBUCKET_TEST_S3_ENDPOINT;
// Tiering interop is exercised only when an S3-compatible endpoint is provided.
const os =
  S3_BUCKET && S3_ENDPOINT
    ? new ObjectStorage({ bucket: S3_BUCKET, endpoint: S3_ENDPOINT })
    : undefined;
const zb = new ZeroBucket({
  connectionString: process.env.ZEROBUCKET_TEST_DATABASE_URL,
  objectStorage: os,
  dedup: DEDUP,
});
const refCount = async (checksum: string) => {
  const pool = new pg.Pool({
    connectionString: process.env.ZEROBUCKET_TEST_DATABASE_URL,
  });
  try {
    return (
      await pool.query(
        "SELECT ref_count FROM zerobucket_blobs WHERE checksum_sha256=$1",
        [checksum],
      )
    ).rows[0]?.ref_count as number | undefined;
  } finally {
    await pool.end();
  }
};

async function noise(
  fmt: "jpeg" | "png" | "webp",
  w: number,
  h: number,
  ch: 3 | 4 = 3,
  opts: object = {},
) {
  const raw = Buffer.alloc(w * h * ch);
  let s = 12345;
  for (let i = 0; i < raw.length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    raw[i] = s >>> 24;
  }
  return sharp(raw, { raw: { width: w, height: h, channels: ch } })
    [fmt](opts)
    .toBuffer();
}

if (mode === "write") {
  const c = join(dir, "corpus");
  mkdirSync(c, { recursive: true });
  const jpeg = await noise("jpeg", 120, 90);
  const files: Record<string, Buffer> = {
    "rgb.png": await noise("png", 64, 48),
    "rgba.png": await noise("png", 33, 17, 4),
    "photo.jpg": jpeg,
    "photo_copy.jpg": jpeg, // identical bytes: shares one blob in dedup mode
    "progressive.jpg": await noise("jpeg", 80, 80, 3, { progressive: true }),
    "img.webp": await noise("webp", 70, 50),
    "one_px.png": await noise("png", 1, 1),
    "anim_or_gif.gif": await sharp({
      create: { width: 8, height: 8, channels: 3, background: "#f00" },
    })
      .gif()
      .toBuffer(),
    "scan.tiff": await sharp({
      create: { width: 8, height: 8, channels: 3, background: "#0f0" },
    })
      .tiff()
      .toBuffer(),
    "vector.svg": Buffer.from(
      '<svg xmlns="http://www.w3.org/2000/svg" width="10" height="10"/>',
    ),
    "truncated.jpg": jpeg.subarray(0, Math.floor(jpeg.length * 0.6)),
    "garbage.bin": Buffer.from("this is not an image at all"),
    "empty.bin": Buffer.alloc(0),
    "doc.pdf": Buffer.from("%PDF-1.4\n%%EOF"),
  };
  for (const [n, b] of Object.entries(files)) writeFileSync(join(c, n), b);

  const verdicts: Record<string, unknown> = {};
  const stored: Record<string, unknown> = {};
  for (const name of readdirSync(c).sort()) {
    const data = readFileSync(join(c, name));
    try {
      const v = await validateImage(data, { maxBytes: 8 * 1024 * 1024 });
      verdicts[name] = { ok: true, mime: v.mimeType, w: v.width, h: v.height };
      const id = await zb.put(data, { filename: name });
      stored[name] = {
        id,
        checksum: sha(data),
        mime: v.mimeType,
        w: v.width,
        h: v.height,
      };
    } catch (e) {
      verdicts[name] = { ok: false };
    }
  }
  if (os) {
    for (const name of ["rgb.png", "photo.jpg"]) {
      const e = stored[name] as { id: string; tiered?: boolean };
      if (await zb.tierToObjectStorage(e.id)) e.tiered = true;
    }
  }
  writeFileSync(
    join(dir, "node_validation.json"),
    JSON.stringify(verdicts, null, 1),
  );
  writeFileSync(join(dir, "node_ids.json"), JSON.stringify(stored, null, 1));
  console.log(
    `node: validated ${Object.keys(verdicts).length} files, stored ${Object.keys(stored).length}`,
  );
} else {
  const py = JSON.parse(
    readFileSync(join(dir, "py_ids.json"), "utf8"),
  ) as Record<string, any>;
  let n = 0;
  for (const [name, exp] of Object.entries(py)) {
    const img = await zb.get(exp.id);
    const bad: string[] = [];
    if (sha(img.data) !== exp.checksum) bad.push("bytes!=python checksum");
    if (img.checksumSha256 !== exp.checksum)
      bad.push("stored checksum mismatch");
    if (img.mimeType !== exp.mime)
      bad.push(`mime ${img.mimeType}!=${exp.mime}`);
    if (img.width !== exp.w || img.height !== exp.h) bad.push("dimensions");
    if (img.filename !== name) bad.push("filename");
    const chunks: Buffer[] = [];
    for await (const ch of await zb.getStream(exp.id, { chunkSize: 1000 }))
      chunks.push(ch);
    if (sha(Buffer.concat(chunks)) !== exp.checksum) bad.push("stream bytes");
    if (bad.length) {
      console.error("FAIL", name, bad);
      process.exitCode = 1;
    } else n++;
  }
  console.log(
    `node: read ${n}/${Object.keys(py).length} Python-written rows OK`,
  );
  if (DEDUP) {
    // Python wrote photo.jpg and photo_copy.jpg (same bytes => one shared blob). Node deletes ONE of them.
    const a = py["photo.jpg"],
      b = py["photo_copy.jpg"];
    const before = await refCount(a.checksum);
    await zb.delete(b.id);
    const after = await refCount(a.checksum);
    const survivor = await zb.get(a.id);
    if (
      before === undefined ||
      after !== before - 1 ||
      sha(survivor.data) !== a.checksum
    ) {
      console.error("FAIL: dedup shared-blob delete", { before, after });
      process.exitCode = 1;
    } else
      console.log(
        `node: deleted a Python-written ref to a shared blob (ref_count ${before} -> ${after}); the other ref still reads fine`,
      );
  }
  if (os) {
    // A row Python tiered must be cleaned up by NODE's delete, in the shared bucket.
    const t = Object.values(py).find((e: any) => e.tiered) as
      | { id: string }
      | undefined;
    if (!t) {
      console.error("FAIL: Python tiered nothing");
      process.exitCode = 1;
    } else {
      const before = await os.exists(t.id);
      await zb.delete(t.id);
      const after = await os.exists(t.id);
      if (!before || after) {
        console.error("FAIL: node delete of python-tiered row", {
          before,
          after,
        });
        process.exitCode = 1;
      } else
        console.log(
          "node: deleted a Python-tiered row and its S3 object was cleaned up",
        );
    }
  }
}
await zb.close();
