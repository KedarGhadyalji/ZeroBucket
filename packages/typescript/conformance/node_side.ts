/**
 * Cross-language conformance, Node half.
 *   tsx conformance/node_side.ts write  <dir>   -> builds corpus, validates + stores it, writes node_*.json
 *   tsx conformance/node_side.ts read   <dir>   -> reads rows the PYTHON package wrote and verifies them
 */
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import sharp from "sharp";
import { ZeroBucket, validateImage } from "../src/index.js";

const [mode, dir] = process.argv.slice(2) as [string, string];
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const zb = new ZeroBucket({
  connectionString: process.env.ZEROBUCKET_TEST_DATABASE_URL,
});

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
}
await zb.close();
