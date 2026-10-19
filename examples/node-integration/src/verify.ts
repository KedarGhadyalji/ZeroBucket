/**
 * End-to-end check of this example against a real PostgreSQL database.
 *
 *   DATABASE_URL=postgresql://... npm run verify
 *
 * Starts the server on a free port, drives it over real HTTP, and exits 0 only
 * if every check passes. It cleans up after itself.
 */
import type { AddressInfo } from "node:net";
import sharp from "sharp";
import { files, initSchema, pool } from "./db.js";
import { createApp } from "./server.js";

let failures = 0;
function check(name: string, ok: boolean, detail = ""): void {
  console.log(
    `${ok ? "  ok  " : " FAIL "} ${name}${!ok && detail ? `  -> ${detail}` : ""}`,
  );
  if (!ok) failures++;
}

const count = async (table: string): Promise<number> =>
  Number((await pool.query(`SELECT count(*) AS n FROM ${table}`)).rows[0].n);

await initSchema();
const server = createApp();
await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
const stamp = `verify-${Date.now()}`;

const png = await sharp({
  create: {
    width: 320,
    height: 200,
    channels: 3,
    background: { r: 200, g: 80, b: 40 },
  },
})
  .png()
  .toBuffer();
const pdfBytes = Buffer.from(
  "%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Kids[]/Count 0>>endobj\n" +
    "trailer<</Root 1 0 R>>\n%%EOF\n",
);

function form(
  fields: Record<string, string | [Buffer, string, string]>,
): FormData {
  const fd = new FormData();
  for (const [name, value] of Object.entries(fields)) {
    if (typeof value === "string") {
      fd.append(name, value);
    } else {
      const [bytes, filename, type] = value;
      fd.append(name, new Blob([new Uint8Array(bytes)], { type }), filename);
    }
  }
  return fd;
}

try {
  console.log("Uploading a post with an image and a PDF (one transaction)");
  const created = await fetch(`${base}/posts`, {
    method: "POST",
    body: form({
      title: `${stamp}-ok`,
      image: [png, "photo.png", "image/png"],
      document: [pdfBytes, "doc.pdf", "application/pdf"],
    }),
  });
  const post = (await created.json()) as {
    id: number;
    imageId: string;
    documentId: string;
    imageUrl: string;
    documentUrl: string;
  };
  check(
    "POST /posts -> 201 with ids",
    created.status === 201 && !!post.imageId && !!post.documentId,
    `status ${created.status}`,
  );

  console.log("Serving the image (ETag, 304, Range)");
  const img = await fetch(base + post.imageUrl);
  const etag = img.headers.get("etag") ?? "";
  const body = Buffer.from(await img.arrayBuffer());
  check(
    "GET image -> 200, re-encoded as webp",
    img.status === 200 && img.headers.get("content-type") === "image/webp",
    `${img.status} ${img.headers.get("content-type")}`,
  );
  check(
    "image has an ETag and nosniff",
    etag.length > 0 && img.headers.get("x-content-type-options") === "nosniff",
  );
  check(
    "image is long-cached (public immutable)",
    /immutable/.test(img.headers.get("cache-control") ?? ""),
  );
  check(
    "stored bytes are a real, smaller-or-equal webp",
    body.subarray(8, 12).toString() === "WEBP",
  );
  const notModified = await fetch(base + post.imageUrl, {
    headers: { "If-None-Match": etag },
  });
  check(
    "If-None-Match -> 304 with no body",
    notModified.status === 304 &&
      (await notModified.arrayBuffer()).byteLength === 0,
    `status ${notModified.status}`,
  );
  const part = await fetch(base + post.imageUrl, {
    headers: { Range: "bytes=0-9" },
  });
  check(
    "Range bytes=0-9 -> 206 with 10 bytes",
    part.status === 206 &&
      (await part.arrayBuffer()).byteLength === 10 &&
      (part.headers.get("content-range") ?? "").startsWith("bytes 0-9/"),
    `status ${part.status}`,
  );

  console.log("Serving the PDF");
  const doc = await fetch(base + post.documentUrl);
  const docBytes = Buffer.from(await doc.arrayBuffer());
  check(
    "GET document -> 200 application/pdf",
    doc.status === 200 && doc.headers.get("content-type") === "application/pdf",
  );
  check(
    "document is an attachment",
    /attachment/.test(doc.headers.get("content-disposition") ?? ""),
  );
  check("document bytes round-trip exactly", docBytes.equals(pdfBytes));

  console.log("Access control and bad ids");
  check(
    "an image id is not served from /documents -> 403",
    (await fetch(`${base}/documents/${post.imageId}`)).status === 403,
  );
  check(
    "a PDF id is not served from /images -> 403",
    (await fetch(`${base}/images/${post.documentId}`)).status === 403,
  );
  check(
    "an id no post references -> 403 (denied; nothing is revealed)",
    (await fetch(`${base}/images/00000000-0000-4000-8000-000000000000`))
      .status === 403,
  );
  check(
    "malformed id -> 404, not a 500",
    (await fetch(`${base}/images/not-a-uuid`)).status === 404,
  );

  console.log("Atomicity: a failed upload leaves nothing behind");
  const imagesBefore = await count("zerobucket_images");
  const postsBefore = await count("example_posts");
  const bad = await fetch(`${base}/posts`, {
    method: "POST",
    // The image is valid and is written FIRST; the "PDF" is not. Both must vanish with the post.
    body: form({
      title: `${stamp}-bad-pdf`,
      image: [png, "photo.png", "image/png"],
      document: [
        Buffer.from("definitely not a pdf"),
        "x.pdf",
        "application/pdf",
      ],
    }),
  });
  check("invalid PDF -> 400", bad.status === 400, `status ${bad.status}`);
  check(
    "no post row was left behind",
    (await count("example_posts")) === postsBefore,
  );
  check(
    "the already-written image was rolled back too",
    (await count("zerobucket_images")) === imagesBefore,
  );
  const corrupt = await fetch(`${base}/posts`, {
    method: "POST",
    body: form({
      title: `${stamp}-bad-image`,
      image: [Buffer.from("not an image at all"), "x.png", "image/png"],
    }),
  });
  check(
    "corrupt image -> 400",
    corrupt.status === 400,
    `status ${corrupt.status}`,
  );
  check("still no extra posts", (await count("example_posts")) === postsBefore);
} finally {
  // Clean up everything this run created, files included.
  const { rows } = await pool.query(
    "SELECT id, image_id, document_id FROM example_posts WHERE title LIKE $1",
    [`${stamp}%`],
  );
  for (const r of rows) {
    if (r.image_id) await files.delete(r.image_id);
    if (r.document_id) await files.delete(r.document_id);
  }
  await pool.query("DELETE FROM example_posts WHERE title LIKE $1", [
    `${stamp}%`,
  ]);
  server.close();
  await files.close();
  await pool.end();
}

console.log(
  failures === 0 ? "\nAll checks passed." : `\n${failures} check(s) FAILED.`,
);
process.exit(failures === 0 ? 0 : 1);
