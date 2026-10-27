/**
 * Byte-exact reads from PostgreSQL.
 *
 * Since 0.10.1 image bytes travel from Postgres as base64 text (encode(data, 'base64')) instead of hex text, which is
 * about a third faster and uses about 40% less memory (see the comment above SELECT_FULL in adapters/postgres.ts). The
 * risk in any change to how bytes are read is silent corruption, so these tests insert arbitrary bytes straight into the
 * tables (bypassing put(), which only accepts valid images) and demand an exact match from every read path: get,
 * getMany, getStream with ranges, and dedup mode.
 */
import { createHash } from "node:crypto";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ImageNotFoundError, ZeroBucket } from "../src/index.js";
import { DATABASE_URL, newBucket } from "./helpers.js";

const d = DATABASE_URL ? describe : describe.skip;

/** Seeded bytes covering all 256 values. */
function bytes(n: number, seed = 1): Buffer {
  const b = Buffer.alloc(n);
  let s = seed >>> 0 || 1;
  for (let i = 0; i < n; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    b[i] = s >>> 24;
  }
  return b;
}
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const MB = 1024 * 1024;

/** Lengths around base64's 3-byte groups and Postgres's 76-character line wrapping (57 bytes per line). */
const EDGE_SIZES = [
  1,
  2,
  3,
  4,
  56,
  57,
  58,
  113,
  114,
  115,
  4095,
  4096,
  4097,
  MB - 1,
  MB,
  MB + 1,
];

const PAYLOADS: [string, Buffer][] = [
  ...EDGE_SIZES.map((n): [string, Buffer] => [
    `${n} random bytes`,
    bytes(n, n),
  ]),
  [
    "every byte value 0..255",
    Buffer.from(Array.from({ length: 256 }, (_, i) => i)),
  ],
  ["all zero bytes (compressible)", Buffer.alloc(100_000, 0)],
  ["all 0xFF bytes", Buffer.alloc(100_000, 0xff)],
  [
    "bytes that look like escapes",
    Buffer.from("\\x41\\000\\\\ \r\n\t'\"%_ \\xDEADBEEF", "latin1"),
  ],
  [
    "NUL-heavy mixed bytes",
    Buffer.concat([Buffer.alloc(300, 0), bytes(300, 9), Buffer.alloc(300, 0)]),
  ],
  ["20 MB random", bytes(20 * MB, 77)],
];

async function collect(it: AsyncIterable<Buffer>): Promise<Buffer> {
  const parts: Buffer[] = [];
  for await (const c of it) parts.push(c);
  return Buffer.concat(parts);
}

d("PostgreSQL reads are byte-exact (base64 transport), classic mode", () => {
  let raw: pg.Pool;
  let zb: ZeroBucket;
  const ids: string[] = [];

  async function insert(data: Buffer): Promise<string> {
    const r = await raw.query(
      `INSERT INTO zerobucket_images (data, mime_type, size_bytes, width, height, checksum_sha256)
       VALUES ($1, 'image/png', $2, 1, 1, $3) RETURNING id`,
      [data, data.length, sha(data)],
    );
    ids.push(r.rows[0].id);
    return r.rows[0].id;
  }

  beforeAll(async () => {
    raw = new pg.Pool({ connectionString: DATABASE_URL });
    zb = newBucket();
    await zb.ready();
  });
  afterAll(async () => {
    if (ids.length)
      await raw.query(
        "DELETE FROM zerobucket_images WHERE id = ANY($1::uuid[])",
        [ids],
      );
    await zb.close();
    await raw.end();
  });

  it("get() returns exactly the stored bytes for every payload", async () => {
    for (const [name, data] of PAYLOADS) {
      const id = await insert(data);
      const rec = await zb.get(id);
      expect(rec, name).not.toBeNull();
      expect(rec!.data.length, name).toBe(data.length);
      expect(Buffer.compare(rec!.data, data), name).toBe(0);
      expect(rec!.sizeBytes, name).toBe(data.length);
    }
  });

  it("getMany() returns exactly the stored bytes, in a mixed batch", async () => {
    const picked = PAYLOADS.filter(([n]) => !n.includes("20 MB")).slice(0, 12);
    const batch = new Map<string, Buffer>();
    for (const [, data] of picked) batch.set(await insert(data), data);
    const out = await zb.getMany([...batch.keys()]);
    expect(out.length).toBe(batch.size);
    for (const r of out) {
      expect(r.success, r.imageId).toBe(true);
      expect(
        Buffer.compare(r.image!.data, batch.get(r.imageId)!),
        r.imageId,
      ).toBe(0);
    }
  });

  it("getStream() reassembles the exact bytes at several chunk sizes", async () => {
    const data = bytes(300_000, 5);
    const id = await insert(data);
    for (const chunkSize of [1, 7, 57, 76, 1000, 65_536, MB]) {
      // chunkSize 1 over 300 KB would be 300k round trips: shorten the image for the tiniest sizes.
      const sub = chunkSize < 100 ? bytes(2_000, 6) : data;
      const sid = sub === data ? id : await insert(sub);
      const got = await collect(await zb.getStream(sid, { chunkSize }));
      expect(Buffer.compare(got, sub), `chunkSize ${chunkSize}`).toBe(0);
    }
  });

  it("getStream() honours ranges at awkward offsets, including the first and last byte", async () => {
    const data = bytes(10_000, 11);
    const id = await insert(data);
    const ranges: [number, number][] = [
      [0, 0],
      [0, 9_999],
      [9_999, 9_999],
      [1, 8_998],
      [56, 57],
      [57, 57 + 76],
      [113, 4_096],
      [4_095, 4_097],
      [5_000, 9_999],
    ];
    for (const [start, end] of ranges) {
      const got = await collect(
        await zb.getStream(id, { range: { start, end }, chunkSize: 1_000 }),
      );
      expect(
        Buffer.compare(got, data.subarray(start, end + 1)),
        `${start}-${end}`,
      ).toBe(0);
    }
    // an end past the last byte is clamped, never padded or truncated wrongly
    const clamped = await collect(
      await zb.getStream(id, { range: { start: 9_990, end: 99_999 } }),
    );
    expect(Buffer.compare(clamped, data.subarray(9_990))).toBe(0);
  });

  it("is unaffected by the session's bytea_output setting (the transport no longer depends on it)", async () => {
    const data = bytes(5_000, 21);
    const id = await insert(data);
    const client = await raw.connect();
    try {
      await client.query("SET bytea_output = 'escape'");
      const rec = await zb.get(id, { connection: client });
      expect(Buffer.compare(rec!.data, data)).toBe(0);
      const streamed = await collect(
        await zb.getStream(id, { connection: client, chunkSize: 999 }),
      );
      expect(Buffer.compare(streamed, data)).toBe(0);
    } finally {
      await client.query("RESET bytea_output");
      client.release();
    }
  });

  it("requests base64 from the server for full reads and chunk reads (guards against falling back to hex text)", async () => {
    const id = await insert(bytes(2_000, 31));
    const client = await raw.connect();
    const seen: string[] = [];
    const spy = {
      query: (text: any, values?: any) => {
        seen.push(typeof text === "string" ? text : text.text);
        return client.query(text, values);
      },
    };
    try {
      await zb.get(id, { connection: spy as any });
      await collect(
        await zb.getStream(id, { connection: spy as any, chunkSize: 500 }),
      );
    } finally {
      client.release();
    }
    const reads = seen.filter((t) => /\bdata\b/.test(t) && /select/i.test(t));
    expect(reads.length).toBeGreaterThanOrEqual(2);
    for (const t of reads) expect(t, t).toMatch(/encode\(.*'base64'\)/s);
  });

  it("a tiered row (no bytes in the database) still reports the missing object storage, not a decoding error", async () => {
    const r = await raw.query(
      `INSERT INTO zerobucket_images (data, mime_type, size_bytes, width, height, checksum_sha256, storage_backend, object_storage_bucket, object_storage_key)
       VALUES (NULL, 'image/png', 5, 1, 1, $1, 'object_storage', 'b', 'k') RETURNING id`,
      [sha(Buffer.from("x"))],
    );
    ids.push(r.rows[0].id);
    await expect(zb.get(r.rows[0].id)).rejects.toThrow(
      /constructed without .objectStorage./,
    );
  });

  it("a missing image and a malformed id are still ImageNotFoundError, and a batch reports them per item", async () => {
    await expect(
      zb.get("00000000-0000-4000-8000-000000000000"),
    ).rejects.toBeInstanceOf(ImageNotFoundError);
    await expect(zb.get("not-a-uuid")).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    const [missing] = await zb.getMany([
      "00000000-0000-4000-8000-000000000000",
    ]);
    expect(missing).toMatchObject({
      image: null,
      error: "not found",
      success: false,
    });
  });
});

d("PostgreSQL reads are byte-exact (base64 transport), dedup mode", () => {
  let raw: pg.Pool;
  let zb: ZeroBucket;
  const refIds: string[] = [];
  const checksums: string[] = [];

  async function insert(data: Buffer): Promise<string> {
    const checksum = sha(data);
    await raw.query(
      `INSERT INTO zerobucket_blobs (checksum_sha256, data, mime_type, size_bytes, width, height, ref_count)
       VALUES ($1, $2, 'image/png', $3, 1, 1, 1) ON CONFLICT (checksum_sha256) DO NOTHING`,
      [checksum, data, data.length],
    );
    checksums.push(checksum);
    const r = await raw.query(
      "INSERT INTO zerobucket_image_refs (checksum_sha256) VALUES ($1) RETURNING id",
      [checksum],
    );
    refIds.push(r.rows[0].id);
    return r.rows[0].id;
  }

  beforeAll(async () => {
    raw = new pg.Pool({ connectionString: DATABASE_URL });
    zb = newBucket({ dedup: true });
    await zb.ready();
  });
  afterAll(async () => {
    if (refIds.length)
      await raw.query(
        "DELETE FROM zerobucket_image_refs WHERE id = ANY($1::uuid[])",
        [refIds],
      );
    if (checksums.length)
      await raw.query(
        "DELETE FROM zerobucket_blobs WHERE checksum_sha256 = ANY($1::text[])",
        [checksums],
      );
    await zb.close();
    await raw.end();
  });

  it("get(), getMany() and getStream() return the exact bytes", async () => {
    const picked = PAYLOADS.filter(([n]) => !n.includes("20 MB")).slice(0, 14);
    const batch = new Map<string, Buffer>();
    for (const [name, data] of picked) {
      const id = await insert(data);
      batch.set(id, data);
      const rec = await zb.get(id);
      expect(Buffer.compare(rec!.data, data), name).toBe(0);
      const streamed = await collect(
        await zb.getStream(id, { chunkSize: 4_096 }),
      );
      expect(Buffer.compare(streamed, data), `${name} (stream)`).toBe(0);
    }
    for (const r of await zb.getMany([...batch.keys()])) {
      expect(r.success, r.imageId).toBe(true);
      expect(
        Buffer.compare(r.image!.data, batch.get(r.imageId)!),
        r.imageId,
      ).toBe(0);
    }
    // ranged read from the shared blob
    const [[, one]] = [PAYLOADS.find(([n]) => n === "4097 random bytes")!];
    const id = await insert(one);
    const got = await collect(
      await zb.getStream(id, {
        range: { start: 57, end: 4_000 },
        chunkSize: 700,
      }),
    );
    expect(Buffer.compare(got, one.subarray(57, 4_001))).toBe(0);
  });
});
