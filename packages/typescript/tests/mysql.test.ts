import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  CreateBucketCommand,
  GetObjectCommand,
  HeadObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import mysql, {
  type Connection,
  type Pool,
  type PoolConnection,
} from "mysql2/promise";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  AccessDeniedError,
  MySQLBackend,
  PDFValidator,
  ObjectStorage,
  StorageError,
  ZeroBucket,
  type MysqlPoolConnectionLike,
  type MysqlPoolLike,
  type ObjectStorageLike,
  type OperationEvent,
  type PreparedRow,
} from "../src/index.js";
import { makeImage } from "./helpers.js";

const URL = process.env.ZEROBUCKET_TEST_MYSQL_URL;
const ENDPOINT = process.env.ZEROBUCKET_TEST_S3_ENDPOINT;
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const collect = async (it: AsyncIterable<Buffer>) => {
  const a: Buffer[] = [];
  for await (const c of it) a.push(c);
  return a;
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const prow = (over: Partial<PreparedRow> = {}): PreparedRow => ({
  data: Buffer.from("abc"),
  mimeType: "image/png",
  originalFilename: null,
  sizeBytes: 3,
  width: 1,
  height: 1,
  checksumSha256: "a".repeat(64),
  ...over,
});
const UUID_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

describe.skipIf(!URL)("MySQL/MariaDB (real server)", () => {
  let raw: Pool;
  let zb: ZeroBucket;
  const events: OperationEvent[] = [];

  const count = async () =>
    (
      (await raw.query("SELECT COUNT(*) AS n FROM zerobucket_images"))[0] as {
        n: number;
      }[]
    )[0]!.n;
  const dbRow = async (id: string) =>
    (
      (await raw.query("SELECT * FROM zerobucket_images WHERE id = ?", [
        id,
      ])) as any
    )[0][0];

  beforeAll(async () => {
    raw = mysql.createPool({ uri: URL!, connectionLimit: 6, timezone: "Z" });
    await raw.query("DROP TABLE IF EXISTS zerobucket_images");
    zb = new ZeroBucket({ mysql: URL, onOperation: (e) => events.push(e) });
    await zb.ready();
  });
  afterAll(async () => {
    await zb.close();
    await raw.end();
  });

  describe("schema (identical to the Python package's MySQLBackend)", () => {
    it("creates exactly the columns, types, indexes and CHECK constraint Python creates", async () => {
      const [cols] = (await raw.query(
        "SELECT COLUMN_NAME AS n, COLUMN_TYPE AS t, IS_NULLABLE AS nullable, COLUMN_DEFAULT AS d FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images' ORDER BY ORDINAL_POSITION",
      )) as any[];
      expect(
        cols.map(
          // MariaDB reports `int(11)`, MySQL 8.0.19+ reports `int`: same column
          (c: any) =>
            `${c.n}:${String(c.t).replace(/^int\(\d+\)$/, "int")}:${c.nullable}`,
        ),
      ).toEqual([
        "id:char(36):NO",
        "data:longblob:YES",
        "mime_type:varchar(255):NO",
        "original_filename:text:YES",
        "size_bytes:int:NO",
        "width:int:YES",
        "height:int:YES",
        "checksum_sha256:char(64):NO",
        "created_at:datetime(6):NO",
        "updated_at:datetime(6):NO",
        "storage_backend:varchar(20):NO",
        "object_storage_bucket:varchar(255):YES",
        "object_storage_key:varchar(512):YES",
      ]);
      expect(cols.find((c: any) => c.n === "storage_backend").d).toContain(
        "mysql",
      );
      const [idx] = (await raw.query(
        "SELECT DISTINCT INDEX_NAME AS n FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images'",
      )) as any[];
      expect(idx.map((i: any) => i.n).sort()).toEqual([
        "PRIMARY",
        "idx_zerobucket_checksum",
        "idx_zerobucket_created_at",
      ]);
      const [chk] = (await raw.query(
        "SELECT CONSTRAINT_NAME AS n FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_images' AND CONSTRAINT_TYPE = 'CHECK'",
      )) as any[];
      expect(chk.map((c: any) => c.n)).toContain("chk_zerobucket_storage");
    });

    it("the CHECK constraint really rejects a row that is neither local nor tiered", async () => {
      await expect(
        raw.query(
          "INSERT INTO zerobucket_images (id, data, mime_type, size_bytes, checksum_sha256, created_at, updated_at, storage_backend) VALUES (UUID(), NULL, 'image/png', 1, 'a', NOW(6), NOW(6), 'mysql')",
        ),
      ).rejects.toThrow();
    });

    it("migrating many times at once from separate pools all succeed (named lock)", async () => {
      await raw.query("DROP TABLE IF EXISTS zerobucket_images");
      const backends = Array.from(
        { length: 8 },
        () => new MySQLBackend({ connectionString: URL!, poolMaxSize: 1 }),
      );
      await Promise.all(backends.map((b) => b.ready()));
      await Promise.all(backends.map((b) => b.close()));
      expect(await count()).toBe(0);
    });

    it("upgrades a Python-Phase-1 table (no tiering columns) in place, keeping its rows readable", async () => {
      const data = await makeImage("png", 20, 20);
      await raw.query("DROP TABLE IF EXISTS zerobucket_images");
      await raw.query(
        "CREATE TABLE zerobucket_images (id CHAR(36) PRIMARY KEY, data LONGBLOB NOT NULL, mime_type VARCHAR(255) NOT NULL, original_filename TEXT, size_bytes INT NOT NULL, width INT, height INT, checksum_sha256 CHAR(64) NOT NULL, created_at DATETIME(6) NOT NULL, updated_at DATETIME(6) NOT NULL, INDEX idx_zerobucket_checksum (checksum_sha256), INDEX idx_zerobucket_created_at (created_at)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
      );
      const id = "0c9d4d7e-6b0a-4f4f-9a53-0a5f3d1b2c3d";
      await raw.query(
        "INSERT INTO zerobucket_images VALUES (?, ?, 'image/png', 'old.png', ?, 20, 20, ?, NOW(6), NOW(6))",
        [id, data, data.length, sha(data)],
      );
      const upgraded = new ZeroBucket({ mysql: URL });
      const img = await upgraded.get(id);
      expect(img.data.equals(data)).toBe(true);
      expect(img.filename).toBe("old.png");
      expect((await dbRow(id)).storage_backend).toBe("mysql");
      await upgraded.close();
    });
  });

  describe("core behaviour", () => {
    it("round-trips bytes exactly, with a non-ASCII filename, and stores UTC timestamps", async () => {
      const data = await makeImage("png", 64, 48, { seed: 7 });
      const id = await zb.put(data, { filename: "фото 📷 café.png" });
      expect(id).toMatch(UUID_RE);
      const img = await zb.get(id);
      expect(img.data.equals(data)).toBe(true);
      expect(img.filename).toBe("фото 📷 café.png");
      expect(img.mimeType).toBe("image/png");
      expect([img.width, img.height]).toEqual([64, 48]);
      expect(img.checksumSha256).toBe(sha(data));
      const r = await dbRow(id);
      expect(r.storage_backend).toBe("mysql");
      expect(r.object_storage_key).toBeNull();
      const [[age]] = (await raw.query(
        "SELECT TIMESTAMPDIFF(SECOND, created_at, UTC_TIMESTAMP(6)) AS s, created_at = updated_at AS same FROM zerobucket_images WHERE id = ?",
        [id],
      )) as any[];
      expect(age.s).toBeGreaterThanOrEqual(-1); // naive UTC, whatever the server's own timezone is
      expect(age.s).toBeLessThan(30);
      expect(Number(age.same)).toBe(1);
    });

    it("stores a multi-megabyte image (blob travels as raw bytes, not a hex literal)", async () => {
      const data = await makeImage("png", 1300, 900, { seed: 3 });
      expect(data.length).toBeGreaterThan(2_500_000);
      const id = await zb.put(data);
      expect((await zb.get(id)).data.equals(data)).toBe(true);
    });

    it("metadata, exists, delete; missing and malformed ids are 'not found', never a database error", async () => {
      const id = await zb.put(await makeImage("jpeg", 30, 20), {
        filename: "m.jpg",
      });
      const m = await zb.metadata(id);
      expect(m).toMatchObject({
        imageId: id,
        mimeType: "image/jpeg",
        filename: "m.jpg",
        width: 30,
        height: 20,
      });
      expect(await zb.exists(id)).toBe(true);
      expect(await zb.exists(id.toUpperCase())).toBe(true);
      expect(await zb.delete(id)).toBe(true);
      expect(await zb.delete(id)).toBe(false);
      expect(await zb.exists(id)).toBe(false);
      for (const bad of [
        "11111111-1111-1111-1111-111111111111",
        "not-a-uuid",
        "1; DROP TABLE zerobucket_images",
        "",
      ]) {
        await expect(zb.get(bad)).rejects.toMatchObject({
          name: "ImageNotFoundError",
        });
        expect(await zb.exists(bad)).toBe(false);
        expect(await zb.delete(bad)).toBe(false);
      }
      expect(await count()).toBeGreaterThan(0); // the table is still there
    });

    it("putMany keeps input order and maps ids to inputs; getMany reports missing ids without throwing", async () => {
      const imgs = await Promise.all(
        Array.from({ length: 12 }, (_, i) =>
          makeImage("png", 20 + i, 20, { seed: i + 1 }),
        ),
      );
      const put = await zb.putMany(imgs, {
        filenames: imgs.map((_, i) => `f${i}.png`),
      });
      expect(put.every((r) => r.success)).toBe(true);
      const ids = put.map((r) => r.imageId!);
      expect(new Set(ids).size).toBe(12);
      const missing = "22222222-2222-2222-2222-222222222222";
      const got = await zb.getMany([ids[3]!, missing, ids[0]!, "junk"]);
      expect(got.map((g) => g.success)).toEqual([true, false, true, false]);
      expect(got[0]!.image!.data.equals(imgs[3]!)).toBe(true);
      expect(got[2]!.image!.filename).toBe("f0.png");
    });

    it("deleteMany deletes only what exists and tolerates duplicate ids", async () => {
      const a = await zb.put(await makeImage("png", 10, 10, { seed: 21 }));
      const b = await zb.put(await makeImage("png", 10, 10, { seed: 22 }));
      const res = await zb.deleteMany([
        a,
        a,
        b,
        "33333333-3333-3333-3333-333333333333",
        "nope",
      ]);
      expect(
        Object.fromEntries(res.map((r) => [r.imageId, r.deleted])),
      ).toMatchObject({
        [a]: true,
        [b]: true,
        "33333333-3333-3333-3333-333333333333": false,
      });
      expect(await zb.exists(a)).toBe(false);
      expect(await zb.exists(b)).toBe(false);
    });

    it("overlapping deleteMany batches in opposite orders never deadlock (retry disabled, so a deadlock would surface)", async () => {
      // Regression: on MariaDB 10.11 `DELETE ... WHERE id IN (...)` is planned as a full scan on a small table, which
      // locks every row it passes and deadlocked two overlapping batches about 40% of the time (MySQL 8.0.46 plans
      // a range there and does not deadlock, so this test only FAILS on MariaDB with the old code). Batches now
      // lock with one SELECT ... FOR UPDATE and delete by primary key.
      const imgs = await Promise.all(
        Array.from({ length: 12 }, (_, i) =>
          makeImage("png", 16 + i, 16, { seed: 300 + i }),
        ),
      );
      const fwd = new ZeroBucket({ mysql: URL, maxRetries: 0 });
      const bwd = new ZeroBucket({ mysql: URL, maxRetries: 0 });
      for (let round = 0; round < 15; round++) {
        const put = await fwd.putMany(imgs);
        const ids = put.map((r) => r.imageId!);
        const half = ids.length / 2;
        const del = await Promise.all([
          fwd.deleteMany(ids.slice(0, half)),
          bwd.deleteMany(ids.slice(half).reverse()),
          bwd.deleteMany(ids.slice(0, half).reverse()), // overlaps the first batch: only one may win each id
        ]);
        expect(del.flat().every((r) => r.success)).toBe(true);
        const deletedOnce = del.flat().filter((r) => r.deleted).length;
        expect(deletedOnce).toBe(ids.length); // every id deleted exactly once, never twice, never missed
        for (const id of ids) expect(await fwd.exists(id)).toBe(false);
      }
      await fwd.close();
      await bwd.close();
    });

    it("accepts mariadb:// URLs", async () => {
      const m = new ZeroBucket({ mysql: URL!.replace(/^mysql:/, "mariadb:") });
      const id = await m.put(await makeImage("png", 10, 10, { seed: 30 }));
      expect(await m.exists(id)).toBe(true);
      await m.close();
    });

    it("refuses two databases at once, and dedup together with objectStorage, with clear errors", () => {
      expect(
        () =>
          new MySQLBackend({
            connectionString: URL!,
            dedup: true,
            objectStorage: new ObjectStorage({ bucket: "b" }),
          }),
      ).toThrow(/objectStorage is not supported together with dedup/);
      expect(() => new ZeroBucket({ mysql: URL, sqlite: ":memory:" })).toThrow(
        /Choose one database/,
      );
      expect(
        () =>
          new ZeroBucket({ mysql: URL, connectionString: "postgresql://x" }),
      ).toThrow(/Choose one database/);
      expect(() => new MySQLBackend({})).toThrow(/connectionString/);
    });

    it("a wrong password fails at ready() with a StorageError, not a raw driver error", async () => {
      // `URL` is shadowed by the test URL string above, hence globalThis. Works whatever the real password is.
      const wrong = new globalThis.URL(URL!);
      wrong.password = "definitely-not-the-password";
      const bad = new ZeroBucket({ mysql: wrong.toString(), maxRetries: 0 });
      await expect(bad.ready()).rejects.toBeInstanceOf(StorageError);
      await expect(bad.ready()).rejects.toThrow(/MySQL\/MariaDB/);
      await bad.close();
    });
  });

  describe("client-layer features run unchanged on MySQL", () => {
    it("access-control hooks gate get/metadata/stream and put, with the caller's context", async () => {
      type Ctx = { user: string };
      const owners = new Map<string, string>();
      const g = new ZeroBucket<Ctx>({
        mysql: URL,
        beforeGet: (id, ctx) => owners.get(id) === ctx?.user,
        beforePut: (ctx) => ctx?.user !== "banned",
      });
      const id = await g.put(await makeImage("png", 9, 9, { seed: 60 }), {
        context: { user: "alice" },
      });
      owners.set(id, "alice");
      const alice = { context: { user: "alice" } };
      const bob = { context: { user: "bob" } };
      expect((await g.get(id, alice)).width).toBe(9);
      expect((await g.metadata(id, alice))!.width).toBe(9);
      expect(
        Buffer.concat(await collect(await g.getStream(id, alice))).length,
      ).toBeGreaterThan(0);
      await expect(g.get(id, bob)).rejects.toBeInstanceOf(AccessDeniedError);
      await expect(g.metadata(id, bob)).rejects.toBeInstanceOf(
        AccessDeniedError,
      );
      await expect(g.getStream(id, bob)).rejects.toBeInstanceOf(
        AccessDeniedError,
      );
      await expect(
        g.put(await makeImage("png", 9, 9, { seed: 61 }), {
          context: { user: "banned" },
        }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      await g.close();
    });

    it("the optimize pipeline downsizes and re-encodes before storing", async () => {
      const big = await makeImage("png", 800, 400, { seed: 62 });
      const id = await zb.put(big, {
        optimize: { maxWidth: 200, format: "webp", quality: 70 },
      });
      const img = await zb.get(id);
      expect(img.mimeType).toBe("image/webp");
      expect([img.width, img.height]).toEqual([200, 100]);
      expect(img.checksumSha256).toBe(sha(img.data));
      expect(img.data.length).toBeLessThan(big.length);
    });

    it("a custom (PDF) validator stores non-images", async () => {
      const pdf = Buffer.from(
        "%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF",
      );
      const id = await zb.put(pdf, {
        validator: new PDFValidator(),
        filename: "doc.pdf",
      });
      const doc = await zb.get(id);
      expect(doc.mimeType).toBe("application/pdf");
      expect(doc.width).toBeNull();
      expect(doc.data.equals(pdf)).toBe(true);
    });

    it("HEIC (iPhone) photos are accepted and their bytes come back unchanged", async () => {
      const tall = readFileSync(
        join(process.cwd(), "tests/fixtures/tall.heic"),
      );
      const id = await zb.put(tall);
      const img = await zb.get(id);
      expect(img.data.equals(tall)).toBe(true);
      expect(img).toMatchObject({
        mimeType: "image/heic",
        width: 90,
        height: 160,
      });
    });

    it("validation failures never reach the database", async () => {
      const before = await count();
      await expect(zb.put(Buffer.from("not an image"))).rejects.toMatchObject({
        name: expect.stringMatching(/Validation|Unsupported|Corrupt/),
      });
      expect(await count()).toBe(before);
    });
  });

  describe("streaming", () => {
    let id: string;
    let data: Buffer;
    beforeAll(async () => {
      data = await makeImage("png", 300, 200, { seed: 5 });
      id = await zb.put(data);
    });

    it("chunks reassemble to the original; every chunk but the last has the requested size", async () => {
      const chunks = await collect(await zb.getStream(id, { chunkSize: 1000 }));
      expect(Buffer.concat(chunks).equals(data)).toBe(true);
      expect(chunks.slice(0, -1).every((c) => c.length === 1000)).toBe(true);
      expect(chunks.at(-1)!.length).toBeLessThanOrEqual(1000);
    });

    it("honours inclusive byte ranges and clamps an end past the file", async () => {
      const mid = await collect(
        await zb.getStream(id, {
          chunkSize: 700,
          range: { start: 10, end: 5009 },
        }),
      );
      expect(Buffer.concat(mid).equals(data.subarray(10, 5010))).toBe(true);
      const tail = await collect(
        await zb.getStream(id, {
          chunkSize: 4096,
          range: { start: data.length - 100, end: data.length * 10 },
        }),
      );
      expect(Buffer.concat(tail).equals(data.subarray(data.length - 100))).toBe(
        true,
      );
    });

    it("rejects an invalid chunk size and a missing image", async () => {
      await expect(zb.getStream(id, { chunkSize: 0 })).rejects.toThrow(
        /chunkSize/,
      );
      await expect(
        zb.getStream("44444444-4444-4444-4444-444444444444", {}),
      ).rejects.toMatchObject({ name: "ImageNotFoundError" });
    });

    it("an image deleted mid-stream raises StorageError instead of silently truncating", async () => {
      const victim = await zb.put(
        await makeImage("png", 300, 200, { seed: 9 }),
      );
      const stream = (await zb.getStream(victim, { chunkSize: 500 }))[
        Symbol.asyncIterator
      ]();
      expect((await stream.next()).done).toBe(false);
      await raw.query("DELETE FROM zerobucket_images WHERE id = ?", [victim]);
      await expect(stream.next()).rejects.toThrow(/deleted while streaming/);
    });
  });

  describe("transactions on a caller-supplied connection", () => {
    let conn: Connection;
    beforeAll(async () => {
      conn = await mysql.createConnection({ uri: URL! });
    });
    afterAll(async () => {
      await conn.end();
    });

    it("joins the caller's transaction: invisible to others until COMMIT, gone after ROLLBACK", async () => {
      const be = new MySQLBackend({ connectionString: URL! });
      await conn.beginTransaction();
      const id = await be.put(prow({ checksumSha256: "b".repeat(64) }), {
        connection: conn,
      });
      expect(await be.exists(id, { connection: conn })).toBe(true); // sees its own uncommitted row
      expect(await be.exists(id)).toBe(false); // another connection does not
      await conn.commit();
      expect(await be.exists(id)).toBe(true);

      await conn.beginTransaction();
      const gone = await be.put(prow({ checksumSha256: "c".repeat(64) }), {
        connection: conn,
      });
      await conn.rollback();
      expect(await be.exists(gone)).toBe(false);
      await be.close();
    });

    it("putMany on an autocommit connection is all-or-nothing (no half-applied batch)", async () => {
      const be = new MySQLBackend({ connectionString: URL! });
      const before = await count();
      await expect(
        be.putMany(
          [
            prow(),
            prow(),
            prow({ mimeType: "x".repeat(300) }), // too long for VARCHAR(255): fails the 3rd insert
            prow(),
          ],
          { connection: conn },
        ),
      ).rejects.toBeInstanceOf(StorageError);
      expect(await count()).toBe(before);
      await be.close();
    });

    it("a failed putMany inside the caller's transaction undoes only its own rows and leaves the transaction usable", async () => {
      const be = new MySQLBackend({ connectionString: URL! });
      await conn.beginTransaction();
      const kept = await be.put(prow({ checksumSha256: "d".repeat(64) }), {
        connection: conn,
      });
      await expect(
        be.putMany([prow(), prow({ mimeType: "x".repeat(300) })], {
          connection: conn,
        }),
      ).rejects.toBeInstanceOf(StorageError);
      expect(await be.exists(kept, { connection: conn })).toBe(true); // caller's earlier work survives
      await conn.commit(); // the transaction is still alive and commits
      expect(await be.exists(kept)).toBe(true);
      const [[n]] = (await raw.query(
        "SELECT COUNT(*) AS n FROM zerobucket_images WHERE checksum_sha256 = ?",
        ["a".repeat(64)],
      )) as any[];
      expect(Number(n.n)).toBe(0); // none of the failed batch's rows leaked
      await be.close();
    });

    it("rejects something that is not a mysql2 connection", async () => {
      const be = new MySQLBackend({ connectionString: URL! });
      await expect(
        be.put(prow(), {
          connection: { query: async () => ({ rows: [] }) } as any,
        }),
      ).rejects.toThrow(/mysql2\/promise connection/);
      await be.close();
    });
  });

  describe("retry, metrics and pooling", () => {
    /**
     * A mysql2 pool wrapper that records the thread id of every connection it hands out and every
     * connection that was destroyed (instead of released), and can inject a failure into the next statement.
     */
    function trackedPool(
      real: Pool,
      onConn?: (c: PoolConnection) => Promise<void>,
    ) {
      const seen: number[] = [];
      const destroyed: number[] = [];
      const inject: Error[] = [];
      const pool: MysqlPoolLike = {
        async getConnection() {
          const c = await real.getConnection();
          seen.push(c.threadId);
          await onConn?.(c);
          return new Proxy(c, {
            get(t, p) {
              if (p === "query" || p === "execute")
                return async (...a: unknown[]) => {
                  const err = inject.shift();
                  if (err) throw err;
                  return (t as any)[p](...a);
                };
              if (p === "destroy")
                return () => {
                  destroyed.push(t.threadId);
                  t.destroy();
                };
              const v = (t as any)[p];
              return typeof v === "function" ? v.bind(t) : v;
            },
          }) as unknown as MysqlPoolConnectionLike;
        },
        end: () => real.end(),
      };
      return { pool, seen, destroyed, inject };
    }

    it("emits exactly one event per operation, with failures reported and a throwing callback harmless", async () => {
      const evs: OperationEvent[] = [];
      let first = true;
      const m = new ZeroBucket({
        mysql: URL,
        maxRetries: 0,
        onOperation: (e) => {
          evs.push(e);
          if (first) {
            first = false;
            throw new Error("a buggy metrics callback");
          }
        },
      });
      const id = await m.put(await makeImage("png", 10, 10, { seed: 40 }));
      await m.get(id);
      const ops = evs.map((e) => e.operation);
      expect(ops).toContain("migrate");
      expect(ops.filter((o) => o === "put")).toHaveLength(1);
      expect(ops.filter((o) => o === "get")).toHaveLength(1);
      expect(evs.every((e) => e.success && e.retryCount === 0)).toBe(true);
      await m.close();
    });

    it("a connection that dies mid-call is destroyed (never reused) and the whole call is retried on a fresh one", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const { pool, seen, destroyed, inject } = trackedPool(real);
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        pool,
        retryBaseDelayMs: 10,
        onOperation: (e) => evs.push(e),
      });
      const id = await be.put(prow({ checksumSha256: "e".repeat(64) }));
      inject.push(
        Object.assign(
          new Error("Connection lost: The server closed the connection."),
          {
            code: "PROTOCOL_CONNECTION_LOST",
            fatal: true,
          },
        ),
      );
      const victim = seen.length;
      expect(await be.exists(id)).toBe(true);
      const ev = evs.filter((e) => e.operation === "exists").at(-1)!;
      expect(ev).toMatchObject({ success: true, retryCount: 1 });
      expect(destroyed).toHaveLength(1);
      expect(destroyed[0]).toBe(seen[victim - 1]); // the connection that failed is the one destroyed
      expect(seen.at(-1)).not.toBe(destroyed[0]); // and the retry ran on a different one
      await be.close();
      await real.end();
    });

    it("transient server errors (deadlock, server shutdown) are retried; constraint errors are not", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const { pool, inject } = trackedPool(real);
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        pool,
        retryBaseDelayMs: 5,
        onOperation: (e) => evs.push(e),
      });
      const id = await be.put(prow({ checksumSha256: "1".repeat(64) }));
      const sqlErr = (errno: number, code: string) =>
        Object.assign(new Error(code), { errno, code });
      inject.push(
        sqlErr(1213, "ER_LOCK_DEADLOCK"),
        sqlErr(1053, "ER_SERVER_SHUTDOWN"),
      );
      expect(await be.exists(id)).toBe(true);
      expect(evs.filter((e) => e.operation === "exists").at(-1)).toMatchObject({
        success: true,
        retryCount: 2,
      });
      inject.push(sqlErr(1062, "ER_DUP_ENTRY"));
      await expect(be.exists(id)).rejects.toBeInstanceOf(StorageError);
      expect(evs.filter((e) => e.operation === "exists").at(-1)).toMatchObject({
        success: false,
        retryCount: 0,
      });
      await be.close();
      await real.end();
    });

    it("an image bigger than the server's max_allowed_packet gets an actionable error and is not retried", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const { pool, inject } = trackedPool(real);
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        pool,
        retryBaseDelayMs: 5,
        onOperation: (e) => evs.push(e),
      });
      await be.ready();
      inject.push(
        Object.assign(
          new Error("Got a packet bigger than 'max_allowed_packet' bytes"),
          {
            errno: 1153,
            code: "ER_NET_PACKET_TOO_LARGE",
          },
        ),
      );
      await expect(be.put(prow())).rejects.toThrow(
        /larger than the server's max_allowed_packet.*SET GLOBAL max_allowed_packet/s,
      );
      expect(evs.filter((e) => e.operation === "put").at(-1)).toMatchObject({
        success: false,
        retryCount: 0,
      });
      expect(await be.put(prow({ checksumSha256: "4".repeat(64) }))).toMatch(
        UUID_RE,
      ); // pool slot not leaked
      await be.close();
      await real.end();
    });

    it("a connection killed on the server is survived: the pool replaces it, with or without a retry", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const { pool, seen } = trackedPool(real);
      const be = new MySQLBackend({ pool, retryBaseDelayMs: 10 });
      const id = await be.put(prow({ checksumSha256: "2".repeat(64) }));
      const victim = seen.at(-1)!;
      await raw.query(`KILL ${victim}`);
      // No sleep: whether mysql2 has noticed the dead socket yet is a race, and both outcomes must work.
      expect(await be.exists(id)).toBe(true);
      expect(await be.exists(id)).toBe(true);
      expect(seen.at(-1)).not.toBe(victim);
      await be.close();
      await real.end();
    });

    it("startup issues no DDL when the schema is current, so it never queues behind an open transaction", async () => {
      const conn = await mysql.createConnection({ uri: URL! });
      const id = "77777777-7777-7777-7777-777777777777";
      await conn.beginTransaction();
      await conn.query(
        "INSERT INTO zerobucket_images (id, data, mime_type, size_bytes, checksum_sha256, created_at, updated_at) VALUES (?, 'x', 'image/png', 1, ?, NOW(6), NOW(6))",
        [id, "3".repeat(64)],
      ); // this transaction now holds a metadata lock on the table
      const late = new MySQLBackend({
        connectionString: URL!,
        poolTimeoutMs: 3000,
      });
      const t0 = Date.now();
      await late.ready(); // a no-op ALTER here would block until the transaction ended
      expect(Date.now() - t0).toBeLessThan(2000);
      expect(await late.exists(id)).toBe(false); // uncommitted rows stay invisible
      await conn.rollback();
      await conn.end();
      await late.close();
    });

    it("a lock-wait timeout is retried as a whole transaction and succeeds once the lock clears", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const { pool } = trackedPool(real, async (c) => {
        await c.query("SET SESSION innodb_lock_wait_timeout = 1");
      });
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        pool,
        retryBaseDelayMs: 20,
        maxRetries: 4,
        onOperation: (e) => evs.push(e),
      });
      const id = await be.put(prow({ checksumSha256: "f".repeat(64) }));
      const holder = await mysql.createConnection({ uri: URL! });
      await holder.beginTransaction();
      await holder.query(
        "SELECT id FROM zerobucket_images WHERE id = ? FOR UPDATE",
        [id],
      );
      const release = sleep(1500).then(() => holder.commit());
      expect(await be.delete(id)).toBe(true);
      await release;
      const ev = evs.filter((e) => e.operation === "delete").at(-1)!;
      expect(ev.success).toBe(true);
      expect(ev.retryCount).toBeGreaterThanOrEqual(1);
      expect(await be.exists(id)).toBe(false);
      await holder.end();
      await be.close();
      await real.end();
    }, 20_000);

    it("with maxRetries: 0 the same lock timeout surfaces as a StorageError after exactly one attempt", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const { pool } = trackedPool(real, async (c) => {
        await c.query("SET SESSION innodb_lock_wait_timeout = 1");
      });
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        pool,
        maxRetries: 0,
        onOperation: (e) => evs.push(e),
      });
      const id = await be.put(prow({ checksumSha256: "9".repeat(64) }));
      const holder = await mysql.createConnection({ uri: URL! });
      await holder.beginTransaction();
      await holder.query(
        "SELECT id FROM zerobucket_images WHERE id = ? FOR UPDATE",
        [id],
      );
      await expect(be.delete(id)).rejects.toBeInstanceOf(StorageError);
      const ev = evs.filter((e) => e.operation === "delete").at(-1)!;
      expect(ev.success).toBe(false);
      expect(ev.retryCount).toBe(0);
      expect(ev.error).toMatch(/lock wait timeout/i);
      await holder.rollback();
      await holder.end();
      expect(await be.delete(id)).toBe(true); // the pool slot was returned: the backend still works
      await be.close();
      await real.end();
    }, 20_000);

    it("a permanent error is not retried", async () => {
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        connectionString: URL!,
        retryBaseDelayMs: 10,
        onOperation: (e) => evs.push(e),
      });
      await expect(
        be.put(prow({ mimeType: "x".repeat(300) })),
      ).rejects.toBeInstanceOf(StorageError);
      const ev = evs.filter((e) => e.operation === "put").at(-1)!;
      expect(ev.success).toBe(false);
      expect(ev.retryCount).toBe(0);
      await be.close();
    });

    it("a caller-supplied connection is never retried", async () => {
      const holder = await mysql.createConnection({ uri: URL! });
      const mine = await mysql.createConnection({ uri: URL! });
      await mine.query("SET SESSION innodb_lock_wait_timeout = 1");
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        connectionString: URL!,
        maxRetries: 5,
        onOperation: (e) => evs.push(e),
      });
      const id = await be.put(prow({ checksumSha256: "8".repeat(64) }));
      await holder.beginTransaction();
      await holder.query(
        "SELECT id FROM zerobucket_images WHERE id = ? FOR UPDATE",
        [id],
      );
      await expect(be.delete(id, { connection: mine })).rejects.toBeInstanceOf(
        StorageError,
      );
      const del = evs.filter((e) => e.operation === "delete");
      expect(del).toHaveLength(1);
      expect(del[0]!.retryCount).toBe(0);
      await holder.rollback();
      await holder.end();
      await mine.end();
      await be.close();
    }, 20_000);

    it("the pool really is bounded: 60 concurrent calls through a 2-slot pool open at most 2 connections", async () => {
      const status = async () =>
        Number(
          (
            (await raw.query("SHOW GLOBAL STATUS LIKE 'Connections'")) as any
          )[0][0].Value,
        );
      const id = await zb.put(await makeImage("png", 10, 10, { seed: 50 }));
      const be = new MySQLBackend({ connectionString: URL!, poolMaxSize: 2 });
      await be.ready();
      const before = await status();
      await Promise.all(Array.from({ length: 60 }, () => be.get(id)));
      const opened = (await status()) - before - 1; // minus the SHOW STATUS call's own pool connection
      expect(opened).toBeLessThanOrEqual(2);
      await be.close();
    });

    it("waiting for a pool slot times out with a clear, non-retried error, and the slot is not leaked", async () => {
      const real = mysql.createPool({ uri: URL!, connectionLimit: 1 });
      const evs: OperationEvent[] = [];
      const be = new MySQLBackend({
        pool: real as unknown as MysqlPoolLike,
        poolTimeoutMs: 300,
        onOperation: (e) => evs.push(e),
      });
      const id = await be.put(prow({ checksumSha256: "7".repeat(64) }));
      const held = await real.getConnection(); // the pool's only connection
      const t0 = Date.now();
      await expect(be.exists(id)).rejects.toThrow(
        /Timed out after 300ms waiting for a MySQL\/MariaDB connection/,
      );
      expect(Date.now() - t0).toBeLessThan(1500); // one wait, not (retries + 1) waits
      const ev = evs.filter((e) => e.operation === "exists").at(-1)!;
      expect(ev).toMatchObject({ success: false, retryCount: 0 });
      held.release();
      await sleep(50); // the timed-out waiter's connection is handed straight back
      expect(await be.exists(id)).toBe(true);
      expect(await be.exists(id)).toBe(true);
      await be.close(); // BYO pool: still ours
      await real.query("SELECT 1");
      await real.end();
    });

    it("close() makes later calls fail clearly, and a pool we own is really closed", async () => {
      const be = new MySQLBackend({ connectionString: URL! });
      await be.ready();
      await be.close();
      await expect(
        be.get("55555555-5555-5555-5555-555555555555"),
      ).rejects.toThrow(/closed/);
    });
  });
});

// ---------------------------------------------------------------------------------------------

describe.skipIf(!URL || !ENDPOINT)(
  "MySQL/MariaDB object-storage tiering (real server + real S3 API)",
  () => {
    const bucket = `zb-mysql-test-${Date.now()}`;
    const ranges: (string | undefined)[] = [];
    let s3: S3Client;
    let raw: Pool;
    let real: ObjectStorage;
    let zb: ZeroBucket;
    const events: OperationEvent[] = [];

    const row = async (id: string) =>
      (
        (await raw.query("SELECT * FROM zerobucket_images WHERE id = ?", [
          id,
        ])) as any
      )[0][0];
    const object = async (key: string) =>
      Buffer.from(
        await (
          await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
        ).Body!.transformToByteArray(),
      );
    const objectExists = (key: string) =>
      s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key })).then(
        () => true,
        () => false,
      );
    const fresh = async (seed = 1) => {
      const data = await makeImage("png", 200, 200, { seed });
      return { data, id: await zb.put(data, { filename: "t.png" }) };
    };
    const spy = (over: Partial<ObjectStorageLike> = {}): ObjectStorageLike => ({
      bucket: real.bucket,
      upload: over.upload ?? real.upload.bind(real),
      download: over.download ?? real.download.bind(real),
      downloadRange: over.downloadRange ?? real.downloadRange.bind(real),
      delete: over.delete ?? real.delete.bind(real),
      exists: real.exists.bind(real),
    });

    beforeAll(async () => {
      const cfg = {
        endpoint: ENDPOINT,
        region: "us-east-1",
        forcePathStyle: true,
        credentials: { accessKeyId: "test", secretAccessKey: "test" },
      };
      s3 = new S3Client(cfg);
      await s3.send(new CreateBucketCommand({ Bucket: bucket }));
      const spyClient = new S3Client(cfg);
      spyClient.middlewareStack.add(
        (next, ctx) => async (args: any) => {
          if (ctx.commandName === "GetObjectCommand")
            ranges.push(args.input.Range);
          return next(args);
        },
        { step: "initialize", name: "rangeSpy" },
      );
      real = new ObjectStorage({ bucket, client: spyClient });
      raw = mysql.createPool({ uri: URL!, connectionLimit: 4 });
      zb = new ZeroBucket({
        mysql: URL,
        objectStorage: real,
        onOperation: (e) => events.push(e),
      });
      await zb.ready();
    });
    afterAll(async () => {
      await zb.close();
      await raw.end();
    });

    it("tiers: bytes move to S3, the row becomes a pointer, and reads are transparent", async () => {
      const { data, id } = await fresh(1);
      expect(await zb.tierToObjectStorage(id)).toBe(true);
      const r = await row(id);
      expect(r.storage_backend).toBe("object_storage");
      expect(r.data).toBeNull();
      expect(r.object_storage_bucket).toBe(bucket);
      expect(r.object_storage_key).toBe(id);
      expect((await object(id)).equals(data)).toBe(true);
      const img = await zb.get(id);
      expect(img.data.equals(data)).toBe(true);
      expect(img.filename).toBe("t.png");
      expect(await zb.exists(id)).toBe(true);
      expect((await zb.metadata(id))!.sizeBytes).toBe(data.length);
    });

    it("tiering is idempotent and tells unknown, malformed and already-tiered ids apart", async () => {
      const { id } = await fresh(2);
      expect(await zb.tierToObjectStorage(id)).toBe(true);
      expect(await zb.tierToObjectStorage(id)).toBe(false);
      for (const bad of ["66666666-6666-6666-6666-666666666666", "not-a-uuid"])
        await expect(zb.tierToObjectStorage(bad)).rejects.toMatchObject({
          name: "ImageNotFoundError",
        });
    });

    it("streams a tiered image with real HTTP Range requests", async () => {
      const { data, id } = await fresh(3);
      await zb.tierToObjectStorage(id);
      ranges.length = 0;
      const chunks = await collect(
        await zb.getStream(id, {
          chunkSize: 4000,
          range: { start: 100, end: 9099 },
        }),
      );
      expect(Buffer.concat(chunks).equals(data.subarray(100, 9100))).toBe(true);
      expect(ranges.filter(Boolean).length).toBeGreaterThanOrEqual(3);
      expect(ranges[0]).toBe("bytes=100-4099");
    });

    it("a failed upload leaves the row byte-for-byte untouched and creates no object", async () => {
      const { id } = await fresh(4);
      const before = await row(id);
      const broken = new ZeroBucket({
        mysql: URL,
        maxRetries: 0,
        objectStorage: spy({
          upload: async () => {
            throw new Error("S3 is down");
          },
        }),
      });
      await expect(broken.tierToObjectStorage(id)).rejects.toThrow(
        /S3 is down/,
      );
      const after = await row(id);
      expect(after).toEqual(before);
      expect(await objectExists(id)).toBe(false);
      await broken.close();
      expect(await zb.tierToObjectStorage(id)).toBe(true); // and a later attempt works
    });

    it("tier needs objectStorage, and a tiered image cannot be read without it (clear errors)", async () => {
      const { id } = await fresh(5);
      const plain = new ZeroBucket({ mysql: URL });
      await expect(plain.tierToObjectStorage(id)).rejects.toThrow(
        /requires objectStorage/,
      );
      await zb.tierToObjectStorage(id);
      await expect(plain.get(id)).rejects.toThrow(/object storage/);
      await expect(plain.getStream(id, {})).rejects.toThrow(/object storage/);
      await plain.close();
    });

    it("two concurrent tier calls for one image: exactly one does the work", async () => {
      const { id } = await fresh(6);
      const results = await Promise.all([
        zb.tierToObjectStorage(id),
        zb.tierToObjectStorage(id),
      ]);
      expect(results.filter((r) => r === true)).toHaveLength(1);
      expect(results.filter((r) => r === false)).toHaveLength(1);
    });

    it("delete cleans up the object; deleteMany handles tiered and untiered rows together", async () => {
      const a = await fresh(7);
      const b = await fresh(8);
      await zb.tierToObjectStorage(a.id);
      expect(await objectExists(a.id)).toBe(true);
      expect(await zb.delete(a.id)).toBe(true);
      expect(await objectExists(a.id)).toBe(false);

      const c = await fresh(9);
      await zb.tierToObjectStorage(c.id);
      const res = await zb.deleteMany([b.id, c.id]);
      expect(res.every((r) => r.deleted)).toBe(true);
      expect(await objectExists(c.id)).toBe(false);
    });

    it("delete on a caller-supplied connection leaves the object (it cannot know whether you will commit)", async () => {
      const { id } = await fresh(10);
      await zb.tierToObjectStorage(id);
      const conn = await mysql.createConnection({ uri: URL! });
      await conn.beginTransaction();
      expect(await zb.delete(id, { connection: conn })).toBe(true);
      await conn.rollback();
      expect(await zb.exists(id)).toBe(true); // rolled back: the row and its object are both intact
      expect(await objectExists(id)).toBe(true);
      await conn.end();
    });

    it("the row lock is per image: a slow upload blocks writes to THAT image only", async () => {
      const a = await fresh(11);
      const b = await fresh(12);
      let started!: () => void;
      const uploadStarted = new Promise<void>((r) => (started = r));
      let open!: () => void;
      const gate = new Promise<void>((r) => (open = r));
      const slow = new ZeroBucket({
        mysql: URL,
        objectStorage: spy({
          upload: async (k, d, o) => {
            started();
            await gate;
            return real.upload(k, d, o);
          },
        }),
      });
      const tiering = slow.tierToObjectStorage(a.id);
      await uploadStarted; // the row for A is now locked inside an open transaction
      const state = { deleteA: false };

      expect(await zb.delete(b.id)).toBe(true); // another row: never blocked
      const deleteA = zb.delete(a.id).then((v) => ((state.deleteA = true), v));
      await sleep(600);
      expect(state.deleteA).toBe(false); // this image's writers wait for the upload
      expect(await zb.exists(a.id)).toBe(true); // plain reads are never blocked

      open();
      expect(await tiering).toBe(true);
      expect(await deleteA).toBe(true); // runs after the tier, sees it, and removes the object too
      expect(await objectExists(a.id)).toBe(false);
      await slow.close();
    }, 30_000);
  },
);
