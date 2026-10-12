/** MySQL/MariaDB dedup-mode tests (real server). Skipped unless ZEROBUCKET_TEST_MYSQL_URL is set. */
import { createHash } from "node:crypto";
import mysql, { type Connection, type Pool } from "mysql2/promise";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  ImageNotFoundError,
  MySQLBackend,
  ObjectStorage,
  StorageError,
  ZeroBucket,
  type MysqlQueryable,
  type OperationEvent,
} from "../src/index.js";
import { createImageHandler } from "../src/http.js";
import { makeImage } from "./helpers.js";

const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const collect = async (it: AsyncIterable<Buffer>) => {
  const a: Buffer[] = [];
  for await (const c of it) a.push(c);
  return Buffer.concat(a);
};
const MISSING = (n: number) =>
  `00000000-0000-4000-8000-0000000000${String(n).padStart(2, "0")}`;

const URL = process.env.ZEROBUCKET_TEST_MYSQL_URL ?? ""; // empty => the whole file is skipped

let raw: Pool;
let zb: ZeroBucket;

const rows = async (sql: string, params: unknown[] = []) =>
  (await raw.query(sql, params))[0] as any[];
const blob = async (data: Buffer) =>
  (
    await rows(
      "SELECT ref_count FROM zerobucket_blobs WHERE checksum_sha256 = ?",
      [sha(data)],
    )
  )[0] as { ref_count: number } | undefined;
const refCount = async (data: Buffer) =>
  Number(
    (
      await rows(
        "SELECT COUNT(*) AS n FROM zerobucket_image_refs WHERE checksum_sha256 = ?",
        [sha(data)],
      )
    )[0].n,
  );
const blobCount = async () =>
  Number((await rows("SELECT COUNT(*) AS n FROM zerobucket_blobs"))[0].n);
/** The invariant that matters: every blob's ref_count equals its real number of refs. Returns violations. */
const violations = () =>
  rows(`SELECT b.checksum_sha256, b.ref_count, COUNT(r.id) AS refs
          FROM zerobucket_blobs b LEFT JOIN zerobucket_image_refs r ON r.checksum_sha256 = b.checksum_sha256
          GROUP BY b.checksum_sha256, b.ref_count HAVING b.ref_count <> COUNT(r.id)`);
const clear = async () => {
  await raw.query("DELETE FROM zerobucket_image_refs");
  await raw.query("DELETE FROM zerobucket_blobs");
};
const dedupBucket = (o: Record<string, unknown> = {}) =>
  new ZeroBucket({ mysql: URL, dedup: true, ...o });

describe.skipIf(!URL)("MySQL/MariaDB dedup mode (real server)", () => {
  beforeAll(async () => {
    raw = mysql.createPool({ uri: URL, connectionLimit: 6 });
    await raw.query("DROP TABLE IF EXISTS zerobucket_image_refs");
    await raw.query("DROP TABLE IF EXISTS zerobucket_blobs");
    zb = dedupBucket();
    await zb.ready();
  });
  beforeEach(clear);
  afterAll(async () => {
    await zb.close();
    await raw.end();
  });

  describe("schema (identical to the Python package's dedup tables)", () => {
    it("creates the same columns, types, indexes and foreign key", async () => {
      const norm = (t: string) => String(t).replace(/^int\(\d+\)$/, "int"); // MariaDB `int(11)` vs MySQL 8 `int`
      const cols = async (table: string) =>
        (
          await rows(
            "SELECT COLUMN_NAME AS n, COLUMN_TYPE AS t, IS_NULLABLE AS nullable FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            [table],
          )
        ).map((c) => `${c.n}:${norm(c.t)}:${c.nullable}`);
      expect(await cols("zerobucket_blobs")).toEqual([
        "checksum_sha256:char(64):NO",
        "data:longblob:NO",
        "mime_type:varchar(255):NO",
        "size_bytes:int:NO",
        "width:int:YES",
        "height:int:YES",
        "ref_count:int:NO",
        "created_at:datetime(6):NO",
      ]);
      expect(await cols("zerobucket_image_refs")).toEqual([
        "id:char(36):NO",
        "checksum_sha256:char(64):NO",
        "original_filename:text:YES",
        "created_at:datetime(6):NO",
        "updated_at:datetime(6):NO",
      ]);
      const idx = await rows(
        "SELECT DISTINCT INDEX_NAME AS n FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_image_refs'",
      );
      expect(idx.map((i) => i.n).sort()).toEqual([
        "PRIMARY",
        "idx_zerobucket_image_refs_checksum",
        "idx_zerobucket_image_refs_created_at",
        // InnoDB adds the FK's own index only when no suitable one exists; the checksum index covers it.
      ]);
      const fk = await rows(
        "SELECT REFERENCED_TABLE_NAME AS t FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'zerobucket_image_refs' AND REFERENCED_TABLE_NAME IS NOT NULL",
      );
      expect(fk.map((f) => f.t)).toEqual(["zerobucket_blobs"]);
    });

    it("many processes starting at once create the tables exactly once (named lock), in any order", async () => {
      await raw.query("DROP TABLE IF EXISTS zerobucket_image_refs");
      await raw.query("DROP TABLE IF EXISTS zerobucket_blobs");
      const backends = Array.from(
        { length: 8 },
        () =>
          new MySQLBackend({
            connectionString: URL,
            dedup: true,
            poolMaxSize: 1,
          }),
      );
      await Promise.all(backends.map((b) => b.ready()));
      await Promise.all(backends.map((b) => b.close()));
      expect(await blobCount()).toBe(0);
    });

    it("startup issues no DDL when the tables exist, so it never queues behind an open transaction", async () => {
      const conn = await mysql.createConnection({ uri: URL });
      const data = await makeImage("png", 10, 10, { seed: 99 });
      await conn.beginTransaction();
      await conn.query(
        "INSERT INTO zerobucket_blobs (checksum_sha256, data, mime_type, size_bytes, ref_count, created_at) VALUES (?, 'x', 'image/png', 1, 1, NOW(6))",
        [sha(data)],
      ); // this transaction holds a metadata lock on the blobs table
      const late = new MySQLBackend({
        connectionString: URL,
        dedup: true,
        poolTimeoutMs: 3000,
      });
      const t0 = Date.now();
      await late.ready();
      expect(Date.now() - t0).toBeLessThan(2000);
      await conn.rollback();
      await conn.end();
      await late.close();
    });
  });

  describe("core behaviour", () => {
    it("identical bytes are stored once; each put gets its own id and filename", async () => {
      const data = await makeImage("png", 80, 80, { seed: 1 });
      const a = await zb.put(data, { filename: "a.png" });
      const b = await zb.put(data, { filename: "b.png" });
      expect(a).not.toBe(b);
      expect(await blobCount()).toBe(1);
      expect((await blob(data))!.ref_count).toBe(2);
      const [ia, ib] = [await zb.get(a), await zb.get(b)];
      expect(ia.data.equals(data) && ib.data.equals(data)).toBe(true);
      expect([ia.filename, ib.filename]).toEqual(["a.png", "b.png"]);
      expect(ia.checksumSha256).toBe(sha(data));
      expect((await zb.metadata(b)).filename).toBe("b.png");
      const other = await zb.put(await makeImage("png", 80, 80, { seed: 2 }));
      expect(await zb.exists(other)).toBe(true);
      expect(await blobCount()).toBe(2);
      expect(await violations()).toEqual([]);
    });

    it("streams, serves ranges and HTTP from a shared blob", async () => {
      const data = await makeImage("png", 200, 200, { seed: 3 });
      const a = await zb.put(data);
      const b = await zb.put(data);
      expect(
        (await collect(await zb.getStream(a, { chunkSize: 30_000 }))).equals(
          data,
        ),
      ).toBe(true);
      expect(
        (
          await collect(
            await zb.getStream(b, { range: { start: 100, end: 199 } }),
          )
        ).equals(data.subarray(100, 200)),
      ).toBe(true);
      const res = await createImageHandler(zb)(
        new Request(`http://x/i/${b}`, { headers: { Range: "bytes=5-14" } }),
      );
      expect(res.status).toBe(206);
      expect(
        Buffer.from(await res.arrayBuffer()).equals(data.subarray(5, 15)),
      ).toBe(true);
      await expect(zb.getStream(MISSING(1))).rejects.toBeInstanceOf(
        ImageNotFoundError,
      );
    });

    it("a stream survives the deletion of a DIFFERENT reference to the same blob, and fails loudly if its own goes", async () => {
      const data = await makeImage("png", 300, 200, { seed: 4 });
      const [a, b] = [await zb.put(data), await zb.put(data)];
      const s1 = (await zb.getStream(a, { chunkSize: 500 }))[
        Symbol.asyncIterator
      ]();
      expect((await s1.next()).done).toBe(false);
      await zb.delete(b); // the blob stays: `a` still references it
      let n = 0;
      for (let r = await s1.next(); !r.done; r = await s1.next()) n += 1;
      expect(n).toBeGreaterThan(0);

      const s2 = (await zb.getStream(a, { chunkSize: 500 }))[
        Symbol.asyncIterator
      ]();
      expect((await s2.next()).done).toBe(false);
      await raw.query("DELETE FROM zerobucket_image_refs WHERE id = ?", [a]);
      await expect(s2.next()).rejects.toThrow(/deleted while streaming/);
    });

    it("getMany works across shared and distinct blobs, in input order", async () => {
      const x = await makeImage("png", 30, 30, { seed: 5 });
      const y = await makeImage("png", 31, 31, { seed: 6 });
      const [x1, y1, x2] = [await zb.put(x), await zb.put(y), await zb.put(x)];
      const res = await zb.getMany([x2, y1, MISSING(2), x1]);
      expect(res.map((r) => r.success)).toEqual([true, true, false, true]);
      expect(res[0]!.image!.data.equals(x)).toBe(true);
      expect(res[1]!.image!.data.equals(y)).toBe(true);
    });

    it("classic and dedup modes use separate tables and never see each other's ids", async () => {
      const classic = new ZeroBucket({ mysql: URL });
      const cid = await classic.put(
        await makeImage("png", 20, 20, { seed: 7 }),
      );
      const did = await zb.put(await makeImage("png", 21, 21, { seed: 8 }));
      await expect(zb.get(cid)).rejects.toBeInstanceOf(ImageNotFoundError);
      await expect(classic.get(did)).rejects.toBeInstanceOf(ImageNotFoundError);
      await classic.delete(cid);
      await classic.close();
    });

    it("dedup cannot be combined with objectStorage, and tiering is refused", async () => {
      expect(
        () =>
          new ZeroBucket({
            mysql: URL,
            dedup: true,
            objectStorage: new ObjectStorage({ bucket: "b" }),
          }),
      ).toThrow(TypeError);
      const id = await zb.put(await makeImage("png", 20, 20, { seed: 9 }));
      await expect(zb.tierToObjectStorage(id)).rejects.toThrow(/dedup/);
    });

    it("optimize and validators work in dedup mode (the checksum is of the stored bytes)", async () => {
      const id = await zb.put(await makeImage("png", 400, 100, { seed: 10 }), {
        optimize: { maxWidth: 100, format: "webp" },
      });
      const img = await zb.get(id);
      expect(img.width).toBe(100);
      expect((await blob(img.data))!.ref_count).toBe(1);
    });

    it("malformed and unknown ids are 'not found', never a database error", async () => {
      for (const bad of [
        MISSING(3),
        "not-a-uuid",
        "1; DROP TABLE zerobucket_blobs",
      ]) {
        await expect(zb.get(bad)).rejects.toBeInstanceOf(ImageNotFoundError);
        expect(await zb.exists(bad)).toBe(false);
        expect(await zb.delete(bad)).toBe(false);
      }
    });
  });

  describe("putMany", () => {
    it("identical images within ONE batch accumulate on one blob; ids map to inputs", async () => {
      const a = await makeImage("png", 40, 40, { seed: 11 });
      const b = await makeImage("png", 41, 41, { seed: 12 });
      const res = await zb.putMany([a, b, a, a], {
        filenames: ["1", "2", "3", "4"],
      });
      expect(res.every((r) => r.success)).toBe(true);
      expect(new Set(res.map((r) => r.imageId)).size).toBe(4);
      expect((await blob(a))!.ref_count).toBe(3);
      expect((await blob(b))!.ref_count).toBe(1);
      expect((await zb.get(res[2]!.imageId!)).filename).toBe("3");
      expect((await zb.get(res[1]!.imageId!)).data.equals(b)).toBe(true);
      expect(await violations()).toEqual([]);
    });

    it("is best-effort: a bad item does not stop the rest", async () => {
      const a = await makeImage("png", 40, 40, { seed: 13 });
      const res = await zb.putMany([a, Buffer.from("junk"), a]);
      expect(res.map((r) => r.success)).toEqual([true, false, true]);
      expect((await blob(a))!.ref_count).toBe(2);
    });

    it("adds to an existing blob", async () => {
      const a = await makeImage("png", 40, 40, { seed: 14 });
      await zb.put(a);
      await zb.putMany([a, a]);
      expect((await blob(a))!.ref_count).toBe(3);
      expect(await violations()).toEqual([]);
    });
  });

  describe("delete and reference counting", () => {
    it("deleting one reference keeps the shared blob; deleting the last removes it", async () => {
      const data = await makeImage("png", 50, 50, { seed: 15 });
      const [a, b] = [await zb.put(data), await zb.put(data)];
      expect(await zb.delete(a)).toBe(true);
      expect((await blob(data))!.ref_count).toBe(1);
      expect((await zb.get(b)).data.equals(data)).toBe(true);
      await expect(zb.get(a)).rejects.toBeInstanceOf(ImageNotFoundError);
      expect(await zb.delete(b)).toBe(true);
      expect(await blob(data)).toBeUndefined(); // the bytes are gone
      expect(await zb.delete(b)).toBe(false); // already gone: no phantom decrement
      expect(await violations()).toEqual([]);
    });

    it("deleteMany decrements each blob by its exact count", async () => {
      const shared = await makeImage("png", 50, 50, { seed: 16 });
      const solo = await makeImage("png", 51, 51, { seed: 17 });
      const ids = [
        await zb.put(shared),
        await zb.put(shared),
        await zb.put(shared),
        await zb.put(shared),
      ];
      const soloId = await zb.put(solo);
      const res = await zb.deleteMany([
        ids[0]!,
        ids[1]!,
        ids[2]!,
        MISSING(4),
        soloId,
      ]);
      expect(res.map((r) => r.deleted)).toEqual([
        true,
        true,
        true,
        false,
        true,
      ]);
      expect((await blob(shared))!.ref_count).toBe(1); // 4 - 3
      expect(await blob(solo)).toBeUndefined();
      expect(await violations()).toEqual([]);
      await zb.deleteMany([ids[3]!]);
      expect(await blob(shared)).toBeUndefined();
    });

    it("a duplicated id inside deleteMany cannot double-decrement", async () => {
      const data = await makeImage("png", 52, 52, { seed: 18 });
      const [a, b] = [await zb.put(data), await zb.put(data)];
      await zb.deleteMany([a, a, a]);
      expect((await blob(data))!.ref_count).toBe(1);
      expect((await zb.get(b)).data.equals(data)).toBe(true);
    });
  });

  describe("concurrency (the point of the refcount design)", () => {
    it("20 concurrent puts of the same bytes give ref_count == 20 exactly", async () => {
      const data = await makeImage("png", 60, 60, { seed: 20 });
      const ids = await Promise.all(
        Array.from({ length: 20 }, () => zb.put(data)),
      );
      expect(new Set(ids).size).toBe(20);
      expect((await blob(data))!.ref_count).toBe(20);
      expect(await refCount(data)).toBe(20);
    });

    it("20 concurrent deletes drain the blob to zero and remove it", async () => {
      const data = await makeImage("png", 60, 60, { seed: 21 });
      const ids = await Promise.all(
        Array.from({ length: 20 }, () => zb.put(data)),
      );
      await Promise.all(ids.map((id) => zb.delete(id)));
      expect(await blob(data)).toBeUndefined();
      expect(await violations()).toEqual([]);
    });

    it("racing put against delete-of-the-last-reference never loses or leaks data", async () => {
      const evs: OperationEvent[] = [];
      const racer = dedupBucket({
        onOperation: (e: OperationEvent) => evs.push(e),
      });
      for (let i = 0; i < 25; i++) {
        const data = await makeImage("png", 30, 30, { seed: 100 + i });
        const first = await racer.put(data);
        const [, p1, p2] = await Promise.all([
          racer.delete(first),
          racer.put(data),
          racer.put(data),
        ]);
        expect(await violations()).toEqual([]);
        expect((await blob(data))!.ref_count).toBe(2);
        expect((await racer.get(p1)).data.equals(data)).toBe(true); // the blob survived the race
        expect((await racer.get(p2)).data.equals(data)).toBe(true);
      }
      await racer.close();
      expect(evs.filter((e) => !e.success)).toEqual([]); // nothing failed, even after retries
    });

    it("overlapping batches in opposite orders never deadlock (retry disabled, so a deadlock would surface)", async () => {
      const imgs = await Promise.all(
        [0, 1, 2, 3, 4, 5].map((n) =>
          makeImage("png", 20 + n, 20, { seed: 200 + n }),
        ),
      );
      const forward = dedupBucket({ maxRetries: 0 });
      const backward = dedupBucket({ maxRetries: 0 });
      for (let round = 0; round < 12; round++) {
        await clear();
        const results = await Promise.all([
          forward.putMany(imgs),
          backward.putMany([...imgs].reverse()),
          forward.putMany(imgs),
          backward.putMany([...imgs].reverse()),
        ]);
        expect(
          results.flat().every((r) => r.success),
          JSON.stringify(
            results
              .flat()
              .filter((r) => !r.success)
              .slice(0, 1),
          ),
        ).toBe(true);
        expect(await violations()).toEqual([]);
        // and the same for deleteMany over the same blobs, opposite orders
        const ids = results.flat().map((r) => r.imageId!);
        const half = Math.floor(ids.length / 2);
        const del = await Promise.all([
          forward.deleteMany(ids.slice(0, half)),
          backward.deleteMany(ids.slice(half).reverse()),
        ]);
        expect(del.flat().every((r) => r.success)).toBe(true);
        expect(await violations()).toEqual([]);
      }
      await forward.close();
      await backward.close();
    });
  });

  describe("atomicity on a caller-supplied connection", () => {
    /** Delegates to a real connection but fails the ref insert, the 2nd statement of a dedup put. */
    const failingRefInsert = (c: Connection): MysqlQueryable => ({
      query: (sql, v) => c.query(sql, v) as any,
      execute: (sql, v) =>
        /INSERT INTO zerobucket_image_refs/.test(sql)
          ? Promise.reject(new Error("injected failure on ref insert"))
          : (c.execute(sql, v) as any),
    });
    let conn: Connection;
    beforeAll(async () => {
      conn = await mysql.createConnection({ uri: URL });
    });
    afterAll(async () => {
      await conn.end();
    });

    it("autocommit connection: a failure after the blob upsert leaves NO inflated ref_count", async () => {
      const data = await makeImage("png", 25, 25, { seed: 30 });
      await expect(
        zb.put(data, { connection: failingRefInsert(conn) }),
      ).rejects.toThrow(/injected/);
      expect(await blob(data)).toBeUndefined(); // the upsert was rolled back with it
      expect(await violations()).toEqual([]);
    });

    it("inside the caller's transaction: a failure rolls back only our work; the caller's tx stays usable", async () => {
      const keep = await makeImage("png", 26, 26, { seed: 31 });
      const fail = await makeImage("png", 27, 27, { seed: 32 });
      await conn.beginTransaction();
      const keptId = await zb.put(keep, { connection: conn });
      await expect(
        zb.put(fail, { connection: failingRefInsert(conn) }),
      ).rejects.toThrow(/injected/);
      await conn.commit(); // the transaction is still alive and commits
      expect((await zb.get(keptId)).data.equals(keep)).toBe(true);
      expect(await blob(fail)).toBeUndefined();
      expect(await violations()).toEqual([]);
    });

    it("a rolled-back caller transaction leaves nothing behind (put, putMany)", async () => {
      const data = await makeImage("png", 28, 28, { seed: 33 });
      await conn.beginTransaction();
      await zb.put(data, { connection: conn });
      await zb.putMany([data, data], { connection: conn });
      expect(await blob(data)).toBeUndefined(); // not visible to others yet
      await conn.rollback();
      expect(await blob(data)).toBeUndefined();
      expect(await refCount(data)).toBe(0);
    });

    it("delete/deleteMany on a caller connection keep counts exact too", async () => {
      const data = await makeImage("png", 29, 29, { seed: 34 });
      const ids = [await zb.put(data), await zb.put(data), await zb.put(data)];
      await conn.beginTransaction();
      expect(await zb.delete(ids[0]!, { connection: conn })).toBe(true);
      await conn.rollback(); // undone entirely
      expect((await blob(data))!.ref_count).toBe(3);
      expect(await violations()).toEqual([]);
      const res = await zb.deleteMany([ids[0]!, ids[1]!], {
        connection: conn,
      }); // autocommit
      expect(res.every((r) => r.deleted)).toBe(true);
      expect((await blob(data))!.ref_count).toBe(1);
      expect(await violations()).toEqual([]);
    });
  });

  describe("errors", () => {
    it("a StorageError (not a raw driver error) reaches the caller when the tables are missing and autoMigrate is off", async () => {
      const noMig = new ZeroBucket({
        mysql: URL,
        dedup: true,
        autoMigrate: false,
        maxRetries: 0,
      });
      await raw.query("DROP TABLE zerobucket_image_refs");
      await raw.query("DROP TABLE zerobucket_blobs");
      await expect(
        noMig.put(await makeImage("png", 10, 10)),
      ).rejects.toBeInstanceOf(StorageError);
      await noMig.close();
      // restore for any test that follows
      const back = dedupBucket();
      await back.ready();
      await back.close();
    });
  });
});
