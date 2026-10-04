import { createHash } from "node:crypto";
import pg from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  ImageNotFoundError,
  StorageError,
  ZeroBucket,
  ObjectStorage,
} from "../src/index.js";
import { createImageHandler } from "../src/http.js";
import { DATABASE_URL, makeImage, newBucket } from "./helpers.js";

const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const collect = async (it: AsyncIterable<Buffer>) => {
  const a: Buffer[] = [];
  for await (const c of it) a.push(c);
  return Buffer.concat(a);
};

describe.skipIf(!DATABASE_URL)("dedup mode against real Postgres", () => {
  let zb: ZeroBucket;
  let raw: pg.Pool;

  const blob = async (data: Buffer) =>
    (
      await raw.query(
        "SELECT ref_count FROM zerobucket_blobs WHERE checksum_sha256=$1",
        [sha(data)],
      )
    ).rows[0] as { ref_count: number } | undefined;
  const refCount = async (data: Buffer) =>
    (
      await raw.query(
        "SELECT count(*)::int n FROM zerobucket_image_refs WHERE checksum_sha256=$1",
        [sha(data)],
      )
    ).rows[0].n as number;
  /** The one invariant that matters: every blob's ref_count equals its real number of refs. Returns violations. */
  const violations = async () =>
    (
      await raw.query(`SELECT b.checksum_sha256, b.ref_count, count(r.id)::int AS refs
                      FROM zerobucket_blobs b LEFT JOIN zerobucket_image_refs r ON r.checksum_sha256 = b.checksum_sha256
                      GROUP BY b.checksum_sha256, b.ref_count HAVING b.ref_count <> count(r.id)`)
    ).rows;

  beforeAll(async () => {
    raw = new pg.Pool({ connectionString: DATABASE_URL });
    zb = newBucket({ dedup: true });
    await zb.ready();
  });
  beforeEach(async () => {
    await raw.query("DELETE FROM zerobucket_image_refs");
    await raw.query("DELETE FROM zerobucket_blobs");
  });
  afterAll(async () => {
    await zb.close();
    await raw.end();
  });

  describe("core behaviour", () => {
    it("identical bytes are stored once; each put gets its own id and filename", async () => {
      const data = await makeImage("png", 80, 80, { seed: 1 });
      const a = await zb.put(data, { filename: "a.png" });
      const b = await zb.put(data, { filename: "b.png" });
      expect(a).not.toBe(b);
      expect(
        (await raw.query("SELECT count(*)::int n FROM zerobucket_blobs"))
          .rows[0].n,
      ).toBe(1);
      expect((await blob(data))!.ref_count).toBe(2);
      const [ia, ib] = [await zb.get(a), await zb.get(b)];
      expect(ia.data.equals(data) && ib.data.equals(data)).toBe(true);
      expect([ia.filename, ib.filename]).toEqual(["a.png", "b.png"]);
      expect(ia.checksumSha256).toBe(sha(data));
      expect((await zb.metadata(b)).filename).toBe("b.png");
      const other = await zb.put(await makeImage("png", 80, 80, { seed: 2 }));
      expect(await zb.exists(other)).toBe(true);
      expect(
        (await raw.query("SELECT count(*)::int n FROM zerobucket_blobs"))
          .rows[0].n,
      ).toBe(2);
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
      await expect(
        zb.getStream("00000000-0000-4000-8000-0000000000f1"),
      ).rejects.toBeInstanceOf(ImageNotFoundError);
    });

    it("getMany works across shared and distinct blobs, in input order", async () => {
      const x = await makeImage("png", 30, 30, { seed: 4 });
      const y = await makeImage("png", 31, 31, { seed: 5 });
      const [x1, y1, x2] = [await zb.put(x), await zb.put(y), await zb.put(x)];
      const res = await zb.getMany([
        x2,
        y1,
        "00000000-0000-4000-8000-0000000000f2",
        x1,
      ]);
      expect(res.map((r) => r.success)).toEqual([true, true, false, true]);
      expect(res[0]!.image!.data.equals(x)).toBe(true);
      expect(res[1]!.image!.data.equals(y)).toBe(true);
    });

    it("classic and dedup modes use separate tables and never see each other's ids", async () => {
      const classic = newBucket();
      const cid = await classic.put(
        await makeImage("png", 20, 20, { seed: 6 }),
      );
      const did = await zb.put(await makeImage("png", 21, 21, { seed: 7 }));
      await expect(zb.get(cid)).rejects.toBeInstanceOf(ImageNotFoundError);
      await expect(classic.get(did)).rejects.toBeInstanceOf(ImageNotFoundError);
      await classic.delete(cid);
      await classic.close();
    });

    it("dedup cannot be combined with objectStorage, and tiering is refused", async () => {
      expect(() =>
        newBucket({
          dedup: true,
          objectStorage: new ObjectStorage({ bucket: "b" }),
        }),
      ).toThrow(TypeError);
      const id = await zb.put(await makeImage("png", 20, 20, { seed: 8 }));
      await expect(zb.tierToObjectStorage(id)).rejects.toThrow(/dedup/);
    });

    it("optimize and validators work in dedup mode (checksum is of the stored bytes)", async () => {
      const id = await zb.put(await makeImage("png", 400, 100, { seed: 9 }), {
        optimize: { maxWidth: 100, format: "webp" },
      });
      const img = await zb.get(id);
      expect(img.width).toBe(100);
      expect((await blob(img.data))!.ref_count).toBe(1);
    });
  });

  describe("putMany", () => {
    it("identical images within ONE batch accumulate on one blob; ids map to inputs", async () => {
      const a = await makeImage("png", 40, 40, { seed: 10 });
      const b = await makeImage("png", 41, 41, { seed: 11 });
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
      const a = await makeImage("png", 40, 40, { seed: 12 });
      const res = await zb.putMany([a, Buffer.from("junk"), a]);
      expect(res.map((r) => r.success)).toEqual([true, false, true]);
      expect((await blob(a))!.ref_count).toBe(2);
    });
    it("adds to an existing blob", async () => {
      const a = await makeImage("png", 40, 40, { seed: 13 });
      await zb.put(a);
      await zb.putMany([a, a]);
      expect((await blob(a))!.ref_count).toBe(3);
      expect(await violations()).toEqual([]);
    });
  });

  describe("delete and reference counting", () => {
    it("deleting one reference keeps the shared blob; deleting the last removes it", async () => {
      const data = await makeImage("png", 50, 50, { seed: 14 });
      const [a, b] = [await zb.put(data), await zb.put(data)];
      expect(await zb.delete(a)).toBe(true);
      expect((await blob(data))!.ref_count).toBe(1);
      expect((await zb.get(b)).data.equals(data)).toBe(true);
      await expect(zb.get(a)).rejects.toBeInstanceOf(ImageNotFoundError);
      expect(await zb.delete(b)).toBe(true);
      expect(await blob(data)).toBeUndefined(); // bytes are gone
      expect(await zb.delete(b)).toBe(false); // already gone: no phantom decrement
    });

    it("deleteMany decrements each blob by its exact count", async () => {
      const shared = await makeImage("png", 50, 50, { seed: 15 });
      const solo = await makeImage("png", 51, 51, { seed: 16 });
      const ids = [
        await zb.put(shared),
        await zb.put(shared),
        await zb.put(shared),
        await zb.put(shared),
      ];
      const soloId = await zb.put(solo);
      const missing = "00000000-0000-4000-8000-0000000000f3";
      const res = await zb.deleteMany([
        ids[0]!,
        ids[1]!,
        ids[2]!,
        missing,
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
      const data = await makeImage("png", 52, 52, { seed: 17 });
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
      const raw0 = newBucket({ dedup: true, maxRetries: 0 }); // no retry: surface any raw failure
      for (let i = 0; i < 25; i++) {
        const data = await makeImage("png", 30, 30, { seed: 100 + i });
        const first = await raw0.put(data);
        const [, p1, p2] = await Promise.all([
          raw0.delete(first),
          raw0.put(data),
          raw0.put(data),
        ]);
        expect(await violations()).toEqual([]);
        expect((await blob(data))!.ref_count).toBe(2);
        expect((await raw0.get(p1)).data.equals(data)).toBe(true); // the blob survived the race
        expect((await raw0.get(p2)).data.equals(data)).toBe(true);
      }
      await raw0.close();
    });

    it("overlapping batches in opposite orders never deadlock (retry disabled, so a deadlock would surface)", async () => {
      const imgs = await Promise.all(
        [0, 1, 2, 3, 4, 5].map((n) =>
          makeImage("png", 20 + n, 20, { seed: 200 + n }),
        ),
      );
      const forward = newBucket({ dedup: true, maxRetries: 0 });
      const backward = newBucket({ dedup: true, maxRetries: 0 });
      for (let round = 0; round < 12; round++) {
        await raw.query("DELETE FROM zerobucket_image_refs");
        await raw.query("DELETE FROM zerobucket_blobs");
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
    /** Delegates to a real client but fails the 2nd statement of a dedup put. */
    const failingRefInsert = (client: pg.PoolClient) => ({
      query: (text: string, values?: unknown[]) => {
        if (/INSERT INTO zerobucket_image_refs/.test(text))
          return Promise.reject(new Error("injected failure on ref insert"));
        return client.query(text, values as any) as any;
      },
    });

    it("autocommit connection: a failure after the blob upsert leaves NO inflated ref_count", async () => {
      const data = await makeImage("png", 25, 25, { seed: 30 });
      const client = await raw.connect(); // no BEGIN: statements autocommit
      try {
        await expect(
          zb.put(data, { connection: failingRefInsert(client) }),
        ).rejects.toThrow(/injected/);
      } finally {
        client.release();
      }
      expect(await blob(data)).toBeUndefined(); // the upsert was rolled back with it
      expect(await violations()).toEqual([]);
    });

    it("inside the caller's transaction: a failure rolls back only our work; the caller's tx stays usable", async () => {
      const keep = await makeImage("png", 26, 26, { seed: 31 });
      const fail = await makeImage("png", 27, 27, { seed: 32 });
      const client = await raw.connect();
      try {
        await client.query("BEGIN");
        const keptId = await zb.put(keep, { connection: client });
        await expect(
          zb.put(fail, { connection: failingRefInsert(client) }),
        ).rejects.toThrow(/injected/);
        await client.query("COMMIT"); // would throw "transaction is aborted" without the savepoint
        expect((await zb.get(keptId)).data.equals(keep)).toBe(true);
      } finally {
        client.release();
      }
      expect(await blob(fail)).toBeUndefined();
      expect(await violations()).toEqual([]);
    });

    it("a rolled-back caller transaction leaves nothing behind (put, putMany)", async () => {
      const data = await makeImage("png", 28, 28, { seed: 33 });
      const client = await raw.connect();
      try {
        await client.query("BEGIN");
        await zb.put(data, { connection: client });
        await zb.putMany([data, data], { connection: client });
        expect((await blob(data)) === undefined).toBe(true); // not visible to others yet
        await client.query("ROLLBACK");
      } finally {
        client.release();
      }
      expect(await blob(data)).toBeUndefined();
      expect(await refCount(data)).toBe(0);
    });

    it("classic putMany is atomic on an autocommit connection too", async () => {
      const classic = newBucket();
      const good = await makeImage("png", 29, 29, { seed: 34 });
      const marker = `atomic-${Date.now()}`;
      const client = await raw.connect();
      let n = 0;
      const failOnSecond = {
        query: (text: string, values?: unknown[]) =>
          /INSERT INTO zerobucket_images/.test(text) && ++n === 2
            ? Promise.reject(new Error("injected"))
            : (client.query(text, values as any) as any),
      };
      try {
        const res = await classic.putMany([good, good, good], {
          filenames: [marker, marker, marker],
          connection: failOnSecond,
        });
        expect(res.every((r) => !r.success)).toBe(true);
      } finally {
        client.release();
      }
      expect(
        (
          await raw.query(
            "SELECT count(*)::int n FROM zerobucket_images WHERE original_filename=$1",
            [marker],
          )
        ).rows[0].n,
      ).toBe(0);
      await classic.close();
    });
  });

  describe("migrateClassicToDedup", () => {
    const classicCount = async () =>
      (await raw.query("SELECT count(*)::int n FROM zerobucket_images")).rows[0]
        .n as number;
    let classic: ZeroBucket;
    beforeEach(async () => {
      await raw.query("DELETE FROM zerobucket_images");
      classic = newBucket();
    });
    afterAll(async () => {
      await raw.query("DELETE FROM zerobucket_images");
    });

    it("copies everything, preserves ids, dedups identical content, never touches the classic table, and is re-runnable", async () => {
      const dup = await makeImage("png", 33, 33, { seed: 40 });
      const imgs = await Promise.all(
        [0, 1, 2].map((n) => makeImage("png", 34 + n, 34, { seed: 41 + n })),
      );
      const ids = [
        await classic.put(dup, { filename: "d1.png" }),
        await classic.put(dup, { filename: "d2.png" }),
        ...(await Promise.all(
          imgs.map((d, i) => classic.put(d, { filename: `u${i}.png` })),
        )),
      ];
      const stats = await zb.migrateClassicToDedup();
      expect(stats).toEqual({
        imagesMigrated: 5,
        distinctBlobsCreated: 4,
        duplicateReferencesFound: 1,
        alreadyMigrated: 0,
      });
      expect(await classicCount()).toBe(5); // untouched
      // exact timestamps survive (microseconds, not truncated to JS milliseconds)
      expect(
        (
          await raw.query(
            "SELECT count(*)::int n FROM zerobucket_images c JOIN zerobucket_image_refs r USING (id) WHERE c.created_at <> r.created_at",
          )
        ).rows[0].n,
      ).toBe(0);
      expect((await zb.get(ids[0]!)).data.equals(dup)).toBe(true); // same id works in dedup mode
      expect((await zb.get(ids[1]!)).filename).toBe("d2.png");
      expect((await blob(dup))!.ref_count).toBe(2);
      expect(await violations()).toEqual([]);

      const again = await zb.migrateClassicToDedup(); // safe to re-run
      expect(again).toEqual({
        imagesMigrated: 0,
        distinctBlobsCreated: 0,
        duplicateReferencesFound: 0,
        alreadyMigrated: 5,
      });
      expect((await blob(dup))!.ref_count).toBe(2); // not double-counted
    });

    it("handles more rows than one batch, including rows sharing one created_at (keyset paging ties)", async () => {
      const imgs = await Promise.all(
        Array.from({ length: 47 }, (_, n) =>
          makeImage("png", 8, 8 + (n % 5), { seed: 500 + n }),
        ),
      );
      const res = await classic.putMany(imgs); // one transaction => identical created_at for every row
      expect(res.every((r) => r.success)).toBe(true);
      const stats = await zb.migrateClassicToDedup();
      expect(stats.imagesMigrated).toBe(47);
      expect(
        (await raw.query("SELECT count(*)::int n FROM zerobucket_image_refs"))
          .rows[0].n,
      ).toBe(47);
      expect(await violations()).toEqual([]);
    });

    it("refuses classic rows that are tiered to object storage, with a clear message, changing nothing", async () => {
      await classic.put(await makeImage("png", 35, 35, { seed: 60 }));
      await raw.query(
        `INSERT INTO zerobucket_images (data, mime_type, size_bytes, checksum_sha256, storage_backend, object_storage_bucket, object_storage_key)
         VALUES (NULL, 'image/png', 10, $1, 'object_storage', 'b', 'k')`,
        ["c".repeat(64)],
      );
      await expect(zb.migrateClassicToDedup()).rejects.toThrow(
        /tiered to object storage/,
      );
      expect(
        (await raw.query("SELECT count(*)::int n FROM zerobucket_image_refs"))
          .rows[0].n,
      ).toBe(0);
    });

    it("errors clearly on a non-dedup instance or when there is no classic table", async () => {
      await expect(classic.migrateClassicToDedup()).rejects.toBeInstanceOf(
        StorageError,
      );
      await raw.query(
        "ALTER TABLE zerobucket_images RENAME TO zerobucket_images_tmp",
      );
      try {
        await expect(zb.migrateClassicToDedup()).rejects.toThrow(
          /No classic zerobucket_images table/,
        );
      } finally {
        await raw.query(
          "ALTER TABLE zerobucket_images_tmp RENAME TO zerobucket_images",
        );
      }
    });
  });
});
