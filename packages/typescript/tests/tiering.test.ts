import { createHash } from "node:crypto";
import {
  CreateBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  AccessDeniedError,
  ImageNotFoundError,
  ObjectStorage,
  StorageError,
  ZeroBucket,
  type ObjectStorageLike,
  type OperationEvent,
} from "../src/index.js";
import { createImageHandler } from "../src/http.js";
import { DATABASE_URL, flakyPool, makeImage, newBucket } from "./helpers.js";

const ENDPOINT = process.env.ZEROBUCKET_TEST_S3_ENDPOINT;
const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const collect = async (it: AsyncIterable<Buffer>) =>
  Buffer.concat(
    await (async () => {
      const a: Buffer[] = [];
      for await (const c of it) a.push(c);
      return a;
    })(),
  );

/** Wrap a real ObjectStorage, counting calls and letting a test override individual methods. */
function spyStorage(
  real: ObjectStorage,
  over: Partial<ObjectStorageLike> = {},
) {
  const calls = { upload: 0, download: 0, range: 0, delete: 0 };
  const wrapped: ObjectStorageLike = {
    bucket: real.bucket,
    upload: (k, d, o) => (
      calls.upload++,
      (over.upload ?? real.upload.bind(real))(k, d, o)
    ),
    download: (k) => (
      calls.download++,
      (over.download ?? real.download.bind(real))(k)
    ),
    downloadRange: (k, a, b) => (
      calls.range++,
      (over.downloadRange ?? real.downloadRange.bind(real))(k, a, b)
    ),
    delete: (k) => (calls.delete++, (over.delete ?? real.delete.bind(real))(k)),
    exists: (k) => (over.exists ?? real.exists.bind(real))(k),
  };
  return { storage: wrapped, calls };
}

describe.skipIf(!DATABASE_URL || !ENDPOINT)(
  "object-storage tiering (real Postgres + real S3 API)",
  () => {
    const bucket = `zb-test-${Date.now()}`;
    const ranges: (string | undefined)[] = [];
    let s3: S3Client;
    let raw: pg.Pool;
    let real: ObjectStorage;
    let zb: ZeroBucket;
    const events: OperationEvent[] = [];

    const row = async (id: string) =>
      (await raw.query("SELECT * FROM zerobucket_images WHERE id=$1", [id]))
        .rows[0];
    const object = async (key: string) => {
      const r = await s3.send(
        new GetObjectCommand({ Bucket: bucket, Key: key }),
      );
      return {
        body: Buffer.from(await r.Body!.transformToByteArray()),
        type: r.ContentType,
      };
    };
    const objectExists = (key: string) =>
      s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key })).then(
        () => true,
        () => false,
      );
    const fresh = async (w = 200, h = 200, seed = 1) => {
      const data = await makeImage("png", w, h, { seed });
      return { data, id: await zb.put(data, { filename: "t.png" }) };
    };

    beforeAll(async () => {
      s3 = new S3Client({
        endpoint: ENDPOINT,
        region: "us-east-1",
        forcePathStyle: true,
        credentials: { accessKeyId: "test", secretAccessKey: "test" },
      });
      await s3.send(new CreateBucketCommand({ Bucket: bucket }));
      // record the Range header of every GetObject, to prove streaming really uses HTTP range requests
      const spyClient = new S3Client({
        endpoint: ENDPOINT,
        region: "us-east-1",
        forcePathStyle: true,
        credentials: { accessKeyId: "test", secretAccessKey: "test" },
      });
      spyClient.middlewareStack.add(
        (next, ctx) => async (args: any) => {
          if (ctx.commandName === "GetObjectCommand")
            ranges.push(args.input.Range);
          return next(args);
        },
        { step: "initialize", name: "rangeSpy" },
      );
      real = new ObjectStorage({ bucket, client: spyClient });
      raw = new pg.Pool({ connectionString: DATABASE_URL });
      zb = newBucket({
        objectStorage: real,
        onOperation: (e) => events.push(e),
      });
      await zb.ready();
    });
    afterAll(async () => {
      await zb.close();
      await raw.end();
    });

    describe("ObjectStorage", () => {
      it("uploads, downloads, ranges, checks existence, and deletes idempotently", async () => {
        const key = `unit-${Date.now()}`;
        const bytes = Buffer.from("0123456789abcdef");
        await real.upload(key, bytes, { mimeType: "image/png" });
        expect((await object(key)).type).toBe("image/png");
        expect((await real.download(key)).equals(bytes)).toBe(true);
        expect((await real.downloadRange(key, 4, 9)).toString()).toBe("456789");
        expect(await real.exists(key)).toBe(true);
        await real.delete(key);
        await real.delete(key); // second delete of a missing key is not an error
        expect(await real.exists(key)).toBe(false);
        await expect(real.download(key)).rejects.toBeInstanceOf(StorageError);
      });
      it("rejects an empty bucket name", () => {
        expect(() => new ObjectStorage({ bucket: "" })).toThrow(TypeError);
      });
      it("surfaces a missing bucket as StorageError, not a raw SDK error", async () => {
        const bad = new ObjectStorage({
          bucket: "does-not-exist-bucket",
          endpoint: ENDPOINT,
          credentials: { accessKeyId: "test", secretAccessKey: "test" },
        });
        await expect(
          bad.upload("k", Buffer.from("x"), { mimeType: "image/png" }),
        ).rejects.toBeInstanceOf(StorageError);
      });
    });

    describe("tiering and transparent reads", () => {
      it("moves the bytes to S3, rewrites the row, and every read path keeps working", async () => {
        const { data, id } = await fresh();
        expect(await zb.tierToObjectStorage(id)).toBe(true);

        const r = await row(id);
        expect(r.storage_backend).toBe("object_storage");
        expect(r.data).toBeNull(); // bytes are gone from Postgres
        expect(r.object_storage_bucket).toBe(bucket);
        expect(r.object_storage_key).toBe(id); // same key scheme as the Python package
        const obj = await object(id);
        expect(obj.body.equals(data)).toBe(true);
        expect(obj.type).toBe("image/png");

        const img = await zb.get(id);
        expect(img.data.equals(data)).toBe(true);
        expect(img.checksumSha256).toBe(sha(data));
        expect((await zb.metadata(id)).sizeBytes).toBe(data.length);
        expect(await zb.exists(id)).toBe(true);
      });

      it("getMany mixes tiered and classic rows in input order", async () => {
        const a = await fresh(50, 50, 11);
        const b = await fresh(60, 60, 12);
        const c = await fresh(70, 70, 13);
        await zb.tierToObjectStorage(a.id);
        await zb.tierToObjectStorage(c.id);
        const res = await zb.getMany([c.id, b.id, a.id]);
        expect(res.map((x) => x.success)).toEqual([true, true, true]);
        expect(res[0]!.image!.data.equals(c.data)).toBe(true);
        expect(res[1]!.image!.data.equals(b.data)).toBe(true);
        expect(res[2]!.image!.data.equals(a.data)).toBe(true);
      });

      it("streams a tiered image with real HTTP Range requests, and serves arbitrary ranges", async () => {
        const { data, id } = await fresh(300, 300, 21);
        await zb.tierToObjectStorage(id);
        ranges.length = 0;
        expect(
          (await collect(await zb.getStream(id, { chunkSize: 50_000 }))).equals(
            data,
          ),
        ).toBe(true);
        expect(ranges.length).toBe(Math.ceil(data.length / 50_000));
        expect(ranges.every((r) => /^bytes=\d+-\d+$/.test(r ?? ""))).toBe(true); // ranged, never whole-object

        ranges.length = 0;
        expect(
          (
            await collect(
              await zb.getStream(id, { range: { start: 1000, end: 1999 } }),
            )
          ).equals(data.subarray(1000, 2000)),
        ).toBe(true);
        expect(ranges).toEqual(["bytes=1000-1999"]); // exactly the bytes asked for, nothing more
        expect(
          (
            await collect(
              await zb.getStream(id, {
                range: { start: data.length - 5, end: 10_000_000 },
              }),
            )
          ).equals(data.subarray(data.length - 5)),
        ).toBe(true);
        expect(
          (
            await collect(
              await zb.getStream(id, { range: { start: data.length + 10 } }),
            )
          ).length,
        ).toBe(0);
      });

      it("the HTTP handler serves 200/206/304 for a tiered image", async () => {
        const { data, id } = await fresh(120, 120, 31);
        await zb.tierToObjectStorage(id);
        const h = createImageHandler(zb);
        const full = await h(new Request(`http://x/i/${id}`));
        expect(Buffer.from(await full.arrayBuffer()).equals(data)).toBe(true);
        const part = await h(
          new Request(`http://x/i/${id}`, {
            headers: { Range: "bytes=10-29" },
          }),
        );
        expect(part.status).toBe(206);
        expect(
          Buffer.from(await part.arrayBuffer()).equals(data.subarray(10, 30)),
        ).toBe(true);
        expect(
          (
            await h(
              new Request(`http://x/i/${id}`, {
                headers: { "If-None-Match": full.headers.get("etag")! },
              }),
            )
          ).status,
        ).toBe(304);
      });

      it("hooks still gate tiered reads, and a denied read never reaches S3", async () => {
        const { id } = await fresh(40, 40, 41);
        await zb.tierToObjectStorage(id);
        const { storage, calls } = spyStorage(real);
        const gated = newBucket({
          objectStorage: storage,
          beforeGet: () => false,
        });
        await expect(gated.get(id)).rejects.toBeInstanceOf(AccessDeniedError);
        await expect(gated.getStream(id)).rejects.toBeInstanceOf(
          AccessDeniedError,
        );
        expect(calls).toEqual({ upload: 0, download: 0, range: 0, delete: 0 });
        await gated.close();
      });
    });

    describe("tierToObjectStorage semantics", () => {
      it("is idempotent: false when already tiered, ImageNotFoundError when missing or malformed", async () => {
        const { id } = await fresh(30, 30, 51);
        expect(await zb.tierToObjectStorage(id)).toBe(true);
        expect(await zb.tierToObjectStorage(id)).toBe(false);
        await expect(
          zb.tierToObjectStorage("00000000-0000-4000-8000-0000000000ee"),
        ).rejects.toBeInstanceOf(ImageNotFoundError);
        await expect(
          zb.tierToObjectStorage("not-a-uuid"),
        ).rejects.toBeInstanceOf(ImageNotFoundError);
      });

      it("fails fast without objectStorage, before touching the database", async () => {
        const evs: OperationEvent[] = [];
        const bare = newBucket({ onOperation: (e) => evs.push(e) });
        const { id } = await fresh(30, 30, 52);
        await expect(bare.tierToObjectStorage(id)).rejects.toThrow(
          /objectStorage/,
        );
        expect(evs).toEqual([]); // no DB operation happened
        await bare.close();
      });

      it("a FAILED UPLOAD rolls back: the row is byte-for-byte untouched and still readable", async () => {
        const { data, id } = await fresh(80, 80, 53);
        const before = await row(id);
        const { storage } = spyStorage(real, {
          upload: async () => {
            throw new StorageError("simulated S3 outage");
          },
        });
        const broken = newBucket({ objectStorage: storage, maxRetries: 0 });
        await expect(broken.tierToObjectStorage(id)).rejects.toThrow(
          /simulated S3 outage/,
        );
        const after = await row(id);
        expect(after).toEqual(before); // every column, including data and updated_at
        expect(after.storage_backend).toBe("postgres");
        expect(await objectExists(id)).toBe(false);
        expect((await zb.get(id)).data.equals(data)).toBe(true);
        await broken.close();
      });

      it("retry after a failed COMMIT re-runs the whole closure and still ends consistent", async () => {
        const { data, id } = await fresh(90, 90, 54);
        const { storage, calls } = spyStorage(real);
        const { pool, stats } = flakyPool(raw, {
          failOnce: (t) => (t === "COMMIT" ? "40001" : undefined),
        });
        const retrying = new ZeroBucket({
          pool,
          autoMigrate: false,
          objectStorage: storage,
          retryBaseDelayMs: 1,
        });
        expect(await retrying.tierToObjectStorage(id)).toBe(true);
        expect(stats.injected).toBe(1);
        expect(calls.upload).toBe(2); // uploaded again on retry: same key, idempotent overwrite
        expect((await row(id)).storage_backend).toBe("object_storage");
        expect((await object(id)).body.equals(data)).toBe(true);
      });

      it("locks only the row being tiered: other rows are never blocked, plain reads are never blocked, writes to the SAME row wait", async () => {
        const a = await fresh(60, 60, 61);
        const b = await fresh(61, 61, 62);
        let release!: () => void;
        let entered!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const inUpload = new Promise<void>((r) => (entered = r));
        const { storage } = spyStorage(real, {
          upload: async (k, d, o) => {
            entered();
            await gate; // hold the transaction (and the row lock) open
            return real.upload(k, d, o);
          },
        });
        const slow = newBucket({ objectStorage: storage });
        const tiering = slow.tierToObjectStorage(a.id);
        await inUpload;

        // other row: delete completes immediately
        expect(await zb.delete(b.id)).toBe(true);
        // same row: a plain read still works (MVCC readers are not blocked)...
        expect((await zb.get(a.id)).data.equals(a.data)).toBe(true);
        // ...but a competing write waits for the lock
        let deleted = false;
        const pendingDelete = zb
          .delete(a.id)
          .then((r) => ((deleted = true), r));
        await new Promise((r) => setTimeout(r, 300));
        expect(deleted).toBe(false);

        release();
        expect(await tiering).toBe(true);
        expect(await pendingDelete).toBe(true); // it ran after tiering committed, saw a tiered row...
        expect(await objectExists(a.id)).toBe(false); // ...and cleaned up the object
        await slow.close();
      });
    });

    describe("delete", () => {
      it("removes the S3 object along with the row", async () => {
        const { id } = await fresh(40, 40, 71);
        await zb.tierToObjectStorage(id);
        expect(await objectExists(id)).toBe(true);
        expect(await zb.delete(id)).toBe(true);
        expect(await objectExists(id)).toBe(false);
        expect(await zb.exists(id)).toBe(false);
      });

      it("deleteMany cleans up every tiered object, and leaves classic rows alone", async () => {
        const x = await fresh(41, 41, 72);
        const y = await fresh(42, 42, 73);
        const z = await fresh(43, 43, 74);
        await zb.tierToObjectStorage(x.id);
        await zb.tierToObjectStorage(z.id);
        const res = await zb.deleteMany([x.id, y.id, z.id]);
        expect(res.every((r) => r.deleted)).toBe(true);
        expect([await objectExists(x.id), await objectExists(z.id)]).toEqual([
          false,
          false,
        ]);
      });

      it("an S3 delete failure never fails the delete; it is reported through onOperation", async () => {
        const { id } = await fresh(44, 44, 75);
        await zb.tierToObjectStorage(id);
        const evs: OperationEvent[] = [];
        const { storage } = spyStorage(real, {
          delete: async () => {
            throw new StorageError("simulated S3 delete failure");
          },
        });
        const flaky = newBucket({
          objectStorage: storage,
          onOperation: (e) => evs.push(e),
        });
        expect(await flaky.delete(id)).toBe(true); // the row IS gone
        expect(await zb.exists(id)).toBe(false);
        const ev = evs.find((e) => e.operation === "object_storage_delete");
        expect(ev).toMatchObject({ success: false });
        expect(ev!.error).toMatch(/simulated S3 delete failure/);
        await flaky.close();
        await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: id })); // tidy the orphan
      });

      it("NEVER deletes the object inside a caller-owned transaction: a rollback must not lose data", async () => {
        const { data, id } = await fresh(45, 45, 76);
        await zb.tierToObjectStorage(id);
        const client = await raw.connect();
        try {
          await client.query("BEGIN");
          expect(await zb.delete(id, { connection: client })).toBe(true);
          expect(await objectExists(id)).toBe(true); // object untouched while the outcome is unknown
          await client.query("ROLLBACK");
        } finally {
          client.release();
        }
        expect((await zb.get(id)).data.equals(data)).toBe(true); // row restored AND still readable
      });
    });

    describe("integrity: loud failures, never silent truncation or wrong data", () => {
      it("reading a tiered row without objectStorage throws StorageError (metadata still works)", async () => {
        const { id } = await fresh(46, 46, 81);
        await zb.tierToObjectStorage(id);
        const bare = newBucket();
        await expect(bare.get(id)).rejects.toBeInstanceOf(StorageError);
        await expect(bare.getStream(id)).rejects.toBeInstanceOf(StorageError);
        await expect(bare.getMany([id])).rejects.toBeInstanceOf(StorageError);
        expect((await bare.metadata(id)).imageId).toBe(id);
        await bare.close();
      });

      it("an object replaced with different-sized bytes is detected on get()", async () => {
        const { id } = await fresh(47, 47, 82);
        await zb.tierToObjectStorage(id);
        await s3.send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: id,
            Body: Buffer.from("tampered"),
            ContentType: "image/png",
          }),
        );
        await expect(zb.get(id)).rejects.toThrow(/corrupted or was replaced/);
      });

      it("a missing or short object makes streaming throw, never yield a truncated image", async () => {
        const { id } = await fresh(150, 150, 83);
        const size = (await zb.metadata(id)).sizeBytes;
        await zb.tierToObjectStorage(id);

        await s3.send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: id,
            Body: Buffer.alloc(Math.floor(size / 2)),
            ContentType: "image/png",
          }),
        );
        const parts: Buffer[] = [];
        await expect(
          (async () => {
            for await (const c of await zb.getStream(id, {
              chunkSize: Math.ceil(size / 4),
            }))
              parts.push(c);
          })(),
        ).rejects.toBeInstanceOf(StorageError);
        expect(Buffer.concat(parts).length).toBeLessThan(size);

        await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: id }));
        await expect(zb.get(id)).rejects.toBeInstanceOf(StorageError);
      });
    });
  },
);
