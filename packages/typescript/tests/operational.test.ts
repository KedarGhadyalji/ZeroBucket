import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  ImageNotFoundError,
  StorageError,
  ZeroBucket,
  type OperationEvent,
} from "../src/index.js";
import { DATABASE_URL, makeImage, newBucket } from "./helpers.js";

const fail = (code: string, message = "injected") =>
  Object.assign(new Error(message), { code });

/** Wrap a real pool, letting a test inject failures into connect() and into specific statements. */
function flakyPool(
  real: pg.Pool,
  plan: {
    connectFailures?: string[];
    failOnce?: (text: string) => string | undefined;
  } = {},
) {
  const stats = { connects: 0, injected: 0 };
  const failures = [...(plan.connectFailures ?? [])];
  const used = new Set<string>();
  const proxy = new Proxy(real, {
    get(target, prop, recv) {
      if (prop === "connect") {
        return async () => {
          stats.connects++;
          const code = failures.shift();
          if (code) {
            stats.injected++;
            throw fail(code);
          }
          const client = await target.connect();
          return new Proxy(client, {
            get(c, p, r) {
              if (p === "query") {
                return async (text: unknown, ...rest: unknown[]) => {
                  const t = typeof text === "string" ? text : "";
                  const code = plan.failOnce?.(t);
                  if (code && !used.has(t)) {
                    used.add(t);
                    stats.injected++;
                    await (c.query as any)("ROLLBACK"); // server never saw a commit
                    throw fail(code);
                  }
                  return (c.query as any)(text, ...rest);
                };
              }
              const v = Reflect.get(c, p, r);
              return typeof v === "function" ? v.bind(c) : v;
            },
          });
        };
      }
      const v = Reflect.get(target, prop, recv);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
  return { pool: proxy as unknown as pg.Pool, stats };
}

describe.skipIf(!DATABASE_URL)(
  "operations: metrics, retry, pooling, migration",
  () => {
    let raw: pg.Pool;
    beforeAll(async () => {
      raw = new pg.Pool({ connectionString: DATABASE_URL });
      await newBucket().ready(); // make sure the schema exists for the injected-pool tests
    });
    afterAll(() => raw.end());

    describe("onOperation", () => {
      it("emits one event per operation with success/failure, retryCount and duration", async () => {
        const events: OperationEvent[] = [];
        const zb = newBucket({ onOperation: (e) => events.push(e) });
        const id = await zb.put(await makeImage());
        await zb.get(id);
        await zb.delete(id);
        await zb.get(id).catch(() => {});
        const ops = events.map((e) => e.operation);
        expect(ops).toEqual(["put", "get", "delete", "get"]);
        expect(
          events.every((e) => e.durationMs >= 0 && e.retryCount === 0),
        ).toBe(true);
        expect(events.map((e) => e.success)).toEqual([true, true, true, true]); // a miss is not a failure
        await zb.close();
      });

      it("reports failures with the error message", async () => {
        const events: OperationEvent[] = [];
        const zb = newBucket({
          onOperation: (e) => events.push(e),
          maxRetries: 0,
        });
        // data=null violates the CHECK constraint -> permanent error
        const backendPut = (zb as any).backend.put.bind((zb as any).backend);
        await expect(
          backendPut({
            data: null,
            mimeType: "x",
            originalFilename: null,
            sizeBytes: 1,
            width: null,
            height: null,
            checksumSha256: "a".repeat(64),
          }),
        ).rejects.toBeInstanceOf(StorageError);
        const e = events.at(-1)!;
        expect(e).toMatchObject({
          operation: "put",
          success: false,
          retryCount: 0,
        });
        expect(e.error).toBeTruthy();
        await zb.close();
      });

      it("never lets a throwing metrics callback break a real operation", async () => {
        const zb = newBucket({
          onOperation: () => {
            throw new Error("metrics bug");
          },
        });
        const id = await zb.put(await makeImage());
        expect((await zb.get(id)).width).toBe(64);
        await zb.close();
      });
    });

    describe("retry", () => {
      const opts = { retryBaseDelayMs: 1, autoMigrate: false } as const;

      it("retries transient failures and reports retryCount", async () => {
        const { pool, stats } = flakyPool(raw, {
          connectFailures: ["40001", "ECONNRESET"],
        });
        const events: OperationEvent[] = [];
        const zb = new ZeroBucket({
          pool,
          ...opts,
          onOperation: (e) => events.push(e),
        });
        const id = await zb.put(await makeImage());
        expect(await zb.exists(id)).toBe(true);
        expect(stats.injected).toBe(2);
        expect(events[0]).toMatchObject({
          operation: "put",
          success: true,
          retryCount: 2,
        });
      });

      it("gives up after maxRetries", async () => {
        const { pool, stats } = flakyPool(raw, {
          connectFailures: Array(10).fill("40P01"),
        });
        const events: OperationEvent[] = [];
        const zb = new ZeroBucket({
          pool,
          ...opts,
          maxRetries: 3,
          onOperation: (e) => events.push(e),
        });
        await expect(
          zb.get("00000000-0000-4000-8000-000000000000"),
        ).rejects.toBeInstanceOf(StorageError);
        expect(stats.connects).toBe(4); // 1 try + 3 retries
        expect(events.at(-1)).toMatchObject({ success: false, retryCount: 3 });
      });

      it("maxRetries: 0 disables retry", async () => {
        const { pool, stats } = flakyPool(raw, {
          connectFailures: ["40001", "40001"],
        });
        const zb = new ZeroBucket({ pool, ...opts, maxRetries: 0 });
        await expect(zb.put(await makeImage())).rejects.toBeInstanceOf(
          StorageError,
        );
        expect(stats.connects).toBe(1);
      });

      it("never retries a permanent error (constraint / syntax class)", async () => {
        const { pool, stats } = flakyPool(raw, {
          connectFailures: ["23505", "42601"],
        });
        const zb = new ZeroBucket({ pool, ...opts });
        await expect(zb.put(await makeImage())).rejects.toBeInstanceOf(
          StorageError,
        );
        expect(stats.connects).toBe(1);
      });

      it("retries the WHOLE transaction: a failed COMMIT never leaves duplicate or half-applied rows", async () => {
        // The Python build hit exactly this class of bug (a pool library replaying
        // only the failed statement). Fail the COMMIT once, after every INSERT ran.
        const marker = `retry-${Date.now()}-${Math.random()}`;
        const { pool, stats } = flakyPool(raw, {
          failOnce: (t) => (t === "COMMIT" ? "40001" : undefined),
        });
        const zb = new ZeroBucket({ pool, ...opts });
        const imgs = await Promise.all(
          [1, 2, 3, 4].map((n) => makeImage("png", 8 + n, 8, { seed: n })),
        );
        const res = await zb.putMany(imgs, {
          filenames: imgs.map(() => marker),
        });
        expect(stats.injected).toBe(1);
        expect(res.every((r) => r.success)).toBe(true);
        const { rows } = await raw.query(
          "SELECT count(*)::int AS n FROM zerobucket_images WHERE original_filename = $1",
          [marker],
        );
        expect(rows[0].n).toBe(4); // exactly 4: the failed attempt was fully rolled back
        for (let i = 0; i < imgs.length; i++)
          expect((await zb.get(res[i]!.imageId!)).data.equals(imgs[i]!)).toBe(
            true,
          );
      });

      it("never retries when the caller owns the transaction (connection=)", async () => {
        let attempts = 0;
        const callerConn = {
          query: async () => {
            attempts++;
            throw fail("40001");
          },
        };
        const zb = new ZeroBucket({ pool: raw, ...opts });
        await expect(
          zb.put(await makeImage(), { connection: callerConn }),
        ).rejects.toBeInstanceOf(StorageError);
        expect(attempts).toBe(1);
      });
    });

    describe("pooling", () => {
      it("pool timeout is explicit: a full pool fails fast instead of hanging", async () => {
        const zb = newBucket({
          poolMaxSize: 1,
          poolTimeoutMs: 150,
          maxRetries: 0,
        });
        await zb.ready();
        const pool = (zb as any).backend.pool as pg.Pool;
        const held = await pool.connect();
        const t0 = Date.now();
        await expect(
          zb.exists("00000000-0000-4000-8000-000000000000"),
        ).rejects.toBeInstanceOf(StorageError);
        expect(Date.now() - t0).toBeLessThan(2000);
        held.release();
        expect(await zb.exists("00000000-0000-4000-8000-000000000000")).toBe(
          false,
        ); // usable again
        await zb.close();
      });

      it("a waiter is served promptly when a slot frees", async () => {
        const zb = newBucket({
          poolMaxSize: 1,
          poolTimeoutMs: 5000,
          maxRetries: 0,
        });
        await zb.ready();
        const pool = (zb as any).backend.pool as pg.Pool;
        const held = await pool.connect();
        setTimeout(() => held.release(), 100);
        const t0 = Date.now();
        expect(await zb.exists("00000000-0000-4000-8000-000000000000")).toBe(
          false,
        );
        expect(Date.now() - t0).toBeLessThan(1500);
        await zb.close();
      });

      it("reuses connections: 60 operations through a 2-slot pool open at most 2", async () => {
        const zb = newBucket({ poolMaxSize: 2 });
        const id = await zb.put(await makeImage());
        await Promise.all(Array.from({ length: 60 }, () => zb.exists(id)));
        const pool = (zb as any).backend.pool as pg.Pool;
        expect(pool.totalCount).toBeLessThanOrEqual(2);
        await zb.close();
      });
    });

    describe("lifecycle", () => {
      it("migration is safe under concurrent first use (advisory lock)", async () => {
        await raw.query("DROP TABLE IF EXISTS zerobucket_images CASCADE");
        const buckets = Array.from({ length: 12 }, () => newBucket());
        await Promise.all(buckets.map((b) => b.ready())); // would throw 'duplicate key ... pg_type' without the lock
        const { rows } = await raw.query(
          "SELECT to_regclass('zerobucket_images') AS t",
        );
        expect(rows[0].t).toBe("zerobucket_images");
        await Promise.all(buckets.map((b) => b.close()));
      });

      it("creates the same constraint and columns as the Python package", async () => {
        const { rows: cols } = await raw.query(
          "SELECT column_name, is_nullable FROM information_schema.columns WHERE table_name='zerobucket_images' ORDER BY ordinal_position",
        );
        expect(cols.map((c) => c.column_name)).toEqual([
          "id",
          "data",
          "mime_type",
          "original_filename",
          "size_bytes",
          "width",
          "height",
          "checksum_sha256",
          "created_at",
          "updated_at",
          "storage_backend",
          "object_storage_bucket",
          "object_storage_key",
        ]);
        expect(cols.find((c) => c.column_name === "data")!.is_nullable).toBe(
          "YES",
        );
        const { rows: cons } = await raw.query(
          "SELECT 1 FROM pg_constraint WHERE conname='zerobucket_storage_location_check'",
        );
        expect(cons.length).toBe(1);
      });

      it("a bad connection string fails with StorageError, at connect() time, without leaking", async () => {
        await expect(
          ZeroBucket.connect({
            connectionString: "postgresql://nobody:x@127.0.0.1:1/none",
          }),
        ).rejects.toBeInstanceOf(StorageError);
        const lazy = new ZeroBucket({
          connectionString: "postgresql://nobody:x@127.0.0.1:1/none",
          maxRetries: 0,
        });
        await expect(lazy.put(await makeImage())).rejects.toBeInstanceOf(
          StorageError,
        );
      });

      it("rejects construction without a connection source; operations after close() fail clearly", async () => {
        expect(() => new ZeroBucket({})).toThrow(TypeError);
        const zb = newBucket();
        const id = await zb.put(await makeImage());
        await zb.close();
        await expect(zb.get(id)).rejects.toBeInstanceOf(StorageError);
      });

      it("does not close a pool it did not create", async () => {
        const mine = new pg.Pool({ connectionString: DATABASE_URL });
        const zb = new ZeroBucket({ pool: mine });
        await zb.put(await makeImage());
        await zb.close();
        expect((await mine.query("SELECT 1 AS ok")).rows[0].ok).toBe(1); // still alive
        await mine.end();
      });

      it("ImageNotFoundError carries the id", async () => {
        const zb = newBucket();
        const err = await zb
          .get("00000000-0000-4000-8000-0000000000bb")
          .catch((e) => e);
        expect(err).toBeInstanceOf(ImageNotFoundError);
        expect(err.imageId).toBe("00000000-0000-4000-8000-0000000000bb");
        await zb.close();
      });
    });
  },
);
