/**
 * migrateClassicToDedup() on SQLite and MySQL/MariaDB (0.11.0), with PostgreSQL alongside for parity.
 *
 * The same scenarios run against every database that is available: SQLite needs nothing (Node >= 22
 * here, like the CLI tests), PostgreSQL needs ZEROBUCKET_TEST_DATABASE_URL, MySQL/MariaDB needs
 * ZEROBUCKET_TEST_MYSQL_URL. A kind that is not configured is skipped, not failed.
 */
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import mysql, { type Pool as MyPool } from "mysql2/promise";
import pg from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  StorageError,
  ZeroBucket,
  type MysqlPoolConnectionLike,
  type MysqlPoolLike,
} from "../src/index.js";
import { DATABASE_URL, makeImage } from "./helpers.js";

const MYSQL_URL = process.env.ZEROBUCKET_TEST_MYSQL_URL;
const nodeMajor = Number(process.versions.node.split(".")[0]);

type Kind = "postgres" | "sqlite" | "mysql";
const kinds: Kind[] = [
  ...(DATABASE_URL ? (["postgres"] as const) : []),
  ...(MYSQL_URL ? (["mysql"] as const) : []),
  ...(nodeMajor >= 22 ? (["sqlite"] as const) : []),
];

// A fixed timestamp, written in each database's own text format, with microseconds (a JS Date would lose them).
const FIXED_TS: Record<Kind, string> = {
  postgres: "2026-01-02 03:04:05.123456+00",
  mysql: "2026-01-02 03:04:05.123456",
  sqlite: "2026-01-02T03:04:05.123456+00:00",
};

for (const kind of kinds) {
  describe(`migrateClassicToDedup on ${kind}`, () => {
    let dir: string;
    let path = "";
    let n = 0;
    let pgPool: pg.Pool | undefined;
    let myPool: MyPool | undefined;
    const open: ZeroBucket[] = [];

    const options = (dedup: boolean) =>
      kind === "postgres"
        ? { connectionString: DATABASE_URL, dedup }
        : kind === "mysql"
          ? { mysql: MYSQL_URL, dedup }
          : { sqlite: path, dedup };
    const make = (dedup: boolean, extra: Record<string, unknown> = {}) => {
      const zb = new ZeroBucket({ ...options(dedup), ...extra } as never);
      open.push(zb);
      return zb;
    };

    /** Run SQL on the raw database, whatever it is. `?` placeholders for every kind (converted for pg). */
    const sql = async (text: string, params: unknown[] = []): Promise<any[]> => {
      if (kind === "postgres") {
        let i = 0;
        return (await pgPool!.query(text.replace(/\?/g, () => `$${++i}`), params))
          .rows;
      }
      if (kind === "mysql") return (await myPool!.query(text, params))[0] as any[];
      const Database = (await import("better-sqlite3")).default;
      const db = new Database(path);
      try {
        const stmt = db.prepare(text);
        return stmt.reader ? stmt.all(...params) : (stmt.run(...params), []);
      } finally {
        db.close();
      }
    };
    const count = async (table: string) =>
      Number((await sql(`SELECT count(*) AS n FROM ${table}`))[0].n);

    const dropAll = async () => {
      if (kind === "postgres")
        await sql(
          "DROP TABLE IF EXISTS zerobucket_images, zerobucket_image_refs, zerobucket_blobs CASCADE",
        );
      else if (kind === "mysql") {
        await sql("DROP TABLE IF EXISTS zerobucket_image_refs"); // references blobs: drop first
        await sql("DROP TABLE IF EXISTS zerobucket_blobs");
        await sql("DROP TABLE IF EXISTS zerobucket_images");
      } else {
        n += 1;
        path = join(dir, `m${n}.db`);
      }
    };

    /** A classic row whose bytes live in object storage (so the database holds none). */
    const insertTiered = async (key: string, checksum: string) => {
      if (kind === "postgres")
        await sql(
          `INSERT INTO zerobucket_images (data, mime_type, size_bytes, checksum_sha256, storage_backend, object_storage_bucket, object_storage_key)
           VALUES (NULL, 'image/png', 10, ?, 'object_storage', 'b', ?)`,
          [checksum, key],
        );
      else
        await sql(
          `INSERT INTO zerobucket_images (id, data, mime_type, size_bytes, checksum_sha256, created_at, updated_at, storage_backend, object_storage_bucket, object_storage_key)
           VALUES (?, NULL, 'image/png', 10, ?, ?, ?, 'object_storage', 'b', ?)`,
          [randomUUID(), checksum, FIXED_TS[kind], FIXED_TS[kind], key],
        );
    };

    beforeAll(async () => {
      dir = mkdtempSync(join(tmpdir(), "zb-migrate-"));
      if (kind === "postgres") pgPool = new pg.Pool({ connectionString: DATABASE_URL });
      if (kind === "mysql") myPool = mysql.createPool({ uri: MYSQL_URL! });
    });
    afterAll(async () => {
      await dropAll().catch(() => {});
      await pgPool?.end();
      await myPool?.end();
      rmSync(dir, { recursive: true, force: true });
    });
    beforeEach(async () => {
      await dropAll();
    });
    const closeAll = async () => {
      while (open.length) await open.pop()!.close();
    };

    it("copies everything, preserves ids, dedups identical content, never touches the classic table, and is re-runnable", async () => {
      const classic = make(false);
      const dup = await makeImage("png", 33, 33, { seed: 40 });
      const ids = [
        await classic.put(dup, { filename: "d1.png" }),
        await classic.put(dup, { filename: "d2.png" }),
        ...(await Promise.all(
          [0, 1, 2].map(async (i) =>
            classic.put(await makeImage("png", 34 + i, 34, { seed: 41 + i }), {
              filename: `u${i}.png`,
            }),
          ),
        )),
      ];
      const dedup = make(true);
      expect(await dedup.migrateClassicToDedup()).toEqual({
        imagesMigrated: 5,
        distinctBlobsCreated: 4,
        duplicateReferencesFound: 1,
        alreadyMigrated: 0,
        tieredSkipped: 0,
      });
      expect(await count("zerobucket_images")).toBe(5); // classic untouched
      expect(await count("zerobucket_image_refs")).toBe(5);
      expect(await count("zerobucket_blobs")).toBe(4);
      // exact timestamps survive (microseconds, not truncated to JS milliseconds). SQLite and MySQL
      // carry updated_at over too; the PostgreSQL migration has always let it reset to the migration time.
      const joinOn = kind === "postgres" ? "USING (id)" : "ON c.id = r.id";
      const stamps =
        kind === "postgres"
          ? "c.created_at <> r.created_at"
          : "c.created_at <> r.created_at OR c.updated_at <> r.updated_at";
      expect(
        Number(
          (
            await sql(
              `SELECT count(*) AS n FROM zerobucket_images c JOIN zerobucket_image_refs r ${joinOn} WHERE ${stamps}`,
            )
          )[0].n,
        ),
      ).toBe(0);
      expect((await dedup.get(ids[0]!)).data.equals(dup)).toBe(true); // the same id works in dedup mode
      expect((await dedup.get(ids[1]!)).filename).toBe("d2.png");
      const checksum = (await dedup.get(ids[0]!)).checksumSha256;
      expect(
        Number(
          (
            await sql(
              "SELECT ref_count AS c FROM zerobucket_blobs WHERE checksum_sha256 = ?",
              [checksum],
            )
          )[0].c,
        ),
      ).toBe(2);
      // every blob's ref_count equals its real number of references
      expect(
        await sql(
          `SELECT b.checksum_sha256 AS k FROM zerobucket_blobs b
           LEFT JOIN (SELECT checksum_sha256, count(*) AS n FROM zerobucket_image_refs GROUP BY checksum_sha256) x
             ON x.checksum_sha256 = b.checksum_sha256
           WHERE b.ref_count <> COALESCE(x.n, 0)`,
        ),
      ).toEqual([]);

      expect(await dedup.migrateClassicToDedup()).toEqual({
        imagesMigrated: 0,
        distinctBlobsCreated: 0,
        duplicateReferencesFound: 0,
        alreadyMigrated: 5,
        tieredSkipped: 0,
      }); // safe to re-run, and not double-counted
      expect(
        Number(
          (
            await sql(
              "SELECT ref_count AS c FROM zerobucket_blobs WHERE checksum_sha256 = ?",
              [checksum],
            )
          )[0].c,
        ),
      ).toBe(2);
      await closeAll();
    });

    it("handles more rows than one batch, including rows that all share one created_at (keyset paging ties)", async () => {
      const classic = make(false);
      const imgs = await Promise.all(
        Array.from({ length: 47 }, (_, i) =>
          makeImage("png", 8, 8 + (i % 5), { seed: 500 + i }),
        ),
      );
      const res = await classic.putMany(imgs);
      expect(res.every((r) => r.success)).toBe(true);
      // Force the worst case for paging: every row has exactly the same created_at.
      await sql("UPDATE zerobucket_images SET created_at = ?", [FIXED_TS[kind]]);
      const stats = await make(true).migrateClassicToDedup();
      expect(stats.imagesMigrated).toBe(47);
      expect(await count("zerobucket_image_refs")).toBe(47);
      expect(await count("zerobucket_images")).toBe(47);
      await closeAll();
    });

    it("refuses classic rows that are tiered to object storage, naming skipTiered, and changes nothing", async () => {
      await make(false).put(await makeImage("png", 35, 35, { seed: 60 }));
      await insertTiered("k", "c".repeat(64));
      const dedup = make(true);
      await expect(dedup.migrateClassicToDedup()).rejects.toThrow(
        /1 classic image\(s\) are tiered to object storage/,
      );
      await expect(dedup.migrateClassicToDedup()).rejects.toThrow(/skipTiered: true/);
      expect(await count("zerobucket_image_refs")).toBe(0);
      expect(await count("zerobucket_blobs")).toBe(0);
      await closeAll();
    });

    it("skipTiered: migrates the rows in the database, skips and counts the tiered ones, leaves them untouched", async () => {
      const classic = make(false);
      const keep = await makeImage("png", 36, 36, { seed: 70 });
      const keepId = await classic.put(keep);
      await classic.put(await makeImage("png", 37, 37, { seed: 71 }));
      await insertTiered("k1", "d".repeat(64));
      await insertTiered("k2", "e".repeat(64));
      const dedup = make(true);
      expect(await dedup.migrateClassicToDedup({ skipTiered: true })).toEqual({
        imagesMigrated: 2,
        distinctBlobsCreated: 2,
        duplicateReferencesFound: 0,
        alreadyMigrated: 0,
        tieredSkipped: 2,
      });
      expect((await dedup.get(keepId)).data.equals(keep)).toBe(true);
      expect(await count("zerobucket_images")).toBe(4); // classic table untouched, tiered rows included
      expect(
        Number(
          (
            await sql(
              "SELECT count(*) AS n FROM zerobucket_images WHERE storage_backend = 'object_storage'",
            )
          )[0].n,
        ),
      ).toBe(2);
      expect(await dedup.migrateClassicToDedup({ skipTiered: true })).toMatchObject({
        imagesMigrated: 0,
        alreadyMigrated: 2,
        tieredSkipped: 2,
      }); // still safe to re-run
      await closeAll();
    });

    it("skipTiered with nothing tiered behaves exactly like the default", async () => {
      await make(false).put(await makeImage("png", 38, 38, { seed: 72 }));
      expect(await make(true).migrateClassicToDedup({ skipTiered: true })).toEqual({
        imagesMigrated: 1,
        distinctBlobsCreated: 1,
        duplicateReferencesFound: 0,
        alreadyMigrated: 0,
        tieredSkipped: 0,
      });
      await closeAll();
    });

    it("errors clearly on a non-dedup instance, and when there is no classic table", async () => {
      await expect(make(false).migrateClassicToDedup()).rejects.toBeInstanceOf(StorageError);
      await expect(make(false).migrateClassicToDedup()).rejects.toThrow(
        /created with dedup: true/,
      );
      // Only the dedup tables exist: nothing to migrate.
      await expect(make(true).migrateClassicToDedup()).rejects.toThrow(
        /No classic zerobucket_images table/,
      );
      await closeAll();
    });

    it("reports a migrate_to_dedup event to onOperation, success and failure", async () => {
      if (kind === "postgres") return; // the PostgreSQL migration has never reported an event
      await make(false).put(await makeImage("png", 21, 21, { seed: 2 }));
      const events: { operation: string; success: boolean }[] = [];
      const dedup = make(true, { onOperation: (e: any) => events.push(e) });
      await dedup.migrateClassicToDedup();
      expect(
        events.filter((e) => e.operation === "migrate_to_dedup"),
      ).toMatchObject([{ success: true }]);
      events.length = 0;
      await sql("DROP TABLE zerobucket_images");
      await expect(dedup.migrateClassicToDedup()).rejects.toThrow(/No classic/);
      expect(
        events.filter((e) => e.operation === "migrate_to_dedup"),
      ).toMatchObject([{ success: false }]);
      await closeAll();
    });

    it("runs fine on an empty classic table", async () => {
      await make(false).put(await makeImage("png", 20, 20, { seed: 1 }));
      await sql("DELETE FROM zerobucket_images");
      expect(await make(true).migrateClassicToDedup()).toEqual({
        imagesMigrated: 0,
        distinctBlobsCreated: 0,
        duplicateReferencesFound: 0,
        alreadyMigrated: 0,
        tieredSkipped: 0,
      });
      await closeAll();
    });

    it("is all-or-nothing: a failure part-way leaves the dedup tables exactly as they were, and a re-run then succeeds", async () => {
      if (kind === "postgres") return; // covered by the PostgreSQL tests in dedup.test.ts
      const classic = make(false);
      for (let i = 0; i < 5; i++)
        await classic.put(await makeImage("png", 20 + i, 20, { seed: 90 + i }), {
          filename: i === 3 ? "boom" : `f${i}.png`,
        });
      await closeAll();

      let dedup: ZeroBucket;
      if (kind === "sqlite") {
        // A trigger makes the 4th reference insert fail, after three were already written. The dedup
        // tables must exist first, so create the instance and let it finish migrating its schema.
        dedup = make(true);
        await dedup.ready();
        await sql(
          `CREATE TRIGGER zb_boom BEFORE INSERT ON zerobucket_image_refs WHEN NEW.original_filename = 'boom'
           BEGIN SELECT RAISE(ABORT, 'injected failure'); END`,
        );
      } else {
        // MySQL: a pool wrapper fails the statement that inserts the 'boom' reference (no privileges needed).
        const real = myPool!;
        let armed = true;
        const pool: MysqlPoolLike = {
          async getConnection() {
            const c = await real.getConnection();
            return new Proxy(c, {
              get(t, p) {
                if (p === "query" || p === "execute")
                  return async (...a: unknown[]) => {
                    const params = (a[1] ?? []) as unknown[];
                    if (
                      armed &&
                      typeof a[0] === "string" &&
                      a[0].includes("INSERT INTO zerobucket_image_refs") &&
                      params.includes("boom")
                    )
                      throw new Error("injected failure");
                    return (t as any)[p](...a);
                  };
                const v = (t as any)[p];
                return typeof v === "function" ? v.bind(t) : v;
              },
            }) as unknown as MysqlPoolConnectionLike;
          },
          end: async () => {},
        };
        dedup = make(true, { mysql: undefined, mysqlPool: pool, maxRetries: 0 });
        (globalThis as any).__zbDisarm = () => (armed = false);
      }

      await expect(dedup.migrateClassicToDedup()).rejects.toThrow(/injected failure/);
      expect(await count("zerobucket_image_refs")).toBe(0);
      expect(await count("zerobucket_blobs")).toBe(0);
      expect(await count("zerobucket_images")).toBe(5);

      if (kind === "sqlite") await sql("DROP TRIGGER zb_boom");
      else (globalThis as any).__zbDisarm();
      expect((await dedup.migrateClassicToDedup()).imagesMigrated).toBe(5);
      expect(await count("zerobucket_image_refs")).toBe(5);
      await closeAll();
    });

    it("MySQL: the migration lock is held until the transaction COMMITS, even when the commit is slow", async () => {
      if (kind !== "mysql") return;
      const classic = make(false);
      for (let i = 0; i < 6; i++)
        await classic.put(await makeImage("png", 25, 30 + i, { seed: 700 + i }));
      await closeAll();

      // A slow COMMIT widens the window between "the copy is done" and "the rows are visible to others".
      // If the lock were released before the commit, the second migration would start inside that window,
      // not see the first one's uncommitted rows, and collide on their primary keys.
      const real = myPool!;
      const slowCommit: MysqlPoolLike = {
        async getConnection() {
          const c = await real.getConnection();
          return new Proxy(c, {
            get(t, p) {
              if (p === "commit")
                return async () => {
                  await new Promise((r) => setTimeout(r, 400));
                  return (t as any).commit();
                };
              const v = (t as any)[p];
              return typeof v === "function" ? v.bind(t) : v;
            },
          }) as unknown as MysqlPoolConnectionLike;
        },
        end: async () => {},
      };
      const opts = { mysql: undefined, mysqlPool: slowCommit, maxRetries: 0 };
      const [a, b] = await Promise.all([
        make(true, opts).migrateClassicToDedup(),
        make(true, opts).migrateClassicToDedup(),
      ]);
      expect(a.imagesMigrated + b.imagesMigrated).toBe(6);
      expect(a.alreadyMigrated + b.alreadyMigrated).toBe(6);
      expect(await count("zerobucket_image_refs")).toBe(6);
      await closeAll();
    });

    it("two migrations started at once never double-count: together they copy each image exactly once", async () => {
      const classic = make(false);
      const dup = await makeImage("png", 25, 25, { seed: 5 });
      for (let i = 0; i < 12; i++)
        await classic.put(i % 3 === 0 ? dup : await makeImage("png", 25, 26 + i, { seed: 6 + i }));
      await closeAll();

      const [a, b] = await Promise.all([
        make(true).migrateClassicToDedup(),
        make(true).migrateClassicToDedup(),
      ]);
      expect(a.imagesMigrated + b.imagesMigrated).toBe(12);
      expect(a.alreadyMigrated + b.alreadyMigrated).toBe(12);
      expect(await count("zerobucket_image_refs")).toBe(12);
      expect(
        await sql(
          `SELECT b.checksum_sha256 AS k FROM zerobucket_blobs b
           LEFT JOIN (SELECT checksum_sha256, count(*) AS n FROM zerobucket_image_refs GROUP BY checksum_sha256) x
             ON x.checksum_sha256 = b.checksum_sha256
           WHERE b.ref_count <> COALESCE(x.n, 0)`,
        ),
      ).toEqual([]);
      await closeAll();
    });
  });
}
