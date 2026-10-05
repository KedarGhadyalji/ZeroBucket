import { spawn } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CreateBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import Database from "better-sqlite3";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import {
  AccessDeniedError,
  CorruptedImageError,
  ImageNotFoundError,
  ObjectStorage,
  SQLiteBackend,
  StorageError,
  UnsupportedFormatError,
  ZeroBucket,
  type ObjectStorageLike,
  type OperationEvent,
} from "../src/index.js";
import { createImageHandler } from "../src/http.js";
import { makeGif, makeImage } from "./helpers.js";

const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const collect = async (it: AsyncIterable<Buffer>) => {
  const a: Buffer[] = [];
  for await (const c of it) a.push(c);
  return Buffer.concat(a);
};
const tmpDb = () =>
  join(mkdtempSync(join(tmpdir(), "zb-sqlite-")), "images.db");
const ENDPOINT = process.env.ZEROBUCKET_TEST_S3_ENDPOINT;
const ROOT = process.cwd();

/** A second PROCESS that holds the SQLite write lock for `ms`, like another app sharing the file. */
function lockFromAnotherProcess(
  path: string,
  ms: number,
): Promise<{ done: Promise<void> }> {
  const script = `const D=require('better-sqlite3');const d=new D(process.argv[1]);d.pragma('busy_timeout=0');d.exec('BEGIN IMMEDIATE');console.log('locked');setTimeout(()=>{d.exec('COMMIT');d.close();},Number(process.argv[2]));`;
  const child = spawn(process.execPath, ["-e", script, path, String(ms)], {
    cwd: ROOT,
    stdio: ["ignore", "pipe", "inherit"],
  });
  const done = new Promise<void>((resolve) =>
    child.on("exit", () => resolve()),
  );
  return new Promise((resolve, reject) => {
    child.stdout.on(
      "data",
      (d) => String(d).includes("locked") && resolve({ done }),
    );
    child.on("error", reject);
    child.on(
      "exit",
      (c) => c !== 0 && reject(new Error(`lock child exited ${c}`)),
    );
  });
}

describe("SQLite: core behaviour (classic mode)", () => {
  let path: string;
  let zb: ZeroBucket;
  beforeEach(async () => {
    path = tmpDb();
    zb = new ZeroBucket({ sqlite: path });
    await zb.ready();
  });
  afterAll(async () => {});

  it("round-trips bytes, checksum and metadata for every supported format", async () => {
    for (const [fmt, mime] of [
      ["jpeg", "image/jpeg"],
      ["png", "image/png"],
      ["webp", "image/webp"],
    ] as const) {
      const data = await makeImage(fmt, 40, 30);
      const id = await zb.put(data, { filename: `x.${fmt}` });
      expect(id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
      const img = await zb.get(id);
      expect(img.data.equals(data)).toBe(true);
      expect(img).toMatchObject({
        mimeType: mime,
        width: 40,
        height: 30,
        sizeBytes: data.length,
        filename: `x.${fmt}`,
        checksumSha256: sha(data),
      });
      expect(await zb.metadata(id)).toMatchObject({
        imageId: id,
        mimeType: mime,
        checksumSha256: sha(data),
      });
    }
    await zb.close();
  });

  it("rejects bad content and reports missing / malformed ids as not found", async () => {
    await expect(zb.put(await makeGif())).rejects.toBeInstanceOf(
      UnsupportedFormatError,
    );
    await expect(zb.put(Buffer.from("not an image"))).rejects.toBeInstanceOf(
      CorruptedImageError,
    );
    await expect(zb.get(randomUUID())).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    await expect(zb.get("not-a-uuid")).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    expect(await zb.exists("not-a-uuid")).toBe(false);
    expect(await zb.delete("not-a-uuid")).toBe(false);
    const id = await zb.put(await makeImage());
    expect(await zb.exists(id)).toBe(true);
    expect(await zb.exists(id.toUpperCase())).toBe(true); // ids are case-insensitive
    expect(await zb.delete(id)).toBe(true);
    expect(await zb.delete(id)).toBe(false);
    await zb.close();
  });

  it("putMany is best-effort, keeps order, and maps ids to inputs", async () => {
    const imgs = await Promise.all(
      Array.from({ length: 25 }, (_, i) =>
        makeImage("png", 8 + i, 8, { seed: i + 1 }),
      ),
    );
    const batch: Buffer[] = [
      ...imgs.slice(0, 2),
      Buffer.from("junk"),
      ...imgs.slice(2),
    ];
    const res = await zb.putMany(batch);
    expect(res.map((r) => r.success)).toEqual(batch.map((_, i) => i !== 2));
    for (let i = 0; i < batch.length; i++) {
      if (i === 2) continue;
      expect((await zb.get(res[i]!.imageId!)).data.equals(batch[i]!)).toBe(
        true,
      );
    }
    await zb.close();
  });

  it("getMany / deleteMany keep input order, report misses, and handle more ids than one SQL chunk", async () => {
    const [a, b] = [
      await zb.put(await makeImage("png", 9, 9)),
      await zb.put(await makeImage("png", 11, 11)),
    ];
    const noise = Array.from({ length: 1300 }, () => randomUUID()); // > 2 chunks of 500 bound variables
    const res = await zb.getMany([
      ...noise.slice(0, 700),
      b,
      ...noise.slice(700),
      a,
      "garbage",
    ]);
    expect(res.length).toBe(1303);
    expect(res[700]).toMatchObject({ success: true, imageId: b });
    expect(res[res.length - 2]).toMatchObject({ success: true, imageId: a });
    expect(res[0]).toMatchObject({ success: false, error: "not found" });
    const del = await zb.deleteMany([...noise, a, b]);
    expect(
      del
        .filter((d) => d.deleted)
        .map((d) => d.imageId)
        .sort(),
    ).toEqual([a, b].sort());
    await zb.close();
  });

  it("uses the same on-disk format as the Python package (so a .db file is shareable)", async () => {
    const id = await zb.put(await makeImage("png", 20, 20), {
      filename: "f.png",
    });
    const raw = new Database(path, { readonly: true });
    const row = raw
      .prepare("SELECT * FROM zerobucket_images WHERE id = ?")
      .get(id) as Record<string, unknown>;
    expect(row.storage_backend).toBe("sqlite"); // NOT 'postgres'
    expect(row.created_at).toMatch(
      /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{6}\+00:00$/,
    ); // Python isoformat(), microseconds
    expect(row.object_storage_key).toBeNull();
    expect(Buffer.isBuffer(row.data)).toBe(true);
    expect(raw.prepare("PRAGMA journal_mode").get()).toEqual({
      journal_mode: "wal",
    });
    const cols = raw
      .prepare("PRAGMA table_info(zerobucket_images)")
      .all()
      .map((c: any) => c.name);
    expect(cols).toEqual([
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
    raw.close();
    await zb.close();
  });

  it("works with an in-memory database", async () => {
    const mem = new ZeroBucket({
      sqlite: ":memory:",
      objectStorage: new ObjectStorage({ bucket: "unused" }),
    });
    const data = await makeImage("png", 12, 12);
    const id = await mem.put(data);
    expect((await mem.get(id)).data.equals(data)).toBe(true);
    await expect(mem.tierToObjectStorage(id)).rejects.toThrow(
      /in-memory|database file/,
    );
    await mem.close();
  });

  it("construction rules: needs a database; sqlite and connectionString are mutually exclusive", () => {
    expect(() => new ZeroBucket({ sqlite: "" })).toThrow(TypeError);
    expect(
      () =>
        new ZeroBucket({ sqlite: "x.db", connectionString: "postgresql://x" }),
    ).toThrow(TypeError);
  });

  it("hooks and the HTTP handler work unchanged on SQLite", async () => {
    const owner = new Map<string, string>();
    const g = new ZeroBucket<{ u: string }>({
      sqlite: path,
      beforeGet: (id, c) => owner.get(id) === c?.u,
    });
    const data = await makeImage("png", 100, 100, { seed: 7 });
    const id = await g.put(data);
    owner.set(id, "alice");
    expect(
      (await g.get(id, { context: { u: "alice" } })).data.equals(data),
    ).toBe(true);
    await expect(g.get(id, { context: { u: "bob" } })).rejects.toBeInstanceOf(
      AccessDeniedError,
    );
    const h = createImageHandler(g, {
      getContext: (r) => ({ u: r.headers.get("x-u") ?? "" }),
    });
    expect(
      (await h(new Request(`http://x/i/${id}`, { headers: { "x-u": "bob" } })))
        .status,
    ).toBe(403);
    const ok = await h(
      new Request(`http://x/i/${id}`, {
        headers: { "x-u": "alice", Range: "bytes=3-12" },
      }),
    );
    expect(ok.status).toBe(206);
    expect(
      Buffer.from(await ok.arrayBuffer()).equals(data.subarray(3, 13)),
    ).toBe(true);
    expect(
      (
        await h(
          new Request(`http://x/i/${id}`, {
            headers: {
              "x-u": "alice",
              "If-None-Match": ok.headers.get("etag")!,
            },
          }),
        )
      ).status,
    ).toBe(304);
    await g.close();
    await zb.close();
  });

  it("emits onOperation events", async () => {
    const evs: OperationEvent[] = [];
    const m = new ZeroBucket({ sqlite: path, onOperation: (e) => evs.push(e) });
    const id = await m.put(await makeImage());
    await m.get(id);
    await m.delete(id);
    expect(evs.map((e) => e.operation)).toEqual(["put", "get", "delete"]);
    expect(
      evs.every((e) => e.success && e.retryCount === 0 && e.durationMs >= 0),
    ).toBe(true);
    await m.close();
    await zb.close();
  });
});

describe("SQLite: streaming", () => {
  let path: string;
  let zb: ZeroBucket;
  let data: Buffer;
  let id: string;
  beforeAll(async () => {
    path = tmpDb();
    zb = new ZeroBucket({ sqlite: path });
    data = await makeImage("png", 300, 300);
    id = await zb.put(data);
  });
  afterAll(() => zb.close());

  it("streams the exact bytes at several chunk sizes, with bounded chunks", async () => {
    for (const chunkSize of [7000, 65536, data.length, data.length * 3]) {
      const sizes: number[] = [];
      const parts: Buffer[] = [];
      for await (const c of await zb.getStream(id, { chunkSize }))
        (sizes.push(c.length), parts.push(c));
      expect(Buffer.concat(parts).equals(data)).toBe(true);
      expect(Math.max(...sizes)).toBeLessThanOrEqual(chunkSize);
      expect(sizes.length).toBe(Math.ceil(data.length / chunkSize));
    }
  });
  it("1-byte chunks on a tiny object, and arbitrary byte ranges", async () => {
    const tiny = await zb.put(await makeImage("png", 2, 2));
    const whole = (await zb.get(tiny)).data;
    expect(
      (await collect(await zb.getStream(tiny, { chunkSize: 1 }))).equals(whole),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, { range: { start: 1000, end: 1999 } }),
        )
      ).equals(data.subarray(1000, 2000)),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, { range: { start: data.length - 9 } }),
        )
      ).equals(data.subarray(data.length - 9)),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, { range: { start: data.length + 5 } }),
        )
      ).length,
    ).toBe(0);
  });
  it("not-found is eager; toWebStream feeds a Response", async () => {
    await expect(zb.getStream(randomUUID())).rejects.toBeInstanceOf(
      ImageNotFoundError,
    );
    expect(
      Buffer.from(
        await new Response(
          await zb.toWebStream(id, { chunkSize: 9000 }),
        ).arrayBuffer(),
      ).equals(data),
    ).toBe(true);
  });
  it("a delete mid-stream raises loudly instead of truncating", async () => {
    const victim = await zb.put(await makeImage("png", 200, 200, { seed: 99 }));
    const it = (await zb.getStream(victim, { chunkSize: 1000 }))[
      Symbol.asyncIterator
    ]();
    await it.next();
    await zb.delete(victim);
    await expect(it.next()).rejects.toBeInstanceOf(StorageError);
  });
  it("a caller-owned read transaction gives a snapshot-consistent stream across a concurrent delete", async () => {
    const victim = await zb.put(await makeImage("png", 200, 200, { seed: 7 }));
    const whole = (await zb.get(victim)).data;
    const mine = new Database(path);
    try {
      mine.exec("BEGIN");
      const it = (
        await zb.getStream(victim, { chunkSize: 1000, connection: mine })
      )[Symbol.asyncIterator]();
      const first = await it.next(); // snapshot taken
      await zb.delete(victim); // concurrent writer (another connection)
      const parts = [first.value as Buffer];
      for (let r = await it.next(); !r.done; r = await it.next())
        parts.push(r.value);
      expect(Buffer.concat(parts).equals(whole)).toBe(true);
      mine.exec("COMMIT");
    } finally {
      mine.close();
    }
  });
});

describe("SQLite: transactions and atomicity on a caller-supplied connection", () => {
  let path: string;
  let zb: ZeroBucket;
  let mine: Database.Database;
  beforeEach(async () => {
    path = tmpDb();
    zb = new ZeroBucket({ sqlite: path });
    await zb.ready();
    mine = new Database(path);
    mine.pragma("busy_timeout = 2000");
  });
  const n = () =>
    (
      mine.prepare("SELECT count(*) AS n FROM zerobucket_images").get() as {
        n: number;
      }
    ).n;

  it("joins the caller's transaction: rollback leaves nothing, commit persists", async () => {
    mine.exec("BEGIN");
    const id = await zb.put(await makeImage("png", 7, 7), { connection: mine });
    expect((await zb.get(id, { connection: mine })).width).toBe(7); // visible inside the tx
    mine.exec("ROLLBACK");
    expect(await zb.exists(id)).toBe(false);

    mine.exec("BEGIN");
    const kept = await zb.put(await makeImage("png", 7, 8), {
      connection: mine,
    });
    mine.exec("COMMIT");
    expect(await zb.exists(kept)).toBe(true);
    mine.close();
    await zb.close();
  });

  const failingSecondInsert = (db: Database.Database) => {
    let inserts = 0;
    return {
      exec: (s: string) => db.exec(s),
      prepare: (sql: string) => {
        const st = db.prepare(sql);
        if (/INSERT INTO zerobucket_images/.test(sql)) {
          return {
            all: (...p: unknown[]) => st.all(...p),
            get: (...p: unknown[]) => st.get(...p),
            run: (...p: unknown[]) => {
              if (++inserts === 2) throw new Error("injected");
              return st.run(...p);
            },
          };
        }
        return st as any;
      },
    };
  };

  it("autocommit handle: a failure mid-batch leaves NO partial rows", async () => {
    const good = await makeImage("png", 9, 9);
    const res = await zb.putMany([good, good, good], {
      connection: failingSecondInsert(mine),
    });
    expect(res.every((r) => !r.success)).toBe(true);
    expect(n()).toBe(0);
    mine.close();
    await zb.close();
  });

  it("inside the caller's transaction: a failure rolls back only our work and the caller's tx stays usable", async () => {
    const keep = await makeImage("png", 9, 10);
    mine.exec("BEGIN");
    const keptId = await zb.put(keep, { connection: mine });
    const good = await makeImage("png", 9, 11);
    await zb.putMany([good, good], { connection: failingSecondInsert(mine) }); // fails, savepoint rolls back
    mine.exec("COMMIT"); // would throw if the savepoint had poisoned the transaction
    expect(n()).toBe(1);
    expect((await zb.get(keptId)).data.equals(keep)).toBe(true);
    mine.close();
    await zb.close();
  });

  it("rejects a pg client passed as connection", async () => {
    await expect(
      zb.put(await makeImage(), {
        connection: { query: async () => ({ rows: [], rowCount: 0 }) },
      }),
    ).rejects.toThrow(/better-sqlite3/);
    mine.close();
    await zb.close();
  });
});

describe("SQLite: lock contention without blocking the event loop", () => {
  it("waits out another PROCESS's write lock with async retries, and the event loop keeps running meanwhile", async () => {
    const path = tmpDb();
    const evs: OperationEvent[] = [];
    const zb = new ZeroBucket({
      sqlite: path,
      onOperation: (e) => evs.push(e),
    });
    await zb.ready();
    const data = await makeImage("png", 20, 20);
    const holder = await lockFromAnotherProcess(path, 700);

    let ticks = 0;
    const timer = setInterval(() => ticks++, 20);
    const t0 = Date.now();
    const id = await zb.put(data); // blocked by the other process
    const waited = Date.now() - t0;
    clearInterval(timer);

    expect(waited).toBeGreaterThanOrEqual(400); // really waited for the lock
    expect(ticks).toBeGreaterThan(10); // the loop was free the whole time (a blocking driver wait would give ~0)
    expect(evs.find((e) => e.operation === "put")!.retryCount).toBeGreaterThan(
      0,
    );
    expect((await zb.get(id)).data.equals(data)).toBe(true);
    await holder.done;
    await zb.close();
  });

  it("gives up with a clear StorageError after busyTimeoutMs", async () => {
    const path = tmpDb();
    const zb = new ZeroBucket({ sqlite: path });
    const quick = new ZeroBucket({
      backend: new SQLiteBackend({ path, busyTimeoutMs: 200 }),
    });
    await zb.ready();
    await quick.ready();
    const holder = await lockFromAnotherProcess(path, 900);
    const t0 = Date.now();
    await expect(quick.put(await makeImage())).rejects.toThrow(/locked|busy/i);
    expect(Date.now() - t0).toBeLessThan(800);
    await holder.done;
    expect(await quick.put(await makeImage("png", 11, 11))).toMatch(
      /^[0-9a-f-]{36}$/,
    ); // healthy again afterwards
    await Promise.all([zb.close(), quick.close()]);
  });

  it("many PROCESSES opening one brand-new file at the same moment all succeed (the scenario that failed on Windows in Python)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "zb-multi-"));
    const path = join(dir, "fresh.db");
    writeFileSync(
      join(dir, "img.png"),
      await makeImage("png", 16, 16, { seed: 3 }),
    );
    writeFileSync(
      join(dir, "child.mts"),
      `import { ZeroBucket } from ${JSON.stringify(join(ROOT, "src/index.ts"))};
       const zb = new ZeroBucket({ sqlite: ${JSON.stringify(path)} });
       const id = await zb.put(${JSON.stringify(join(dir, "img.png"))});
       console.log("OK " + id); await zb.close();`,
    );
    const N = 8;
    const runs = Array.from(
      { length: N },
      () =>
        new Promise<{ code: number | null; out: string; err: string }>(
          (resolve) => {
            const c = spawn(
              process.execPath,
              ["--import", "tsx", join(dir, "child.mts")],
              { cwd: ROOT },
            );
            let out = "",
              err = "";
            c.stdout.on("data", (d) => (out += d));
            c.stderr.on("data", (d) => (err += d));
            c.on("exit", (code) => resolve({ code, out, err }));
          },
        ),
    );
    const results = await Promise.all(runs);
    expect(results.map((r) => r.err.trim()).filter(Boolean)).toEqual([]);
    expect(results.every((r) => r.code === 0 && r.out.includes("OK "))).toBe(
      true,
    );
    const raw = new Database(path, { readonly: true });
    expect(
      (
        raw.prepare("SELECT count(*) AS n FROM zerobucket_images").get() as {
          n: number;
        }
      ).n,
    ).toBe(N);
    raw.close();
  }, 60_000);

  it("50 concurrent in-process operations all succeed (serialized, never interleaved inside a transaction)", async () => {
    const zb = new ZeroBucket({ sqlite: tmpDb() });
    const imgs = await Promise.all(
      Array.from({ length: 50 }, (_, i) =>
        makeImage("png", 8 + (i % 7), 8, { seed: i + 1 }),
      ),
    );
    const ids = await Promise.all(imgs.map((d) => zb.put(d)));
    expect(new Set(ids).size).toBe(50);
    const got = await Promise.all(ids.map((i) => zb.get(i)));
    got.forEach((g, i) => expect(g.data.equals(imgs[i]!)).toBe(true));
    await Promise.all(ids.map((i) => zb.delete(i)));
    await zb.close();
  });
});

describe("SQLite: dedup mode", () => {
  let path: string;
  let zb: ZeroBucket;
  let raw: Database.Database;
  beforeEach(async () => {
    path = tmpDb();
    zb = new ZeroBucket({ sqlite: path, dedup: true });
    await zb.ready();
    raw = new Database(path);
  });
  const blob = (d: Buffer) =>
    raw
      .prepare(
        "SELECT ref_count FROM zerobucket_blobs WHERE checksum_sha256 = ?",
      )
      .get(sha(d)) as { ref_count: number } | undefined;
  const violations = () =>
    raw
      .prepare(
        `SELECT b.checksum_sha256 FROM zerobucket_blobs b LEFT JOIN zerobucket_image_refs r ON r.checksum_sha256 = b.checksum_sha256
                 GROUP BY b.checksum_sha256, b.ref_count HAVING b.ref_count <> count(r.id)`,
      )
      .all();
  const done = async () => (raw.close(), zb.close());

  it("stores identical bytes once; each put keeps its own id and filename; reads/streams/ranges work", async () => {
    const data = await makeImage("png", 120, 120, { seed: 1 });
    const [a, b] = [
      await zb.put(data, { filename: "a.png" }),
      await zb.put(data, { filename: "b.png" }),
    ];
    expect(a).not.toBe(b);
    expect(
      (raw.prepare("SELECT count(*) AS n FROM zerobucket_blobs").get() as any)
        .n,
    ).toBe(1);
    expect(blob(data)!.ref_count).toBe(2);
    expect((await zb.get(b)).filename).toBe("b.png");
    expect(
      (await collect(await zb.getStream(a, { chunkSize: 5000 }))).equals(data),
    ).toBe(true);
    expect(
      (
        await collect(await zb.getStream(b, { range: { start: 10, end: 19 } }))
      ).equals(data.subarray(10, 20)),
    ).toBe(true);
    const many = await zb.getMany([b, a]);
    expect(many.every((m) => m.success)).toBe(true);
    await done();
  });

  it("deleting references decrements exactly; the last one removes the blob", async () => {
    const data = await makeImage("png", 50, 50, { seed: 2 });
    const ids = [
      await zb.put(data),
      await zb.put(data),
      await zb.put(data),
      await zb.put(data),
    ];
    await zb.delete(ids[0]!);
    expect(blob(data)!.ref_count).toBe(3);
    const res = await zb.deleteMany([ids[1]!, ids[2]!, ids[1]!, randomUUID()]);
    expect(
      new Set(res.filter((r) => r.deleted).map((r) => r.imageId)).size,
    ).toBe(2); // a repeated id reports deleted at both positions
    expect(blob(data)!.ref_count).toBe(1);
    expect((await zb.get(ids[3]!)).data.equals(data)).toBe(true);
    await zb.delete(ids[3]!);
    expect(blob(data)).toBeUndefined();
    expect(await zb.delete(ids[3]!)).toBe(false);
    expect(violations()).toEqual([]);
    await done();
  });

  it("putMany accumulates identical images in one batch; mixes with existing blobs", async () => {
    const a = await makeImage("png", 40, 40, { seed: 10 });
    const b = await makeImage("png", 41, 41, { seed: 11 });
    await zb.put(a);
    const res = await zb.putMany([a, b, a, a], {
      filenames: ["1", "2", "3", "4"],
    });
    expect(res.every((r) => r.success)).toBe(true);
    expect(blob(a)!.ref_count).toBe(4);
    expect(blob(b)!.ref_count).toBe(1);
    expect((await zb.get(res[2]!.imageId!)).filename).toBe("3");
    expect(violations()).toEqual([]);
    await done();
  });

  it("20 concurrent puts of the same bytes give ref_count == 20; concurrent put/delete races never corrupt counts", async () => {
    const data = await makeImage("png", 60, 60, { seed: 20 });
    const ids = await Promise.all(
      Array.from({ length: 20 }, () => zb.put(data)),
    );
    expect(blob(data)!.ref_count).toBe(20);
    await Promise.all(ids.slice(0, 10).map((i) => zb.delete(i)));
    expect(blob(data)!.ref_count).toBe(10);
    for (let i = 0; i < 15; i++) {
      const d = await makeImage("png", 30, 30, { seed: 300 + i });
      const first = await zb.put(d);
      const [, p1, p2] = await Promise.all([
        zb.delete(first),
        zb.put(d),
        zb.put(d),
      ]);
      expect(blob(d)!.ref_count).toBe(2);
      expect(
        (await zb.get(p1)).data.equals(d) && (await zb.get(p2)).data.equals(d),
      ).toBe(true);
    }
    expect(violations()).toEqual([]);
    await done();
  });

  it("atomic on an autocommit caller handle: a failure after the blob upsert leaves no inflated ref_count", async () => {
    const data = await makeImage("png", 25, 25, { seed: 30 });
    const mine = new Database(path);
    const failing = {
      exec: (s: string) => mine.exec(s),
      prepare: (sql: string) => {
        if (/INSERT INTO zerobucket_image_refs/.test(sql))
          throw new Error("injected failure on ref insert");
        return mine.prepare(sql) as any;
      },
    };
    await expect(zb.put(data, { connection: failing })).rejects.toThrow(
      /injected/,
    );
    expect(blob(data)).toBeUndefined();
    mine.close();
    await done();
  });

  it("refuses tiering, objectStorage, and classic->dedup migration (not supported on SQLite)", async () => {
    const id = await zb.put(await makeImage("png", 20, 20, { seed: 40 }));
    await expect(zb.tierToObjectStorage(id)).rejects.toThrow(/dedup/);
    expect(
      () =>
        new ZeroBucket({
          sqlite: tmpDb(),
          dedup: true,
          objectStorage: new ObjectStorage({ bucket: "b" }),
        }),
    ).toThrow(TypeError);
    await expect(zb.migrateClassicToDedup()).rejects.toThrow(
      /does not support/,
    );
    await done();
  });
});

describe.skipIf(!ENDPOINT)(
  "SQLite: object-storage tiering (real S3 API)",
  () => {
    const bucket = `zb-sqlite-${Date.now()}`;
    const creds = { accessKeyId: "test", secretAccessKey: "test" };
    const ranges: (string | undefined)[] = [];
    let s3: S3Client;
    let real: ObjectStorage;
    let path: string;
    let zb: ZeroBucket;
    let raw: Database.Database;

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
    const row = (id: string) =>
      raw
        .prepare("SELECT * FROM zerobucket_images WHERE id = ?")
        .get(id) as any;
    const fresh = async (w = 200, h = 200, seed = 1) => {
      const data = await makeImage("png", w, h, { seed });
      return { data, id: await zb.put(data, { filename: "t.png" }) };
    };
    const wrapStorage = (
      over: Partial<ObjectStorageLike>,
    ): ObjectStorageLike => ({
      bucket: real.bucket,
      upload: over.upload ?? real.upload.bind(real),
      download: over.download ?? real.download.bind(real),
      downloadRange: over.downloadRange ?? real.downloadRange.bind(real),
      delete: over.delete ?? real.delete.bind(real),
      exists: real.exists.bind(real),
    });

    beforeAll(async () => {
      s3 = new S3Client({
        endpoint: ENDPOINT,
        region: "us-east-1",
        forcePathStyle: true,
        credentials: creds,
      });
      await s3.send(new CreateBucketCommand({ Bucket: bucket }));
      const spyClient = new S3Client({
        endpoint: ENDPOINT,
        region: "us-east-1",
        forcePathStyle: true,
        credentials: creds,
      });
      spyClient.middlewareStack.add(
        (next, ctx) => async (args: any) => (
          ctx.commandName === "GetObjectCommand" &&
            ranges.push(args.input.Range),
          next(args)
        ),
        { step: "initialize", name: "rangeSpy" },
      );
      real = new ObjectStorage({ bucket, client: spyClient });
      path = tmpDb();
      zb = new ZeroBucket({ sqlite: path, objectStorage: real });
      await zb.ready();
      raw = new Database(path);
    });
    afterAll(async () => {
      raw.close();
      await zb.close();
    });

    it("tiers an image: row flips, bytes move, every read path stays transparent", async () => {
      const { data, id } = await fresh();
      expect(await zb.tierToObjectStorage(id)).toBe(true);
      const r = row(id);
      expect(r).toMatchObject({
        storage_backend: "object_storage",
        data: null,
        object_storage_bucket: bucket,
        object_storage_key: id,
      });
      expect((await object(id)).equals(data)).toBe(true);
      expect((await zb.get(id)).data.equals(data)).toBe(true);
      expect((await zb.getMany([id]))[0]!.success).toBe(true);
      ranges.length = 0;
      expect(
        (await collect(await zb.getStream(id, { chunkSize: 50_000 }))).equals(
          data,
        ),
      ).toBe(true);
      expect(ranges.length).toBe(Math.ceil(data.length / 50_000));
      expect(ranges.every((x) => /^bytes=\d+-\d+$/.test(x ?? ""))).toBe(true);
      const part = await createImageHandler(zb)(
        new Request(`http://x/i/${id}`, { headers: { Range: "bytes=4-13" } }),
      );
      expect(part.status).toBe(206);
      expect(
        Buffer.from(await part.arrayBuffer()).equals(data.subarray(4, 14)),
      ).toBe(true);
    });

    it("is idempotent; missing/malformed ids are ImageNotFoundError; no objectStorage fails fast; connection= is refused", async () => {
      const { id } = await fresh(30, 30, 5);
      expect(await zb.tierToObjectStorage(id)).toBe(true);
      expect(await zb.tierToObjectStorage(id)).toBe(false);
      await expect(zb.tierToObjectStorage(randomUUID())).rejects.toBeInstanceOf(
        ImageNotFoundError,
      );
      await expect(zb.tierToObjectStorage("nope")).rejects.toBeInstanceOf(
        ImageNotFoundError,
      );
      const bare = new ZeroBucket({ sqlite: path });
      await expect(bare.tierToObjectStorage(id)).rejects.toThrow(
        /objectStorage/,
      );
      await expect(bare.get(id)).rejects.toBeInstanceOf(StorageError); // tiered, but no object storage configured
      await bare.close();
      await expect(
        zb.tierToObjectStorage(id, { connection: raw }),
      ).rejects.toThrow(/does not accept `connection`/);
    });

    it("a FAILED UPLOAD rolls back: the row is byte-for-byte untouched and still readable", async () => {
      const { data, id } = await fresh(80, 80, 6);
      const before = row(id);
      const broken = new ZeroBucket({
        sqlite: path,
        objectStorage: wrapStorage({
          upload: async () => {
            throw new StorageError("simulated S3 outage");
          },
        }),
      });
      await expect(broken.tierToObjectStorage(id)).rejects.toThrow(
        /simulated S3 outage/,
      );
      expect(row(id)).toEqual(before);
      expect(row(id).storage_backend).toBe("sqlite");
      expect(await objectExists(id)).toBe(false);
      expect((await zb.get(id)).data.equals(data)).toBe(true);
      // and the database is not left locked by the failed attempt
      expect(await zb.put(await makeImage("png", 12, 13))).toMatch(
        /^[0-9a-f-]{36}$/,
      );
      await broken.close();
    });

    it("locks the WHOLE database during the upload: other processes' writes are refused, this process queues, reads are never blocked", async () => {
      const a = await fresh(60, 60, 61);
      const b = await fresh(61, 61, 62);
      let release!: () => void;
      let entered!: () => void;
      const gate = new Promise<void>((r) => (release = r));
      const inUpload = new Promise<void>((r) => (entered = r));
      const slow = new ZeroBucket({
        sqlite: path,
        objectStorage: wrapStorage({
          upload: async (k, d, o) => (
            entered(),
            await gate,
            real.upload(k, d, o)
          ),
        }),
      });
      const tiering = slow.tierToObjectStorage(a.id);
      await inUpload;

      // another connection/process cannot write anywhere in the file (coarser than Postgres: even a different row)
      const other = new Database(path, { timeout: 0 });
      expect(() =>
        other.prepare("DELETE FROM zerobucket_images WHERE id = ?").run(b.id),
      ).toThrowError(/locked|busy/i);
      // reads are not blocked (WAL)
      expect(
        (
          other
            .prepare("SELECT count(*) AS n FROM zerobucket_images")
            .get() as any
        ).n,
      ).toBeGreaterThan(0);
      expect((await zb.get(b.id)).data.equals(b.data)).toBe(true);
      other.close();

      // an in-process write queues behind the tiering, then completes (it does not fail or freeze the loop)
      let finished = false;
      const queued = zb.delete(b.id).then((r) => ((finished = true), r));
      await new Promise((r) => setTimeout(r, 250));
      expect(finished).toBe(false);

      release();
      expect(await tiering).toBe(true);
      expect(await queued).toBe(true);
      await slow.close();
    });

    it("delete removes the object; failures are reported not thrown; never inside a caller transaction", async () => {
      const { id } = await fresh(40, 40, 71);
      await zb.tierToObjectStorage(id);
      expect(await objectExists(id)).toBe(true);
      expect(await zb.delete(id)).toBe(true);
      expect(await objectExists(id)).toBe(false);

      const x = await fresh(41, 41, 72);
      const z = await fresh(43, 43, 74);
      await zb.tierToObjectStorage(x.id);
      await zb.tierToObjectStorage(z.id);
      expect((await zb.deleteMany([x.id, z.id])).every((r) => r.deleted)).toBe(
        true,
      );
      expect([await objectExists(x.id), await objectExists(z.id)]).toEqual([
        false,
        false,
      ]);

      const f = await fresh(44, 44, 75);
      await zb.tierToObjectStorage(f.id);
      const evs: OperationEvent[] = [];
      const flaky = new ZeroBucket({
        sqlite: path,
        objectStorage: wrapStorage({
          delete: async () => {
            throw new StorageError("simulated delete failure");
          },
        }),
        onOperation: (e) => evs.push(e),
      });
      expect(await flaky.delete(f.id)).toBe(true);
      expect(
        evs.find((e) => e.operation === "object_storage_delete"),
      ).toMatchObject({ success: false });
      await flaky.close();
      await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: f.id }));

      const keep = await fresh(45, 45, 76);
      await zb.tierToObjectStorage(keep.id);
      const mine = new Database(path);
      mine.exec("BEGIN");
      expect(await zb.delete(keep.id, { connection: mine })).toBe(true);
      expect(await objectExists(keep.id)).toBe(true); // untouched while the outcome is unknown
      mine.exec("ROLLBACK");
      mine.close();
      expect((await zb.get(keep.id)).data.equals(keep.data)).toBe(true); // rollback lost nothing
    });

    it("detects a tampered or missing object instead of returning wrong data or truncating", async () => {
      const { id } = await fresh(150, 150, 83);
      const size = (await zb.metadata(id)).sizeBytes;
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
    });
  },
);
