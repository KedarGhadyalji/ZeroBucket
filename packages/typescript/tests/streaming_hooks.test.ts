import { createHash } from "node:crypto";
import { Writable } from "node:stream";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  AccessDeniedError,
  ImageNotFoundError,
  PostgresBackend,
  StorageError,
  type StorageBackend,
  type ZeroBucket,
} from "../src/index.js";
import { DATABASE_URL, makeImage, newBucket } from "./helpers.js";

const sha = (b: Buffer) => createHash("sha256").update(b).digest("hex");
const collect = async (it: AsyncIterable<Buffer>) => {
  const parts: Buffer[] = [];
  for await (const c of it) parts.push(c);
  return Buffer.concat(parts);
};

describe.skipIf(!DATABASE_URL)("streaming", () => {
  let zb: ZeroBucket;
  let raw: pg.Pool;
  let data: Buffer;
  let id: string;
  beforeAll(async () => {
    zb = newBucket();
    raw = new pg.Pool({ connectionString: DATABASE_URL });
    data = await makeImage("png", 300, 300); // ~270KB of noise
    id = await zb.put(data);
  });
  afterAll(async () => {
    await zb.close();
    await raw.end();
  });

  it("streams the exact bytes at any chunk size, and chunks are bounded", async () => {
    for (const chunkSize of [16384, 65536, data.length, data.length * 3]) {
      const sizes: number[] = [];
      const parts: Buffer[] = [];
      for await (const c of await zb.getStream(id, { chunkSize })) {
        sizes.push(c.length);
        parts.push(c);
      }
      expect(Buffer.concat(parts).equals(data)).toBe(true);
      expect(Math.max(...sizes)).toBeLessThanOrEqual(chunkSize);
      expect(sizes.length).toBe(Math.ceil(data.length / chunkSize));
    }
  });

  it("works for a tiny object with 1-byte chunks", async () => {
    const tiny = await zb.put(await makeImage("png", 2, 2));
    const whole = (await zb.get(tiny)).data;
    expect(
      (await collect(await zb.getStream(tiny, { chunkSize: 1 }))).equals(whole),
    ).toBe(true);
  });

  it("rejects invalid chunk sizes and ranges", async () => {
    await expect(zb.getStream(id, { chunkSize: 0 })).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(
      zb.getStream(id, { range: { start: -1 } }),
    ).rejects.toBeInstanceOf(RangeError);
    await expect(
      zb.getStream(id, { range: { start: 10, end: 5 } }),
    ).rejects.toBeInstanceOf(RangeError);
  });

  it("serves byte ranges (inclusive, clamped) straight from the database", async () => {
    expect(
      (
        await collect(await zb.getStream(id, { range: { start: 0, end: 99 } }))
      ).equals(data.subarray(0, 100)),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, {
            range: { start: 1000, end: 1999 },
            chunkSize: 300,
          }),
        )
      ).equals(data.subarray(1000, 2000)),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, { range: { start: data.length - 10 } }),
        )
      ).equals(data.subarray(data.length - 10)),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, { range: { start: 5, end: 10_000_000 } }),
        )
      ).equals(data.subarray(5)),
    ).toBe(true);
    expect(
      (
        await collect(
          await zb.getStream(id, { range: { start: data.length + 5 } }),
        )
      ).length,
    ).toBe(0);
  });

  it("streamTo writes everything and honours backpressure", async () => {
    const received: Buffer[] = [];
    let highWaterHits = 0;
    const slow = new Writable({
      highWaterMark: 1024, // tiny, so write() returns false constantly
      write(chunk, _enc, cb) {
        received.push(chunk);
        setImmediate(cb);
      },
    });
    const origWrite = slow.write.bind(slow);
    slow.write = ((...args: Parameters<typeof origWrite>) => {
      const ok = origWrite(...args);
      if (!ok) highWaterHits++;
      return ok;
    }) as typeof slow.write;
    const n = await zb.streamTo(id, slow, { chunkSize: 8192 });
    expect(n).toBe(data.length);
    expect(Buffer.concat(received).equals(data)).toBe(true);
    expect(highWaterHits).toBeGreaterThan(0); // backpressure really was exercised
  });

  it("toWebStream feeds a standard Response, and cancel stops reading", async () => {
    const res = new Response(await zb.toWebStream(id, { chunkSize: 5000 }));
    expect(Buffer.from(await res.arrayBuffer()).equals(data)).toBe(true);

    const events: string[] = [];
    const zb2 = newBucket({ onOperation: (e) => events.push(e.operation) });
    const stream = await zb2.toWebStream(id, { chunkSize: 1000 });
    const reader = stream.getReader();
    await reader.read();
    await reader.cancel();
    const before = events.filter((e) => e === "get_stream").length;
    await new Promise((r) => setTimeout(r, 50));
    expect(events.filter((e) => e === "get_stream").length).toBe(before); // no further chunk fetches
    await zb2.close();
  });

  it("not-found is eager: rejects on await, before any iteration", async () => {
    await expect(
      zb.getStream("00000000-0000-4000-8000-0000000000aa"),
    ).rejects.toBeInstanceOf(ImageNotFoundError);
  });

  it("a delete mid-stream raises loudly instead of silently truncating", async () => {
    const victim = await zb.put(await makeImage("png", 200, 200, { seed: 99 }));
    const stream = await zb.getStream(victim, { chunkSize: 1000 });
    const it = stream[Symbol.asyncIterator]();
    await it.next();
    await zb.delete(victim);
    await expect(it.next()).rejects.toBeInstanceOf(StorageError);
    // A JS generator that has thrown is finished: it must not yield any further data afterwards.
    expect(await it.next()).toEqual({ value: undefined, done: true });
  });

  it("a caller-owned transaction gives a consistent read across a concurrent delete", async () => {
    const victim = await zb.put(await makeImage("png", 200, 200, { seed: 7 }));
    const whole = (await zb.get(victim)).data;
    const client = await raw.connect();
    try {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
      const it = (
        await zb.getStream(victim, { chunkSize: 1000, connection: client })
      )[Symbol.asyncIterator]();
      const first = await it.next(); // snapshot taken
      await zb.delete(victim); // concurrent writer
      const parts = [first.value as Buffer];
      for (let r = await it.next(); !r.done; r = await it.next())
        parts.push(r.value);
      expect(Buffer.concat(parts).equals(whole)).toBe(true);
      await client.query("COMMIT");
    } finally {
      client.release();
    }
  });

  it("emits one event per chunk plus the info lookup", async () => {
    const events: string[] = [];
    const zb2 = newBucket({ onOperation: (e) => events.push(e.operation) });
    const small = await zb2.put(await makeImage("png", 40, 40));
    events.length = 0;
    const size = (await zb2.metadata(small)).sizeBytes;
    events.length = 0;
    await collect(
      await zb2.getStream(small, { chunkSize: Math.ceil(size / 3) }),
    );
    expect(events).toEqual([
      "get_metadata",
      "get_stream",
      "get_stream",
      "get_stream",
    ]);
    await zb2.close();
  });
});

describe.skipIf(!DATABASE_URL)("access-control hooks", () => {
  type Ctx = { user: string };
  const owners = new Map<string, string>();
  let zb: ZeroBucket<Ctx>;
  let aliceImg: string;
  let bobImg: string;
  beforeAll(async () => {
    zb = newBucket<Ctx>({
      beforeGet: (id, ctx) => owners.get(id) === ctx?.user,
      beforePut: (ctx) => ctx?.user !== "banned",
    });
    aliceImg = await zb.put(await makeImage("png", 9, 9), {
      context: { user: "alice" },
    });
    bobImg = await zb.put(await makeImage("png", 9, 10), {
      context: { user: "bob" },
    });
    owners.set(aliceImg, "alice");
    owners.set(bobImg, "bob");
  });
  afterAll(() => zb.close());

  it("allows the owner and denies others across get/metadata/getStream/streamTo/toWebStream", async () => {
    const alice = { context: { user: "alice" } };
    const bob = { context: { user: "bob" } };
    expect((await zb.get(aliceImg, alice)).width).toBe(9);
    expect((await zb.metadata(aliceImg, alice)).width).toBe(9);
    await expect(zb.get(aliceImg, bob)).rejects.toBeInstanceOf(
      AccessDeniedError,
    );
    await expect(zb.metadata(aliceImg, bob)).rejects.toBeInstanceOf(
      AccessDeniedError,
    );
    await expect(zb.getStream(aliceImg, bob)).rejects.toBeInstanceOf(
      AccessDeniedError,
    );
    await expect(zb.toWebStream(aliceImg, bob)).rejects.toBeInstanceOf(
      AccessDeniedError,
    );
    await expect(
      zb.streamTo(aliceImg, { write: () => true }, bob),
    ).rejects.toBeInstanceOf(AccessDeniedError);
    await expect(zb.get(aliceImg)).rejects.toBeInstanceOf(AccessDeniedError); // no context at all
  });

  it("getMany gates per id without aborting the batch", async () => {
    const res = await zb.getMany([aliceImg, bobImg], {
      context: { user: "alice" },
    });
    expect(res[0]!.success).toBe(true);
    expect(res[1]).toMatchObject({
      success: false,
      error: "access denied",
      image: null,
    });
  });

  it("putMany evaluates beforePut ONCE and denies the whole batch", async () => {
    const hook = vi.fn((ctx?: Ctx) => ctx?.user !== "banned");
    const g = newBucket<Ctx>({ beforePut: hook });
    const png = await makeImage("png", 5, 5);
    const denied = await g.putMany([png, png, png], {
      context: { user: "banned" },
    });
    expect(denied.map((r) => r.error)).toEqual([
      "access denied",
      "access denied",
      "access denied",
    ]);
    expect(hook).toHaveBeenCalledTimes(1);
    await g.close();
    await expect(
      zb.put(png, { context: { user: "banned" } }),
    ).rejects.toBeInstanceOf(AccessDeniedError);
  });

  it("exists() is deliberately not gated", async () => {
    expect(await zb.exists(aliceImg)).toBe(true);
  });

  it("FAILS CLOSED: a throwing hook propagates and is never treated as allow", async () => {
    const boom = new Error("auth service down");
    const g = newBucket({
      beforeGet: () => {
        throw boom;
      },
      beforePut: () => {
        throw boom;
      },
    });
    const png = await makeImage("png", 5, 5);
    await expect(g.get(aliceImg)).rejects.toBe(boom); // the ORIGINAL error, not wrapped
    await expect(g.put(png)).rejects.toBe(boom);
    const many = await g.getMany([aliceImg]);
    expect(many[0]).toMatchObject({
      success: false,
      error: "auth service down",
    });
    const put = await g.putMany([png]);
    expect(put[0]).toMatchObject({
      success: false,
      error: "auth service down",
    });
    await g.close();
  });

  it("async hooks work", async () => {
    const g = newBucket({
      beforeGet: async () => (await Promise.resolve(), false),
    });
    await expect(g.get(aliceImg)).rejects.toBeInstanceOf(AccessDeniedError);
    await g.close();
  });

  it("denied calls NEVER reach the storage layer", async () => {
    const calls: string[] = [];
    const spy = new Proxy(
      new PostgresBackend({ connectionString: DATABASE_URL }),
      {
        get(target, prop, recv) {
          const v = Reflect.get(target, prop, recv);
          return typeof v === "function" && prop !== "ready" && prop !== "close"
            ? (...args: unknown[]) => (
                calls.push(String(prop)),
                v.apply(target, args)
              )
            : typeof v === "function"
              ? v.bind(target)
              : v;
        },
      },
    ) as unknown as StorageBackend;
    const g = newBucket({
      backend: spy,
      beforeGet: () => false,
      beforePut: () => false,
    });
    const png = await makeImage("png", 5, 5);
    await g.get(aliceImg).catch(() => {});
    await g.metadata(aliceImg).catch(() => {});
    await g.getStream(aliceImg).catch(() => {});
    await g.put(png).catch(() => {});
    await g.putMany([png]);
    await g.getMany([aliceImg]);
    expect(calls).toEqual([]);
    await g.close();
  });
});
