import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { ZeroBucket } from "../src/index.js";
import {
  createImageHandler,
  etagMatches,
  parseRange,
  toNodeHandler,
} from "../src/http.js";
import { DATABASE_URL, makeImage, newBucket } from "./helpers.js";

describe("parseRange (pure)", () => {
  const size = 1000;
  it.each([
    ["bytes=0-499", { start: 0, end: 499 }],
    ["bytes=500-", { start: 500, end: 999 }],
    ["bytes=-100", { start: 900, end: 999 }],
    ["bytes=-5000", { start: 0, end: 999 }], // suffix larger than the file -> whole file
    ["bytes=990-5000", { start: 990, end: 999 }], // end clamped
    ["BYTES=0-0", { start: 0, end: 0 }],
    ["bytes=1000-", "unsatisfiable"],
    ["bytes=5000-6000", "unsatisfiable"],
    ["bytes=-0", "unsatisfiable"],
    ["bytes=0-1,5-6", null], // multi-range: ignored, full body served
    ["bytes=5-2", null], // invalid: ignored
    ["bytes=-", null],
    ["items=0-5", null],
    ["garbage", null],
  ])("%s", (header, expected) => {
    expect(parseRange(header, size)).toEqual(expected);
  });
});

describe("etagMatches (pure)", () => {
  it("handles lists, weak validators and *", () => {
    expect(etagMatches('"a"', '"a"')).toBe(true);
    expect(etagMatches('W/"a"', '"a"')).toBe(true);
    expect(etagMatches('"x", "a"', '"a"')).toBe(true);
    expect(etagMatches("*", '"a"')).toBe(true);
    expect(etagMatches('"b"', '"a"')).toBe(false);
    expect(etagMatches(null, '"a"')).toBe(false);
  });
});

describe.skipIf(!DATABASE_URL)(
  "createImageHandler against real Postgres",
  () => {
    type Ctx = { user: string };
    const owners = new Map<string, string>();
    let zb: ZeroBucket<Ctx>;
    let id: string;
    let data: Buffer;
    const url = (i = id) => `http://x.test/images/${i}`;

    beforeAll(async () => {
      zb = newBucket<Ctx>({
        beforeGet: (i, ctx) => !owners.has(i) || owners.get(i) === ctx?.user,
      });
      data = await makeImage("png", 200, 200);
      id = await zb.put(data, { filename: "pic ü.png" });
    });
    afterAll(() => zb.close());

    const handler = () => createImageHandler(zb);

    it("200: full body, correct headers, content verified byte-for-byte", async () => {
      const res = await handler()(new Request(url()));
      expect(res.status).toBe(200);
      expect(res.headers.get("content-type")).toBe("image/png");
      expect(res.headers.get("content-length")).toBe(String(data.length));
      expect(res.headers.get("accept-ranges")).toBe("bytes");
      expect(res.headers.get("x-content-type-options")).toBe("nosniff");
      expect(res.headers.get("cache-control")).toBe("private, no-cache");
      expect(res.headers.get("etag")).toMatch(/^"[0-9a-f]{64}"$/);
      expect(Buffer.from(await res.arrayBuffer()).equals(data)).toBe(true);
    });

    it("304 when If-None-Match matches, and no body is read", async () => {
      const etag = (
        await handler()(new Request(url(), { method: "HEAD" }))
      ).headers.get("etag")!;
      const res = await handler()(
        new Request(url(), { headers: { "If-None-Match": etag } }),
      );
      expect(res.status).toBe(304);
      expect(res.body).toBeNull();
      expect(
        (
          await handler()(
            new Request(url(), { headers: { "If-None-Match": '"nope"' } }),
          )
        ).status,
      ).toBe(200);
    });

    it("HEAD returns headers and no body", async () => {
      const res = await handler()(new Request(url(), { method: "HEAD" }));
      expect(res.status).toBe(200);
      expect(res.headers.get("content-length")).toBe(String(data.length));
      expect(res.body).toBeNull();
    });

    it("206: serves exactly the requested bytes with a correct Content-Range", async () => {
      const res = await handler()(
        new Request(url(), { headers: { Range: "bytes=100-299" } }),
      );
      expect(res.status).toBe(206);
      expect(res.headers.get("content-range")).toBe(
        `bytes 100-299/${data.length}`,
      );
      expect(res.headers.get("content-length")).toBe("200");
      expect(
        Buffer.from(await res.arrayBuffer()).equals(data.subarray(100, 300)),
      ).toBe(true);

      const suffix = await handler()(
        new Request(url(), { headers: { Range: "bytes=-50" } }),
      );
      expect(suffix.status).toBe(206);
      expect(
        Buffer.from(await suffix.arrayBuffer()).equals(
          data.subarray(data.length - 50),
        ),
      ).toBe(true);

      const open = await handler()(
        new Request(url(), { headers: { Range: `bytes=${data.length - 7}-` } }),
      );
      expect(
        Buffer.from(await open.arrayBuffer()).equals(
          data.subarray(data.length - 7),
        ),
      ).toBe(true);
    });

    it("416 for an unsatisfiable range, with Content-Range: bytes */size", async () => {
      const res = await handler()(
        new Request(url(), {
          headers: { Range: `bytes=${data.length + 10}-` },
        }),
      );
      expect(res.status).toBe(416);
      expect(res.headers.get("content-range")).toBe(`bytes */${data.length}`);
    });

    it("ignores multi-range and invalid Range headers (full 200)", async () => {
      for (const r of ["bytes=0-1,5-6", "bytes=9-3", "nonsense"]) {
        const res = await handler()(
          new Request(url(), { headers: { Range: r } }),
        );
        expect(res.status).toBe(200);
        expect(Buffer.from(await res.arrayBuffer()).equals(data)).toBe(true);
      }
    });

    it("If-Range: matching ETag honours the range; stale ETag falls back to the full body", async () => {
      const etag = (
        await handler()(new Request(url(), { method: "HEAD" }))
      ).headers.get("etag")!;
      const ok = await handler()(
        new Request(url(), {
          headers: { Range: "bytes=0-9", "If-Range": etag },
        }),
      );
      expect(ok.status).toBe(206);
      const stale = await handler()(
        new Request(url(), {
          headers: { Range: "bytes=0-9", "If-Range": '"stale"' },
        }),
      );
      expect(stale.status).toBe(200);
      expect(Buffer.from(await stale.arrayBuffer()).equals(data)).toBe(true);
    });

    it("404 for unknown, malformed, and missing ids; 405 for other methods", async () => {
      expect(
        (
          await handler()(
            new Request(url("00000000-0000-4000-8000-0000000000cc")),
          )
        ).status,
      ).toBe(404);
      expect((await handler()(new Request(url("not-a-uuid")))).status).toBe(
        404,
      );
      expect((await handler()(new Request("http://x.test/"))).status).toBe(404);
      const post = await handler()(new Request(url(), { method: "POST" }));
      expect(post.status).toBe(405);
      expect(post.headers.get("allow")).toBe("GET, HEAD");
    });

    it("ctx.id overrides URL parsing (the Next.js `params` pattern)", async () => {
      const res = await handler()(
        new Request("http://x.test/anything/at/all"),
        { id },
      );
      expect(res.status).toBe(200);
    });

    it("403 when beforeGet denies, driven by getContext; no bytes leak", async () => {
      owners.set(id, "alice");
      const h = createImageHandler(zb, {
        getContext: (req) => ({ user: req.headers.get("x-user") ?? "anon" }),
      });
      expect(
        (await h(new Request(url(), { headers: { "x-user": "mallory" } })))
          .status,
      ).toBe(403);
      expect(
        (
          await h(
            new Request(url(), {
              headers: { "x-user": "mallory", Range: "bytes=0-9" },
            }),
          )
        ).status,
      ).toBe(403);
      expect(
        (
          await h(
            new Request(url(), {
              method: "HEAD",
              headers: { "x-user": "mallory" },
            }),
          )
        ).status,
      ).toBe(403);
      expect(
        (await h(new Request(url(), { headers: { "x-user": "alice" } })))
          .status,
      ).toBe(200);
      owners.delete(id);
    });

    it("500 is generic and never leaks internals; onError receives the real cause", async () => {
      const seen: unknown[] = [];
      const boom = new Error("secret internal detail: db host 10.0.0.5");
      const h = createImageHandler(zb, {
        getContext: () => {
          throw boom;
        },
        onError: (e) => seen.push(e),
      });
      const res = await h(new Request(url()));
      expect(res.status).toBe(500);
      expect(await res.text()).toBe("Internal Server Error");
      expect(seen).toEqual([boom]);
    });

    it("configurable Cache-Control and attachment disposition (non-ASCII filename safe)", async () => {
      const h = createImageHandler(zb, {
        cacheControl: "public, max-age=31536000, immutable",
        disposition: "attachment",
      });
      const res = await h(new Request(url()));
      expect(res.headers.get("cache-control")).toBe(
        "public, max-age=31536000, immutable",
      );
      const cd = res.headers.get("content-disposition")!;
      expect(cd).toContain('filename="pic _.png"');
      expect(cd).toContain("filename*=UTF-8''pic%20%C3%BC.png");
    });

    describe("Node adapter over a real socket", () => {
      let server: Server;
      let base: string;
      beforeAll(async () => {
        const node = toNodeHandler(createImageHandler(zb));
        server = createServer((req, res) => {
          // Express-style: id comes from the route, not the URL parser
          const m = /^\/img\/([^/?]+)/.exec(req.url ?? "");
          node(req, res, m ? { id: m[1] } : undefined).catch(() =>
            res.destroy(),
          );
        });
        await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
        base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
      });
      afterAll(() => new Promise<void>((r) => server.close(() => r())));

      it("serves full, ranged, conditional and HEAD requests", async () => {
        const full = await fetch(`${base}/img/${id}`);
        expect(full.status).toBe(200);
        expect(Buffer.from(await full.arrayBuffer()).equals(data)).toBe(true);

        const part = await fetch(`${base}/img/${id}`, {
          headers: { Range: "bytes=10-19" },
        });
        expect(part.status).toBe(206);
        expect(
          Buffer.from(await part.arrayBuffer()).equals(data.subarray(10, 20)),
        ).toBe(true);

        const etag = full.headers.get("etag")!;
        expect(
          (
            await fetch(`${base}/img/${id}`, {
              headers: { "If-None-Match": etag },
            })
          ).status,
        ).toBe(304);
        const head = await fetch(`${base}/img/${id}`, { method: "HEAD" });
        expect(head.status).toBe(200);
        expect(head.headers.get("content-length")).toBe(String(data.length));
        expect(
          (await fetch(`${base}/img/00000000-0000-4000-8000-0000000000dd`))
            .status,
        ).toBe(404);
      });

      it("a client that disconnects mid-download does not crash the server", async () => {
        const big = await zb.put(await makeImage("png", 800, 800, { seed: 5 }));
        const ac = new AbortController();
        const res = await fetch(`${base}/img/${big}`, { signal: ac.signal });
        const reader = res.body!.getReader();
        await reader.read();
        ac.abort();
        await new Promise((r) => setTimeout(r, 100));
        expect((await fetch(`${base}/img/${id}`)).status).toBe(200); // server still healthy
      });
    });
  },
);
