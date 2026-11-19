/**
 * Does the BUILT package (dist/) work on this JavaScript runtime?
 *
 *   node tests/runtimes/check.mjs
 *   bun  tests/runtimes/check.mjs
 *   deno run -A tests/runtimes/check.mjs
 *
 * (or `npm run test:runtimes`, which runs it on every runtime you have installed).
 *
 * Plain JavaScript on purpose: no vitest, no tsx, no TypeScript loader, so the only thing running is the package as users
 * get it. Exits 0 only if every check passes. SQLite, validation, optimize, HEIC and the HTTP handler always run;
 * PostgreSQL, MySQL/MariaDB and S3 run when their ZEROBUCKET_TEST_* settings are in the environment (the same names the
 * test suite uses) and are reported as "skipped" otherwise.
 *
 * It does not call process.exit() when everything passes: a runtime that keeps the process alive after the work is done
 * (an unclosed pool, a HEIC worker thread) would then hang, and the runner (run.mjs) turns that hang into a failure.
 */
import { createRequire } from "node:module";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import sharp from "sharp";

const here = fileURLToPath(new URL(".", import.meta.url));
const dist = (f) => new URL(`../../dist/${f}`, import.meta.url).href;

const runtime = globalThis.Deno
  ? `Deno ${Deno.version.deno}`
  : globalThis.Bun
    ? `Bun ${Bun.version}`
    : `Node ${process.versions.node}`;
console.log(`runtime: ${runtime}`);

let failures = 0;
let passed = 0;
let skipped = 0;
async function check(name, fn) {
  try {
    await fn();
    passed++;
    console.log(`  ok    ${name}`);
  } catch (error) {
    failures++;
    console.log(
      `  FAIL  ${name}\n          ${String(error?.stack ?? error)
        .split("\n")
        .slice(0, 4)
        .join("\n          ")}`,
    );
  }
}
function skip(name, why) {
  skipped++;
  console.log(`  skip  ${name} (${why})`);
}
function assert(cond, message) {
  if (!cond) throw new Error(message ?? "assertion failed");
}
async function rejects(promise, ErrorClass, label) {
  try {
    await promise;
  } catch (error) {
    assert(
      error instanceof ErrorClass,
      `${label}: expected ${ErrorClass.name}, got ${error?.constructor?.name}: ${error?.message}`,
    );
    return error;
  }
  throw new Error(`${label}: expected a rejection`);
}
async function collect(stream) {
  const parts = [];
  for await (const chunk of stream) parts.push(Buffer.from(chunk));
  return Buffer.concat(parts);
}

/** A real, decodable image whose bytes do not compress to nothing. */
async function noisyImage(format = "png", width = 96, height = 64, seed = 7) {
  const raw = Buffer.alloc(width * height * 3);
  let s = seed >>> 0 || 1;
  for (let i = 0; i < raw.length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    raw[i] = (s >>> 24) & 0xff;
  }
  return sharp(raw, { raw: { width, height, channels: 3 } })
    [format]()
    .toBuffer();
}

const lib = await import(dist("index.js"));
const httpLib = await import(dist("http.js"));
const { ZeroBucket, ObjectStorage } = lib;
const tmp = mkdtempSync(join(tmpdir(), "zerobucket-runtime-"));
let n = 0;
const dbPath = () => join(tmp, `t${++n}.db`);

// ---- loading ---------------------------------------------------------------------------------------------------------
console.log("\nloading");
await check("ESM: index.js and http.js export the public API", () => {
  for (const name of [
    "ZeroBucket",
    "SQLiteBackend",
    "PostgresBackend",
    "MySQLBackend",
    "ObjectStorage",
    "validateImage",
    "optimizeImage",
  ]) {
    assert(typeof lib[name] === "function", `${name} is not exported`);
  }
  assert(
    typeof httpLib.createImageHandler === "function",
    "createImageHandler missing",
  );
  assert(typeof httpLib.toNodeHandler === "function", "toNodeHandler missing");
});
await check(
  "CommonJS: require() of index.cjs and http.cjs gives the same API",
  () => {
    const require = createRequire(import.meta.url);
    const cjs = require("../../dist/index.cjs");
    const cjsHttp = require("../../dist/http.cjs");
    assert(
      typeof cjs.ZeroBucket === "function" &&
        typeof cjs.SQLiteBackend === "function",
      "index.cjs API missing",
    );
    assert(
      typeof cjsHttp.createImageHandler === "function",
      "http.cjs API missing",
    );
    assert(
      cjs.ContentValidationError !== undefined,
      "errors missing from index.cjs",
    );
  },
);

// ---- SQLite ------------------------------------------------------------------------------------------------------------
console.log("\nSQLite (better-sqlite3)");
const events = [];
const zb = new ZeroBucket({
  sqlite: dbPath(),
  onOperation: (e) => events.push(e),
});
const png = await noisyImage("png");
let pngId;
await check("put, get, metadata, delete round trip", async () => {
  pngId = await zb.put(png, { filename: "a.png" });
  const got = await zb.get(pngId);
  assert(
    Buffer.compare(Buffer.from(got.data), png) === 0,
    "bytes differ after round trip",
  );
  assert(got.mimeType === "image/png", `mime ${got.mimeType}`);
  const meta = await zb.metadata(pngId);
  assert(
    meta.sizeBytes === png.length && meta.checksumSha256.length === 64,
    "metadata wrong",
  );
});
await check("getStream yields the exact bytes in several chunks", async () => {
  const chunks = [];
  for await (const c of await zb.getStream(pngId, { chunkSize: 1000 }))
    chunks.push(Buffer.from(c));
  assert(chunks.length > 1, "expected more than one chunk");
  assert(
    Buffer.compare(Buffer.concat(chunks), png) === 0,
    "streamed bytes differ",
  );
});
await check("onOperation reports events", () => {
  assert(
    events.length > 0 &&
      events.every(
        (e) =>
          typeof e.operation === "string" && typeof e.durationMs === "number",
      ),
    JSON.stringify(events[0]),
  );
});
await check(
  "putMany stores several images and reports a bad one without losing the rest",
  async () => {
    const res = await zb.putMany([
      await noisyImage("jpeg", 40, 40, 1),
      Buffer.from("not an image"),
      await noisyImage("webp", 40, 40, 2),
    ]);
    const ok = res.filter((r) => r.ok !== false && !r.error).length;
    assert(
      res.length === 3 && ok === 2,
      `results: ${JSON.stringify(res.map((r) => Object.keys(r)))}`,
    );
  },
);
await check(
  "delete removes the image and a later get is ImageNotFoundError",
  async () => {
    assert((await zb.delete(pngId)) === true, "delete returned false");
    await rejects(zb.get(pngId), lib.ImageNotFoundError, "get after delete");
  },
);
await check(
  "concurrent writes from one process all land (write mutex)",
  async () => {
    const ids = await Promise.all(
      Array.from({ length: 20 }, async (_, i) =>
        zb.put(await noisyImage("png", 32, 32, 100 + i)),
      ),
    );
    assert(new Set(ids).size === 20, "duplicate ids");
  },
);

// ---- validation, optimize, HEIC ----------------------------------------------------------------------------------------
console.log("\nvalidation (sharp), optimize, HEIC (libheif-js worker)");
await check(
  "rejects non-images, corrupted images, and unsupported formats with typed errors",
  async () => {
    await rejects(
      zb.put(Buffer.from("hello")),
      lib.ContentValidationError,
      "text",
    );
    const truncated = png.subarray(0, Math.floor(png.length / 2));
    await rejects(
      zb.put(truncated),
      lib.ContentValidationError,
      "truncated png",
    );
    const gif = await sharp({
      create: { width: 8, height: 8, channels: 3, background: "#f00" },
    })
      .gif()
      .toBuffer();
    const e = await rejects(zb.put(gif), lib.ContentValidationError, "gif");
    assert(
      e instanceof lib.UnsupportedFormatError,
      `gif gave ${e.constructor.name}`,
    );
  },
);
await check(
  "a decompression-bomb size limit is enforced before decoding",
  async () => {
    const tooBig = new ZeroBucket({ sqlite: dbPath(), maxBytes: 100 });
    await rejects(tooBig.put(png), lib.ImageTooLargeError, "maxBytes");
    await tooBig.close();
  },
);
await check(
  "optimize converts the format and downscales on the way in, and the stored bytes are the converted ones",
  async () => {
    const id = await zb.put(await noisyImage("jpeg", 120, 80, 3), {
      optimize: { format: "webp", quality: 80, maxWidth: 60 },
    });
    const got = await zb.get(id);
    assert(got.mimeType === "image/webp", `stored as ${got.mimeType}`);
    const meta = await sharp(Buffer.from(got.data)).metadata();
    assert(
      meta.format === "webp" && meta.width === 60 && meta.height === 40,
      `stored image is ${meta.format} ${meta.width}x${meta.height}`,
    );
  },
);
const heicFixture = join(here, "..", "fixtures", "small.heic");
if (lib.heicAvailable?.() ?? true) {
  await check(
    "HEIC is validated and stored byte-exact (decoded in a worker thread that does not keep the process alive)",
    async () => {
      const heic = readFileSync(heicFixture);
      const id = await zb.put(heic);
      const got = await zb.get(id);
      assert(got.mimeType === "image/heic", `mime ${got.mimeType}`);
      assert(
        Buffer.compare(Buffer.from(got.data), heic) === 0,
        "HEIC bytes changed",
      );
    },
  );
} else {
  skip("HEIC", "libheif-js is not installed");
}

// ---- HTTP handler ------------------------------------------------------------------------------------------------------
console.log("\nHTTP (Request/Response handler and a real socket)");
const handler = httpLib.createImageHandler(zb);
let httpId;
await check(
  "handler: 200 with ETag, 304 on If-None-Match, 206 on Range, 404 for unknown id",
  async () => {
    httpId = await zb.put(await noisyImage("png", 128, 128, 9));
    const base = "http://localhost/images/";
    const r200 = await handler(new Request(base + httpId));
    assert(r200.status === 200, `status ${r200.status}`);
    const etag = r200.headers.get("etag");
    assert(
      etag && r200.headers.get("content-type") === "image/png",
      "headers wrong",
    );
    await r200.arrayBuffer();
    const r304 = await handler(
      new Request(base + httpId, { headers: { "if-none-match": etag } }),
    );
    assert(r304.status === 304, `304 expected, got ${r304.status}`);
    const r206 = await handler(
      new Request(base + httpId, { headers: { range: "bytes=0-9" } }),
    );
    assert(
      r206.status === 206 && (await r206.arrayBuffer()).byteLength === 10,
      "range wrong",
    );
    const r404 = await handler(
      new Request(base + "00000000-0000-0000-0000-000000000000"),
    );
    assert(r404.status === 404, `404 expected, got ${r404.status}`);
  },
);
await check(
  "toNodeHandler on node:http: real socket, streamed body matches, Range and HEAD work",
  async () => {
    const server = createServer(httpLib.toNodeHandler(handler));
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    const base = `http://127.0.0.1:${server.address().port}/images/${httpId}`;
    try {
      const expected = await zb.get(httpId);
      const full = await fetch(base);
      assert(full.status === 200, `status ${full.status}`);
      assert(
        Buffer.compare(
          Buffer.from(await full.arrayBuffer()),
          Buffer.from(expected.data),
        ) === 0,
        "body differs",
      );
      const part = await fetch(base, { headers: { range: "bytes=5-14" } });
      assert(
        part.status === 206 &&
          Buffer.from(await part.arrayBuffer()).length === 10,
        "range body wrong",
      );
      const head = await fetch(base, { method: "HEAD" });
      assert(
        head.status === 200 &&
          Number(head.headers.get("content-length")) === expected.data.length,
        "HEAD wrong",
      );
    } finally {
      server.closeAllConnections?.();
      await new Promise((resolve) => server.close(resolve));
    }
  },
);

// ---- CLI ---------------------------------------------------------------------------------------------------------------
console.log("\nCLI (dist/bin.js, run by this same runtime)");
const binPath = fileURLToPath(new URL("../../dist/bin.js", import.meta.url));
function cli(...args) {
  // Deno needs `run -A script`; Node and Bun take the script path directly.
  const argv = globalThis.Deno
    ? ["run", "-A", "--node-modules-dir=manual", binPath, ...args]
    : [binPath, ...args];
  const r = spawnSync(process.execPath, argv, {
    encoding: "utf8",
    timeout: 60_000,
  });
  return { code: r.status, out: `${r.stdout ?? ""}${r.stderr ?? ""}` };
}
const cliDb = dbPath();
await check(
  "init, info and verify work on a SQLite file, and verify exits 1 on a corrupted image",
  async () => {
    const seed = new ZeroBucket({ sqlite: cliDb });
    await seed.put(await noisyImage("png", 64, 64, 41));
    await seed.put(await noisyImage("jpeg", 64, 64, 42));
    await seed.close();
    const init = cli("init", "--sqlite", cliDb);
    assert(init.code === 0, `init exit ${init.code}: ${init.out}`);
    const info = cli("info", "--sqlite", cliDb);
    assert(
      info.code === 0 && /\b2\b/.test(info.out),
      `info exit ${info.code}: ${info.out}`,
    );
    const good = cli("verify", "--sqlite", cliDb);
    assert(good.code === 0, `verify (healthy) exit ${good.code}: ${good.out}`);
    const raw = new Database(cliDb);
    raw
      .prepare(
        "UPDATE zerobucket_images SET data = zeroblob(length(data)) WHERE rowid = (SELECT min(rowid) FROM zerobucket_images)",
      )
      .run();
    raw.close();
    const bad = cli("verify", "--sqlite", cliDb);
    assert(
      bad.code === 1,
      `verify (corrupted) should exit 1, got ${bad.code}: ${bad.out}`,
    );
  },
);
await check(
  "an unknown command is a usage error (exit 2 or 1), not a crash",
  () => {
    const r = cli("definitely-not-a-command");
    assert(
      r.code !== 0 &&
        r.code !== null &&
        !/TypeError|ReferenceError|at file:/.test(r.out),
      `exit ${r.code}: ${r.out}`,
    );
  },
);

// ---- Postgres, MySQL/MariaDB, S3 ---------------------------------------------------------------------------------------
const PG = process.env.ZEROBUCKET_TEST_DATABASE_URL;
const MY = process.env.ZEROBUCKET_TEST_MYSQL_URL;
const S3 = process.env.ZEROBUCKET_TEST_S3_ENDPOINT;

async function backendRoundTrip(label, make) {
  console.log(`\n${label}`);
  const db = make();
  try {
    await check("put, get, stream, metadata, delete", async () => {
      const bytes = await noisyImage("png", 80, 60, 21);
      const id = await db.put(bytes);
      const got = await db.get(id);
      assert(
        Buffer.compare(Buffer.from(got.data), bytes) === 0,
        "bytes differ",
      );
      assert(
        Buffer.compare(
          await collect(await db.getStream(id, { chunkSize: 700 })),
          bytes,
        ) === 0,
        "stream differs",
      );
      assert(
        (await db.metadata(id)).sizeBytes === bytes.length,
        "metadata size",
      );
      assert((await db.delete(id)) === true, "delete");
    });
    await check(
      "20 concurrent puts use the connection pool without errors",
      async () => {
        const ids = await Promise.all(
          Array.from({ length: 20 }, async (_, i) =>
            db.put(await noisyImage("png", 32, 32, 300 + i)),
          ),
        );
        assert(new Set(ids).size === 20, "duplicate ids");
        await Promise.all(ids.map((id) => db.delete(id)));
      },
    );
    await check("an unknown id is a typed ImageNotFoundError", async () => {
      await rejects(
        db.get("00000000-0000-0000-0000-000000000000"),
        lib.ImageNotFoundError,
        "unknown id",
      );
    });
  } finally {
    await db.close();
  }
}
if (PG)
  await backendRoundTrip(
    "PostgreSQL (pg)",
    () => new ZeroBucket({ connectionString: PG }),
  );
else skip("PostgreSQL", "ZEROBUCKET_TEST_DATABASE_URL is not set");
if (MY)
  await backendRoundTrip(
    "MySQL / MariaDB (mysql2)",
    () => new ZeroBucket({ mysql: MY }),
  );
else skip("MySQL / MariaDB", "ZEROBUCKET_TEST_MYSQL_URL is not set");

if (S3) {
  console.log("\nS3 tiering (@aws-sdk/client-s3), on SQLite");
  await check(
    "tierToObjectStorage moves the bytes to S3 and every read path still works",
    async () => {
      const { S3Client, CreateBucketCommand } =
        await import("@aws-sdk/client-s3");
      const bucket = `zerobucket-runtime-${Date.now()}`;
      const credentials = {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "test",
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "test",
      };
      await new S3Client({
        endpoint: S3,
        region: "us-east-1",
        credentials,
        forcePathStyle: true,
      }).send(new CreateBucketCommand({ Bucket: bucket }));
      const tiered = new ZeroBucket({
        sqlite: dbPath(),
        objectStorage: new ObjectStorage({
          bucket,
          endpoint: S3,
          region: "us-east-1",
          credentials,
          forcePathStyle: true,
        }),
      });
      try {
        const bytes = await noisyImage("png", 90, 70, 5);
        const id = await tiered.put(bytes);
        assert(
          (await tiered.tierToObjectStorage(id)) === true,
          "tier returned false",
        );
        assert(
          Buffer.compare(Buffer.from((await tiered.get(id)).data), bytes) === 0,
          "get after tiering differs",
        );
        assert(
          Buffer.compare(
            await collect(await tiered.getStream(id, { chunkSize: 900 })),
            bytes,
          ) === 0,
          "stream after tiering differs",
        );
      } finally {
        await tiered.close();
      }
    },
  );
} else {
  skip("S3 tiering", "ZEROBUCKET_TEST_S3_ENDPOINT is not set");
}

await zb.close();
rmSync(tmp, { recursive: true, force: true });

console.log(
  `\n${runtime}: ${passed} passed, ${failures} failed, ${skipped} skipped`,
);
if (failures > 0) {
  // Not process.exitCode: Deno's node-compat layer ignores it in some versions, and a failing run must fail.
  process.exit(1);
}
