import { spawn } from "node:child_process";
import { mkdtempSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  CreateBucketCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ZeroBucket } from "../src/index.js";
import { main } from "../src/cli.js";
import { DATABASE_URL, makeImage } from "./helpers.js";

const ENDPOINT = process.env.ZEROBUCKET_TEST_S3_ENDPOINT;
const nodeMajor = Number(process.versions.node.split(".")[0]);
const num = (n: number) => n.toLocaleString("en-US");
const version = JSON.parse(readFileSync("package.json", "utf8"))
  .version as string;

async function run(
  args: string[],
  env: Record<string, string | undefined> = {},
) {
  const out: string[] = [];
  const err: string[] = [];
  const code = await main(args, {
    out: (l) => out.push(l),
    err: (l) => err.push(l),
    env,
  });
  return {
    code,
    out: out.join("\n"),
    err: err.join("\n"),
    all: [...out, ...err].join("\n"),
  };
}

// ---------------------------------------------------------------------------------------------------------------
describe("CLI: usage, flags and environment (no database needed)", () => {
  it("--version and --help", async () => {
    expect(await run(["--version"])).toMatchObject({
      code: 0,
      out: `zerobucket ${version}`,
    });
    expect(await run(["-v"])).toMatchObject({
      code: 0,
      out: `zerobucket ${version}`,
    });
    const h = await run(["--help"]);
    expect(h.code).toBe(0);
    expect(h.out).toMatch(/Usage: zerobucket <command>/);
    for (const c of ["init", "migrate", "info", "verify", "tier"])
      expect(h.out).toContain(c);
  });
  it("no command, unknown command, unknown option and misplaced options are usage errors (exit 2)", async () => {
    expect((await run([])).code).toBe(2);
    expect(await run(["frobnicate"])).toMatchObject({ code: 2 });
    expect((await run(["frobnicate"])).err).toMatch(
      /unknown command "frobnicate"/,
    );
    expect((await run(["info", "--nope"])).code).toBe(2);
    const misplaced = await run(["init", "--sample", "5", "--sqlite", "x.db"]);
    expect(misplaced).toMatchObject({ code: 2 });
    expect(misplaced.err).toMatch(/--sample is not valid for 'init'/);
    expect((await run(["info", "extra", "--sqlite", "x.db"])).code).toBe(2);
  });
  it("needs a database; refuses ambiguous ones; flags beat environment variables", async () => {
    const none = await run(["info"]);
    expect(none.code).toBe(2);
    expect(none.err).toMatch(/no database provided/);
    expect(
      (
        await run([
          "info",
          "--sqlite",
          "a.db",
          "--database-url",
          "postgresql://x",
        ])
      ).err,
    ).toMatch(/choose one database/);
    expect(
      (
        await run(["info"], {
          ZEROBUCKET_DATABASE_URL: "postgresql://x",
          ZEROBUCKET_SQLITE_PATH: "a.db",
        })
      ).err,
    ).toMatch(/both ZEROBUCKET_DATABASE_URL and ZEROBUCKET_SQLITE_PATH/);
  });
  it("tier validates its arguments before touching anything", async () => {
    const base = ["--sqlite", "x.db", "--bucket", "b"];
    expect((await run(["tier", ...base])).err).toMatch(
      /specify either a single IMAGE_ID/,
    );
    expect((await run(["tier", "abc", "--all", ...base])).err).toMatch(
      /can't combine/,
    );
    expect((await run(["tier", "--all", "--sqlite", "x.db"])).err).toMatch(
      /--bucket is required/,
    );
    expect((await run(["tier", "--all", "--dedup", ...base])).err).toMatch(
      /not supported with --dedup/,
    );
    expect(
      (await run(["tier", "--all", "--min-size", "abc", ...base])).err,
    ).toMatch(/--min-size must be an integer/);
    expect((await run(["tier", "--all", "--limit", "0", ...base])).err).toMatch(
      /--limit must be an integer >= 1/,
    );
    expect(
      (await run(["tier", "--all", ...base, "--aws-access-key-id", "x"])).err,
    ).toMatch(/both --aws-access-key-id and --aws-secret-access-key/);
    expect(
      (await run(["verify", "--sample", "0", "--sqlite", "x.db"])).err,
    ).toMatch(/--sample must be an integer >= 1/);
  });
  it("a Postgres connection failure is reported cleanly (exit 2, no stack trace)", async () => {
    const r = await run([
      "info",
      "--database-url",
      "postgresql://nobody:x@127.0.0.1:1/none",
    ]);
    expect(r.code).toBe(2);
    expect(r.err).toMatch(/^Error: could not connect/);
    expect(r.err).not.toMatch(/\n\s+at /);
  });
});

// ---------------------------------------------------------------------------------------------------------------
type Kind = "postgres" | "sqlite";
const kinds: Kind[] = [
  ...(DATABASE_URL ? (["postgres"] as const) : []),
  ...(nodeMajor >= 22 ? (["sqlite"] as const) : []),
];

for (const kind of kinds) {
  describe(`CLI against ${kind}`, () => {
    let dir: string;
    let path: string;
    let raw: pg.Pool | undefined;
    let n = 0;

    const target = () =>
      kind === "postgres"
        ? ["--database-url", DATABASE_URL!]
        : ["--sqlite", path];
    const fresh = () => {
      n += 1;
      path = join(dir, `t${n}.db`);
    };
    const client = (dedup = false) =>
      new ZeroBucket(
        kind === "postgres"
          ? { connectionString: DATABASE_URL, dedup }
          : { sqlite: path, dedup },
      );
    const dropAll = async () => {
      if (kind === "postgres")
        await raw!.query(
          "DROP TABLE IF EXISTS zerobucket_images, zerobucket_image_refs, zerobucket_blobs CASCADE",
        );
      else fresh();
    };
    const reset = async (dedup = false) => {
      await dropAll();
      await run(["init", ...(dedup ? ["--dedup"] : []), ...target()]);
    };
    /** Corrupt one stored image's bytes without touching its checksum. */
    const tamper = async (id: string, dedup = false) => {
      if (kind === "postgres") {
        await raw!.query(
          dedup
            ? "UPDATE zerobucket_blobs SET data = set_byte(data, 10, 255 - get_byte(data, 10)) WHERE checksum_sha256 = (SELECT checksum_sha256 FROM zerobucket_image_refs WHERE id = $1)"
            : "UPDATE zerobucket_images SET data = set_byte(data, 10, 255 - get_byte(data, 10)) WHERE id = $1",
          [id],
        );
      } else {
        const Database = (await import("better-sqlite3")).default;
        const db = new Database(path);
        const sel = dedup
          ? "SELECT b.checksum_sha256 AS k, b.data AS data FROM zerobucket_blobs b JOIN zerobucket_image_refs r ON r.checksum_sha256 = b.checksum_sha256 WHERE r.id = ?"
          : "SELECT id AS k, data FROM zerobucket_images WHERE id = ?";
        const row = db.prepare(sel).get(id) as { k: string; data: Buffer };
        row.data[10] = 255 - row.data[10]!;
        db.prepare(
          dedup
            ? "UPDATE zerobucket_blobs SET data = ? WHERE checksum_sha256 = ?"
            : "UPDATE zerobucket_images SET data = ? WHERE id = ?",
        ).run(row.data, row.k);
        db.close();
      }
    };
    const setAgeDays = async (id: string, days: number) => {
      if (kind === "postgres")
        await raw!.query(
          "UPDATE zerobucket_images SET created_at = now() - make_interval(days => $2::int) WHERE id = $1",
          [id, days],
        );
      else {
        const Database = (await import("better-sqlite3")).default;
        const db = new Database(path);
        db.prepare(
          "UPDATE zerobucket_images SET created_at = ? WHERE id = ?",
        ).run(
          new Date(Date.now() - days * 86_400_000)
            .toISOString()
            .replace("Z", "000+00:00"),
          id,
        );
        db.close();
      }
    };

    beforeAll(async () => {
      dir = mkdtempSync(join(tmpdir(), "zb-cli-"));
      fresh();
      if (kind === "postgres")
        raw = new pg.Pool({ connectionString: DATABASE_URL });
    });
    afterAll(async () => {
      if (kind === "postgres") {
        await run(["init", ...target()]); // leave the schema behind for the other test files
        await raw!.end();
      }
    });

    describe("init / migrate", () => {
      it("creates the schema, is idempotent, and supports dedup tables", async () => {
        await dropAll();
        const a = await run(["init", ...target()]);
        expect(a).toMatchObject({
          code: 0,
          out: "zerobucket_images table and indexes are ready.",
        });
        expect((await run(["init", ...target()])).code).toBe(0);
        const d = await run(["init", "--dedup", ...target()]);
        expect(d).toMatchObject({
          code: 0,
          out: "zerobucket_blobs and zerobucket_image_refs tables and indexes are ready.",
        });
        expect((await run(["info", "--dedup", ...target()])).code).toBe(0);
      });
      it("migrate says what it does (same as init) and works", async () => {
        await dropAll();
        const m = await run(["migrate", ...target()]);
        expect(m.code).toBe(0);
        expect(m.out).toMatch(/does not have versioned migrations yet/);
        expect(m.out).toMatch(/table and indexes are ready/);
      });
      it("migrate --to-dedup copies classic images (PostgreSQL) or is refused (SQLite)", async () => {
        await reset();
        if (kind === "sqlite") {
          const r = await run(["migrate", "--to-dedup", ...target()]);
          expect(r).toMatchObject({ code: 2 });
          expect(r.err).toMatch(/only supported for PostgreSQL/);
          return;
        }
        await raw!.query("DELETE FROM zerobucket_image_refs").catch(() => {});
        await raw!.query("DELETE FROM zerobucket_blobs").catch(() => {});
        const zb = client();
        const a = await makeImage("png", 30, 30, { seed: 1 });
        await zb.put(a);
        await zb.put(a);
        await zb.put(await makeImage("png", 31, 31, { seed: 2 }));
        await zb.close();
        const r = await run(["migrate", "--to-dedup", ...target()]);
        expect(r.code).toBe(0);
        expect(r.out).toContain("Migrated 3 image(s) into 2 distinct blob(s).");
        expect(r.out).toContain("Duplicate references found: 1");
        expect((await run(["info", "--dedup", ...target()])).out).toMatch(
          /3 image\(s\) sharing 2 distinct blob\(s\)/,
        );
        expect(
          (await run(["migrate", "--to-dedup", ...target()])).out,
        ).toContain("Already migrated (skipped): 3"); // re-runnable
      });
    });

    describe("info", () => {
      it("reports a missing schema / file without creating anything", async () => {
        await dropAll();
        const r = await run(["info", ...target()]);
        expect(r.code).toBe(1);
        if (kind === "sqlite") {
          expect(r.out).toMatch(
            /does not exist yet. Run 'zerobucket init --sqlite PATH'/,
          );
          expect(existsSync(path)).toBe(false); // looking must never create the database
        } else {
          expect(r.out).toBe(
            "zerobucket_images table does not exist yet. Run 'zerobucket init' first.",
          );
          expect(
            (await raw!.query("SELECT to_regclass('zerobucket_images') AS t"))
              .rows[0].t,
          ).toBeNull();
        }
        await run(["init", ...target()]);
      });
      it("reports an empty database", async () => {
        await reset();
        const r = await run(["info", ...target()]);
        expect(r.code).toBe(0);
        expect(r.out).toContain("zerobucket_images: 0 image(s)");
        expect(r.out).not.toContain("By format");
      });
      it("reports counts, sizes with thousands separators, and the per-format breakdown", async () => {
        await reset();
        const zb = client();
        const imgs = [
          await makeImage("png", 60, 60, { seed: 1 }),
          await makeImage("png", 61, 61, { seed: 2 }),
          await makeImage("png", 62, 62, { seed: 3 }),
          await makeImage("jpeg", 70, 70, { seed: 4 }),
        ];
        for (const d of imgs) await zb.put(d);
        await zb.close();
        const total = imgs.reduce((s, d) => s + d.length, 0);
        const r = await run(["info", ...target()]);
        expect(r.code).toBe(0);
        expect(r.out).toContain("zerobucket_images: 4 image(s)");
        expect(r.out).toContain(`${num(total)} bytes`);
        expect(r.out).toMatch(/On-disk size/);
        expect(r.out).toMatch(/Oldest: \d{4}-\d\d-\d\d/);
        expect(r.out).toMatch(/^  (Oldest|Newest): \S+[ T]\S+[+-]\d\d:\d\d$/m); // same "+00:00" form as Python's CLI
        expect(r.out).toMatch(/image\/png\s+3 image\(s\)/);
        expect(r.out).toMatch(/image\/jpeg\s+1 image\(s\)/);
        expect(r.out.indexOf("image/png")).toBeLessThan(
          r.out.indexOf("image/jpeg"),
        ); // most common first
      });
      it("dedup mode reports shared blobs and the bytes saved", async () => {
        await reset(true);
        const zb = client(true);
        const a = await makeImage("png", 80, 80, { seed: 9 });
        await zb.put(a);
        await zb.put(a);
        await zb.put(a);
        await zb.close();
        const r = await run(["info", "--dedup", ...target()]);
        expect(r.code).toBe(0);
        expect(r.out).toContain(
          "zerobucket_image_refs: 3 image(s) sharing 1 distinct blob(s)",
        );
        expect(r.out).toContain(
          `Physical bytes after dedup:                ${num(a.length)} bytes (saved ${num(a.length * 2)} bytes, 66.7%)`,
        );
      });
    });

    describe("verify", () => {
      it("passes on healthy data, and says so when there is nothing to check", async () => {
        await reset();
        expect((await run(["verify", ...target()])).out).toBe(
          "No images to verify.",
        );
        const zb = client();
        for (let i = 0; i < 3; i++)
          await zb.put(await makeImage("png", 40 + i, 40, { seed: 20 + i }));
        await zb.close();
        const r = await run(["verify", ...target()]);
        expect(r.code).toBe(0);
        expect(r.out).toContain("Verifying 3 image(s)...");
        expect(r.out).toContain(
          "OK: all 3 image(s) verified, no checksum mismatches.",
        );
      });
      it("detects a corrupted image, names it, and exits 1", async () => {
        await reset();
        const zb = client();
        const good = await zb.put(await makeImage("png", 50, 50, { seed: 30 }));
        const bad = await zb.put(await makeImage("png", 51, 51, { seed: 31 }));
        await zb.close();
        await tamper(bad);
        const r = await run(["verify", ...target()]);
        expect(r.code).toBe(1);
        expect(r.out).toContain(
          "FAILED: 1 of 2 image(s) have a checksum mismatch",
        );
        expect(r.out).toContain(bad);
        expect(r.out).not.toContain(`  ${good}`);
      });
      it("dedup mode: one corrupted blob flags every reference that shares it", async () => {
        await reset(true);
        const zb = client(true);
        const d = await makeImage("png", 55, 55, { seed: 40 });
        const [a, b] = [await zb.put(d), await zb.put(d)];
        await zb.put(await makeImage("png", 56, 56, { seed: 41 }));
        await zb.close();
        await tamper(a, true);
        const r = await run(["verify", "--dedup", ...target()]);
        expect(r.code).toBe(1);
        expect(r.out).toContain("2 of 3 image(s) have a checksum mismatch");
        expect(r.out).toContain(a);
        expect(r.out).toContain(b);
      });
      it("--sample checks only N random images", async () => {
        await reset();
        const zb = client();
        for (let i = 0; i < 6; i++)
          await zb.put(await makeImage("png", 40 + i, 40, { seed: 50 + i }));
        await zb.close();
        const r = await run(["verify", "--sample", "2", ...target()]);
        expect(r.out).toContain("Verifying 2 image(s)...");
        expect(r.out).toContain("OK: all 2 image(s) verified");
        expect(
          (await run(["verify", "--sample", "100", ...target()])).out,
        ).toContain("Verifying 6 image(s)...");
      });
      it("skips tiered images with a note instead of crashing on them (Python's verify crashes here)", async () => {
        await reset();
        const zb = client();
        await zb.put(await makeImage("png", 44, 44, { seed: 60 }));
        await zb.close();
        // a row that lives in object storage: its database `data` is NULL, so there is nothing local to checksum
        const id = "00000000-0000-4000-8000-0000000000aa";
        if (kind === "postgres") {
          await raw!.query(
            "INSERT INTO zerobucket_images (id, data, mime_type, size_bytes, checksum_sha256, storage_backend, object_storage_bucket, object_storage_key) VALUES ($1::uuid, NULL, 'image/png', 10, $2, 'object_storage', 'b', $1::text)",
            [id, "a".repeat(64)],
          );
        } else {
          const Database = (await import("better-sqlite3")).default;
          const db = new Database(path);
          db.prepare(
            "INSERT INTO zerobucket_images (id, data, mime_type, size_bytes, checksum_sha256, created_at, updated_at, storage_backend, object_storage_bucket, object_storage_key) VALUES (?, NULL, 'image/png', 10, ?, '2026-01-01T00:00:00.000000+00:00', '2026-01-01T00:00:00.000000+00:00', 'object_storage', 'b', ?)",
          ).run(id, "a".repeat(64), id);
          db.close();
        }
        const r = await run(["verify", ...target()]);
        expect(r.code).toBe(0);
        expect(r.out).toContain(
          "Note: 1 image(s) tiered to object storage were skipped",
        );
        expect(r.out).toContain("OK: all 1 image(s) verified");
        const info = await run(["info", ...target()]);
        expect(info.out).toMatch(/Tiered to object storage:\s+1 image\(s\)/);
      });
    });

    describe.skipIf(!ENDPOINT)("tier (real S3 API)", () => {
      const bucket = `zb-cli-${kind}-${Date.now()}`;
      const s3 = () =>
        new S3Client({
          endpoint: ENDPOINT,
          region: "us-east-1",
          forcePathStyle: true,
          credentials: { accessKeyId: "test", secretAccessKey: "test" },
        });
      const s3flags = [
        "--bucket",
        bucket,
        "--endpoint-url",
        ENDPOINT ?? "",
        "--aws-access-key-id",
        "test",
        "--aws-secret-access-key",
        "test",
      ];
      const objectExists = (key: string) =>
        s3()
          .send(new HeadObjectCommand({ Bucket: bucket, Key: key }))
          .then(
            () => true,
            () => false,
          );
      beforeAll(async () => {
        await s3().send(new CreateBucketCommand({ Bucket: bucket }));
      });
      const seed = async (specs: { w: number; seed: number }[]) => {
        const zb = client();
        const out: { id: string; data: Buffer }[] = [];
        for (const s of specs) {
          const data = await makeImage("png", s.w, s.w, { seed: s.seed });
          out.push({ id: await zb.put(data), data });
        }
        await zb.close();
        return out;
      };

      it("tiers a single image; re-running it is a harmless skip; unknown ids fail with exit 1", async () => {
        await reset();
        const [a] = await seed([{ w: 40, seed: 1 }]);
        const r = await run(["tier", a!.id, ...s3flags, ...target()]);
        expect(r).toMatchObject({ code: 0 });
        expect(r.out).toContain(
          "Tiered: 1, already tiered (skipped): 0, failed: 0",
        );
        expect(await objectExists(a!.id)).toBe(true);
        expect(
          (await run(["tier", a!.id, ...s3flags, ...target()])).out,
        ).toContain("Tiered: 0, already tiered (skipped): 1, failed: 0");
        const missing = await run([
          "tier",
          "00000000-0000-4000-8000-0000000000bb",
          ...s3flags,
          ...target(),
        ]);
        expect(missing.code).toBe(1);
        expect(missing.err).toContain("NOT FOUND");
        expect((await run(["info", ...target()])).out).toMatch(
          /Tiered to object storage:\s+1 image\(s\)/,
        );
      });

      it("--dry-run lists the candidates and changes nothing", async () => {
        await reset();
        const imgs = await seed([
          { w: 30, seed: 2 },
          { w: 31, seed: 3 },
        ]);
        const r = await run([
          "tier",
          "--all",
          "--dry-run",
          ...s3flags,
          ...target(),
        ]);
        expect(r.code).toBe(0);
        expect(r.out).toContain("Would tier 2 image(s):");
        for (const i of imgs) expect(r.out).toContain(i.id);
        expect(await objectExists(imgs[0]!.id)).toBe(false);
        expect((await run(["info", ...target()])).out).not.toContain(
          "Tiered to object storage",
        );
      });

      it("--min-size genuinely filters (only large images move)", async () => {
        await reset();
        const [small, big] = await seed([
          { w: 20, seed: 4 },
          { w: 120, seed: 5 },
        ]);
        expect(small!.data.length).toBeLessThan(big!.data.length);
        const threshold = String(small!.data.length + 1);
        const r = await run([
          "tier",
          "--min-size",
          threshold,
          ...s3flags,
          ...target(),
        ]);
        expect(r.out).toContain(
          "Tiered: 1, already tiered (skipped): 0, failed: 0",
        );
        expect([
          await objectExists(small!.id),
          await objectExists(big!.id),
        ]).toEqual([false, true]);
      });

      it("--older-than genuinely filters by age", async () => {
        await reset();
        const [young, old] = await seed([
          { w: 30, seed: 6 },
          { w: 31, seed: 7 },
        ]);
        await setAgeDays(old!.id, 10);
        const r = await run([
          "tier",
          "--older-than",
          "5",
          ...s3flags,
          ...target(),
        ]);
        expect(r.out).toContain("Tiered: 1,");
        expect([
          await objectExists(young!.id),
          await objectExists(old!.id),
        ]).toEqual([false, true]);
      });

      it("--all with --limit, and a later --all finishes the rest; already-tiered images are never re-selected", async () => {
        await reset();
        const imgs = await seed([
          { w: 30, seed: 8 },
          { w: 31, seed: 9 },
          { w: 32, seed: 10 },
        ]);
        expect(
          (
            await run([
              "tier",
              "--all",
              "--limit",
              "2",
              ...s3flags,
              ...target(),
            ])
          ).out,
        ).toContain("Tiered: 2,");
        const rest = await run(["tier", "--all", ...s3flags, ...target()]);
        expect(rest.out).toContain(
          "Tiered: 1, already tiered (skipped): 0, failed: 0",
        );
        expect(
          (await run(["tier", "--all", ...s3flags, ...target()])).out,
        ).toBe("No matching images to tier (or all already tiered).");
        for (const i of imgs) expect(await objectExists(i.id)).toBe(true);
      });

      it("a failing upload is reported per image, exits 1, and leaves the data untouched", async () => {
        await reset();
        const [a] = await seed([{ w: 33, seed: 11 }]);
        const r = await run([
          "tier",
          a!.id,
          "--bucket",
          "no-such-bucket-xyz",
          ...s3flags.slice(2),
          ...target(),
        ]);
        expect(r.code).toBe(1);
        expect(r.err).toMatch(new RegExp(`FAILED ${a!.id}`));
        expect(r.out).toContain("failed: 1");
        const zb = client();
        expect((await zb.get(a!.id)).data.equals(a!.data)).toBe(true); // still safely in the database
        await zb.close();
      });

      it("verify --bucket checks tiered images too, and catches a tampered object", async () => {
        await reset();
        const [a, b] = await seed([
          { w: 34, seed: 12 },
          { w: 35, seed: 13 },
        ]);
        await run(["tier", "--all", ...s3flags, ...target()]);
        const ok = await run(["verify", ...s3flags, ...target()]);
        expect(ok).toMatchObject({ code: 0 });
        expect(ok.out).toContain("OK: all 2 image(s) verified");
        expect(ok.out).not.toContain("skipped");
        await s3().send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: b!.id,
            Body: Buffer.from("tampered"),
            ContentType: "image/png",
          }),
        );
        const bad = await run(["verify", ...s3flags, ...target()]);
        expect(bad.code).toBe(1);
        expect(bad.out).toContain(`ERROR: could not read 1 image(s)`);
        expect(bad.out).toContain(b!.id);
        expect(bad.out).not.toContain(`${a!.id}:`);
        expect(
          (
            await s3().send(
              new GetObjectCommand({ Bucket: bucket, Key: a!.id }),
            )
          ).$metadata.httpStatusCode,
        ).toBe(200);
      });
    });
  });
}

// ---------------------------------------------------------------------------------------------------------------
describe("the real executable", () => {
  it("propagates exit codes and prints to the right streams (end to end, as a child process)", async () => {
    const dir = mkdtempSync(join(tmpdir(), "zb-bin-"));
    const runBin = (args: string[]) =>
      new Promise<{ code: number | null; out: string; err: string }>(
        (resolve) => {
          const c = spawn(
            process.execPath,
            ["--import", "tsx", "src/bin.ts", ...args],
            {
              cwd: process.cwd(),
              env: {
                ...process.env,
                ZEROBUCKET_DATABASE_URL: "",
                ZEROBUCKET_SQLITE_PATH: "",
              },
            },
          );
          let out = "";
          let err = "";
          c.stdout.on("data", (d) => (out += d));
          c.stderr.on("data", (d) => (err += d));
          c.on("exit", (code) => resolve({ code, out, err }));
        },
      );
    const v = await runBin(["--version"]);
    expect(v).toMatchObject({ code: 0, out: `zerobucket ${version}\n` });
    const bad = await runBin(["info"]);
    expect(bad.code).toBe(2);
    expect(bad.err).toMatch(/no database provided/);
    expect(bad.out).toBe("");
    if (nodeMajor >= 22) {
      const path = join(dir, "e2e.db");
      expect((await runBin(["init", "--sqlite", path])).code).toBe(0);
      expect(readdirSync(dir)).toContain("e2e.db");
      const zb = new ZeroBucket({ sqlite: path });
      const id = await zb.put(await makeImage("png", 40, 40, { seed: 77 }));
      await zb.close();
      expect((await runBin(["verify", "--sqlite", path])).code).toBe(0);
      const Database = (await import("better-sqlite3")).default;
      const db = new Database(path);
      const row = db
        .prepare("SELECT data FROM zerobucket_images WHERE id = ?")
        .get(id) as { data: Buffer };
      row.data[10] = 255 - row.data[10]!;
      db.prepare("UPDATE zerobucket_images SET data = ? WHERE id = ?").run(
        row.data,
        id,
      );
      db.close();
      const failed = await runBin(["verify", "--sqlite", path]);
      expect(failed.code).toBe(1); // a scripted `zerobucket verify && deploy` must stop here
      expect(failed.out).toContain(id);
    }
  }, 60_000);
});
