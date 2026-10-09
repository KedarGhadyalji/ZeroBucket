/**
 * zerobucket CLI.
 *
 *   zerobucket init      create the schema if it does not exist
 *   zerobucket migrate   apply schema migrations (currently: same as init); --to-dedup copies classic -> dedup
 *   zerobucket info      storage stats: count, size, breakdown by format
 *   zerobucket verify    re-checksum stored images to detect corruption
 *   zerobucket tier      move image(s) into S3-compatible object storage
 *
 * Same commands, flags, messages and exit codes as the Python package's CLI (0 ok, 1 problems found / operation
 * failed, 2 usage or connection error), plus what Python's CLI lacks: it works on PostgreSQL AND SQLite, in classic
 * AND dedup mode, and `info` / `verify` understand images that were tiered to object storage.
 *
 * No dependencies: argument parsing is Node's built-in util.parseArgs. `tier` (and `verify --bucket`) need
 * `@aws-sdk/client-s3`, loaded lazily so every other command works without it.
 */
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { parseArgs } from "node:util";
import type { ImageListEntry, StorageBackend } from "./adapters/base.js";
import { PostgresBackend } from "./adapters/postgres.js";
import { msg } from "./adapters/shared.js";
import { SQLiteBackend } from "./adapters/sqlite.js";
import { ObjectStorage } from "./object-storage.js";

export interface CliIO {
  out(line: string): void;
  err(line: string): void;
  env: Record<string, string | undefined>;
}

const defaultIO = (): CliIO => ({
  out: (l) => console.log(l),
  err: (l) => console.error(l),
  env: process.env,
});

class CliExit extends Error {
  constructor(
    public readonly code: number,
    message: string,
  ) {
    super(message);
  }
}
const usageError = (m: string) => new CliExit(2, m);

export function cliVersion(): string {
  try {
    return JSON.parse(
      readFileSync(new URL("../package.json", import.meta.url), "utf8"),
    ).version as string;
  } catch {
    return "unknown";
  }
}

const HELP = `zerobucket - database-native image storage.

Usage: zerobucket <command> [options]

Commands:
  init       Create the schema (tables and indexes) if missing.
  migrate    Apply schema migrations (currently: same as init).
             --to-dedup  copy every classic image into the dedup tables (PostgreSQL only; non-destructive)
  info       Show storage stats: count, size, breakdown by format.
  verify     Re-checksum stored images to detect corruption.
               --sample N        check a random sample of N images instead of every image
               --bucket ...      also verify images tiered to object storage (same options as 'tier')
  tier       Move image(s) from the database into S3-compatible object storage.
               IMAGE_ID                          tier a single image, OR bulk:
               --all | --min-size BYTES | --older-than DAYS   (combinable), --limit N, --dry-run
               --bucket NAME (required)          bucket must already exist
               --endpoint-url URL                for non-AWS stores (R2, MinIO, B2, Spaces)
               --region NAME  --aws-access-key-id ID  --aws-secret-access-key KEY

Database (choose one; flags beat environment variables):
  --database-url URL    PostgreSQL connection string   (env: ZEROBUCKET_DATABASE_URL)
  --sqlite PATH         SQLite database file            (env: ZEROBUCKET_SQLITE_PATH)
  --dedup               operate on the dedup-mode tables instead of the classic table

Other:
  --version, -v         print the version
  --help, -h            show this help

Exit codes: 0 success, 1 problems found or an operation failed, 2 usage or connection error.`;

const OPTIONS = {
  "database-url": { type: "string" },
  sqlite: { type: "string" },
  dedup: { type: "boolean" },
  help: { type: "boolean", short: "h" },
  version: { type: "boolean", short: "v" },
  "to-dedup": { type: "boolean" },
  sample: { type: "string" },
  bucket: { type: "string" },
  "endpoint-url": { type: "string" },
  region: { type: "string" },
  "aws-access-key-id": { type: "string" },
  "aws-secret-access-key": { type: "string" },
  "min-size": { type: "string" },
  "older-than": { type: "string" },
  all: { type: "boolean" },
  limit: { type: "string" },
  "dry-run": { type: "boolean" },
} as const;

type Values = ReturnType<
  typeof parseArgs<{ options: typeof OPTIONS; allowPositionals: true }>
>["values"];

/** Which options each command accepts (besides the database options). Anything else is a usage error, not silently ignored. */
const COMMAND_OPTIONS: Record<string, string[]> = {
  init: [],
  migrate: ["to-dedup"],
  info: [],
  verify: [
    "sample",
    "bucket",
    "endpoint-url",
    "region",
    "aws-access-key-id",
    "aws-secret-access-key",
  ],
  tier: [
    "bucket",
    "endpoint-url",
    "region",
    "aws-access-key-id",
    "aws-secret-access-key",
    "min-size",
    "older-than",
    "all",
    "limit",
    "dry-run",
  ],
};
const COMMON = new Set(["database-url", "sqlite", "dedup", "help", "version"]);

type Target =
  | { kind: "postgres"; url: string }
  | { kind: "sqlite"; path: string };

function resolveTarget(v: Values, env: CliIO["env"]): Target {
  if (v.sqlite && v["database-url"])
    throw usageError(
      "Error: choose one database: --sqlite OR --database-url, not both.",
    );
  if (v.sqlite) return { kind: "sqlite", path: v.sqlite };
  if (v["database-url"]) return { kind: "postgres", url: v["database-url"] };
  const url = env.ZEROBUCKET_DATABASE_URL;
  const path = env.ZEROBUCKET_SQLITE_PATH;
  if (url && path) {
    throw usageError(
      "Error: both ZEROBUCKET_DATABASE_URL and ZEROBUCKET_SQLITE_PATH are set. Pass --database-url or --sqlite to choose.",
    );
  }
  if (url) return { kind: "postgres", url };
  if (path) return { kind: "sqlite", path };
  throw usageError(
    "Error: no database provided. Pass --database-url (PostgreSQL) or --sqlite PATH, or set the " +
      "ZEROBUCKET_DATABASE_URL / ZEROBUCKET_SQLITE_PATH environment variable.",
  );
}

type Admin = StorageBackend &
  Required<Pick<StorageBackend, "getInfo" | "listImages">>;

function openBackend(
  t: Target,
  o: { dedup: boolean; autoMigrate: boolean; objectStorage?: ObjectStorage },
): Admin {
  return (
    t.kind === "sqlite"
      ? new SQLiteBackend({
          path: t.path,
          dedup: o.dedup,
          autoMigrate: o.autoMigrate,
          objectStorage: o.objectStorage,
        })
      : new PostgresBackend({
          connectionString: t.url,
          dedup: o.dedup,
          autoMigrate: o.autoMigrate,
          objectStorage: o.objectStorage,
          maxRetries: 0,
        })
  ) as Admin;
}

/** Connect (and, if autoMigrate, create the schema). Connection problems are usage-class errors (exit 2). */
async function connect(backend: StorageBackend, what: string): Promise<void> {
  try {
    await backend.ready();
  } catch (exc) {
    await backend.close().catch(() => {});
    throw usageError(`Error: could not ${what}: ${msg(exc)}`);
  }
}

function positiveInt(
  name: string,
  raw: string | undefined,
  min: number,
): number | undefined {
  if (raw === undefined) return undefined;
  if (!/^\d+$/.test(raw) || Number(raw) < min)
    throw usageError(`Error: --${name} must be an integer >= ${min}.`);
  return Number(raw);
}

const num = (n: number) => n.toLocaleString("en-US");

function buildObjectStorage(v: Values): ObjectStorage {
  if (!v.bucket) throw usageError("Error: --bucket is required.");
  const id = v["aws-access-key-id"];
  const secret = v["aws-secret-access-key"];
  if (Boolean(id) !== Boolean(secret))
    throw usageError(
      "Error: pass both --aws-access-key-id and --aws-secret-access-key, or neither.",
    );
  return new ObjectStorage({
    bucket: v.bucket,
    endpoint: v["endpoint-url"],
    region: v.region,
    credentials:
      id && secret ? { accessKeyId: id, secretAccessKey: secret } : undefined,
  });
}

function requireSqliteFile(t: Target, hint: string): void {
  if (t.kind === "sqlite" && t.path !== ":memory:" && !existsSync(t.path)) {
    throw new CliExit(
      1,
      `SQLite database file ${JSON.stringify(t.path)} does not exist yet. ${hint}`,
    );
  }
}

// ---- commands -----------------------------------------------------------------------------------------------

async function cmdInit(
  v: Values,
  env: CliIO["env"],
  io: CliIO,
): Promise<number> {
  const backend = openBackend(resolveTarget(v, env), {
    dedup: Boolean(v.dedup),
    autoMigrate: true,
  });
  await connect(backend, "initialize schema");
  io.out(
    v.dedup
      ? "zerobucket_blobs and zerobucket_image_refs tables and indexes are ready."
      : "zerobucket_images table and indexes are ready.",
  );
  await backend.close();
  return 0;
}

async function cmdMigrate(
  v: Values,
  env: CliIO["env"],
  io: CliIO,
): Promise<number> {
  if (v["to-dedup"]) {
    const target = resolveTarget(v, env);
    if (target.kind !== "postgres")
      throw usageError("Error: --to-dedup is only supported for PostgreSQL.");
    const backend = openBackend(target, { dedup: true, autoMigrate: true });
    await connect(backend, "connect");
    try {
      const r = await backend.migrateClassicToDedup!();
      io.out(
        `Migrated ${num(r.imagesMigrated)} image(s) into ${num(r.distinctBlobsCreated)} distinct blob(s).`,
      );
      io.out(
        `  Duplicate references found: ${num(r.duplicateReferencesFound)}`,
      );
      io.out(`  Already migrated (skipped): ${num(r.alreadyMigrated)}`);
      io.out("The classic table was not modified.");
      return 0;
    } catch (exc) {
      throw new CliExit(1, `Error: ${msg(exc)}`);
    } finally {
      await backend.close();
    }
  }
  io.out(
    "Note: zerobucket does not have versioned migrations yet -- this currently just ensures the base schema exists, same as 'init'.",
  );
  return cmdInit(v, env, io);
}

async function cmdInfo(
  v: Values,
  env: CliIO["env"],
  io: CliIO,
): Promise<number> {
  const target = resolveTarget(v, env);
  requireSqliteFile(target, "Run 'zerobucket init --sqlite PATH' first.");
  const backend = openBackend(target, {
    dedup: Boolean(v.dedup),
    autoMigrate: false,
  });
  await connect(backend, "connect");
  try {
    const info = await backend.getInfo();
    if (!info) {
      io.out(
        v.dedup
          ? "zerobucket_blobs / zerobucket_image_refs tables do not exist yet. Run 'zerobucket init --dedup' first."
          : "zerobucket_images table does not exist yet. Run 'zerobucket init' first.",
      );
      return 1;
    }
    if (info.mode === "dedup") {
      io.out(
        `zerobucket_image_refs: ${num(info.count)} image(s) sharing ${num(info.dedup!.blobs)} distinct blob(s)`,
      );
    } else {
      io.out(`zerobucket_images: ${num(info.count)} image(s)`);
    }
    io.out(
      `  Total stored bytes (application-recorded): ${num(info.totalBytes)} bytes`,
    );
    if (info.dedup) {
      const saved = info.totalBytes - info.dedup.storedBytes;
      const pct =
        info.totalBytes > 0
          ? ((saved / info.totalBytes) * 100).toFixed(1)
          : "0.0";
      io.out(
        `  Physical bytes after dedup:                ${num(info.dedup.storedBytes)} bytes (saved ${num(saved)} bytes, ${pct}%)`,
      );
    }
    if (info.onDisk) {
      io.out(
        target.kind === "postgres"
          ? `  On-disk size (table + TOAST + indexes):    ${info.onDisk}`
          : `  On-disk size (database file + WAL):        ${info.onDisk}`,
      );
    }
    if (info.tiered && info.tiered.count > 0) {
      io.out(
        `  Tiered to object storage:                  ${num(info.tiered.count)} image(s), ${num(info.tiered.bytes)} bytes (not in the database)`,
      );
    }
    if (info.oldest !== null) {
      io.out(`  Oldest: ${info.oldest}`);
      io.out(`  Newest: ${info.newest}`);
    }
    if (info.byFormat.length > 0) {
      io.out("  By format:");
      for (const f of info.byFormat) {
        io.out(
          `    ${f.mimeType.padEnd(12)} ${String(f.count).padStart(8)} image(s)  ${num(f.bytes).padStart(14)} bytes`,
        );
      }
    }
    return 0;
  } finally {
    await backend.close();
  }
}

async function cmdVerify(
  v: Values,
  env: CliIO["env"],
  io: CliIO,
): Promise<number> {
  const sample = positiveInt("sample", v.sample, 1);
  const os = v.bucket ? buildObjectStorage(v) : undefined;
  const target = resolveTarget(v, env);
  requireSqliteFile(target, "Nothing to verify.");
  const backend = openBackend(target, {
    dedup: Boolean(v.dedup),
    autoMigrate: false,
    objectStorage: os,
  });
  await connect(backend, "connect");
  const mismatches: string[] = [];
  const errors: string[] = [];
  let checked = 0;
  let skippedTiered = 0;
  try {
    let entries: ImageListEntry[];
    try {
      entries = await backend.listImages({ sample });
    } catch (exc) {
      throw new CliExit(
        1,
        `Error: ${msg(exc)} (has 'zerobucket init' been run?)`,
      );
    }
    const total = entries.length;
    if (total === 0) {
      io.out("No images to verify.");
      return 0;
    }
    io.out(`Verifying ${num(total)} image(s)...`);
    // One image at a time: this table can legitimately be large, so never hold more than one image's bytes.
    for (let i = 1; i <= total; i++) {
      const e = entries[i - 1]!;
      if (e.tiered && !os) {
        skippedTiered += 1;
      } else {
        try {
          const rec = await backend.get(e.id);
          if (rec) {
            checked += 1;
            if (
              createHash("sha256").update(rec.data).digest("hex") !==
              rec.checksumSha256.trim()
            )
              mismatches.push(e.id);
          } // else: deleted between listing and checking, which is not corruption
        } catch (exc) {
          errors.push(`${e.id}: ${msg(exc)}`);
        }
      }
      if (i % 100 === 0 || i === total) io.out(`  ${i}/${total} checked...`);
    }
  } finally {
    await backend.close();
  }
  io.out("");
  if (mismatches.length > 0) {
    io.out(
      `FAILED: ${mismatches.length} of ${checked} image(s) have a checksum mismatch (possible corruption):`,
    );
    for (const id of mismatches) io.out(`  ${id}`);
  }
  if (errors.length > 0) {
    io.out(`ERROR: could not read ${errors.length} image(s):`);
    for (const e of errors) io.out(`  ${e}`);
  }
  if (skippedTiered > 0) {
    io.out(
      `Note: ${skippedTiered} image(s) tiered to object storage were skipped. Pass --bucket (and credentials) to verify them too.`,
    );
  }
  if (mismatches.length > 0 || errors.length > 0) return 1;
  io.out(`OK: all ${checked} image(s) verified, no checksum mismatches.`);
  return 0;
}

async function cmdTier(
  v: Values,
  positional: string | undefined,
  env: CliIO["env"],
  io: CliIO,
): Promise<number> {
  const bulk =
    Boolean(v.all) ||
    v["min-size"] !== undefined ||
    v["older-than"] !== undefined;
  if (positional && bulk)
    throw usageError(
      "Error: can't combine a single IMAGE_ID with --all/--min-size/--older-than.",
    );
  if (!positional && !bulk) {
    throw usageError(
      "Error: specify either a single IMAGE_ID to tier, or a bulk selection filter (--all, --min-size, and/or --older-than).",
    );
  }
  if (v.dedup)
    throw usageError(
      "Error: tier is not supported with --dedup (a deduplicated blob can be shared by many images).",
    );
  const minSize = positiveInt("min-size", v["min-size"], 0);
  const olderThanDays = positiveInt("older-than", v["older-than"], 0);
  const limit = positiveInt("limit", v.limit, 1);
  const os = buildObjectStorage(v); // pure argument validation: do it before touching the filesystem or the network
  const target = resolveTarget(v, env);
  requireSqliteFile(target, "Nothing to tier.");
  const backend = openBackend(target, {
    dedup: false,
    autoMigrate: false,
    objectStorage: os,
  });
  await connect(backend, "connect");
  try {
    let candidates: string[];
    if (positional) {
      candidates = [positional];
    } else {
      candidates = (
        await backend.listImages({
          minSize,
          olderThanDays,
          limit,
          onlyUntiered: true,
        })
      ).map((e) => e.id);
      if (candidates.length === 0) {
        io.out("No matching images to tier (or all already tiered).");
        return 0;
      }
    }
    if (v["dry-run"]) {
      io.out(`Would tier ${candidates.length} image(s):`);
      for (const id of candidates) io.out(`  ${id}`);
      return 0;
    }
    let tiered = 0;
    let skipped = 0;
    let failed = 0;
    let notAttempted = 0;
    const total = candidates.length;
    // Sequential on purpose: each tiering holds a lock for its duration, so racing them gains nothing.
    for (let i = 1; i <= total; i++) {
      const id = candidates[i - 1]!;
      try {
        const r = await backend.tierToObjectStorage(id);
        if (r === null) {
          io.err(`  NOT FOUND ${id}`);
          failed += 1;
        } else if (r === false) skipped += 1;
        else tiered += 1;
      } catch (exc) {
        io.err(`  FAILED ${id}: ${msg(exc)}`);
        failed += 1;
        // A missing SDK fails every image identically: say so once and stop instead of printing it N times.
        if (/optional dependency '@aws-sdk\/client-s3'/.test(msg(exc))) {
          notAttempted = total - i;
          if (notAttempted > 0)
            io.err(
              `  Stopping: the remaining ${notAttempted} image(s) would fail the same way.`,
            );
          break;
        }
      }
      if (total > 1 && (i % 50 === 0 || i === total))
        io.out(`  ${i}/${total} processed...`);
    }
    io.out("");
    io.out(
      `Tiered: ${tiered}, already tiered (skipped): ${skipped}, failed: ${failed}${notAttempted > 0 ? `, not attempted: ${notAttempted}` : ""}`,
    );
    return failed > 0 ? 1 : 0;
  } finally {
    await backend.close();
  }
}

// ---- entry point --------------------------------------------------------------------------------------------

/** Run the CLI. Returns the process exit code instead of exiting, so it is testable. */
export async function main(
  argv: string[],
  io: CliIO = defaultIO(),
): Promise<number> {
  try {
    let parsed: ReturnType<
      typeof parseArgs<{ options: typeof OPTIONS; allowPositionals: true }>
    >;
    try {
      parsed = parseArgs({
        args: argv,
        options: OPTIONS,
        allowPositionals: true,
        strict: true,
      });
    } catch (exc) {
      throw usageError(
        `Error: ${msg(exc)}\nRun 'zerobucket --help' for usage.`,
      );
    }
    const { values: v, positionals } = parsed;
    if (v.version) {
      io.out(`zerobucket ${cliVersion()}`);
      return 0;
    }
    if (v.help) {
      io.out(HELP);
      return 0;
    }
    const command = positionals[0];
    if (!command) {
      io.err(HELP);
      return 2;
    }
    const allowed = COMMAND_OPTIONS[command];
    if (!allowed)
      throw usageError(
        `Error: unknown command ${JSON.stringify(command)}.\nRun 'zerobucket --help' for usage.`,
      );
    for (const name of Object.keys(v)) {
      if (!COMMON.has(name) && !allowed.includes(name))
        throw usageError(`Error: --${name} is not valid for '${command}'.`);
    }
    if (positionals.length > (command === "tier" ? 2 : 1))
      throw usageError(
        `Error: unexpected argument ${JSON.stringify(positionals[command === "tier" ? 2 : 1])}.`,
      );
    if (command !== "tier" && positionals.length > 1)
      throw usageError(
        `Error: unexpected argument ${JSON.stringify(positionals[1])}.`,
      );

    switch (command) {
      case "init":
        return await cmdInit(v, io.env, io);
      case "migrate":
        return await cmdMigrate(v, io.env, io);
      case "info":
        return await cmdInfo(v, io.env, io);
      case "verify":
        return await cmdVerify(v, io.env, io);
      default:
        return await cmdTier(v, positionals[1], io.env, io);
    }
  } catch (exc) {
    if (exc instanceof CliExit) {
      (exc.code === 1 && !exc.message.startsWith("Error") ? io.out : io.err)(
        exc.message,
      );
      return exc.code;
    }
    io.err(`Error: ${msg(exc)}`);
    return 1;
  }
}
