/**
 * `npm run test:env`: shows exactly what the test run will use, and tries to connect to each service.
 *
 * Run it when tests fail with "password authentication failed", "Access denied", or are mysteriously skipped. It reads the
 * same `.env` files the test run reads (see loadTestEnv.ts), prints which files were found and how they were decoded,
 * shows each setting with passwords hidden, then really connects to PostgreSQL, MySQL/MariaDB and the S3 endpoint and
 * reports what each one said. Output is plain ASCII on purpose, so a Windows console does not garble it.
 *
 * Exits 1 if a configured service cannot be reached with the settings it was given.
 */
import { existsSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import mysql from "mysql2/promise";
import pg from "pg";
import {
  BACKEND_KEYS,
  loadTestEnv,
  maskSecrets,
  readEnvFile,
} from "./loadTestEnv.js";

const packageDir = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const repoRoot = resolve(packageDir, "../..");
const dirs = [repoRoot, packageDir];

interface Probe {
  ok: boolean;
  detail: string;
  hint?: string;
}

const withTimeout = <T>(p: Promise<T>, ms: number, what: string): Promise<T> =>
  Promise.race([
    p,
    new Promise<T>((_, rej) =>
      setTimeout(
        () =>
          rej(
            Object.assign(
              new Error(`no answer from ${what} within ${ms / 1000}s`),
              { code: "ETIMEOUT" },
            ),
          ),
        ms,
      ),
    ),
  ]);

function networkHint(code: string | undefined): string | undefined {
  if (code === "ECONNREFUSED")
    return "nothing is listening on that host and port: is the server running, and is the port right?";
  if (code === "ENOTFOUND")
    return "the host name could not be resolved: check the spelling of the host in the URL";
  if (code === "ETIMEDOUT" || code === "ETIMEOUT")
    return "no answer: a firewall, a wrong host, or a server that is not accepting connections";
  return undefined;
}

async function probePostgres(url: string): Promise<Probe> {
  const client = new pg.Client({
    connectionString: url,
    connectionTimeoutMillis: 5000,
  });
  try {
    await withTimeout(client.connect(), 6000, "PostgreSQL");
    const r = await client.query(
      "select current_user as u, current_database() as d, version() as v",
    );
    const row = r.rows[0] as { u: string; d: string; v: string };
    return {
      ok: true,
      detail: `connected as "${row.u}" to database "${row.d}" (${row.v.split(",")[0]})`,
    };
  } catch (e) {
    const err = e as { code?: string; message: string };
    const hints: Record<string, string> = {
      "28P01":
        "the server rejected the password for that user. Fix the password in the URL (URL-encode special characters), or reset it in PostgreSQL with: ALTER USER <user> PASSWORD '...';",
      "28000":
        "the server does not allow this user, host and database combination (see pg_hba.conf)",
      "3D000":
        "the database does not exist yet. Create it: CREATE DATABASE zerobucket_test;",
    };
    return {
      ok: false,
      detail: `${err.code ?? "error"}: ${err.message}`,
      hint: hints[err.code ?? ""] ?? networkHint(err.code),
    };
  } finally {
    await client.end().catch(() => undefined);
  }
}

async function probeMysql(url: string): Promise<Probe> {
  let conn: Awaited<ReturnType<typeof mysql.createConnection>> | undefined;
  try {
    conn = await withTimeout(
      mysql.createConnection({ uri: url, connectTimeout: 5000 }),
      6000,
      "MySQL",
    );
    const [rows] = await conn.query(
      "SELECT CURRENT_USER() AS u, DATABASE() AS d, VERSION() AS v",
    );
    const row = (rows as { u: string; d: string; v: string }[])[0]!;
    return {
      ok: true,
      detail: `connected as ${row.u} to database "${row.d}" (${row.v})`,
    };
  } catch (e) {
    const err = e as { code?: string; message: string };
    const hints: Record<string, string> = {
      ER_ACCESS_DENIED_ERROR:
        "the server rejected this user or password. The account name in the error is how the server sees the connection (it says 'localhost' even for 127.0.0.1), so check the password, and that the account exists for that host",
      ER_DBACCESS_DENIED_ERROR:
        "the user exists but may not use this database: GRANT ALL ON zerobucket_test.* TO the user",
      ER_BAD_DB_ERROR:
        "the database does not exist yet. Create it: CREATE DATABASE zerobucket_test;",
    };
    return {
      ok: false,
      detail: `${err.code ?? "error"}: ${err.message}`,
      hint: hints[err.code ?? ""] ?? networkHint(err.code),
    };
  } finally {
    await conn?.end().catch(() => undefined);
  }
}

async function probeHttp(url: string): Promise<Probe> {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    return { ok: true, detail: `reachable (HTTP ${res.status})` };
  } catch (e) {
    const err = e as { cause?: { code?: string }; message: string };
    const code = err.cause?.code;
    return {
      ok: false,
      detail: `${code ?? "error"}: ${err.message}`,
      hint:
        networkHint(code) ??
        "is the S3 emulator (for example moto_server -p 5000) running?",
    };
  }
}

async function main(): Promise<number> {
  console.log(`node ${process.version} on ${process.platform}`);
  console.log(`repo root:   ${repoRoot}`);
  console.log(`package dir: ${packageDir}\n`);

  console.log("Files looked for (later ones win):");
  let anyFile = false;
  for (const dir of dirs) {
    for (const name of [".env", ".env.local"]) {
      const file = join(dir, name);
      const shown = relative(repoRoot, file) || name;
      if (existsSync(file)) {
        anyFile = true;
        const info = readEnvFile(file, repoRoot);
        console.log(
          `  FOUND    ${shown}  (${info.bytes} bytes, ${info.encoding}, ${Object.keys(info.vars).length} KEY=value lines)`,
        );
      } else if (existsSync(file + ".txt")) {
        console.log(
          `  MISSING  ${shown}  <- but ${shown}.txt exists: rename it, dropping the .txt`,
        );
      } else {
        console.log(`  missing  ${shown}`);
      }
    }
  }
  if (!anyFile)
    console.log(
      "  (no .env file found anywhere: create one from .env.example at the repo root)",
    );

  const loaded = loadTestEnv(dirs, process.env, repoRoot);
  console.log("\nWhat the test run reports at startup:");
  for (const line of loaded.lines) console.log("  " + line);

  const ignoring = Boolean(process.env.ZEROBUCKET_IGNORE_DOTENV);
  const effective = (k: string) =>
    ignoring ? process.env[k] : (loaded.env[k] ?? process.env[k]);

  console.log("\nConnecting with those settings:");
  let failures = 0;
  const run = async (
    key: string,
    label: string,
    probe: (v: string) => Promise<Probe>,
  ) => {
    const value = effective(key);
    if (!value) {
      console.log(
        `  --  ${label}: ${key} is not set, so ${BACKEND_KEYS[key]} will be SKIPPED (a skipped test is not a passed one)`,
      );
      return;
    }
    const r = await probe(value);
    console.log(`  ${r.ok ? "OK  " : "FAIL"} ${label}: ${maskSecrets(value)}`);
    console.log(`       ${r.detail}`);
    if (r.hint) console.log(`       -> ${r.hint}`);
    if (!r.ok) failures++;
  };
  await run("ZEROBUCKET_TEST_DATABASE_URL", "PostgreSQL", probePostgres);
  await run("ZEROBUCKET_TEST_MYSQL_URL", "MySQL/MariaDB", probeMysql);
  await run("ZEROBUCKET_TEST_S3_ENDPOINT", "S3 endpoint", probeHttp);

  console.log(
    failures === 0
      ? "\nNothing configured here is unreachable. If the tests still fail, paste this whole output."
      : `\n${failures} service(s) rejected the settings above. Fix those, then run this again before running the tests.`,
  );
  return failures === 0 ? 0 : 1;
}

main().then(
  (code) => process.exit(code),
  (e) => {
    console.error(e);
    process.exit(2);
  },
);
