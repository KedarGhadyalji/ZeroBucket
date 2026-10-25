/**
 * Loads `.env` files for the test run.
 *
 * Vitest does not put non-`VITE_` variables from `.env` files into `process.env`, so the documented "copy .env.example
 * to .env" workflow silently did nothing: tests read `process.env.ZEROBUCKET_TEST_*` directly, skip a backend when it is
 * unset, and use whatever stale value an old `set` / `setx` left in the shell. This module closes that gap with no new
 * dependency, and is deliberately noisy about everything it cannot find or read, because the failures it prevents were
 * all silent ones.
 *
 * Files, lowest priority first (later wins): the repo root's `.env` and `.env.local`, then the same two in
 * `packages/typescript/`. A value from a file wins over one inherited from the shell, because the stale shell value is
 * the failure this exists to fix; the run says so when that happens. Set `ZEROBUCKET_IGNORE_DOTENV=1` to skip the files and
 * let the shell rule for one run. Only `ZEROBUCKET_*` and `AWS_*` keys are read, so a stray `.env` cannot change anything
 * else. In CI (`CI` is set) there are no `.env` files and this prints nothing.
 */
import { existsSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";

const ALLOWED_KEY = /^(ZEROBUCKET_|AWS_)/;
/** Keys whose value is a credential on its own (AWS_SECRET_ACCESS_KEY and the like), not a URL with a password in it. */
const SECRET_KEY = /SECRET|PASSWORD|TOKEN|KEY/i;
const PLACEHOLDER =
  /your[_ -]?(real[_ -]?)?(mysql[_ -]?)?password|YOUR_[A-Z_]+/i;
/** The settings that switch a whole group of tests on. When one is missing those tests are skipped, not failed. */
export const BACKEND_KEYS: Record<string, string> = {
  ZEROBUCKET_TEST_DATABASE_URL: "the PostgreSQL tests",
  ZEROBUCKET_TEST_MYSQL_URL: "the MySQL/MariaDB tests",
  ZEROBUCKET_TEST_S3_ENDPOINT: "the object-storage (tiering) tests",
};
const ENCODING_HELP =
  "a special character in the password probably needs URL-encoding (@ is %40, : is %3A, / is %2F, # is %23, % is %25)";

export type Encoding = "utf8" | "utf16le" | "utf16be";

/**
 * Decodes the bytes of an env file. Windows PowerShell's `echo X > .env` writes UTF-16 with a byte-order mark, and
 * Notepad can too; read as UTF-8 that is unreadable, so a BOM (or an obvious UTF-16 pattern without one) is honoured.
 */
export function decodeEnvBuffer(buf: Buffer): {
  text: string;
  encoding: Encoding;
} {
  if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe)
    return { text: buf.subarray(2).toString("utf16le"), encoding: "utf16le" };
  if (buf.length >= 2 && buf[0] === 0xfe && buf[1] === 0xff)
    return {
      text: Buffer.from(buf.subarray(2)).swap16().toString("utf16le"),
      encoding: "utf16be",
    };
  if (
    buf.length >= 4 &&
    buf[0] !== 0 &&
    buf[1] === 0 &&
    buf[2] !== 0 &&
    buf[3] === 0
  )
    return { text: buf.toString("utf16le"), encoding: "utf16le" };
  if (
    buf.length >= 4 &&
    buf[0] === 0 &&
    buf[1] !== 0 &&
    buf[2] === 0 &&
    buf[3] !== 0
  )
    return {
      text: Buffer.from(buf).swap16().toString("utf16le"),
      encoding: "utf16be",
    };
  return {
    text: buf.toString("utf8").replace(/^\uFEFF/, ""),
    encoding: "utf8",
  };
}

/** Parses `KEY=value` lines: `#` comment lines, an optional `export `, single or double quotes, CRLF and a BOM. */
export function parseDotenv(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const raw of text.replace(/^\uFEFF/, "").split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const m = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$/.exec(line);
    if (!m) continue;
    let value = m[2]!.trim();
    const quote = value[0];
    if (
      (quote === '"' || quote === "'") &&
      value.length >= 2 &&
      value.endsWith(quote)
    ) {
      value = value.slice(1, -1);
    } else {
      // An unquoted value may carry a trailing " # comment". A bare '#' inside a value is kept.
      const hash = value.search(/\s#/);
      if (hash >= 0) value = value.slice(0, hash).trimEnd();
    }
    out[m[1]!] = value;
  }
  return out;
}

/**
 * `postgresql://user:secret@host/db` becomes `postgresql://user:***@host/db`, so the run can show what it used.
 * Everything between the user name and the LAST `@` is hidden, so a password that wrongly contains an unencoded `@` or `/`
 * is still hidden in full instead of being printed in part.
 */
export function maskSecrets(value: string): string {
  const m = /^([a-z][a-z0-9+.-]*:\/\/[^:/@\s]*:)(.*)$/is.exec(value);
  if (!m) return value;
  const lastAt = m[2]!.lastIndexOf("@");
  return lastAt < 0 ? value : `${m[1]}***${m[2]!.slice(lastAt)}`;
}

/** How to show a value in the run's output: a secret key is hidden entirely, a URL has only its password hidden. */
function show(key: string, value: string): string {
  return SECRET_KEY.test(key) ? "***" : maskSecrets(value);
}

/** Problems worth telling the user about a value, without printing it. */
export function lintValue(value: string): string[] {
  const problems: string[] = [];
  if (PLACEHOLDER.test(value)) {
    problems.push(
      "still looks like the .env.example placeholder, so put your real value in",
    );
  }
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(value)) {
    let valid = true;
    try {
      new URL(value);
    } catch {
      valid = false;
    }
    const authority = value.slice(value.indexOf("//") + 2).split(/[/?#]/)[0]!;
    if (!valid || authority.split("@").length > 2) {
      problems.push(`is not a valid URL: ${ENCODING_HELP}. Check`);
    }
  }
  return problems;
}

export interface EnvFileInfo {
  /** Path as shown to the user, relative to the repo root. */
  shown: string;
  bytes: number;
  encoding: Encoding;
  /** Every KEY=value line in the file, before the ZEROBUCKET_/AWS_ filter. */
  vars: Record<string, string>;
}

export function readEnvFile(file: string, root: string): EnvFileInfo {
  const buf = readFileSync(file);
  const { text, encoding } = decodeEnvBuffer(buf);
  return {
    shown: relative(root, file) || file,
    bytes: buf.length,
    encoding,
    vars: parseDotenv(text),
  };
}

export interface LoadedTestEnv {
  /** Values to apply to `process.env` for the tests (file values only; the shell's other variables are untouched). */
  env: Record<string, string>;
  /** Human-readable lines describing what was loaded, empty when there was nothing to report. */
  lines: string[];
}

/**
 * @param dirs   directories to look in, lowest priority first
 * @param shell  the environment the run inherited (injectable for tests)
 * @param root   used only to print short relative paths
 */
export function loadTestEnv(
  dirs: string[],
  shell: NodeJS.ProcessEnv = process.env,
  root: string = process.cwd(),
): LoadedTestEnv {
  if (shell.ZEROBUCKET_IGNORE_DOTENV) {
    return {
      env: {},
      lines: [
        "zerobucket tests: ZEROBUCKET_IGNORE_DOTENV is set, not reading .env files",
      ],
    };
  }

  const env: Record<string, string> = {};
  const source: Record<string, string> = {};
  const found: EnvFileInfo[] = [];
  const hints: string[] = [];
  for (const dir of dirs) {
    for (const name of [".env", ".env.local"]) {
      const file = join(dir, name);
      if (!existsSync(file)) {
        // Notepad and some editors append .txt without saying so, which makes the file invisible to every loader.
        if (existsSync(file + ".txt")) {
          hints.push(
            `  found ${relative(root, file + ".txt") || file + ".txt"}: it must be named ${name}, without .txt (turn on "File name extensions" in Explorer to see this)`,
          );
        }
        continue;
      }
      const info = readEnvFile(file, root);
      found.push(info);
      for (const [k, v] of Object.entries(info.vars)) {
        if (!ALLOWED_KEY.test(k)) continue;
        env[k] = v;
        source[k] = info.shown;
      }
    }
  }

  const inCI = Boolean(shell.CI);
  const lines: string[] = [];
  if (found.length > 0) {
    lines.push(
      `zerobucket tests: read ${found.map((f) => f.shown).join(", ")}`,
    );
    for (const f of found) {
      const usable = Object.keys(f.vars).filter((k) => ALLOWED_KEY.test(k));
      if (usable.length === 0) {
        lines.push(
          `  ${f.shown}: no ZEROBUCKET_* or AWS_* settings found (${f.bytes} bytes read as ${f.encoding}, ${Object.keys(f.vars).length} KEY=value lines)`,
        );
      } else if (f.encoding !== "utf8") {
        lines.push(`  ${f.shown} is ${f.encoding}-encoded; read it as such`);
      }
    }
  } else if (!inCI) {
    lines.push(
      `zerobucket tests: no .env found (looked for .env and .env.local in ${dirs.map((d) => relative(root, d) || ".").join(" and ")}); using the variables from your shell`,
    );
  }
  lines.push(...hints);

  for (const k of Object.keys(env).sort()) {
    const notes: string[] = [`from ${source[k]}`];
    const inherited = shell[k];
    if (inherited !== undefined && inherited !== env[k]) {
      // Passwords are masked, so say so when that is the only difference instead of showing two identical lines.
      notes.push(
        show(k, inherited) === show(k, env[k]!)
          ? "replaces a value in your shell that differs only in the secret part"
          : `replaces a different value set in your shell: ${show(k, inherited)}`,
      );
    }
    lines.push(`  ${k}=${show(k, env[k]!)}  (${notes.join("; ")})`);
    for (const problem of lintValue(env[k]!)) {
      lines.push(`  warning: ${k} ${problem} ${source[k]}`);
    }
  }

  // Without a file, show what the shell is supplying, so a stale value is visible rather than silently used.
  if (found.length === 0 && !inCI) {
    for (const k of Object.keys(BACKEND_KEYS)) {
      if (shell[k] !== undefined) {
        lines.push(`  ${k}=${show(k, shell[k]!)}  (from your shell)`);
        for (const problem of lintValue(shell[k]!)) {
          lines.push(`  warning: ${k} ${problem} your shell setting`);
        }
      }
    }
  }

  // A missing setting means skipped tests, which look like passing ones. Say so, outside CI.
  if (!inCI) {
    for (const [k, what] of Object.entries(BACKEND_KEYS)) {
      if (env[k] === undefined && shell[k] === undefined) {
        lines.push(`  ${k} is not set: ${what} will be skipped`);
      }
    }
  }
  return { env, lines };
}
