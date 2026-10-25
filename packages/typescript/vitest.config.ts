import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { configDefaults, defineConfig } from "vitest/config";
import { loadTestEnv } from "./tests/support/loadTestEnv.js";

// Tests read their database settings from process.env. Vitest does not load .env files into it, so do that here: the repo
// root's .env (the one .env.example tells you to create) and an optional one in this package. See the module for details.
const packageDir = fileURLToPath(new URL(".", import.meta.url));
const repoRoot = resolve(packageDir, "../..");
const testEnv = loadTestEnv([repoRoot, packageDir], process.env, repoRoot);
// Printed once per run (the config can be evaluated more than once, for example in watch mode).
const logged = globalThis as { __zerobucketEnvLogged?: boolean };
if (testEnv.lines.length > 0 && !logged.__zerobucketEnvLogged) {
  logged.__zerobucketEnvLogged = true;
  for (const line of testEnv.lines) console.log(line);
}

// better-sqlite3 13 requires Node >= 22 and hard-crashes (segfault) on older Node, which would take the
// whole test run down. Everything else in this package is tested on Node 20 as well.
const nodeMajor = Number(process.versions.node.split(".")[0]);
const sqliteSupported = nodeMajor >= 22;

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    exclude: sqliteSupported
      ? configDefaults.exclude
      : [...configDefaults.exclude, "tests/sqlite.test.ts"],
    env: testEnv.env,
    testTimeout: 20_000,
    // Tests share one real Postgres database; run files serially.
    fileParallelism: false,
  },
});
