import { configDefaults, defineConfig } from "vitest/config";

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
    testTimeout: 20_000,
    // Tests share one real Postgres database; run files serially.
    fileParallelism: false,
  },
});
