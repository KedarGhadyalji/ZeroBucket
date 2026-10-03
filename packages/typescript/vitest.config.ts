import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/**/*.test.ts"],
    testTimeout: 20_000,
    // Tests share one real Postgres database; run files serially.
    fileParallelism: false,
  },
});
