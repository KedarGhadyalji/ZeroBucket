import { defineConfig } from "tsup";

const external = ["pg", "sharp", "@aws-sdk/client-s3", "better-sqlite3"];

export default defineConfig([
  {
    entry: { index: "src/index.ts", http: "src/http.ts" },
    format: ["esm", "cjs"],
    dts: true,
    clean: false, // dist/ is removed by the `build` script, so the parallel builds below cannot delete each other's output
    target: "node20",
    sourcemap: true,
    external,
  },
  {
    // The `zerobucket` command. ESM only (the package is "type": "module"), no type declarations needed.
    entry: { bin: "src/bin.ts" },
    format: ["esm"],
    dts: false,
    clean: false,
    target: "node20",
    sourcemap: false,
    banner: { js: "#!/usr/bin/env node" },
    external,
  },
]);
