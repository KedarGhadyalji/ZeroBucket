import { defineConfig } from "tsup";

export default defineConfig({
  entry: { index: "src/index.ts", http: "src/http.ts" },
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  target: "node20",
  sourcemap: true,
  external: ["pg", "sharp"],
});
