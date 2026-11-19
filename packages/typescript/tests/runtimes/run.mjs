/**
 * `npm run test:runtimes`: runs check.mjs on every JavaScript runtime found on this machine (Node always; Bun and Deno if
 * they are on PATH) and fails if any of them fails or does not exit by itself.
 *
 * Needs a built package (`npm run build`). PostgreSQL, MySQL/MariaDB and S3 checks run when the ZEROBUCKET_TEST_* settings
 * are in the environment, exactly as for `npm test`.
 */
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const check = fileURLToPath(new URL("./check.mjs", import.meta.url));
const runtimes = [
  { name: "Node", cmd: process.execPath, args: [check] },
  { name: "Bun", cmd: "bun", args: [check] },
  {
    name: "Deno",
    cmd: "deno",
    args: ["run", "-A", "--node-modules-dir=manual", check],
  },
];

let failed = 0;
for (const rt of runtimes) {
  const probe = spawnSync(rt.cmd, ["--version"], {
    encoding: "utf8",
    shell: process.platform === "win32",
  });
  if (probe.error || probe.status !== 0) {
    console.log(`\n=== ${rt.name}: not installed, skipped\n`);
    continue;
  }
  console.log(`\n=== ${rt.name}\n`);
  const r = spawnSync(rt.cmd, rt.args, {
    stdio: "inherit",
    timeout: 180_000,
    shell: process.platform === "win32",
  });
  if (r.error?.code === "ETIMEDOUT") {
    console.log(
      `\n${rt.name}: did not exit within 3 minutes (something kept the process alive)`,
    );
    failed++;
  } else if (r.status !== 0) {
    console.log(`\n${rt.name}: failed (exit ${r.status})`);
    failed++;
  }
}
console.log(
  failed === 0
    ? "\nAll available runtimes passed."
    : `\n${failed} runtime(s) failed.`,
);
process.exit(failed === 0 ? 0 : 1);
