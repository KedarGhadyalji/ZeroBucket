/**
 * The executable that test helper processes should run.
 *
 * Several tests start a small helper process: one that holds a SQLite write lock, one that loads the library through
 * `--import tsx`, and so on. Those helpers are written for Node (`-e` scripts that `require()`, Node's `argv` layout, the
 * `--import` flag). On Node and Bun `process.execPath` runs them fine, and on Bun that keeps the helper on the runtime
 * under test. On Deno `process.execPath` is `deno`, which cannot, so the helpers run on a real Node there. The library
 * code under test still runs in the test process itself, on Deno.
 */
export const nodeBinary: string = process.versions.deno
  ? "node"
  : process.execPath;
