/**
 * Optional HEIC/HEIF support (iPhone photos).
 *
 * sharp's prebuilt libvips cannot decode HEIC (HEVC is patent-encumbered), so HEIC is handled by the optional
 * peer dependency `libheif-js` (libheif + libde265 compiled to WebAssembly).
 *
 * WHY A WORKER THREAD: the WASM decoder is synchronous CPU work. Measured: a 12-megapixel photo takes ~2 seconds.
 * Run on the main thread, every iPhone upload would freeze the whole server for that long. The decoder therefore
 * lives in one long-lived worker thread (unref'd, so it never keeps your process alive) and is called through
 * promises. sharp itself uses libuv's thread pool the same way.
 *
 * LICENSE NOTE: libheif-js is LGPL-3.0. It is not bundled with or redistributed by this package: you install it
 * yourself (`npm install libheif-js`), which is what keeps this package MIT.
 *
 * LIMITS (honest): decoding only. There is no HEVC encoder in the WASM build, so `optimize` can convert FROM HEIC
 * to JPEG/WebP/PNG but not TO HEIC (Python's pillow-heif can). If a file holds several images, the first
 * top-level image is used.
 */
import { createRequire } from "node:module";
import { join } from "node:path";
import { Worker } from "node:worker_threads";
import { StorageError } from "./errors.js";

/** ISO-BMFF `ftyp` major brands that identify HEIC/HEIF. Identical to the Python package's list. */
const HEIC_BRANDS = new Set([
  "heic",
  "heix",
  "hevc",
  "heim",
  "heis",
  "mif1",
  "msf1",
]);

/** True if the bytes start with an `ftyp` box carrying a HEIC/HEIF brand (content sniffing only; says nothing about validity). */
export function looksLikeHeic(data: Uint8Array): boolean {
  if (data.length < 12) return false;
  if (String.fromCharCode(data[4]!, data[5]!, data[6]!, data[7]!) !== "ftyp")
    return false;
  return HEIC_BRANDS.has(
    String.fromCharCode(data[8]!, data[9]!, data[10]!, data[11]!),
  );
}

export const HEIC_INSTALL_HINT = "npm install libheif-js";

// Runs inside the worker (CommonJS, evaluated from a string so it needs no separate file in either build format).
const WORKER_SOURCE = `
const { parentPort, workerData } = require("node:worker_threads");
const libheif = require(workerData.libPath);
// libheif-js reports decode problems by dumping a raw error object to the console. Capture that text instead:
// it becomes part of the error we return (the useful diagnosis), and nothing is printed to the host's stderr.
let diag = "";
for (const level of ["log", "info", "warn", "error"]) {
  console[level] = (...args) => {
    diag = args.map((a) => (a && a.message) || (typeof a === "string" ? a : "")).filter(Boolean).join(" ").trim();
  };
}
const describe = (e) => {
  const base = String((e && e.message) || e);
  return diag && !base.includes(diag) ? base + ": " + diag : base;
};
function open(bytes) {
  const images = new libheif.HeifDecoder().decode(bytes);
  if (!images || images.length === 0) throw new Error("no images found in HEIF container");
  return images[0];
}
parentPort.on("message", async ({ id, op, data }) => {
  diag = "";
  try {
    const img = open(new Uint8Array(data));
    const width = img.get_width();
    const height = img.get_height();
    if (op === "info") return parentPort.postMessage({ id, ok: true, width, height });
    const out = await new Promise((resolve, reject) =>
      img.display({ data: new Uint8ClampedArray(width * height * 4), width, height }, (d) =>
        d ? resolve(d) : reject(new Error("HEIF processing error")),
      ),
    );
    if (op === "validate") return parentPort.postMessage({ id, ok: true, width, height });
    const rgba = out.data.buffer;
    parentPort.postMessage({ id, ok: true, width, height, rgba }, [rgba]);
  } catch (e) {
    parentPort.postMessage({ id, ok: false, error: describe(e) });
  }
});
`;

/** The decoder worker itself failed (crashed or was killed). That is an infrastructure fault, not a bad image. */
export class HeicWorkerError extends StorageError {}

/** `owner` ties a job to the worker that is running it, so one worker's death can never fail another worker's jobs. */
type Pending = { owner: Worker; resolve(v: any): void; reject(e: Error): void };

let worker: Worker | undefined;
let nextId = 1;
const pending = new Map<number, Pending>();
const stats = { info: 0, validate: 0, decode: 0, workersStarted: 0 };

/** How the library path is found. Overridable for tests only. */
let resolveLib: () => string = () => {
  const attempts: (() => string)[] = [
    () => createRequire(import.meta.url).resolve("libheif-js/wasm-bundle"), // next to / above this package
    () =>
      createRequire(join(process.cwd(), "noop.js")).resolve(
        "libheif-js/wasm-bundle",
      ), // the app's own node_modules
  ];
  let last: unknown;
  for (const attempt of attempts) {
    try {
      return attempt();
    } catch (e) {
      last = e;
    }
  }
  throw last;
};

/** Whether `libheif-js` can be found. */
export function heicAvailable(): boolean {
  try {
    resolveLib();
    return true;
  } catch {
    return false;
  }
}

/**
 * Reference lifecycle: the worker is ref'd ONLY while it has jobs in flight and unref'd when idle.
 *  - always ref'd: an idle decoder would keep your process (a CLI, a script, a test run) from ever exiting;
 *  - always unref'd: the process could exit in the middle of a decode, since an unfinished promise does not
 *    keep Node alive on its own.
 */
function releaseIfIdle(w: Worker): void {
  for (const p of pending.values()) if (p.owner === w) return;
  w.unref();
}

function startWorker(): Worker {
  let libPath: string;
  try {
    libPath = resolveLib();
  } catch (cause) {
    throw new StorageError(
      `HEIC support requires the optional dependency 'libheif-js'. Install it with: ${HEIC_INSTALL_HINT}`,
      { cause },
    );
  }
  const w = new Worker(WORKER_SOURCE, { eval: true, workerData: { libPath } });
  stats.workersStarted += 1;
  w.on(
    "message",
    (
      m: { id: number; ok: boolean; error?: string } & Record<string, unknown>,
    ) => {
      const p = pending.get(m.id);
      if (!p) return;
      pending.delete(m.id);
      releaseIfIdle(w);
      if (m.ok) p.resolve(m);
      else p.reject(new Error(m.error));
    },
  );
  const fail = (err: Error) => {
    if (worker === w) worker = undefined; // next call starts a fresh worker
    // A dying worker emits 'error' and 'exit' separately, and a replacement may already be running by the time the
    // second one arrives. Only ever fail the jobs THIS worker owned.
    for (const [id, p] of pending) {
      if (p.owner !== w) continue;
      pending.delete(id);
      p.reject(err);
    }
  };
  w.on("error", (e) =>
    fail(
      new HeicWorkerError(
        `HEIC decoder worker crashed: ${e instanceof Error ? e.message : String(e)}`,
        { cause: e },
      ),
    ),
  );
  w.on("exit", (code) =>
    fail(
      new HeicWorkerError(
        `HEIC decoder worker exited unexpectedly (code ${code})`,
      ),
    ),
  );
  w.unref(); // an idle decoder must never keep the host process alive (after listeners: adding them can re-ref the port)
  return w;
}

function call(
  op: "info" | "validate" | "decode",
  data: Uint8Array,
): Promise<any> {
  stats[op] += 1;
  const w = (worker ??= startWorker());
  const id = nextId++;
  // Copy: the caller's Buffer may be a view into a shared pool, and the copy is transferred without a second copy.
  const copy = new Uint8Array(data).buffer;
  return new Promise((resolve, reject) => {
    pending.set(id, { owner: w, resolve, reject });
    w.ref(); // keep the process alive until this job settles
    w.postMessage({ id, op, data: copy }, [copy]);
  });
}

/** Read the dimensions from the container header. Cheap: no pixels are decoded. */
export async function heicInfo(
  data: Uint8Array,
): Promise<{ width: number; height: number }> {
  const r = await call("info", data);
  return { width: r.width, height: r.height };
}

/** Fully decode every pixel (to catch truncated/corrupted bodies) but return only the dimensions. */
export async function heicValidate(
  data: Uint8Array,
): Promise<{ width: number; height: number }> {
  const r = await call("validate", data);
  return { width: r.width, height: r.height };
}

/** Decode to raw RGBA pixels (4 bytes per pixel, already rotated/mirrored per the file's transform boxes). */
export async function decodeHeic(
  data: Uint8Array,
): Promise<{ width: number; height: number; rgba: Buffer }> {
  const r = await call("decode", data);
  return {
    width: r.width,
    height: r.height,
    rgba: Buffer.from(r.rgba as ArrayBuffer),
  };
}

/** Test hooks. Not exported from the package entry point. */
export const _testing = {
  stats,
  setResolver(fn: (() => string) | undefined): void {
    resolveLib = fn ?? _defaultResolver;
    void worker?.terminate();
    worker = undefined;
  },
  terminateWorker(): Promise<number> | undefined {
    return worker?.terminate();
  },
  hasWorker: () => worker !== undefined,
};
const _defaultResolver = resolveLib;
