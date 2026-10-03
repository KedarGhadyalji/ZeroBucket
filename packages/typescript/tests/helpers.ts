import type pg from "pg";
import sharp from "sharp";
import { ZeroBucket, type ZeroBucketOptions } from "../src/index.js";

export const DATABASE_URL = process.env.ZEROBUCKET_TEST_DATABASE_URL;

export type Fmt = "jpeg" | "png" | "webp";

/** A real, decodable test image with deterministic-ish noisy content (so it does not compress to nothing). */
export async function makeImage(
  fmt: Fmt = "png",
  width = 64,
  height = 48,
  opts: { alpha?: boolean; seed?: number } = {},
): Promise<Buffer> {
  const channels = opts.alpha ? 4 : 3;
  const raw = Buffer.alloc(width * height * channels);
  let s = (opts.seed ?? 1) >>> 0 || 1;
  for (let i = 0; i < raw.length; i++) {
    s = (s * 1664525 + 1013904223) >>> 0;
    raw[i] = (s >>> 24) & 0xff;
  }
  return sharp(raw, { raw: { width, height, channels } })[fmt]().toBuffer();
}

export async function makeGif(): Promise<Buffer> {
  return sharp({
    create: { width: 8, height: 8, channels: 3, background: "#f00" },
  })
    .gif()
    .toBuffer();
}

export function newBucket<C = unknown>(
  opts: ZeroBucketOptions<C> = {},
): ZeroBucket<C> {
  return new ZeroBucket<C>({ connectionString: DATABASE_URL, ...opts });
}

export const fail = (code: string, message = "injected") =>
  Object.assign(new Error(message), { code });

/** Wrap a real pool, letting a test inject failures into connect() and into specific statements. */
export function flakyPool(
  real: pg.Pool,
  plan: {
    connectFailures?: string[];
    failOnce?: (text: string) => string | undefined;
  } = {},
) {
  const stats = { connects: 0, injected: 0 };
  const failures = [...(plan.connectFailures ?? [])];
  const used = new Set<string>();
  const proxy = new Proxy(real, {
    get(target, prop, recv) {
      if (prop === "connect") {
        return async () => {
          stats.connects++;
          const code = failures.shift();
          if (code) {
            stats.injected++;
            throw fail(code);
          }
          const client = await target.connect();
          return new Proxy(client, {
            get(c, p, r) {
              if (p === "query") {
                return async (text: unknown, ...rest: unknown[]) => {
                  const t = typeof text === "string" ? text : "";
                  const code = plan.failOnce?.(t);
                  if (code && !used.has(t)) {
                    used.add(t);
                    stats.injected++;
                    await (c.query as any)("ROLLBACK"); // server never saw a commit
                    throw fail(code);
                  }
                  return (c.query as any)(text, ...rest);
                };
              }
              const v = Reflect.get(c, p, r);
              return typeof v === "function" ? v.bind(c) : v;
            },
          });
        };
      }
      const v = Reflect.get(target, prop, recv);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
  return { pool: proxy as unknown as pg.Pool, stats };
}
