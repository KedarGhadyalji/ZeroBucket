/**
 * 0.11.1: `olderThanDays` (and so `zerobucket tier --older-than`) is right whatever time zone the
 * MySQL session is in.
 *
 * created_at is stored as naive UTC, so the age comparison must use UTC_TIMESTAMP(), never NOW()
 * (NOW() is in the session's time zone). Until now every test ran against a UTC server, where NOW()
 * and UTC_TIMESTAMP() agree, so a wrong NOW() would have passed everything. Here every connection's
 * session time zone is set (no privilege needed) to near-extreme offsets, +13:00 (the most MariaDB
 * accepts) and -12:00, and the images are 23 and 25 hours old: with NOW() the +13:00 session counts
 * the 23-hour-old one as older than a day, and the -12:00 session misses the 25-hour-old one.
 */
import mysql, { type Pool } from "mysql2/promise";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  MySQLBackend,
  ZeroBucket,
  type MysqlPoolLike,
} from "../src/index.js";
import { makeImage } from "./helpers.js";

const URL = process.env.ZEROBUCKET_TEST_MYSQL_URL;
const TABLES = [
  "zerobucket_image_refs",
  "zerobucket_blobs",
  "zerobucket_images",
];
const HOUR = 3600 * 1000;

/** A naive-UTC DATETIME(6) string for `msAgo` milliseconds in the past. */
const utcAgo = (msAgo: number) =>
  new Date(Date.now() - msAgo)
    .toISOString()
    .replace("T", " ")
    .replace("Z", "");

describe.skipIf(!URL)("MySQL/MariaDB older-than under non-UTC session time zones", () => {
  let raw: Pool;

  const dropAll = async () => {
    const conn = await raw.getConnection();
    try {
      await conn.query("SET FOREIGN_KEY_CHECKS = 0");
      for (const t of TABLES) await conn.query(`DROP TABLE IF EXISTS ${t}`);
      await conn.query("SET FOREIGN_KEY_CHECKS = 1");
    } finally {
      conn.release();
    }
  };

  beforeEach(async () => {
    raw = mysql.createPool({ uri: URL!, connectionLimit: 2, timezone: "Z" });
    await dropAll();
  });
  afterEach(async () => {
    await dropAll();
    await raw.end();
  });

  for (const zone of ["+13:00", "-12:00", "+00:00"]) {
    for (const dedup of [false, true]) {
      it(`uses UTC, not the session's clock (session ${zone}, ${dedup ? "dedup" : "classic"})`, async () => {
        const pool = mysql.createPool({
          uri: URL!,
          connectionLimit: 4,
          timezone: "Z",
        });
        // Every connection this pool ever opens starts in the chosen session time zone.
        (pool as unknown as { pool: { on(e: string, f: (c: { query(s: string): void }) => void): void } }).pool.on(
          "connection",
          (conn) => conn.query(`SET time_zone = '${zone}'`),
        );
        const backend = new MySQLBackend({
          pool: pool as unknown as MysqlPoolLike,
          dedup,
        });
        const zb = new ZeroBucket({ backend });
        try {
          await zb.ready();
          // The setup must really have changed the session, or this test proves nothing.
          const [tz] = (await pool.query("SELECT @@session.time_zone AS tz")) as unknown as [
            { tz: string }[],
          ];
          expect(tz[0]!.tz).toBe(zone);

          const recent = await zb.put(await makeImage("png", 64, 48, { seed: 1 })); // 23 hours old
          const older = await zb.put(await makeImage("png", 64, 48, { seed: 2 })); // 25 hours old
          const ancient = await zb.put(await makeImage("png", 64, 48, { seed: 3 })); // 40 days old
          const table = dedup ? "zerobucket_image_refs" : "zerobucket_images";
          for (const [id, ago] of [
            [recent, 23 * HOUR],
            [older, 25 * HOUR],
            [ancient, 40 * 24 * HOUR],
          ] as const) {
            await raw.query(`UPDATE ${table} SET created_at = ? WHERE id = ?`, [utcAgo(ago), id]);
          }

          const found = await backend.listImages({ olderThanDays: 1 });
          expect(found.map((e) => e.id).sort()).toEqual([older, ancient].sort());
          // And the boundary the other way: nothing is older than 60 days.
          expect(await backend.listImages({ olderThanDays: 60 })).toEqual([]);
        } finally {
          await zb.close();
          await pool.end();
        }
      });
    }
  }
});
