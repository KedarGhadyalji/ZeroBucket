/** The .env loader used by vitest.config.ts (tests/support/loadTestEnv.ts). */
import { describe, expect, it } from "vitest";
import { mkdtempSync, mkdirSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  decodeEnvBuffer,
  lintValue,
  loadTestEnv,
  maskSecrets,
  parseDotenv,
} from "./support/loadTestEnv.js";

const tmp = () => mkdtempSync(join(tmpdir(), "zb-env-"));

describe("parseDotenv", () => {
  it("reads KEY=value, ignoring comments, blank lines and an export prefix", () => {
    expect(
      parseDotenv(
        "# note\n\nA=1\nexport B=two\n  C = spaced  \nnot a setting\n",
      ),
    ).toEqual({ A: "1", B: "two", C: "spaced" });
  });

  it("handles Windows line endings and a byte-order mark", () => {
    expect(parseDotenv("\uFEFFA=1\r\nB=2\r\n")).toEqual({ A: "1", B: "2" });
  });

  it("strips matching quotes, and keeps '=' and '#' that are part of a value", () => {
    expect(
      parseDotenv(
        [
          `Q1="quoted value"`,
          `Q2='single'`,
          `URL=postgresql://u:p%23x@h:5432/db?a=b`,
          `HASH=abc#def`,
          `TRAIL=value # a trailing comment`,
          `KEEP="not # a comment"`,
        ].join("\n"),
      ),
    ).toEqual({
      Q1: "quoted value",
      Q2: "single",
      URL: "postgresql://u:p%23x@h:5432/db?a=b",
      HASH: "abc#def",
      TRAIL: "value",
      KEEP: "not # a comment",
    });
  });

  it("allows an empty value", () => {
    expect(parseDotenv("EMPTY=\n")).toEqual({ EMPTY: "" });
  });
});

describe("maskSecrets", () => {
  it("hides a password that itself contains an unencoded '@'", () => {
    expect(maskSecrets("postgresql://u:p@ss@localhost:5432/db")).toBe(
      "postgresql://u:***@localhost:5432/db",
    );
  });

  it("hides the password in a URL but keeps user, host and database", () => {
    expect(maskSecrets("postgresql://postgres:p%40ss@localhost:5432/db")).toBe(
      "postgresql://postgres:***@localhost:5432/db",
    );
    expect(
      maskSecrets("mysql://zerobucket:Zb-Real-2026@127.0.0.1:3306/t"),
    ).toBe("mysql://zerobucket:***@127.0.0.1:3306/t");
  });

  it("leaves text without credentials alone", () => {
    expect(maskSecrets("http://localhost:5000")).toBe("http://localhost:5000");
    expect(maskSecrets("sqlite:///tmp/x.db")).toBe("sqlite:///tmp/x.db");
  });
});

describe("decodeEnvBuffer", () => {
  const text = "ZEROBUCKET_X=1\r\nZEROBUCKET_Y=two\r\n";
  it.each([
    ["UTF-8", Buffer.from(text, "utf8"), "utf8"],
    [
      "UTF-8 with a byte-order mark",
      Buffer.concat([Buffer.from([0xef, 0xbb, 0xbf]), Buffer.from(text)]),
      "utf8",
    ],
    [
      "UTF-16LE with a byte-order mark (PowerShell)",
      Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(text, "utf16le")]),
      "utf16le",
    ],
    ["UTF-16LE without one", Buffer.from(text, "utf16le"), "utf16le"],
    [
      "UTF-16BE with a byte-order mark",
      Buffer.concat([
        Buffer.from([0xfe, 0xff]),
        Buffer.from(text, "utf16le").swap16(),
      ]),
      "utf16be",
    ],
  ])("reads %s", (_name, buf, encoding) => {
    const out = decodeEnvBuffer(buf);
    expect(out.encoding).toBe(encoding);
    expect(parseDotenv(out.text)).toEqual({
      ZEROBUCKET_X: "1",
      ZEROBUCKET_Y: "two",
    });
  });
});

describe("lintValue", () => {
  it("passes a well-formed, encoded URL and plain values", () => {
    expect(
      lintValue("postgresql://postgres:p%40ss%3Aw%2Frd@localhost:5432/db"),
    ).toEqual([]);
    expect(lintValue("http://localhost:5000")).toEqual([]);
    expect(lintValue("us-east-1")).toEqual([]);
  });
  it("flags unencoded '@' and '/' in a password", () => {
    expect(lintValue("postgresql://u:p@ss@h/db").join()).toContain(
      "not a valid URL",
    );
    expect(lintValue("mysql://u:a/b@h:3306/db").join()).toContain(
      "not a valid URL",
    );
  });
});

describe("loadTestEnv", () => {
  it("returns nothing, and prints nothing, when there are no .env files in CI", () => {
    expect(loadTestEnv([tmp()], { CI: "true" })).toEqual({
      env: {},
      lines: [],
    });
  });

  it("says plainly that no .env was found, and what the shell is supplying instead (outside CI)", () => {
    const dir = tmp();
    const out = loadTestEnv(
      [dir],
      {
        ZEROBUCKET_TEST_DATABASE_URL:
          "postgresql://postgres:stale@localhost/db",
      },
      dir,
    );
    expect(out.env).toEqual({});
    const text = out.lines.join("\n");
    expect(text).toContain("no .env found");
    expect(text).toContain(
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:***@localhost/db  (from your shell)",
    );
    expect(text).not.toContain("stale");
  });

  it("warns when the file is really called .env.txt", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env.txt"),
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://u:p@h/db\n",
    );
    const text = loadTestEnv([dir], {}, dir).lines.join("\n");
    expect(text).toContain("no .env found");
    expect(text).toMatch(/found .*\.env\.txt: it must be named \.env/);
  });

  it("announces which test groups will be skipped because a setting is missing", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://u:p@h/db\n",
    );
    const text = loadTestEnv([dir], {}, dir).lines.join("\n");
    expect(text).toContain(
      "ZEROBUCKET_TEST_MYSQL_URL is not set: the MySQL/MariaDB tests will be skipped",
    );
    expect(text).toContain("ZEROBUCKET_TEST_S3_ENDPOINT is not set");
    expect(text).not.toContain("ZEROBUCKET_TEST_DATABASE_URL is not set");
    // ...and a setting that only the shell provides counts as set.
    expect(
      loadTestEnv(
        [dir],
        { ZEROBUCKET_TEST_MYSQL_URL: "mysql://u:p@h/db" },
        dir,
      ).lines.join("\n"),
    ).not.toContain("ZEROBUCKET_TEST_MYSQL_URL is not set");
  });

  it("reads a UTF-16 .env, as Windows PowerShell's `echo X > .env` writes it", () => {
    const dir = tmp();
    const text =
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:secret@localhost/db\r\n";
    writeFileSync(
      join(dir, ".env"),
      Buffer.concat([Buffer.from([0xff, 0xfe]), Buffer.from(text, "utf16le")]),
    );
    const out = loadTestEnv(
      [dir],
      { ZEROBUCKET_TEST_DATABASE_URL: "stale" },
      dir,
    );
    expect(out.env.ZEROBUCKET_TEST_DATABASE_URL).toBe(
      "postgresql://postgres:secret@localhost/db",
    );
    expect(out.lines.join("\n")).toContain("utf16le-encoded");
  });

  it("explains a file it could read but found nothing usable in, with its size and encoding", () => {
    const dir = tmp();
    writeFileSync(join(dir, ".env"), "# only a comment\nUNRELATED=1\n");
    const text = loadTestEnv([dir], {}, dir).lines.join("\n");
    expect(text).toMatch(
      /no ZEROBUCKET_\* or AWS_\* settings found \(\d+ bytes read as utf8, 1 KEY=value lines\)/,
    );
  });

  it("loads ZEROBUCKET_* and AWS_* keys only", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://u:p@h/db\nAWS_ACCESS_KEY_ID=k\nPATH=/evil\nNODE_OPTIONS=--inspect\n",
    );
    const { env } = loadTestEnv([dir], {}, dir);
    expect(env).toEqual({
      ZEROBUCKET_TEST_DATABASE_URL: "postgresql://u:p@h/db",
      AWS_ACCESS_KEY_ID: "k",
    });
  });

  it("applies the priority order: later directories and .env.local win", () => {
    const root = tmp();
    const pkg = join(root, "pkg");
    mkdirSync(pkg);
    writeFileSync(
      join(root, ".env"),
      "ZEROBUCKET_A=root\nZEROBUCKET_B=root\nZEROBUCKET_C=root\nZEROBUCKET_D=root\n",
    );
    writeFileSync(
      join(root, ".env.local"),
      "ZEROBUCKET_B=root-local\nZEROBUCKET_C=root-local\nZEROBUCKET_D=root-local\n",
    );
    writeFileSync(join(pkg, ".env"), "ZEROBUCKET_C=pkg\nZEROBUCKET_D=pkg\n");
    writeFileSync(join(pkg, ".env.local"), "ZEROBUCKET_D=pkg-local\n");
    expect(loadTestEnv([root, pkg], {}, root).env).toEqual({
      ZEROBUCKET_A: "root",
      ZEROBUCKET_B: "root-local",
      ZEROBUCKET_C: "pkg",
      ZEROBUCKET_D: "pkg-local",
    });
  });

  it("lets the file win over a stale shell value, and says so without printing the password", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:NEWsecret@localhost/db\n",
    );
    const out = loadTestEnv(
      [dir],
      {
        ZEROBUCKET_TEST_DATABASE_URL:
          "postgresql://postgres:OLDsecret@localhost/db",
      },
      dir,
    );
    expect(out.env.ZEROBUCKET_TEST_DATABASE_URL).toBe(
      "postgresql://postgres:NEWsecret@localhost/db",
    );
    const text = out.lines.join("\n");
    expect(text).toContain("differs only in the secret part");
    expect(text).not.toMatch(/NEWsecret|OLDsecret/);
  });

  it("never prints a credential-style key, in the listing or as a replaced shell value", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "AWS_SECRET_ACCESS_KEY=newSECRETvalue\nZEROBUCKET_API_TOKEN=tok123\n",
    );
    const text = loadTestEnv(
      [dir],
      { AWS_SECRET_ACCESS_KEY: "oldSECRETvalue" },
      dir,
    ).lines.join("\n");
    expect(text).toContain("AWS_SECRET_ACCESS_KEY=***");
    expect(text).toContain("ZEROBUCKET_API_TOKEN=***");
    expect(text).not.toMatch(/SECRETvalue|tok123/);
  });

  it("names a differently-shaped shell value, still masked", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:a@localhost/db\n",
    );
    const text = loadTestEnv(
      [dir],
      {
        ZEROBUCKET_TEST_DATABASE_URL: "postgresql://other:b@elsewhere/db",
      },
      dir,
    ).lines.join("\n");
    expect(text).toContain("postgresql://other:***@elsewhere/db");
  });

  it("does not mention the shell when the values already agree", () => {
    const dir = tmp();
    writeFileSync(join(dir, ".env"), "ZEROBUCKET_X=same\n");
    expect(
      loadTestEnv([dir], { ZEROBUCKET_X: "same" }, dir).lines.join("\n"),
    ).not.toMatch(/shell/);
  });

  it("warns when a value is still the .env.example placeholder", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "ZEROBUCKET_TEST_DATABASE_URL=postgresql://postgres:YOUR_REAL_PASSWORD@localhost/db\nZEROBUCKET_TEST_MYSQL_URL=mysql://u:your_password@h/db\nZEROBUCKET_FINE=postgresql://postgres:hunter2@localhost/db\n",
    );
    const lines = loadTestEnv([dir], {}, dir).lines;
    const warned = lines.filter((l) => l.includes("warning:")).join("\n");
    expect(warned).toContain("ZEROBUCKET_TEST_DATABASE_URL");
    expect(warned).toContain("ZEROBUCKET_TEST_MYSQL_URL");
    expect(warned).not.toContain("ZEROBUCKET_FINE");
  });

  it("warns about a URL whose password was not URL-encoded, without printing the password", () => {
    const dir = tmp();
    writeFileSync(
      join(dir, ".env"),
      "ZEROBUCKET_A=postgresql://postgres:p@ss@localhost:5432/db\nZEROBUCKET_B=postgresql://postgres:p@ss:w/rd@localhost:5432/db\nZEROBUCKET_OK=postgresql://postgres:p%40ss@localhost:5432/db\n",
    );
    const lines = loadTestEnv([dir], {}, dir).lines;
    const warned = lines.filter((l) => l.includes("warning:")).join("\n");
    expect(warned).toMatch(
      /ZEROBUCKET_A is not a valid URL: a special character/,
    );
    expect(warned).toMatch(/ZEROBUCKET_B is not a valid URL/);
    expect(warned).not.toContain("ZEROBUCKET_OK");
    expect(lines.join("\n")).not.toMatch(/p@ss|ss:w/);
  });

  it("does nothing when ZEROBUCKET_IGNORE_DOTENV is set, so the shell can rule for one run", () => {
    const dir = tmp();
    writeFileSync(join(dir, ".env"), "ZEROBUCKET_X=1\n");
    const out = loadTestEnv([dir], { ZEROBUCKET_IGNORE_DOTENV: "1" }, dir);
    expect(out.env).toEqual({});
    expect(out.lines.join("\n")).toContain("ZEROBUCKET_IGNORE_DOTENV");
  });
});
