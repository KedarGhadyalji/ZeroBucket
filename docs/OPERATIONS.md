# Operations Guide: Backups & Space

These aren't code -- they're the operational surprises that catch people
off guard once the ZeroBucket tables have real data in them, documented
up front rather than discovered the hard way. They apply to both the
Python and the npm package, which share the same tables.

- [PostgreSQL](#postgresql): backups, autovacuum
- [MySQL / MariaDB](#mysql--mariadb): backups, `max_allowed_packet`, reclaiming space
- [SQLite](#sqlite): backing up a WAL database, reclaiming space
- [What ZeroBucket does NOT do here](#what-zerobucket-does-not-do-here)

**Which tables?** Classic mode (the default) stores everything in
`zerobucket_images`. Dedup mode (`dedup: true` / `dedup=True`) uses
`zerobucket_blobs` (the image bytes, one row per distinct image) and
`zerobucket_image_refs` (one small row per id). **`zerobucket_blobs` is the big
table; `zerobucket_image_refs` has a foreign key to it, so back the two up and
restore them together** (blobs first, if you restore separately). In every
command below, substitute the dedup tables if that is the mode you use.

# PostgreSQL

## Backups: your nightly `pg_dump` will get slower, possibly a lot slower

`zerobucket_images` stores full image bytes in a `BYTEA` column, in the
same database as the rest of your application data. A standard
`pg_dump` of your whole database dumps this table right along with
everything else -- meaning your routine app backup now includes every
image byte you've ever stored, every single night.

**If this table is large, consider dumping it separately from the rest
of your schema:**

```bash
# Exclude the image table from the main app backup (fast, unchanged size)
pg_dump --exclude-table=zerobucket_images -Fc mydb > app_backup.dump

# Dump the image table on its own, separately (can run less often, or
# in parallel, or to different storage/retention policy)
pg_dump -Fc -t zerobucket_images mydb > images_backup.dump
```

Restoring requires both dumps:

```bash
pg_restore -d mydb app_backup.dump
pg_restore -d mydb images_backup.dump
```

**Why this matters in practice:** this is exactly the kind of thing that
shows up as "wait, why did our nightly backup suddenly take 10x longer"
weeks after nobody changed anything about the backup job itself -- the
data just grew. Splitting the dump doesn't reduce total backup size, but
it does mean your fast, frequent app-data backup stays fast, and the
slower image dump can run on its own schedule.

## Autovacuum: TOAST-heavy tables often need different tuning than narrow rows

Postgres automatically moves large column values (like our `BYTEA` image
data) into a separate TOAST table behind the scenes. TOAST tables have
different practical vacuum needs than the narrow, frequently-updated rows
autovacuum's defaults are tuned for.

**If you're seeing autovacuum struggle to keep up on this table**
(visible via `SELECT * FROM pg_stat_user_tables WHERE relname =
'zerobucket_images';` -- watch `n_dead_tup` and `last_autovacuum`),
consider a table-specific tuning override rather than changing global
settings:

```sql
ALTER TABLE zerobucket_images SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- default 0.2; vacuum sooner
    autovacuum_analyze_scale_factor = 0.05
);
```

This isn't a default ZeroBucket applies automatically -- reasonable
tuning depends on your actual delete/update rate on this table, which
varies per application. If you're rarely deleting images, the practical
impact of this table on autovacuum is much smaller than a table with
frequent updates; this guidance matters most once `delete()` (and,
eventually, deduplication cleanup) is a regular part of your traffic
pattern.

# MySQL / MariaDB

Same shape of problem as PostgreSQL: the image bytes live in a `LONGBLOB` column
in your application database, so a routine whole-database dump now includes every
image you've ever stored.

## Backups

Dump the image table separately so the fast, frequent app backup stays fast
(`--single-transaction` gives a consistent snapshot of InnoDB tables without
locking writers):

```bash
# App data, without the image table
mysqldump --single-transaction --ignore-table=mydb.zerobucket_images mydb > app_backup.sql

# The image table on its own, on its own schedule
mysqldump --single-transaction mydb zerobucket_images > images_backup.sql
```

In dedup mode, dump `zerobucket_blobs` and `zerobucket_image_refs` together
(`mysqldump ... mydb zerobucket_blobs zerobucket_image_refs`).

## `max_allowed_packet` caps the size of one image

The server refuses to accept a single value larger than `max_allowed_packet`.
ZeroBucket's own size limit (`maxBytes` / `max_bytes`) defaults to 8 MiB, which
is under both servers' defaults, but **if you raise it, raise `max_allowed_packet`
to match**, or large images will fail to store. The default differs by server: **16 MB on MariaDB
10.11 and 64 MB on MySQL 8.0** (both measured). Check yours with
`SELECT @@max_allowed_packet;` and raise it on the server if you store larger
images. It is a server setting, not something ZeroBucket changes.

## Deleting images does not shrink the table file

InnoDB keeps the space after `DELETE` for reuse. Measured on MySQL 8.0: a table
file that grew to 32 MB stayed at 32 MB after deleting 29 of its 30 rows, and
dropped to 4 MB only after `OPTIMIZE TABLE`.

```sql
OPTIMIZE TABLE zerobucket_images;   -- rebuilds the table and returns the space
```

`OPTIMIZE TABLE` rebuilds the table, which takes time and disk space roughly
proportional to its size, so run it in a quiet period. If you rarely delete
images you may never need it.

# SQLite

## Backups: don't just copy the `.db` file

ZeroBucket opens SQLite in WAL mode. Recently written data can live in the
`-wal` file next to your database and not yet in the main file, so **copying
only the main file while the app is running can give you a backup that is
missing data, or empty**. Measured: after storing 5 images, a copy of just the
main file contained no table at all.

Use SQLite's own backup instead, which is safe while the app is running:

```bash
sqlite3 images.db ".backup 'images_backup.db'"
# or, producing a compacted copy:
sqlite3 images.db "VACUUM INTO 'images_backup.db'"
```

(Both were verified to produce a complete copy under the same conditions.) If you
copy files instead, stop the app first, or copy the `-wal` and `-shm` files
together with the main file.

## Deleting images does not shrink the file

SQLite keeps freed pages for reuse. Measured: a 2 MB database stayed 2 MB after
every row was deleted, and dropped to 8 KB after `VACUUM`.

```sql
VACUUM;   -- rewrites the whole file; needs free disk space about the size of the database
```

SQLite allows one writer at a time, and `VACUUM` is a writer, so run it when
traffic is low.

## What ZeroBucket does NOT do here

None of the above is automated or enforced by the library. This is
operational guidance for adopting ZeroBucket at real scale, not a
built-in feature -- consistent with keeping the core library itself
simple and unopinionated about your specific backup/ops tooling.
