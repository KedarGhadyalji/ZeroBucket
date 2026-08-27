# Operations Guide: Backups & Autovacuum

These aren't code -- they're the two operational surprises that catch
people off guard once a `zerobucket_images` table has real data in it,
documented up front rather than discovered the hard way.

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

## What ZeroBucket does NOT do here

Neither of the above is automated or enforced by the library. This is
operational guidance for adopting ZeroBucket at real scale, not a
built-in feature -- consistent with keeping the core library itself
simple and unopinionated about your specific backup/ops tooling.
