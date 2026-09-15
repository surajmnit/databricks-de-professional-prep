# Day 9 — Cheat Sheet: Delta Lake Fundamentals

## Transaction Log Structure

```
my_table/
├── part-*.snappy.parquet
└── _delta_log/
    ├── 0000...0000.json    <- commit 0
    ├── 0000...0001.json    <- commit 1
    ├── 0000...0010.checkpoint.parquet   <- every 10 commits (default)
    └── _last_checkpoint
```

| Action | Records |
|---|---|
| `add` | New data file + column min/max stats |
| `remove` | Tombstoned file (bytes stay until `VACUUM`) |
| `metaData` | Schema, partition columns, table properties |
| `protocol` | Min reader/writer version required |
| `commitInfo` | Operation type, timestamp, metrics |
| `txn` | Streaming idempotency marker |

**Checkpoint default:** every 10 commits (`delta.checkpointInterval`).
**Reconstruction:** latest checkpoint + replay newer JSON commits = current snapshot.

---

## ACID Mapping

| Guarantee | Mechanism |
|---|---|
| Atomicity | Whole write = one JSON commit, all-or-nothing |
| Consistency | Schema enforcement (default) |
| Isolation | **Optimistic concurrency control** — no locks, conflicting writers retry |
| Durability | Log persisted to cloud object storage |

---

## Managed vs. External Tables

| | Managed | External |
|---|---|---|
| Storage location | UC-controlled | User-specified `LOCATION` |
| `DROP TABLE` | **Deletes data + log** | **Metastore reference only** — files untouched |
| Predictive Optimization | Eligible | Not eligible |

---

## Predictive Optimization (managed tables only)

| Runs | Automatically does | Does NOT do |
|---|---|---|
| `OPTIMIZE` | Compacts small files; incremental Liquid Clustering | **No automatic `ZORDER`** |
| `VACUUM` | Removes tombstoned files past retention | — |
| `ANALYZE` | Collects stats for planning/skipping | — |

Excluded: **external tables**, **Delta Sharing recipient tables**.

---

## Partitioning Rules

- Partition on **low-to-moderate cardinality**, **commonly filtered** columns (e.g., `date`).
- **Never** partition on a near-unique column (`post_id`, `user_id`, raw `timestamp`) -> small-file explosion.
- Heuristic: each partition should hold **>= ~1 GB**.

---

## Z-Ordering (intro -- full comparison Day 11)

```sql
OPTIMIZE table_name ZORDER BY (col);
```
- Sorts/clusters rows **within files** -- no new directories (unlike partitioning).
- **Manual/scheduled only** -- not auto-maintained; Predictive Optimization does NOT run `ZORDER` for you.
- Best for 1-3 high-value filter/join columns.

---

## Time Travel

```sql
SELECT * FROM t VERSION AS OF 12;
SELECT * FROM t TIMESTAMP AS OF '2026-01-01T00:00:00Z';
DESCRIBE HISTORY t;
RESTORE TABLE t TO VERSION AS OF 12;
```
Bounded by `delta.logRetentionDuration` / `delta.deletedFileRetentionDuration`. **`VACUUM` past retention permanently ends time travel** to affected versions.

---

## Schema Enforcement vs. Evolution

| Action | Behavior |
|---|---|
| Default write | **Rejects** mismatched schema |
| `mergeSchema=true` | Allows **additive-only** changes (new nullable columns) |
| `overwriteSchema=true` | Full schema replace -- deliberate, dangerous |
| Lossy type change (e.g. STRING->INT) | **Not** silently allowed by `mergeSchema` |

---

## Exam Trap Shortlist

1. `ALTER TABLE ... RENAME` -> **metastore pointer update only**, no new log entry, no file rewrite (official sample question).
2. Checkpoint every **10 commits** by default.
3. Isolation = **optimistic concurrency**, not locking.
4. `remove` = tombstone; physical delete only after `VACUUM`.
5. **External table `DROP TABLE`** -> files survive; **managed** -> files deleted.
6. **Predictive Optimization** = managed tables only, and never auto-runs `ZORDER`.
7. Partition column: `date` beats `post_id`/`user_id`/`post_time` -- cardinality trap (official sample question).
8. Z-Ordering is **manual**, not self-maintaining (contrast with Liquid Clustering, Day 11).
9. `mergeSchema` != lossy type conversion -- additive only.
10. Time travel dies for a version once `VACUUM` deletes the files it depends on.
