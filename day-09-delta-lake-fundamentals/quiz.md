# Day 9 — Quiz: Delta Lake Fundamentals

**Objective coverage:** Section 10 (6%) and Section 6 (13%) — foundational transaction log, managed/external tables, partitioning.

---

## Question 1
**Objective:** Catalog/metastore operations vs. the transaction log.

A table was created with a typo (`prod.sales_by_stor`) and is renamed to fix it: `ALTER TABLE prod.sales_by_stor RENAME TO prod.sales_by_store`. What happens?

A. All related files and metadata are dropped and recreated in a single ACID transaction
B. The table name change is recorded as a new entry in the Delta transaction log
C. A new `_delta_log` is created for the renamed table
D. The table reference in the metastore is updated

---

## Question 2
**Objective:** Transaction log checkpoints.

By default, how often does Delta Lake write a checkpoint file to `_delta_log`?

A. Every commit
B. Every 10 commits
C. Every 100 commits
D. Only when `VACUUM` runs

---

## Question 3
**Objective:** Optimistic concurrency control.

Two writers attempt to commit to the same Delta table at nearly the same time, modifying different, non-overlapping partitions. What is the most likely outcome?

A. Both fail — Delta only allows one writer at a time
B. Both succeed — Delta uses optimistic concurrency control, and non-conflicting changes can both commit (the second retries at the next version if needed)
C. The second writer's data is silently merged into the first writer's file
D. A distributed lock is acquired, and the second writer waits until the first completes

---

## Question 4
**Objective:** How `remove` actions work.

A `DELETE` is run against a Delta table. Is the deleted row's data immediately and permanently removed from cloud storage?

A. Yes, immediately
B. No — the file is marked as removed (tombstoned) in the transaction log; the physical bytes remain until `VACUUM` clears them past the retention window
C. No, and there is no way to ever remove it
D. Yes, but only after 24 hours automatically

---

## Question 5
**Objective:** Managed vs. external table `DROP TABLE` behavior.

A team drops an **external** Delta table by mistake. What happens to the underlying data files?

A. They are deleted along with the table
B. They remain untouched at their original location — only the metastore reference is removed
C. They are moved to a trash/recycle location for 30 days
D. External tables cannot be dropped

---

## Question 6
**Objective:** Predictive Optimization scope.

A team notices that `OPTIMIZE`, `VACUUM`, and `ANALYZE` are running automatically on their Unity Catalog managed tables but never on an external table pointing at the same kind of data. Why?

A. This is a bug — Predictive Optimization should apply to all tables equally
B. Predictive Optimization is scoped specifically to Unity Catalog managed tables; external tables are excluded
C. External tables require a separate paid add-on to enable Predictive Optimization
D. Predictive Optimization only applies to tables under 1 GB

---

## Question 7
**Objective:** Predictive Optimization and Z-Ordering.

Does Predictive Optimization's automatic `OPTIMIZE` runs include `ZORDER BY` clustering?

A. Yes, always, using the most recently used filter columns
B. No — automatic `OPTIMIZE` under Predictive Optimization does not include `ZORDER`; Z-Ordering must be run manually, or Liquid Clustering used instead for automatic maintenance
C. Yes, but only on external tables
D. `ZORDER` was deprecated and no longer exists in any form

---

## Question 8
**Objective:** Choosing a partition column.

Given a table with columns `user_id BIGINT`, `post_id STRING`, `post_time TIMESTAMP`, and `date DATE`, which column should be used for partitioning?

A. `post_id`
B. `post_time`
C. `date`
D. `user_id`

---

## Question 9
**Objective:** Small-file problem from over-partitioning.

A table is partitioned by a column with extremely high cardinality (nearly unique per row). What is the most likely operational consequence?

A. Query performance improves because every query can prune to a single partition
B. A large number of very small partitions/files are created, hurting both write throughput and read performance
C. Delta automatically merges high-cardinality partitions to prevent this
D. No practical consequence — partition cardinality doesn't affect performance

---

## Question 10
**Objective:** Schema enforcement vs. evolution.

A write includes a new column not present in the target table's current schema, and `mergeSchema` is not set. What happens?

A. The write succeeds, and the new column is silently added
B. The write fails — schema enforcement rejects the mismatch by default
C. The write succeeds, but the new column's values are discarded
D. The table is automatically dropped and recreated with the new schema

---

## Question 11
**Objective:** `mergeSchema` limitations.

A team sets `mergeSchema=true` and writes data where an existing column's type has changed from `STRING` to `INT` (a lossy, incompatible change). What happens?

A. `mergeSchema` silently converts the column and may lose data
B. `mergeSchema` only allows safe, additive changes (like new columns) — an incompatible type change like this is not silently permitted
C. The write always succeeds regardless of type compatibility when `mergeSchema` is set
D. `mergeSchema` has no effect on column type changes at all, ever, under any circumstance

---

## Question 12
**Objective:** Time travel and retention.

After `VACUUM` removes data files past the retention window, can you still time-travel to a version of the table that depended on those files?

A. Yes, always — time travel is independent of `VACUUM`
B. No — once the underlying files are physically deleted, versions depending on them can no longer be reconstructed
C. Yes, but only via `DESCRIBE HISTORY`
D. Time travel and `VACUUM` are unrelated features that never interact

---

## Question 13
**Objective:** Z-Ordering mechanics.

How does Z-Ordering differ from partitioning in how it organizes data?

A. Z-Ordering creates separate directories per value, just like partitioning
B. Z-Ordering sorts/clusters rows within existing files along the specified column(s) rather than creating separate directories
C. Z-Ordering and partitioning are two names for the same mechanism
D. Z-Ordering only works on partitioned tables

---

## Question 14
**Objective:** Z-Ordering maintenance.

Does Z-Ordering automatically stay effective as new data is continuously appended to a table?

A. Yes, it re-clusters automatically on every write
B. No — `OPTIMIZE ... ZORDER BY` is a manual/scheduled operation; it must be re-run periodically to keep clustering effective as data grows
C. Yes, but only if Predictive Optimization is enabled
D. No — Z-Ordering can only be applied once, at table creation

---

## Question 15
**Objective:** Transaction log actions.

Which transaction log action records the minimum reader/writer protocol version required to safely access a Delta table?

A. `metaData`
B. `commitInfo`
C. `protocol`
D. `txn`

---

## Answer Key

### Q1: D
Renaming a table is a metastore/catalog operation — it updates the name-to-location pointer. It does not rewrite files, does not create a new transaction log, and is not itself recorded as a transaction log entry on the table's data.
**Wrong options:** A and C invent file/log recreation that doesn't happen. B misattributes a metastore-level change to the transaction log.

### Q2: B
Delta writes a checkpoint every 10 commits by default (`delta.checkpointInterval`), consolidating the JSON history into a single Parquet snapshot for faster state reconstruction.

### Q3: B
Delta's isolation model is optimistic concurrency control, not locking — writers to genuinely non-conflicting data (e.g., different partitions/files) can both succeed, with the second retrying at the next version number if a conflict check is needed.
**Wrong options:** A and D describe a locking model Delta doesn't use. C invents a merge behavior that isn't how Delta commits work.

### Q4: B
`DELETE` (and `UPDATE`/`MERGE`) mark old files as removed (tombstoned) in the log; the physical bytes remain until `VACUUM` clears them past the retention window — this is the same mechanism Day 19's purge lifecycle depends on.

### Q5: B
External tables only have their metastore reference removed on `DROP TABLE` — the data files at the specified `LOCATION` are left untouched, since other systems/teams may depend on them directly.

### Q6: B
Predictive Optimization is explicitly scoped to Unity Catalog **managed** tables — external tables (and Delta Sharing recipient tables) are excluded from this automation.

### Q7: B
Automatic `OPTIMIZE` runs under Predictive Optimization compact small files but do not perform `ZORDER` clustering — that remains a manual operation, or you adopt Liquid Clustering (Day 11) for automatic, ongoing clustering.

### Q8: C
`date` is the correct partition column — coarse-grained, low-to-moderate cardinality, and commonly filtered on. `post_id` and `user_id` are high-cardinality identifiers; `post_time` is a full timestamp, effectively unique per row — both would cause a small-file explosion.

### Q9: B
High-cardinality partition columns create a directory (and often very small files) per distinct value, hurting both write throughput and read performance despite the theoretical pruning benefit.
**Wrong options:** A ignores the small-file cost that outweighs pruning benefits at this cardinality. C invents an automatic-merge behavior Delta doesn't perform for partitions. D denies a well-documented performance concern.

### Q10: B
Schema enforcement is Delta's default behavior — a write with a mismatched schema is rejected unless schema evolution is explicitly requested.

### Q11: B
`mergeSchema` is designed for safe, additive schema changes only (e.g., new nullable columns); it does not silently perform lossy or incompatible type conversions — those require an explicit, deliberate schema change.

### Q12: B
Once `VACUUM` physically deletes files past the retention window, any table version whose reconstruction depended on those files can no longer be read via time travel, even if the JSON commit history describing that version still technically exists.

### Q13: B
Z-Ordering sorts/co-locates related rows within the existing file layout using a space-filling curve — it does not create separate directories the way partitioning does.

### Q14: B
Z-Ordering via `OPTIMIZE ... ZORDER BY` is a manual or scheduled batch operation — it is not continuously/automatically re-applied as new data lands, unlike Liquid Clustering's incremental maintenance (Day 11).

### Q15: C
The `protocol` action records the minimum reader/writer version required to safely access the table, preventing incompatible older clients from corrupting a table that uses newer features.
**Wrong options:** `metaData` carries schema/partition/table-property info, `commitInfo` carries operation metadata, and `txn` is the streaming idempotency marker — none of these carry protocol version requirements.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Rename = metastore operation, not a log transaction (official sample pattern) |
| 2 | Easy | Default checkpoint interval |
| 3 | Medium | Optimistic concurrency control behavior |
| 4 | Easy | `remove` = tombstone, not physical delete |
| 5 | Easy | External table `DROP TABLE` behavior |
| 6 | Medium | Predictive Optimization scope (managed only) |
| 7 | Medium | Predictive Optimization excludes automatic `ZORDER` |
| 8 | Medium | Partition column choice (official sample pattern) |
| 9 | Medium | Small-file problem from high-cardinality partitioning |
| 10 | Easy | Schema enforcement is the default |
| 11 | Medium | `mergeSchema` additive-only limitation |
| 12 | Medium | Time travel vs. `VACUUM` retention interaction |
| 13 | Medium | Z-Ordering vs. partitioning mechanism |
| 14 | Medium | Z-Ordering requires manual/scheduled re-runs |
| 15 | Easy | `protocol` action purpose |
