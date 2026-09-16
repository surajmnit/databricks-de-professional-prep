# Day 11 — Quiz: Delta Optimization — Deletion Vectors, Liquid Clustering, Data Skipping

**Objective coverage:** Section 6 (Cost & Performance Optimization, 13%), Section 10 (Data Modeling, 6%).

---

## Question 1
**Objective:** Deletion vectors — copy-on-write vs. merge-on-read.

What does enabling deletion vectors on a Delta table change about `DELETE`, `UPDATE`, and `MERGE` operations?

A. They now require a full table rewrite to commit
B. They mark affected rows in a soft-delete side-file rather than rewriting the entire Parquet data file
C. They automatically trigger `VACUUM` after each operation
D. They disable the transaction log entirely

---

## Question 2
**Objective:** Deletion vectors — protocol upgrade and compatibility.

A workspace with DBR 12.1 LTS readers (no deletion-vector support) needs to read a table that was just enabled with deletion vectors. What happens?

A. The readers can still read the table, but only as read-only
B. The readers can still read the table; deletion vectors are a write-only optimization
C. The readers will fail — enabling deletion vectors upgrades the table protocol and blocks older clients
D. Databricks automatically downgrades the table back to the older protocol format

---

## Question 3
**Objective:** Deletion vectors — soft-delete physicalization.

After running a `DELETE` on a table with deletion vectors enabled, what is the correct sequence of operations to fully remove the deleted data from storage?

A. `VACUUM` only — removes soft-deleted rows automatically
B. `REORG TABLE ... APPLY (PURGE)` only — physically rewrites files
C. `DELETE` (already done), then `REORG TABLE ... APPLY (PURGE)`, then `VACUUM`
D. `DELETE`, then `OPTIMIZE`, then `VACUUM`

---

## Question 4
**Objective:** Liquid Clustering — what it replaces.

A table currently uses Hive-style partitioning on a `region` column. A query that filters on `region` has good performance, but a new workload filters primarily on a high-cardinality `customer_id` column with no obvious partition candidate. Which migration approach is correct?

A. Keep partitioning on `region` and add Z-Order on `customer_id` within each partition
B. Migrate to Liquid Clustering using `customer_id` as a clustering key (replacing the partition)
C. Add a second partition column on `customer_id`
D. Switch from Delta to a Hive-managed table

---

## Question 5
**Objective:** Liquid Clustering — mutual exclusivity.

An analyst has a table that currently uses `ZORDER BY (order_id)`. They want to try Liquid Clustering. What operational behavior will they encounter?

A. Both approaches can coexist; Spark will automatically use whichever benefits the current query
B. You can enable both simultaneously to get the benefits of each
C. Liquid Clustering is mutually exclusive with Z-Order on the same table — you must choose one or the other
D. Liquid Clustering automatically converts the existing Z-Order layout without any action needed

---

## Question 6
**Objective:** Liquid Clustering — maintenance requirement.

Which statement about Liquid Clustering's ongoing maintenance is correct?

A. Liquid Clustering automatically reorganizes data on every write — no manual action ever needed
B. `OPTIMIZE` is still required to reorganize new/changed data; the maintenance is incremental (only touches what's changed), not that it happens automatically
C. Only `VACUUM` is needed; `OPTIMIZE` is not relevant for liquid-clustered tables
D. Liquid Clustering writes are always immediately optimal; the clustering keys are advisory only

---

## Question 7
**Objective:** Liquid Clustering — redefining clustering keys.

The business has changed access patterns: a table was originally clustered on `customer_id, region` but now queries filter most frequently on `order_date`. What does Databricks allow that partitioning does not?

A. Redefine clustering keys without rewriting historical data already on disk (new OPTIMIZE runs adapt incrementally)
B. Add a third clustering key without any command
C. Partition on `order_date` while keeping the existing clustering keys
D. Automatically recompute statistics without running OPTIMIZE

---

## Question 8
**Objective:** Data skipping — two distinct layers.

A query filters on `region = 'EMEA'`. The table is partitioned on `region`. How many separate "skipping" mechanisms are actively at play?

A. One — partition pruning handles it completely
B. Two — partition pruning skips the directory first, then file pruning (data skipping) within the matching directory
C. Three — partition pruning, file pruning, and a third mechanism called column pruning
D. Zero — no skipping occurs without Liquid Clustering enabled

---

## Question 9
**Objective:** Data skipping — column statistics scope.

A table has 50 columns. The 40th column (`last_event_ts`) is frequently filtered on, but file pruning on this column has unexpectedly poor performance. What is the most likely cause, assuming default configuration?

A. `last_event_ts` is a timestamp column, which doesn't support data skipping
B. The first 32 columns get statistics by default; `last_event_ts` is outside that range unless you explicitly configure `delta.dataSkippingStatsColumns`
C. Data skipping is disabled on all timestamp columns by default
D. The query is using a range predicate rather than an equality predicate, which disables skipping

---

## Question 10
**Objective:** Partitioning vs. Liquid Clustering — high-cardinality column.

A data engineer needs to organize a table where the most useful query filter is a `user_id` column with 50,000 distinct values. Which layout approach is most appropriate, and why?

A. Partition on `user_id` — directories per user will be manageable at 50K partitions
B. Z-Order on `user_id` — space-filling curve will co-locate each user's rows
C. Liquid Clustering — designed for high-cardinality columns without the small-file risk partitioning creates
D. Do not optimize the layout — Spark will automatically skip irrelevant files without any specific strategy

---

## Question 11
**Objective:** Liquid Clustering — recommended default.

A data engineer is designing a new Delta table in a modern DBR 15.4+ workspace with Unity Catalog enabled. There are no unusual constraints. What is Databricks' current explicit recommendation for this table's data layout?

A. Hive-style partitioning on a low-cardinality column
B. Z-Order on the most frequently filtered column
C. Liquid Clustering (either explicit CLUSTER BY or CLUSTER BY AUTO)
D. No optimization — just use the default ordering

---

## Question 12
**Objective:** Deletion vectors — row-level concurrency (DBR 14.2+).

What operational improvement does deletion vectors enable for concurrent writes to the same table?

A. Writers can now acquire table-level locks to prevent conflicts
B. Two writers touching different rows in the same Parquet file can both succeed without conflicting, since no full-file rewrite is needed
C. Writers automatically deduplicate themselves
D. Deletion vectors eliminate the need for checkpointing in streaming workloads

---

## Question 13
**Objective:** Data skipping — `ANALYZE TABLE` vs. transaction log stats.

A query plan shows a poor join strategy selection (broadcast join chosen when the large table is not actually small). Which command would most directly help the query optimizer make a better cardinality estimate?

A. `VACUUM` — removes stale statistics
B. `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` — refreshes optimizer statistics
C. `REORG TABLE t APPLY (PURGE)` — cleans up soft-deleted data
D. `OPTIMIZE t` — reorganizes the data

---

## Question 14
**Objective:** Liquid Clustering — clustering key data type restrictions.

An engineer attempts to cluster a table on a `BINARY` column containing hashed IDs. Does this work?

A. Yes, all Spark data types are supported as clustering keys
B. No — BINARY is not in the supported list (supported: Date, Timestamp, TimestampNTZ, String, integer types, floating/decimal types)
C. Yes, but only if the BINARY column is fewer than 16 bytes
D. Yes, but only in DBR 16.0 and above

---

## Question 15
**Objective:** Deletion vectors — GDPR purge purpose.

A compliance team asks why the purge workflow requires three distinct commands. What is the most precise explanation?

A. `DELETE` removes the row from query results; `REORG TABLE ... APPLY (PURGE)` removes it from the transaction log; `VACUUM` cleans up storage
B. `DELETE` marks the row in the deletion vector; `REORG TABLE ... APPLY (PURGE)` physically rewrites the data file excluding the soft-deleted row; `VACUUM` removes unreferenced old file versions from storage
C. `DELETE` and `VACUUM` together are sufficient; `REORG TABLE` is optional
D. All three are required for GDPR compliance only if the table uses Liquid Clustering

---

## Answer Key

### Q1: B
Deletion vectors replace copy-on-write (full file rewrite) with merge-on-read: affected rows are marked in a small side-file, and reads transparently apply the vector to resolve current state. No full file rewrite, no VACUUM trigger.

### Q2: C
Enabling deletion vectors upgrades the table's protocol version. Older clients/readers that don't support deletion vectors **cannot read the table at all** — this is a real compatibility break, not a warning.

### Q3: C
The complete GDPR purge lifecycle: `DELETE` (logical soft-delete via deletion vector) → `REORG TABLE ... APPLY (PURGE)` (physically rewrites files excluding the marked rows) → `VACUUM` (removes unreferenced old file versions from storage). Any step alone is incomplete.

### Q4: B
Since Liquid Clustering is mutually exclusive with partitioning and Z-Order on the same table, and partitioning on a 50K+ cardinality column would cause small-file explosion, the correct approach is to replace the partition with Liquid Clustering using `customer_id` as the clustering key. Historical partition data migrates incrementally with `OPTIMIZE`.

### Q5: C
Liquid Clustering is mutually exclusive with Z-Order on the same table — you must choose one approach. You cannot have both active simultaneously.

### Q6: B
Liquid Clustering does NOT automatically reorganize data on write. `OPTIMIZE` is still required to actually reorganize new/changed data according to the clustering keys. The advantage is that `OPTIMIZE` under Liquid Clustering is **incremental** (only touches changed data) rather than a full batch re-clustering that Z-Order requires.

### Q7: A
This is the headline flexibility advantage of Liquid Clustering over partitioning: you can change your clustering column strategy with `ALTER TABLE ... CLUSTER BY` + `OPTIMIZE` without rewriting historical data that hasn't been reorganized yet. Partitioning would require a full table rewrite to change the partition scheme.

### Q8: A
In this specific scenario (filter on partition column), only partition pruning is actively at play — the query touches one directory and never even lists others. File pruning/data skipping (via transaction log min/max stats) is a separate mechanism that runs within the directories that were NOT pruned.

### Q9: B
By default, only the first 32 columns get statistics collected (`delta.dataSkippingNumIndexedCols = 32`). If your frequently-filtered column is column 40 and you haven't explicitly set `delta.dataSkippingStatsColumns` to include it, it has no statistics — and file pruning on it silently doesn't happen.

### Q10: C
Liquid Clustering is explicitly designed for high-cardinality columns like `user_id`. Partitioning on 50K values creates 50K directories (small-file explosion). Z-Order within existing files doesn't solve the fundamental cardinality problem as cleanly as Liquid Clustering does.

### Q11: C
Databricks explicitly recommends Liquid Clustering for all new tables (including streaming tables and materialized views) — either via explicit `CLUSTER BY (keys)` or `CLUSTER BY AUTO` under Predictive Optimization. A scenario describing "designing a new table with no unusual constraints" increasingly points to Liquid Clustering as the default-right-answer.

### Q12: B
Before deletion vectors, a `DELETE`/`UPDATE` required rewriting the entire file to remove one row — so two writers touching the same file would conflict. With deletion vectors, writers only update their respective side-file markers, allowing row-level concurrency in DBR 14.2+.

### Q13: B
`ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL COLUMNS` refreshes the optimizer's per-column cardinality statistics (used for join strategy selection, broadcast threshold decisions, etc.). This is distinct from the per-file min/max stats automatically stored in the transaction log for data skipping.

### Q14: B
Supported clustering key data types are: Date, Timestamp, TimestampNTZ (DBR 14.3+), String, integer types (Integer/Long/Short/Byte), and floating/decimal types (Float/Double/Decimal). BINARY is not supported — the engineer would need to cast/conver the column to a supported type (e.g., hex string) to use it as a clustering key.

### Q15: B
Deletion vectors make `DELETE` a logical-only operation (soft-delete). `REORG TABLE ... APPLY (PURGE)` is required to physically rewrite the files, excluding the soft-deleted rows. `VACUUM` then removes the old, now-unreferenced file versions from storage. This three-step sequence is precisely why deletion vectors are the mechanism that makes a plain `DELETE` insufficient for true physical erasure.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Deletion vectors — merge-on-read concept |
| 2 | Medium | DV protocol upgrade — compatibility break |
| 3 | Medium | GDPR purge three-step lifecycle |
| 4 | Medium | Liquid Clustering migration from partitioning |
| 5 | Easy | Liquid Clustering mutual exclusivity with Z-Order |
| 6 | Medium | Liquid Clustering maintenance — OPTIMIZE still required |
| 7 | Medium | Liquid Clustering redefine keys advantage |
| 8 | Easy | Two skipping layers — partition vs. file pruning |
| 9 | Hard | Default 32-column stats scope — data skipping failure |
| 10 | Medium | High-cardinality column — Liquid Clustering is right fit |
| 11 | Easy | Databricks recommendation for new tables |
| 12 | Medium | DV row-level concurrency improvement |
| 13 | Medium | ANALYZE TABLE vs. transaction log stats distinction |
| 14 | Medium | Clustering key data type restrictions |
| 15 | Medium | GDPR purge rationale — why three steps |
