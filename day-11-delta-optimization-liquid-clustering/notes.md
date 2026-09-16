# Day 11 — Delta Optimization: Deletion Vectors, Liquid Clustering, Data Skipping & File Pruning

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 6: Cost & Performance Optimization (13%)**:
- "Understand delta optimization techniques, such as deletion vectors and liquid clustering."
- "Understand the optimization techniques used by Databricks to ensure the performance of queries on large datasets (data skipping, file pruning, etc.)."

And **Section 10: Data Modeling (6%)**:
- "Simplify data layout decisions and optimize query performance using Liquid Clustering."
- "Identify the benefits of using liquid Clustering over Partitioning and ZOrder."

*(Day 9 introduced these concepts at a high level and deliberately deferred the mechanics here. This is the deep dive — don't re-read Day 9's intro table as if it were new.)*

---

## Part 1 — Deletion Vectors: Copy-on-Write to Merge-on-Read

### The problem they solve
Without deletion vectors, **any** `DELETE`, `UPDATE`, or `MERGE` that touches even a single row in a Parquet file requires **rewriting the entire file** (Delta's traditional copy-on-write model). For wide tables or large files, this is expensive for what might be a one-row change.

### How they work
With deletion vectors enabled, `DELETE`/`UPDATE`/`MERGE` instead **mark affected rows in a small side-file** (the deletion vector) rather than rewriting the Parquet data file — a **merge-on-read** model. Reads transparently apply the deletion vector to resolve the current, correct state of the table.

### Enabling
```sql
CREATE TABLE t (...) TBLPROPERTIES ('delta.enableDeletionVectors' = true);
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```
- Deletion vectors are **enabled by default** for new tables created via a SQL warehouse or Databricks Runtime 14.1+ (governed by a workspace admin setting that controls auto-enablement).
- They are **not** enabled by default for materialized views or streaming tables stored in the legacy Hive Metastore, and you **cannot** use `ALTER TABLE` to toggle deletion vectors on a materialized view or streaming table directly.

**Exam trap — protocol upgrade:** enabling deletion vectors **upgrades the table's protocol version**. After upgrading, older clients/readers that don't support deletion vectors **cannot read the table at all** — this is a real compatibility break, not a warning you can ignore. (Writing with full optimizations needs DBR 14.3 LTS+; reading needs DBR 12.2 LTS+.)

### When soft-deletes become physical
A deletion vector is a **logical, soft-delete**. The underlying Parquet bytes are only physically rewritten when:
1. An `OPTIMIZE` command runs on the table, or
2. `REORG TABLE ... APPLY (PURGE)` is explicitly run.

```sql
DELETE FROM customer_activity WHERE customer_id = 12345;   -- soft-delete: deletion vector recorded, no file rewrite
REORG TABLE customer_activity APPLY (PURGE);                -- physically rewrites files, removing the marked rows
VACUUM customer_activity;                                    -- removes the now-unreferenced OLD file versions from storage
```
**This is precisely why Day 19's GDPR purge lifecycle needs all three steps** (`DELETE` -> `REORG TABLE ... APPLY (PURGE)` -> `VACUUM`) — deletion vectors are the exact mechanism that makes a plain `DELETE` insufficient for true physical erasure.

**Performance tuning for large purges:** `spark.databricks.delta.reorg.purgeMode` defaults to `all` (scans every file's footer, checking for both soft-deleted rows and dropped-column data). Setting it to `rows` speeds up a purge that's only clearing soft-deleted rows, skipping the dropped-column check.

### Why this matters beyond compliance
- **Faster `DELETE`/`UPDATE`/`MERGE`** — no full-file rewrite for small changes, which is exactly the "MERGE is slow" performance angle from Day 10.
- **Row-level concurrency** (DBR 14.2+) — because writers no longer need to rewrite whole files to remove a row, two writers touching different rows in the *same* file can both succeed without conflicting (an improvement to the optimistic concurrency model from Day 9).
- **Photon predictive I/O** uses deletion vectors to further accelerate `UPDATE` operations specifically.

---

## Part 2 — Liquid Clustering

### What it replaces
Liquid Clustering is Databricks' modern data layout technique that **replaces both Hive-style partitioning and `ZORDER`** — it is **not compatible with partitioning or `ZORDER` on the same table** (you choose one approach, not a combination on the same table, aside from the migration mapping in Part 4).

### Enabling
```sql
-- At table creation
CREATE TABLE sales (order_id BIGINT, customer_id BIGINT, region STRING, order_date DATE)
CLUSTER BY (customer_id, region);

-- On an existing unpartitioned table
ALTER TABLE sales CLUSTER BY (customer_id, region);

-- Automatic (Databricks manages the clustering keys for you) — DBR 15.4 LTS+, UC managed tables
CREATE TABLE sales (...) CLUSTER BY AUTO;
```

**Clustering keys must be columns with statistics collected.** By default, the **first 32 columns** of a Delta table have statistics collected (`delta.dataSkippingNumIndexedCols`) — if your desired clustering column is outside that default set, you need to adjust which columns are indexed.

**Supported clustering key data types:** Date, Timestamp, TimestampNTZ (DBR 14.3+), String, integer types (Integer/Long/Short/Byte), and floating/decimal types (Float/Double/Decimal).

### The critical operational fact: `OPTIMIZE` is still required
Liquid Clustering does **not** cluster data automatically the instant it's written. **You (or Predictive Optimization, if enabled — Day 9) must still run `OPTIMIZE`** to incrementally organize new/changed data according to the clustering keys:
```sql
OPTIMIZE sales;
```
**Exam trap:** this is subtle — Liquid Clustering's big advantage over Z-Ordering is *not* "no maintenance ever," it's that the maintenance (`OPTIMIZE`) is **incremental** (only reorganizes what's needed) and the clustering keys **can be redefined without rewriting historical data** — not that clustering happens with zero operational action at all (unless you're specifically using `CLUSTER BY AUTO` under Predictive Optimization).

### Redefining clustering keys — the headline flexibility feature
```sql
-- Change clustering keys on an existing liquid-clustered (or even previously partitioned) table
ALTER TABLE sales CLUSTER BY (order_date, customer_id);
OPTIMIZE sales;   -- required to actually apply the new clustering to data
```
Unlike partitioning (which requires a full rewrite to change the partition scheme), Liquid Clustering lets you **change your mind about clustering columns as query patterns evolve**, without touching historical data that hasn't been reorganized yet — new `OPTIMIZE` runs incrementally adapt the layout.

### Migrating from partitioning or Z-Order
| Current technique | Recommended clustering keys |
|---|---|
| Hive-style partitioning | Use the existing partition column(s) as clustering keys |
| Z-Order indexing | Use the existing `ZORDER BY` column(s) as clustering keys |
| Both partitioning and Z-Order | Use both sets of columns together as clustering keys |

```sql
ALTER TABLE t1 REPLACE PARTITIONED BY WITH CLUSTER BY (day, id);
OPTIMIZE t1;   -- required to actually benefit from the new clustering
```

### When to use it (straight from Databricks' guidance — good scenario-matching material)
- Queries that filter on **high-cardinality columns**
- Tables with **heavy data skew**
- **Fast-growing tables** requiring ongoing maintenance/tuning
- Tables with **concurrent write requirements**
- Tables with **varied or changing access patterns**
- Tables where a "natural" partition key would leave **too many or too few** partitions

**Exam trap:** Databricks now **recommends Liquid Clustering for all new tables** (including streaming tables and materialized views) — a scenario simply describing "designing a new table" with no unusual constraints increasingly points to Liquid Clustering as the default-right-answer, not partitioning.

---

## Part 3 — The Comparison the Exam Explicitly Names: Liquid Clustering vs. Partitioning vs. Z-Order

| Aspect | Partitioning | Z-Ordering | Liquid Clustering |
|---|---|---|---|
| Physical layout | Separate **directory per value** | Sorts rows **within existing files** along a space-filling curve | Organizes files around clustering keys **without fixed directories** |
| High-cardinality columns | **Bad** — small-file explosion (Day 9) | Workable, but files can still be numerous | **Designed for this** — a named use case |
| Changing the layout later | Requires a **full table rewrite** | Re-run `OPTIMIZE ... ZORDER BY` (rewrites relevant files) | `ALTER TABLE ... CLUSTER BY` + `OPTIMIZE` — **incremental**, doesn't rewrite untouched historical data |
| Maintenance model | Static, set at creation | **Manual/scheduled** batch job you must remember to re-run | Still needs `OPTIMIZE`, but **incremental**; can be automated via `CLUSTER BY AUTO`/Predictive Optimization |
| Data skew handling | Poor (skewed partitions) | Improves co-location, doesn't fix skew directly | **Explicitly designed** to handle skew well |
| Combinable with partitioning? | N/A | Yes, historically (Z-Order within partitions) | **No — mutually exclusive** with partitioning and Z-Order on the same table |
| Databricks' current recommendation | Legacy default | Legacy, still supported but no longer the recommended default | **Recommended for all new tables** |

**Exam framing — "identify the benefits":** if asked directly why Liquid Clustering is preferred over Partitioning/Z-Order, the three defensible, documented reasons are: **(1)** it removes the small-file risk of high-cardinality partitioning, **(2)** it lets you **redefine clustering keys without a full rewrite** as query patterns evolve (partitioning cannot do this at all; Z-Order requires re-running against the whole affected dataset), and **(3)** its maintenance (`OPTIMIZE`) is **incremental** rather than a full manual re-clustering pass.

---

## Part 4 — Data Skipping and File Pruning (the mechanism underneath everything above)

Recall from Day 9: every `add` action in the transaction log stores **column-level min/max statistics** for the file it describes. This is what makes data skipping possible.

### Two distinct layers of "skipping"
| Layer | What it skips | Mechanism |
|---|---|---|
| **Partition pruning** | Entire directories | Query filter matches the partition column -> directories that can't match are never even listed |
| **File pruning (data skipping)** | Individual files within a directory/table | Query filter is compared against each file's min/max stats **from the transaction log** — no partitioning required at all, works with Liquid Clustering too |

### Controlling which columns get statistics
```sql
-- Default: first 32 columns get stats collected
SET spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols = 32;

-- Explicitly name which columns to collect stats for (overrides the "first N columns" default)
ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'col_a,col_b,col_c');
```
**Exam trap:** if a frequently-filtered column happens to be the **33rd+ column** in table order and you haven't customized `dataSkippingStatsColumns`, that column has **no statistics collected**, and file pruning on it silently doesn't happen — this is a real, subtle cause of "why is my query scanning far more data than it should" (the exact "bad data skipping" signal from Day 8's Query Profile material).

### Refreshing stats for query planning
```sql
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;
```
Improves the query optimizer's cardinality estimates (join strategy selection, etc.) — a distinct concept from the automatic per-file min/max stats in the transaction log, but works toward the same goal of the optimizer making better decisions.

### Tying it back to Day 8
The "bad data skipping" bottleneck category from Query Profile (Day 8) is the **symptom**; this day is the **cause and fix**: poor clustering (no Liquid Clustering/Z-Order/partitioning aligned with query filters), or missing statistics on the filtered column, are exactly what produces a high scanned-vs-pruned ratio on the Scan operator.

---

## Part 5 — Exam Traps Recap

1. Deletion vectors shift `DELETE`/`UPDATE`/`MERGE` from copy-on-write (full file rewrite) to **merge-on-read** (soft-delete side-file) — faster small changes, at the cost of needing an explicit `OPTIMIZE`/`REORG TABLE ... APPLY (PURGE)` to physically materialize the change.
2. Enabling deletion vectors **upgrades the table protocol** — older clients can lose read access entirely.
3. Deletion vectors are **why** the Day 19 purge lifecycle needs `REORG TABLE ... APPLY (PURGE)` — a plain `DELETE` is only a logical marker.
4. Liquid Clustering **still requires `OPTIMIZE`** to actually reorganize data — it is incremental and flexible, not maintenance-free by default (`CLUSTER BY AUTO` is the maintenance-free variant).
5. Liquid Clustering is **mutually exclusive** with partitioning and Z-Order on the same table.
6. The headline Liquid Clustering advantage the exam wants: **redefine clustering keys without a full rewrite**, unlike partitioning.
7. Clustering keys need collected statistics — by default only the **first 32 columns** are indexed; adjust `dataSkippingStatsColumns` if your key falls outside that.
8. Two separate skipping layers: **partition pruning** (skip directories) vs. **file pruning/data skipping** (skip individual files via transaction-log min/max stats) — Liquid Clustering only benefits from the latter, since it has no directory structure.

---

## Cross-References
- Day 8: Query Profile's "bad data skipping" signal — this day supplies the root cause and the fix.
- Day 9: Transaction log `add` action statistics — the foundation data skipping is built on; managed vs. external table and Predictive Optimization scope.
- Day 10: `MERGE` performance and skewed merge keys — deletion vectors and Liquid Clustering are the two concrete fixes referenced there.
- Day 19: The physical purge lifecycle (`DELETE` -> `REORG TABLE ... APPLY (PURGE)` -> `VACUUM`) — now fully explained by deletion-vector mechanics.
- Day 26: Dimensional modeling — Liquid Clustering vs. partitioning trade-offs applied to fact/dimension table design.
