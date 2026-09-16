# Day 11 — Cheat Sheet: Delta Optimization — Deletion Vectors, Liquid Clustering, Data Skipping

## Deletion Vectors

**Enables:** Copy-on-write → merge-on-read (no full file rewrite on DELETE/UPDATE/MERGE)

```sql
ALTER TABLE t SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```
- Enabled by default: new tables via SQL warehouse or DBR 14.1+ (workspace admin governs auto-enable)
- **NOT** enabled by default for materialized views / streaming tables (cannot toggle with ALTER TABLE)
- Enabling **upgrades table protocol** — older clients without DV support **cannot read the table**
- Writing with full optimizations: DBR 14.3 LTS+; Reading: DBR 12.2 LTS+

**Soft-delete lifecycle (GDPR purge = all three steps):**
```sql
DELETE FROM t WHERE ...;                    -- soft-delete: marks rows in deletion vector
REORG TABLE t APPLY (PURGE);                 -- physically rewrites files (skips dropped-column check if mode='rows')
VACUUM t;                                    -- removes unreferenced old file versions
```

---

## Liquid Clustering

**Replaces:** Hive partitioning + Z-ORDER — **mutually exclusive** with both on the same table.

```sql
-- At creation
CREATE TABLE sales (...) CLUSTER BY (customer_id, region);

-- On existing table
ALTER TABLE sales CLUSTER BY (order_date, customer_id);

-- Automatic (Databricks manages keys) — DBR 15.4 LTS+, UC managed tables only
CREATE TABLE sales (...) CLUSTER BY AUTO;
```

**Still requires OPTIMIZE** to reorganize data (incremental, not full rewrite unless Predictive Optimization handles it).

**Supported clustering key types:** Date, Timestamp, TimestampNTZ (DBR 14.3+), String, Integer/Long/Short/Byte, Float/Double/Decimal.

**Clustering keys need stats:** default first 32 columns only — adjust `delta.dataSkippingStatsColumns` if key is beyond that.

**Redefine keys without full table rewrite (the headline advantage):**
```sql
ALTER TABLE sales CLUSTER BY (new_col_a, new_col_b);
OPTIMIZE sales;
```

---

## Liquid Clustering vs. Partitioning vs. Z-Order

| | Partitioning | Z-Ordering | Liquid Clustering |
|---|---|---|---|
| Layout | Separate directory per value | Sorts rows within existing files (space-filling curve) | Files organized around clustering keys, no fixed directories |
| High-cardinality columns | **Bad** — small-file explosion | Workable but files still numerous | **Designed for this** |
| Changing layout later | Requires full table rewrite | Re-run `OPTIMIZE ... ZORDER BY` | `ALTER TABLE ... CLUSTER BY` + `OPTIMIZE` — **incremental**, historical data untouched |
| Maintenance | Static | Manual/scheduled batch job | Still needs `OPTIMIZE`, but incremental; automatable via `CLUSTER BY AUTO` + Predictive Optimization |
| Data skew handling | Poor | Improves co-location, doesn't fix skew directly | **Explicitly designed** to handle skew |
| Combinable with others? | N/A | Yes (Z-Order within partitions) | **No — mutually exclusive** with partitioning and Z-Order |
| Current recommendation | Legacy | Legacy, still supported | **Recommended for all new tables** |

---

## Data Skipping and File Pruning

**Two distinct layers:**
| Layer | What it skips | Mechanism |
|---|---|---|
| **Partition pruning** | Entire directories | Query filter matches partition column → directories never listed |
| **File pruning (data skipping)** | Individual files | Query filter compared against per-file min/max stats in transaction log — works with Liquid Clustering too |

**Control which columns get stats:**
```sql
-- Default: first 32 columns
SET spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols = 32;

-- Explicit columns
ALTER TABLE t SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'col_a,col_b,col_c');
```
**Common pitfall:** if frequently-filtered column is 33rd+ and you haven't customized `dataSkippingStatsColumns` → **no statistics collected** → file pruning silently doesn't happen.

**Refresh optimizer stats:**
```sql
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## Exam Trap Shortlist

1. Deletion vectors → merge-on-read (soft-delete), NOT immediate physical erase.
2. Enabling deletion vectors **upgrades protocol** → older clients may lose read access.
3. Liquid Clustering still requires **OPTIMIZE** to reorganize data (incremental, not automatic by default).
4. Liquid Clustering is **mutually exclusive** with partitioning and Z-Order — cannot combine.
5. Headline advantage: **redefine clustering keys without a full rewrite**.
6. Clustering keys need collected statistics — default only first 32 columns.
7. Two skipping layers: **partition pruning** (directories) vs. **file pruning** (individual files via transaction log stats).
8. `delta.dataSkippingStatsColumns` = which columns to collect stats on; `dataSkippingNumIndexedCols` = how many leading columns are indexed by default.
