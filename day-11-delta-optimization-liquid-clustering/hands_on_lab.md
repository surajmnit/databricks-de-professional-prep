# Day 11 — Hands-On Lab: Deletion Vectors and Liquid Clustering

## Lab Objectives

1. Enable deletion vectors and observe soft-delete vs. physical-delete behavior.
2. Physically materialize soft-deletes with `REORG TABLE ... APPLY (PURGE)`.
3. Create a Liquid Clustered table and confirm `OPTIMIZE` is required to actually cluster data.
4. Reproduce the "Liquid Clustering incompatible with partitioning" error on purpose.
5. Redefine clustering keys on an existing table without a full rewrite.
6. Inspect data-skipping statistics and column indexing limits.

**Environment note:** deletion vectors and Liquid Clustering require DBR 14.1+/14.3+ respectively (Community Edition support varies by version — if a feature isn't available, read the "what to observe" notes as reference).

---

## Step 1 — Enable deletion vectors and observe soft-delete behavior

```sql
CREATE TABLE main.default.customer_activity_day11 (customer_id INT, event STRING)
TBLPROPERTIES ('delta.enableDeletionVectors' = true);

INSERT INTO main.default.customer_activity_day11 VALUES (1, 'login'), (2, 'purchase'), (3, 'logout');

DESCRIBE DETAIL main.default.customer_activity_day11;
-- Note the minReaderVersion / minWriterVersion — these reflect the protocol upgrade from enabling deletion vectors
```
```sql
DELETE FROM main.default.customer_activity_day11 WHERE customer_id = 2;
```
**What to observe:** run `%fs ls` (or `dbutils.fs.ls`) on the table's storage location — the original Parquet file is still present unchanged; only a small deletion-vector side-file and a new `_delta_log` commit have been added. No data file was rewritten.

---

## Step 2 — Physically materialize the soft-delete

```sql
REORG TABLE main.default.customer_activity_day11 APPLY (PURGE);
```
**What to observe:** now check the storage location again — the affected file has been rewritten without the deleted row. Confirm via:
```sql
DESCRIBE HISTORY main.default.customer_activity_day11;
-- Look for the REORG operation as its own commit, separate from the original DELETE commit
```

---

## Step 3 — 💥 Break it on purpose: purge a table that was never soft-deleted, then check `VACUUM`

```python
try:
    spark.sql("VACUUM main.default.customer_activity_day11 RETAIN 0 HOURS")
except Exception as e:
    print("Expected — the default retention safety guard blocks this (Day 19 callback):")
    print(str(e)[:300])
```
This reinforces that `REORG ... APPLY (PURGE)` and `VACUUM` are two **separate** steps — purge rewrites files, vacuum removes old file *versions* — you did both operations for different reasons across Steps 1–3.

---

## Step 4 — Create a Liquid Clustered table

```sql
CREATE TABLE main.default.sales_day11 (
  order_id BIGINT, customer_id BIGINT, region STRING, order_date DATE, amount DOUBLE
) CLUSTER BY (customer_id, region);

INSERT INTO main.default.sales_day11
SELECT id, id % 1000, CASE WHEN id % 3 = 0 THEN 'US' WHEN id % 3 = 1 THEN 'EU' ELSE 'APAC' END,
       DATE'2026-01-01' + CAST(id % 300 AS INT), rand() * 500
FROM range(500000) AS t(id);
```
**Predict then verify:** is the data already clustered immediately after this `INSERT`? **Answer:** not necessarily optimally — clustering is applied incrementally, primarily during `OPTIMIZE`.
```sql
OPTIMIZE main.default.sales_day11;
```
Re-run a filtered query on `customer_id` before and after `OPTIMIZE` and compare the Query Profile / Spark UI scan metrics (Day 8) — you should see fewer files scanned after `OPTIMIZE`.

---

## Step 5 — 💥 Break it on purpose: Liquid Clustering + partitioning incompatibility

```python
try:
    spark.sql("""
        CREATE TABLE main.default.bad_combo_day11 (id INT, region STRING)
        PARTITIONED BY (region)
        CLUSTER BY (id)
    """)
except Exception as e:
    print("Expected — clustering is not compatible with partitioning on the same table:")
    print(str(e)[:300])
```

---

## Step 6 — Redefine clustering keys without a full rewrite

```sql
-- Change the clustering key from (customer_id, region) to (order_date, customer_id)
ALTER TABLE main.default.sales_day11 CLUSTER BY (order_date, customer_id);
OPTIMIZE main.default.sales_day11;

DESCRIBE DETAIL main.default.sales_day11;
-- Confirm the clustering columns have changed, and note that no full-table rewrite command was needed —
-- only an incremental OPTIMIZE.
```

---

## Step 7 — Inspect data-skipping statistics

```sql
DESCRIBE DETAIL main.default.sales_day11;
SHOW TBLPROPERTIES main.default.sales_day11;
-- Look for delta.dataSkippingNumIndexedCols / delta.dataSkippingStatsColumns if customized

ANALYZE TABLE main.default.sales_day11 COMPUTE STATISTICS FOR ALL COLUMNS;
```
**Stretch check:** if this table had more than 32 columns and you wanted to cluster/filter efficiently on column #40, what property would you need to set first? (Answer: `delta.dataSkippingStatsColumns`, to explicitly include that column in the indexed set.)

---

## Stretch Task

Take an existing Hive-style partitioned table (from an earlier day's lab, e.g., Day 9's partition-by-`date` example) and migrate it to Liquid Clustering following the documented migration guidance:
```sql
ALTER TABLE <table> REPLACE PARTITIONED BY WITH CLUSTER BY (date, <other_key>);
OPTIMIZE <table>;
```
Compare file counts and a representative filtered query's scan metrics before and after.

---

## Lab Checklist

- [ ] Enabled deletion vectors and confirmed `DELETE` doesn't rewrite files immediately
- [ ] Ran `REORG TABLE ... APPLY (PURGE)` and confirmed physical file rewrite
- [ ] Confirmed `VACUUM RETAIN 0 HOURS` is still blocked by the default safety guard
- [ ] Created a Liquid Clustered table and confirmed `OPTIMIZE` improves scan efficiency
- [ ] Reproduced the Liquid Clustering + partitioning incompatibility error
- [ ] Redefined clustering keys on an existing table without a full rewrite
- [ ] Inspected data-skipping statistics and `dataSkippingStatsColumns`
- [ ] (Stretch) Migrated a partitioned table to Liquid Clustering

---

## Cross-References
- Day 8: Query Profile scan metrics — used here to observe the before/after `OPTIMIZE` effect.
- Day 9: Transaction log `add` action statistics underlying data skipping.
- Day 19: `VACUUM`/`REORG TABLE ... APPLY (PURGE)` purge lifecycle — now grounded in deletion-vector mechanics.
