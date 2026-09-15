# Day 10 — Hands-On Lab: MERGE, SCD, and Change Data Feed

## Lab Objectives

1. Perform a basic upsert with `MERGE INTO` and reproduce the "multiple match" error on purpose.
2. Implement SCD Type 1 (overwrite) via `MERGE`.
3. Implement SCD Type 2 (full history) via the two-statement pattern.
4. Enable Change Data Feed and read changes both as a batch query and as a stream.
5. Use CDF output to incrementally `MERGE` into a downstream aggregate table.
6. Confirm CDF does not backfill history from before it was enabled.

**Environment note:** everything below works on Community Edition or any Unity Catalog / Hive Metastore workspace — no special features required beyond standard Delta Lake.

---

## Step 1 — Basic `MERGE` upsert

```sql
CREATE TABLE IF NOT EXISTS main.default.dim_customer_day10 (
  customer_id INT, email STRING, address STRING
);
INSERT INTO main.default.dim_customer_day10 VALUES (1, 'a@x.com', '1 Main St');

CREATE OR REPLACE TEMP VIEW customer_updates AS
SELECT * FROM VALUES
  (1, 'a@x.com', '2 Oak Ave'),   -- address changed
  (2, 'b@x.com', '5 Pine Rd')    -- new customer
AS t(customer_id, email, address);

MERGE INTO main.default.dim_customer_day10 AS t
USING customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.email = s.email, t.address = s.address
WHEN NOT MATCHED THEN INSERT (customer_id, email, address) VALUES (s.customer_id, s.email, s.address);

SELECT * FROM main.default.dim_customer_day10;
```

---

## Step 2 — 💥 Break it on purpose: the "multiple match" error

```python
try:
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW bad_updates AS
        SELECT * FROM VALUES
          (1, 'dup1@x.com', 'Addr A'),
          (1, 'dup2@x.com', 'Addr B')   -- SAME customer_id twice
        AS t(customer_id, email, address)
    """)
    spark.sql("""
        MERGE INTO main.default.dim_customer_day10 AS t
        USING bad_updates AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET t.email = s.email, t.address = s.address
    """)
except Exception as e:
    print("Expected — multiple source rows matched one target row:")
    print(str(e)[:400])
```
**Fix — deduplicate the source before merging:**
```sql
CREATE OR REPLACE TEMP VIEW bad_updates_deduped AS
SELECT customer_id, email, address FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY email DESC) AS rn
  FROM bad_updates
) WHERE rn = 1;
```

---

## Step 3 — SCD Type 2 via the two-statement pattern

```sql
CREATE TABLE IF NOT EXISTS main.default.dim_customer_scd2 (
  customer_id INT, email STRING, address STRING,
  effective_date DATE, end_date DATE, is_current BOOLEAN
);
INSERT INTO main.default.dim_customer_scd2 VALUES
  (1, 'a@x.com', '1 Main St', DATE'2025-01-01', NULL, true);

CREATE OR REPLACE TEMP VIEW scd2_updates AS
SELECT * FROM VALUES
  (1, 'a@x.com', '2 Oak Ave', DATE'2026-01-15')   -- address changed
AS t(customer_id, email, address, effective_date);

-- Step A: close out the changed current record
MERGE INTO main.default.dim_customer_scd2 AS t
USING scd2_updates AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.address != s.address OR t.email != s.email) THEN
  UPDATE SET t.end_date = s.effective_date, t.is_current = false;

-- Step B: insert the new current record
INSERT INTO main.default.dim_customer_scd2 (customer_id, email, address, effective_date, end_date, is_current)
SELECT s.customer_id, s.email, s.address, s.effective_date, NULL, true
FROM scd2_updates s
LEFT ANTI JOIN main.default.dim_customer_scd2 t
  ON s.customer_id = t.customer_id AND t.is_current = true AND t.email = s.email AND t.address = s.address;

SELECT * FROM main.default.dim_customer_scd2 ORDER BY customer_id, effective_date;
```
**What to observe:** customer 1 now has **two rows** — the original (now closed with an `end_date` and `is_current = false`) and the new current one — this is the full-history behavior SCD Type 2 is for.

---

## Step 4 — Enable CDF and read changes

```sql
CREATE TABLE IF NOT EXISTS main.default.orders_day10 (order_id INT, status STRING, amount DOUBLE)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

INSERT INTO main.default.orders_day10 VALUES (1, 'PLACED', 100.0), (2, 'PLACED', 250.0);
UPDATE main.default.orders_day10 SET status = 'SHIPPED' WHERE order_id = 1;
DELETE FROM main.default.orders_day10 WHERE order_id = 2;
```
```sql
-- Read all changes from version 0 forward
SELECT * FROM table_changes('main.default.orders_day10', 0);
```
**What to observe:** the `UPDATE` produces **two** rows for `order_id = 1` — one `update_preimage` (status = `PLACED`) and one `update_postimage` (status = `SHIPPED`) — plus a `delete` row for `order_id = 2` and `insert` rows for the original two inserts.

```python
# Same thing via PySpark batch read
changes_df = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("main.default.orders_day10"))
changes_df.select("order_id", "status", "_change_type", "_commit_version").show()
```

---

## Step 5 — 💥 Break it on purpose: no backfill before enabling CDF

```python
spark.sql("CREATE TABLE IF NOT EXISTS main.default.no_cdf_at_first (id INT, val STRING)")
spark.sql("INSERT INTO main.default.no_cdf_at_first VALUES (1, 'first')")   # version 1, BEFORE CDF enabled
spark.sql("ALTER TABLE main.default.no_cdf_at_first SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")  # version 2
spark.sql("INSERT INTO main.default.no_cdf_at_first VALUES (2, 'second')")  # version 3, AFTER CDF enabled

spark.sql("SELECT * FROM table_changes('main.default.no_cdf_at_first', 0)").show()
```
**Predict then verify:** does the change feed show the `id = 1` insert (version 1, before CDF was enabled)?
**Answer:** No — only the `id = 2` insert (made after CDF was turned on) appears. This confirms CDF never backfills.

---

## Step 6 — Use CDF for incremental downstream aggregation

```python
from pyspark.sql import functions as F

# Incrementally apply only NEW changes into a downstream summary table, instead of full recompute
new_changes = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table("main.default.orders_day10")
    .filter("_change_type IN ('insert', 'update_postimage')"))

new_changes.groupBy("status").agg(F.sum("amount").alias("total")).show()
print("This aggregation only processed the CHANGED rows via CDF —")
print("a materialized view recompute would have re-scanned the entire source table instead.")
```

---

## Stretch Task

Build a mini streaming SCD Type 2 consumer: enable CDF on a source dimension-staging table, `readStream` it with `readChangeFeed=true`, and inside a `foreachBatch` function implement the two-statement SCD2 pattern from Step 3 against a target dimension table. This mirrors what `AUTO CDC ... STORED AS SCD TYPE 2` does for you automatically in Day 15 — building it by hand first will make Day 15's declarative version click immediately.

---

## Lab Checklist

- [ ] Performed a basic `MERGE` upsert
- [ ] Reproduced and fixed the "multiple match" `MERGE` error
- [ ] Implemented SCD Type 2 with the two-statement close-then-insert pattern
- [ ] Enabled CDF and read changes via `table_changes()` and PySpark `readChangeFeed`
- [ ] Confirmed an `UPDATE` produces both `update_preimage` and `update_postimage` rows
- [ ] Confirmed CDF does not backfill changes from before it was enabled
- [ ] Used CDF to incrementally aggregate only changed rows into a downstream table
- [ ] (Stretch) Built a streaming `foreachBatch` SCD2 consumer using CDF as the source

---

## Cross-References
- Day 9: Transaction log actions and retention — the mechanism CDF's version-bounded reads rely on.
- Day 15: `AUTO CDC` — the declarative replacement for everything hand-built in Steps 1–3 today.
- Day 16: Streaming Tables vs. Materialized Views — the limitation CDF is designed to address.
- Day 19: `VACUUM` retention interaction with CDF version availability.
