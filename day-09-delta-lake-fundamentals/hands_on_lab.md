# Day 9 — Hands-On Lab: Delta Transaction Log, Managed vs. External Tables, Partitioning & Z-Ordering

## Lab Objectives

1. Inspect the raw `_delta_log` JSON commit files and a checkpoint file directly.
2. Observe `DESCRIBE HISTORY` and time travel in action.
3. Reproduce the official exam sample scenario: rename a mistyped table and confirm the log/files are untouched.
4. Compare managed vs. external table behavior on `DROP TABLE`.
5. Reproduce the small-file problem from a poorly chosen (high-cardinality) partition column, and contrast with a well-chosen one.
6. Run `OPTIMIZE ... ZORDER BY` and inspect its effect via `DESCRIBE HISTORY` operation metrics.

**Environment note:** All steps work on Community Edition. Predictive Optimization (Part 4 of notes) is an account/metastore-level feature you likely cannot toggle yourself on a personal CE workspace — read that section conceptually; the `_delta_log` inspection and partitioning exercises are fully hands-on regardless.

---

## Step 1 — Inspect the transaction log directly

```python
spark.sql("DROP TABLE IF EXISTS main.default.day9_orders")
spark.sql("""
    CREATE TABLE main.default.day9_orders (order_id INT, amount DOUBLE)
    USING DELTA
""")
spark.sql("INSERT INTO main.default.day9_orders VALUES (1, 100.0), (2, 200.0)")
spark.sql("INSERT INTO main.default.day9_orders VALUES (3, 300.0)")
spark.sql("UPDATE main.default.day9_orders SET amount = 999.0 WHERE order_id = 1")

# Find the table's storage location
location = spark.sql("DESCRIBE DETAIL main.default.day9_orders").collect()[0]["location"]
print(location)

# List the _delta_log folder
display(dbutils.fs.ls(f"{location}/_delta_log/"))
```

```python
# Read one JSON commit file directly to see the raw actions
commit_path = f"{location}/_delta_log/00000000000000000002.json"  # adjust to the UPDATE's version
for line in spark.read.text(commit_path).collect():
    print(line.value)
```
**What to observe:** the `UPDATE` commit should show a `remove` action for the old file (containing order_id=1's original row) and an `add` action for a new file containing the corrected row — confirming Delta rewrites files rather than editing them in place.

---

## Step 2 — History and time travel

```python
spark.sql("DESCRIBE HISTORY main.default.day9_orders").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# Query an earlier version
spark.sql("SELECT * FROM main.default.day9_orders VERSION AS OF 1").show()

# Restore to before the UPDATE
spark.sql("RESTORE TABLE main.default.day9_orders TO VERSION AS OF 1")
spark.sql("SELECT * FROM main.default.day9_orders").show()
```

---

## Step 3 — Reproduce the official exam scenario: rename doesn't touch the log

```python
spark.sql("CREATE TABLE main.default.sales_by_stor (id INT, amount DOUBLE) USING DELTA")
spark.sql("INSERT INTO main.default.sales_by_stor VALUES (1, 50.0)")

location_before = spark.sql("DESCRIBE DETAIL main.default.sales_by_stor").collect()[0]["location"]
history_before = spark.sql("DESCRIBE HISTORY main.default.sales_by_stor").count()

# Fix the typo
spark.sql("ALTER TABLE main.default.sales_by_stor RENAME TO main.default.sales_by_store")

location_after = spark.sql("DESCRIBE DETAIL main.default.sales_by_store").collect()[0]["location"]
history_after = spark.sql("DESCRIBE HISTORY main.default.sales_by_store").count()

print(f"Location unchanged: {location_before == location_after}")
print(f"History entry count unchanged: {history_before == history_after}")
```
**Predict then verify:** does the rename create a new transaction log or change the commit history count?
**Answer:** No on both counts — only the metastore's name-to-location mapping changed. This is the exact official sample question (Q1) reproduced practically.

---

## Step 4 — Managed vs. external table drop behavior

```python
ext_path = "/tmp/day9_external_demo"
dbutils.fs.rm(ext_path, recurse=True)

spark.sql(f"""
    CREATE TABLE main.default.day9_external (id INT)
    USING DELTA
    LOCATION '{ext_path}'
""")
spark.sql("INSERT INTO main.default.day9_external VALUES (1), (2)")

# Drop the external table
spark.sql("DROP TABLE main.default.day9_external")

# Check if the files still exist on disk
files_remain = len(dbutils.fs.ls(ext_path)) > 0
print(f"External table files remain after DROP: {files_remain}")   # Expect: True

# Compare with a managed table
spark.sql("CREATE TABLE main.default.day9_managed (id INT) USING DELTA")
managed_location = spark.sql("DESCRIBE DETAIL main.default.day9_managed").collect()[0]["location"]
spark.sql("INSERT INTO main.default.day9_managed VALUES (1)")
spark.sql("DROP TABLE main.default.day9_managed")

try:
    dbutils.fs.ls(managed_location)
    print("Managed table files still exist (unexpected)")
except Exception:
    print("Managed table files were deleted along with the table, as expected")
```

---

## Step 5 — Break it on purpose: the small-file problem from a bad partition column

```python
from pyspark.sql import functions as F

# Simulate a "post" table like the official sample question's schema
posts = spark.range(50000).select(
    F.col("id").alias("post_id"),
    (F.col("id") % 5000).alias("user_id"),
    (F.current_timestamp() - F.expr("INTERVAL 1 SECONDS") * F.col("id")).alias("post_time")
).withColumn("date", F.to_date("post_time"))

# BAD: partition by a full timestamp (effectively unique per row)
posts.write.format("delta").mode("overwrite").partitionBy("post_time").save("/tmp/day9_bad_partition")
bad_file_count = len(dbutils.fs.ls("/tmp/day9_bad_partition"))
print(f"Partition directories with post_time (BAD): {bad_file_count}")

# GOOD: partition by a coarse date column
posts.write.format("delta").mode("overwrite").partitionBy("date").save("/tmp/day9_good_partition")
good_file_count = len(dbutils.fs.ls("/tmp/day9_good_partition"))
print(f"Partition directories with date (GOOD): {good_file_count}")
```
**What to observe:** the `post_time` version should produce a directory (partition) for nearly every single row — a dramatic small-file explosion — while the `date` version produces a small, manageable number of partitions holding many rows each. This is the official sample question's `post_id`/`post_time`/`date`/`user_id` scenario reproduced with real file counts.

---

## Step 6 — Z-Ordering and its effect

```python
large_table = spark.range(1_000_000).withColumn("customer_id", (F.col("id") % 10000))
large_table.write.format("delta").mode("overwrite").save("/tmp/day9_zorder_demo")

spark.sql("CREATE TABLE IF NOT EXISTS main.default.day9_zorder_demo USING DELTA LOCATION '/tmp/day9_zorder_demo'")

# Before Z-Ordering: query a specific customer_id and note files scanned (check Query Profile / Spark UI, Day 8)
spark.sql("SELECT * FROM main.default.day9_zorder_demo WHERE customer_id = 42").explain("formatted")

# Apply Z-Ordering
spark.sql("OPTIMIZE main.default.day9_zorder_demo ZORDER BY (customer_id)")

# After: re-run the same filter and compare files-scanned metrics (Day 8 Query Profile skill)
spark.sql("SELECT * FROM main.default.day9_zorder_demo WHERE customer_id = 42").explain("formatted")

spark.sql("DESCRIBE HISTORY main.default.day9_zorder_demo").select("operation", "operationMetrics").show(truncate=False)
```
**What to observe:** the `OPTIMIZE` operation's `operationMetrics` should show file counts before/after clustering. On a SQL warehouse, compare the two queries' Query Profile Scan operator metrics (Day 8) for the scanned-vs-pruned improvement.

---

## Stretch Task

Using `system.billing.usage` (Day 21) if your workspace has Predictive Optimization enabled and system tables accessible, look for `OPTIMIZE`/`VACUUM`/`ANALYZE` usage entries attributed to a "Predictive Optimization" SKU/job type rather than a job you scheduled yourself — this confirms the automation described in Part 4 of the notes is actually running against your managed tables.

---

## Lab Checklist

- [ ] Read a raw JSON commit file and identified `add`/`remove` actions from an `UPDATE`
- [ ] Used `DESCRIBE HISTORY`, time travel (`VERSION AS OF`), and `RESTORE TABLE`
- [ ] Reproduced the official exam rename scenario and confirmed the log/location are unchanged
- [ ] Compared managed vs. external table file survival after `DROP TABLE`
- [ ] Reproduced the small-file problem with a timestamp partition column vs. a date column
- [ ] Ran `OPTIMIZE ... ZORDER BY` and inspected before/after operation metrics
- [ ] (Stretch) Looked for Predictive Optimization activity in system tables

---

## Cross-References
- Day 8: Query Profile's Scan operator metrics — used here to evaluate Z-Ordering's effect.
- Day 10: `MERGE`/CDC — builds directly on the `add`/`remove` mechanics inspected in Step 1.
- Day 11: Liquid Clustering — the automatic alternative to the manual `OPTIMIZE ... ZORDER BY` from Step 6.
- Day 19: `VACUUM` and physical purge — the same tombstone lifecycle observed in Step 1's `UPDATE`.
