# Day 3 — Hands-On Lab: SQL Transformations and Testing

## Lab Objectives

1. Practice window functions (ROWS vs RANGE, ranking, LAG/LEAD)
2. Implement LEFT SEMI and LEFT ANTI joins
3. Configure a broadcast join and observe the plan
4. Fix a real skewed join using the salted-join pattern
5. Build a testable ETL pipeline using .transform()
6. Write unit tests with assertDataFrameEqual and assertSchemaEqual

**Note:** All steps run in a Databricks notebook. Community Edition is sufficient.

---

## Step 1 — Window Functions: ROWS vs RANGE with Duplicates

**Objective:** Observe the exact difference between ROWS and RANGE when ORDER BY has duplicate values.

### 1a. Create test data with duplicate keys

```python
from pyspark.sql import Window
from pyspark.sql.functions import sum as spark_sum, col

data = [
    ("A", "2024-01-01", 100),
    ("A", "2024-01-01", 200),   # duplicate date for A
    ("A", "2024-01-03", 150),
    ("B", "2024-01-01", 50),
    ("B", "2024-01-02", 75),
]
df = spark.createDataFrame(data, ["grp", "date", "amount"])
df.show()
```

### 1b. Apply ROWS window

```python
rows_window = Window.partitionBy("grp").orderBy("date").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

df_rows = df.withColumn("cumsum_rows", spark_sum("amount").over(rows_window))
df_rows.orderBy("grp", "date").show()
```

**Expected:** For group A: both rows with date 2024-01-01 show cumulative 300. Row with date 2024-01-03 shows 450.

### 1c. Apply RANGE window

```python
range_window = Window.partitionBy("grp").orderBy("date").rangeBetween(
    Window.unboundedPreceding, Window.currentRow
)

df_range = df.withColumn("cumsum_range", spark_sum("amount").over(range_window))
df_range.orderBy("grp", "date").show()
```

**What to observe:** When ORDER BY has no duplicates, ROWS and RANGE produce the same results. The difference emerges with duplicate ORDER BY values — RANGE groups by value (both 2024-01-01 rows get the same cumulative total of 300, since RANGE includes every peer row sharing that value), ROWS counts by physical row position (the two 2024-01-01 rows would differ if their physical order mattered — here they still both land at 300 because both are already included by the time either is the "current row" under ROWS UNBOUNDED PRECEDING, so compare this against Step 1b's output carefully rather than assuming they must differ on every dataset).

---

## Step 2 — Ranking Functions for Deduplication

**Objective:** Get exactly one row per group (top earner per department).

### 2a. Create employee data with ties

```python
employees = [
    ("Engineering", "Alice", 90000),
    ("Engineering", "Bob", 95000),
    ("Engineering", "Carol", 95000),  # tie for top
    ("Sales", "Dave", 80000),
    ("Sales", "Eve", 85000),
]
df_emp = spark.createDataFrame(employees, ["dept", "name", "salary"])
df_emp.show()
```

### 2b. Apply ROW_NUMBER, RANK, DENSE_RANK

```python
from pyspark.sql.functions import row_number, rank, dense_rank

window = Window.partitionBy("dept").orderBy(col("salary").desc())

df_ranked = df_emp \
    .withColumn("row_num", row_number().over(window)) \
    .withColumn("rank", rank().over(window)) \
    .withColumn("dense_rank", dense_rank().over(window))

df_ranked.orderBy("dept", col("salary").desc()).show()
```

**Expected output for Engineering:**
| name | salary | row_num | rank | dense_rank |
|------|--------|---------|------|------------|
| Bob  | 95000  | 1       | 1    | 1          |
| Carol| 95000  | 2       | 1    | 1          |
| Alice| 90000  | 3       | 3    | 2          |

**Key observation:** ROW_NUMBER breaks ties arbitrarily. RANK and DENSE_RANK both assign rank 1 to Bob and Carol. RANK skips to 3, DENSE_RANK goes to 2.

### 2c. Extract top earner per department

```python
df_top = df_ranked.filter(col("row_num") == 1)
df_top.show()
```

---

## Step 3 — LEFT SEMI and LEFT ANTI Joins

**Objective:** Practice the two most-misunderstood join types.

### 3a. LEFT SEMI: customers who have placed orders

```python
customers = spark.createDataFrame([
    (1, "Alice"), (2, "Bob"), (3, "Carol"), (4, "Dave")
], ["customer_id", "name"])

orders = spark.createDataFrame([
    (1, "2024-01-01", 100),
    (1, "2024-01-15", 200),
    (2, "2024-01-10", 150),
], ["customer_id", "order_date", "amount"])

# LEFT SEMI: customers who HAVE placed at least one order
semi_result = customers.join(orders, "customer_id", "leftsemi")
semi_result.show()
```

**Expected:** Alice and Bob (customer_ids 1 and 2). Carol and Dave have no orders and are excluded.

### 3b. LEFT ANTI: customers who have NOT placed orders

```python
anti_result = customers.join(orders, "customer_id", "leftanti")
anti_result.show()
```

**Expected:** Carol and Dave (customer_ids 3 and 4). Alice and Bob are excluded because they have orders.

### 3c. Verify equivalence with IN/NOT IN subqueries

```python
# Register the DataFrames as temp views first — spark.sql() can only see
# named views/tables, not Python variable names
customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")

# LEFT SEMI equivalent: IN subquery
spark.sql("""
    SELECT * FROM customers WHERE customer_id IN (SELECT customer_id FROM orders)
""").show()

# LEFT ANTI equivalent: NOT IN subquery
spark.sql("""
    SELECT * FROM customers WHERE customer_id NOT IN (SELECT customer_id FROM orders)
""").show()
```

**Exam note:** LEFT SEMI does NOT return columns from the right table. It only filters the left. This is frequently tested.

---

## Step 4 — Broadcast Join and Plan Inspection

**Objective:** Observe how Spark implements a broadcast join vs a sort-merge join.

### 4a. Create a small and a large table

```python
# Small dimension table (10 rows)
dim = spark.createDataFrame([(i, f"product_{i}") for i in range(10)], ["product_id", "product_name"])

# Large fact table (100,000 rows)
import random
fact_data = [(random.randint(0, 9), random.randint(1, 1000)) for _ in range(100_000)]
fact = spark.createDataFrame(fact_data, ["product_id", "quantity"])
print(f"Fact partitions: {fact.rdd.getNumPartitions()}")
```

### 4b. Join without broadcast hint — observe the plan

```python
result = fact.join(dim, "product_id")
result.explain("formatted")
```

**What to look for in the plan:** "Exchange" = shuffle. "BroadcastExchange" = broadcast join (no shuffle).

### 4c. Join with broadcast hint

```python
from pyspark.sql.functions import broadcast

result_broadcast = fact.join(broadcast(dim), "product_id")
result_broadcast.explain("formatted")
```

**What to observe:** The plan should show "BroadcastExchange" instead of "Exchange." The small dim table is sent to all executors instead of shuffling the large fact table.

### 4d. Verify correctness and timing

```python
count_normal = result.count()
count_broadcast = result_broadcast.count()
print(f"Counts match: {count_normal == count_broadcast}")

import time
start = time.time()
result.count()
normal_time = time.time() - start

start = time.time()
result_broadcast.count()
broadcast_time = time.time() - start

print(f"Normal join: {normal_time:.2f}s, Broadcast join: {broadcast_time:.2f}s")
```

**Break it on purpose:** Broadcast a large table instead of dim:

```python
# BROKEN: Broadcasting a large table causes memory pressure
large_table = fact  # 100k rows — too big to broadcast
result_broken = dim.join(broadcast(large_table), "product_id")
result_broken.explain("formatted")  # Observe the plan even if it doesn't OOM on CE
```

On a small CE cluster this may not crash but you will see the broadcast metadata in the plan.

---

## Step 5 — Fix a Real Skewed Join with Salting

**Objective:** Reproduce a skewed join, confirm the skew signature, then fix it with the corrected salted-join pattern from `notes.md`.

### 5a. Create a skewed join scenario

```python
# 90% of fact rows share one key — a classic skew scenario
heavy_fact = [("heavy_key", i) for i in range(90_000)]
light_fact = [(f"key_{i}", i) for i in range(10_000)]
fact_skewed = spark.createDataFrame(heavy_fact + light_fact, ["join_key", "value"])

dim_small = spark.createDataFrame(
    [("heavy_key", "Heavy Segment")] + [(f"key_{i}", f"Segment_{i}") for i in range(10_000)],
    ["join_key", "segment_name"]
)

# Disable AQE skew handling temporarily so the skew is visible unmitigated
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

result_skewed = fact_skewed.join(dim_small, "join_key")
result_skewed.explain("formatted")
result_skewed.count()
```

**What to observe in the Spark UI Stages tab:** one task should show dramatically higher shuffle read/duration than the rest — the `"heavy_key"` partition dominating.

### 5b. Apply the salted-join fix

```python
from pyspark.sql.functions import rand, explode, array, lit, concat, col

n_salt = 10

fact_salted = (
    fact_skewed
    .withColumn("salt", (rand() * n_salt).cast("int"))
    .withColumn("join_key_salted", concat(col("join_key"), lit("-"), col("salt")))
)

dim_salted = (
    dim_small
    .withColumn("salt", explode(array([lit(i) for i in range(n_salt)])))
    .withColumn("join_key_salted", concat(col("join_key"), lit("-"), col("salt")))
)

result_salted = fact_salted.join(dim_salted, "join_key_salted")
print(f"Row count matches unsalted result: {result_salted.count() == result_skewed.count()}")
result_salted.explain("formatted")
```

**What to observe:** row counts match the unsalted version exactly (the salting is purely a physical redistribution trick — it must never change the logical result), and the Spark UI Stages tab should now show much more even task durations, since the `"heavy_key"` rows are spread across `n_salt` partitions instead of one.

```python
# Re-enable AQE skew handling for the rest of the notebook
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**Break it on purpose:** comment out the `explode(array(...))` line on `dim_salted` and instead just add a constant `salt = 0` column, then re-run the join. **Predict then verify:** does the row count still match `result_skewed`? **Answer:** No — you'll only get matches for large-table rows that happened to be randomly salted to `0`, silently dropping roughly `(n_salt - 1) / n_salt` of the real matches. This reproduces exactly the kind of broken salting that looks like it works (no error is raised) but silently returns wrong data — the row-count check above is what would have caught it.

---

## Step 6 — Build a Testable ETL Pipeline with .transform()

**Objective:** Convert inline DataFrame operations into transform functions that can be unit tested.

### 6a. Define transform functions

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, trim, col, when, current_timestamp

def clean_pii(df: DataFrame) -> DataFrame:
    return df.withColumn("email", when(col("email").isNotNull(), "REDACTED").otherwise(None))

def standardize_names(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("first_name", upper(trim(col("first_name"))))
        .withColumn("last_name",  upper(trim(col("last_name"))))
    )

def add_metadata(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", current_timestamp())

def run_silver_pipeline(df: DataFrame) -> DataFrame:
    return (df
        .transform(clean_pii)
        .transform(standardize_names)
        .transform(add_metadata)
    )
```

### 6b. Test each transform function individually

```python
test_df = spark.createDataFrame([
    ("alice@example.com", "  Alice  ", "Smith", 1000),
    ("bob@example.com", "Bob", "Jones", 2000),
], ["email", "first_name", "last_name", "revenue"])

# Test clean_pii
cleaned = clean_pii(test_df)
print("After clean_pii:")
cleaned.select("email").show()

# Verify no PII remains
pii_count = cleaned.filter(col("email").isNotNull()).filter(col("email") != "REDACTED").count()
print(f"Unmasked PII remaining (should be 0): {pii_count}")

# Test standardize_names
standardized = standardize_names(test_df)
print("After standardize_names:")
standardized.select("first_name").show()
```

### 6c. Run full pipeline

```python
result = run_silver_pipeline(test_df)
print("Full pipeline result:")
result.show()
result.printSchema()
```

---

## Step 7 — Write Unit Tests with assertDataFrameEqual

**Objective:** Learn the testing utilities the exam covers, and confirm the order-independence default for yourself.

```python
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

# Test 1: Data content equality
def test_clean_pii_masks_emails(spark):
    input_df = spark.createDataFrame([
        ("alice@example.com", "Alice", 100),
    ], ["email", "name", "revenue"])

    result = clean_pii(input_df)
    expected = spark.createDataFrame([
        ("REDACTED", "Alice", 100),
    ], ["email", "name", "revenue"])

    assertDataFrameEqual(result, expected)
    print("test_clean_pii_masks_emails: PASSED")

# Test 2: Schema equality
def test_pipeline_schema(spark):
    input_df = spark.createDataFrame([
        ("a@b.com", "Alice", "Smith", 100),
    ], ["email", "first_name", "last_name", "revenue"])

    result = run_silver_pipeline(input_df)
    expected_schema = "email STRING, first_name STRING, last_name STRING, revenue BIGINT, processed_at TIMESTAMP"
    assertSchemaEqual(result, expected_schema)
    print("test_pipeline_schema: PASSED")

# Run tests
test_clean_pii_masks_emails(spark)
test_pipeline_schema(spark)
```

### Confirm the row-order default for yourself

```python
# Same data, deliberately different row order
actual_order = spark.createDataFrame([("Bob", 25), ("Alice", 30)], ["name", "age"])
expected_order = spark.createDataFrame([("Alice", 30), ("Bob", 25)], ["name", "age"])

# Default: passes, because checkRowOrder defaults to False
assertDataFrameEqual(actual_order, expected_order)
print("Default comparison passed despite different row order — as expected")

# Force order sensitivity: this one should now raise
try:
    assertDataFrameEqual(actual_order, expected_order, checkRowOrder=True)
except AssertionError as e:
    print("checkRowOrder=True correctly failed on the reordered rows:")
    print(str(e)[:300])
```

### Break it on purpose: assertDataFrameEqual with wrong expected value

```python
expected_broken = spark.createDataFrame([
    ("NOT_REDACTED", "Alice", 100),  # email not masked
], ["email", "name", "revenue"])

try:
    assertDataFrameEqual(cleaned, expected_broken)
except AssertionError as e:
    print("AssertionError caught as expected:")
    print(str(e)[:500])
```

This shows clear diff output for debugging test failures.

---

## Stretch Task: Deduplication Pipeline

Implement a deduplication pipeline using ROW_NUMBER:

```python
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window
from pyspark.testing import assertDataFrameEqual

def deduplicate_customers(df):
    window = Window.partitionBy("email").orderBy(col("last_updated").desc())
    return df.withColumn("rn", row_number().over(window)).filter(col("rn") == 1).drop("rn")

def test_deduplication(spark):
    input_df = spark.createDataFrame([
        ("alice@example.com", "Alice", "2024-01-01"),
        ("alice@example.com", "Alice X", "2024-01-15"),  # more recent
    ], ["email", "name", "last_updated"])

    result = deduplicate_customers(input_df)
    expected = spark.createDataFrame([
        ("alice@example.com", "Alice X", "2024-01-15"),
    ], ["email", "name", "last_updated"])

    assertDataFrameEqual(result, expected)
    print("test_deduplication: PASSED")
```

---

## Lab Checklist

- [ ] Observed ROWS vs RANGE difference with duplicate ORDER BY values
- [ ] Applied ROW_NUMBER, RANK, DENSE_RANK for deduplication
- [ ] Implemented LEFT SEMI and LEFT ANTI joins (via DataFrame API and SQL subqueries)
- [ ] Inspected broadcast join plan vs sort-merge plan
- [ ] Reproduced a skewed join, confirmed the skew signature, and fixed it with a correct salted join
- [ ] Broke the salted join on purpose (constant salt on one side) and observed the silent row-count mismatch
- [ ] Created transform() pipeline functions
- [ ] Unit tested individual transform functions
- [ ] Written assertDataFrameEqual and assertSchemaEqual tests
- [ ] Confirmed assertDataFrameEqual's row-order default for yourself with `checkRowOrder=True`
- [ ] Verified test failure output with broken assertion
- [ ] (Stretch) Completed deduplication with ROW_NUMBER

---

## Cross-References

- Day 5: Wide vs narrow transformations — groupBy creates shuffle boundaries
- Day 6: Shuffle behavior during large joins; AQE skew join vs. manual salting
- Day 8: Query Profile for identifying inefficient join strategies
- Day 17: Quarantine patterns and data quality validation
