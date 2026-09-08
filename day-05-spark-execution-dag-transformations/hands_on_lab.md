# Day 5 — Hands-On Lab: Spark Execution, DAG, and Physical Plans

## Lab Objectives

1. Observe lazy evaluation — confirm transformations are not executed until actions
2. Identify narrow vs wide transformations via explain()
3. Read and interpret physical plans
4. Observe multiple Job creation from multiple actions
5. Trigger whole-stage code generation observations
6. Break a query by causing unnecessary shuffles

**Note:** All steps run in a Databricks notebook. Community Edition is sufficient.

---

## Step 1 — Lazy Evaluation: Transformations Are Not Executed Until Action

**Objective:** Confirm that transformations build the plan but do not execute until an action.

### 1a. Demonstrate lazy evaluation

```python
import time
from pyspark.sql import functions as F

# Create a dataset with an intentional delay (simulating expensive read)
data = [(i, f"value_{i}") for i in range(10000)]
df = spark.createDataFrame(data, ["id", "payload"])

# Add a filter — this does NOT execute yet
df_filtered = df.filter(F.col("id") % 2 == 0)

# Add a groupBy — still NOT executed
df_grouped = df_filtered.groupBy((F.col("id") % 10).alias("bucket")).count()

# The plan is built but no execution has happened yet
print("Plan built — no execution yet. Time: 0.0s")

# First action — triggers full execution
start = time.time()
result = df_grouped.collect()
elapsed = time.time() - start
print(f"Action executed in {elapsed:.3f}s. Result: {result[:3]}")
```

### 1b. Demonstrate two actions = two Jobs, no shared caching

```python
# Without caching, the same transformation chain runs twice
start = time.time()
count = df_filtered.count()
print(f"Job 1 (count): {time.time() - start:.3f}s -> {count} rows")

start = time.time()
total = df_filtered.select(F.sum("id")).collect()
print(f"Job 2 (sum): {time.time() - start:.3f}s -> {total}")

# With caching — the plan executes once, subsequent actions use cached data
print("\n--- With caching ---")
df_filtered_cached = df_filtered.cache()
start = time.time()
count = df_filtered_cached.count()  # Reads source, filters, caches
print(f"Job 1 (count with cache): {time.time() - start:.3f}s")

start = time.time()
total = df_filtered_cached.select(F.sum("id")).collect()  # Reads from cache
print(f"Job 2 (sum from cache): {time.time() - start:.3f}s -> {total}")
```

**What to observe:** Job 2 with cache is much faster — it reads from cached data, not from source.

---

## Step 2 — Narrow vs Wide Transformations via explain()

**Objective:** Use explain("formatted") to see where stage boundaries are created.

### 2a. Narrow transformations — same Stage

```python
df = spark.range(1000).repartition(10)
df_filtered = df.filter(F.col("id") % 2 == 0)
df_with_col = df_filtered.withColumn("doubled", F.col("id") * 2)

# Trigger execution
result = df_with_col.count()

# Inspect the plan
print("=== Plan for narrow transformations ===")
df_with_col.explain("formatted")
```

**What to look for:** There should be NO "Exchange" (shuffle) operator. All operations are in one stage.

### 2b. Wide transformation — new Stage boundary

```python
# Same DataFrame, add a groupBy (wide transformation)
df_wide = df_filtered.groupBy((F.col("id") % 5).alias("bucket")).count()

# Trigger execution
result2 = df_wide.count()

print("\n=== Plan for wide transformation (groupBy) ===")
df_wide.explain("formatted")
```

**What to look for:** "Exchange" operator appears — this is the shuffle boundary, creating a new Stage. The formatted explain shows Stage separation via the Exchange node.

### 2c. Multiple shuffles — multiple Stage boundaries

```python
df_multi = (spark.range(10000)
    .repartition(20)                        # Shuffle 1: new Stage
    .filter(F.col("id") % 3 == 0)           # Same Stage as above
    .groupBy(F.col("id") % 10)               # Shuffle 2: new Stage
    .count()
    .orderBy(F.col("count").desc())           # Shuffle 3: new Stage
)

result3 = df_multi.collect()
print("Check Spark UI — should show 4 Stages (initial read + 3 shuffle boundaries)")
df_multi.explain("formatted")
```

**Exam note:** Each shuffle boundary in the physical plan = a new Stage in the Spark UI.

---

## Step 3 — Reading Physical Plans in Detail

**Objective:** Interpret the formatted explain output.

### 3a. GroupBy with count

```python
df = spark.createDataFrame([
    ("A", 100), ("B", 200), ("A", 150), ("B", 250)
], ["region", "amount"])

# Basic explain
print("=== Simple explain ===")
df.groupBy("region").count().explain()

print("\n=== Formatted explain ===")
df.groupBy("region").count().explain("formatted")
```

Read the formatted output bottom-to-top (this is execution order):
- Bottom: Scan (read data)
- Middle: Exchange (shuffle — new Stage boundary)
- Top: HashAggregate (aggregation in new Stage)

### 3b. Join — observe BroadcastExchange vs SortMergeJoin

```python
from pyspark.sql.functions import broadcast

# Small table (will be broadcast)
dim = spark.createDataFrame([("A", "Region-A"), ("B", "Region-B")], ["region", "region_name"])

# Large table
fact = spark.createDataFrame([(i, f"key_{i%3}", i*10) for i in range(1000)], ["id", "region", "amount"])

# Join without hint — observe the plan
print("=== Join without broadcast hint ===")
fact.join(dim, "region").explain("formatted")

print("\n=== Join with broadcast hint ===")
fact.join(broadcast(dim), "region").explain("formatted")
```

**What to observe:**
- Without hint: SortMergeJoin + Exchange (shuffle on both sides)
- With hint: BroadcastExchange on dim + no shuffle on fact side

### 3c. Filter and column pruning

```python
# Create a wide table
wide_data = [(i, f"val_{i}", i*2, i**2, i%100, "extra") for i in range(1000)]
df_wide = spark.createDataFrame(wide_data, ["id", "name", "a", "b", "c", "extra_col"])

# Select only needed columns AFTER filter
df_selected = df_wide.filter(F.col("id") > 500).select("id", "name", "a")
df_selected.explain("formatted")
```

**Catalyst optimization:** Notice that the Scan reads all columns but the Project (select) happens early. Catalyst tries to push column selection down, but not all sources support column pruning.

---

## Step 4 — Multiple Actions = Multiple Independent Jobs

**Objective:** Confirm each action creates a new Job.

```python
df = spark.range(1000).repartition(20)

# Job 1: count
job1 = df.count()
print(f"Job 1 (count): {job1}")

# Job 2: collect (separate Job)
job2 = df.filter(F.col("id") % 2 == 0).collect()
print(f"Job 2 (collect): {len(job2)} rows")

# Job 3: take (separate Job)
job3 = df.take(10)
print(f"Job 3 (take): {job3}")

# Job 4: write (separate Job)
df.write.mode("overwrite").format("noop").execute()
print("Job 4 (write): completed")
```

**What to check in Spark UI:**
1. Go to Jobs tab — should show 4 completed Jobs
2. Each Job has its own set of Stages
3. Without caching, each Job re-reads data from source

---

## Step 5 — Whole-Stage Code Generation

**Objective:** Observe the `*` prefix on operators (indicates whole-stage codegen).

```python
# Whole-stage codegen collapses multiple operators into one generated function
df = spark.createDataFrame([(i, f"name_{i}") for i in range(100)], ["id", "name"])

# Multiple narrow transformations — should use whole-stage codegen
df_result = (df
    .filter(F.col("id") > 10)
    .withColumn("doubled", F.col("id") * 2)
    .withColumn("upper_name", F.upper(F.col("name")))
    .select("id", "doubled", "upper_name")
)

df_result.explain("formatted")
```

**What to observe:** Operators with `*` prefix (e.g., `* Project`, `* Filter`) are part of whole-stage codegen — they are collapsed into a single generated function for performance.

**Wide transformations break codegen:** groupBy, join, sort break whole-stage codegen because they require shuffle operations. Each side of the shuffle is a separate codegen stage.

---

## Step 6 — Break It: Unnecessary Shuffles

**Objective:** Identify and fix unnecessary shuffle operations.

### 6a. Repartition then coalesce (unnecessary intermediate shuffle)

```python
# Create data
df = spark.range(10000).repartition(100)  # Shuffle to 100 partitions
print(f"After repartition(100): {df.rdd.getNumPartitions()} partitions")

# The above repartition is unnecessary if you immediately coalesce
df_bad = df.coalesce(10)  # Reduces from 100 to 10 — but 100-partition shuffle was wasteful
print(f"After coalesce(10): {df_bad.rdd.getNumPartitions()} partitions")

result_bad = df_bad.count()
print(f"Plan for bad approach:")
df_bad.explain("formatted")

# Better: Start with the right partition count
df_good = spark.range(10000).coalesce(10)  # Start with default parallelism, coalesce to 10
print(f"\nAfter coalesce(10) from default: {df_good.rdd.getNumPartitions()} partitions")
result_good = df_good.count()

print("Good plan:")
df_good.explain("formatted")
```

**What to observe:** The bad approach has one Exchange (repartition to 100), then a local coalesce. The good approach has no Exchange — just a local partition reduction.

### 6b. sort before take (unnecessary global sort)

```python
# Bad: Sort entire dataset, then take 10
import time
df = spark.range(100000).repartition(100)
start = time.time()
result_sort = df.orderBy(F.col("id").desc()).take(10)
print(f"Sort-then-take: {time.time() - start:.3f}s")

# Better: Use window function to rank and filter
from pyspark.sql.window import Window
start = time.time()
result_window = (df
    .withColumn("rn", F.row_number().over(Window.orderBy(F.col("id").desc())))
    .filter(F.col("rn") <= 10)
    .take(10)
)
print(f"Window-then-filter: {time.time() - start:.3f}s")
```

**What to observe:** On large datasets, the window approach avoids a global sort by computing row numbers locally. For very small n (e.g., top 10 of 100k), the difference may be small — but for massive datasets the sort cost dominates.

---

## Lab Checklist

- [ ] Observed lazy evaluation — no execution until action
- [ ] Confirmed two actions without caching = two independent reads
- [ ] Confirmed two actions with caching = second action reads from cache
- [ ] Identified narrow transformations (no Exchange in plan)
- [ ] Identified wide transformations (Exchange = new Stage boundary)
- [ ] Observed BroadcastExchange vs SortMergeJoin in join plans
- [ ] Confirmed multiple actions = multiple independent Jobs
- [ ] Read formatted explain bottom-to-top (execution order)
- [ ] Observed whole-stage codegen (`*` prefix on operators)
- [ ] Identified unnecessary shuffle pattern (repartition then coalesce)
- [ ] Compared sort-then-take vs window-then-filter

---

## Cross-References

- **Day 4:** Jobs/Stages/Tasks hierarchy — Stage boundaries come from these shuffle operations
- **Day 6:** Deep-dive on what happens during the Exchange (shuffle read/write)
- **Day 7:** Memory usage during HashAggregate vs SortAggregate
- **Day 8:** Spark UI — visualize the DAG and stage boundaries
