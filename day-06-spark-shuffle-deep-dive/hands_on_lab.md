# Day 6 — Hands-On Lab: Spark Shuffle Deep-Dive

> ⚠️ **Correction notice:** This version fixes two issues found in an earlier draft: (1) Step 2b claimed "1000 reduce tasks, each handling 1000 rows" for a groupBy that only has 10 distinct keys — in reality, with 1000 shuffle partitions and only 10 possible key values, roughly 990 of those partitions would be empty; the step now describes this correctly (and it's actually a better illustration of the "too many partitions" overhead problem). (2) Step 6b compared an integer column to a string literal (`F.col("bucket") == "5"`), which has version/ANSI-mode-dependent behavior — fixed to compare against the integer `5`.

## Lab Objectives

1. Observe shuffle write and read metrics in the Spark UI
2. Compare shuffle partition counts and their impact
3. Trigger and diagnose data skew via shuffle metrics
4. Observe spill behavior
5. Compare broadcast join vs shuffle join via explain()
6. Break it: cause unnecessary shuffle

**Note:** All steps run in a Databricks notebook. Community Edition is sufficient.

---

## Step 1 — Observe Shuffle Write and Read Metrics

**Objective:** Understand what the Spark UI shows for shuffle operations.

### 1a. Run a groupBy and inspect shuffle metrics

```python
from pyspark.sql import functions as F

# Create data with known size
data = [(f"key_{i % 50}", i) for i in range(100000)]  # 50 unique keys
df = spark.createDataFrame(data, ["key", "value"])
df = df.repartition(20)  # Start with 20 partitions

print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# Run a groupBy — this triggers a shuffle
result = df.groupBy("key").agg(F.sum("value")).collect()
print(f"Result: {len(result)} unique keys")

print("Check Spark UI -> Stages tab:")
print("  - Look at Shuffle Write: Bytes Written and Records Written")
print("  - Look at Shuffle Read: Bytes Read and Remote Reads")
print("  - Note the number of tasks (should be 20 for map stage, 200 for shuffle reduce stage)")
```

### 1b. Compare shuffle metrics across operations

```python
import time

# Operation 1: groupBy (shuffle)
start = time.time()
df.groupBy("key").count().collect()
print(f"groupBy shuffle time: {time.time() - start:.3f}s")

# Operation 2: filter (no shuffle)
start = time.time()
df.filter(F.col("value") > 50000).count().collect()
print(f"filter (no shuffle) time: {time.time() - start:.3f}s")

print("Observe: groupBy is significantly slower due to shuffle overhead")
print("Check Spark UI: filter Stage shows no Shuffle Write/Read columns")
```

---

## Step 2 — Shuffle Partition Count Impact

**Objective:** Observe how changing shuffle partition count affects performance.

### 2a. Too few shuffle partitions

```python
# Set very few shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 5)

df = spark.range(100000).repartition(10)
start = time.time()
result = df.groupBy((F.col("id") % 10).alias("bucket")).count().collect()
time_few = time.time() - start
print(f"5 shuffle partitions: {time_few:.3f}s")

# Check Spark UI: each shuffle partition handles 20,000 rows -> large per-task memory
```

### 2b. Too many shuffle partitions

```python
# Set very high shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", 1000)

df = spark.range(100000).repartition(10)
start = time.time()
result = df.groupBy((F.col("id") % 10).alias("bucket")).count().collect()
time_many = time.time() - start
print(f"1000 shuffle partitions: {time_many:.3f}s")

# Check Spark UI: this groupBy only has 10 distinct keys (id % 10), so with
# 1000 shuffle partitions, roughly 990 of them receive ZERO rows and complete
# almost instantly, while only ~10 partitions actually get data.
# The overhead here isn't "each task does more work" — it's the opposite:
# Spark still has to schedule, launch, and track 1000 mostly-empty tasks,
# and that bookkeeping overhead is what makes this slower than a
# right-sized partition count for such low cardinality.
```

### 2c. Appropriate shuffle partitions

```python
# Set appropriate shuffle partitions (for ~100k rows)
spark.conf.set("spark.sql.shuffle.partitions", 50)

df = spark.range(100000).repartition(10)
start = time.time()
result = df.groupBy((F.col("id") % 10).alias("bucket")).count().collect()
time_appropriate = time.time() - start
print(f"50 shuffle partitions: {time_appropriate:.3f}s")

print(f"\nComparison: 5={time_few:.3f}s, 50={time_appropriate:.3f}s, 1000={time_many:.3f}s")
print("Too few = OOM risk on large data. Too many = scheduling overhead from empty/mostly-idle tasks. Right size = balance.")
```

### 2d. Reset shuffle partitions

```python
# Reset to default
spark.conf.set("spark.sql.shuffle.partitions", 200)
print("Reset to 200 partitions")
```

---

## Step 3 — Data Skew: Observe the Spark UI Signature

**Objective:** Create intentional skew and observe the shuffle metrics that reveal it.

### 3a. Create skewed data (one key dominates)

```python
from pyspark.sql.functions import lit

# 90% of rows = "heavy" key, 10% = other keys
heavy = [("heavy", i) for i in range(90000)]
light = [(f"key_{i}", i) for i in range(10000)]
skewed_data = heavy + light

df_skew = spark.createDataFrame(skewed_data, ["key", "value"])
print(f"Partition count: {df_skew.rdd.getNumPartitions()}")

# Run groupBy
start = time.time()
result_skew = df_skew.groupBy("key").count().collect()
time_skew = time.time() - start
print(f"groupBy on skewed data: {time_skew:.3f}s")
print("Check Spark UI Stages tab:")
print("  - One task should have significantly higher Duration than others")
print("  - Shuffle Write Bytes for one task should be much larger")
print("  - Input bytes for one partition should dominate")
```

### 3b. Observe skew in Spark UI Stage details

Go to the Spark UI -> Stages -> click the groupBy stage:
- **Task Duration histogram**: one bar will be much higher than others
- **Task metrics table**: sort by Duration — one task takes 10x longer
- **Aggregated metrics**: look at Input bytes distribution

### 3c. Enable AQE skew optimization

```python
# AQE skew join is enabled by default
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Skew join enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# Run the skewed groupBy again
# AQE may split the skewed partition automatically
start = time.time()
result_skew_aqe = df_skew.groupBy("key").count().collect()
time_skew_aqe = time.time() - start
print(f"groupBy on skewed data (with AQE): {time_skew_aqe:.3f}s")
print("AQE may have split the 'heavy' partition into smaller sub-partitions")
```

---

## Step 4 — Observe Shuffle Spill Behavior

**Objective:** Trigger spill and observe the metrics.

### 4a. Set very low executor memory (to trigger spill)

```python
# This step simulates what happens when executor memory is insufficient
# On CE, we can't change memory easily, but we can observe the concept:

# Create a very large aggregation that forces sorting
data = [(i, i * 2, f"payload_{i}") for i in range(100000)]
df_spill = spark.createDataFrame(data, ["id", "value", "payload"])

# Group by a high-cardinality column with many distinct values
result = (df_spill
    .groupBy("id")  # High cardinality = lots of keys per partition
    .agg(F.collect_list("payload").alias("payloads"))
    .count()
)

print("Check Spark UI -> Stage details -> Task Metrics:")
print("  - Spilled Records: rows written to disk due to memory pressure")
print("  - Spilled Size: bytes written to disk")
print("Spill indicates memory pressure — consider increasing partitions or reducing memory use")
```

### 4b. Observe spill metrics (if present)

In the Spark UI, go to the Stage details for the groupBy stage:
- Look for "Spilled Records" in the task metrics
- If no spill is shown, the data fit in memory
- With very large data or very constrained memory, spill would appear

**Note:** On Community Edition with small data, spill may not trigger. The important concept is: spill = data written to disk because memory was insufficient.

---

## Step 5 — Broadcast Join vs Shuffle Join

**Objective:** Observe the physical plan difference.

### 5a. Create a large and a small table

```python
# Large fact table (100,000 rows)
import random
fact_data = [(random.randint(0, 99), random.randint(1, 1000)) for _ in range(100000)]
fact = spark.createDataFrame(fact_data, ["product_id", "quantity"])

# Small dimension table (100 rows)
dim_data = [(i, f"Product_{i}", f"Category_{i % 10}") for i in range(100)]
dim = spark.createDataFrame(dim_data, ["product_id", "name", "category"])

print(f"Fact size: {fact.count()} rows, {fact.rdd.getNumPartitions()} partitions")
print(f"Dim size: {dim.count()} rows")
```

### 5b. Join WITHOUT broadcast hint — observe shuffle plan

```python
print("\n=== Join WITHOUT broadcast hint ===")
result_no_hint = fact.join(dim, "product_id")
result_no_hint.explain("formatted")
print("Observe: SortMergeJoin + Exchange (shuffle on both sides)")
```

### 5c. Join WITH broadcast hint — observe broadcast plan

```python
print("\n=== Join WITH broadcast hint ===")
from pyspark.sql.functions import broadcast

result_broadcast = fact.join(broadcast(dim), "product_id")
result_broadcast.explain("formatted")
print("Observe: BroadcastHashJoin + BroadcastExchange (no shuffle on fact side)")
```

### 5d. Compare execution times

```python
import time

start = time.time()
result_no_hint.count()
time_no_hint = time.time() - start

start = time.time()
result_broadcast.count()
time_broadcast = time.time() - start

print(f"\nSortMergeJoin: {time_no_hint:.3f}s")
print(f"BroadcastHashJoin: {time_broadcast:.3f}s")
print(f"Broadcast speedup: {time_no_hint / time_broadcast:.1f}x faster")
```

---

## Step 6 — Break It: Unnecessary Shuffle Patterns

**Objective:** Create scenarios that cause wasted shuffle operations.

### 6a. Repartition then coalesce — unnecessary intermediate shuffle

```python
# BAD: repartition to 500, then immediately coalesce to 10
spark.conf.set("spark.sql.shuffle.partitions", 500)

df = spark.range(10000).repartition(500)
df_coalesce = df.coalesce(10)

print("Plan for repartition(500).coalesce(10):")
df_coalesce.explain("formatted")
print("Observe: Exchange (shuffle to 500) followed by Coalesce (local reduction to 10)")
print("The 500-partition shuffle was wasted")

# BETTER: coalesce directly from default parallelism
df_direct = spark.range(10000).coalesce(10)
print("\nPlan for coalesce(10) directly:")
df_direct.explain("formatted")
print("Observe: No Exchange — just local partition reduction")
```

### 6b. filter after groupBy (unnecessary late filter)

```python
# BAD: groupBy first, then filter (filter runs AFTER the expensive shuffle)
df_bad = (spark.range(100000)
    .repartition(50)
    .groupBy((F.col("id") % 10).alias("bucket"))
    .agg(F.sum("id").alias("total"))
    .filter(F.col("bucket") == 5)  # Filter after aggregation — compare int to int
)
df_bad.explain("formatted")

# BETTER: filter before groupBy (filter runs BEFORE the expensive shuffle)
df_good = (spark.range(100000)
    .repartition(50)
    .filter(F.col("id") % 10 == 5)  # Filter first
    .groupBy((F.col("id") % 10).alias("bucket"))
    .agg(F.sum("id").alias("total"))
)
df_good.explain("formatted")

print("Compare: In df_good, the Filter appears BEFORE the Exchange")
print("In df_bad, the Filter appears AFTER the Exchange (less efficient)")
```

---

## Stretch Task: Diagnose a Shuffle Bottleneck

Write a notebook that:
1. Creates a 1 million row dataset with intentional skew (one key = 50% of rows)
2. Runs a groupBy aggregation
3. Opens the Spark UI and identifies the skew from task duration variance
4. Tries three fixes and compares: (a) increasing shuffle partitions, (b) AQE skew join, (c) manual salting
5. Reports which fix was most effective

```python
# Skeleton for stretch task
import time

# 1. Create skewed data
heavy = [("HEAVY", i) for i in range(500000)]
light = [(f"key_{i}", i) for i in range(500000)]
df = spark.createDataFrame(heavy + light, ["key", "value"])
df = df.repartition(20)

# 2. Baseline with default partitions
spark.conf.set("spark.sql.shuffle.partitions", 200)
start = time.time()
df.groupBy("key").count().collect()
print(f"Baseline: {time.time() - start:.3f}s")

# 3. Fix 1: more partitions
spark.conf.set("spark.sql.shuffle.partitions", 400)
start = time.time()
df.groupBy("key").count().collect()
print(f"More partitions: {time.time() - start:.3f}s")

# 4. Fix 2: AQE skew join (already enabled)
# Just verify and run
print(f"AQE: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# 5. Fix 3: manual salt (from Day 4's salted-groupBy pattern)
```

---

## Lab Checklist

- [ ] Observed Shuffle Write and Shuffle Read metrics in Spark UI
- [ ] Compared groupBy (with shuffle) vs filter (no shuffle) execution times
- [ ] Tested too few vs too many vs appropriate shuffle partitions, and understood *why* too-many hurts (empty-task overhead, not larger per-task work)
- [ ] Created intentional skew and observed one slow task in Spark UI
- [ ] Verified AQE skew join optimization reduces skew impact
- [ ] (If memory constrained) Observed spill metrics
- [ ] Compared SortMergeJoin (no hint) vs BroadcastHashJoin (with hint)
- [ ] Compared execution times for shuffle vs broadcast join
- [ ] Identified unnecessary shuffle pattern (repartition + coalesce)
- [ ] Identified filter-after-groupBy vs filter-before-groupBy plan difference
- [ ] (Stretch) Compared three skew fixes: partitions, AQE, salting

---

## Cross-References

- **Day 4:** Stage boundaries are created by shuffle operations
- **Day 5:** explain("formatted") shows Exchange operators at stage boundaries
- **Day 7:** Memory used during shuffle sort and aggregation
- **Day 8:** Spark UI metrics for identifying shuffle bottlenecks
- **Day 3:** Broadcast join optimization
