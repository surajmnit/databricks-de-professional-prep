# Day 1 — Hands-On Lab: Exploring Spark Architecture on Databricks

## Lab Objectives

1. Inspect the Spark application hierarchy (Jobs, Stages, Tasks) in the Spark UI
2. Observe how DataFrame operations create stages via shuffle boundaries
3. Compare `repartition` vs `coalesce` behavior
4. Observe partition-to-task mapping
5. Identify the difference between narrow and wide transformations using the DAG

**Note:** This lab assumes you have a running Databricks cluster (Community Edition or paid workspace). All code is run in a notebook cell.

---

## Prerequisites

- A running Databricks cluster (Community Edition is sufficient for most steps)
- Attach a notebook and run each cell in sequence

---

## Step 1 — Confirm Spark Context and Cluster Configuration

**Objective:** Understand the cluster setup before running any jobs.

Run this cell and note the output:

```python
# Step 1: Inspect Spark configuration
print(f"Spark Version: {spark.version}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print(f"App Name: {spark.sparkContext.appName}")
print(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")
print(f"Master: {spark.sparkContext.master}")

# Check executor memory
executor_info = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
print(f"\nExecutor Count: {len(executor_info)}")
```

**Expected output:** You should see your Spark version, application ID, and executor count (1 on Community Edition single-node).

**What to note:** Default parallelism determines the number of tasks for operations like `groupBy`. On CE, this is typically equal to the number of cores.

---

## Step 2 — Create a Test Dataset and Observe the Job/Stage Hierarchy

**Objective:** See how actions create Jobs, and how groupBy creates shuffle boundaries (new stages).

```python
# Step 2a: Create a simple dataset
from pyspark.sql import functions as F

# Create test data: 1000 rows across 10 key groups
data = [(f"key_{i % 10}", i) for i in range(1000)]
df = spark.createDataFrame(data, ["grp", "value"])

print(f"Initial partitions: {df.rdd.getNumPartitions()}")
df.explain("formatted")  # Shows the physical plan with stage boundaries
```

**Expected output:** The `explain("formatted")` output shows the number of partitions. For a simple `createDataFrame`, expect 1 partition (or number of cores on CE).

**Key concept:** Notice that `createDataFrame` produces a single partition. No shuffle has occurred yet. Now run a wide transformation:

```python
# Step 2b: Trigger a shuffle via groupBy — creates a new stage
aggregated = df.groupBy("grp").agg(F.sum("value").alias("total"))
result = aggregated.collect()  # ACTION — triggers the job

print(f"Result count: {len(result)}")
print(f"Sum values: {result[:3]}")
```

**Expected output:** This action creates a Job with 2 stages:
- Stage 1: Read + shuffle write
- Stage 2: shuffle read + aggregate + result

**What to observe in Spark UI:**
1. Go to the Spark UI (accessible at port 4040 on the driver, or via Databricks Jobs UI)
2. Look at the Jobs tab — you should see 1 completed job
3. Click into it — you should see 2 stages
4. Click into Stage 1 — note the number of tasks (should equal number of partitions)
5. Note that Stage 1 is labeled "Exchange" — this is a shuffle map stage

---

## Step 3 — Inspect the DAG

**Objective:** Understand lazy evaluation and the DAG structure.

```python
# Step 3a: Lazy evaluation demonstration
# Note: This cell creates a logical plan but executes NOTHING yet
plan = (
    spark.range(10000)
    .withColumn("category", F.expr("id % 5"))
    .withColumn("squared", F.col("id") * F.col("id"))
    .filter(F.col("category") == 2)
    .groupBy("category")
    .count()
)

# Step 3b: Trigger execution
print("About to execute...")
result = plan.collect()
print(f"Done. Result: {result}")

# Step 3c: Inspect the logical plan
print("\n--- Logical Plan ---")
plan.explain("logical")
```

**What to observe:** The logical plan shows the sequence of transformations without executing them. The physical plan (which you see after the action) shows how Spark actually executes.

```python
# Step 3d: Compare physical plans for narrow vs wide transformations
# Narrow transformation only — no new stage
narrow_plan = spark.range(1000).filter(F.col("id") < 500)
narrow_plan.explain("formatted")
print(f"\nNarrow transformation partitions: {narrow_plan.rdd.getNumPartitions()}")

# Wide transformation — creates a new stage
wide_plan = spark.range(1000).groupBy(F.col("id") % 100).count()
wide_plan.explain("formatted")
print(f"\nWide transformation partitions: {wide_plan.rdd.getNumPartitions()}")
```

**Expected output:** The formatted explain shows Stage boundaries. Narrow transformations (filter, withColumn) stay within a single stage. groupBy triggers a new stage boundary.

---

## Step 4 — Repartition vs Coalesce

**Objective:** Observe the behavioral difference between `repartition` and `coalesce`.

```python
# Step 4a: Start with 20 partitions
df = spark.range(1000).repartition(20)
print(f"Repartitioned to: {df.rdd.getNumPartitions()} partitions")

# Step 4b: Repartition to a HIGHER number — triggers shuffle
df_up = df.repartition(50)
print(f"After repartition(50): {df_up.rdd.getNumPartitions()} partitions")

# Step 4c: Coalesce to a LOWER number — NO shuffle
df_down = df.coalesce(5)
print(f"After coalesce(5): {df_down.rdd.getNumPartitions()} partitions")

# Step 4d: COALESCE to a HIGHER number — does NOT increase partitions
df_try_up = df.coalesce(100)
print(f"After coalesce(100): {df_try_up.rdd.getNumPartitions()} partitions (unchanged!)")
```

**Expected output:**
- `repartition(20)` → 20 partitions
- `repartition(50)` → 50 partitions (shuffle happened)
- `coalesce(5)` → 5 partitions (no shuffle)
- `coalesce(100)` → 20 partitions (unchanged — coalesce cannot increase)

**Exam trap to verify:** The last line is the key insight — `coalesce(n)` where n > current partitions returns the original partition count. This is a commonly tested behavior.

---

## Step 5 — Break It: Trigger an Executor OOM

**Objective:** Understand what happens when partitions are too large for executor memory.

This step requires caution — use a small dataset and intentionally misconfigure for learning purposes.

```python
# Step 5a: Normal execution — 1000 partitions (each gets tiny data)
df = spark.range(10000).repartition(1000)
print(f"Partitions: {df.rdd.getNumPartitions()}")
result = df.agg(F.sum("id")).collect()
print(f"Result: {result}")

# Step 5b: Same data, FEWER partitions — each partition is larger
df_large = spark.range(10000).repartition(2)
print(f"Large partitions: {df_large.rdd.getNumPartitions()}")

# Observe task size in Spark UI:
# - 1000 partitions: each task processes ~10 rows — fast, many tasks
# - 2 partitions: each task processes ~5000 rows — slower, but fewer tasks

# This is a trade-off, not a failure. The OOM would occur with MUCH larger data.
# For a true OOM demonstration, you would need GB-scale data:
# df_huge = spark.read.parquet("path/to/large/data").repartition(1)
# This single partition would need to hold the entire dataset — OOM on executors
```

**What to observe:** In the Spark UI Stages tab, look at the "Duration" column for each task. With 1000 partitions, all tasks should complete quickly. With 2 partitions, each takes longer. The difference illustrates the parallelism vs. task-size trade-off.

**Important:** On a large dataset with 1 partition, an executor OOM would occur. This is why partition count matters.

---

## Step 6 — Observe the Spark UI Details

**Objective:** Get comfortable navigating the Spark UI for the exam.

1. **Jobs tab:**
   - Each action creates a Job
   - Note the Job ID, status (succeeded/failed), and duration
   - Click into a Job to see its Stages

2. **Stages tab:**
   - See all stages for all Jobs
   - Note the number of tasks per stage
   - Note the "Input" and "Output" metrics
   - For shuffle stages, you see "Shuffle Read" and "Shuffle Write" bytes

3. **Storage tab:**
   - If you cached any DataFrames, they appear here
   - See the cached size vs. the actual size
   - Note the "Caching Level" (MEMORY_ONLY, DISK_ONLY, etc.)

4. **Environment tab:**
   - Shows all Spark properties (useful for debugging configuration issues)
   - Shows Java version, Python version, executor info

5. **Executors tab:**
   - See all executors, their memory usage, task counts
   - Spot executor imbalance (one executor processing more data than others)

---

## Step 7 — Check Memory via Spark UI

**Objective:** Visualize executor memory usage.

```python
# Generate enough data to see memory pressure on Community Edition
# This is a toy example — real memory pressure requires GB-scale data

import numpy as np

# Create a larger DataFrame to observe memory behavior
data = [(f"group_{i % 50}", i, f"payload_{i}") for i in range(50000)]
df_large = spark.createDataFrame(data, ["grp", "id", "payload"])

# Cache it
df_large.cache()
df_large.count()  # Force caching by triggering an action

print("DataFrame cached. Check the Storage tab in Spark UI.")
print(f"Partition count: {df_large.rdd.getNumPartitions()}")
```

**What to observe in Storage tab:** The cached size should be visible. On Community Edition with limited resources, you might see `MEMORY_AND_DISK` caching level.

---

## Stretch Task

**Task:** Write a notebook that:

1. Creates a dataset with intentional data skew (one key has 90% of rows)
2. Runs a `groupBy` aggregation
3. Opens the Spark UI and identifies the skew (one task takes much longer than others)
4. Fixes the skew using `repartitionByColumn` or `salting`
5. Verifies the fix in the Spark UI

**Code skeleton:**

```python
# Skewed data: key "heavy" gets 90% of rows
from pyspark.sql import functions as F

skewed_data = (
    [("heavy", i) for i in range(90000)] +
    [(f"light_{i}", i) for i in range(10000)]
)
df_skewed = spark.createDataFrame(skewed_data, ["key", "value"])

print("Before fix:")
df_skewed.groupBy("key").count().explain("formatted")

# Fix: Salt the heavy key
from pyspark.sql.functions import rand

df_salted = df_skewed.withColumn("salt", (rand() * 10).cast("int"))
df_salted.groupBy("key", "salt").count().groupBy("key").sum("count").show()
```

**What to verify:** In the Spark UI, the duration column for the salted query should show more even task durations compared to the unsalted version.

---

## Lab Cleanup

```python
# Unpersist any cached data
spark.catalog.clearCache()

# Check for any running jobs (should be none)
print("Lab complete. Cache cleared.")
```

---

## Lab Checklist

- [ ] Confirmed Spark version and cluster configuration
- [ ] Observed Job/Stage hierarchy in Spark UI
- [ ] Distinguished narrow vs. wide transformations via explain
- [ ] Verified coalesce cannot increase partition count
- [ ] Navigated Spark UI (Jobs, Stages, Storage, Environment, Executors)
- [ ] Observed memory behavior via Storage tab
- [ ] (Stretch) Completed the skew/salting exercise

---

## Cross-References

- **Day 5 (Spark Execution):** Deep-dives into DAG, lazy evaluation, and the physical plan.
- **Day 6 (Shuffle):** What exactly happens during the shuffle between stages.
- **Day 7 (Spark Memory):** GC behavior and memory pressure scenarios.
- **Day 8 (Spark UI + Query Profile):** Full Spark UI navigation and bottleneck identification.
