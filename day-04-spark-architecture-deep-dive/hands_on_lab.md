# Day 4 — Hands-On Lab: Spark Architecture Deep-Dive

## Lab Objectives

1. Observe the Job / Stage / Task hierarchy in the Spark UI
2. Distinguish driver OOM from executor OOM
3. Compare partition-to-task mapping for different operations
4. Trigger data skew and observe the Spark UI signature
5. Inspect Spark configuration and executor registration

**Note:** All steps run in a Databricks notebook. Community Edition is sufficient.

---

## Step 1 — Observe the Job/Stage/Task Hierarchy

**Objective:** Confirm that actions create Jobs, shuffle boundaries create Stages, and partitions create Tasks.

### 1a. Create test data with known partition count

```python
from pyspark.sql import functions as F

# Create 20 partitions
df = spark.range(1000).repartition(20)
print(f"Partitions after repartition(20): {df.rdd.getNumPartitions()}")

# Add a filter (narrow transformation — same stage)
df_filtered = df.filter(F.col("id") % 2 == 0)

# Add a groupBy (wide transformation — new stage boundary)
df_grouped = df_filtered.groupBy((F.col("id") % 10).alias("bucket")).count()

# Trigger execution with an action
result = df_grouped.collect()
print(f"Result: {result[:3]}")

print("Open the Spark UI -> Jobs tab. You should see 1 Job with 2 Stages.")
print("Open Stage 0: should show 20 tasks (one per partition)")
print("Open Stage 1: should show tasks based on shuffle partitions")
```

### 1b. Inspect the DAG via explain

```python
# Explain the physical plan
df_grouped.explain("formatted")
```

**What to observe:**
- Stage 0: Exchange (shuffle write) — starts when groupBy is encountered
- Stage 1: Result stage — receives shuffle output, runs count, returns to driver

### 1c. Multiple actions = multiple independent Jobs

```python
# Action 1
job1_count = df_filtered.count()

# Action 2
job2_collect = df_grouped.collect()

# Action 3
job3_foreach = df_grouped.foreach(lambda x: None)

print("Check Spark UI Jobs tab: should show 3 completed Jobs")
print("Each action is a separate Job, not nested")
```

---

## Step 2 — Driver OOM Scenario (Break It on Purpose)

**Objective:** Understand what happens when the driver runs out of memory collecting a large result.

### 2a. Create a large-ish dataset (not actually large, but demonstrate the pattern)

```python
# Create data that produces a large result set
# On a real cluster with GB of data, this would OOM the driver

# Simulate: large number of small groups that produce large string results
data = [(f"key_{i % 100}", f"long_payload_{'x'*200}") for i in range(5000)]
df = spark.createDataFrame(data, ["key", "payload"])

# Normal count (small result at driver)
count = df.groupBy("key").count().collect()
print(f"Count result size: {len(count)} items at driver — fine")

# This would OOM the driver if data were GB-scale:
# result = df.groupBy("key").agg(F.collect_list("payload")).collect()
# The collect_list creates an array per group; driver must hold all arrays

# Instead, write to storage
df.groupBy("key").agg(F.collect_list("payload")).write.mode("overwrite").format("noop").execute()
print("Written to storage instead of collecting — driver never holds the data")
```

### 2b. Observe driver memory behavior

```python
# Check driver memory configuration
print(f"Driver memory: {spark.sparkContext._conf.get('spark.driver.memory')}")
print(f"Max result size: {spark.sparkContext._conf.get('spark.driver.maxResultSize')}")

# Inspect Spark UI Environment tab for full driver config
print("Check Spark UI -> Environment tab for all Spark properties")
```

---

## Step 3 — Executor OOM Scenario (Break It on Purpose)

**Objective:** Understand what happens when executor memory is exhausted.

### 3a. Create a scenario with too few partitions (executor-level memory pressure)

```python
# Create a DataFrame with very few partitions
df = spark.range(100000).coalesce(2)  # Only 2 partitions — each processes 50k rows
print(f"Partitions: {df.rdd.getNumPartitions()}")

# This is a toy example — real OOM requires GB-scale data
# The principle: fewer partitions = larger per-partition memory requirement

# To observe the effect, look at the Spark UI after this runs:
# - Stage shows 2 tasks (one per partition)
# - Each task processes 50k rows
# - On a large cluster with GB data, this would OOM

# Instead, demonstrate with a query that would OOM at scale:
# df_large = spark.read.parquet("s3://production-data/").coalesce(1)
# result = df_large.groupBy("id").count().collect()
# On GB-scale data, coalesce(1) forces one partition to hold all data -> executor OOM
```

### 3b. Observe cache bloat OOM pattern

```python
# Cache data
df.cache()
df.count()  # Force caching

print("Data cached. Check Spark UI -> Storage tab.")
print("Note the cached size vs actual size.")
print("On limited memory clusters, caching too much causes subsequent operations to OOM.")

# Clear cache
spark.catalog.clearCache()
print("Cache cleared")
```

---

## Step 4 — Data Skew: One Slow Task

**Objective:** Create intentional skew and observe the Spark UI signature.

### 4a. Create skewed data (one key dominates)

```python
from pyspark.sql.functions import lit

# 90% of rows have key "heavy", 10% have other keys
heavy_data = [("heavy", i) for i in range(90000)]
light_data = [(f"light_{i}", i) for i in range(10000)]
skewed_data = heavy_data + light_data

df_skewed = spark.createDataFrame(skewed_data, ["key", "value"])
print(f"Partition count: {df_skewed.rdd.getNumPartitions()}")

# Run groupBy — one task will dominate
print("Running groupBy — check Spark UI Stages tab for task duration imbalance")
result = df_skewed.groupBy("key").count().collect()
print(f"Result: {len(result)} unique keys")
```

### 4c. Observe the Spark UI for skew

After running the cell above, go to the Spark UI:
1. **Stages tab** — Click the groupBy stage. Look at the "Duration" column.
   - One task will take much longer than others (skewed partition)
   - Other tasks complete quickly (small partitions)
2. **Aggregated metrics** — Check if "Input" size is imbalanced across tasks
3. **Task distribution** — Note which executor handled the heavy partition

### 4c. Fix with AQE (automatic)

```python
# AQE handles skew automatically when enabled (default true)
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"Skew join enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

# Run again — AQE may automatically split the skewed partition
result_aqe = df_skewed.groupBy("key").count().collect()
print("With AQE: skewed partition may be split automatically")
```

### 4d. Fix with manual salting

```python
from pyspark.sql.functions import expr, rand

# Salt the skewed table
n_salt = 10
df_salted = df_skewed.withColumn("salt", (rand() * n_salt).cast("int"))
df_salted = df_salted.withColumn("key_salted", expr("concat(key, '-', salt)"))

# Group by salted key
result_salted = df_salted.groupBy("key_salted").count().collect()
print(f"Salted result: {len(result_salted)} rows (one per salt)")

# Aggregate back to original key
from pyspark.sql.functions import sum as spark_sum
final = df_salted.groupBy("key").agg(spark_sum("count").alias("total_count")).collect()
print(f"Final aggregated: {final[:3]}")
```

---

## Step 5 — Inspect Executor Registration and Configuration

**Objective:** Understand how executors register and how to check their status.

### 5a. List active executors

```python
# Get executor info from SparkContext
sc = spark.sparkContext
executors = sc._jsc.sc().statusTracker().getActiveExecutorIds()
print(f"Active executor IDs: {executors}")

# Get detailed executor info
executor_info = sc._jsc.sc().getExecutorMemoryStatus()
print(f"Number of executors: {len(executor_info)}")

for i, info in enumerate(executor_info):
    print(f"Executor {i}: {info}")
```

### 5b. Inspect cluster configuration

```python
# Read Spark configuration
print("=== Spark Configuration ===")
print(f"Default parallelism: {sc.defaultParallelism}")
print(f"Executor cores: {sc._conf.get('spark.executor.cores')}")
print(f"Executor memory: {sc._conf.get('spark.executor.memory')}")
print(f"Shuffle partitions: {sc._conf.get('spark.sql.shuffle.partitions')}")
print(f"AQE enabled: {sc._conf.get('spark.sql.adaptive.enabled')}")
print(f"Max partition bytes: {sc._conf.get('spark.sql.files.maxPartitionBytes')}")

# Spark UI Environment tab shows all of this plus library versions, Java version
print("\nCheck Spark UI -> Environment for complete system info")
```

---

## Step 6 — Partition-to-Task Mapping

**Objective:** Confirm that partition count = task count per stage.

### 6a. Track task count across stages

```python
# Start with n partitions
df = spark.range(1000).repartition(8)
print(f"Initial partitions: {df.rdd.getNumPartitions()}")

# After a narrow transformation — same partition count
df2 = df.filter(F.col("id") % 2 == 0)
print(f"After filter: {df2.rdd.getNumPartitions()} (unchanged — same stage)")

# After a shuffle — uses shuffle partitions
df3 = df2.groupBy((F.col("id") % 4).alias("bucket")).count()
print(f"After groupBy: uses shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")

# Trigger and check
result = df3.collect()
print("Check Spark UI: Stage 0 should have 8 tasks, Stage 1 should have 200 tasks (shuffle partitions)")
```

### 6c. Repartition vs coalesce behavior

```python
df = spark.range(100).repartition(20)
print(f"After repartition(20): {df.rdd.getNumPartitions()} partitions")

df_up = df.repartition(50)
print(f"After repartition(50): {df_up.rdd.getNumPartitions()} (shuffle, can increase)")

df_down = df.coalesce(5)
print(f"After coalesce(5): {df_down.rdd.getNumPartitions()} (no shuffle, can only decrease)")

df_noop = df.coalesce(100)  # n > current partitions
print(f"After coalesce(100) on 20-partition DF: {df_noop.rdd.getNumPartitions()} (no-op — coalesce cannot increase)")
```

---

## Lab Checklist

- [ ] Observed 1 Job with 2 Stages in Spark UI
- [ ] Confirmed narrow transformations stay in same Stage, groupBy creates new Stage
- [ ] Ran multiple actions confirming separate Jobs
- [ ] Inspected explain("formatted") for stage boundaries
- [ ] Examined driver memory configuration
- [ ] Demonstrated executor OOM patterns (cache, partition size)
- [ ] Created intentional data skew and observed one slow task
- [ ] Applied AQE skew join fix
- [ ] Applied manual salted join fix
- [ ] Listed active executors and inspected configuration
- [ ] Verified partition-to-task mapping across narrow and wide transformations
- [ ] Confirmed coalesce cannot increase partition count

---

## Cross-References

- **Day 5:** DAG formation, lazy evaluation, and physical plan construction
- **Day 6:** What exactly happens during the shuffle between Stages
- **Day 7:** Executor memory regions, GC, and spill behavior
- **Day 8:** Using Spark UI to identify skew, slow tasks, and memory pressure visually
