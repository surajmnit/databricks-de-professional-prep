# Day 7 — Hands-On Lab: Spark Memory, GC, and OOM

## Lab Objectives

1. Observe executor memory regions and cache behavior in Spark UI
2. Compare storage levels and their memory impact
3. Create scenarios that trigger executor OOM and driver OOM
4. Observe GC pressure metrics
5. Diagnose memory issues using Spark UI metrics
6. Break it: cause memory pressure deliberately

**Note:** All steps run in a Databricks notebook. CE is sufficient but some OOM scenarios require scaled-up data.

---

## Step 1 — Inspect Executor Memory Regions

**Objective:** Understand the memory landscape on your cluster.

### 1a. Check executor memory configuration

```python
# Read executor memory config
executor_mem = spark.sparkContext._conf.get("spark.executor.memory")
spark_mem_fraction = spark.sparkContext._conf.get("spark.memory.fraction")
spark_storage_fraction = spark.sparkContext._conf.get("spark.memory.storageFraction")

print(f"Executor memory: {executor_mem}")
print(f"Spark memory fraction: {spark_mem_fraction}")
print(f"Storage memory fraction: {spark_storage_fraction}")

# Calculate actual regions
# spark.memory.fraction of executor.memory = total Spark memory
# spark.memory.storageFraction of Spark memory = storage portion
import re
mem_bytes = int(re.sub(r'[^\d]', '', executor_mem)) * 1024 * 1024 * 1024 if 'g' in executor_mem else int(re.sub(r'[^\d]', '', executor_mem)) * 1024 * 1024
spark_total = int(float(spark_mem_fraction) * mem_bytes)
storage_mem = int(float(spark_storage_fraction) * spark_total)
exec_mem = int(float(spark_mem_fraction) * (1 - float(spark_storage_fraction)) * spark_total)

print(f"\nApproximate memory regions for {executor_mem} executor:")
print(f"  Execution Memory: ~{exec_mem // (1024**3)} GB")
print(f"  Storage Memory: ~{storage_mem // (1024**3)} GB")
print(f"  User Memory: ~{(mem_bytes - spark_total) // (1024**3)} GB")
```

### 1b. Check Spark UI Storage tab

After running some operations, go to **Spark UI -> Storage tab**:
- Lists all cached DataFrames
- Shows size in memory vs on disk
- Shows caching level (MEMORY_ONLY, MEMORY_AND_DISK, etc.)

---

## Step 2 — Cache Behavior and Storage Memory

**Objective:** Observe how caching affects storage memory and what happens when cache grows.

### 2a. Cache a DataFrame and observe

```python
from pyspark.sql import functions as F

# Create test data
data = [(i, f"value_{i}", i * 1.5) for i in range(100000)]
df = spark.createDataFrame(data, ["id", "name", "value"])
df = df.repartition(20)

# Cache it
df_cached = df.cache()
count = df_cached.count()  # Force caching by triggering action

print("Data cached. Check Spark UI -> Storage tab:")
print("  - Should see one cached DataFrame")
print("  - Size in memory should be visible")
print(f"  - Partition count: {df_cached.rdd.getNumPartitions()}")
```

### 2b. Cache multiple DataFrames and observe memory pressure

```python
# Cache multiple DataFrames
df1 = spark.range(500000).withColumn("payload", F.lit("x") * 100).repartition(20).cache()
df2 = spark.range(500000).withColumn("payload", F.lit("y") * 100).repartition(20).cache()
df3 = spark.range(500000).withColumn("payload", F.lit("z") * 100).repartition(20).cache()

df1.count()
df2.count()
df3.count()

print("Three large DataFrames cached.")
print("Check Spark UI -> Storage tab:")
print("  - Total cached size vs available storage memory")
print("  - Some data may have been evicted (MEMORY_AND_DISK behavior)")
```

### 2c. Clear cache

```python
# Clear all cache
spark.catalog.clearCache()
print("Cache cleared. Check Storage tab — should be empty.")
```

---

## Step 3 — Compare Storage Levels

**Objective:** Observe the memory difference between MEMORY_ONLY and MEMORY_AND_DISK.

### 3a. MEMORY_AND_DISK (default)

```python
from pyspark import StorageLevel

df_and_disk = spark.range(100000).withColumn("payload", F.lit("data") * 50).repartition(10)
df_and_disk.persist(StorageLevel.MEMORY_AND_DISK)
count_and_disk = df_and_disk.count()

print("MEMORY_AND_DISK cached.")
print("Check Spark UI -> Storage: Caching Level column should show MEMORY_AND_DISK")
```

### 3b. MEMORY_ONLY

```python
df_mem_only = spark.range(100000).withColumn("payload", F.lit("data") * 50).repartition(10)
df_mem_only.persist(StorageLevel.MEMORY_ONLY)
count_mem_only = df_mem_only.count()

print("MEMORY_ONLY cached.")
print("Check Spark UI -> Storage: Caching Level should show MEMORY_ONLY")
print("If data exceeds available memory, some partitions are NOT cached (recomputed on access)")
```

### 3c. MEMORY_ONLY_SER (serialized)

```python
df_ser = spark.range(100000).withColumn("payload", F.lit("data") * 50).repartition(10)
df_ser.persist(StorageLevel.MEMORY_ONLY_SER)
count_ser = df_ser.count()

print("MEMORY_ONLY_SER cached (serialized).")
print("Compare sizes in Spark UI Storage tab:")
print("  - SER version uses less memory (serialized bytes, not Java objects)")
print("  - But takes more CPU to deserialize on read")
```

---

## Step 4 — Driver OOM Scenario (Break It)

**Objective:** Understand what happens when the driver runs out of memory collecting results.

### 4a. Collect a large result at the driver

```python
# Create a dataset that, when collected, produces a large result at the driver
# On CE this may not actually OOM but demonstrates the pattern

# Generate many rows with large string payloads
data = [(f"key_{i % 100}", f"long_payload_{'x'*500}") for i in range(50000)]
df = spark.createDataFrame(data, ["key", "payload"])

# This collects ALL data to driver JVM
# On a real cluster with GB-scale data, this would cause driver OOM
print("About to collect()...")
print("On large data, this would OOM the driver.")

# Safe alternative: write to storage
df.groupBy("key").agg(F.collect_list("payload")).write.mode("overwrite").format("noop").execute()
print("Written to storage instead — driver never holds the full result")
```

### 4b. Check driver maxResultSize

```python
max_result = spark.sparkContext._conf.get("spark.driver.maxResultSize")
print(f"spark.driver.maxResultSize: {max_result}")
print("Increase this to allow larger collect() results, or avoid collect() on large data")
```

---

## Step 5 — Executor OOM via Partition Size (Break It)

**Objective:** Understand what happens when partitions are too large for executor memory.

### 5a. Too few partitions causes large per-task memory

```python
# Create a DataFrame with very few partitions
df_few = spark.range(1000000).repartition(2)
print(f"Partitions: {df_few.rdd.getNumPartitions()}")

# With large data (GB scale), this would OOM executors
# On CE with small data, it doesn't OOM but demonstrates the pattern

# Run groupBy — each partition has 500k rows
result = df_few.groupBy((F.col("id") % 10).alias("bucket")).count().collect()
print(f"Result: {result}")
print("On GB-scale data: 2 partitions × large data = OOM on each executor")
```

### 5b. Correct approach: more partitions

```python
# Correct: more partitions = smaller per-partition memory
df_many = spark.range(1000000).repartition(100)
print(f"Partitions: {df_many.rdd.getNumPartitions()}")

# Each partition handles 10k rows — fits easily in executor memory
result2 = df_many.groupBy((F.col("id") % 10).alias("bucket")).count().collect()
print(f"Result: {result2}")
```

---

## Step 6 — Python UDF Memory Pressure

**Objective:** Understand Python worker process memory (separate from JVM heap).

### 6a. Run a Python UDF and observe behavior

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Create data
data = [(i,) for i in range(100000)]
df = spark.createDataFrame(data, ["id"])

# Python UDF — runs in Python subprocess, not JVM
@udf(IntegerType())
def heavy_compute(x):
    # Simulate heavy computation with many Python objects
    result = []
    for j in range(100):
        result.append(x * j)
    return sum(result)

result = df.withColumn("computed", heavy_compute("id")).agg(F.sum("computed")).collect()
print(f"Python UDF result: {result}")

print("Observe: Python worker process memory (spark.python.worker.memory) is used.")
print("If data were larger, Python worker would OOM while JVM heap looks fine.")
```

### 6b. Compare with Pandas UDF (Arrow-based, JVM)

```python
import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf(IntegerType())
def pandas_heavy_compute(s: pd.Series) -> pd.Series:
    # Vectorized: much faster, uses JVM Arrow, not Python subprocess
    return s * 100  # Simplified computation

result_pandas = df.withColumn("computed", pandas_heavy_compute("id")).agg(F.sum("computed")).collect()
print(f"Pandas UDF result: {result_pandas}")
print("Pandas UDF uses JVM memory, not Python worker memory.")
```

---

## Step 7 — GC Pressure Observation

**Objective:** Observe GC metrics in Spark UI.

### 7a. Create a scenario that generates many short-lived objects

```python
# Many string operations generate many temporary objects -> GC pressure
df = spark.range(50000).repartition(20)

# Multiple narrow transformations = many intermediate objects
df_result = (df
    .withColumn("a", F.concat(F.lit("prefix_"), F.col("id").cast("string")))
    .withColumn("b", F.upper(F.col("a")))
    .withColumn("c", F.trim(F.col("b")))
    .withColumn("d", F.regexp_replace(F.col("c"), "prefix_", "cleaned_"))
    .withColumn("e", F.length(F.col("d")))
)

result = df_result.agg(F.sum("e")).collect()
print(f"Result: {result}")
```

### 7b. Check GC metrics in Spark UI

After running the cell above, go to **Spark UI -> Executors tab**:
- Look at the **GC Time** column
- High GC time (> 10% of executor time) indicates GC pressure
- GC Count shows number of GC events

**What to look for:**
- Executors with high GC time: objects being created and collected frequently
- The string-heavy transformations above generate many temporary String objects → more GC

---

## Step 8 — Diagnose a Memory Issue

**Objective:** Use the Spark UI to diagnose a memory problem.

### Scenario: A pipeline that worked on small data now fails on large data

```python
# Pipeline that worked on small data
df = spark.range(10000).repartition(5)
df_cached = df.cache()
df_cached.count()  # Cache it

df2 = spark.range(10000).repartition(5)
df2_cached = df2.cache()
df2_cached.count()  # Cache second DataFrame

# Third large cache
df3 = spark.range(500000).repartition(10)
df3_cached = df3.cache()
df3_cached.count()  # Cache third

# Aggregation — if all three cached DataFrames fill storage memory,
# the aggregation may spill or OOM
result = df_cached.join(df3_cached, "id").groupBy("id").count().collect()
```

Check Spark UI:
1. **Storage tab:** Shows all three cached DataFrames. Total size may exceed available storage memory.
2. **Executors tab:** GC Time may be high from managing cached data.
3. **Stage metrics:** Spilled Records may appear if storage was overfilled.

---

## Lab Checklist

- [ ] Inspected executor memory configuration and calculated memory regions
- [ ] Checked Spark UI Storage tab for cached DataFrames
- [ ] Cached multiple DataFrames and observed storage memory behavior
- [ ] Compared MEMORY_AND_DISK vs MEMORY_ONLY vs MEMORY_ONLY_SER caching levels
- [ ] Discussed driver OOM from collect() of large results
- [ ] Compared Python UDF vs Pandas UDF memory behavior
- [ ] Observed GC metrics in Spark UI Executors tab
- [ ] Identified GC pressure from string-heavy transformations
- [ ] Demonstrated partition count → memory per task relationship
- [ ] Diagnosed a multi-cache memory scenario

---

## Cross-References

- **Day 4:** Driver and Executor are the two separate memory contexts
- **Day 6:** Shuffle spill when execution memory is insufficient
- **Day 8:** Spark UI for memory, GC, and cache metrics
- **Day 3:** Python UDF vs Pandas UDF
- **Day 1:** Driver OOM scenarios
