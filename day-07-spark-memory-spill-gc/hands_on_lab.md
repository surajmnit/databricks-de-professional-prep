# Day 7 — Hands-On Lab: Spark Memory, Spill, and GC

## Lab Objectives

1. Inspect executor memory configuration and current usage in Spark UI
2. Observe memory behavior during caching and unpersisting
3. Compare Python UDF vs Pandas UDF memory usage
4. Trigger and observe spill behavior (if data is large enough)
5. Observe GC pressure metrics
6. Break it: cause executor OOM via cache bloat

**Note:** CE has limited resources — OOM may not trigger on small data, but the patterns and metrics are observable even at small scale.

---

## Step 1 — Inspect Executor Memory Configuration

**Objective:** Understand the current executor memory configuration and how to read Spark UI memory metrics.

### 1a. Check memory configuration via SparkContext

```python
sc = spark.sparkContext

print("=== Executor Memory Configuration ===")
print(f"Executor memory: {sc._conf.get('spark.executor.memory')}")
print(f"Memory fraction: {sc._conf.get('spark.memory.fraction')}")
print(f"Storage fraction: {sc._conf.get('spark.memory.storageFraction')}")
print(f"Python worker memory: {sc._conf.get('spark.python.worker.memory')}")
print(f"Driver memory: {sc._conf.get('spark.driver.memory')}")
print(f"Driver max result size: {sc._conf.get('spark.driver.maxResultSize')}")

print("\n=== Check Spark UI ===")
print("1. Go to Spark UI -> Executors tab")
print("2. Look at Memory Used vs Memory Max for each executor")
print("3. Check On-heap Storage Memory and On-heap Execution Memory")
print("4. These add up to executor.memory * spark.memory.fraction")
```

### 1b. Calculate memory regions

```python
executor_memory = "16g"  # Example from your cluster config

# Approximate calculation (actual varies by DBR version)
# (16GB - 300MB reserved) * 0.6 (memory fraction) = ~9.4GB for execution + storage
# (16GB - 300MB reserved) * 0.2 (user memory fraction) = ~3.1GB for user memory

print(f"With executor.memory={executor_memory}:")
print(f"  Execution + Storage: ~9.4 GB")
print(f"  User Memory: ~3.1 GB")
print(f"  Reserved: ~300 MB")
print("\nExecution and Storage share a pool — see Spark UI for actual split")
```

### 1c. Observe Spark UI memory over time

```python
# Generate data that exercises memory
data = [(i, f"payload_{i}" * 100) for i in range(50000)]
df = spark.createDataFrame(data, ["id", "payload"])

# Run operations and observe memory changes in Spark UI Executors tab
result1 = df.groupBy((F.col("id") % 100).alias("bucket")).count().collect()
print("Operation 1 complete — check memory in Spark UI Executors tab")

result2 = df.filter(F.col("id") % 2 == 0).select("id").collect()
print("Operation 2 complete — check memory change")
```

---

## Step 2 — Cache Bloat and Unpersist

**Objective:** Observe how caching consumes executor memory and how to recover it.

### 2a. Cache data and observe memory usage

```python
# Create data
data = [(i, f"value_{i}") for i in range(100000)]
df = spark.createDataFrame(data, ["id", "value"])

# Cache the data
df_cached = df.cache()
print("Caching data...")
count = df_cached.count()  # Force caching
print(f"Cached {count} rows")

print("Check Spark UI -> Storage tab:")
print("  - Cached table should appear")
print("  - Size in memory vs size on disk")
print("  - Caching level (MEMORY_ONLY or MEMORY_AND_DISK)")
```

### 2c. Observe executor memory with cache

```python
print("Check Spark UI -> Executors tab:")
print("  - On-heap Storage Memory should show increased usage")
print("  - This memory is 'borrowed' from the execution pool")
print("  - Storage memory can grow up to the full memory fraction")

# Run another operation that needs execution memory
print("Running groupBy (needs execution memory)...")
result = df_cached.groupBy((F.col("id") % 50).alias("bucket")).count().collect()
print("Check Spark UI: did execution memory reclaim from storage?")
```

### 2d. Unpersist to free memory

```python
# Unpersist the cached data
df_cached.unpersist()
print("Unpersisted — check Spark UI -> Storage tab (should be empty)")
print("Check Spark UI -> Executors tab: Storage memory should be freed")
```

### 2e. Cache bloat scenario

```python
# Cache multiple large DataFrames
df1 = spark.range(100000).withColumn("data", F.lit("x" * 1000)).cache()
df2 = spark.range(100000).withColumn("data", F.lit("y" * 1000)).cache()
df3 = spark.range(100000).withColumn("data", F.lit("z" * 1000)).cache()

df1.count()
df2.count()
df3.count()

print("Three DataFrames cached.")
print("Check Spark UI -> Storage: all three should appear")
print("Check Spark UI -> Executors: Storage memory usage should be high")

# If storage is full and execution needs memory, cache eviction occurs
# Spark evicts least-recently-used cached partitions to make room

# Clear all
spark.catalog.clearCache()
print("Cache cleared")
```

---

## Step 3 — Python UDF vs Pandas UDF Memory

**Objective:** Understand that Python UDFs use off-heap memory separate from JVM heap.

### 3a. Check Python worker memory configuration

```python
print(f"Python worker memory: {spark.conf.get('spark.python.worker.memory')}")
print("Python UDF subprocess memory is separate from executor JVM heap")
print("Default: 512MB per Python worker")
```

### 3b. Python UDF (uses separate Python process)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python UDF — runs in separate Python subprocess
@udf(returnType=StringType())
def heavy_python_processing(x):
    # Simulate memory-intensive operation
    result = str(x) * 1000
    return result

df = spark.range(10000).withColumn("data", F.lit("x"))
result_python = df.withColumn("processed", heavy_python_processing("data")).collect()
print("Python UDF complete")
print("Note: Python subprocess memory does NOT show in executor JVM heap metrics")
print("Check Spark UI: executor memory may look normal even if Python worker is stressed")
```

### 3c. Pandas UDF (uses Arrow, within JVM)

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd
from pyspark.sql.types import StringType

# Pandas UDF — Arrow-backed, processes within JVM
@pandas_udf(returnType=StringType())
def pandas_processing(s: pd.Series) -> pd.Series:
    return s.astype(str) + "_processed"

result_pandas = df.withColumn("processed", pandas_processing("data")).collect()
print("Pandas UDF complete")
print("Pandas UDF uses Arrow and JVM memory — visible in executor heap metrics")
```

### 3d. Compare behavior

```python
print("\n=== Memory Comparison ===")
print("Python UDF:")
print("  - Runs in separate Python subprocess")
print("  - spark.python.worker.memory controls its heap")
print("  - NOT counted in executor JVM heap")
print("  - JVM heap looks fine even if Python worker OOMs")
print("")
print("Pandas UDF:")
print("  - Arrow-backed, processes in JVM")
print("  - Uses executor JVM heap")
print("  - Visible in executor memory metrics")
print("  - Better for large data (less GC pressure)")
```

---

## Step 4 — Spill Behavior

**Objective:** Observe spill when memory is constrained.

### 4a. Trigger spill with large aggregation

```python
# Create data with high cardinality (many distinct keys)
data = [(i, i % 10000, f"payload_{i}") for i in range(100000)]
df = spark.createDataFrame(data, ["id", "key", "payload"])

# Group by high cardinality key — forces large hash table
# On limited CE, this may trigger spill
print("Running high-cardinality groupBy...")
result = df.groupBy("key").agg(F.collect_list("payload").alias("payloads")).count()
print("Operation complete")

print("\nCheck Spark UI -> Stages -> groupBy stage -> Task Metrics:")
print("  - Spilled Records: rows written to disk")
print("  - Spilled Size: bytes written to disk")
print("  - If these are non-zero, spill occurred")
```

### 4b. Observe spill with sort

```python
# Sort requires memory for sort buffer
# With limited memory, sort spills to disk
print("Running large sort...")
result_sort = df.orderBy("id").take(100)
print("Sort complete")

print("\nCheck Spark UI: look for spill metrics in the sort stage")
```

### 4c. Reduce spill by increasing partitions

```python
# More partitions = smaller per-partition data = less spill risk
spark.conf.set("spark.sql.shuffle.partitions", "400")

df_large = spark.createDataFrame(data, ["id", "key", "payload"])
start = time.time()
result = df_large.groupBy("key").agg(F.collect_list("payload").alias("payloads")).count()
time_many = time.time() - start

# Reset
spark.conf.set("spark.sql.shuffle.partitions", "200")

print(f"groupBy with 400 partitions: {time_many:.3f}s")
print("More partitions = less spill = faster execution (unless overhead dominates)")
```

---

## Step 5 — Break It: Cache Bloat OOM

**Objective:** Observe what happens when cached data fills executor memory.

```python
# This pattern can cause OOM when data + cache exceed executor memory

# Step 1: Cache very large DataFrame
print("Caching large DataFrame...")
df_large = spark.range(1000000).withColumn("data", F.lit("x" * 10000))
df_large.cache().count()
print("Large DataFrame cached")

# Step 2: Try to cache another large DataFrame
print("Caching second large DataFrame...")
df_large2 = spark.range(1000000).withColumn("data", F.lit("y" * 10000))
df_large2.cache().count()
print("Second DataFrame cached")

# Step 3: Run memory-intensive operation
print("Running groupBy (needs execution memory)...")
try:
    result = spark.range(100000).groupBy(F.col("id") % 100).count().collect()
    print("Operation succeeded")
except Exception as e:
    print(f"Operation failed: {e}")
    print("This demonstrates cache bloat — storage memory consumed, execution memory starved")

# On limited CE this may not actually OOM, but on a properly-sized production cluster it would
print("\nOn production with GB-scale data, this pattern causes executor OOM")
print("Fix: unpersist() cache when done, or limit cached data size")
```

---

## Step 6 — GC Pressure Observation

**Objective:** Look for GC-related performance issues in Spark UI.

```python
# Create operations that generate many short-lived objects
# (lots of small row objects = more GC pressure)

data = [(i, i % 100, f"val_{i}") for i in range(100000)]
df = spark.createDataFrame(data, ["id", "key", "value"])

# Multiple narrow transformations create many intermediate objects
result = (df
    .filter(F.col("id") % 2 == 0)
    .withColumn("x1", F.col("id") * 1)
    .withColumn("x2", F.col("id") * 2)
    .withColumn("x3", F.col("id") * 3)
    .withColumn("x4", F.col("id") * 4)
    .withColumn("x5", F.col("id") * 5)
    .groupBy("key")
    .agg(F.sum("id").alias("total"))
    .collect()
)

print("Multiple transformations complete")
print("Check Spark UI: in Stage metrics, look for GC-related metrics if available")
print("On Databricks, executor GC metrics may appear in the cluster logs")
```

---

## Lab Checklist

- [ ] Inspected executor memory configuration via SparkContext
- [ ] Read Spark UI Executors tab for memory usage
- [ ] Cached DataFrame and observed Storage tab entry
- [ ] Unpersisted cache and confirmed memory freed
- [ ] Observed cache bloat pattern with multiple cached DataFrames
- [ ] Compared Python UDF vs Pandas UDF memory (process vs JVM)
- [ ] Triggered groupBy and observed spill metrics if present
- [ ] Reduced spill by increasing shuffle partitions
- [ ] Observed cache bloat failure pattern
- [ ] Looked for GC pressure in multiple transformations

---

## Cross-References

- **Day 4:** Driver vs executor OOM distinction
- **Day 6:** Shuffle memory used during sort/aggregation
- **Day 8:** Spark UI full diagnostics
- **Day 3:** Python UDFs vs Pandas UDFs
