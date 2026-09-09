# Day 7 — Spark Memory, Spill, and Garbage Collection

## Exam Objectives

This day covers **Section 6: Cost & Performance Optimization (13%)** — specifically Spark's memory model, how OOM errors occur, what spill means, and how GC affects performance.

Understanding memory is essential for:
- Distinguishing driver OOM from executor OOM
- Understanding why cache bloat causes failures
- Knowing when Python UDFs cause OOM (Python process vs JVM)
- Interpreting Spark UI memory metrics
- Configuring Spark memory for production workloads

---

## Part 1 — Executor Memory Architecture

### The Unified Memory Model (Spark 2.x+)

Executor memory is divided into regions. In Spark 2.0+, execution memory and storage memory share a pool (unified memory) — they can borrow from each other:

```
Executor JVM Heap
+-------------------------------------------------------+
|                  Execution Memory                     |
|   (~50% of heap minus memory overhead)                |
|   - Shuffle sort buffer                               |
|   - Hash table for groupBy/join                       |
|   - In-memory aggregation                             |
+-------------------------------------------------------+
|                  Storage Memory                       |
|   (~50% of heap minus memory overhead)                |
|   - Cache (RDD/DataFrame)                             |
|   - Broadcast variables                               |
|   - Internal metadata                                 |
+-------------------------------------------------------+
|              User Memory (30% of heap)                |
|   - User-defined variables                            |
|   - Data structures in UDFs                          |
|   - Metadata not managed by Spark                     |
+-------------------------------------------------------+
|              Reserved Memory (10% of heap)             |
|   - System reserved                                  |
+-------------------------------------------------------+
```

### Key Memory Fractions (Spark 3.x Defaults)

| Property | Default | Meaning |
|---|---|---|
| spark.executor.memory | Config (cluster) | Total JVM heap per executor |
| spark.memory.fraction | 0.6 | Fraction of heap used for execution + storage |
| spark.memory.storageFraction | 0.5 | Fraction of memory fraction for storage (when not used by execution) |
| spark.memory.offHeap.size | false | Off-heap memory if enabled |

**Actual formula:**
- Execution + Storage = (executor memory - 300MB) × spark.memory.fraction (0.6)
- User Memory = (executor memory - 300MB) × 0.2
- Reserved = 300MB

### Execution Memory vs Storage Memory

**Execution Memory (for shuffles, sorts, joins):**
- Can evict storage memory when needed
- Cannot be evicted by storage operations
- Used for: sort buffers, hash tables, aggregation state

**Storage Memory (for caching):**
- Can evict execution memory when needed
- Used for: cached DataFrames, broadcasts
- Controlled by spark.memory.storageFraction

**Exam trap:** Execution CAN borrow from Storage. Storage CANNOT borrow from Execution (when execution needs memory, it evicts storage). This is the pre-Spark 1.6 behavior that was changed — do not confuse them.

---

## Part 2 — Driver Memory

### Driver Memory Usage

The driver JVM uses memory for:
- **SparkContext** and DAG management (small)
- **Broadcast variables** (can be large)
- **collect()** results (can be very large)
- **GroupBy outputs** (can be large)
- **Python process** for notebooks (separate from JVM)

### Driver OOM Scenarios

```python
# Scenario 1: collect() of large result
result = df.collect()  # All data pulled to driver JVM heap

# Scenario 2: Broadcasting a large table
df_large.join(broadcast(df_big), "key")  # Driver sends broadcast metadata to executors

# Scenario 3: Large groupBy output
df.groupBy("high_cardinality").agg(collect_list("text")).collect()

# Scenario 4: Too many cached DataFrames on driver
# Driver doesn't cache data (only executors do), but broadcast variables are on driver
```

### Driver Memory Configuration

```python
spark.conf.set("spark.driver.memory", "4g")        # JVM heap
spark.conf.set("spark.driver.maxResultSize", "2g") # Max collect() result
spark.conf.set("spark.driver.memoryOverhead", "1g") # Off-heap
```

**spark.driver.maxResultSize:** Limits the size of `collect()`, `take()`, etc. Default: 1GB. If result exceeds this, Spark throws SparkException. Increase if you need to collect large results (or don't collect large results — write to storage instead).

---

## Part 3 — Executor OOM — Root Causes

### OOM Pattern 1: Partition Too Large

```python
# Read 1 TB file with 2 partitions -> 500 GB per partition
df = spark.read.parquet("s3://huge-data/")  # Returns 2 partitions
df.groupBy("key").count().collect()  # Each partition 500 GB -> executor OOM
```

**Fix:** Increase partitions: `spark.sql.shuffle.partitions`, `repartition(n)` before heavy operations, or `spark.sql.files.maxPartitionBytes` for reads.

### OOM Pattern 2: Cache Bloat

```python
df1 = spark.read.parquet("data1")  # Large
df2 = spark.read.parquet("data2")  # Large

df1.cache().count()  # Uses storage memory
df2.cache().count()  # Uses storage memory

# Now: groupBy on df1 — execution needs memory but storage took it all
result = df1.groupBy("key").count().collect()  # OOM because execution memory was evicted
```

**Fix:** Unpersist cached DataFrames when done: `df1.unpersist()`, `spark.catalog.clearCache()`.

### OOM Pattern 3: Python UDF Memory

Python UDFs run in a **separate Python subprocess** on each executor:
- NOT counted in executor JVM heap
- Controlled by `spark.python.worker.memory` (default 512MB per worker)
- Each Python worker has its own heap

```python
@udf(returnType=StringType())
def heavy_python_udf(x):
    # This runs in Python subprocess, NOT in executor JVM
    result = heavy_computation(x)  # Can exhaust Python worker memory
    return result

df.withColumn("result", heavy_python_udf("col"))  # May OOM on Python subprocess
```

**Fix:** Increase `spark.python.worker.memory`, use Pandas UDFs (Arrow-based, within JVM), or reduce data per partition.

### OOM Pattern 4: Skew (One Partition Dominates)

```python
# If one key has 50% of rows
df.groupBy("key").count().collect()  # One partition 50% of data -> OOM on that executor
```

**Fix:** AQE skew join (auto), salted join (manual), increase partitions.

---

## Part 4 — Spill

### What Is Spill?

When executor memory is insufficient for the current operation (typically sort or hash aggregation), Spark writes data to disk — this is **spill**.

Spill is different from normal shuffle write:
- **Shuffle write:** Normal operation — map output written for reduce stage
- **Shuffle spill:** Overflow operation — data written because memory was insufficient

### When Spill Occurs

Spill occurs during shuffle operations that require sorting or hash aggregation:
- `sortByKey()` — sort spills to disk
- `groupBy()` — hash table spills to disk
- `join()` with sort-merge — merge sort spills

### Spill Metrics in Spark UI

In Stage details -> Task Metrics:
- **Spilled Records**: rows written to disk
- **Spilled Size**: bytes written to disk

High spill = memory pressure = performance degradation.

### Reducing Spill

| Cause | Fix |
|---|---|
| Too few partitions (large per-partition data) | Increase `spark.sql.shuffle.partitions` |
| Too much cached data | Unpersist DataFrames: `df.unpersist()` |
| Python UDF memory | Increase `spark.python.worker.memory` |
| High-cardinality groupBy | Use AQE skew join, salt join |
| Sort on wide strings | Use compact data types |

---

## Part 5 — Garbage Collection

### GC Pressure in Spark

Spark executors create many short-lived objects during data processing:
- Row objects for each record
- Aggregation accumulators
- Intermediate sort buffers

When GC runs frequently (GC pressure), executor pauses increase, slowing tasks.

### Signs of GC Pressure

- Long GC pauses in Spark UI (visible as task duration spikes)
- High GC time relative to task runtime
- "GC overhead limit exceeded" errors

### Reducing GC Pressure

```python
# Use more memory for caching and execution (reduces object churn)
spark.conf.set("spark.memory.fraction", "0.7")  # Default 0.6

# Use larger object (avoid small objects)
# Structure data efficiently

# Use broadcast joins to reduce data volume
# Use coalesce instead of repartition (fewer partition objects)

# For Python UDFs: use Pandas UDFs (Arrow, less GC pressure)
```

### GC in Python UDFs vs Pandas UDFs

| UDF Type | Process | GC Behavior |
|---|---|---|
| Python UDF | Separate Python subprocess | Python GC on Python objects |
| Pandas UDF | Within JVM (Arrow) | Java GC on Arrow-backed data |

**Pandas UDFs reduce GC pressure** because they process Arrow batches without creating per-row Python objects.

---

## Part 6 — Off-Heap Memory

Spark can use off-heap memory (outside JVM heap) for:
- Internal Spark usage (NIO direct buffers)
- Python UDF subprocess memory
- User-configured off-heap storage

```python
spark.conf.set("spark.memory.offHeap.enabled", True)
spark.conf.set("spark.memory.offHeap.size", "2g")  # Off-heap allocation
```

**On Databricks:** Off-heap is typically configured at the cluster level, not in notebook code.

---

## Part 7 — Memory and Spark UI Diagnostics

### Reading Memory Metrics in Spark UI

**Stages Tab — Aggregated Metrics:**
- **Input**: bytes read from storage
- **Output**: bytes written to storage or shuffle
- **Shuffle Read/Write**: shuffle metrics

**Executors Tab:**
- **Memory Used**: current heap usage
- **Memory Max**: configured heap size
- **On-heap Storage Memory**: used/available for caching
- **On-heap Execution Memory**: used/available for computation

### Driver vs Executor Memory in Spark UI

| Metric | Driver | Executors |
|---|---|---|
| Memory metrics in Executors tab | No (driver not an executor) | Yes |
| OOM shows in | Application status (FAILED) | Executor status (LOST) |
| "Lost task" errors | No | Yes (executor failure) |
| "Driver disconnected" | Yes | No |

---

## Part 8 — Production Memory Configuration Checklist

```python
# Key memory configurations for production workloads

# 1. Executor memory (set at cluster level, not here)
# spark.executor.memory: "64g" (example)

# 2. Shuffle partitions (adjust for data size)
spark.conf.set("spark.sql.shuffle.partitions", "400")  # For large joins/aggregations

# 3. Enable AQE (default true, confirm)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 4. Python worker memory (if using Python UDFs)
spark.conf.set("spark.python.worker.memory", "2g")  # Default 512MB

# 5. Driver max result size
spark.conf.set("spark.driver.maxResultSize", "4g")  # Default 1GB

# 6. Memory fraction (careful — tuning)
spark.conf.set("spark.memory.fraction", "0.6")  # Default — don't change unless you know what you're doing
```

---

## Cross-References

- **Day 4:** Architecture — driver vs executor roles
- **Day 6:** Shuffle — where memory is used during sort/aggregation
- **Day 8:** Spark UI — viewing memory metrics in practice
- **Day 3:** Python UDFs vs Pandas UDFs — memory difference
- **Day 6:** Skew — one partition dominating memory on one executor
