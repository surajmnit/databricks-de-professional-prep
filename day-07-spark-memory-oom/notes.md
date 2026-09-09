# Day 7 — Spark Memory, Garbage Collection, and OOM

## Exam Objectives

This day covers **Section 6: Cost & Performance Optimization (13%)** — understanding where memory goes, why OOMs happen, and how to prevent them.

This is one of the highest-value days on the exam because memory-related questions appear in:
- Troubleshooting questions (driver vs executor OOM)
- Configuration questions (which property to increase)
- Performance questions (why is this slow, is it memory-related)
- Caching questions (what happens when I cache this)

---

## Part 1 — Executor Memory Regions

### The Complete Picture

Executor memory is divided into regions. The JVM heap is the largest portion:

```
Executor Memory (spark.executor.memory)
├─ Spark Memory (spark.memory.fraction = 0.6 of heap)
│   ├─ Execution Memory (~60% of Spark Memory)
│   │   └─ Shuffle sort, hash join, aggregation buffers
│   └─ Storage Memory (~40% of Spark Memory)
│       └─ Cached DataFrames, broadcast variables
│
├─ User Memory (~30% of heap)
│   └─ User code, UDF data, data structures
│
└─ System Memory (~10% of heap)
    └─ JVM internal structures, code buffers
```

### Execution Memory

Used for:
- **Shuffle sort buffers:** Sorting keys during groupBy, sort-based aggregations
- **Hash join tables:** Hash table for hash join operations
- **Aggregation buffers:** Holding intermediate aggregation results

**Behavior when full:** Execution memory cannot evict from Storage memory. When execution is full, Spark spills to disk (shuffle spill).

### Storage Memory

Used for:
- **Cached DataFrames/RDDs:** Data stored in memory (MEMORY_ONLY, MEMORY_AND_DISK)
- **Broadcast variables:** Small tables broadcast to all executors
- **Internal metadata:** Task result metadata

**Behavior when full:** Storage memory can evict cached data (LRU) to reclaim space. Evicted data must be recomputed if needed.

### User Memory

Used for:
- User Python/Scala code data structures
- Python UDFs (non-Pandas, runs in separate process — see Part 3)
- String interning, object headers

**Behavior when full:** JVM heap exhaustion → OOM. User memory is not managed by Spark — overflows here cause direct OOM.

### System Memory

Reserved for:
- JVM internal structures (method tables, class metadata)
- Direct byte buffers
- Native memory

**Behavior when full:** JVM itself crashes. Hard limit.

---

## Part 2 — Unified Memory Model (Spark 1.6+)

### The Key Change from Spark 1.5

Before Spark 1.6, execution and storage memory were hard-partitioned. If execution was using 80% and storage had free space, execution couldn't use it → spill.

Spark 1.6+ introduced **unified memory** — execution and storage share a pool:

```
Execution Memory + Storage Memory = spark.memory.fraction × executor_heap
```

**Crucial rule:** When execution is full, it **cannot evict** cached data from storage memory. This is the opposite of what happens when storage is full.

| Who is Full | Can Evict Other? | Action |
|---|---|---|
| Execution | NO | Spill to disk (shuffle spill) |
| Storage | YES | Evict LRU cached data |

This is why "increase storage memory" doesn't help shuffle operations — execution can't use evicted storage memory.

---

## Part 3 — Python UDF Memory (Off-Heap)

### The Separate Python Process

Non-Pandas Python UDFs run in a **separate Python subprocess** on each executor, not in the JVM heap:

```
Executor JVM (spark.executor.memory)
└─ Spark tasks, Spark memory, JVM GC

Python Worker Process (spark.python.worker.memory = 512MB default)
└─ Python UDF execution, Python heap, Python GC
```

### Memory Sizing

| Property | Default | Scope |
|---|---|---|
| spark.python.worker.memory | 512MB per worker | Per Python worker process |
| spark.python.worker.reuse | true | Reuse workers across tasks |
| spark.python.worker.inherit parent's classpath | false | Worker isolation |

### When Python UDF OOM Occurs

A Python UDF creates many Python objects (lists, dictionaries, custom objects). When the Python worker process exhausts its 512MB, the Python process crashes → **executor reports OOM even though JVM heap looks fine**.

**Why JVM heap looks fine:** Python process is separate from JVM. The JVM didn't run out of heap — the Python subprocess did.

**Fix:**
- Use Pandas UDFs (operate on Arrow-backed data in JVM, not Python process)
- Increase `spark.python.worker.memory` (gives Python more off-heap memory)
- Reduce the amount of data processed per Python UDF call (increase partitions)

### Pandas UDFs and Memory

Pandas UDFs (scalar, grouped map) use **Apache Arrow** for zero-copy JVM-to-Python data transfer. They process batches in the JVM, not in a separate Python process:

```python
# Non-Pandas UDF: runs in Python process
@udf(IntegerType())
def add_one(x):
    return x + 1

# Pandas UDF: runs in JVM via Arrow
@pandas_udf(IntegerType())
def add_one_batch(s: pd.Series) -> pd.Series:
    return s + 1
```

Pandas UDFs are subject to JVM memory management, not Python worker memory. This is why Pandas UDFs are preferred for performance — and why they avoid Python OOM.

---

## Part 4 — Driver vs Executor OOM

### Driver OOM

| Cause | Scenario | Fix |
|---|---|---|
| collect() of large result | `df.collect()` on 500 GB DataFrame | Write to storage instead; increase spark.driver.maxResultSize |
| Broadcasting large table | `broadcast(large_df)` | Don't broadcast tables > 10 GB |
| Large groupBy result | `df.groupBy("key").agg(collect_list("text")).collect()` | Use write, not collect |
| High cardinality groupBy | `df.groupBy("high_card_col").count().collect()` | Don't collect high-cardinality groupBy results |

**Driver OOM signature:** "Lost connection to driver node" or "Driver process exited"

### Executor OOM

| Cause | Scenario | Fix |
|---|---|---|
| Partition too large | `df.coalesce(2)` on 100 GB → 50 GB per partition | Increase partitions; use repartition |
| Cache bloat | Caching multiple large DataFrames | Unpersist; use MEMORY_AND_DISK |
| Python UDF memory | Python UDF processing 50M rows | Use Pandas UDF; increase spark.python.worker.memory |
| Shuffle spill overflow | Large groupBy with high cardinality | Increase shuffle partitions |
| All partitions large | `spark.sql.shuffle.partitions = 10` on 50 GB | Increase to 200+ |

**Executor OOM signature:** "Lost task X.0 in stage Y.0 (TID Z)" + Java heap space error

### The Decision Tree for OOM Diagnosis

```
OOM occurs:
  │
  ├─ Who failed?
  │   ├─ Driver (driver connection lost)
  │   │   └─ collect(), broadcast, large groupBy result
  │   │       Fix: write to storage, increase maxResultSize
  │   │
  │   └─ Executor (lost task, java heap space)
  │       ├─ One executor at 100%, others idle
  │       │   └─ Data skew (one partition dominates)
  │       │       Fix: AQE skew join, salted join
  │       │
  │       ├─ All executors at high memory
  │       │   ├─ Too many cached DataFrames
  │       │   │   Fix: unpersist, reduce cache level
  │       │   │
  │       │   └─ Partition count too low
  │       │       Fix: increase spark.sql.shuffle.partitions
  │       │
  │       └─ Python process OOM (JVM heap looks fine)
  │           Fix: Pandas UDFs, increase spark.python.worker.memory
  │
  └─ What operation was running?
      ├─ Shuffle (groupBy, join, sort)
      │   └─ Increase shuffle partitions; AQE
      ├─ Python UDF
      │   └─ Use Pandas UDF; increase Python worker memory
      └─ collect() / broadcast
          └─ Driver OOM → fix at driver level
```

---

## Part 5 — Garbage Collection

### GC Pressure

Spark applications generate many short-lived objects:
- Each row → new object
- Intermediate aggregation results → many objects
- Python UDFs → even more objects (Python object overhead)

When GC runs frequently, execution pauses → **GC pause = latency**.

### Types of GC

| GC Type | Trigger | Impact |
|---|---|---|
| Minor GC (Scavenge) | Young generation full | Low pause |
| Major/Full GC | Old generation full | High pause |
| G1 GC (default in Java 11+) | Mixed | Adaptive pause targets |

**Spark JVM tuning goal:** Minimize GC pause time so tasks complete without long interruptions.

### Reducing GC Pressure

| Technique | How It Helps |
|---|---|
| Increase partitions | Fewer objects per partition → less GC per task |
| Use Pandas UDFs | Arrow data is off-heap, not Java heap objects |
| Avoid Python UDFs on large data | Python objects have higher memory overhead |
| Cache at appropriate level | MEMORY_AND_DISK vs MEMORY_ONLY |
| Avoid very high shuffle partitions | More tasks = more GC cycles |
| Use Tungsten/Arrow backend | Off-heap columnar storage |

### Spark UI GC Metrics

In the Spark UI Executors tab:
- **GC Time:** Time spent in GC per executor
- **GC Count:** Number of GC events

**High GC time** (> 10% of executor time) = GC pressure problem. Look at:
- Number of cached DataFrames
- Python UDF usage
- Object allocation rate (too many small objects)

---

## Part 6 — Caching and Persistence

### Cache Levels

```python
# Default: MEMORY_AND_DISK (tries memory, spills to disk if needed)
df.cache()

# Explicit levels
df.persist(pyspark.StorageLevel.MEMORY_ONLY)       # Only memory, recompute if evicted
df.persist(pyspark.StorageLevel.MEMORY_AND_DISK)   # Memory, then disk
df.persist(pyspark.StorageLevel.DISK_ONLY)          # Disk only
df.persist(pyspark.StorageLevel.MEMORY_ONLY_SER)     # Memory, serialized (less memory, more CPU)
df.persist(pyspark.StorageLevel.MEMORY_AND_DISK_SER) # Memory + disk, serialized

# Unpersist
df.unpersist()
spark.catalog.clearCache()  # Clear all cache
```

### Memory Usage of Cached Data

| Storage Level | Memory Usage | CPU Cost | Disk Usage |
|---|---|---|---|
| MEMORY_ONLY | High (full data in heap) | Low | None |
| MEMORY_AND_DISK | High | Low | Spill to disk if needed |
| MEMORY_ONLY_SER | Medium (serialized) | High (deserialization) | None |
| DISK_ONLY | None | Low | Full data |

**MEMORY_ONLY_SER** uses less memory because data is serialized (bytes, not objects). But deserializing on read adds CPU cost.

### Cache Memory from Storage Memory Pool

Cached data consumes **storage memory**, which is part of the unified memory pool. If cached data fills storage memory:
- New caching requests may fail or evict existing cached data
- Evicted cached data must be recomputed if accessed again

**Best practice:** Only cache DataFrames that are reused multiple times. Caching a one-time-use DataFrame wastes memory.

### Cache Size in Spark UI

In the Spark UI Storage tab:
- **Cached DataFrames:** Shows all cached tables
- **Size in Memory:** Actual bytes in executor heap
- **Size on Disk:** Bytes spilled to disk (MEMORY_AND_DISK)
- **Caching Level:** e.g., MEMORY_AND_DISK

---

## Part 7 — Key Config Properties

| Property | Default | Meaning |
|---|---|---|
| spark.executor.memory | Cluster config | Total executor JVM heap |
| spark.memory.fraction | 0.6 | Fraction of heap for Spark (execution + storage) |
| spark.memory.storageFraction | 0.5 | Fraction of Spark memory for storage (rest for execution) |
| spark.driver.memory | Cluster config | Driver JVM heap |
| spark.driver.maxResultSize | 1GB | Max collect() result size |
| spark.python.worker.memory | 512MB | Python worker process heap |
| spark.python.worker.reuse | true | Reuse Python workers across tasks |
| spark.storage.memoryFraction | Deprecated in 3.x | Was used in pre-unified-memory model |
| spark.executor.overhead | driverMemory × 0.1, min 384MB | Off-heap memory for executor |

### Off-Heap Memory

spark.executor.memoryOverhead controls off-heap memory for:
- JVM overhead
- Direct byte buffers
- Native code allocations

This is separate from JVM heap. Increase when using native libraries or seeing off-heap OOM.

---

## Cross-References

- **Day 4:** Architecture — Driver and Executor are the two memory contexts
- **Day 6:** Shuffle spill happens when executor memory is insufficient for sort/aggregation
- **Day 8:** Spark UI shows memory usage, GC time, and cache metrics
- **Day 3:** Python UDF vs Pandas UDF memory behavior
- **Day 2:** Cluster-level library installation (notebook-scoped installs don't help UDFs)
