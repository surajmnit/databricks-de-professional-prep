# Day 6 — Spark Shuffle Deep-Dive

> ⚠️ **Correction notice:** This version fixes one issue found in an earlier draft: the manual salted-join example had a syntax error — `expr("concat(key, '-', cast(salt as string)")` has unbalanced parentheses (`concat(` and `cast(` both open, but only one `)` closes) and would raise a parse error the moment Spark evaluated it. The example is rewritten below using the same clean, verified pattern used in Day 3.

## Exam Objectives

This day covers **Section 6: Cost & Performance Optimization (13%)** — specifically the most expensive operation in Spark: the shuffle.

Understanding shuffle is essential for:
- Identifying why a query is slow (it's usually the shuffle)
- Configuring the right number of shuffle partitions
- Recognizing when a broadcast join avoids a shuffle entirely
- Understanding Spark UI metrics (shuffle read/write bytes)
- Diagnosing spill and skew issues

---

## Part 1 — What Is a Shuffle?

### The Core Definition

A **shuffle** occurs when Spark needs to move data between executors. This is the most expensive operation in Spark because it involves:

1. **Writing data to local disk** (shuffle write)
2. **Network transfer** (data sent across the network to other executors)
3. **Reading data from disk** (shuffle read)

Unlike narrow transformations where each executor processes its own partition independently, a shuffle requires coordination across the cluster.

### Why Shuffles Happen

A shuffle is triggered whenever Spark needs to co-locate data based on a key or partitioning scheme:

- **groupBy()** — all rows with the same key must be on the same executor
- **join()** — matching keys from both sides must be co-located
- **repartition()** — data must be redistributed
- **distinct()** — all identical values must be on the same executor
- **sort() / orderBy()** — global ordering requires all data together
- **Window functions** — when the window frame spans partitions

### Narrow vs Wide — The Shuffle Test

If a transformation requires data from other partitions, it is wide:
- **groupBy** — wide (keys may be on any partition)
- **join** — wide (matching keys must be found on both sides)
- **repartition** — wide (data redistributed)
- **sort** — wide (global order)
- **filter** — narrow (each row stands alone)
- **withColumn** — narrow (same row)
- **select** — narrow (same row)

---

## Part 2 — Shuffle Write

### How Shuffle Write Works

When a Stage finishes processing, its output must be written to disk so the next Stage can read it. This is the **shuffle write**.

```
Stage 0 (Map Stage)
  Task 0: processes partition 0 -> writes shuffle file(s)
  Task 1: processes partition 1 -> writes shuffle file(s)
  Task 2: processes partition 2 -> writes shuffle file(s)
  ...
  Task N: processes partition N -> writes shuffle file(s)
```

Each task writes its output to **local disk** (on the executor that ran the task). These are the shuffle write files.

### Shuffle Write Output

Spark writes one **bucket per reduce task**:
- Number of shuffle write files = (number of map tasks) x (number of shuffle partitions)
- Default shuffle partition count = 200 (`spark.sql.shuffle.partitions`)

For a 100-partition DataFrame with default shuffle partitions:
- 100 map tasks × 200 shuffle partitions = 20,000 shuffle write files
- Each map task writes 200 small files

This is why **too many shuffle partitions creates too many small files** — a common performance problem.

### Partitioners

During shuffle write, Spark uses a **partitioner** to decide which reduce partition each key goes to:

| Partitioner | Used For |
|---|---|
| HashPartitioner | Default for groupBy, count, distinct, etc. |
| RangePartitioner | Used for sortByKey, orderBy (orders by range) |

**HashPartitioner behavior:**
```python
target_partition = hash(key) % num_partitions
```

If a key has a very high cardinality (or a skewed hash), many keys map to the same partition → **data skew**.

### Shuffle Write Metrics

In the Spark UI Stages tab, the **Shuffle Write** column shows:
- **Bytes Written**: total data written to shuffle files
- **Records Written**: total rows written
- **Write Time**: time spent writing to disk

**Shuffle write time increases with:**
- Number of output files (more partitions = more files)
- Data size per partition
- Disk I/O speed on the executor
- Network bandwidth

---

## Part 3 — Shuffle Read

### How Shuffle Read Works

The next Stage (Reduce Stage) must read the shuffle output from all map tasks. This is the **shuffle read**.

```
Stage 1 (Reduce Stage)
  Task 0: reads shuffle output from all Stage 0 tasks (partition 0 from each map task)
  Task 1: reads shuffle output from all Stage 0 tasks (partition 1 from each map task)
  ...
```

Each reduce task reads **one partition worth of data from all map tasks**. The reduce task's partition determines which keys it handles.

### Locality and Remote Reads

Spark tries to schedule reduce tasks on executors that have the relevant shuffle data (locality optimization):
- **Local read**: data is on the same executor → fast (no network)
- **Remote read**: data is on another executor → network transfer

When local reads are not possible (no executor has the data), Spark fetches data over the network. This adds latency and network cost.

### Shuffle Read Metrics

In the Spark UI Stages tab, the **Shuffle Read** column shows:
- **Bytes Read**: total data read from shuffle files
- **Records Read**: total rows read
- **Read Time**: time spent reading from disk/network
- **Remote Reads**: bytes read from remote executors (high = locality problem)
- **Local Reads**: bytes read from local disk (high = good)

**High remote reads** = reduce tasks are running on executors that don't have the shuffle data → Spark couldn't schedule tasks optimally.

---

## Part 4 — Shuffle Partition Count

### The Default: 200

`spark.sql.shuffle.partitions` (default 200) controls the number of partitions for shuffle operations.

```python
df.groupBy("region").count()  # Uses 200 shuffle partitions

spark.conf.set("spark.sql.shuffle.partitions", 400)  # Override
```

### Sizing Shuffle Partitions

| Scenario | Partition Count | Reasoning |
|---|---|---|
| Small data (< 1 GB) | 50–200 | Default may cause overhead |
| Medium data (1–100 GB) | 200–400 | Balance parallelism and overhead |
| Large data (100 GB+) | 400–2000 | More parallelism, smaller per-partition work |
| Very large data (1 TB+) | 1000–4000 | High parallelism needed |

**Rule of thumb:** Target 128–256 MB per shuffle partition.

For a 10 GB groupBy with 200 shuffle partitions: 10 GB / 200 = 50 MB per partition — reasonable.
For a 10 GB groupBy with 10 shuffle partitions: 10 GB / 10 = 1 GB per partition — OOM risk.

### Too Many Shuffle Partitions

| Problem | Impact |
|---|---|
| Too many small files | Excessive file system overhead |
| More tasks = more task scheduling overhead | Slower job startup |
| More objects in memory | GC pressure |
| Shuffle write: 100 tasks × 200 partitions = 20,000 files | Too many files, slow |

**Exam trap:** setting shuffle partitions far higher than the number of *distinct keys* in the aggregation doesn't help — most of those partitions will simply sit empty (0 bytes, 0 records), while the few partitions that do receive data are unaffected in size. The overhead here is purely scheduling/bookkeeping cost for thousands of empty tasks, not better load balancing.

### Too Few Shuffle Partitions

| Problem | Impact |
|---|---|
| Larger per-partition data | OOM risk on executors |
| Less parallelism | Underutilized cluster |
| Skew more likely | One large partition dominates |

### spark.sql.adaptive.coalescePartitions

AQE (enabled by default) coalesces shuffle partitions automatically when partitions are too small:

```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")  # Default
```

This helps but is not a substitute for proper partition sizing.

---

## Part 5 — Shuffle and Spill

### What Is Spill?

When **executor memory is insufficient** for the shuffle sort or aggregation, Spark writes data to disk (spill). This is controlled by:

- `spark.shuffle.memoryFraction` (deprecated in Spark 3.x) — used to control shuffle memory
- `spark.executor.memory` — total executor heap
- `spark.memory.storageFraction` — storage vs execution memory ratio

In Spark 3.x with AQE, spill handling is managed more dynamically.

### Spill Metrics in Spark UI

In the Stage details:
- **Spilled Records**: rows written to disk because memory was insufficient
- **Spilled Size**: bytes written to disk

**Spill is bad** because:
- Disk I/O is 10–100x slower than memory operations
- Spill during shuffle write or read adds latency
- Large spills can fill disk and crash the executor

### Reducing Spill

| Cause | Fix |
|---|---|
| Too few shuffle partitions (large partitions) | Increase `spark.sql.shuffle.partitions` |
| Too much cached data filling storage memory | Unpersist cached DataFrames |
| Python UDF memory pressure | Increase `spark.python.worker.memory` |
| High-cardinality groupBy | Use AQE skew join optimization |

---

## Part 6 — Shuffle and Data Skew

### How Skew Causes Shuffle Problems

In a groupBy:
```python
df.groupBy("key").count()
```

If key="UNKNOWN" appears in 40% of rows:
- 40% of data goes to one shuffle partition
- One reduce task gets 40% of total data
- That task takes 40% of total time → overall job is slow
- That executor's memory is stressed → OOM risk

### Skew Signs in Spark UI

| Metric | Skew Indicator |
|---|---|
| Task durations | One task >> all others |
| Shuffle write bytes | One task writes far more than others |
| Shuffle read bytes | One task reads far more than others |
| Input bytes | One partition >> others |

### AQE Skew Join Handling

Spark 3.x AQE automatically detects skewed partitions and splits them:

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)  # Default: True
```

When AQE detects skew:
- It splits the skewed partition into smaller sub-partitions
- Each sub-partition is processed by a separate task
- Reduces the impact of skew on overall job time

### Manual Skew Handling

When AQE is insufficient, salt **both** sides of the join consistently — the large (skewed) side gets a random salt suffix, and the small side must be exploded/replicated across every possible salt value so a match still exists:

```python
from pyspark.sql.functions import rand, explode, array, lit, concat, col

n_salt = 10

# Salt the large (skewed) table
df_large_salted = (
    df_large
    .withColumn("salt", (rand() * n_salt).cast("int"))
    .withColumn("key_salted", concat(col("key"), lit("-"), col("salt")))
)

# Replicate every row of the small table across all n_salt buckets, so that
# whichever salt value a large-table row was randomly assigned, a matching
# small-table row exists
df_small_salted = (
    df_small
    .withColumn("salt", explode(array([lit(i) for i in range(n_salt)])))
    .withColumn("key_salted", concat(col("key"), lit("-"), col("salt")))
)

# Join on the salted key
result = df_large_salted.join(df_small_salted, "key_salted")
```

**Exam trap:** salting only the large side (or giving the small side a fabricated key unrelated to its real join column) silently drops most matches — no error is raised, the join just returns far fewer rows than it should. Always sanity-check row counts against the unsalted result while developing a salted join (see Day 3's hands-on lab for exactly this check).

---

## Part 7 — Broadcast Joins: Avoiding Shuffle Entirely

### When a Broadcast Works

If one table is small enough to fit in all executors' memory, Spark broadcasts it — **no shuffle needed**.

```python
from pyspark.sql.functions import broadcast

result = df_large.join(broadcast(df_small), "key")
```

The small table is serialized and sent to all executors. The large table is processed locally without a shuffle.

### Broadcast Threshold

`spark.sql.autoBroadcastJoinThreshold` (default: 10MB)
- Tables < 10MB: auto-broadcasted
- Tables > 10MB × 1.1: never auto-broadcasted
- Between: Spark estimates and may broadcast

**For Parquet/Delta:** The table statistics (size in metadata) determine the estimate. For in-memory DataFrames, Spark uses the plan's estimate.

### When NOT to Broadcast

- Table > 10 GB: broadcasting causes memory pressure on all executors
- Many executors: each executor receives the full broadcast → multiplied memory usage
- Repeated broadcasts: consider caching the small table instead

---

## Cross-References

- **Day 4:** Stage boundaries come from shuffle operations
- **Day 5:** DAG stage boundaries are where shuffles occur
- **Day 7:** Executor memory used during shuffle (sort, aggregation, spill)
- **Day 8:** Spark UI shuffle read/write metrics for diagnosis
- **Day 3:** Broadcast join optimization; the salted-*join* pattern (join-side salting must replicate the small table, unlike salted-*groupBy* re-aggregation)
