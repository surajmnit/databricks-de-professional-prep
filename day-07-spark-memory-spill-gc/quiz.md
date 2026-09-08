# Day 7 — Quiz: Spark Memory, Spill, and Garbage Collection

**Objective coverage:** Section 6 (13%) — memory reasoning.

---

## Question 1

**Objective:** Understand the unified memory model.

In Spark's unified memory model (Spark 2.0+), execution memory and storage memory share a pool. Which statement is correct?

A. Execution memory can evict storage memory when needed, but storage memory cannot evict execution memory
B. Storage memory can evict execution memory when needed, but execution memory cannot evict storage
C. Execution and storage memory are hard-separated with no sharing
D. Both can evict each other without restriction

---

## Question 2

**Objective:** Understand why Python UDF OOM doesn't show in executor JVM heap.

A job fails with an executor OOM. The Spark UI shows the executor's JVM heap usage at 60%, not at 100%. The failure occurred during a Python UDF execution. Investigation reveals that the executor has significant off-heap memory usage.

What is the most likely explanation?

A. The executor JVM is misconfigured and not using all available heap
B. Python UDF subprocess memory is off-heap (spark.python.worker.memory) and separate from JVM heap
C. The executor has a memory leak in native code
D. Spark is using off-heap storage for cached data

---

## Question 3

**Objective:** Understand driver OOM from collect().

A notebook runs:

```python
result = df.groupBy("category").agg(F.collect_list("text_column")).collect()
```

The driver loses connection and the job fails. Which property controls the maximum size of this result?

A. spark.executor.memory
B. spark.driver.memory
C. spark.driver.maxResultSize
D. spark.sql.shuffle.partitions

---

## Question 4

**Objective:** Understand cache bloat OOM.

An executor running a groupBy aggregation fails with OOM. Prior to this, two large DataFrames were cached:

```python
df1 = spark.read.parquet("data1").cache().count()
df2 = spark.read.parquet("data2").cache().count()
result = df1.groupBy("key").count().collect()
```

What caused the OOM?

A. The groupBy output was too large for driver memory
B. The cached DataFrames consumed storage memory, leaving insufficient execution memory for the groupBy
C. The executor had too few CPU cores
D. The shuffle partition count was too high

---

## Question 5

**Objective:** Understand spill.

During a sort operation, Spark writes intermediate data to disk because the executor's memory was insufficient to hold all the data for sorting.

What term describes this behavior?

A. Shuffle write
B. Shuffle spill
C. Partition overflow
D. Memory eviction

---

## Question 6

**Objective:** Understand what reduces spill.

An executor frequently spills during groupBy aggregations. Which change most directly reduces spill?

A. Increase spark.executor.memory (if cluster allows)
B. Increase spark.sql.shuffle.partitions (more partitions = smaller per-partition data)
C. Enable spark.sql.autoBroadcastJoinThreshold (reduces shuffle volume)
D. Increase spark.python.worker.memory

---

## Question 7

**Objective:** Understand GC pressure in Spark.

A Spark job shows long task durations with frequent GC pauses visible in the task timeline. The operations involve many narrow transformations creating intermediate row objects.

Which approach most directly reduces GC pressure?

A. Increase the number of shuffle partitions
B. Use broadcast joins to reduce data volume
C. Use Pandas UDFs instead of Python UDFs (Arrow-backed processing)
D. Reduce the number of executors

---

## Question 8

**Objective:** Understand executor memory regions.

On a Databricks cluster with 64 GB executor memory, what is the approximate size of the execution + storage memory pool? (Using defaults: memory.fraction = 0.6, reserved = 300MB)

A. 38.4 GB
B. 32 GB
C. 6.4 GB
D. 38.1 GB

---

## Question 9

**Objective:** Distinguish driver OOM from executor OOM failure modes.

A Spark job fails with the error: "Lost task 0.3 in stage 2.0 (TID 0), executor 2." The error log shows a Java heap space error on the executor.

Which investigation confirms this is an executor OOM (not driver)?

A. Check spark.driver.maxResultSize configuration
B. Check spark.executor.memory and the Spark UI Executors tab for that executor
C. Check if collect() was used in the query
D. Check spark.driver.memory configuration

---

## Question 10

**Objective:** Understand when to use unpersist().

A pipeline caches multiple large DataFrames early in the session, uses them for multiple operations, and then runs for several more hours without needing them.

Which statement is most accurate?

A. Keeping them cached is fine — Spark manages cache eviction automatically
B. Unpersist() should be called to free executor memory after use
C. Cached data is automatically evicted when memory pressure increases
D. The cache will clear at the end of the session automatically

---

## Question 11

**Objective:** Understand Python worker memory.

The spark.python.worker.memory is set to 512MB (default). A Python UDF processes a very large dataset and fails with an out-of-memory error on the executor, but the Spark UI shows executor JVM heap is only 40% used.

What is the most direct fix?

A. Increase spark.executor.memory to give the JVM more heap
B. Increase spark.python.worker.memory to give the Python subprocess more memory
C. Restart the cluster to clear the Python worker state
D. Convert to a Scala UDF instead of a Python UDF

---

## Question 12

**Objective:** Understand when Python UDFs vs Pandas UDFs use JVM heap.

Which statement correctly describes the memory behavior of Python UDFs vs Pandas UDFs?

A. Python UDFs use JVM heap; Pandas UDFs use off-heap memory
B. Python UDFs use off-heap Python process memory; Pandas UDFs use JVM heap via Arrow
C. Both use JVM heap but Pandas UDFs are more GC-efficient
D. Both use off-heap memory

---

## Answer Key

### Q1: A — Execution memory can evict storage memory when needed, but storage memory cannot evict execution memory

In the unified memory model, execution memory and storage memory share a pool. When execution needs memory, it can evict cached storage data. When storage needs memory (to cache new data), it cannot evict execution memory — execution is protected.

**Why others are wrong:** B reverses the relationship. C is the pre-Spark 1.6 behavior (hard separation). D overstates the sharing — execution cannot evict storage.

---

### Q2: B — Python UDF subprocess memory is off-heap (spark.python.worker.memory) and separate from JVM heap

Python UDFs run in a separate Python subprocess on each executor. That subprocess has its own heap controlled by `spark.python.worker.memory` (default 512MB) and is NOT counted in the executor JVM heap. So the JVM heap can look fine while the Python worker OOMs.

**Why others are wrong:** A is not the root cause. C (native code memory leak) is possible but the Python UDF subprocess is the known cause. D (off-heap storage) is not the pattern described.

---

### Q3: C — spark.driver.maxResultSize

`collect()` pulls results to the driver JVM. `spark.driver.maxResultSize` (default 1GB) caps how large that result can be. Exceeding it causes SparkException.

**Why others are wrong:** A (executor.memory) is not related to driver operations. B (driver.memory) is the JVM heap size, not the result size limit. D (shuffle.partitions) affects parallelism, not collect result size.

---

### Q4: B — Cached DataFrames consumed storage memory, leaving insufficient execution memory for the groupBy

Cached DataFrames use storage memory. The groupBy aggregation needs execution memory (for hash table, sort buffers). When storage memory is full (from cache) and execution needs more, the cached data is evicted — but if eviction cannot keep up or the data is too large, OOM occurs.

**Why others are wrong:** A (driver OOM) — collect() collects groupBy results, but the failure message says "Lost task" on an executor. C (CPU cores) is not a memory issue. D (shuffle partitions) affects parallelism, not memory allocation for caching.

---

### Q5: B — Shuffle spill

Spill occurs when executor memory is insufficient for the current operation. Unlike normal shuffle write (writing map output for the next stage), spill is an overflow mechanism — data written to disk because it doesn't fit in memory.

**Why others are wrong:** A (shuffle write) is a normal operation, not an overflow. C (partition overflow) is not a standard Spark term. D (memory eviction) describes cache eviction from storage, not spill.

---

### Q6: B — Increase spark.sql.shuffle.partitions (more partitions = smaller per-partition data)

Spill occurs when per-partition data is too large for executor memory. Increasing shuffle partitions means each partition is smaller, reducing the memory required per task.

**Why others are wrong:** A (increase executor.memory) is valid but requires cluster reconfiguration. C (broadcast threshold) affects join strategy, not spill in groupBy. D (python worker memory) affects Python UDF memory, not groupBy spill.

---

### Q7: C — Use Pandas UDFs instead of Python UDFs (Arrow-backed processing)

Pandas UDFs use Apache Arrow to process data within the JVM without creating per-row Python objects. This dramatically reduces object creation and GC pressure.

**Why others are wrong:** A (more shuffle partitions) affects parallelism, not GC overhead. B (broadcast joins) reduces data volume but doesn't reduce object churn. D (fewer executors) reduces parallelism, not GC pressure.

---

### Q8: D — 38.1 GB

Formula: (64GB - 300MB reserved) × 0.6 = 63.7GB × 0.6 = 38.22GB ≈ 38.1GB

63.7GB × 0.6 = 38.22GB. The options: A (38.4) is 64 × 0.6. B (32) is 64 × 0.5. C (6.4) is 64 × 0.1. D (38.1) is correct: (64 - 0.3) × 0.6 ≈ 38.1.

**Why others are wrong:** A ignores the 300MB reserved memory. B uses 0.5 instead of 0.6. C is 0.1 fraction.

---

### Q9: B — Check spark.executor.memory and the Spark UI Executors tab for that executor

"Lost task" errors originate from executors, not the driver. To confirm executor OOM, check the executor's memory configuration and the Spark UI Executors tab — the failed executor should show high or maxed memory.

**Why others are wrong:** A (driver.maxResultSize) and D (driver.memory) are driver-side configurations — not relevant to "Lost task" errors. C (collect()) is a driver-side operation and would cause driver failure, not executor task failure.

---

### Q10: B — Unpersist() should be called to free executor memory after use

Spark does not automatically clear cache when it's no longer needed. Cached DataFrames remain in storage memory until evicted by memory pressure, unpersist() is called, or the session ends. For long-running pipelines, unpersist() frees memory for other operations.

**Why others are wrong:** A (Spark manages automatically) — Spark evicts LRU when memory is needed, but not proactively. C (auto-eviction) — eviction only happens under memory pressure, not proactively. D (auto-clear at session end) — yes at session end, but for multi-hour sessions this is too late.

---

### Q11: B — Increase spark.python.worker.memory to give the Python subprocess more memory

The Python worker subprocess is separate from the JVM heap. Increasing `spark.python.worker.memory` gives the Python subprocess more off-heap memory to process data.

**Why others are wrong:** A (increase executor.memory) increases JVM heap, not Python worker memory — they are separate. C (restart cluster) is a workaround, not a configuration fix. D (Scala UDF) is not the direct fix for Python UDF memory.

---

### Q12: B — Python UDFs use off-heap Python process memory; Pandas UDFs use JVM heap via Arrow

Python UDFs run in a separate Python subprocess (off-heap). Pandas UDFs use Apache Arrow, which passes data within the JVM without the Python subprocess. This is the fundamental memory architecture difference.

**Why others are wrong:** A (Python UDFs use JVM heap) — incorrect, Python UDFs use a separate subprocess. C (both JVM heap) — Python UDFs use off-heap subprocess. D (both off-heap) — Pandas UDFs use JVM heap via Arrow.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Unified memory model eviction rules |
| 2 | Medium | Python UDF off-heap memory |
| 3 | Easy | spark.driver.maxResultSize |
| 4 | Medium | Cache bloat OOM mechanism |
| 5 | Easy | Shuffle spill terminology |
| 6 | Medium | Spill reduction via partitions |
| 7 | Medium | GC pressure and Pandas UDFs |
| 8 | Hard | Memory fraction calculation |
| 9 | Medium | Driver vs executor OOM failure mode |
| 10 | Easy | When to call unpersist() |
| 11 | Medium | Python worker memory fix |
| 12 | Easy | Python UDF vs Pandas UDF memory model |
