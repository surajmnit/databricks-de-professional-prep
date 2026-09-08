# Day 7 — Quiz: Spark Memory, Garbage Collection, and OOM

**Objective coverage:** Section 6 (13%) — memory architecture and OOM diagnosis.

---

## Question 1

**Objective:** Understand executor memory regions.

Executor memory is divided into regions. Which region holds cached DataFrames?

A. Execution Memory
B. Storage Memory
C. User Memory
D. System Memory

---

## Question 2

**Objective:** Understand the unified memory model.

In Spark's unified memory model, when execution memory is full and needs more space, what happens?

A. Execution evicts cached data from storage memory
B. Execution spills to disk (shuffle spill)
C. Spark reduces the user memory region to give more to execution
D. The executor is killed and tasks are rescheduled

---

## Question 3

**Objective:** Understand Python UDF memory behavior.

A Spark job uses a Python UDF to process 10 million rows. The job fails with an executor OOM error. The Spark UI shows the executor JVM heap is at 60% usage — not full.

Which explanation is most accurate?

A. The executor JVM is misconfigured
B. Python UDFs run in a separate Python subprocess with its own memory allocation
C. Spark's unified memory model is not working correctly
D. The cluster has insufficient disk space

---

## Question 4

**Objective:** Distinguish driver OOM from executor OOM.

A notebook calls `df.collect()` on a 500 GB DataFrame. The driver node loses connection. The Spark UI shows no executor memory issues.

What is the root cause?

A. Executor OOM during the groupBy aggregation
B. Driver OOM from collecting too much data to the driver JVM
C. Network partition between driver and cluster manager
D. Python worker memory exceeded in the UDF subprocess

---

## Question 5

**Objective:** Understand GC pressure causes.

A Spark job is observed to have 40% of executor time spent in GC. Looking at the code, there are many Python UDFs that process small objects, and multiple large DataFrames are cached.

Which fix most directly addresses the GC pressure?

A. Increase executor memory
B. Replace Python UDFs with Pandas UDFs
C. Remove all caching to reduce storage memory usage
D. Decrease spark.sql.shuffle.partitions

---

## Question 6

**Objective:** Understand cache levels.

A team caches a DataFrame using `df.cache()` (MEMORY_AND_DISK). The DataFrame is larger than available storage memory.

What happens?

A. An error is raised and the cache fails
B. Cached data is evicted from all executors
C. As much data as possible is cached in memory; the rest is spilled to disk
D. Spark automatically reduces the number of cached DataFrames

---

## Question 7

**Objective:** Understand storage memory eviction.

Cached DataFrames are stored in storage memory. What happens when storage memory is full and a new DataFrame needs to be cached?

A. Execution memory is reduced to make room for the new cache
B. Older cached data is evicted (LRU) to make room
C. The executor crashes
D. New caching requests are queued until space is available

---

## Question 8

**Objective:** Configure memory for Python UDFs.

A pipeline uses Python UDFs and fails with memory errors. The executor JVM heap looks healthy. The Python worker process is exhausting its memory.

Which property should be increased?

A. spark.executor.memory
B. spark.memory.fraction
C. spark.python.worker.memory
D. spark.driver.maxResultSize

---

## Question 9

**Objective:** Understand what causes executor OOM.

A groupBy aggregation on a 50 GB table fails with executor OOM. The shuffle partition count is 10.

What is the most likely cause?

A. Network bandwidth is insufficient
B. 50 GB / 10 partitions = 5 GB per partition — may exceed executor memory on skewed data
C. The executor has too much memory allocated
D. Shuffle partitions should equal the number of executors

---

## Question 10

**Objective:** Understand broadcast memory impact.

A 15 GB table is broadcast during a join. Each executor has 64 GB memory.

What memory consideration is most relevant?

A. Executor memory is unaffected because broadcasts use network buffers
B. Each executor must hold 15 GB in storage memory for the broadcast
C. The driver must hold 15 GB in driver memory
D. Broadcasts do not use executor memory

---

## Question 11

**Objective:** Apply cache memory optimization.

A pipeline caches 5 DataFrames. The total cached size exceeds available storage memory. Not all cached data is reused.

Which approach is most memory-efficient?

A. Cache all 5 DataFrames at MEMORY_ONLY level
B. Cache only the DataFrames that are reused multiple times
C. Increase executor memory to 128 GB
D. Use DISK_ONLY caching for all DataFrames

---

## Question 12

**Objective:** Understand MEMORY_ONLY_SER trade-offs.

A DataFrame is cached using MEMORY_ONLY_SER. Which statement correctly describes the trade-off vs MEMORY_ONLY?

A. MEMORY_ONLY_SER uses less memory but requires more CPU to deserialize
B. MEMORY_ONLY_SER uses less memory and also uses less CPU
C. MEMORY_ONLY_SER uses more memory but requires less CPU
D. Both use the same amount of memory

---

## Answer Key

### Q1: B — Storage Memory

Cached DataFrames and broadcast variables consume **storage memory**, which is part of Spark's unified memory pool.

**Why others are wrong:** A (Execution Memory) holds shuffle sort/join buffers. C (User Memory) holds user code and Python UDF data. D (System Memory) holds JVM internal structures.

---

### Q2: B — Execution spills to disk (shuffle spill)

In unified memory, execution memory cannot evict cached data from storage memory. When execution is full, it spills to disk.

**Why others are wrong:** A — execution cannot evict storage. C — user memory is fixed. D — executor is not killed, tasks spill.

---

### Q3: B — Python UDFs run in a separate Python subprocess with its own memory allocation

Python UDFs run in a separate Python worker process (`spark.python.worker.memory`, default 512MB). The JVM heap is unaffected — Python process memory is off-heap.

**Why others are wrong:** A (misconfigured) is not the core issue. C (unified memory broken) — the issue is that Python runs outside JVM heap. D (disk space) — disk space is not the constraint.

---

### Q4: B — Driver OOM from collecting too much data to the driver JVM

`collect()` pulls all data to the driver JVM. 500 GB of data in `collect()` would exhaust driver memory.

**Why others are wrong:** A (executor OOM) — executor OOM shows "Lost task" and executor memory metrics. C (network partition) — network issues don't produce a clean driver OOM. D (Python worker) — there was no Python UDF mentioned.

---

### Q5: B — Replace Python UDFs with Pandas UDFs

Python UDFs generate many Python objects, each with overhead. Pandas UDFs use Arrow-backed JVM memory, reducing object creation and GC pressure.

**Why others are wrong:** A (increase memory) delays the problem but doesn't fix the root cause (object creation rate). C (remove caching) loses the performance benefit of caching. D (fewer shuffle partitions) doesn't address GC from UDFs.

---

### Q6: C — As much data as possible is cached in memory; the rest is spilled to disk

MEMORY_AND_DISK behavior: cache what fits in storage memory, spill the rest to disk. No error is raised.

**Why others are wrong:** A (error raised) — MEMORY_AND_DISK never fails on size. B (evict all) — only evicted when needed for new caching. D (auto-reduce) — Spark does not auto-reduce anything.

---

### Q7: B — Older cached data is evicted (LRU) to make room

Storage memory uses LRU eviction — when space is needed for new caching, the least recently used cached data is evicted.

**Why others are wrong:** A (execution reduced) — execution and storage are in the same pool but execution cannot evict storage. C (executor crash) — LRU eviction prevents crash. D (queued) — no queuing mechanism.

---

### Q8: C — spark.python.worker.memory

This controls the Python worker subprocess heap, which is separate from JVM executor memory.

**Why others are wrong:** A (executor.memory) — JVM heap, not Python process. B (memory.fraction) — Spark fraction config, not Python. D (driver.maxResultSize) — driver memory, not Python subprocess.

---

### Q9: B — 50 GB / 10 partitions = 5 GB per partition — may exceed executor memory on skewed data

5 GB per partition may fit, but with skew (one partition getting much more), one task could exceed executor memory. This is the most likely cause.

**Why others are wrong:** A (network bandwidth) — network issues cause slowness, not OOM. C (too much memory) — more memory doesn't cause OOM. D (executors = partitions) — shuffle partitions should be sized for data volume, not cluster size.

---

### Q10: B — Each executor must hold 15 GB in storage memory for the broadcast

When a table is broadcast, every executor holds a copy of it in storage memory. With 15 GB broadcast, all executors use 15 GB of their storage memory.

**Why others are wrong:** A (network buffers) — broadcasts use memory, not just network. C (driver holds 15 GB) — the driver coordinates but doesn't hold the broadcast. D (no memory use) — incorrect, broadcasts consume storage memory.

---

### Q11: B — Cache only the DataFrames that are reused multiple times

Caching DataFrames that are accessed only once wastes storage memory. Only cache DataFrames that are reused in multiple downstream actions.

**Why others are wrong:** A (MEMORY_ONLY) — MEMORY_ONLY doesn't solve the memory size problem. C (increase memory) — delays the problem. D (DISK_ONLY) — DISK_ONLY removes the memory benefit of caching entirely.

---

### Q12: A — MEMORY_ONLY_SER uses less memory but requires more CPU to deserialize

MEMORY_ONLY_SER stores data in serialized format (bytes, not Java objects). This uses less memory but requires CPU to deserialize on each read. MEMORY_ONLY stores objects directly — more memory but no deserialization cost.

**Why others are wrong:** B (less CPU) — serialized data requires deserialization. C (more memory) — serialization reduces memory. D (same memory) — clearly incorrect.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Memory regions — storage holds cache |
| 2 | Medium | Unified memory — execution can't evict storage |
| 3 | Hard | Python UDF off-heap subprocess memory |
| 4 | Easy | Driver OOM from collect() |
| 5 | Medium | GC pressure from Python UDFs |
| 6 | Easy | MEMORY_AND_DISK spill behavior |
| 7 | Easy | Storage memory LRU eviction |
| 8 | Easy | spark.python.worker.memory config |
| 9 | Medium | Partition size and OOM risk |
| 10 | Easy | Broadcast consumes storage memory |
| 11 | Medium | Cache only reused DataFrames |
| 12 | Medium | MEMORY_ONLY_SER trade-offs |
