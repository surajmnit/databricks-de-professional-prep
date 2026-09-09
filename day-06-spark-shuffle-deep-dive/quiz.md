# Day 6 — Quiz: Spark Shuffle Deep-Dive

**Objective coverage:** Section 6 (13%) — understanding why queries are slow.

---

## Question 1

**Objective:** Understand what triggers a shuffle.

Which of the following Spark operations triggers a shuffle?

A. df.filter(F.col("status") == "active")
B. df.select("id", "name", "amount")
C. df.repartition(50)
D. df.withColumn("doubled", F.col("id") * 2)

---

## Question 2

**Objective:** Understand shuffle write mechanics.

A DataFrame has 100 partitions and the default shuffle partition count (200) is in effect. A groupBy aggregation is executed.

How many shuffle write files are produced by the map stage?

A. 100 (one per input partition)
B. 200 (one per shuffle partition)
C. 20,000 (100 input partitions × 200 shuffle partitions)
D. 300 (100 + 200)

---

## Question 3

**Objective:** Understand shuffle read mechanics.

During a shuffle read, each reduce task reads data from all map tasks for its assigned partition. If there are 50 map tasks and 200 shuffle partitions, how many remote reads does one reduce task need?

A. 1 (reads one shuffle file from one map task)
B. 50 (reads one partition from each of the 50 map tasks)
C. 200 (reads from all 200 shuffle partition outputs)
D. 10,000 (50 × 200)

---

## Question 4

**Objective:** Configure shuffle partition count appropriately.

A groupBy aggregation processes 20 GB of data. The shuffle partition count is set to 10. What is the most likely performance outcome?

A. Excellent performance due to low task count
B. Out-of-memory errors on executors (20 GB / 10 = 2 GB per partition)
C. Faster than 200 partitions because fewer tasks = less overhead
D. Shuffle write time is faster because there are fewer files

---

## Question 5

**Objective:** Identify skew from Spark UI metrics.

A Spark job processes a table with a groupBy. In the Spark UI Stage details, the task duration histogram shows one bar at 30 seconds with all others at 2-3 seconds. The Shuffle Write Bytes metric shows one task writing 8 GB while others write ~200 MB each.

What is the most likely diagnosis?

A. The cluster has insufficient network bandwidth
B. Data skew: one key value dominates one partition
C. The executor running that task is overloaded
D. The Spark shuffle partition count is too high

---

## Question 6

**Objective:** Understand when broadcast is appropriate.

A data engineer has a 50 GB fact table and a 50 MB dimension table. They want to optimize a join between them.

Which approach is most appropriate?

A. Broadcast the 50 GB fact table to all executors
B. Broadcast the 50 MB dimension table to all executors
C. Use sort-merge join with increased shuffle partitions
D. Repartition both tables before joining

---

## Question 7

**Objective:** Understand AQE skew join optimization.

Spark 3.x AQE automatically handles skewed partitions in groupBy operations. Which behavior is triggered?

A. AQE increases the executor memory to accommodate the skewed partition
B. AQE retries the skewed task multiple times until it succeeds
C. AQE splits the skewed partition into smaller sub-partitions processed by separate tasks
D. AQE falls back to a broadcast join to avoid the shuffle

---

## Question 8

**Objective:** Understand spill behavior.

An executor runs a groupBy aggregation that requires sorting keys. The executor's memory is insufficient to hold all the key-value pairs for its partition, so Spark writes data to disk.

What term describes this behavior?

A. Shuffle write
B. Shuffle spill
C. Coalesce
D. Partition overflow

---

## Question 9

**Objective:** Understand broadcast threshold.

The `spark.sql.autoBroadcastJoinThreshold` is set to 10 MB. A table is estimated at exactly 10 MB in size.

What happens?

A. It is never broadcasted because 10 MB exceeds the threshold
B. It is always broadcasted because it is exactly at the threshold
C. Spark estimates the actual size and may or may not broadcast it
D. An error is raised because the size equals the threshold

---

## Question 10

**Objective:** Diagnose unnecessary shuffle patterns.

A pipeline reads data, repartitions to 1000 partitions, and then immediately coalesces to 10 partitions before writing.

What is the unnecessary cost?

A. Reading 1000 files takes too long
B. The repartition to 1000 causes a shuffle that is immediately undone by coalesce
C. The coalesce to 10 causes a global sort
D. Writing 10 partitions is the bottleneck

---

## Question 11

**Objective:** Understand shuffle write vs read locality.

A Spark job has 10 executors. During a shuffle, one reduce task needs to read data from map tasks. The reduce task is running on executor-3. Map task output for that partition is distributed across executors 1-10.

Executor-3 has the shuffle data for partition 7 in its local disk.

Which type of shuffle read occurs for partition 7?

A. Local read (no network transfer needed)
B. Remote read (network transfer required)
C. Broadcast read (data sent to all executors)
D. No read occurs (data is cached)

---

## Question 12

**Objective:** Apply shuffle optimization reasoning.

A query performs a groupBy on a 500 GB table with 200 shuffle partitions. The cluster has 20 executors, each with 64 GB memory.

What is a key performance concern?

A. 500 GB / 200 partitions = 2.5 GB per partition — too small for efficient processing
B. 500 GB / 200 partitions = 2.5 GB per partition — fits in memory but disk spill risk if skew
C. 500 GB / 20 executors = 25 GB per executor — exceeds memory, OOM guaranteed
D. 200 shuffle partitions / 20 executors = 10 tasks per executor — well balanced

---

## Answer Key

### Q1: C — df.repartition(50)

repartition(n) triggers a shuffle — data must be redistributed across all partitions to achieve exactly n partitions.

**Why others are wrong:** A (filter), B (select), D (withColumn) are all narrow transformations — each row is processed within its partition, no data moves between executors.

---

### Q2: C — 20,000 (100 input partitions × 200 shuffle partitions)

Each map task writes one shuffle file per reduce task (per shuffle partition). With 100 map tasks and 200 shuffle partitions, that is 100 × 200 = 20,000 shuffle write files.

**Why others are wrong:** A (100) ignores that each map task writes for all shuffle partitions. B (200) is the number of reduce partitions, not the number of files written. D (300) is the sum of map and reduce counts, not the product.

**Exam trap:** This is why too many shuffle partitions causes too many small files — the file count is the product of map tasks and shuffle partitions, not the sum.

---

### Q3: B — 50 (reads one partition from each of the 50 map tasks)

Each reduce task reads the data for its assigned shuffle partition from ALL map tasks. With 50 map tasks, reduce task 0 reads partition 0 from each of the 50 map outputs.

**Why others are wrong:** A (1) ignores that all map tasks contribute data to each reduce partition. C (200) is the number of shuffle partitions, not the number of map tasks read from. D (10,000) is the total number of shuffle read operations across all reduce tasks.

---

### Q4: B — Out-of-memory errors (20 GB / 10 = 2 GB per partition)

With 10 shuffle partitions and 20 GB of data, each partition handles 2 GB. With executor memory typically between 8-64 GB, having 2 GB per partition may fit, but combined with skew (if one key dominates) and other memory usage, OOM is likely. The real risk is skew: if one key dominates, one 2 GB partition could become 15 GB → OOM.

**Why others are wrong:** A (excellent performance) ignores the OOM risk. C (faster than 200 partitions) ignores that fewer partitions means less parallelism and more work per task. D (faster shuffle write) ignores that fewer large files vs many small files is a trade-off, not a win.

---

### Q5: B — Data skew: one key value dominates one partition

The signature is unmistakable: one task takes 30s while others take 2-3s, and one task writes 8 GB while others write ~200 MB. This is classic data skew.

**Why others are wrong:** A (network bandwidth) would affect all tasks, not just one. C (executor overload) is a symptom of skew, not the root cause. D (too many shuffle partitions) would show similar durations across tasks, not one outlier.

---

### Q6: B — Broadcast the 50 MB dimension table

Broadcasting the large table (50 GB) would cause memory pressure on every executor (each executor holds a copy of 50 GB). Broadcasting the small table (50 MB) avoids the shuffle on the large fact table entirely.

**Why others are wrong:** A (broadcast fact) would cause executor OOM. C (sort-merge with more partitions) is valid but less optimal than broadcast when one table is small. D (repartition both) does not solve the problem of the large fact table being shuffled.

---

### Q7: C — AQE splits the skewed partition into smaller sub-partitions

When AQE detects skew (a partition much larger than others), it automatically splits that partition into smaller sub-partitions, each processed by a separate task. This reduces the impact of skew on overall job time.

**Why others are wrong:** A (increases memory) — AQE does not increase cluster memory. B (retries task) — retrying a skewed task doesn't fix the underlying skew. D (falls back to broadcast) — AQE skew join does not fall back to broadcast.

---

### Q8: B — Shuffle spill

Shuffle spill occurs when executor memory is insufficient for the sort/aggregation during shuffle. Spark writes data to disk, which is slower than memory operations. The Spark UI shows "Spilled Records" and "Spilled Size" metrics.

**Why others are wrong:** A (shuffle write) is a normal part of shuffle operation, not an overflow condition. C (coalesce) is a partition reduction operation, not a memory-pressure behavior. D (partition overflow) is not a standard Spark term.

---

### Q9: C — Spark estimates the actual size and may or may not broadcast

At exactly 10 MB, Spark uses a heuristic. Tables smaller than the threshold are broadcasted; tables larger than threshold × 1.1 are not. At exactly the threshold, Spark estimates and makes a decision — it is not deterministic.

**Why others are wrong:** A (never broadcasted) is incorrect — tables at or below the threshold can be broadcasted. B (always broadcasted) is incorrect — Spark doesn't blindly broadcast at the threshold. D (error raised) — no error is raised.

---

### Q10: B — The repartition to 1000 causes a shuffle that is immediately undone by coalesce

repartition(1000) triggers a full shuffle — data is redistributed to 1000 partitions. coalesce(10) immediately reduces this to 10 partitions using local coalescence (no shuffle). The 1000-partition shuffle was wasteful.

**Why others are wrong:** A (read time) is not described as the problem. C (coalesce global sort) — coalesce does not sort, it only reduces partitions locally. D (write bottleneck) is not the primary inefficiency described.

---

### Q11: A — Local read (no network transfer needed)

Spark's locality scheduler places reduce tasks on executors that have the relevant shuffle data. When a reduce task runs on an executor that already has its shuffle data locally (written by a map task on that same executor), it reads from local disk — a local read.

**Why others are wrong:** B (remote read) — remote reads occur when the executor doesn't have the data locally. C (broadcast read) — broadcast is used for small tables, not normal shuffle reads. D (no read) — data is always read during shuffle.

---

### Q12: B — 2.5 GB per partition — fits in memory but disk spill risk if skew

500 GB / 200 = 2.5 GB per partition. At 64 GB executor memory, this fits — but with skew, one partition could be 20 GB while others are near zero → spill or OOM. The risk is skew and spill, not the partition size itself.

**Why others are wrong:** A (too small) — 2.5 GB per partition is reasonable. C (25 GB per executor) — executor memory allocation is per executor JVM, not a simple division. D (10 tasks per executor) — 200 shuffle partitions / 20 executors = 10, but tasks are distributed across stages, not all running simultaneously.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | What triggers a shuffle |
| 2 | Hard | Shuffle write file count formula |
| 3 | Medium | Shuffle read mechanics |
| 4 | Medium | Shuffle partition sizing |
| 5 | Easy | Spark UI skew diagnosis |
| 6 | Easy | Broadcast which table |
| 7 | Medium | AQE skew join behavior |
| 8 | Easy | Shuffle spill terminology |
| 9 | Medium | Broadcast threshold edge case |
| 10 | Medium | Unnecessary shuffle pattern |
| 11 | Medium | Local vs remote shuffle read |
| 12 | Hard | Shuffle partition sizing reasoning |
