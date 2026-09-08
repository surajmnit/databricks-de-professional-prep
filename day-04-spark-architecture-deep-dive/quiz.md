# Day 4 — Quiz: Spark Architecture Deep-Dive

**Objective coverage:** Section 6 (13%) and Section 5 (10%) — foundational for performance reasoning.

---

## Question 1

**Objective:** Understand the Spark application hierarchy.

A notebook executes the following code:

```python
df = spark.read.parquet("/data/")           # 10 partitions
df2 = df.filter(F.col("status") == "active")
df3 = df2.groupBy("region").count()
result = df3.collect()
```

How many Spark Jobs are created and how many Stages does the resulting job contain?

A. 4 Jobs, 1 Stage each
B. 1 Job, 2 Stages
C. 1 Job, 3 Stages
D. 3 Jobs, 3 Stages

---

## Question 2

**Objective:** Distinguish narrow from wide transformations.

Which of the following transformations does NOT create a new Stage boundary?

A. df.join(df2, "key") — joining with another DataFrame
B. df.filter(F.col("status") == "active")
C. df.repartition(100)
D. df.sort("date")

---

## Question 3

**Objective:** Understand driver OOM patterns.

A notebook runs a query that performs a complex aggregation and then calls collect() on a 200 GB dataset. The driver node loses connection and the job fails.

Which investigation is most likely to confirm the root cause?

A. Check executor logs for OOM errors in the JVM heap
B. Check the Spark UI to see if all executors show 100% memory usage
C. Check if spark.driver.maxResultSize is set to a value smaller than the result set
D. Check if spark.sql.shuffle.partitions is set too low for the data volume

---

## Question 4

**Objective:** Understand partition-to-task mapping.

A DataFrame has 50 partitions. A groupBy aggregation is executed. The default shuffle partition count (200) is in effect.

How many tasks run in the shuffle map stage?

A. 50 (one per original partition)
B. 200 (shuffle partitions)
C. 250 (50 original + 200 shuffle)
D. It depends on the physical plan

---

## Question 5

**Objective:** Understand executor heartbeat and failure.

An executor stops sending heartbeats to the driver. The driver marks it as lost after 30 seconds and reschedules its tasks on other executors.

Which configuration property controls the 30-second threshold?

A. spark.executor.heartbeatInterval (10s) and implicit multiplier of 3
B. spark.task.maxFailures * spark.executor.heartbeatInterval
C. spark.stage.maxAttempts * spark.executor.heartbeatInterval
D. spark.network.timeout * spark.executor.heartbeatInterval

---

## Question 6

**Objective:** Understand coalesce behavior.

A DataFrame has 100 partitions. A data engineer calls:

```python
df_result = df.coalesce(500)
print(df_result.rdd.getNumPartitions())
```

What is the output?

A. 100
B. 500
C. 400
D. An error is raised

---

## Question 7

**Objective:** Understand data skew and executor OOM.

A Spark job fails with an executor OOM. Investigation shows one executor is at 100% memory usage while all other executors are at 30%. The failure occurred during a groupBy operation.

Which statement is most accurate?

A. Python UDF subprocess memory was exhausted
B. The groupBy caused a shuffle of 100 GB, exceeding executor memory
C. Data skew is causing one partition to be much larger than others
D. The spark.sql.shuffle.partitions was set too low

---

## Question 8

**Objective:** Understand executor.cores configuration.

A cluster has 4 worker nodes, each running 1 executor with 4 cores. spark.executor.cores is set to 4.

How many concurrent tasks can run on this cluster?

A. 4 (one per node)
B. 8 (one per executor but core-limited)
C. 16 (4 cores * 4 executors)
D. 1 (default behavior)

---

## Question 9

**Objective:** Understand dynamic allocation.

A Spark job with dynamic allocation enabled starts with 2 executors. The workload increases and Spark adds more executors up to the maximum of 10. As the workload decreases, executors are released.

Which property determines the maximum number of executors that can be added?

A. spark.dynamicAllocation.initialExecutors
B. spark.dynamicAllocation.maxExecutors
C. spark.dynamicAllocation.minExecutors
D. spark.executor.instances

---

## Question 10

**Objective:** Diagnose driver vs executor OOM from failure mode.

A production Spark job fails with the message "Lost task 0.0 in stage 2.0 (TID 0)". The error logs show a Java heap space error.

Which investigation first determines whether this is a driver or executor issue?

A. Check spark.executor.memory configuration
B. Check which node the task was assigned to and its memory usage
C. Check if spark.driver.maxResultSize is configured
D. Check if the query uses collect()

---

## Question 11

**Objective:** Understand shuffle partition configuration.

A data engineer processing a 1 TB dataset sets spark.sql.shuffle.partitions to 50. The job runs but some executors hit OOM while others are underutilized.

Which explanation is most accurate?

A. 50 partitions at 1 TB = 20 GB per partition, exceeding executor memory
B. 50 is too few for parallel processing; the shuffle write buffer fills up
C. The number of shuffle partitions should equal the number of executors
D. spark.sql.files.maxPartitionBytes overrides shuffle partition settings

---

## Question 12

**Objective:** Understand task execution flow.

A task on executor-2 fails with a serialization error. Spark retries the same task on executor-3. After executor-3 also fails, Spark retries all tasks in the stage.

Which property controls how many times the entire stage is retried?

A. spark.task.maxFailures
B. spark.stage.maxAttempts
C. spark.executor.failures
D. spark.driver.maxRetries

---

## Answer Key

### Q1: B — 1 Job, 2 Stages

The single collect() action creates one Job. Within that Job: read + filter are narrow transformations (Stage 0). groupBy is a wide transformation requiring a shuffle (Stage 1 boundary). Result Stage runs after the shuffle.

**Exam trap:** Many students answer "3 Stages" (read, filter, groupBy). But read and filter are in the same Stage — they are narrow transformations with no shuffle boundary between them.

---

### Q2: B — filter()

filter() is a narrow transformation — it processes each partition independently without moving data across the network. No new Stage.

**Why the others are wrong:** join(), repartition(), and sort() all require data to be shuffled across executors, creating a new Stage boundary.

---

### Q3: C — Check spark.driver.maxResultSize

collect() forces the driver to hold all result data in JVM heap. The property spark.driver.maxResultSize (default 1GB) caps this. If the result set exceeds this, the driver OOMs.

**Why the others are wrong:** A (executor logs) would show executor OOM, not driver. B (all executors at 100%) describes uniform load, not a driver failure. D (shuffle partitions) affects parallelism, not driver memory for collect().

---

### Q4: A — 50 (one per original partition)

The shuffle map stage (Stage 0) has one task per input partition. The 200 shuffle partition count applies to the output of the shuffle (the map stage output), not to the input. In this case: 50 input partitions = 50 tasks in the shuffle map stage.

**Why the others are wrong:** B (200) is the shuffle partition count but this controls output of the shuffle, not input task count. C (250) adds input and shuffle partitions — these are different stages, not additive. D is not correct for this specific question.

---

### Q5: A — spark.executor.heartbeatInterval (10s) and implicit multiplier of 3

Executors send heartbeats every spark.executor.heartbeatInterval (default 10s). If 3 consecutive heartbeats are missed (3 * 10s = 30s), the driver marks the executor as lost.

**Why the others are wrong:** B (task.maxFailures * heartbeat) is task-level, not executor-level. C (stage.maxAttempts * heartbeat) is stage-level. D (network.timeout) is a separate property.

---

### Q6: A — 100

coalesce(n) where n > current partition count returns the current partition count unchanged. It is a no-op, not an error. coalesce() uses local coalescence — it cannot increase partition count because that would require a global shuffle.

**Exam trap:** This is a frequently tested behavior. Students confuse coalesce with repartition. Remember: repartition can go up or down (shuffle). coalesce can only go down (no shuffle) and returns current count if n > current.

---

### Q7: C — Data skew is causing one partition to be much larger than others

The specific signature is one executor at 100% while others are at 30%. This is the textbook definition of data skew. One key (or set of keys) dominated one partition, causing that executor's memory to fill while others sat idle.

**Why the others are wrong:** A (Python UDF) would show Python process failure, not executor JVM OOM. B (100 GB shuffle) is possible but the pattern description (one high, others low) points to skew, not volume. D (shuffle partitions too low) causes spill but not one-executor-at-100% while others idle.

---

### Q8: C — 16 (4 cores * 4 executors)

spark.executor.cores = 4 means each executor can run 4 tasks concurrently. With 4 executors: 4 * 4 = 16 concurrent tasks.

**Why the others are wrong:** A (4) ignores executor.cores. B (8) is wrong arithmetic. D (1) would be true if executor.cores was not set.

---

### Q9: B — spark.dynamicAllocation.maxExecutors

This property sets the ceiling for dynamic allocation. Spark will not add more than maxExecutors, regardless of workload.

**Why the others are wrong:** A (initialExecutors) sets the starting count, not the maximum. C (minExecutors) sets the floor, not the ceiling. D (executor.instances) is a YARN property, not Spark dynamic allocation.

---

### Q10: B — Check which node and memory usage

"Lost task" errors come from executors, not the driver. To determine if this is an executor OOM (common) vs a driver OOM (rare for "Lost task" errors), check which node the task ran on and its memory state.

**Why the others are wrong:** A (spark.executor.memory) is the config, not the diagnostic. C (driver.maxResultSize) applies to collect(), not "Lost task" errors. D (collect()) is already ruled out by the "Lost task" message — collect() failures say "driver" not "Lost task."

---

### Q11: A — 20 GB per partition exceeds executor memory

1 TB / 50 partitions = 20 GB per partition. If executor memory is, say, 16 GB, each partition exceeds available memory, causing OOM.

**Why the others are wrong:** B (too few for parallelism) describes performance, not OOM. C (should equal executors) is a myth — shuffle partitions should be sized for data volume, not cluster size. D (maxPartitionBytes) controls read partitioning, not shuffle partitioning.

---

### Q12: B — spark.stage.maxAttempts

When a task fails after all retries, Spark retries the entire stage (all tasks in the stage) up to spark.stage.maxAttempts times (default 4).

**Why the others are wrong:** A (spark.task.maxFailures) controls per-task retries, not per-stage. C (spark.executor.failures) is not a standard property. D (spark.driver.maxRetries) is not a standard property.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Application/Job/Stage/Task hierarchy |
| 2 | Easy | Narrow vs wide transformations |
| 3 | Medium | Driver OOM from collect() |
| 4 | Hard | Shuffle map stage task count vs shuffle partition count |
| 5 | Medium | Executor heartbeat and loss detection |
| 6 | Easy | coalesce cannot increase partitions |
| 7 | Medium | Data skew OOM signature (one high, others low) |
| 8 | Easy | executor.cores concurrency calculation |
| 9 | Easy | Dynamic allocation maxExecutors |
| 10 | Hard | "Lost task" error interpretation |
| 11 | Hard | Shuffle partition sizing for data volume |
| 12 | Medium | spark.stage.maxAttempts vs spark.task.maxFailures |
