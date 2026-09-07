# Day 1 — Quiz: Exam Strategy + Databricks Architecture + Spark Deep-Dive

**Objective coverage:** These questions test foundational knowledge that underpins Sections 1, 5, 6, and 9 of the exam guide.

**Instructions:** Answer each question before checking the answer key at the bottom. Rate your confidence 1–5 for each question.

---

## Question 1

**Objective:** Understand Spark application hierarchy and stage boundaries.

A DataFrame is created from a 10-partition Parquet file. The following notebook code is executed:

```python
df = spark.read.parquet("/path/to/data")  # 10 partitions
result = df.filter(F.col("status") == "active").groupBy("region").count().collect()
```

How many Spark Jobs are created and how many Stages does the resulting job contain?

A. 1 Job, 1 Stage
B. 1 Job, 2 Stages
C. 2 Jobs, 2 Stages total
D. 1 Job, 3 Stages

---

## Question 2

**Objective:** Distinguish between Spark Tasks and Databricks Job Tasks.

An enterprise data team has configured a Databricks Job with three tasks: Task A (notebook), Task B (notebook, depends on A), Task C (notebook, depends on A). During a job run, Task A and Task B complete successfully, but Task C fails.

Which statement is correct?

A. All changes from Tasks A and B are rolled back automatically due to Task C failure.
B. Task A and Task B changes are committed; Task C changes are not applied.
C. The job run fails and no data is committed to the Lakehouse.
D. Task C would be automatically retried up to 3 times before failing the job.

---

## Question 3

**Objective:** Understand Spark memory model and OOM scenarios.

A Spark job processing a 500 GB dataset fails with an Executor Out of Memory error. Upon investigation in the Spark UI, one executor shows 100% memory usage while all other executors show 40% usage. The failure occurs during a `groupBy` operation.

What is the most likely root cause?

A. The Python worker memory limit is too small for the Python UDF being used.
B. The driver is collecting the entire dataset due to a misconfigured broadcast.
C. Data skew is causing one partition to be significantly larger than others.
D. The shuffle partition count is set too low, causing excessive spill to disk.

---

## Question 4

**Objective:** Understand platform architecture and terminology.

Which of the following best describes the relationship between DBFS and cloud storage on Databricks?

A. DBFS is a separate storage layer that replicates data between Databricks and cloud storage for performance.
B. DBFS is a local file system on each cluster node used for temporary data only.
C. DBFS is a mount layer over cloud storage (S3/ADLS/GCS) that provides a file system interface.
D. DBFS stores data in the Databricks control plane, separate from the customer cloud account.

---

## Question 5

**Objective:** Understand partition behavior for coalesce vs. repartition.

A DataFrame has 100 partitions. A data engineer executes:

```python
df_repart = df.coalesce(200)
print(df_repart.rdd.getNumPartitions())
```

What is the output?

A. 100
B. 200
C. 50
D. An error is raised

---

## Question 6

**Objective:** Distinguish driver OOM from executor OOM.

A notebook cell running a complex aggregation fails with the driver node losing connection to the cluster. Examining the code shows:

```python
large_df = spark.read.parquet("s3://data/large_table")  # 500 GB
summary = large_df.groupBy("category").agg(F.sum("revenue")).collect()
```

What is the most likely cause?

A. One executor ran out of memory during the groupBy shuffle.
B. The Python worker process exceeded its memory limit.
C. The driver ran out of memory trying to collect the entire result set.
D. The cluster manager lost communication with one of the worker nodes.

---

## Question 7

**Objective:** Understand Unity Catalog vs. legacy cluster ACLs.

A data engineer has enabled Unity Catalog on their Databricks workspace. The team has historically used workspace-level ACLs to control access to notebooks and dashboards. A new data scientist is unable to read a table in the analytics schema despite being granted read access to that schema in Unity Catalog.

Which investigation is most likely to reveal the root cause?

A. Check if the data scientist's group is a member of the `developers` group in the workspace ACL settings.
B. Verify that Unity Catalog is enabled on the SQL warehouse the data scientist is using.
C. Confirm the data scientist's cluster has the correct instance profile attached.
D. Check if the table has a row filter defined that excludes the data scientist.

---

## Question 8

**Objective:** Understand Python UDF memory behavior.

A notebook uses a Python UDF to process 50 million rows. The job fails with an executor OOM error, but the Spark UI shows executor memory usage at only 60%. The same operation succeeds when replaced with an identical Pandas UDF.

What is the most likely explanation?

A. The Python UDF is running out of heap space in the JVM executor process.
B. Python UDFs use off-heap memory (python.worker.memory) separate from executor heap, which was exhausted.
C. The Pandas UDF is optimized by Spark's broadcast mechanism.
D. The executor JVM GC was triggered by Python UDF object creation.

---

## Question 9

**Objective:** Apply keyword-driven triage for exam questions.

A scenario-based question describes: a streaming pipeline processing events with a 5-second trigger interval. During peak hours, processing time exceeds 30 seconds, causing a backlog. The engineer needs to reduce processing time without changing the cluster size.

Which concept is most likely being tested?

A. Data skew in the streaming aggregation
B. Shuffle partition misconfiguration
C. Trigger interval and microbatch sizing
D. Watermark window configuration

---

## Question 10

**Objective:** Understand executor failure and task retry behavior.

A Spark job fails after a `groupBy` aggregation. The Spark UI shows that 190 out of 200 tasks in the shuffle map stage completed successfully, but 10 tasks failed. The executor logs show an OOM error on the 10 failing tasks.

Which property controls whether the entire job fails or retries only the failed tasks?

A. `spark.task.maxFailures`
B. `spark.stage.maxAttempts`
C. `spark.sql.adaptive.enabled`
D. `spark.executor.memory`

---

## Answer Key

### Question 1: **Answer B — 1 Job, 2 Stages**

**Why:** The `collect()` action creates one Spark Job. Within that Job:
- **Stage 1:** Reads from Parquet (narrow) + filter (narrow) — no shuffle
- **groupBy** is a wide transformation — it requires a shuffle, creating a new stage boundary
- **Stage 2:** shuffle read + count (aggregate) → result

Therefore: 1 Job, 2 Stages. The initial read and filter stay in Stage 1 because they are narrow transformations.

**Why the other options are wrong:**
- A (1 Job, 1 Stage): Incorrect — `groupBy` always creates a shuffle boundary, requiring a new stage.
- C (2 Jobs, 2 Stages): Incorrect — only one action (`collect()`) was called, so only one Job is created.
- D (1 Job, 3 Stages): Incorrect — there are only 2 operations requiring stages: the read (Stage 1) and the groupBy (Stage 2). No additional shuffle boundaries exist.

**Exam trap:** Students often think the read and write are separate jobs. They are not — they are part of the same Job until an action separates them.

---

### Question 2: **Answer B — Task A and Task B changes are committed; Task C changes are not applied.**

**Why:** This is the exact question from the official sample questions (Question 9). Databricks Jobs do not have transactional job-level commits — each task commits independently upon success. When a downstream task (Task C) fails, prior tasks (A and B) have already committed their results to the Lakehouse. There is no automatic rollback of completed tasks.

**Why the other options are wrong:**
- A (All rolled back): Incorrect — Databricks Jobs do not support ACID-style transaction rollback across tasks. Each task commits independently.
- C (No data committed): Incorrect — this confuses Databricks Job task semantics with Delta Lake transaction semantics. Delta transactions are atomic at the file level, but the Job orchestration layer does not enforce all-or-nothing across task boundaries.
- D (Auto-retry 3 times): Incorrect — task-level retries are controlled by `spark.task.maxFailures` (Spark-level), not Databricks Job task retry settings. Job task retries are configured at the Job level, and the default is not 3.

**Exam trap:** This question tests that you understand the difference between Delta Lake ACID semantics (which are atomic per transaction log entry) and the Job orchestration layer (which is not transactional across task boundaries).

---

### Question 3: **Answer C — Data skew is causing one partition to be significantly larger than others.**

**Why:** The specific clue is "one executor at 100% memory, others at 40%." In a balanced workload, all executors would show similar memory usage. One executor hitting 100% while others are idle points to one partition being dramatically larger than others — classic data skew.

The `groupBy` operation is a common place for skew to manifest: if one key has vastly more rows than others, the partition containing that key will be much larger, causing OOM on the executor that processes it while others sit idle.

**Why the other options are wrong:**
- A (Python worker memory): Possible but would show different symptoms (Python process crash, not executor OOM in the JVM).
- B (Driver collecting data): Driver OOM from collect would show the driver at high memory, not an executor. The scenario explicitly mentions "executor" OOM.
- D (Too few shuffle partitions): This causes spill but not a single executor hitting 100% while others are at 40%. Low shuffle partitions cause all executors to handle more data, not one executor to dominate.

**Exam trap:** The phrase "one executor at 100%, others at 40%" is the specific clue that identifies skew. If all executors were at 80%, the answer would be "partition count too low."

---

### Question 4: **Answer C — DBFS is a mount layer over cloud storage (S3/ADLS/GCS) that provides a file system interface.**

**Why:** DBFS (Databricks File System) is not a separate storage backend — it is a POSIX-like file system abstraction that maps paths to cloud storage locations. When you write to `dbfs:/mnt/data/file.parquet`, the data physically lives in S3/ADLS/GCS. DBFS provides the `/mnt/` mount convention for easy access.

**Why the other options are wrong:**
- A (Separate storage layer replicating data): Incorrect — DBFS does not replicate or cache data as a separate storage layer.
- B (Local file system on cluster nodes): Incorrect — this describes temporary local storage on executors, not DBFS.
- D (Data in control plane): Incorrect — the control plane manages metadata and orchestration, not data storage. Data is always in the customer's cloud account.

**Exam trap:** Questions about cost optimization or data residency often rely on understanding that DBFS = cloud storage. There is no separate "Databricks storage" that costs extra.

---

### Question 5: **Answer A — 100**

**Why:** `coalesce(n)` reduces the number of partitions to `n` only if `n` is less than the current number of partitions. If `n` is greater than the current partition count, `coalesce(n)` returns the original partition count unchanged (no-op). This is because `coalesce` uses **local coalescence** — it cannot increase partition count because doing so would require a global shuffle.

**Why the other options are wrong:**
- B (200): This is the trap answer — it assumes `coalesce` behaves like `repartition` and can increase partition count.
- C (50): This might happen if the query planner decided to coalesce further, but it is not the guaranteed behavior of `coalesce`.
- D (Error): Incorrect — Spark does not raise an error for `coalesce(n)` where `n` > current partitions.

**Exam trap:** This is a frequently-tested behavior. Students confuse `coalesce` with `repartition`. Remember: `repartition` always shuffles (can go up or down). `coalesce` only reduces and only returns the minimum of (n, current partitions).

---

### Question 6: **Answer C — The driver ran out of memory trying to collect the entire result set.**

**Why:** `collect()` is a driver-side action that pulls all data from all executors to the driver JVM. With a 500 GB dataset, the driver must hold all aggregated results in memory. If the result set is too large to fit in driver memory, the driver JVM crashes with an OOM. The scenario explicitly mentions "driver node losing connection" — this is the driver failing.

**Why the other options are wrong:**
- A (Executor OOM during shuffle): Would manifest as an executor failure, not a driver connection loss. The scenario is clear about the driver being the point of failure.
- B (Python worker memory): Python worker memory issues cause Python process failures, not driver JVM OOM.
- D (Cluster manager communication loss): Cluster manager issues would not be preceded by a successful aggregation; the job would fail during scheduling, not during data collection.

**Exam trap:** The key is recognizing that `collect()` on a large dataset is a driver memory hazard, not an executor memory hazard. This is one of the most important exam concepts for memory-related questions.

---

### Question 7: **Answer B — Verify that Unity Catalog is enabled on the SQL warehouse the data scientist is using.**

**Why:** When Unity Catalog is enabled, data access permissions from UC apply. However, if the data scientist is querying via a SQL warehouse (classic capacity) that does not have Unity Catalog enabled, the warehouse falls back to legacy workspace-level metastore permissions, and UC permissions are ignored.

The question states that the engineer "has enabled Unity Catalog" but does not confirm it is enabled on the compute being used. This is a common configuration gap.

**Why the other options are wrong:**
- A (Workspace ACL check): Once UC is enabled, workspace ACLs do not apply to table data access. This is a fundamental architectural change.
- C (Instance profile): Instance profiles relate to cloud authentication for storage access, not UC permission enforcement.
- D (Row filter check): Possible, but the question specifically mentions that the user was "granted read access" in UC — row filters apply after permission checks, so if permission was granted, row filter would be the next step. But the most likely cause of "no access at all" is compute not using UC.

**Exam trap:** When UC is enabled, ALL access to UC-managed data must go through UC. If the compute does not have UC enabled, the data scientist gets legacy metastore permissions, not UC permissions. This is a specific configuration scenario the exam tests.

---

### Question 8: **Answer B — Python UDFs use off-heap memory (python.worker.memory) separate from executor heap, which was exhausted.**

**Why:** Python UDFs (non-Pandas) run in a separate Python process spawned by each executor. This process is **not** part of the executor JVM heap. Its memory is controlled by `spark.python.worker.memory` (default 512MB per Python worker). If the UDF processes more data than fits in that 512MB, the Python worker process exhausts its memory — visible as an executor OOM but caused by the Python subprocess, not the JVM.

Pandas UDFs (via Arrow) avoid this because they process data within the JVM using Arrow-encoded data, not by spawning separate Python processes.

**Why the other options are wrong:**
- A (JVM heap): Incorrect — Python UDF subprocess memory is off-heap and not counted in JVM heap metrics.
- C (Broadcast optimization): Pandas UDFs are not broadcast-optimized in the traditional sense. The improvement is from Arrow serialization, not broadcasting.
- D (JVM GC triggered): JVM GC would show gradual heap pressure, not a sudden OOM when switching from Python UDF to Pandas UDF.

**Exam trap:** Students often think executor OOM means the JVM heap is full. For Python UDFs, the OOM occurs in the Python worker subprocess, not the JVM. The Spark UI may show the executor as "dead" even though the JVM heap looked fine.

---

### Question 9: **Answer C — Trigger interval and microbatch sizing**

**Why:** The scenario explicitly mentions: "5-second trigger interval," "processing time exceeds 30 seconds," and "needs to reduce processing time without changing cluster size." This is a classic Structured Streaming microbatch sizing problem.

When the trigger interval (5 seconds) is shorter than the processing time (30 seconds), microbatches queue up and backlog grows. The trigger interval determines how much data accumulates per microbatch.

**Why the other options are wrong:**
- A (Data skew): Possible in streaming, but the scenario does not mention uneven task durations or one task dominating — it mentions "processing time" for the entire microbatch.
- B (Shuffle partition misconfiguration): Shuffle partitions affect batch processing parallelism, not the trigger backlog issue described here.
- D (Watermark configuration): Watermarks relate to late data handling in event time windows. The scenario does not mention late data or window boundaries.

**Exam trap:** The keyword "trigger interval" is the specific exam signal. This directly maps to Structured Streaming configuration, which is explicitly tested in Section 1 (comparing Structured Streaming with Lakeflow).

---

### Question 10: **Answer B — `spark.stage.maxAttempts`**

**Why:** The question describes 190/200 tasks succeeding and 10 failing within one stage. In modern Spark, stages are retried as a whole (not individual tasks). The property `spark.stage.maxAttempts` controls how many times Spark will retry a failed stage before failing the entire job.

`spark.task.maxFailures` controls per-task retry attempts, but in Spark 3.x, the default behavior is to retry entire stages when tasks fail repeatedly, not individual tasks.

**Why the other options are wrong:**
- A (`spark.task.maxFailures`): This controls how many times an individual task can fail before the stage fails. The default is 4. But once the stage fails (after task retries exhausted), the job-level retry policy applies, not the task-level policy.
- C (`spark.sql.adaptive.enabled`): AQE is an optimization feature, not a retry mechanism.
- D (`spark.executor.memory`): This is the memory configuration, not a retry policy.

**Exam trap:** Spark 3.x changed stage retry behavior significantly. Students often default to thinking "task retries = job retries." In practice, Spark retries failed stages by rescheduling all tasks in the stage. The property `spark.stage.maxAttempts` governs this behavior.
