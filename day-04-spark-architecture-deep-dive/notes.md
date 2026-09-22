# Day 4 — Spark Architecture Deep-Dive

## Exam Objectives

This day is the foundation for **Section 6: Cost & Performance Optimization** and **Section 5: Monitoring and Alerting** of the Databricks Data Engineer Professional exam. (Check the current official exam guide for exact section names and weights, since they get revised.)

The exam expects you to reason from first principles about why a driver OOM differs from an executor OOM, what a specific Spark UI metric means in context, and why a configuration causes a specific failure pattern.

Day 4 focuses on architecture (how Spark is structured). Days 5-8 drill into execution, shuffle, memory, and monitoring.

---

## Part 1 — The Full Spark Application Stack

### Layer-by-Layer Anatomy

**1. User Code**
```python
result = (spark.read.parquet('/data/')
    .filter(F.col('status') == 'active')
    .groupBy('region')
    .count()
    .collect())
```
- In **PySpark**, your Python code runs in a **Python process** on the driver node. It talks to the driver **JVM** through **Py4J**. DataFrame/SQL operations are executed by the JVM; Python code only runs on the JVM's behalf via Py4J calls.
- Python **UDFs** are the exception: they run in separate **Python worker processes on the executors**, with data moving between JVM and Python (Arrow/pickle serialization).
- Scala/Java code runs directly in the driver JVM.

**2. Driver (SparkContext / SparkSession)**
The entry point and coordinator of the application. It:
- Turns your DataFrame/SQL code into a plan (Catalyst builds the logical -> optimized -> physical plan)
- Builds the RDD lineage and the **stage DAG** (DAGScheduler splits it at shuffle boundaries)
- Requests executors from the Cluster Manager
- Schedules tasks (TaskScheduler) and **sends them directly to executors**
- Tracks task status, retries failures, and collects results of actions
- Hosts the Spark UI (default port 4040 in OSS; on Databricks, reach it through the cluster/compute UI)

**3. Cluster Manager**
Only **allocates resources** (containers/nodes for executors). It does not receive your code and does not schedule tasks.

**On Databricks, this is fully abstracted away** — you choose a cloud (AWS/Azure/GCP), node types, and autoscaling limits, and Databricks provisions and manages the driver/executor machines itself. You never configure or select an OSS cluster manager (Standalone/YARN/Kubernetes/Mesos) on Databricks.

**Exam trap:** the exam tests **Spark execution behavior** (Driver/Executor/Stage/Task, memory, shuffle, skew), not which cluster-manager daemon runs underneath Databricks. Don't confuse unrelated products (e.g. Azure HDInsight is a separate Azure service, not part of Databricks).

**4. Executors (Worker Nodes)**
Each executor is a JVM process that:
- Registers with the driver on startup
- Receives serialized tasks **from the driver** and runs them against assigned partitions
- Caches data (storage memory) and holds shuffle data on local disk
- Sends **heartbeats to the driver** (every 10s by default) reporting liveness and task metrics

**On Databricks**, each worker node typically runs **one executor that uses all of that node's cores**, so you size compute by choosing node type and worker count rather than tuning executor layout.

**5. Data Storage**
Executors read/write directly from cloud storage (S3/ADLS/GCS). The driver plans and coordinates but does not process data, except when you pull results to it (`collect()`, `toPandas()`, broadcast build side, etc.).

### Data Flow

```
Your code
  -> Driver: Catalyst plan -> RDD DAG -> stages (DAGScheduler)
  -> Driver asks Cluster Manager for executors
  -> Cluster Manager launches executors; they register with the driver
  -> Driver (TaskScheduler) sends tasks DIRECTLY to executors
  -> Executors read/write cloud storage; report status/results to driver
```

**Critical insight:** Executor failures are recoverable (failed tasks are retried, up to `spark.task.maxFailures`). A driver failure kills the whole application.

---

## Part 2 — Application / Job / Stage / Task Hierarchy

### Jobs

- A Job is created each time an **Action** runs: `collect()`, `count()`, `write`, `take()`, etc.
- Jobs are independent, not nested — multiple actions = multiple Jobs.

**Exam trap:** a multi-stage query is normally ONE Job. The `groupBy` in the middle creates a new **Stage**, not a new Job.

**Nuance:** one action can occasionally trigger extra jobs, e.g. `sort`/`orderBy` runs a small sampling job to compute range boundaries, and `take(n)` may scan partitions incrementally in several jobs.

### Stages

A Stage is a set of tasks that can run without a shuffle between them. Stages are delimited by **shuffle (wide dependency) boundaries**.

| Operation | New Stage? |
|---|---|
| filter() | No (narrow) |
| withColumn() | No (narrow) |
| select() | No (narrow) |
| groupBy().agg() | Yes (shuffle) |
| join() — sort-merge / shuffle-hash | Yes (shuffle) |
| join() — **broadcast hash join** | **No shuffle** for the broadcast join itself |
| repartition(n) / repartition(col) | Yes (shuffle) |
| distinct() | Yes (shuffle) |
| coalesce(n), n < current | No (narrow) |
| coalesce(n), n > current | No-op, keeps current count (not an error) |
| sort() / orderBy() | Yes (range-partitioning shuffle) |

**Note on AQE:** with Adaptive Query Execution, the plan is re-optimized at runtime. It can switch a sort-merge join to a broadcast join, coalesce shuffle partitions, and split skewed partitions. The stage boundaries you predict from the code may differ from what actually runs.

### Tasks

- Smallest unit of parallel work
- One Task processes **one Partition** of that stage
- Number of tasks in a stage = number of partitions of the stage's final RDD
- Tasks run concurrently up to the number of available **slots** (total executor cores); extra tasks queue

---

## Part 3 — Driver Deep-Dive

### What the Driver Does

1. Initializes the SparkSession / SparkContext
2. Plans the query (Catalyst) and builds the stage DAG
3. Requests executors via the Cluster Manager
4. Schedules tasks onto executors and tracks them
5. Receives results of actions (e.g. `collect()`)
6. Hosts the Spark UI

### Driver Failure Modes

| Failure Type | Cause | Impact |
|---|---|---|
| Driver JVM OOM | `collect()`/`toPandas()` of large results; large broadcast build; very many tasks/partitions (task metadata) | Application fails or driver becomes unresponsive |
| `maxResultSize` exceeded | Total serialized task results larger than `spark.driver.maxResultSize` | Job aborted with a `SparkException` (a config guard, not an OOM) |
| Driver process killed | OS OOM killer, node failure | Application terminates |
| Executor lost | No heartbeat from executor within `spark.network.timeout` | Driver marks executor lost and reschedules its tasks |

### Driver OOM — Common Scenarios

```python
# Scenario 1: collect() of a large dataset
result = df.collect()  # Driver holds ALL rows in its heap

# Scenario 2: Broadcasting a large table
# The driver first COLLECTS the entire broadcast table into its own heap,
# then ships it to executors. A big build side therefore OOMs the driver.
df_large.join(broadcast(df_big), 'key')

# Scenario 3: Large aggregated result pulled to the driver
df.groupBy('category').agg(collect_list('text_column')).collect()

# Scenario 4: High-cardinality groupBy result collected
df.groupBy('high_cardinality_col').count().collect()
```

### Driver Memory Configuration

| Property | Default | Purpose |
|---|---|---|
| spark.driver.memory | Set by cluster/node type on Databricks | Driver JVM heap |
| spark.driver.maxResultSize | 1g in OSS (Databricks may set a different default) | Cap on total serialized results returned to the driver; exceeding it aborts the job |
| spark.driver.cores | 1 (OSS) | Driver core count |
| spark.driver.memoryOverhead | max(driverMemory × 0.10, 384MB) | Non-heap memory (native, Python, etc.) |

These are launch-time settings. On Databricks, driver sizing mainly comes from choosing the driver node type.

**Exam traps:**
- `maxResultSize` and driver OOM are **different failures**. Exceeding `maxResultSize` gives an explicit "serialized results ... bigger than spark.driver.maxResultSize" error. A true driver OOM needs a larger driver or less data collected.
- Raising `maxResultSize` only moves the limit and can make a real OOM **more** likely. The real fix is to not collect large results: aggregate/filter first, or write to storage.

---

## Part 4 — Executor Deep-Dive

### Executor Startup

1. Cluster Manager launches the executor JVM on a worker node
2. Executor registers with the driver (host, cores, memory)
3. Driver adds it to its executor registry
4. Executor begins receiving tasks

### Heartbeat and Timeout

- Executors send heartbeats to the driver every `spark.executor.heartbeatInterval` (default **10s**)
- If the driver hears nothing for `spark.network.timeout` (default **120s**), it marks the executor as **lost** and reschedules its tasks
- `spark.executor.heartbeatInterval` must be significantly smaller than `spark.network.timeout`

| Property | Default |
|---|---|
| spark.executor.heartbeatInterval | 10s |
| spark.network.timeout | 120s |
| spark.task.maxFailures | 4 (task retries before the stage/job fails) |
| spark.stage.maxConsecutiveAttempts | 4 |

### Executor OOM — Common Scenarios

```python
# Scenario 1: A few very large partitions
# Typical causes: too few shuffle partitions for the data volume,
# an over-aggressive coalesce(), or non-splittable inputs (e.g. large gzip CSV).
# Parquet reads themselves are split by size, so they rarely produce this alone.
df.coalesce(2).write...   # 100GB into 2 partitions -> ~50GB per task -> OOM

# Scenario 2: Data skew
# One key dominates -> one task gets a huge partition while others are tiny

# Scenario 3: Heavy per-task memory use
# Large explodes, wide rows, big collect_list groups, or large broadcast
# variables deserialized on each executor

# Scenario 4: Python UDF / pandas UDF memory
# Python workers run outside the JVM heap. Excessive Python memory gets the
# container killed (governed by memoryOverhead / spark.executor.pyspark.memory).
# spark.python.worker.memory (512m default) is a SPILL THRESHOLD for
# Python-side aggregation, not a hard limit.
```

### Cache and Memory Pressure

- DataFrame `cache()` defaults to **memory-and-disk**, and storage memory is **evictable**.
- Cache pressure usually appears as eviction, recomputation, spill, and GC pressure, not as a clean OOM. It can contribute to an OOM when execution memory cannot reclaim enough space.
- Full memory model (unified memory, storage vs execution, GC, spill) is covered on Day 7.

### Driver vs Executor OOM — Decision Tree

**Who failed?**
- Driver failed -> `collect()`/`toPandas()`, large broadcast, huge task counts, oversized results
- Executor failed -> partition size, skew, per-task memory, Python UDF memory

**One executor or all?**
- One task/executor much heavier than the rest -> data skew
- All executors under memory pressure -> partitions too large overall (too few partitions) or memory-heavy operations

**What operation was running?**
- Shuffle read/write -> shuffle partition count, data size, skew
- Python UDF -> Python worker / container overhead memory
- `collect()` -> driver memory or `maxResultSize`

---

## Part 5 — Partitioning in Depth

### Partition Count by Operation

| Operation | Resulting partitions |
|---|---|
| spark.read.parquet(path) | Determined by size: files are split/packed based on `spark.sql.files.maxPartitionBytes` (128MB default), `spark.sql.files.openCostInBytes`, and default parallelism. Large files split; small files are packed together. |
| spark.range(n) | `n` is the **row count**, not the partition count. Partitions default to default parallelism (or set `numPartitions`). |
| spark.createDataFrame(local_data) | Based on default parallelism (usually total cores) |
| df.repartition(n) | Exactly n partitions (full shuffle, round-robin) |
| df.repartition(col) | Hash-partitioned by the column into `spark.sql.shuffle.partitions` partitions. **Not** one partition per value; rows with the same key land together. |
| df.coalesce(n), n < current | Reduces to n partitions (no full shuffle) |
| df.coalesce(n), n > current | No-op; keeps current count (not an error) |
| df.groupBy(col).agg(...) | Output uses `spark.sql.shuffle.partitions` (default 200), subject to AQE coalescing |

### Shuffle Partitions

`spark.sql.shuffle.partitions` (default 200) controls the number of partitions after shuffles:
```python
spark.conf.set('spark.sql.shuffle.partitions', 400)  # SQL confs can be set at runtime
```
With **AQE** enabled (default in recent Spark/Databricks), small shuffle partitions are coalesced at runtime, so the final number may be lower than the configured value.

### Target Partition Size

- **Target: roughly 128MB-256MB per partition**
- 1TB at a 128MB target = ~8,192 partitions (1TB = 1,048,576MB)

**Exam trap:** `spark.sql.shuffle.partitions` affects only shuffles, not the initial read. To change read partitioning, use `spark.sql.files.maxPartitionBytes` or `repartition()` after the read.

---

## Part 6 — Dynamic Allocation, Autoscaling, and Executor Concurrency

### Dynamic Allocation (OSS Spark)

```
spark.dynamicAllocation.enabled          true
spark.dynamicAllocation.minExecutors     1
spark.dynamicAllocation.maxExecutors     10
spark.dynamicAllocation.initialExecutors 2
```
These are **launch-time** settings (set in cluster/spark-submit config). Calling `spark.conf.set(...)` on a running session does not enable dynamic allocation.

**On Databricks**, use **cluster autoscaling** (min/max workers in the compute configuration) rather than configuring Spark dynamic allocation yourself.

### Executor Cores and Slots

`spark.executor.cores` sets how many tasks can run concurrently **inside one executor**.

- Total task slots = number of executors × cores per executor
- Example: 4 executors × 4 cores = 16 concurrent tasks
- On OSS YARN/Kubernetes the default is 1 core per executor (on Standalone it defaults to all available cores). On Databricks, one executor per worker uses all of the worker's cores.

**Exam trap:** more partitions than slots just means tasks run in waves; more slots than partitions means idle cores.

---

## Part 7 — Key Exam Patterns

| Scenario | Diagnosis | Fix |
|---|---|---|
| One slow task, others fast | Data skew | AQE skew-join handling, salting the hot key, broadcast the small side; isolate/handle the hot key |
| One executor much busier than others | One partition much larger than the rest | Fix the skew (salting/AQE). **Do not** hash-repartition by the skewed column, since that puts all of that key in one partition. Round-robin `repartition(n)` only helps if the imbalance is from uneven partition sizes, not a dominant key. |
| Driver unresponsive / job fails after a large `collect()` | Driver OOM or `maxResultSize` exceeded | Don't collect large results; aggregate first or write to storage; then size the driver if needed |
| Works on small data, OOM on large | Partitions too large (too few partitions) | Increase shuffle partitions, `repartition()` after read, lower `maxPartitionBytes`; check for non-splittable input |
| Many executors spill/fail with memory errors | Partitions too big overall, or memory-heavy operations | Increase partition count; reduce per-task data; right-size memory/node type; check UDF memory |
| Executor marked lost | No heartbeat within `spark.network.timeout` (long GC pause, node loss, OOM kill) | Investigate GC/memory, node health; check executor logs |

"Increase partitions" helps when partitions are uniformly too large. It does **not** fix skew from a single dominant key.

---

## Cross-References

- **Day 5:** DAG formation, lazy evaluation, physical execution plan
- **Day 6:** Shuffle at stage boundaries
- **Day 7:** Full memory model, GC, spill
- **Day 8:** Spark UI + Query Profile visual inspection
- **Day 1:** Platform architecture overview
- **Day 23:** Debugging with Spark UI and cluster logs
