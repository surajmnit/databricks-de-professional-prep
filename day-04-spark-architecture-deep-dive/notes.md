# Day 4 — Spark Architecture Deep-Dive

> ⚠️ **Correction notice:** This version fixes one issue found in an earlier draft: the Cluster Manager table incorrectly associated Azure HDInsight (an unrelated Azure big-data service, not part of Databricks) with Databricks, and implied GCP Databricks specifically uses Kubernetes as an exam-testable fact. Both are corrected below, consistent with the same fix already applied in Day 1.

## Exam Objectives

This day is the foundation for **Section 6: Cost & Performance Optimization (13%)** and **Section 5: Monitoring and Alerting (10%)**.

The exam expects you to reason from first principles about why driver OOM vs executor OOM occurs, what a specific Spark UI metric means in context, and why a configuration causes a specific failure pattern.

Day 4 focuses on architecture (how Spark is structured). Days 5-8 drill into execution, shuffle, memory, and monitoring.

---

## Part 1 — The Full Spark Application Stack

### Layer-by-Layer Anatomy

**1. User Code (Driver JVM)**
```python
result = (spark.read.parquet('/data/')
    .filter(F.col('status') == 'active')
    .groupBy('region')
    .count()
    .collect())
```
Python/SQL code running in the driver JVM.

**2. SparkContext (Driver)**
The entry point. It:
- Serializes user code and sends it to the Cluster Manager
- Builds the DAG of transformations
- Requests executors from the Cluster Manager
- Coordinates task scheduling
- Hosts the Spark UI at driver:4040

**3. Cluster Manager**
Allocates containers for executors on cluster nodes. **On Databricks, this is fully abstracted away** — you choose a cloud (AWS/Azure/GCP) and node type, and Databricks provisions and manages the underlying driver/executor containers itself. You never configure or select a specific OSS cluster manager (Standalone/YARN/Kubernetes/Mesos) on Databricks, and there is no reliable one-to-one mapping between a given cloud and a specific OSS cluster manager exposed to you.

**Exam trap:** don't assume "GCP Databricks uses Kubernetes" as an exam-testable fact, and don't confuse unrelated products — Azure HDInsight is a separate Azure big-data service, not part of Databricks at all. The exam tests your understanding of **Spark execution behavior** (Driver/Executor/Stage/Task, memory, shuffle), not which literal cluster-manager daemon runs underneath Databricks' compute layer.

**4. Executors (Worker Nodes)**
Each executor is a JVM that:
- Receives serialized task bytecode from the driver
- Runs tasks against assigned data partitions
- Stores results in memory or disk
- Reports heartbeat to driver every 10s (default)

**5. Data Storage**
Executors read/write directly from cloud storage (S3/ADLS/GCS). The driver orchestrates but does not touch data (except for collect()).

### Data Flow

```
User Code (Driver JVM)
  -> SparkContext builds DAG
  -> Cluster Manager requests executors
  -> Executors read/write cloud storage directly
```

**Critical insight:** The driver does NOT touch data directly (except for collect()). Executor failures are retried safely. Driver failures crash the application.

---

## Part 2 — Application / Job / Stage / Task Hierarchy

### Jobs

- Created every time an **Action** is called: collect(), count(), write(), take()
- Each action = one independent Job
- **Jobs are NOT nested** — multiple actions = multiple independent Jobs

**Exam trap:** A multi-stage query is ONE Job. The groupBy in the middle creates a new Stage, not a new Job.

### Stages

A Stage is a set of tasks that can run without a shuffle between them. Stages are delimited by **shuffle boundaries**.

| Operation | New Stage? |
|---|---|
| filter() | No (narrow) |
| withColumn() | No (narrow) |
| select() | No (narrow) |
| groupBy().count() | YES (shuffle) |
| join() | YES (shuffle) |
| repartition(n) | YES (shuffle) |
| distinct() | YES (shuffle) |
| coalesce(n) where n < current | No (narrow) |
| coalesce(n) where n > current | Returns current (no-op) |
| sort() | YES (shuffle) |

### Tasks

- Smallest unit of parallel work
- One Task processes **one Partition**
- Tasks run in parallel across all available executors
- Task count = sum of partitions in the current Stage

---

## Part 3 — Driver Deep-Dive

### What the Driver Does

1. Initializes SparkContext
2. Builds the DAG (logical plan)
3. Requests executors from Cluster Manager
4. Schedules tasks onto executors
5. Collects results from final Stage
6. Hosts Spark UI at driver:4040

### Driver Failure Modes

| Failure Type | Cause | Impact |
|---|---|---|
| Driver JVM OOM | collect() of large result; large broadcast; large groupBy output | Application crashes |
| Driver process killed | OOM Killer, node failure | Application terminates |
| Driver heartbeat loss | Network partition | CM marks driver as lost |

### Driver OOM — Common Scenarios

```python
# Scenario 1: collect() of large dataset
result = df.collect()  # Driver holds ALL data in JVM heap

# Scenario 2: Broadcasting a large table
df_large.join(broadcast(df_big), 'key')  # Driver receives broadcast metadata

# Scenario 3: Large groupBy result
df.groupBy('category').agg(collect_list('text_column')).collect()

# Scenario 4: High cardinality groupBy with collect()
df.groupBy('high_cardinality_col').count().collect()
```

### Driver Memory Configuration

| Property | Default | Purpose |
|---|---|---|
| spark.driver.memory | Cluster config | Driver JVM heap |
| spark.driver.maxResultSize | 1GB | Max collect() result size |
| spark.driver.cores | 1 | Driver core count |
| spark.driver.memoryOverhead | driverMemory * 0.1, min 384MB | Off-heap memory |

**Exam trap:** spark.driver.memory is JVM heap only. Off-heap memory is additional. collect() OOM fix: increase maxResultSize or reduce collected data volume.

---

## Part 4 — Executor Deep-Dive

### Executor Startup

1. Cluster Manager launches executor JVM on worker node
2. Executor registers with driver (hostname, cores, memory)
3. Driver adds executor to registry
4. Executor begins receiving tasks

### Heartbeat and Timeout

- Heartbeats every `spark.executor.heartbeatInterval` (default 10s)
- 3 missed heartbeats (30s default) = executor marked as lost
- Lost tasks rescheduled on other executors

| Property | Default |
|---|---|
| spark.executor.heartbeatInterval | 10s |
| spark.task.maxFailures | 4 |
| spark.stage.maxAttempts | 4 |

### Executor OOM — Common Scenarios

```python
# Scenario 1: Too few partitions, each too large
df = spark.read.parquet('/data/')  # 2 partitions for 100GB -> 50GB per partition -> OOM

# Scenario 2: Cache bloat
df.cache().count()  # All data in storage memory; subsequent ops may OOM

# Scenario 3: Python UDF memory
# Python subprocess has spark.python.worker.memory (512MB default)
# Too many Python objects exhausts this -> OOM on Python process, NOT JVM
```

### Driver vs Executor OOM — Decision Tree

**Who failed?**
- Driver failed -> collect(), broadcast, groupBy result size
- Executor failed -> partition size, cache, Python UDF

**One executor or all?**
- One executor at 100%, others idle -> data skew
- All executors at high memory -> partition count too low OR cache bloat

**What operation was running?**
- shuffle write/read -> shuffle partition count, data size
- Python UDF -> Python worker memory
- collect() -> driver memory

---

## Part 5 — Partitioning in Depth

### Partition Count by Operation

| Operation | Effect on Partitions |
|---|---|
| spark.read.parquet(path) | One partition per file (or controlled by maxPartitionBytes) |
| spark.range(n) | n partitions |
| spark.createDataFrame(data) | Default parallelism (num cores) or 1 on CE |
| df.repartition(n) | Exactly n partitions (triggers shuffle) |
| df.repartition(col) | Partition by column value (shuffle) |
| df.coalesce(n) where n < current | Reduce to n partitions (no shuffle) |
| df.coalesce(n) where n > current | Returns current partition count (no-op, NOT an error) |
| df.groupBy(col).count() | Shuffle uses shuffle partitions (default 200) |

### Default Parallelism

spark.sql.shuffle.partitions (default 200) controls shuffle partition count:
```python
df.groupBy('key').count()  # Uses 200 shuffle partitions
spark.conf.set('spark.sql.shuffle.partitions', 400)  # Override
```

### Target Partition Size

- **Target: 128MB-256MB per partition**
- 1TB dataset at 128MB target = ~8,192 partitions

**Exam trap:** spark.sql.shuffle.partitions only affects shuffles, not initial read. To change read partitioning: spark.sql.files.maxPartitionBytes or repartition() after read.

---

## Part 6 — Dynamic Allocation and Executor Concurrency

### Dynamic Allocation

```python
spark.conf.set('spark.dynamicAllocation.enabled', True)
spark.conf.set('spark.dynamicAllocation.minExecutors', 1)
spark.conf.set('spark.dynamicAllocation.maxExecutors', 10)
spark.conf.set('spark.dynamicAllocation.initialExecutors', 2)
```

### Executor Cores

spark.executor.cores controls concurrent tasks per executor (default 1):
- 4-executor cluster with executor.cores=4 = 16 concurrent tasks total

**Exam trap:** executor.cores does not affect total cluster cores — it controls intra-executor parallelism.

---

## Part 7 — Key Exam Patterns

| Scenario | Diagnosis | Fix |
|---|---|---|
| One slow task, others fast | Data skew | Salt join, increase partitions, AQE |
| One executor at 100%, others at 40% | One partition much larger | Repartition by skewed column |
| Driver connection lost on large job | Driver OOM from collect() | Don't collect large results; write to storage |
| Works on small data, OOM on large | Partition count too low | Increase shuffle partitions; repartition after read |
| All executors OOM simultaneously | Cache bloat or all partitions too large | Unpersist; increase partitions; reduce cache level |

---

## Cross-References

- **Day 5:** DAG formation, lazy evaluation, physical execution plan
- **Day 6:** Shuffle at stage boundaries
- **Day 7:** Full memory model, GC, spill
- **Day 8:** Spark UI + Query Profile visual inspection
- **Day 1:** Platform architecture overview
- **Day 23:** Debugging with Spark UI and cluster logs
