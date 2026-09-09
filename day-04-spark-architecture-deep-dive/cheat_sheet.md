# Day 4 — Cheat Sheet: Spark Architecture Deep-Dive

## Application Hierarchy

```
SparkApplication (SparkContext)
  -> Job (one per ACTION: collect(), count(), write(), take())
      -> Stage 0 (no shuffle boundary)
      -> Stage 1 (shuffle boundary — groupBy, join, repartition, sort, distinct)
      -> Stage 2 (result — returns to driver)
          -> Task 0 (partition 0)
          -> Task 1 (partition 1)
          ...
```

**Key rule:** Read + filter + withColumn = same Stage (narrow). groupBy/join/repartition = new Stage (wide/shuffle boundary).

---

## Driver vs Executor OOM

| Who Failed | Most Likely Cause | Key Sign |
|---|---|---|
| Driver | collect() of large result; large broadcast; groupBy with large result | "Driver lost connection" |
| One executor (others idle) | Data skew — one partition much larger | One at 100%, others at 40% |
| All executors | Partition count too low; cache bloat | All at high memory |
| Python subprocess | Python UDF memory exceeded | Python process OOM, JVM fine |

---

## Partition Behavior

| Operation | Partition Effect |
|---|---|
| spark.read.parquet | One per file (or controlled by maxPartitionBytes) |
| df.repartition(n) | Exactly n; triggers shuffle |
| df.coalesce(n) where n < current | Reduce to n; no shuffle |
| df.coalesce(n) where n > current | Returns current count (no-op, not an error) |
| groupBy().count() | Shuffle uses spark.sql.shuffle.partitions (default 200) |
| spark.sql.shuffle.partitions | Only affects shuffles, NOT initial read |

**Target:** 128–256 MB per partition. For 1TB: ~8,192 partitions at 128MB.

---

## Shuffle Boundary — Creates New Stage

- groupBy, join, repartition, sort, distinct, count, collect (in some cases)
- Filter, withColumn, select = same Stage (no new boundary)

---

## Executor Configuration

| Property | Default | Meaning |
|---|---|---|
| spark.executor.memory | Cluster config | JVM heap per executor |
| spark.executor.cores | 1 | Concurrent tasks per executor |
| spark.executor.heartbeatInterval | 10s | Heartbeat frequency |
| (3 missed = 30s) | | Executor marked lost |
| spark.task.maxFailures | 4 | Per-task retry limit |
| spark.stage.maxAttempts | 4 | Per-stage retry limit |
| spark.driver.maxResultSize | 1GB | Max collect() result |
| spark.sql.shuffle.partitions | 200 | Shuffle partition count |

**executor.cores x executors = concurrent tasks total.** 4 executors x 4 cores = 16 concurrent tasks.

---

## Data Skew Diagnosis

| Spark UI Signal | Interpretation |
|---|---|
| One task duration >> others | Skewed partition |
| One executor memory at 100%, others idle | Skew on that partition |
| Aggregated metrics show uneven input sizes | Partition size imbalance |

**Fix:** AQE skew join (default enabled), manual salted join, repartition by skewed column.

---

## Dynamic Allocation

- minExecutors: floor
- maxExecutors: ceiling
- initialExecutors: starting count

---

## Key Exam Traps

1. coalesce(500) on 100-partition DF = 100 (not an error, not 500)
2. filter + groupBy = 1 Job, 2 Stages (read+filter in Stage 0, groupBy in Stage 1)
3. spark.sql.shuffle.partitions only affects shuffles, not initial read
4. "Lost task" error = executor failure, not driver failure
5. spark.executor.cores controls intra-executor concurrency, not total cluster cores
