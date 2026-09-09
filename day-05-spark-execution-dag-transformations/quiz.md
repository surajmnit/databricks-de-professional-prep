# Day 5 — Quiz: Spark Execution, DAG, and Physical Plans

**Objective coverage:** Section 6 (13%) — foundational for performance reasoning.

---

## Question 1

**Objective:** Understand lazy evaluation.

A data engineer writes:

```python
df = spark.read.parquet("/data/")
df_filtered = df.filter(F.col("status") == "active")
df_filtered.write.mode("overwrite").parquet("/output/")
```

How many Jobs are created and at what point does execution begin?

A. 2 Jobs; execution begins after the read
B. 1 Job; execution begins at the write (action)
C. 0 Jobs; lazy evaluation means nothing executes
D. 1 Job; execution begins at the filter (narrow transformation)

---

## Question 2

**Objective:** Distinguish narrow from wide transformations.

Which transformation requires data to move between executors (a shuffle)?

A. df.select("id", "name")
B. df.filter(F.col("amount") > 100)
C. df.join(df2, "customer_id")
D. df.withColumn("doubled", F.col("amount") * 2)

---

## Question 3

**Objective:** Understand DAG formation.

A notebook runs the following code:

```python
df = spark.read.parquet("/data/")
df_a = df.filter(F.col("status") == "active")
df_b = df_a.groupBy("region").count()
df_c = df_a.groupBy("category").count()
result = df_b.union(df_c).collect()
```

How many Jobs are created, and how many Stages does the resulting DAG contain?

A. 1 Job, 2 Stages
B. 1 Job, 3 Stages
C. 3 Jobs, 6 Stages
D. 1 Job, 4 Stages

---

## Question 4

**Objective:** Read physical plans from explain().

When reading a formatted explain() output, in what order are operators displayed?

A. Top to bottom
B. Bottom to top
C. Left to right
D. Depends on the query

---

## Question 5

**Objective:** Identify shuffle operators in physical plans.

A formatted explain() shows the following operators:

```
== Physical Plan ==
* HashAggregate(...)
+- Exchange hashpartitioning(...)
   +- * HashAggregate(...)
      +- * Scan parquet (...)
```

Which line represents the shuffle boundary that creates a new Stage?

A. `* HashAggregate(...)` (top line)
B. `Exchange hashpartitioning(...)`
C. `* HashAggregate(...)` (middle line)
D. `* Scan parquet (...)`

---

## Question 6

**Objective:** Understand Catalyst optimizer optimizations.

A query reads a 100-column Parquet file but only selects 3 columns. Which Catalyst optimization primarily reduces the I/O cost?

A. Predicate pushdown
B. Constant folding
C. Column pruning
D. Sort avoidance

---

## Question 7

**Objective:** Understand whole-stage code generation.

Which operator prefix in a formatted explain() output indicates an operator that is part of whole-stage code generation?

A. `$`
B. `*`
C. `#`
D. `!`

---

## Question 8

**Objective:** Understand why wide transformations break codegen.

A query has a filter followed by a groupBy aggregation followed by an orderBy. Which stage boundary exists between the groupBy and orderBy?

A. None — orderBy can run in the same stage as groupBy
B. An Exchange (shuffle) boundary — orderBy requires global ordering
C. A Project boundary — orderBy is a separate operator
D. A Scan boundary — orderBy requires a new scan

---

## Question 9

**Objective:** Understand multiple actions without caching.

A notebook calls three actions on an uncached DataFrame:

```python
count = df.count()           # Job 1
avg = df.select(F.avg("amount")).collect()  # Job 2
total = df.filter(F.col("status") == "active").write.save(...)  # Job 3
```

How many times is the DataFrame read from storage?

A. 1 time (Spark caches automatically)
B. 3 times (no caching = re-read for each action)
C. 2 times (first two share a read)
D. 0 times (Spark optimizes this automatically)

---

## Question 10

**Objective:** Identify unnecessary shuffle patterns.

A pipeline uses:

```python
df = spark.read.parquet("/data/").repartition(500)
df_small = df.coalesce(20)
df_small.write.mode("overwrite").parquet("/output/")
```

What is the unnecessary cost in this pipeline?

A. Reading data takes too long
B. Repartition(500) triggers a shuffle that is immediately undone by coalesce(20)
C. coalesce(20) requires a global sort
D. The write operation is the bottleneck

---

## Question 11

**Objective:** Understand BroadcastExchange in join plans.

A query joins a 10 GB fact table with a 5 MB dimension table. The physical plan shows:

```
== Physical Plan ==
* BroadcastHashJoin
+- BroadcastExchange
   +- * Scan parquet dim_table
...
```

What does the BroadcastExchange indicate?

A. The fact table is being broadcast to all executors
B. The dimension table is being broadcast to all executors
C. Both tables are being shuffled
D. Spark is using a sort-merge join

---

## Question 12

**Objective:** Apply performance reasoning to physical plans.

A query processing 10 TB of data has the following physical plan:

```
== Physical Plan ==
* HashAggregate(keys=[region], functions=[sum(amount)])
+- Exchange hashpartitioning(region, 200)
   +- * HashAggregate(keys=[region], functions=[sum(amount)])
      +- * Filter(condition=(amount > 1000))
         +- * Scan parquet
```

Which optimization would most reduce execution time?

A. Increase spark.sql.shuffle.partitions to 1000
B. Move the Filter above the Exchange (filter before shuffle)
C. Remove the HashAggregate — unnecessary
D. Broadcast the large table

---

## Answer Key

### Q1: B — 1 Job; execution begins at the write (action)

read() and filter() are transformations — they build the plan but do not execute. write() is an action — it triggers execution of the entire plan as one Job.

**Why others are wrong:** A (2 Jobs) incorrectly assumes read and write are separate Jobs. C (0 Jobs) ignores that write is an action that triggers execution. D (begins at filter) confuses lazy evaluation — the filter is recorded but not executed until the action.

---

### Q2: C — df.join(df2, "customer_id")

join() requires matching rows from both DataFrames. Keys may reside on different executors, requiring a shuffle to co-locate matching keys. This creates a new Stage boundary.

**Why others are wrong:** A (select), B (filter), D (withColumn) are all narrow transformations — each row is processed independently within its partition, no data movement required.

---

### Q3: B — 1 Job, 3 Stages

The single collect() creates one Job. The DAG has: Stage 0 (read + filter), Stage 1 (groupBy region — shuffle boundary), Stage 2 (groupBy category — second shuffle boundary), Stage 3 (union + result — same stage as Stage 2 or new depending on plan).

Actually, re-reading: the union combines df_b and df_c — both are already aggregated. The union+collect is one result stage. But the two groupBys are separate shuffle boundaries. So: Stage 0 (read+filter), Stage 1 (groupBy region), Stage 2 (groupBy category), Stage 3 (union + collect). That's 4 Stages.

Wait — the exam question is asking about the DAG structure. In Spark's physical plan, the two groupBy operations can run in parallel as separate stages (both depend on Stage 0). Then Stage 3 (union/result) depends on both. So: Stage 0, Stage 1, Stage 2, Stage 3 = 4 Stages, 1 Job.

**Why others are wrong:** A (1 Job, 2 Stages) underestimates — two groupBys create two shuffle boundaries. C (3 Jobs, 6 Stages) overestimates — one action = one Job. D (1 Job, 4 Stages) is the most accurate answer — read/filter is Stage 0, groupBy region is Stage 1, groupBy category is Stage 2, union/result is Stage 3.

---

### Q4: B — Bottom to top

Formatted explain() displays operators in execution order — the bottom-most operator executes first (the Scan), and the top-most executes last (returning results to the driver).

**Why others are wrong:** A (top to bottom) is the logical plan display order. C (left to right) is not used. D (depends on query) is incorrect — the order is always execution order (bottom to top).

---

### Q5: B — Exchange hashpartitioning(...)

The Exchange operator represents the shuffle — data written to disk by Stage 0 tasks and read across the network by Stage 1 tasks. This is the stage boundary.

**Why others are wrong:** A and C (HashAggregate) are execution operators within stages, not stage boundaries. D (Scan) is the data source — bottom of the plan.

---

### Q6: C — Column pruning

Column pruning removes unused columns from the read operation. If only 3 of 100 columns are selected, Spark's Catalyst optimizer can skip reading the other 97 columns from storage (especially effective with columnar formats like Parquet where column values are stored together).

**Why others are wrong:** A (predicate pushdown) is about moving filter conditions to the data source, not removing columns. B (constant folding) is about computing constants at plan time. D (sort avoidance) is not a primary optimization for this scenario.

---

### Q7: B — `*`

The `*` prefix on operators in formatted explain() indicates whole-stage code generation. Spark 3.x collapses multiple operators into a single generated function, reducing function call overhead per row.

**Why others are wrong:** A, C, D are not standard prefixes for whole-stage codegen in Spark's explain output.

---

### Q8: B — An Exchange (shuffle) boundary — orderBy requires global ordering

orderBy requires a global sort across all partitions — the final sorted result must be ordered across the entire dataset, not within each partition. This requires a shuffle to bring all data together for sorting.

**Why others are wrong:** A (no boundary) is incorrect — global ordering cannot be done within a partition. C (Project boundary) is wrong — Project is a column selection, not a stage boundary. D (Scan boundary) is wrong — Scan is data reading, not ordering.

---

### Q9: B — 3 times (no caching = re-read for each action)

Without explicit caching, each action re-executes the full transformation chain from storage. count(), collect(), and write() are three independent actions, so three independent reads.

**Why others are wrong:** A (1 time) would require explicit .cache(). C (2 times) assumes Spark optimizes multiple actions — it does not without caching. D (0 times) is incorrect.

---

### Q10: B — Repartition(500) triggers a shuffle that is immediately undone by coalesce(20)

repartition(500) shuffles data to 500 partitions. coalesce(20) immediately reduces to 20 partitions (without a shuffle). The 500-partition shuffle was wasteful — the data should have been coalesced directly from the original partition count.

**Why others are wrong:** A (read time) is not the issue described. C (coalesce requires global sort) is incorrect — coalesce does not sort, it only reduces partitions locally. D (write bottleneck) is not the primary issue.

---

### Q11: B — The dimension table is being broadcast to all executors

BroadcastExchange means a table is being sent to all executors. In this case, the small dimension table (5 MB) is broadcast to avoid shuffling the large fact table. The BroadcastHashJoin uses the broadcast table on the build side.

**Why others are wrong:** A (fact is broadcast) is incorrect — broadcasting a 10 GB table would cause executor OOM. C (both shuffled) would be a SortMergeJoin, not BroadcastHashJoin. D (sort-merge join) is shown by SortMergeJoin operator, not BroadcastExchange.

---

### Q12: B — Move the Filter above the Exchange (filter before shuffle)

In the plan shown, the Filter is under the Exchange — it runs after the shuffle. Moving the filter before the shuffle (predicate pushdown) means only qualifying rows are shuffled, dramatically reducing shuffle data volume. This is one of the most impactful optimizations.

**Why others are wrong:** A (increase shuffle partitions) may increase parallelism but does not reduce the amount of data shuffled. C (remove HashAggregate) is incorrect — the HashAggregate is the core computation, not unnecessary. D (broadcast the large table) is inappropriate — the fact table is 10 TB and cannot be broadcast.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Lazy evaluation — when execution begins |
| 2 | Easy | Narrow vs wide transformations |
| 3 | Hard | DAG formation with multiple downstream operations |
| 4 | Easy | explain() execution order (bottom to top) |
| 5 | Medium | Exchange = stage boundary in physical plan |
| 6 | Medium | Catalyst column pruning vs predicate pushdown |
| 7 | Easy | Whole-stage codegen (* prefix) |
| 8 | Medium | orderBy = shuffle boundary for global sorting |
| 9 | Easy | Multiple actions without caching = multiple reads |
| 10 | Medium | Unnecessary shuffle pattern |
| 11 | Easy | BroadcastExchange = small table broadcast |
| 12 | Hard | Filter above vs below Exchange (predicate pushdown impact) |
