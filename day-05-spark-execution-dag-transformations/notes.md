# Day 5 — Spark Execution: DAG, Lazy Evaluation, Transformations, and Physical Plans

> ⚠️ **Correction notice:** This version fixes two issues found in an earlier draft: (1) the "Key symbols in physical plans" table incorrectly defined the `*` prefix as "column used in later stages (passed through shuffle)" — this directly contradicted Part 7 of the same document, which correctly identifies `*` as the whole-stage code generation marker; the table is now corrected to match. (2) The example formatted physical plan mixed up aggregate function names (`merged_sum` inside a `count` example) and showed three stacked `HashAggregate` nodes where real two-phase aggregation only has two (partial, pre-shuffle; final, post-shuffle) — the example is now accurate.

## Exam Objectives

This day covers **Section 6: Cost & Performance Optimization (13%)** — specifically how Spark executes queries, where time is spent, and why some operations are expensive.

Key concepts: lazy evaluation, DAG formation, action vs transformation, wide vs narrow, physical plan reading.

---

## Part 1 — Lazy Evaluation

### What It Means

Spark transformations are **lazy** — they are recorded but not executed until an Action is called.

```python
# No execution yet — just building the plan
df = spark.read.parquet("/data/")
df2 = df.filter(F.col("status") == "active")
df3 = df2.groupBy("region").count()

# NOW execution happens — action triggers the entire DAG
result = df3.collect()
```

### Why Lazy Evaluation Exists

1. **Optimization opportunity:** Spark can see the entire transformation chain before executing, allowing it to optimize the physical plan.
2. **Reduced data movement:** Filter pushdown, column pruning, and predicate pushdown reduce unnecessary data movement.
3. **Fault tolerance:** Spark knows the full lineage of transformations — if a partition fails, it can recompute from the source.

### Catalyst Optimizer

The Catalyst Optimizer is Spark's query planning engine. It transforms the logical plan through multiple stages:

```
User Code (Python/SQL)
    |
Logical Plan (what you want to compute)
    |
Optimized Logical Plan (after rule-based optimizations)
    |
Physical Plan (how to compute it)
    |
Selected Physical Plan (cost-based selection)
    |
Execution (on executors)
```

**Key optimizations Catalyst applies:**
- Filter pushdown (filter moves closer to source)
- Column pruning (unused columns removed early)
- Constant folding (2+2 -> 4 at plan time)
- Predicate pushdown (filter conditions pushed to data sources)
- Partition pruning (skip unnecessary partitions)

### When Lazy Evaluation Causes Confusion

```python
# Common mistake: assuming write happens in order
df = spark.read.parquet("/data/")
df_filtered = df.filter(F.col("status") == "active")
df_filtered.write.mode("overwrite").parquet("/output/")  # This IS an action — write triggers execution
df_filtered.filter(F.col("amount") > 100).show()  # This is a SEPARATE action
```

**Key insight:** Two actions = two independent Jobs. The filter() result is NOT cached between the write and the show() — unless you explicitly cache it.

---

## Part 2 — Transformations: Narrow vs Wide

### Narrow Transformations

A narrow transformation processes each partition independently — no data moves between executors.

| Transformation | Why Narrow |
|---|---|
| filter() | Each row evaluated independently within its partition |
| withColumn() | Column computed from values in the same row |
| select() | Columns selected from same row |
| drop() | Rows removed from same partition |
| coalesce(n) where n < current | Local reduction of partitions |

**Properties:**
- One input partition -> one output partition (in most cases)
- No shuffle required
- Stage boundary NOT created

### Wide Transformations

A wide transformation requires data to move across executors — a shuffle.

| Transformation | Why Wide |
|---|---|
| groupBy().count() | Must group rows by key across all partitions |
| join() | Matching keys may be on different executors |
| repartition(n) | Data must be redistributed across all partitions |
| sort() | Global ordering requires all data on each executor |
| distinct() | Must see all rows to detect duplicates |
| aggregate() | Aggregation requires all values per key |
| pivot() | Creates new columns from row values across partitions |

**Properties:**
- One input partition -> multiple output partitions
- Shuffle required (data written to disk, read across network)
- **Stage boundary created**

### DAG Implications

```
df.read() -> filter() -> withColumn()     [All same Stage — narrow]
                                            |
                                      [SHUFFLE] (Stage boundary)
                                            |
                                     groupBy() -> count()          [New Stage]
```

Stage 0 tasks produce shuffle output. Stage 1 tasks read that shuffle output.

---

## Part 3 — The DAG (Directed Acyclic Graph)

### How the DAG Forms

The DAG is built by the SparkContext as it processes your transformation chain:

```python
# Spark builds DAG like this:
df.read.parquet()         # Node 1
    .filter(...)           # Node 2 (depends on Node 1)
    .withColumn(...)       # Node 3 (depends on Node 2)
    .groupBy(...)          # Node 4 (depends on Node 3) -> SHUFFLE
    .count()               # Node 5 (action, triggers execution)
```

Each node is a transformation. Edges represent data dependencies.

### Why It's a DAG (Not a Tree)

Because a single partition can feed multiple downstream operations:

```python
df_filtered = df.filter(F.col("status") == "active")
df_a = df_filtered.groupBy("region").count()
df_b = df_filtered.groupBy("category").count()
result = df_a.union(df_b).collect()
```

The filter result is used by two different groupBy operations — a DAG, not a tree.

### DAG to Stages

Spark optimizes the DAG to minimize shuffle boundaries. But every wide transformation (groupBy, join, repartition, sort, distinct) creates a stage boundary.

**Rule:** A shuffle boundary = a new Stage.

### Reading the DAG in Spark UI

In the Spark UI Jobs -> Stages view:
- Each box is a Stage
- Lines between boxes are shuffle operations
- Arrow direction shows data flow

---

## Part 4 — Physical Plan Execution

### Physical Plan vs Logical Plan

- **Logical Plan:** What you want to compute (declarative)
- **Physical Plan:** How Spark will compute it (imperative)

### Reading explain() Output

```python
df.groupBy("region").count().explain("formatted")
```

The formatted output shows operators in execution order (bottom to top). Aggregations that require a shuffle are always split into two phases: a **partial** aggregation before the shuffle (pre-combines values within each input partition, reducing shuffle volume) and a **final** aggregation after the shuffle (combines the partial results per key):

```
== Physical Plan ==
* HashAggregate(keys=[region#1], functions=[count(1)])
+- Exchange hashpartitioning(region#1, 200)
   +- * HashAggregate(keys=[region#1], functions=[partial_count(1)])
      +- * Scan parquet ...
```

**Key symbols in physical plans:**

| Symbol | Meaning |
|---|---|
| `*` (prefix) | **Whole-stage code generation** is active for this operator — Spark collapsed it (and often its neighbors) into a single generated JVM function for faster execution |
| Exchange | Shuffle operation (new stage boundary) |
| HashAggregate | Hash-based aggregation (used for groupBy); appears as `partial_*` before an Exchange and the plain function name after |
| SortAggregate | Sort-based aggregation (used when hash not applicable) |
| Scan | Reading data from storage |
| BroadcastExchange | Broadcast join (small table sent to all executors) |
| SortMergeJoin | Default join strategy for large tables |
| Filter | Predicate filter |
| Project | Column selection (SELECT clause) |
| TakeOrdered | Ordered result retrieval |

**Exam trap:** Don't confuse the `*` prefix (whole-stage codegen) with anything about shuffle or column lineage — it purely indicates code-generation, and it can appear on operators both before and after an `Exchange`.

### Shuffle Write and Shuffle Read

When a stage finishes, its output is written to disk (shuffle write) so the next stage can read it (shuffle read):

```
Stage 0 (map tasks)
  |
  |-- Task 0: process partition 0 -> write to local disk
  |-- Task 1: process partition 1 -> write to local disk
  ...
  |
  [Shuffle Write — data to disk, one file per partition]
  |
  [Network transfer — executors read their assigned shuffle partitions]
  |
Stage 1 (reduce tasks)
  |
  |-- Task 0: read shuffle output from Stage 0 tasks 0-N -> process
  |-- Task 1: read shuffle output from Stage 0 tasks 0-N -> process
  ...
```

**Shuffle write output:** One file per task, stored on the executor that ran the task.
**Shuffle read input:** Each Stage 1 task reads from multiple Stage 0 tasks (all partitions that contribute to its keys).

---

## Part 5 — Action Types and Their Implications

### Actions That Trigger Execution

| Action | Behavior | Driver Memory Impact |
|---|---|---|
| collect() | Returns all rows to driver | HIGH — driver holds all data |
| take(n) | Returns first n rows to driver | LOW |
| head(n) | Returns first n rows | LOW |
| count() | Returns count only | LOW |
| first() | Returns first row | LOW |
| show() | Prints to console | LOW |
| write | Writes to storage | LOW |
| foreach(func) | Applies func to each row | Depends on func |
| reduce(func) | Aggregates to single value | LOW |

### Actions That Create New Jobs

Each call to an action = one Job. Multiple actions = multiple independent Jobs.

```python
df = spark.read.parquet("/data/")

# Job 1
count = df.count()

# Job 2 (separate Job, re-reads data if not cached)
avg = df.select(F.avg("amount")).collect()

# Job 3
df.write.mode("overwrite").parquet("/output/")
```

**Important:** If you need the same filtered data for multiple actions, cache it first:
```python
df_cached = df.filter(F.col("status") == "active").cache()
df_cached.count()  # Job 1: reads, filters, caches
df_cached.collect()  # Job 2: reads from cache, not from source
```

---

## Part 6 — Spark SQL Execution Internals

### How Spark SQL Processes a Query

```sql
SELECT region, COUNT(*) as cnt
FROM orders
WHERE status = 'active'
GROUP BY region
ORDER BY cnt DESC
LIMIT 10
```

**Execution stages:**

1. **Parsing:** SQL string -> Abstract Syntax Tree (AST)
2. **Analysis:** AST -> Logical Plan (resolve table/column references)
3. **Optimization:** Logical Plan -> Optimized Logical Plan (Catalyst rules)
4. **Cost Calculation:** Multiple physical plans generated, cost estimated
5. **Plan Selection:** Cheapest physical plan selected
6. **Execution:** Physical plan executed on executors

### Columnar Execution (Arrow)

Spark 3.x uses Apache Arrow for efficient JVM-to-Python data transfer:
- **Pandas UDFs:** Arrow-encoded data passed between JVM and Python subprocesses
- **Zero-copy serialization:** Faster than Py4J (Java-to-Python serialization)
- **Vectorized processing:** Python processes Arrow batches, not individual rows

### Whole-Stage Code Generation

Spark 3.x uses whole-stage code generation to collapse multiple operators into a single function, reducing function call overhead:

```python
# Instead of: scan -> filter -> project -> aggregate (5 separate function calls per row)
# Whole-stage codegen: single generated function processes row through all operators
```

**In explain output:** Whole-stage codegen shows as `*` prefix on operators — the same symbol defined in Part 4's table above. Operators separated by an `Exchange` (shuffle) are always in different codegen stages, since codegen cannot span a shuffle boundary.

---

## Part 7 — Common Performance Anti-Patterns

### Anti-Pattern 1: Unnecessary Wide Transformations

```python
# Bad: Sort then take (sort is expensive)
df.orderBy(F.col("timestamp").desc()).take(10)

# Better: Use broadcast and limit
from pyspark.sql.window import Window
df.withColumn("rn", F.row_number().over(Window.orderBy(F.col("timestamp").desc())) \\
              ).filter(F.col("rn") <= 10).take(10)
```

### Anti-Pattern 2: Multiple Actions on Uncached Data

```python
# Bad: Reads source 3 times
df.count()
df.filter(F.col("amount") > 100).collect()
df.select("region").distinct().collect()

# Better: Cache once
df_cached = df.cache()
df_cached.count()   # Job 1
df_cached.filter(F.col("amount") > 100).collect()  # Job 2 (uses cache)
df_cached.select("region").distinct().collect()   # Job 3 (uses cache)
```

### Anti-Pattern 3: Selecting All Columns Before Filter

```python
# Bad: Reads all columns, then filters
df = spark.read.parquet("/data/")
df.filter(F.col("status") == "active").select("*")

# Better: Push column selection before filter (if possible)
df = spark.read.parquet("/data/").select("status", "region", "amount") \\
              .filter(F.col("status") == "active")
```

Catalyst often pushes column selection and simple, deterministic filters automatically (column pruning, predicate pushdown — Part 1), but explicit control still helps when a source doesn't support pushdown or the filter depends on something Catalyst can't reason about (a UDF, for example).

---

## Cross-References

- **Day 4:** Architecture — Jobs/Stages/Tasks hierarchy established here
- **Day 6:** Shuffle — what happens at the Exchange boundary between Stages
- **Day 7:** Memory — execution memory used during aggregation
- **Day 8:** Spark UI — see the physical plan visually in Query Profile
- **Day 3:** SQL Transformations — how joins and aggregations create stage boundaries
