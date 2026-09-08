# Day 5 — Cheat Sheet: Spark Execution, DAG, and Physical Plans

## Lazy Evaluation

- Transformations: **recorded but NOT executed** until an Action
- Actions: collect(), count(), write(), take(), show() — trigger execution
- Multiple actions without cache = multiple independent reads from source

---

## Narrow vs Wide Transformations

| Narrow (No Shuffle) | Wide (Shuffle = New Stage) |
|---|---|
| filter() | groupBy() |
| withColumn() | join() |
| select() | repartition() |
| coalesce(n) where n < current | sort() |
| drop() | distinct() |
| | orderBy() |

**Rule: Any operation requiring data movement across executors = new Stage boundary.**

---

## DAG Formation

```
User Code
  |
Logical Plan (what to compute)
  |
Optimized Logical Plan (Catalyst rules applied)
  |
Physical Plan (how to compute)
  |
Execution
```

Catalyst optimizations: filter pushdown, column pruning, constant folding, predicate pushdown.

---

## Reading explain("formatted")

**Bottom to top = execution order.**

```
Top: HashAggregate       <- executes last (sends result to driver)
  Exchange               <- shuffle boundary (Stage 1 starts here)
    HashAggregate        <- executes second (reads shuffle output)
      Scan               <- executes first (reads from storage)
```

**Exchange = stage boundary.**

---

## Physical Plan Symbols

| Symbol | Meaning |
|---|---|
| `*` (prefix) | Whole-stage code generation active |
| Exchange | Shuffle — new Stage boundary |
| BroadcastExchange | Small table broadcast |
| HashAggregate | Hash-based aggregation (groupBy) |
| SortAggregate | Sort-based aggregation |
| SortMergeJoin | Default large-table join |
| Filter | Predicate filter |
| Project | Column selection |

---

## Whole-Stage Code Generation

- Collapses multiple operators into one generated function
- `*` prefix = codegen active
- Broken by: shuffle operations (groupBy, join, sort, distinct)
- Result: fewer function calls per row = faster execution

---

## Key Exam Traps

1. **Lazy evaluation**: transformation is recorded, not executed. Execution starts at the FIRST action.
2. **explain() order**: bottom to top, not top to bottom.
3. **Exchange = stage boundary**: all other operators (HashAggregate, Filter, Project) are within a stage.
4. **Multiple actions without cache = multiple reads**: each action re-executes from source.
5. **repartition(500).coalesce(20)**: the 500-partition shuffle is wasteful — coalesce directly.
6. **orderBy** = shuffle boundary (global ordering requires all data on each executor).
7. **Filter under Exchange**: filter runs AFTER shuffle. Move filter before shuffle to reduce shuffle volume.

---

## Performance Anti-Patterns

| Anti-Pattern | Fix |
|---|---|
| Multiple actions on uncached df | cache() once |
| repartition(n).coalesce(m) | coalesce(m) directly |
| sort().take(n) | window row_number().filter() |
| Filter after shuffle | Filter before shuffle |
