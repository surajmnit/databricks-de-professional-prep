# Day 3 — Cheat Sheet: SQL Transformations and Testing

## Window Functions

| Function | Behavior with ties | Exam use |
|---|---|---|
| ROW_NUMBER | Unique, no gaps (1,2,3,4) | Deduplication — guarantees 1 row per partition |
| RANK | Same rank for ties, gaps after (1,2,2,4) | |
| DENSE_RANK | Same rank, no gaps (1,2,2,3) | |
| LAG(col, n, default) | Previous row value | Time-series |
| LEAD(col, n, default) | Next row value | Time-series |
| FIRST_VALUE | First in frame | |
| LAST_VALUE | Last in frame — needs ROWS UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING | |

**ROWS vs RANGE:** ROWS = physical row count. RANGE = logical value grouping. Same result when ORDER BY has no duplicates. Differ when ORDER BY has duplicates.

**Off-by-one trap:** `ROWS BETWEEN N PRECEDING AND CURRENT ROW` spans **N + 1** rows total. A true 7-row moving average needs `6 PRECEDING`, not `7 PRECEDING`.

## Join Types

| Join | Returns |
|---|---|
| LEFT SEMI | Left rows where key EXISTS in right (like IN) |
| LEFT ANTI | Left rows where key does NOT exist in right (like NOT IN) |
| BROADCAST | Small table sent to all executors; avoid shuffle |

**Broadcast threshold:** spark.sql.autoBroadcastJoinThreshold = 10MB default.
**Broadcast the SMALL table only.** Broadcasting large tables causes executor OOM.

## Skew Join Fix

Salt the join: add a random salt column to the **large** table's key, and **explode/replicate every row of the small table across all salt values** so every salted large-side key has a matching small-side row. AQE auto-handles skew: `spark.sql.adaptive.skewJoin.enabled = true` (default) — try this before manual salting.

**Common implementation bug:** salting only one side (or inventing placeholder keys on the small side) silently drops most matches, with no error raised. Always verify row counts against the unsalted result while testing a salted join.

## Aggregations

| Syntax | Output |
|---|---|
| GROUP BY ROLLUP(a,b) | (a,b), (a), grand total |
| GROUP BY CUBE(a,b) | (a,b), (a), (b), grand total |
| GROUP BY GROUPING SETS(...) | Explicit levels |

PIVOT: rows to columns. UNPIVOT (LATERAL VIEW EXPLODE(MAP(...))): columns to rows.

## Testing

| Method | Purpose |
|---|---|
| DataFrame.transform(fn) | Chain transformation functions; each independently testable |
| assertDataFrameEqual(df1, df2) | Compare DataFrames; **order-independent by default** (`checkRowOrder=False`) |
| assertSchemaEqual(df, schema) | Compare schemas exactly |

**Exam trap (direction matters):** `assertDataFrameEqual` does **not** check row order unless you pass `checkRowOrder=True`. Don't memorize this backwards — `False` is the default, and it means "order doesn't matter."

**LAST_VALUE exam trap:** Default frame is RANGE UNBOUNDED PRECEDING TO CURRENT ROW. 
Always add ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING for true last-in-partition.

## Join Ordering Rule

Filter BEFORE joining. Use CTEs to push filters early — most valuable when the filter can't already be pushed down automatically by Catalyst (e.g., it depends on a UDF or non-deterministic expression).
