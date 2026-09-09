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

## Join Types

| Join | Returns |
|---|---|
| LEFT SEMI | Left rows where key EXISTS in right (like IN) |
| LEFT ANTI | Left rows where key does NOT exist in right (like NOT IN) |
| BROADCAST | Small table sent to all executors; avoid shuffle |

**Broadcast threshold:** spark.sql.autoBroadcastJoinThreshold = 10MB default.
**Broadcast the SMALL table only.** Broadcasting large tables causes executor OOM.

## Skew Join Fix

Salt the join: add random salt column to large table, replicate small table rows by salt.
AQE auto-handles skew: spark.sql.adaptive.skewJoin.enabled = true (default).

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
| assertDataFrameEqual(df1, df2) | Compare DataFrames; order-independent with checkRowOrder=False |
| assertSchemaEqual(df, schema) | Compare schemas exactly |

**LAST_VALUE exam trap:** Default frame is RANGE UNBOUNDED PRECEDING TO CURRENT ROW. 
Always add ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING for true last-in-partition.

## Join Ordering Rule

Filter BEFORE joining. Use CTEs to push filters early.
