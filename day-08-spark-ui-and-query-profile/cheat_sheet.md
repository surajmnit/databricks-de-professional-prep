# Day 8 — Cheat Sheet: Spark UI and Query Profile

## The Diagnostic Sequence (memorize the order)

```
1. Stages tab → sort by Duration → click the longest stage
2. Pull: Median task duration, Max task duration, Median vs Max shuffle read
3. Ask in order:
   a. Max > 5x Median AND shuffle read also skewed?      → DATA SKEW
   b. Spill on most tasks OR GC time > 10%?                → MEMORY PRESSURE
   c. Task count < ~2x executor cores?                     → UNDERPARALLELISM
4. None of those? → SQL/DataFrame tab: check join strategy, pushdown, cross joins
5. Validate the FIX against the METRIC, not just wall-clock time
```

---

## Spark UI Tabs

| Tab | Use it for |
|---|---|
| Jobs | Which Action took longest overall |
| Stages | Primary bottleneck hunting — Duration, Shuffle R/W, Spilled Bytes |
| Storage | Did `.cache()` fit in memory or spill to disk? |
| Environment | Confirm actual active config (AQE, shuffle.partitions) |
| Executors | GC Time %, per-executor shuffle/memory — confirms *which* node is struggling |
| SQL/DataFrame | Physical plan — `Exchange`, `BroadcastExchange`, `SortMergeJoin` |

---

## AQE (Adaptive Query Execution) — ON by default

| Feature | Config | Effect |
|---|---|---|
| Coalesce partitions | `spark.sql.adaptive.coalescePartitions.enabled` (default `true`) | Merges small post-shuffle partitions |
| Skew join | `spark.sql.adaptive.skewJoin.enabled` (default `true`) | Splits an oversized partition into parallel sub-tasks — **try this before manual salting** |
| Dynamic join switching | (built into AQE re-planning) | Can convert `SortMergeJoin` → `BroadcastHashJoin` mid-query using real post-shuffle stats |

**Key idea:** AQE re-optimizes **after each completed shuffle stage** using actual stats — not just pre-execution estimates.

---

## Query Profile (SQL Warehouses / DBSQL)

**Access:** query owner, or `CAN MONITOR` on the warehouse.

| Operator | Meaning |
|---|---|
| Scan | Read from source — **data-skipping metrics live here** (bytes/files read vs. pruned) |
| Join | Combine relations |
| Union | Concatenate same-schema relations |
| Shuffle | Redistribute data — most expensive category |
| Hash/Sort (Aggregate) | Group + aggregate (`SUM`, `COUNT`, `MAX`) |

- **Verbose mode** reveals hidden low-impact operators/metrics.
- **Insights panel** auto-flags: skew, spill, inefficient join, poor data skipping.

### The 3 exam-named bottleneck categories → where you see them
| Category | Signal |
|---|---|
| Bad data skipping | High scanned-vs-pruned ratio on **Scan** |
| Inefficient join type | `SortMergeJoin`/cross join where broadcast would work |
| Data shuffling | Large **Shuffle**/`Exchange` dominating time |

---

## Query Profile vs. Spark UI

| | Query Profile | Spark UI |
|---|---|---|
| Compute | SQL warehouse (DBSQL/Photon) | Any cluster |
| Scope | One query | Jobs/Stages/Tasks/Executors across the session |
| Access | Owner or `CAN MONITOR` on warehouse | Cluster access |

---

## Exam Trap Shortlist

1. Diagnose **in order**: skew → memory pressure → underparallelism. Don't pattern-match randomly.
2. AQE is **on by default** — try automatic skew join before manual salting.
3. AQE can switch join strategy **mid-query**, not just resize partitions.
4. Query Profile needs **ownership or `CAN MONITOR`** — a warehouse permission, not a UC data grant (Day 18).
5. Query Profile = SQL warehouses only; Spark UI = any Spark compute.
6. "Bad data skipping" = high scanned-vs-pruned ratio on Scan — root-cause fix is Day 9–11 (Z-Order/Liquid Clustering/partitioning).
7. Verbose mode needed to see all operators/metrics — default view hides low-impact ones.
8. Validate a fix against the **actual metric**, not just improved wall-clock time.
