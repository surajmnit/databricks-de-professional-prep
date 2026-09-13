# Day 8 — Spark UI and Query Profile: Diagnosing Bottlenecks

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 6: Cost & Performance Optimization (13%)**:
- "Use the query profile to analyze the query and identify bottlenecks, such as bad data skipping, inefficient types of joins, and data shuffling."

Also underpins the "Query Profiler UI and Spark UI" bullet in **Section 5** (Day 21 covers that bullet's system-tables/observability angle — this day owns the actual *tool navigation and diagnostic skill*).

*(This day assumes Day 4's Driver/Executor/Stage model, Day 5's DAG/physical-plan reading, Day 6's shuffle mechanics, and Day 7's memory/spill model. We don't re-derive those concepts here — we apply them inside the actual UI.)*

---

## Part 1 — Spark UI: A Systematic Investigation Workflow

Symptoms overlap in the Spark UI: a long-running stage with spill and a few slow tasks could be data skew, insufficient executor memory, too few partitions, **or** an inefficient join strategy. Guessing wastes time. Use this fixed sequence:

### Step 1: Stages tab, sorted by Duration (descending)
Click into the single longest stage. Everything else is noise until you understand that one stage — most jobs have one dominant bottleneck stage, not many equally-bad ones.

### Step 2: Pull three numbers from that stage's Task Metrics
- **Median task duration**
- **Max task duration**
- **Median vs. Max shuffle read size per task**

### Step 3: Ask three diagnostic questions, in this order

| Question | If yes → | Fix (cross-reference) |
|---|---|---|
| Is Max Duration > 5× Median, **and** is shuffle read also skewed across tasks? | **Data skew** | Broadcast join if the small side fits in memory; otherwise salting (Day 6) or let AQE skew join handle it (Part 3 below) |
| Does spill appear on most tasks, or is GC time above 10% in the Executors tab? | **Memory pressure** | Increase `spark.sql.shuffle.partitions` before reaching for more executor memory (Day 7) |
| Is task count below ~2× your executor core count? | **Underparallelism** | Raise `spark.sql.shuffle.partitions` or add an explicit `repartition()` (Day 6) |

### Step 4: If none of those fit, open the SQL/DataFrame tab
Check the physical plan for cross joins, missing predicate pushdown, or a `SortMergeJoin` where a `BroadcastHashJoin` would work instead (Day 5/6).

### Step 5: Validate the fix against the metric, not wall-clock time alone
Confirm the underlying number actually moved — GC time back near zero, Max/Median ratio below ~2× — not just that the job "felt faster." Wall-clock time can improve for unrelated reasons (cluster warm-up, cached data, etc.) and mask a fix that didn't really work.

**Exam trap:** a scenario describing overlapping symptoms (slow stage + some spill + a couple of slow tasks) is testing whether you diagnose in the right *order* — skew first (task/shuffle-read imbalance), then memory pressure (spill/GC), then underparallelism (task count vs. cores) — not whether you can name all three causes in isolation.

---

## Part 2 — Spark UI Tabs Reference (the observability angle)

| Tab | What it's for | Key signal to check |
|---|---|---|
| **Jobs** | One row per Action (Day 4/5) | Which Job took the longest overall |
| **Stages** | One row per shuffle-bounded stage | Duration, Shuffle Read/Write, Spilled Bytes — the primary bottleneck-hunting tab |
| **Storage** | Cached RDDs/DataFrames | Whether a `.cache()` actually fit in memory or spilled to disk (Day 7) |
| **Environment** | Full Spark config for this session | Confirms actual `spark.sql.shuffle.partitions`, AQE settings, etc. actually in effect — not just what you *think* you set |
| **Executors** | Per-executor resource usage | GC Time %, Storage Memory used, Shuffle Read/Write per executor — reveals *which* executor is struggling, useful for confirming skew lands on one specific node |
| **SQL / DataFrame** | The physical query plan (Day 5) | `Exchange` = shuffle boundary, `BroadcastExchange` = broadcast join, look for `SortMergeJoin` where broadcast would be cheaper |

---

## Part 3 — Adaptive Query Execution (AQE): What It Actually Changes at Runtime

AQE re-optimizes the **physical plan mid-query**, using real statistics gathered after each completed shuffle stage — not just the pre-execution estimates Catalyst starts with. It is **enabled by default** (`spark.sql.adaptive.enabled = true`).

| AQE Feature | Config | What it does |
|---|---|---|
| **Coalesce shuffle partitions** | `spark.sql.adaptive.coalescePartitions.enabled` (default `true`) | After a shuffle, merges adjacent small partitions into fewer, right-sized ones — directly fixes the "too many tiny shuffle partitions" underparallelism-adjacent problem without you manually tuning `spark.sql.shuffle.partitions` per query |
| **Skew join optimization** | `spark.sql.adaptive.skewJoin.enabled` (default `true`) | Detects a partition significantly larger than its peers post-shuffle and **splits it into smaller sub-tasks** processed in parallel — this is the automatic alternative to manual salting from Day 6 |
| **Dynamic join strategy switching** | (part of AQE's runtime re-planning, no separate toggle) | If actual post-shuffle statistics show one side of a `SortMergeJoin` is actually small enough to fit under the broadcast threshold, AQE can **convert the join to a broadcast join mid-query**, even though the original plan (based on pre-execution estimates) chose sort-merge |

**Exam trap:** AQE's skew handling and manual salting (Day 6) are **two different tools for the same problem** — a scenario emphasizing "with minimal code changes" or "without modifying the query" points to **AQE skew join** (it's on by default and requires no code change); a scenario where AQE is explicitly disabled, or the skew is too extreme for AQE's thresholds, points to **manual salting**.

**Exam trap:** AQE decisions happen **per completed shuffle stage**, not for the whole query upfront — this is why it can catch cases where the *initial* estimate was wrong (e.g., a heavily-filtered table turns out to be much smaller than its unfiltered statistics suggested).

---

## Part 4 — Query Profile (Databricks SQL / DBSQL Warehouses)

Query Profile is the **DBSQL/Photon-aware** equivalent of digging through the Spark UI — purpose-built for SQL warehouse query performance, with more automated insight surfacing than the general Spark UI provides.

### Access requirements
To view a query profile, you must be **the query's owner**, or hold **`CAN MONITOR`** permission on the SQL warehouse that executed it. (Cross-reference Day 18's ACL model — this is a workspace/warehouse-level permission, not a Unity Catalog data grant.)

### Common operators shown
| Operator | Meaning |
|---|---|
| **Scan** | Data read from a data source, output as rows — this is where **data skipping** metrics live (bytes/files read vs. pruned) |
| **Join** | Rows from multiple relations combined |
| **Union** | Rows from same-schema relations concatenated |
| **Shuffle** | Data redistributed across the cluster — the most expensive operator category, same concept as Spark UI's `Exchange` |
| **Hash/Sort (Aggregate)** | Rows grouped by key and aggregated (`SUM`, `COUNT`, `MAX`, etc.) |

By default, low-impact operators are hidden; use **Enable verbose mode** to see every operator and additional metrics when the default view doesn't show enough detail.

### Reading the three named bottleneck categories from the exam objective

| Exam category | Where you see it in Query Profile |
|---|---|
| **Bad data skipping** | The **Scan** operator's metrics show a high ratio of bytes/files *read* vs. bytes/files *pruned* — meaning the query is scanning far more data than its filter should require. Root causes are covered in depth on Day 9–11 (Z-Ordering, Liquid Clustering, partitioning) — this day's job is knowing **where to see the symptom**, not yet the storage-layer fix. |
| **Inefficient join type** | A `SortMergeJoin` (or, worse, a Cartesian/cross join) appears where a `BroadcastHashJoin` would be cheaper — visible either in the operator tree directly or flagged by Query Profile's automated **insights** panel |
| **Data shuffling** | Large **Shuffle** operators dominating the time breakdown — same diagnostic value as `Exchange` nodes and high Shuffle Read/Write in the Spark UI Stages tab |

### Automated insights
Query Profile proactively surfaces performance insights in the query details panel and inline on the operator graph — flagging likely-avoidable costs (skew, spill, an inefficient join strategy, poor data skipping) without you having to manually compare every operator's metrics. Recognize these insight categories by name; a scenario describing symptoms (one task much slower, a spill metric, a shuffle step dominating total time) maps directly back to one of them.

**Exam trap:** Query Profile is scoped to **SQL warehouses** (DBSQL, Photon-aware); the general Spark UI is the tool for **any cluster/any workload** (notebooks, jobs, Lakeflow pipelines). A scenario naming the compute type (SQL warehouse vs. all-purpose/job cluster) is telling you which tool to reach for.

---

## Part 5 — Exam Traps Recap

1. Diagnose in **order**: skew (task + shuffle-read imbalance) → memory pressure (spill/GC%) → underparallelism (task count vs. cores) — not a flat list to pattern-match randomly.
2. **AQE is on by default** and handles skew joins and small-shuffle-partition coalescing automatically — manual salting (Day 6) is the fallback when AQE is disabled or insufficient, not the default-first tool.
3. AQE can **convert a SortMergeJoin to a BroadcastHashJoin mid-query** based on actual post-shuffle statistics, not just pre-execution estimates.
4. Query Profile access needs **ownership or `CAN MONITOR`** on the warehouse — a workspace-level compute permission, distinct from Unity Catalog `SELECT` grants (Day 18).
5. Query Profile = **SQL warehouses**; Spark UI = **any Spark compute**. Pick based on the compute type named in the scenario.
6. "Bad data skipping" shows up as a high scanned-vs-pruned ratio on the **Scan** operator — the fix (Z-Ordering, Liquid Clustering, partitioning) is Day 9–11 material; this day is about recognizing the symptom.
7. Verbose mode is needed to see hidden/low-impact operators and extra metrics — don't assume the default view shows everything.
8. Validate a performance fix against the **actual metric that was wrong** (GC%, skew ratio, bytes scanned) — not just improved wall-clock time, which can be misleading.

---

## Cross-References
- Day 4: Driver/Executor/Stage/Task model underlying everything the Spark UI displays.
- Day 5: Physical plan reading — `Exchange`, `BroadcastExchange`, `SortMergeJoin`, whole-stage codegen.
- Day 6: Shuffle mechanics and manual salted-join skew fix — compare against AQE's automatic skew join here.
- Day 7: Memory regions, spill, and GC — the metrics behind the "memory pressure" diagnostic branch.
- Day 9–11: Data skipping, Z-Ordering, and Liquid Clustering — the storage-layer *fixes* for the "bad data skipping" symptom this day teaches you to *detect*.
- Day 18: `CAN MONITOR` warehouse permission vs. Unity Catalog data grants — two different permission planes.
- Day 21: System-tables/observability framing of Query Profile (`system.query.history`, `statement_id` join key) for building programmatic dashboards on top of what this day covers manually.
