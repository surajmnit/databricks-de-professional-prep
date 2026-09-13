# Day 8 — Quiz: Spark UI and Query Profile

**Objective coverage:** Section 6 (13%) — bottleneck diagnosis via Query Profile and Spark UI.

---

## Question 1
**Objective:** Systematic Spark UI investigation order.

A stage shows spill on most tasks and GC time above 10% in the Executors tab, but task durations are roughly even across all tasks. What is the most likely root cause?

A. Data skew
B. Memory pressure
C. Underparallelism
D. An inefficient join strategy

---

## Question 2
**Objective:** Distinguishing skew from underparallelism.

A stage has task count roughly equal to 1.5× the executor core count, with even task durations and no spill. What should you investigate?

A. Data skew — salt the join
B. Underparallelism — raise `spark.sql.shuffle.partitions` or repartition
C. Memory pressure — increase executor memory
D. Nothing — this is optimal

---

## Question 3
**Objective:** AQE default behavior.

Is Adaptive Query Execution (AQE) enabled by default in current Databricks Runtime?

A. No, it must be manually enabled per query
B. Yes — `spark.sql.adaptive.enabled` defaults to `true`
C. Only for SQL warehouses, never for job clusters
D. Only when `spark.sql.shuffle.partitions` is set above 200

---

## Question 4
**Objective:** AQE skew join vs. manual salting.

A team has a groupBy with a known skewed key. AQE is enabled with default settings. What should they try first?

A. Manually salt the join immediately — AQE cannot handle skew
B. Let AQE's skew join optimization attempt to split the skewed partition automatically first, since it requires no code change
C. Disable AQE, since it interferes with skew handling
D. Increase `spark.sql.shuffle.partitions` to 1, which forces a single skew-free partition

---

## Question 5
**Objective:** AQE dynamic join strategy switching.

A query's initial physical plan chooses `SortMergeJoin` because the optimizer's pre-execution size estimate for one side was too high. At runtime, after a shuffle stage completes, actual statistics show that side is small enough to broadcast. What can AQE do here?

A. Nothing — the join strategy is fixed once chosen
B. AQE can convert the join to a `BroadcastHashJoin` mid-query based on the updated runtime statistics
C. AQE can only adjust shuffle partition count, never join strategy
D. AQE requires a query restart to change join strategy

---

## Question 6
**Objective:** Query Profile access requirements.

A user who did not create a query wants to view its Query Profile. What is required?

A. Nothing — Query Profile is visible to all workspace users by default
B. The user must be the query owner, or hold `CAN MONITOR` permission on the SQL warehouse that ran it
C. The user must have `SELECT` on every table referenced by the query
D. Only account admins can ever view a Query Profile

---

## Question 7
**Objective:** Query Profile common operators.

In a Query Profile, which operator specifically indicates data being redistributed across the cluster, and is generally the most expensive operator category?

A. Scan
B. Join
C. Shuffle
D. Union

---

## Question 8
**Objective:** Data skipping detection in Query Profile.

Which Query Profile signal indicates a "bad data skipping" problem, as referenced in the exam objective?

A. A high ratio of bytes/files read vs. pruned in the Scan operator's metrics
B. A `BroadcastExchange` operator appearing in the plan
C. A high GC time percentage in the Executors tab
D. A `Union` operator combining more than two relations

---

## Question 9
**Objective:** Verbose mode.

By default, some operators and metrics are hidden in Query Profile. What must be done to see them?

A. Nothing can reveal them — they are permanently hidden
B. Enable verbose mode from the kebab menu
C. Re-run the query with a `VERBOSE` SQL hint
D. Contact Databricks support to unlock hidden metrics

---

## Question 10
**Objective:** Query Profile vs. Spark UI scope.

A workload runs on a Databricks SQL warehouse and is running slower than expected. Which tool is the most directly applicable for diagnosing it?

A. Spark UI Executors tab
B. Query Profile
C. `system.compute.node_types`
D. The Lakeflow pipeline event log

---

## Question 11
**Objective:** Validating a performance fix.

After applying a fix for suspected data skew, wall-clock time for the job dropped noticeably. What should you check before concluding the skew was actually resolved?

A. Nothing further — faster wall-clock time is sufficient proof
B. Confirm the underlying metric that indicated skew (e.g., Max/Median task duration ratio) has actually improved, not just that the job felt faster
C. Re-run the job exactly once more to be sure
D. Check the billing dashboard for reduced DBU cost

---

## Question 12
**Objective:** AQE coalesce partitions.

After a shuffle produces many small partitions, which AQE feature merges them into fewer, appropriately-sized partitions automatically?

A. `spark.sql.adaptive.skewJoin.enabled`
B. `spark.sql.adaptive.coalescePartitions.enabled`
C. `spark.sql.autoBroadcastJoinThreshold`
D. `spark.sql.shuffle.partitions` (static, non-adaptive)

---

## Question 13
**Objective:** Reading the SQL/DataFrame tab for join strategy.

In the Spark UI SQL/DataFrame tab, which plan operator specifically indicates a broadcast join was used, avoiding a shuffle on the large side?

A. `Exchange`
B. `SortMergeJoin`
C. `BroadcastExchange`
D. `HashAggregate`

---

## Question 14
**Objective:** Choosing the right diagnostic tool by compute type.

A scenario says a job runs on an all-purpose interactive cluster (not a SQL warehouse) and is underperforming. Which tool should be used first?

A. Query Profile
B. Spark UI
C. `system.query.history`
D. Notification Destinations

---

## Question 15
**Objective:** AQE stage-by-stage re-optimization.

Why can AQE make better decisions than the static Catalyst optimizer alone for some queries?

A. AQE re-optimizes the physical plan using actual statistics gathered after each completed shuffle stage, not just pre-execution estimates
B. AQE ignores statistics entirely and always picks the same plan
C. AQE only works on queries with no joins
D. AQE re-writes the SQL query text before parsing

---

## Answer Key

### Q1: B
Spill on most tasks plus high GC time, with even task durations (no skew signature), points to memory pressure rather than skew or underparallelism.
**Wrong options:** A requires an imbalance in task duration/shuffle read, which isn't described. C would show under-count of tasks relative to cores, not spill/GC symptoms. D is a plan-level issue, not what spill/GC directly indicates.

### Q2: D
Roughly 2× cores with even durations and no spill is the described "good" zone from the diagnostic sequence — no fix is indicated.
**Wrong options:** A/B/C each solve a problem that isn't present per the given symptoms.

### Q3: B
AQE is enabled by default in current Databricks Runtime.
**Wrong options:** A, C, D all invent conditions/restrictions that don't reflect the default-on behavior.

### Q4: B
AQE's skew join optimization runs automatically with no code change required — the sensible first attempt before manual salting.
**Wrong options:** A skips the free, automatic option. C is counterproductive. D doesn't meaningfully address skew and harms parallelism broadly.

### Q5: B
AQE re-evaluates the plan after each shuffle stage using real statistics and can switch a `SortMergeJoin` to a `BroadcastHashJoin` when the data turns out to be small enough.
**Wrong options:** A and D describe pre-AQE static planning behavior. C understates AQE's scope — it also adjusts join strategy, not just partition count.

### Q6: B
Documented requirement: query owner, or `CAN MONITOR` on the SQL warehouse that executed it.
**Wrong options:** A overstates default visibility. C conflates data-layer UC grants with warehouse-level monitoring permission. D overstates the restriction — regular users can view profiles under the stated conditions.

### Q7: C
Shuffle is explicitly the data-redistribution operator and the most resource-expensive category, since it moves data across the network between executors.
**Wrong options:** A and D are read/combine operators, not redistribution. B combines relations but the actual data movement it may require is itself implemented via a shuffle operator, not "Join" as the redistribution signal.

### Q8: A
A high scanned-vs-pruned ratio on the Scan operator is the specific, documented signal for a data-skipping problem.
**Wrong options:** B relates to join strategy, not scanning efficiency. C is a Spark UI executor-level memory signal, unrelated to data skipping. D is an unrelated operator type.

### Q9: B
Enable verbose mode surfaces hidden low-impact operators and additional metrics.
**Wrong options:** A denies a documented feature. C and D invent mechanisms that don't exist.

### Q10: B
Query Profile is purpose-built for SQL warehouse (DBSQL/Photon) query diagnostics.
**Wrong options:** A is the general-cluster tool, not warehouse-specific. C and D don't provide per-query execution diagnostics.

### Q11: B
Validate against the specific metric that indicated the original problem (e.g., Max/Median duration ratio, shuffle read skew) rather than trusting wall-clock time alone, which can improve for unrelated reasons.
**Wrong options:** A, C, D don't actually confirm the root cause was addressed.

### Q12: B
`coalescePartitions` merges small post-shuffle partitions into appropriately-sized ones automatically.
**Wrong options:** A addresses skew, not partition sizing. C controls broadcast threshold, unrelated. D is the static (non-adaptive) partition count setting, not an AQE feature.

### Q13: C
`BroadcastExchange` specifically marks a broadcast join, sending the small table to all executors instead of shuffling the large table.
**Wrong options:** A is the generic shuffle marker (used by sort-merge joins, not broadcast). B is the join operator itself that would appear with `Exchange` in a shuffle-based join. D is an aggregation operator, unrelated to join strategy.

### Q14: B
All-purpose/job clusters running general Spark workloads are diagnosed via the Spark UI, not Query Profile (which is SQL-warehouse-specific).
**Wrong options:** A is scoped to the wrong compute type. C and D don't provide per-query execution diagnostics for this workload.

### Q15: A
AQE's advantage over static optimization is specifically that it uses real, post-shuffle-stage statistics to re-plan, catching cases where pre-execution estimates were wrong.
**Wrong options:** B, C, D all mischaracterize or invent AQE's actual mechanism.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Memory pressure signature (spill + GC%, even durations) |
| 2 | Medium | Recognizing a "no fix needed" healthy stage |
| 3 | Easy | AQE default-enabled status |
| 4 | Medium | AQE skew join as first attempt vs. manual salting |
| 5 | Hard | AQE dynamic join-strategy switching mid-query |
| 6 | Medium | Query Profile access requirement (`CAN MONITOR`) |
| 7 | Easy | Shuffle as the redistribution/expensive operator |
| 8 | Medium | Scanned-vs-pruned ratio = data skipping signal |
| 9 | Easy | Verbose mode purpose |
| 10 | Easy | Query Profile scope = SQL warehouses |
| 11 | Medium | Validating fixes against the actual metric, not wall-clock time |
| 12 | Medium | `coalescePartitions` vs. `skewJoin` vs. static shuffle partitions |
| 13 | Medium | `BroadcastExchange` vs. `Exchange` vs. `SortMergeJoin` |
| 14 | Easy | Tool choice by compute type (cluster vs. warehouse) |
| 15 | Hard | Why AQE beats static-only optimization |
