# Day 8 — Hands-On Lab: Spark UI and Query Profile

## Lab Objectives

1. Practice the systematic Stages-tab bottleneck-diagnosis workflow on a real query.
2. Observe AQE dynamically converting a sort-merge join to a broadcast join.
3. Observe AQE's automatic skew-join handling and compare it against AQE disabled.
4. Read a physical plan for `Exchange`/`BroadcastExchange` nodes in the SQL/DataFrame tab.
5. (If you have SQL warehouse access) Read a Query Profile for Scan/Join/Shuffle operators and the scanned-vs-pruned data-skipping signal.
6. Break AQE on purpose and observe the difference.

**Environment note:** Steps 1–4 work on any cluster (Community Edition included) using the Spark UI. Step 5 requires a Databricks SQL warehouse — if you don't have one, read the "what to observe" notes; the operator names and insight categories are exam-identical to what you'd see live.

---

## Step 1 — Build a query with an intentional inefficiency

```python
from pyspark.sql import functions as F

# Large-ish fact table
fact = spark.range(2_000_000).withColumn("key", (F.col("id") % 1000))
# Small dimension table, but NOT hinted as broadcast — let's see what Spark decides
dim = spark.createDataFrame([(i, f"name_{i}") for i in range(1000)], ["key", "name"])

result = fact.join(dim, "key").groupBy("name").count()
result.collect()
```

Open the **SQL/DataFrame tab** for this query and look at the physical plan.

**What to observe:** with AQE enabled (default), check whether the plan shows `BroadcastHashJoin`/`BroadcastExchange` or `SortMergeJoin`/`Exchange` on both sides. Because `dim` is small, AQE (or even the static optimizer, since it's well under the default 10MB threshold) should pick a broadcast strategy without any hint from you.

---

## Step 2 — Force the inefficient path, then compare

```python
# Disable auto-broadcast to force a sort-merge join, simulating a case where
# Spark's static size estimate was wrong (e.g., dim came from a complex upstream query)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

result_smj = fact.join(dim, "key").groupBy("name").count()
result_smj.collect()

# Reset
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # back to 10MB default
```
**What to observe in the SQL tab:** now you should see `SortMergeJoin` with `Exchange` on both `fact` and `dim` — the "inefficient join type" category from the exam objective, reproduced on purpose. Compare stage duration against Step 1.

---

## Step 3 — Trigger skew and watch AQE handle it automatically

```python
# 90% of rows share one key
heavy = [("heavy", i) for i in range(900_000)]
light = [(f"key_{i}", i) for i in range(100_000)]
df_skew = spark.createDataFrame(heavy + light, ["key", "value"])

print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"AQE skew join enabled: {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")

result_skew = df_skew.groupBy("key").count().collect()
```
**What to observe in the Stages tab:** look at the task duration histogram for the shuffle stage. With AQE's skew join optimization on, the "heavy" partition should be split into several sub-tasks running in parallel rather than one task processing 900k rows alone — check the task count and duration spread.

---

## Step 4 — 💥 Break it on purpose: disable AQE and re-run

```python
spark.conf.set("spark.sql.adaptive.enabled", "false")

result_skew_no_aqe = df_skew.groupBy("key").count().collect()

# Reset immediately after — don't leave AQE off for the rest of the notebook
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
**Predict then verify:** does the Stages tab now show one dramatically longer task instead of several evenly-split sub-tasks?
**Answer:** yes — with AQE disabled, the skewed partition is processed by a single task end-to-end, reproducing the "Max Duration >> Median Duration" skew signature from Part 1 of the notes with no automatic mitigation. This is the exact comparison a "what does AQE actually do" exam question is testing.

---

## Step 5 — Run the three-question diagnostic on your own slow stage

Pick the longest stage from any exercise above (or an earlier day's lab) and manually work through the sequence from `notes.md`:
1. Stages tab → sort by Duration → click the longest stage.
2. Record: median task duration, max task duration, median vs. max shuffle read.
3. Answer the three questions in order (skew? memory pressure? underparallelism?) before proposing any fix.
4. Only if none of those fit, open the SQL/DataFrame tab for a join-strategy/pushdown problem.

Write down which branch of the diagnostic tree your stage fell into and why — this rehearses the exact reasoning path a scenario question expects.

---

## Step 6 — (If you have a SQL warehouse) Read a Query Profile

```sql
-- Run this on a SQL warehouse, then open its Query Profile from Query History
SELECT d.name, SUM(f.value) AS total
FROM range(2000000) f
JOIN (SELECT id AS key, concat('name_', id) AS name FROM range(1000)) d
  ON f.id % 1000 = d.key
GROUP BY d.name;
```
In the Query Profile: identify the **Scan** operator(s) and check bytes/files read vs. pruned; identify the **Join** operator and confirm which strategy was chosen; identify any **Shuffle** operator and note what fraction of total time it consumes. Toggle **Enable verbose mode** if any operator's detail looks incomplete.

---

## Lab Checklist

- [ ] Observed a small-table join automatically pick a broadcast strategy
- [ ] Forced a sort-merge join and compared its plan/duration against the broadcast version
- [ ] Observed AQE automatically splitting a skewed partition into sub-tasks
- [ ] Disabled AQE and reproduced the un-mitigated skew signature, then re-enabled AQE
- [ ] Ran the full three-question diagnostic sequence on a real stage
- [ ] (If available) Read a Query Profile's Scan/Join/Shuffle operators and data-skipping metric

---

## Cross-References
- Day 5: Physical plan operators (`Exchange`, `BroadcastExchange`, `SortMergeJoin`).
- Day 6: Manual salted-join skew fix — the fallback when AQE is off or insufficient.
- Day 7: Spill/GC metrics used in the "memory pressure" diagnostic branch.
- Day 9–11: The storage-layer fixes (Z-Ordering, Liquid Clustering, partitioning) for a bad-data-skipping Scan operator.
