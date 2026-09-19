# Day 23 — Hands-On Lab: Debugging and Troubleshooting Failures End to End

## Lab Objectives

1. Read the **root cause** out of a wrapped Spark/Python error instead of the top-level wrapper.
2. Reproduce a **non-serializable closure** failure and a **UDF data-error** failure and fix both.
3. Reproduce a Delta **`ConcurrentAppendException`** on purpose, then fix it with disjoint partition predicates.
4. Reproduce a **streaming checkpoint incompatibility** and read `lastProgress` for evidence.
5. Build a small **repair-run simulator** to internalize retry vs. repair vs. re-run semantics, and prove that repair on a non-idempotent task **double-applies writes**.
6. Do the same on a **real multi-task Job**: fail a task, repair with a **parameter override**, observe repair history.
7. Break a cluster with a failing **init script**, read the event log, and configure **cluster log delivery**.
8. Run **system-table forensics** on failed tasks and slow queries (and verify the `result_state` vocabulary flagged in Day 23 notes).
9. Debug a **Lakeflow pipeline** using the pipeline UI and `event_log()`.

## Environment Matrix (read this first)

| Step | Topic | Community Edition | Free trial / paid workspace | Notes |
|---|---|---|---|---|
| 1 | Root-cause reading, UDF errors, closures | ✅ | ✅ | Any notebook |
| 2 | Delta concurrency conflict | ✅* | ✅ | *Needs Delta; on CE use `hive_metastore.default` (see below) |
| 3 | Streaming checkpoint break | ✅ | ✅ | Uses `rate` source only |
| 4 | Repair simulator | ✅* | ✅ | Pure Python + Delta table |
| 5 | Real Job + repair | ❌ (no Jobs) | ✅ | Read-only alternative: Step 4 covers the semantics |
| 6 | Init script failure + log delivery | ❌ | ✅ | Use a throwaway cluster; see access-mode caveat |
| 7 | System tables | ❌ | ⚠️ if `system` schemas enabled | Else read-only |
| 8 | Lakeflow pipeline debugging | ❌ | ✅ (pipeline compute) | Read-only alternative: read the code and event-log queries |

**Catalog note:** all SQL uses `main.default`. If you are on Community Edition or a non-Unity-Catalog workspace, replace `main.default.` with `hive_metastore.default.` everywhere (or drop the prefix and use `default.`). Statements that depend on UC-only features are called out.

**Time budget (~2 hours):** Steps 1–4 ≈ 50 min, Step 5 ≈ 25 min, Steps 6–8 ≈ 45 min. If you only have Community Edition, do Steps 1–4 fully and read Steps 5–8.

---

## Step 1 — Read the Root Cause, Not the Wrapper

**Objective:** PySpark wraps executor-side errors (`Py4JJavaError`/`PythonException`). The actionable text is at the **bottom** of the chain. Build a reusable helper so you practice extracting it.

```python
import re
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

def tail(e: Exception, n: int = 6) -> str:
    """Return the last n non-empty lines of an error message (root cause lives at the bottom)."""
    lines = [l for l in str(e).splitlines() if l.strip()]
    return "\n".join(lines[-n:])

print("helper ready")
```

### 1a. 💥 Break it on purpose: a UDF that fails on one bad record

```python
@F.udf(IntegerType())
def ratio(a, b):
    return a // b            # blows up when b == 0

df = spark.createDataFrame([(10, 2), (5, 0), (9, 3)], ["a", "b"])

try:
    df.select("a", "b", ratio("a", "b").alias("r")).collect()
except Exception as e:
    print("TOP LINE :", str(e).splitlines()[0][:200])
    print("--- ROOT CAUSE (last lines) ---")
    print(tail(e))
```

**Predict then verify:** does the top line tell you *which row* failed or *what* the error is? Does the bottom of the message name `ZeroDivisionError`? **Answer:** the top line is a generic job/stage/task failure header; the actionable `ZeroDivisionError: integer division or modulo by zero` (with the UDF line number) is near the bottom. In the Spark UI's failed stage view, the same exception appears in the failed-task table — check it and note which executor ran the task.

**Fix — guard against bad input instead of crashing the stage:**

```python
@F.udf(IntegerType())
def safe_ratio(a, b):
    return None if b in (None, 0) else a // b

df.select("a", "b", safe_ratio("a", "b").alias("r")).show()
```

Better still (Day 2 rule — prefer SQL functions): `F.when(F.col("b") != 0, (F.col("a") / F.col("b")).cast("int"))` needs no UDF and no Python worker at all.

### 1b. 💥 Break it on purpose: a non-serializable object captured by a UDF

```python
import threading

lock = threading.Lock()          # stand-in for a DB connection / client / SparkSession

@F.udf("int")
def uses_lock(x):
    with lock:                    # closure captures a non-picklable object
        return x + 1

try:
    spark.range(3).select(uses_lock("id")).collect()
except Exception as e:
    print("Expected — closure cannot be serialized to ship to executors:")
    print(tail(e, 4))
```

**What to observe:** the failure is a **serialization error on the driver side** (something like `cannot pickle '_thread.lock' object`) — the UDF never runs on an executor. This is the Python equivalent of Scala's `Task not serializable`.

**Fix — construct the resource inside the function (or per partition), never capture it:**

```python
@F.udf("int")
def no_capture(x):
    import threading            # created on the executor, not shipped from the driver
    with threading.Lock():
        return x + 1

spark.range(3).select(no_capture("id").alias("v")).show()
```

**Exam link:** "closure captures SparkSession/connection → not serializable → create inside `mapPartitions`/the UDF" (Notes Part 3).

---

## Step 2 — Reproduce and Fix a Delta `ConcurrentAppendException`

**Objective:** see optimistic-concurrency conflict detection (Day 9) fail a real write, and fix it by making the two writers touch **disjoint partitions**.

### 2a. Set up an unpartitioned table with deletion vectors OFF

Deletion vectors enable *row-level concurrency* (Day 11), which would mask the conflict — turn them off so the lab is deterministic.

```python
import threading, time
from pyspark.sql import functions as F

spark.sql("DROP TABLE IF EXISTS main.default.day23_conc")
spark.sql("""
    CREATE TABLE main.default.day23_conc (id BIGINT, region STRING, payload STRING)
    USING DELTA
    TBLPROPERTIES ('delta.enableDeletionVectors' = false)
""")

(spark.range(3_000_000)
    .select("id", F.lit("EU").alias("region"),
            F.sha2(F.col("id").cast("string"), 256).alias("payload"))
    .write.mode("append").saveAsTable("main.default.day23_conc"))

print(spark.table("main.default.day23_conc").count(), "rows loaded")
```

### 2b. 💥 Break it on purpose: a long UPDATE races a small INSERT

```python
def run_race(table: str, update_pred: str, insert_sql: str, delay: float = 4.0):
    result = {}

    def slow_update():
        try:
            spark.sql(f"""
                UPDATE {table}
                SET payload = sha2(concat(payload, cast(id AS STRING)), 256)
                WHERE {update_pred}
            """)
            result["update"] = "committed"
        except Exception as e:
            result["update"] = f"FAILED -> {type(e).__name__}: {str(e)[:250]}"

    t = threading.Thread(target=slow_update)
    t.start()
    time.sleep(delay)              # let the UPDATE read its snapshot first
    spark.sql(insert_sql)          # this small INSERT commits while the UPDATE is still running
    t.join()
    return result

res = run_race(
    "main.default.day23_conc",
    "region = 'EU'",
    "INSERT INTO main.default.day23_conc VALUES (-1, 'EU', 'late-arriving')",
)
print(res["update"])
```

**Predict then verify:** does the `UPDATE` commit? **Expected:** it fails with a `ConcurrentAppendException` (message along the lines of "files were added to the root of the table by a concurrent update"), because the table is unpartitioned, the `UPDATE` read the whole table, and the INSERT added a file the UPDATE's read predicate could have matched. **Timing caveat:** this is a race. If the UPDATE committed (finished before your INSERT), increase the table size (e.g., `range(10_000_000)`) or lower `delay` to `2.0` and rerun. If it *still* commits, check that deletion vectors really are off: `SHOW TBLPROPERTIES main.default.day23_conc`.

Confirm what happened in the log:

```sql
DESCRIBE HISTORY main.default.day23_conc;
```
You should see the INSERT commit but **no** UPDATE commit — the loser's work was discarded, not half-applied (atomicity).

### 2c. Fix — partition the table and keep writers disjoint

```python
spark.sql("DROP TABLE IF EXISTS main.default.day23_conc_part")
spark.sql("""
    CREATE TABLE main.default.day23_conc_part (id BIGINT, region STRING, payload STRING)
    USING DELTA
    PARTITIONED BY (region)
    TBLPROPERTIES ('delta.enableDeletionVectors' = false)
""")

(spark.range(3_000_000)
    .select("id", F.lit("EU").alias("region"),
            F.sha2(F.col("id").cast("string"), 256).alias("payload"))
    .write.mode("append").saveAsTable("main.default.day23_conc_part"))

res2 = run_race(
    "main.default.day23_conc_part",
    "region = 'EU'",                                                           # UPDATE touches EU only
    "INSERT INTO main.default.day23_conc_part VALUES (-1, 'US', 'other-partition')",  # INSERT touches US only
)
print(res2["update"])
```

**Expected:** `committed` — the writers touch disjoint partitions, and the partition column is in the UPDATE's predicate so Delta's conflict check can prove there is no overlap. **Key lesson:** the fix is *predicate design*, not retries or "unlocking" anything. Stretch: recreate the unpartitioned table with `'delta.enableDeletionVectors' = true` (needs DBR 14.2+) and rerun 2b — row-level concurrency should let both commits succeed.

---

## Step 3 — Streaming Checkpoint Incompatibility (and Reading `lastProgress`)

**Objective:** reproduce the "changed a stateful query, reused the old checkpoint" failure and collect the evidence Structured Streaming gives you (Day 14).

```python
import time
from pyspark.sql import functions as F

chk = "/tmp/day23/chk_state"
dbutils.fs.rm(chk, True)

def noop(batch_df, batch_id):
    pass

def start(key_col):
    src = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    agg = src.groupBy(key_col.alias("k")).count()
    return (agg.writeStream
        .outputMode("update")
        .foreachBatch(noop)
        .option("checkpointLocation", chk)
        .trigger(processingTime="3 seconds")
        .start())

q1 = start(F.col("value") % 10)          # grouping key is BIGINT
time.sleep(15)
p = q1.lastProgress
print("batchId              :", p["batchId"])
print("inputRowsPerSecond   :", p["inputRowsPerSecond"])
print("processedRowsPerSecond:", p["processedRowsPerSecond"])
print("durationMs           :", p["durationMs"])           # addBatch / triggerExecution / walCommit ...
print("stateOperators       :", p["stateOperators"])      # numRowsTotal = state rows held
q1.stop()
```

**What to observe:** `inputRowsPerSecond` vs. `processedRowsPerSecond` (persistently input > processed = backlog), `durationMs.triggerExecution` vs. the 3-second trigger, and `stateOperators[0].numRowsTotal` (≈ 10 groups).

### 💥 Break it on purpose: change the state schema, reuse the checkpoint

```python
q2 = None
try:
    q2 = start((F.col("value") % 10).cast("string"))   # key type changed: BIGINT -> STRING
    q2.awaitTermination(20)                            # raises if the query failed
    print("Query still running — no failure observed (see note below)")
except Exception as e:
    print("Expected — existing state was written with a different key schema:")
    print(tail(e, 5))
finally:
    if q2 is not None and q2.isActive:
        q2.stop()
```

**Expected:** the query fails on restart with a state-schema-compatibility error (exact wording/error-class varies by DBR version, e.g. "Provided schema doesn't match to the schema for existing state"). **Why:** the checkpoint holds versioned state whose key schema no longer matches the query. **Fix options:** revert the change; or start a **new** `checkpointLocation` — which means state starts empty and the source is reprocessed from its starting offset (plan for duplicates with a non-idempotent sink).

**Contrast (quiet failure):** change the key expression but keep the *same type* (e.g., `% 10` → `% 5`, both BIGINT) and restart on the same checkpoint. It typically starts without error and silently reuses old state built under the old semantics. Try it — this is why "the query started fine" is not evidence the change was safe.

```python
dbutils.fs.rm("/tmp/day23", True)   # cleanup
```

---

## Step 4 — Repair-Run Simulator: Retry vs. Repair vs. Re-run (all environments)

**Objective:** build the mental model before touching a real Job. The simulator mimics a 4-task job:

```
A ──► B ──┐
 └──► C ──┴──► D
```

### 4a. Build it

```python
AUDIT = "main.default.day23_sim_audit"
spark.sql(f"DROP TABLE IF EXISTS {AUDIT}")
spark.sql(f"CREATE TABLE {AUDIT} (task STRING, step STRING) USING DELTA")

def log(task, step):
    (spark.createDataFrame([(task, step)], "task string, step string")
        .write.mode("append").saveAsTable(AUDIT))

def task_a(p): log("A", "only_step")
def task_b(p): log("B", "only_step")
def task_d(p): log("D", "only_step")

def task_c_naive(p):
    """Two NON-atomic writes with a failure between them."""
    log("C", "step1")
    if p.get("fail_c") == "true":
        raise RuntimeError("simulated failure after step1")
    log("C", "step2")

def task_c_idempotent(p):
    """Same work, but each step is a MERGE keyed on (task, step) — safe to rerun."""
    for step in ("step1", "step2"):
        if step == "step2" and p.get("fail_c") == "true":
            raise RuntimeError("simulated failure after step1")
        spark.sql(f"""
            MERGE INTO {AUDIT} t
            USING (SELECT 'C' AS task, '{step}' AS step) s
            ON t.task = s.task AND t.step = s.step
            WHEN NOT MATCHED THEN INSERT *
        """)

class MiniJob:
    ORDER = ["A", "B", "C", "D"]
    DEPS = {"A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]}

    def __init__(self, funcs):
        self.funcs = funcs
        self.state = {t: "PENDING" for t in self.ORDER}
        self.attempts = {t: 0 for t in self.ORDER}

    def _execute(self, todo, params):
        for t in self.ORDER:
            if t not in todo:
                continue
            if any(self.state[d] != "SUCCESS" for d in self.DEPS[t]):
                self.state[t] = "UPSTREAM_FAILED"          # symptom, not root cause
                continue
            self.attempts[t] += 1
            try:
                self.funcs[t](params)
                self.state[t] = "SUCCESS"
            except Exception as e:
                self.state[t] = f"FAILED ({e})"

    def run(self, params):                                 # "Run now": everything, new run
        self.state = {t: "PENDING" for t in self.ORDER}
        self._execute(set(self.ORDER), params)

    def repair(self, params):                              # "Repair run": failed + dependents only
        todo = {t for t in self.ORDER if self.state[t] != "SUCCESS"}
        self._execute(todo, params)

    def show(self, title):
        print(f"\n== {title} ==")
        for t in self.ORDER:
            print(f"  {t}: {self.state[t]:<55} attempts={self.attempts[t]}")

def audit():
    spark.table(AUDIT).orderBy("task", "step").show()
```

### 4b. First run fails; observe the states

```python
job = MiniJob({"A": task_a, "B": task_b, "C": task_c_naive, "D": task_d})
job.run({"fail_c": "true"})
job.show("After first run")
audit()
```

**Expected:** A = SUCCESS, B = SUCCESS, C = FAILED, **D = UPSTREAM_FAILED** (a symptom). The audit table shows `C/step1` — the partial write from the failed task **is still there** (no rollback across the task).

### 4c. Repair with a parameter override

```python
job.repair({"fail_c": "false"})        # override: fix the "bad parameter"
job.show("After repair (fail_c=false)")
audit()
```

**Predict then verify:** did A and B rerun? **No** — their `attempts` stay at 1 (their results are reused, and the new parameter never reaches them). Only C and D ran.

### 4d. 💥 Break it on purpose: repair on a non-idempotent task

Look at the audit table after 4c: **`C/step1` appears twice** (once from the failed attempt, once from the repair). Repair reran C *from the beginning* and re-applied the first write.

```python
spark.sql(f"SELECT task, step, COUNT(*) AS n FROM {AUDIT} GROUP BY task, step HAVING COUNT(*) > 1").show()
```

### 4e. Fix — make the task idempotent, then repeat

```python
spark.sql(f"TRUNCATE TABLE {AUDIT}")
job2 = MiniJob({"A": task_a, "B": task_b, "C": task_c_idempotent, "D": task_d})
job2.run({"fail_c": "true"})
job2.repair({"fail_c": "false"})
job2.show("Idempotent C after repair")
spark.sql(f"SELECT task, step, COUNT(*) AS n FROM {AUDIT} GROUP BY task, step ORDER BY task, step").show()
```

**Expected:** every `(task, step)` appears exactly once. **Lesson (Notes Part 6):** Delta is atomic **per statement**, not per task; repair (and retry!) are only safe on idempotent tasks. Also note the difference: a *task retry* (`max_retries`) reruns the same failing task automatically and would duplicate `C/step1` in exactly the same way.

### 4f. Contrast: re-run everything

```python
job2.run({"fail_c": "false"})
job2.show("Full re-run (new run)")
print("A attempts:", job2.attempts["A"], "(re-executed — repair would have reused it)")
```

---

## Step 5 — A Real Multi-Task Job: Fail, Repair, Override (paid/trial workspace)

**Community Edition:** no Jobs — read this step and rely on Step 4.

### 5a. Create the task notebook

Create a notebook at `/Workspace/Shared/day23/task_notebook` with this content:

```python
dbutils.widgets.text("task_name", "A")
dbutils.widgets.text("fail_c", "true")
dbutils.widgets.text("mode", "naive")          # naive | idempotent

task = dbutils.widgets.get("task_name")
fail = dbutils.widgets.get("fail_c") == "true"
mode = dbutils.widgets.get("mode")

TABLE = "main.default.day23_job_audit"
spark.sql(f"CREATE TABLE IF NOT EXISTS {TABLE} (task STRING, step STRING) USING DELTA")

def naive(step):
    spark.createDataFrame([(task, step)], "task string, step string").write.mode("append").saveAsTable(TABLE)

def idem(step):
    spark.sql(f"""MERGE INTO {TABLE} t
                  USING (SELECT '{task}' AS task, '{step}' AS step) s
                  ON t.task = s.task AND t.step = s.step
                  WHEN NOT MATCHED THEN INSERT *""")

write = idem if mode == "idempotent" else naive

if task == "C":
    write("step1")
    if fail:
        raise RuntimeError("Task C failing on purpose (fail_c=true)")
    write("step2")
else:
    write("only_step")
print(f"task {task} finished (mode={mode}, fail_c={fail})")
```

### 5b. Create the job (UI)

1. **Workflows → Create job**, name `day23_repair_lab`.
2. Add four **notebook tasks**, all pointing at `task_notebook`, with these **task base parameters** and dependencies:

| Task key | `task_name` | Depends on |
|---|---|---|
| `A` | `A` | — |
| `B` | `B` | `A` |
| `C` | `C` | `A` |
| `D` | `D` | `B`, `C` |

3. Job-level **parameters**: `fail_c = true`, `mode = naive`.
4. Compute: serverless, or a small single-node job cluster. Leave **retries at 0** on every task (default).
5. **Run now.**

### 5c. Observe the failure (the evidence you'd use in the exam)

- Run page: A ✅, B ✅, **C ❌**, D shows **Upstream failed**. Open C's output — the root-cause line is `Task C failing on purpose`.
- Query the table: `SELECT * FROM main.default.day23_job_audit ORDER BY task, step;` — A, B, and `C/step1` are present (nothing rolled back — the official sample-question behavior).

### 5d. Repair with a parameter override

1. On the failed run page choose **Repair run**.
2. Leave the failed task (C) and its dependent (D) selected; in the parameters section change **`fail_c` → `false`**; submit.
3. Observe: only C and D execute; **A and B are not rerun**; the run keeps the **same run ID** and gains a **repair history** entry.
4. Check the table again — you should now see **`C/step1` twice** (the naive task double-applied on repair).
5. Open the **job definition** — confirm `fail_c` is still `true`. The override was **one-time**, not persisted.

### 5e. 💥 Break it on purpose → fix: idempotent mode

```sql
TRUNCATE TABLE main.default.day23_job_audit;
```
Set the job parameters to `fail_c = true`, `mode = idempotent`, **Run now** (C fails again), then **Repair run** with `fail_c = false`. Query the audit table: each `(task, step)` appears exactly **once**.

### 5f. Retry vs. repair

Edit task C: set **Retries = 1** (min retry interval 0), job params `fail_c = true`, `mode = naive`, truncate the audit table, run. Observe C's **attempt 2** happens automatically inside the same run, fails identically (deterministic error), and again leaves `C/step1` written **twice**. **Takeaway:** retries suit *transient* errors; a deterministic failure needs a fix + repair, and both need idempotent tasks.

**Cleanup:** delete the job and drop the audit table when done.

---

## Step 6 — Cluster Failure: Init Script Break + Log Delivery (paid/trial workspace)

**Objective:** use the right evidence when the cluster (not Spark) is what failed.

**Access-mode caveat:** on **standard (shared) access mode**, init scripts stored in Volumes must be **allowlisted by a metastore admin**. Use a **dedicated (single-user) access mode** throwaway cluster, or an allowlisted path, otherwise the script won't run and you'll see a different error.

### 6a. Create the failing init script

```python
spark.sql("CREATE VOLUME IF NOT EXISTS main.default.day23_ops")

dbutils.fs.put(
    "/Volumes/main/default/day23_ops/fail_init.sh",
    """#!/bin/bash
echo "day23 init script: failing on purpose"
exit 1
""",
    True,
)
```

### 6b. 💥 Break it on purpose

1. Create a **new** small dedicated-mode cluster named `day23-init-fail` (do not modify a real cluster).
2. **Advanced options → Init scripts → Source: Volumes → Path:** `/Volumes/main/default/day23_ops/fail_init.sh`.
3. Start it and wait for it to fail.

**What to observe (record each):**
- Termination reason is an **init script failure** (e.g., `INIT_SCRIPT_FAILURE`), not a Spark error.
- **Compute → Event log** shows the lifecycle: starting → init scripts started → failure → terminated. There is **no Spark UI**, because no Spark application ever started.
- Init-script output is only retrievable if log delivery is configured (next step) or from the cluster's init-script log view where available.

**Predict then verify:** which tool would you open first for this symptom? **Answer:** the cluster **event log**/termination reason and the init script log — not the Spark UI (Notes Part 2).

### 6c. Fix and capture logs with cluster log delivery

1. Remove the init script from the cluster (or edit it to `exit 0`).
2. Under **Advanced options → Logging**, set destination type **Volumes** and path `/Volumes/main/default/day23_ops/cluster_logs`.
3. Start the cluster, run a notebook cell that prints something and then raises an error:

```python
print("day23 driver stdout marker")
raise RuntimeError("day23 deliberate driver error")
```

4. Wait ~5–10 minutes (delivery is periodic), then list what was delivered:

```python
base = "/Volumes/main/default/day23_ops/cluster_logs"
for cluster_dir in dbutils.fs.ls(base):
    print(cluster_dir.path)
    for sub in dbutils.fs.ls(cluster_dir.path):
        print("   ", sub.path)          # expect driver/, executor/, eventlog/ (and init_scripts/ if any ran)
```

Open the delivered driver `stdout`/`log4j` and find your marker and error. Terminate the cluster, then confirm the logs are **still there** — this is what you would have needed for an ephemeral **job cluster**. Delete the throwaway cluster afterward.

---

## Step 7 — System-Table Forensics (needs `system` schemas enabled)

**Objective:** answer "what failed, why, and was it resource-related?" with SQL (Day 21 schemas; Day 23 notes Part 5). If you lack access, read the queries and predicted outcomes.

### 7a. Verify the `result_state` vocabulary (Day 22 correction check)

```sql
SELECT DISTINCT result_state FROM system.lakeflow.job_run_timeline;
```
**Predict then verify:** do you see `SUCCEEDED` or `SUCCESS`? Write down the exact values. If the value is `SUCCEEDED`, the Day 22 filter `result_state = 'SUCCESS'` matches nothing — go fix `day-22-alerting-and-notifications/hands_on_lab.md` Step 7 (and the same query in the notes).

### 7b. 💥 Break it on purpose: count rows vs. runs

```sql
SELECT COUNT(*) AS timeline_rows,
       COUNT(DISTINCT run_id) AS distinct_runs
FROM system.lakeflow.job_run_timeline
WHERE period_start_time > current_timestamp() - INTERVAL 7 DAYS;
```
`timeline_rows` ≥ `distinct_runs`, because runs are recorded in period slices (a run crossing a clock-hour boundary produces multiple rows). If they are equal, all your runs fit inside one slice — trigger a run that lasts across an hour boundary, or accept the documented behavior. **Correct final outcome per run:**

```sql
SELECT job_id, run_id, result_state, termination_code, period_end_time
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY period_end_time DESC) AS rn
  FROM system.lakeflow.job_run_timeline
  WHERE period_start_time > current_timestamp() - INTERVAL 7 DAYS
)
WHERE rn = 1 AND result_state <> 'SUCCEEDED'      -- adjust to the value you found in 7a
ORDER BY period_end_time DESC;
```
Find the run you failed in Step 5 and confirm its `termination_code`.

### 7c. Failed *tasks* and compute pressure

```sql
WITH failed AS (
  SELECT run_id, task_key, explode(compute_ids) AS cluster_id,
         period_start_time, period_end_time
  FROM system.lakeflow.job_task_run_timeline
  WHERE result_state IS NOT NULL AND result_state <> 'SUCCEEDED'
    AND period_start_time > current_timestamp() - INTERVAL 7 DAYS
)
SELECT f.run_id, f.task_key,
       MAX(n.mem_used_percent) AS peak_mem_pct,
       MAX(n.cpu_user_percent + n.cpu_system_percent) AS peak_cpu_pct
FROM failed f
JOIN system.compute.node_timeline n
  ON n.cluster_id = f.cluster_id
 AND n.start_time BETWEEN f.period_start_time AND f.period_end_time
GROUP BY f.run_id, f.task_key
ORDER BY peak_mem_pct DESC;
```
**Interpret:** your Step 5 failure was a deliberate exception — expect **low** memory. That is the point: a failed task with normal utilization is a **code/data** problem, not a capacity problem. (Serverless tasks won't have classic `node_timeline` rows.)

### 7d. A slow or failed SQL warehouse query

Run this on a **SQL warehouse**, then find it:

```sql
SELECT count(*) FROM range(50000000) r1 CROSS JOIN range(20) r2;   -- takes noticeable time
```
```sql
SELECT statement_id, execution_status, error_message,
       total_duration_ms, waiting_for_compute_duration_ms,
       waiting_at_capacity_duration_ms, execution_duration_ms,
       read_bytes, spilled_local_bytes
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY start_time DESC
LIMIT 5;
```
Classify where the time went using the decision table in the notes (compute start vs. queued vs. execution). Then open the same statement's **Query Profile** from Query History and match the operator that dominates. (History rows can lag by a few minutes.)

### 7e. Permission-change forensics

```sql
SELECT event_time, user_identity.email, action_name, request_params
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
  AND event_time > current_timestamp() - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```
Recall a grant/revoke you made in the Day 18 lab and find it here — this is how you'd answer "it worked yesterday, `PERMISSION_DENIED` today."

---

## Step 8 — Debug a Lakeflow Pipeline (needs pipeline compute)

**Community Edition:** read only — predict each outcome before reading the answer.

### 8a. Create the pipeline source

In a **regular notebook**, create the append-only source table used later:

```sql
CREATE TABLE IF NOT EXISTS main.default.day23_source (order_id INT, amount DOUBLE);
INSERT INTO main.default.day23_source VALUES (1, 100.0), (2, 50.0);
```

Create a **pipeline notebook** and create a pipeline (Development mode ON, target schema `main.default`) with this code:

```python
import dlt

@dlt.table(comment="Bronze demo data (batch)")
def day23_bronze():
    return spark.sql("""
        SELECT * FROM VALUES (1,'alice',100.0), (2,'bob',-5.0), (3,'carol',50.0)
        AS t(order_id, customer, amount)
    """)

@dlt.table(comment="Silver with a HARD data-quality gate")
@dlt.expect_or_fail("amount_positive", "amount > 0")
def day23_silver():
    return dlt.read("day23_bronze")
```

### 8b. 💥 Break it on purpose: `expect_or_fail` halts the update

Start an update. **Observe in the UI:** `day23_bronze` completes, `day23_silver` fails, the update state is FAILED, and the error banner names the `amount_positive` expectation. Note that in **Development** mode it failed immediately with no automatic retry.

Now query the evidence in a regular notebook/SQL editor (use the pipeline ID from the pipeline settings):

```sql
SELECT timestamp, level, event_type, message
FROM event_log('<pipeline_id>')
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC
LIMIT 20;

SELECT timestamp, origin.flow_name,
       details:flow_progress.data_quality.expectations AS expectations
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 20;
```
(As in Day 21, only the pipeline owner can call `event_log()` directly by default; if your workspace requires the table-reference form of `event_log`, use the syntax from your pipeline's event-log settings.)

**Predict then verify:** do you need the Spark UI to find the root cause here? **Answer:** no — the cause is a Lakeflow-level data-quality rule, visible in the pipeline UI and event log (Notes Part 7).

### 8c. Fix deliberately and compare metrics

Change the silver table to drop instead of halt:

```python
@dlt.table(comment="Silver: drop bad rows, keep metrics")
@dlt.expect_or_drop("amount_positive", "amount > 0")
def day23_silver():
    return dlt.read("day23_bronze")
```
Rerun. The update succeeds; re-run the second event-log query and locate the expectation's passed/failed counts (1 failed record). **Decision point:** you turned a *hard gate* into a *silent drop* — in production that trade-off should be a conscious choice plus a SQL Alert on the failed-record metric (Day 22), not a way to make the red icon go away.

### 8d. 💥 Break it on purpose: a typo in a dataset name → use **Validate**

Add to the pipeline code:

```python
@dlt.table
def day23_gold():
    return dlt.read("day23_bronzee")      # typo: dataset does not exist
```
Instead of a normal update, run a **Validate** update. **Observe:** it reports the unresolved dataset **without processing any data** — the cheap way to catch definition errors. Fix the typo.

### 8e. Reasoning check — full refresh risk and `pipelines.reset.allowed` (read-only)

Add a streaming table on the append-only source, with the protection property:

```python
@dlt.table(table_properties={"pipelines.reset.allowed": "false"})
def day23_protected():
    return spark.readStream.table("main.default.day23_source")

@dlt.table
def day23_unprotected():
    return spark.readStream.table("main.default.day23_source")
```
Run a normal update (both tables hold 2 rows). **Question:** if the upstream system now discards its history (imagine the source's retention expired) and you run **Full refresh all**, what happens to each table? **Answer:** a full refresh **clears the target data and streaming checkpoints and reprocesses from the source**, so `day23_unprotected` would be rebuilt from whatever the source still has — history permanently lost — while `pipelines.reset.allowed = false` marks `day23_protected` as **not resettable** by full refresh. (Don't actually delete the source in this lab: incremental streaming reads of a table with deletes fail for a different reason. Verify the property with `SHOW TBLPROPERTIES main.default.day23_protected`.) Confirm current behavior details for your DBR/pipeline channel in the docs.

**Cleanup:** delete the pipeline and drop `day23_*` tables.

---

## Stretch Task — Automate the First Look at a Failed Run

Write a notebook cell that, given a failed `run_id`, pulls the task-level facts you'd read manually (Day 24 builds on this with repair calls):

```python
import requests

host = "https://<your-workspace-url>"
token = dbutils.secrets.get(scope="monitoring", key="pat_token")   # never hardcode a token
run_id = <your-failed-run-id>

r = requests.get(
    f"{host}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"run_id": run_id},
).json()

print("Run state:", r["state"])
for t in r.get("tasks", []):
    s = t["state"]
    print(f'{t["task_key"]:<6} life_cycle={s.get("life_cycle_state"):<12} '
          f'result={s.get("result_state")!s:<12} msg={s.get("state_message","")[:80]}')
```
Then extend it to print only the **first** failed task (skip `UPSTREAM_FAILED` ones) — the "debug the first failure" rule in code. Note in the API the run/task results use the **REST vocabulary** (`SUCCESS`/`FAILED`), which differs from the system tables (Step 7a).

---

## Cleanup

```sql
DROP TABLE IF EXISTS main.default.day23_conc;
DROP TABLE IF EXISTS main.default.day23_conc_part;
DROP TABLE IF EXISTS main.default.day23_sim_audit;
DROP TABLE IF EXISTS main.default.day23_job_audit;
DROP TABLE IF EXISTS main.default.day23_source;
DROP VOLUME IF EXISTS main.default.day23_ops;
```
Also delete the `day23_repair_lab` job, the `day23-init-fail` cluster, and the pipeline.

---

## Lab Checklist

- [ ] Extracted a root cause from the bottom of a wrapped Spark/Python error and found the failing task in the Spark UI
- [ ] Reproduced and fixed a non-serializable closure failure
- [ ] Reproduced a Delta `ConcurrentAppendException` and fixed it with a partitioned table and disjoint predicates
- [ ] Confirmed via `DESCRIBE HISTORY` that the conflicting write left no partial commit
- [ ] Reproduced a streaming checkpoint incompatibility and read `lastProgress` (input vs. processed rate, `durationMs`, state rows)
- [ ] Built the repair simulator and confirmed repair reruns only failed + dependent tasks
- [ ] Proved repair on a non-idempotent task double-applies writes, then fixed it with `MERGE`
- [ ] (Paid) Repaired a real multi-task Job with a parameter override; confirmed the override didn't persist
- [ ] (Paid) Observed retry vs. repair on the same deterministic failure
- [ ] (Paid) Broke a cluster with a failing init script and diagnosed it from the event log, not the Spark UI
- [ ] (Paid) Configured cluster log delivery and listed delivered driver/executor/event logs
- [ ] Verified the system-table `result_state` values (and corrected Day 22 if needed)
- [ ] Explained `COUNT(*)` vs. `COUNT(DISTINCT run_id)` in `job_run_timeline`
- [ ] Classified a query's time using `system.query.history` duration columns
- [ ] (Pipeline compute) Diagnosed an `expect_or_fail` failure from the UI and `event_log()`, and used **Validate** to catch a definition error
- [ ] Explained why a full refresh can permanently lose data and what `pipelines.reset.allowed` protects
- [ ] (Stretch) Scripted a first-look summary of a failed run via `runs/get`

---

## Cross-References
- Day 2: `%pip` is driver-scoped — the `ModuleNotFoundError`-in-UDF signature and UDF preference order.
- Day 4/7: Driver/executor OOM, `maxResultSize` (not re-run here — see those labs).
- Day 8: Spark UI failed-stage view and the Query Profile you matched in Step 7d.
- Day 9/11: Optimistic concurrency and deletion-vector row-level concurrency (Step 2 stretch).
- Day 13/14: Checkpoints, `lastProgress`, state store.
- Day 15/16: Expectations, Development vs. Production mode, streaming-table limitations.
- Day 18/21: UC audit trail and system-table schemas used in Step 7.
- Day 22: The `result_state` filter you verified in Step 7a — fix the Day 22 lab if it used `'SUCCESS'`.
- Day 24: Jobs REST API/CLI — the `runs/repair` call, `rerun_tasks`, `latest_repair_id`, and API-driven parameter overrides that automate Step 5.
