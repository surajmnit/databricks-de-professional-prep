# Day 23 — Debugging and Troubleshooting: Spark UI, Cluster Logs, System Tables, Query Profiles, Job Repair, and Lakeflow Pipeline Debugging

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 9: Debugging and Deploying (10%)** — the *Debugging and Troubleshooting* half (Deploying/CI-CD is Day 25):
- "Identify pertinent diagnostic information using Spark UI, cluster logs, system tables, and query profiles to troubleshoot errors."
- "Analyze the errors and remediate the failed job runs with job repairs and parameter overrides."
- "Use Lakeflow Spark Declarative Pipelines event logs and the Spark UI to debug Lakeflow Spark Declarative Pipelines and Spark pipelines."

Also touches **Section 1** ("...including a built-in debugger") and the official sample question on multi-task job failure (Exam Guide Q9).

*(This day is a **triage and evidence-selection** day. The mechanics are already taught — Spark UI diagnostic sequence and Query Profile: Day 8; memory/OOM: Day 7; system tables and event-log schema: Day 21; alert routing: Day 22. Don't re-derive them; this day teaches which artifact answers which question, and how to remediate. Job/pipeline REST calls, `parameter overrides` syntax, and pipeline failure remediation via API are Day 24.)*

---

## Part 1 — The Triage Ladder: Which Layer Failed?

The exam rarely asks "what does this error mean" in isolation. It gives a symptom and asks **which artifact you open first** or **which remediation is correct**. Classify the failure layer first, then pick the evidence source.

| Layer | Typical symptom | First evidence source |
|---|---|---|
| **Orchestration** (Jobs) | Task shows `UPSTREAM_FAILED`, `SKIPPED`, `TIMEDOUT`, run never started | Job run page → task result state + termination code; `system.lakeflow.job_run_timeline` / `job_task_run_timeline` |
| **Compute / cluster** | Cluster failed to start, init script failure, library install failure, driver unresponsive, nodes lost | Cluster **event log** + init script logs + termination reason; delivered cluster logs |
| **Spark execution** | `Lost task`, `ExecutorLostFailure`, `FetchFailedException`, one slow task, spill | Spark UI (Stages/Executors/Storage — Day 8), driver `log4j`/`stderr`, executor logs |
| **SQL warehouse query** | Slow or failed dashboard/alert query | **Query Profile** + `system.query.history` (error_message, durations) |
| **Code / data** | `AnalysisException`, schema mismatch, Delta concurrency exception, `ModuleNotFoundError` | Full stack trace ("Caused by:" chain), Delta history (`DESCRIBE HISTORY`) |
| **Permissions** | `PERMISSION_DENIED`, insufficient privileges | UC traversal chain (Day 18) + `system.access.audit` (Day 21) |
| **Lakeflow pipeline** | Update `FAILED`, expectation violations, flow errors | Pipeline UI update details + **event log** (`event_log()`) |
| **Streaming** | Backlog growing, batch duration > trigger, checkpoint errors | Structured Streaming tab / `lastProgress` (Day 14) |

**Rule of thumb for scenario questions:** compute type in the stem decides the tool.
- SQL warehouse → Query Profile.
- All-purpose/job cluster → Spark UI + cluster logs.
- Lakeflow pipeline → event log first, Spark UI second.
- Serverless compute → no classic cluster logs/Spark UI to open; rely on run output, query profile/history, and system tables (verify current serverless observability surface in docs — it has been evolving).

---

## Part 2 — Cluster Logs and Cluster Events

Three different things get called "logs." The exam distinguishes them.

| Artifact | What it contains | Use when |
|---|---|---|
| **Cluster event log** (Compute → Event log tab) | Lifecycle events: `CREATING`, `STARTING`, `RUNNING`, `RESIZING`, `NODES_LOST`, `DRIVER_NOT_RESPONDING`, `INIT_SCRIPTS_STARTED/FINISHED`, `TERMINATING` + termination reason | Cluster never became healthy, unexpected termination, spot/preemption loss, autoscaling behavior |
| **Driver logs** (`stdout`, `stderr`, `log4j-active.log`) | Notebook `print` output, Python tracebacks, JVM/Spark driver log lines | Driver-side errors, `collect()` failures, driver GC messages |
| **Executor logs** (Spark UI → Executors → stdout/stderr, or delivered logs) | Per-executor task errors, `OutOfMemoryError`, Python worker crashes | `Lost task`/`ExecutorLostFailure`, UDF failures |
| **Init script logs** | Output of each init script | Cluster stuck starting, `INIT_SCRIPT_FAILURE` termination |
| **Spark UI event log** | Data behind the Spark UI (jobs, stages, tasks) | Post-mortem UI on a terminated cluster (retained only for a limited window — don't treat it as long-term forensics) |

### Cluster log delivery (persist logs beyond the cluster)

Terminated clusters lose local logs. To keep them, configure **cluster log delivery** (`cluster_log_conf`) to a Volume/cloud path; Databricks delivers driver, executor, and event logs on an interval (about every 5 minutes) under `<destination>/<cluster-id>/`:

```json
"cluster_log_conf": {
  "volumes": { "destination": "/Volumes/main/ops/cluster_logs" }
}
```
(Older pattern: `dbfs`/`s3` destination blocks — same idea, different key.) For **job clusters**, the cluster is ephemeral and is gone when the run ends, so delivery is the only way to get executor logs after the fact.

**Exam trap:** a scenario says "the job cluster terminated and the engineer can no longer see executor logs." The fix is **configuring cluster log delivery on the job cluster before the next run**, not "restart the cluster" (it no longer exists).

### Reading a termination reason

A cluster/run termination carries a **type** and **code**. Representative signals:

| Signal | Meaning | Remediation direction |
|---|---|---|
| `INIT_SCRIPT_FAILURE` | An init script exited non-zero | Read init script log; fix script; scripts must be idempotent |
| `DRIVER_UNREACHABLE` / `DRIVER_NOT_RESPONDING` / "driver up but not responsive, likely due to GC" | Driver overloaded (heap exhaustion, GC thrash) | Driver-side cause (collect/broadcast — Day 4/7); bigger driver node; remove `collect()` |
| `NODES_LOST` / spot termination | Worker instances reclaimed | Executor task retries; use on-demand for driver, spot with fallback for workers; expect `FetchFailedException` downstream |
| Cloud provider quota/launch failure | Cloud capacity/quota, not Spark | Different node type/zone, raise quota — not a Spark tuning problem |
| `LIBRARY_INSTALLATION_ERROR` (job termination code) | A cluster-scoped library failed to install | Fix dependency/wheel path/PyPI reachability (Day 2) |
| `STORAGE_ACCESS_ERROR` / `UNAUTHORIZED_ERROR` | Credentials/permissions to storage or workspace object | Access problem — check UC/credentials, not code |

**Exam trap:** cluster-*infrastructure* failures (quota, launch failure, init script) are **not** fixed by Spark configuration changes. If the stem says "cluster failed to start," Spark UI is the wrong tool — there is no Spark application yet.

---

## Part 3 — Error Signatures: Reading Spark and Delta Failures

Always read the **root cause line** (`Caused by:`), not the wrapper. PySpark wraps JVM errors in `Py4JJavaError`; the useful text is at the bottom of the chain.

### Spark execution errors

| Signature | Usual cause | Fix (cross-ref) |
|---|---|---|
| `Job aborted due to stage failure: ... Lost task X.Y ... ExecutorLostFailure` + `java.lang.OutOfMemoryError: Java heap space` | **Executor** heap exhausted: large/skewed partition, cache bloat | More/smaller partitions, AQE skew, unpersist (Day 6/7) |
| `ExecutorLostFailure ... exit code 137` (SIGKILL) with JVM heap looking fine | Container killed for **off-heap/overhead** memory — commonly Python worker (non-Pandas UDF) or `memoryOverhead` too small | Pandas UDF, `spark.python.worker.memory`, overhead (Day 7) |
| `Total size of serialized results ... is bigger than spark.driver.maxResultSize` | **Driver**: `collect()`/large result | Write to storage; bounded collect; raise limit only if bounded (Day 4/7) |
| Driver "not responsive, likely due to GC" / `DRIVER_NOT_RESPONDING` | Driver heap pressure | Same as above; larger driver |
| `FetchFailedException` (shuffle block not found) | Executor holding shuffle files was lost (spot loss, OOM kill) → stage retried | Fix the underlying executor loss; more stable nodes; don't blame the reader task |
| `No space left on device` / `DISK_FULL` | Local disk exhausted by shuffle spill/temp | More partitions, larger local storage/disk autoscaling |
| `TaskNotSerializableException` / `Task not serializable` | Closure captures a non-serializable object (e.g., a `SparkSession`, a connection) | Create the object inside the UDF/`mapPartitions`, not outside |
| `ModuleNotFoundError` inside a UDF, works on driver | Notebook-scoped `%pip` is driver-only (Day 2) | Cluster-scoped library |
| Broadcast timeout / `SparkException: ... not enough memory to build the hash map` | Broadcast side too big (Day 6) | Remove hint, raise threshold carefully, or let AQE choose |

### Delta failures

| Exception | Meaning | Fix |
|---|---|---|
| `DELTA_SCHEMA_MISMATCH` / "A schema mismatch detected when writing to the Delta table" | Enforcement rejected extra/changed columns (Day 9) | Deliberate `mergeSchema`/`ALTER TABLE ADD COLUMNS`; never silently `overwriteSchema` |
| `ConcurrentAppendException` | Another writer added files your operation had read (e.g., overlapping partition) | Make predicates disjoint (put the partition column in the `ON`/`WHERE`), retry; deletion vectors give row-level concurrency (Day 11) |
| `ConcurrentDeleteReadException` | A concurrent operation deleted/rewrote a file your operation read | Retry; narrow predicates |
| `ConcurrentDeleteDeleteException` | Two operations rewrote/removed the same file | Serialize or partition the work |
| `MetadataChangedException` / `ProtocolChangedException` | Concurrent `ALTER`/schema/protocol change | Serialize DDL vs. writers; retry |
| `ConcurrentTransactionException` | Same streaming query (same checkpoint) started twice | One writer per checkpoint |
| `FileNotFoundException ... A file referenced in the transaction log cannot be found` | Underlying file removed — often **`VACUUM` retention shorter than a long-running query/stream lag** or manual deletion | Longer retention; don't `VACUUM RETAIN 0 HOURS` casually (Day 19); `REFRESH TABLE` for stale cache |

**Exam trap:** all Delta concurrency exceptions are **optimistic-concurrency conflict detection** (Day 9), not corruption and not a locking problem. "The table is locked" is never the right diagnosis.

### Structured Streaming errors (Day 13/14 callbacks)

- `UnknownFieldException` from Auto Loader → new column; schema already updated in `schemaLocation`; **restart** (Job auto-retry) — Day 13.
- Checkpoint incompatible after changing a stateful operator/aggregation/keys → cannot resume from old checkpoint; new checkpoint (and reprocessing plan) required.
- Deleted/corrupted checkpoint → reprocess from earliest source offset (duplicates for a non-idempotent sink).
- Kafka `Offsets out of range` / data loss → source retention expired before the stream consumed it.

---

## Part 4 — Spark UI and Query Profile as Debugging Tools

The diagnostic mechanics are Day 8; the **debug-time** additions:

- **Failed stage view**: the failed-task table at the bottom of a failed stage shows the exception per task and which executor. One executor repeatedly failing → node problem; failures spread across executors → data/code problem.
- **Executors tab**: dead executors, "Removed reason", GC time %, and links to per-executor `stdout`/`stderr`.
- **Structured Streaming tab**: input rate vs. process rate and batch duration vs. trigger interval (Day 14).
- **Stage retries**: a stage shown with multiple attempts (`Stage 5.0`, `5.1`) points at `FetchFailedException`/executor loss upstream, not at the retried stage's own logic.

### SQL warehouse: Query Profile + `system.query.history`

For a slow or failed warehouse query, the programmatic equivalent of "open the profile" is the row in `system.query.history`, joined on **`statement_id`** (Day 21):

```sql
SELECT statement_id, executed_by, execution_status, error_message,
       total_duration_ms, waiting_for_compute_duration_ms,
       waiting_at_capacity_duration_ms, execution_duration_ms,
       read_bytes, read_files, pruned_files, spilled_local_bytes, shuffle_read_bytes
FROM system.query.history
WHERE start_time > current_timestamp() - INTERVAL 1 DAY
  AND (execution_status = 'FAILED' OR total_duration_ms > 60000)
ORDER BY total_duration_ms DESC;
```

Decision table — **where did the time go?**

| Dominant duration | Diagnosis | Fix |
|---|---|---|
| `waiting_for_compute_duration_ms` high | Warehouse was stopped/starting | Auto-stop/start settings, serverless, keep-warm |
| `waiting_at_capacity_duration_ms` high | Queries **queued** on a saturated warehouse | **Scale the warehouse** (max clusters / size) — query tuning won't help |
| `execution_duration_ms` high, high `read_bytes` vs `pruned_files` low | Bad data skipping (Day 11) | Liquid Clustering/stats on the filter column |
| High `spilled_local_bytes` | Memory pressure in the query | Larger warehouse / fix join or aggregation |
| `from_result_cache = true` | Result served from cache — timings not comparable | Disable cache when benchmarking |

**Exam trap:** "queries are slow at 9 AM only, each one runs quickly when run alone" → **capacity/queuing** (`waiting_at_capacity`), remedy is warehouse scaling, not rewriting SQL or Z-Ordering.

---

## Part 5 — System Tables for Failure Forensics

Schema and access rules are Day 21 (SCD2 pattern, `USE`+`SELECT` on the schema, enablement gap). Debug-specific queries:

```sql
-- Failed/unsuccessful task runs in the last 24h, with termination code
SELECT job_id, run_id, task_key, result_state, termination_code,
       period_start_time, period_end_time
FROM system.lakeflow.job_task_run_timeline
WHERE period_start_time > current_timestamp() - INTERVAL 24 HOURS
  AND result_state IS NOT NULL
  AND result_state <> 'SUCCEEDED'
ORDER BY period_end_time DESC;
```

> ⚠️ **Value check + Day 22 correction flag:** the Jobs **REST API** reports run results as `SUCCESS`/`FAILED`/`TIMEDOUT`, but the **system table** `result_state` values are a different vocabulary (e.g., `SUCCEEDED`, `FAILED`, `ERROR`, `CANCELLED`, `SKIPPED`, `TIMED_OUT`). Run `SELECT DISTINCT result_state FROM system.lakeflow.job_run_timeline` in your workspace to confirm. If it returns `SUCCEEDED`, then the Day 22 lab/notes filter `result_state = 'SUCCESS'` matches **zero rows** and its "stale job" alert would fire for every job. Fix that filter to `'SUCCEEDED'` after verifying.

**Timeline tables are sliced.** `job_run_timeline`/`job_task_run_timeline` emit rows in **period slices** (long runs appear as multiple rows, roughly hourly). Counting rows ≠ counting runs — use `COUNT(DISTINCT run_id)` and take the latest slice's `result_state`/`termination_code` for a run's final outcome.

```sql
-- Was the failing task memory-starved? Join task -> compute -> node utilization
WITH failed AS (
  SELECT run_id, task_key, explode(compute_ids) AS cluster_id,
         period_start_time, period_end_time
  FROM system.lakeflow.job_task_run_timeline
  WHERE result_state IS NOT NULL AND result_state <> 'SUCCEEDED'
    AND period_start_time > current_timestamp() - INTERVAL 24 HOURS
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
Peak memory pinned near 100% across nodes → capacity/partitioning problem; one node hot, others idle → skew (Day 4/8). `node_timeline` covers classic compute nodes and has ~90-day retention (Day 21).

**Permission-failure forensics:** `system.access.audit` shows the `unityCatalog` action that was denied or the grant that changed just before failures began — "it worked yesterday, fails today, no code change" is frequently a **revoke** or an ownership/group change (Day 18/21).

---

## Part 6 — Remediating Failed Job Runs: Retry vs. Repair vs. Rerun

### The three recovery mechanisms

| | **Task retry** | **Repair run** | **Re-run (Run now)** |
|---|---|---|---|
| Trigger | Automatic (configured) | Manual/API after the run **terminated unsuccessfully** | Manual/scheduled |
| Scope | The single failing task, immediately | **Failed tasks + their dependent (skipped/upstream-failed) tasks** in the *same* run | Entire job from scratch, **new run** |
| Successful upstream tasks | n/a | **Not re-executed**; their results are reused | Re-executed |
| Configured via | `max_retries`, `min_retry_interval_millis`, `retry_on_timeout` (default: **no retries**) | UI "Repair run" / `POST /api/2.1/jobs/runs/repair` (Day 24) | `run-now` |
| Run identity | Same run, incremented attempt | **Same `run_id`**, with a repair history | New `run_id` |
| Best for | Transient failures (spot loss, flaky network) | Deterministic failure you have now fixed (bad data, missing permission, wrong parameter) | Need a clean full reprocess |

### Why repair exists (and the official sample question)

Retrieved from the exam guide sample (Q9): a job has Task A; Tasks B and C run in parallel after A. A and B succeed, C fails. **A and B's work is committed; C's operations may be partially applied; nothing is rolled back** — Jobs are not a cross-task transaction. Remediation: fix the cause, then **repair the run** so only C (and anything downstream of C) reruns — A and B are not repeated.

**Exam trap — repair and idempotency:** repair reruns the failed task from its beginning. If the task performed several non-atomic writes before failing (e.g., two `INSERT`s, one succeeded), the rerun can **double-apply** them. Design tasks to be idempotent (`MERGE`, `replaceWhere`, or checkpointed/streaming writes with `Trigger.AvailableNow`) — otherwise repair is unsafe. Delta guarantees atomicity **per statement/commit**, not per notebook.

### Dependency behavior worth knowing (`run_if`)

Downstream tasks decide whether to run based on upstream outcomes:

| `run_if` | Runs when |
|---|---|
| `ALL_SUCCESS` (default) | All upstream succeeded |
| `AT_LEAST_ONE_SUCCESS` | ≥ 1 upstream succeeded |
| `NONE_FAILED` | No upstream failed (skipped/excluded upstream allowed) |
| `ALL_DONE` | All upstream finished, regardless of outcome (cleanup/notify tasks) |
| `AT_LEAST_ONE_FAILED` | ≥ 1 upstream failed (error handler) |
| `ALL_FAILED` | Every upstream failed |

A failed task makes default-`ALL_SUCCESS` downstream tasks show **`UPSTREAM_FAILED`** — those are *symptoms*; debug the first failed task, never the `UPSTREAM_FAILED` ones.

### Parameter overrides during repair (concept; syntax Day 24)

When repairing you may **override parameters** (job parameters / notebook params) — e.g., point a failed backfill at a corrected date range or a different source path. Key semantics:
- Overrides apply to the **tasks that rerun**; tasks that already succeeded are not re-executed, so they never see the new values.
- This is the intended remediation for "failure caused by a bad parameter": **repair with corrected parameter**, not editing the job definition and rerunning everything.
- Changing the job definition (code path, cluster) is a different action from a one-time override; overrides do not persist to future scheduled runs.

**Repair caveats:**
- If failed tasks share a **job cluster**, a repair run provisions a **new** job cluster for them (the original terminated).
- Repair applies after a run has finished unsuccessfully — you cannot repair a run still in progress.
- Repair is orchestration-level; it does not fix the root cause. A deterministic error (e.g., missing `USE SCHEMA`) will fail again — fix first, then repair.

### Serverless "auto-optimization" and retries (Exam Guide Section 1 bullet, link to Day 15)

On serverless compute for jobs, failed tasks may be **retried automatically by the platform's auto-optimization**, and a task-level setting exists to **disable auto-optimization** so failures surface on the first attempt (the exam bullet: "auto-optimization to disallow retries"). Debug relevance: a run with unexplained multiple attempts on serverless is auto-retry, not your `max_retries`. For memory-bound notebook tasks on serverless you cannot resize executors — the lever the guide names is the **high-memory** option for notebook tasks (verify exact field names in the Jobs API doc). Classic compute: change node type/`spark.executor.memory`; serverless: choose high memory or reduce per-task data.

---

## Part 7 — Debugging Lakeflow Spark Declarative Pipelines

*(Event-log schema and querying: Day 21 Part 6. Expectations, modes: Day 15. This part is the debugging workflow.)*

### Workflow

1. **Pipeline UI → update details**: which **flow/table** failed, and the error banner. The graph shows the first failed dataset; downstream datasets show as *waiting/skipped* — same "debug the first failure" rule as job tasks.
2. **Event log** for the root cause and metrics:

```sql
SELECT timestamp, level, event_type, message,
       error.exceptions[0].message AS root_message
FROM event_log('<pipeline_id>')
WHERE level IN ('ERROR','WARN')
ORDER BY timestamp DESC;

-- Expectation failures per flow (data-quality regressions)
SELECT timestamp, origin.flow_name,
       details:flow_progress.data_quality.expectations AS expectations
FROM event_log('<pipeline_id>')
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```
3. **Spark UI / driver logs** only when the event log shows the failure is *inside Spark execution* (OOM, skew, spill within a flow). Lakeflow-level errors (missing dataset, cycle, expectation halt, schema change) never need the Spark UI.

### Common pipeline failure causes

| Symptom | Cause | Remediation |
|---|---|---|
| Update `FAILED` immediately, message names an expectation | `@dlt.expect_or_fail` violated | Fix data or relax to `expect_or_drop`/`expect` deliberately (Day 15) |
| "Table/view not found", unresolved dataset | Wrong name in `dlt.read`/`dlt.read_stream`, or dataset defined only conditionally (`if` at authoring time — Day 15) | Fix reference / config |
| Streaming source changed schema | New column in source; incompatible type | Auto Loader evolution/rescue (Day 13); full refresh only if needed |
| Every table generated by a `for` loop reads the same source | Python **late-binding closure** (Day 15) | Default-argument capture |
| Streaming table shows stale values after upstream corrections | Append-only assumption violated (Day 16) | Materialized view, or CDF + `MERGE` |
| Update fails once then succeeds on its own | **Production** mode retried with backoff | Expected; in **Development** mode there are no automatic retries — failures show immediately (Day 15) |
| Pipeline succeeds but downstream numbers are wrong | Data-quality regression not gated | Event-log/expectation metrics + SQL Alert (Day 22) |

### Update types — choose the least destructive

| Action | Effect | Risk |
|---|---|---|
| **Refresh** (default update) | Incremental for streaming tables; MV maintained per Lakeflow's decision | Low |
| **Refresh selection** | Only chosen tables (and needed upstream) | Low |
| **Full refresh** (all/selected) | **Clears the target data and streaming checkpoints, reprocesses from source** | High — if the source no longer retains the history (e.g., Kafka retention passed, files deleted), the data is **gone for good** |
| **Validate** | Checks source code/graph without processing data | None — use it to catch definition errors cheaply |

**Protect a table from accidental full refresh:** set the table property `pipelines.reset.allowed = false`.

**Exam trap:** "the streaming table has bad data → run a full refresh" is only safe when the **source retains the full history**. For Kafka topics with limited retention or landing folders that get archived, a full refresh permanently loses the older data. Prefer targeted fixes (a corrective flow, `DELETE`/`MERGE` on the target where allowed, or refresh selection) and verify source retention first.

Retry-related pipeline settings exist (`pipelines.maxFlowRetryAttempts`, `pipelines.numUpdateRetryAttempts`) — know they govern **automatic** retry inside an update; they are distinct from a job's task-level `max_retries` when the pipeline is orchestrated as a Jobs task, and distinct from job **repair** (which reruns the pipeline-update task after the whole job run failed). Verify current defaults in docs before relying on numbers.

---

## Part 8 — Notebook Debugging Tools (Section 1 crossover)

- **Interactive debugger** (Python notebooks, recent DBR — DBR 13.3 LTS+ per current docs, verify): set a breakpoint in the line-number gutter, run with **Debug**, inspect variables, step over/into. It debugs **driver-side Python**; it does not step through code executing inside an executor UDF.
- **`%debug`**: post-mortem — after an uncaught exception, opens a debugger at the failure frame.
- For UDF logic, **test the function on a small local Pandas/plain-Python input first**, then wrap it as a UDF; a stack trace from inside an executor is much slower to iterate on (Day 2/3 testing patterns: `assertDataFrameEqual`, `DataFrame.transform`).
- `df.explain("formatted")`, `df.limit(n).display()`, and checking `df.rdd.getNumPartitions()` are the fast, local, no-cluster-rebuild diagnostics (Day 5).

**Exam trap:** the built-in debugger inspects the **driver**. A bug that only appears inside a `pandas_udf`/`udf` running on executors won't hit a driver breakpoint in the UDF body — reproduce it locally or log from the UDF instead.

---

## Part 9 — Symptom → First Action (Scenario Cheat Table)

| Stem says... | Do first | Do not |
|---|---|---|
| "Job cluster failed to start after a new init script was added" | Init script log + cluster event log | Open Spark UI |
| "One task of 200 runs 10× longer, others finish quickly" | Stages tab: Max vs. Median, shuffle read skew (Day 8) → AQE skew/salt | Add executor memory first |
| "`Lost task` + Java heap space; one executor 100%" | Skew diagnosis (Day 4/7) | Raise `maxResultSize` |
| "Notebook driver disconnects after `collect()`" | Driver OOM path (Day 7) | Look at executor logs |
| "UDF `ModuleNotFoundError`, works in the notebook" | Cluster-scoped library (Day 2) | Re-run `%pip install` |
| "Only task C failed; A and B are fine" | Fix cause → **repair run** for C | Full re-run of A and B |
| "Failure caused by wrong date parameter" | Repair with **parameter override** | Edit and redeploy the whole job |
| "Dashboard query fast alone, slow at peak" | `system.query.history` `waiting_at_capacity_duration_ms` → scale warehouse | Rewrite the SQL |
| "Pipeline update failed; which rule?" | Pipeline UI + `event_log()` (`level='ERROR'`, expectations) | Spark UI |
| "Streaming table lost the source history after full refresh" | (Post-mortem) source retention + `pipelines.reset.allowed` | Repeat full refresh |
| "Worked yesterday, `PERMISSION_DENIED` today, no code change" | `system.access.audit` for grant/revoke; UC chain (Day 18) | Change code |

---

## Part 10 — Exam Traps Recap

1. Classify the failing **layer** first; the compute type in the stem picks the tool (SQL warehouse → Query Profile; cluster → Spark UI/logs; pipeline → event log).
2. Cluster start/init/quota failures happen **before any Spark application exists** — Spark UI is the wrong tool; use the cluster event log and init script logs.
3. Job-cluster logs vanish with the cluster — **cluster log delivery** must be configured beforehand.
4. Read the **root `Caused by`** line; `Py4JJavaError` is a wrapper. Exit code **137** with a healthy JVM heap points to off-heap/Python worker memory.
5. `FetchFailedException` is a *consequence* of lost executors (shuffle files lost), not a shuffle-read bug.
6. Delta `Concurrent*Exception`s are optimistic-concurrency conflicts — disjoint predicates and retry, not "unlock the table"; `FileNotFoundException` on transaction-log files often traces to **`VACUUM` retention** shorter than query/stream lag.
7. **Retry vs. repair vs. re-run:** retry = automatic, task-only; repair = same run, failed + dependent tasks only, successful upstream reused; re-run = new run, everything again.
8. Repair does not roll back partial writes — tasks must be **idempotent**; official sample Q9: A and B committed, C's changes not rolled back.
9. Parameter overrides in a repair affect only the **tasks that rerun** and don't persist to the job definition/future runs.
10. `UPSTREAM_FAILED`/skipped tasks are symptoms — debug the first failed task/flow.
11. `waiting_at_capacity_duration_ms` high = **queuing**; scale the warehouse, don't tune the query.
12. System-table `result_state` vocabulary differs from the Jobs REST API's; timeline tables are **sliced**, so count `DISTINCT run_id`.
13. Lakeflow: event log first for pipeline-level causes; **Full refresh clears data and checkpoints** — unsafe when the source doesn't retain history; protect with `pipelines.reset.allowed = false`; **Validate** checks a graph without processing data.
14. Development mode has **no automatic retries** by design; Production mode retries with backoff — "why did it fail instantly" is not a bug.
15. The built-in notebook debugger inspects **driver** code, not executor-side UDF bodies.

---

## Glossary Updates (append to `00-resources/glossary.md`)

| Term | Definition | Day |
|---|---|---|
| Cluster event log | Lifecycle events for a cluster (start, resize, nodes lost, termination reason) | 23 |
| Cluster log delivery | `cluster_log_conf` setting that persists driver/executor/event logs to a Volume/cloud path | 23 |
| Termination code | Machine-readable reason a cluster/run ended (e.g., `INIT_SCRIPT_FAILURE`, `DRIVER_ERROR`) | 23 |
| Repair run | Rerun only failed tasks and their dependents within the same job run; successful upstream results reused | 23 |
| Task retry | Automatic re-attempt of a failing task (`max_retries`); default none | 23 |
| Parameter override (repair) | One-time changed parameters applied to tasks that rerun in a repair | 23 |
| `run_if` | Task dependency condition (`ALL_SUCCESS`, `NONE_FAILED`, `ALL_DONE`, `AT_LEAST_ONE_FAILED`, etc.) | 23 |
| `UPSTREAM_FAILED` | Result state of a task skipped because an upstream task failed | 23 |
| `waiting_at_capacity_duration_ms` | `system.query.history` column measuring time queued on a saturated warehouse | 23 |
| Full refresh (pipeline) | Clears target data + streaming checkpoints and reprocesses from source | 23 |
| `pipelines.reset.allowed` | Table property; `false` blocks full refresh of that table | 23 |
| Validate (pipeline) | Update type that checks pipeline source/graph without processing data | 23 |
| `ConcurrentAppendException` | Delta conflict: concurrent writer added files your operation read | 23 |
| Exit code 137 | SIGKILL; typically container killed for exceeding memory (often off-heap/Python worker) | 23 |

## Weak-Topics Updates (append to `00-resources/weak-topics.md`)

| Topic | Priority | Trigger | Note |
|---|---|---|---|
| Driver vs. executor OOM signatures (extends Day 4/7) | 🔴 | Recurring wrong-pattern | Add exit-137/off-heap and `FetchFailedException`-is-a-consequence |
| Retry vs. repair vs. re-run | 🟠 | New Day 23 | Same `run_id`, dependents only, successful upstream reused |
| Repair is not a rollback; idempotency required | 🟠 | Official sample Q9 | A/B committed, C partial |
| Warehouse queuing vs. query tuning | 🟡 | New Day 23 | `waiting_at_capacity_duration_ms` |
| Pipeline full refresh data-loss risk | 🟠 | New Day 23 | `pipelines.reset.allowed=false`, verify source retention |

---

## Udemy Sync (Derar Alhussein course)

I can't see the course outline, so match by topic: cover the course's lessons on **Databricks Jobs / task dependencies / repair runs**, and any **Spark UI and cluster logs** demos, *before* doing this day's lab. Skip re-watching Spark UI basics (Day 8 material). If the course demos a failed-job repair, pay attention to whether parameters can be overridden in the repair dialog. If the course doesn't cover the Lakeflow event log or `system.query.history` for debugging, this file is your only source for those.

---

## Cross-References
- Day 2: Notebook-scoped vs. cluster-scoped libraries — the `ModuleNotFoundError`-in-UDF signature.
- Day 4/7: Driver vs. executor OOM, Python worker memory, `maxResultSize`.
- Day 5/6/8: Physical plans, shuffle/skew, the Spark UI diagnostic ladder and Query Profile.
- Day 9/11/19: Optimistic concurrency, deletion-vector row-level concurrency, `VACUUM` retention interaction.
- Day 13/14: Auto Loader `UnknownFieldException`, checkpoints, streaming progress metrics.
- Day 15/16: Expectations, Development vs. Production mode, streaming table append-only limitation.
- Day 18/21: UC permission traversal, `system.access.audit`, system-table SCD2/enablement rules, event log schema.
- Day 22: Alerts that notify on the failures diagnosed here (**note the `result_state` value check above**).
- Day 24: Jobs REST API/CLI — `runs/repair` payload, `rerun_tasks`, `latest_repair_id`, parameter overrides, pipeline failure remediation via API.
