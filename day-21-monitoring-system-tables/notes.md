# Day 21 — Monitoring: System Tables, Spark UI, Lakeflow Event Logs, REST API

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 5: Monitoring and Alerting (10%)** — the Monitoring half (Alerting is Day 22):
- "Use system tables for observability over resource utilization, cost, auditing and workload monitoring."
- "Use Query Profiler UI and Spark UI to monitor workloads."
- "Use the Databricks REST APIs/Databricks CLI for monitoring jobs and pipelines."
- "Use Lakeflow Spark Declarative Pipelines Event Logs to monitor pipelines."

*(Spark UI internals — Jobs/Stages/Tasks, shuffle metrics, memory — were covered in depth on Day 8. This day focuses on the observability-specific angle: system tables, Query Profile as a monitoring surface, the REST/CLI monitoring surface, and Lakeflow event logs. Don't re-derive Day 8 material here — cross-reference it.)*

---

## Part 1 — System Tables: What They Are

System tables are **Delta tables Databricks manages automatically** in a special `system` catalog, giving you SQL-queryable observability across your entire account/workspace without needing custom logging infrastructure.

```sql
SHOW SCHEMAS IN system;
-- access, billing, compute, lakeflow, query, marketplace, ...
```

### Access requirements (exam-tested)
To query system tables, a principal must either:
1. Be **both a metastore admin and an account admin**, or
2. Hold **`USE` and `SELECT`** privileges on the specific system schema (e.g., `system.billing`), granted the normal Unity Catalog way (Day 18):

```sql
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing TO `finops_team`;
GRANT USE SCHEMA, SELECT ON SCHEMA system.access TO `security_team`;
```

**Exam trap:** some system schemas are **not enabled by default** and must be explicitly enabled (via the account console, Unity Catalog API, or `databricks account settings`) before their tables populate — a question describing "the table exists but always returns zero rows" is testing this enablement gap, not a permissions problem.

### Retention (representative — verify current per-table specifics if a question hinges on exact numbers)
Most system tables retain **365 days** of history; `system.compute.node_timeline` retains **90 days**; `system.compute.node_types` is **indefinite** (it's just current hardware metadata, not a time series). Lineage tables (`table_lineage`/`column_lineage`) retain a rolling **1-year** window.

---

## Part 2 — The Core System Tables to Know

| Table | Schema | Purpose |
|---|---|---|
| `system.billing.usage` | Billing | Row per unit of billable usage — DBUs, SKU, workspace, cluster/job/pipeline/warehouse ID, `usage_metadata` struct |
| `system.billing.list_prices` | Billing | Historical SKU pricing — join with `usage` to compute actual $ cost |
| `system.access.audit` | Access | **All audit log events** — logins, permission changes, resource creation/deletion (Day 18's `updatePermissions` events live here) |
| `system.access.table_lineage` | Access | Table-level read/write lineage — which job/notebook/pipeline/dashboard touched which table |
| `system.access.column_lineage` | Access | Column-level lineage — finer-grained than table lineage; doesn't capture columns written via literal/explicit values with no source |
| `system.access.outbound_network` | Access | Records every time outbound internet access was **denied** from your account (network security posture) |
| `system.compute.clusters` | Compute | SCD2 history of every cluster configuration (all-purpose, job, pipeline compute) |
| `system.compute.node_types` | Compute | Static reference: available node types + hardware specs |
| `system.compute.node_timeline` | Compute | Minute-by-minute CPU/memory utilization per node |
| `system.lakeflow.jobs` | Lakeflow | SCD2 history of every job's configuration |
| `system.lakeflow.job_run_timeline` | Lakeflow | Start/end/status of every job **run** |
| `system.lakeflow.job_task_run_timeline` | Lakeflow | Start/end/status of every **task** within a job run, including which `compute_ids` (clusters) it used |
| `system.lakeflow.pipelines` | Lakeflow | SCD2 history of every Lakeflow Declarative Pipeline's configuration |
| `system.lakeflow.pipeline_update_timeline` | Lakeflow | Start/end time and compute used for each pipeline **update** (run) |
| `system.query.history` | Query | Every SQL statement executed on a SQL warehouse — text, duration, status, rows/bytes processed |

**Exam trap:** `system.lakeflow.jobs`, `system.lakeflow.pipelines`, and `system.compute.clusters` are all **slowly changing dimension (SCD2) tables** — every configuration change emits a **new row**, so querying "current state" requires a `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY change_time DESC) QUALIFY rn = 1` pattern, not a plain `SELECT *`.

---

## Part 3 — Common Monitoring Query Patterns

### Cost attribution: which job/pipeline is spending the most?
```sql
WITH latest_pipelines AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, pipeline_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.pipelines QUALIFY rn = 1
)
SELECT p.name, SUM(u.usage_quantity) AS total_dbus
FROM system.billing.usage u
JOIN latest_pipelines p
  ON u.workspace_id = p.workspace_id
 AND u.usage_metadata.dlt_pipeline_id = p.pipeline_id
GROUP BY p.name
ORDER BY total_dbus DESC;
```

### Cluster/job usage vs. instance-type efficiency
```sql
SELECT c.cluster_name, c.owned_by, u.usage_quantity, u.usage_start_time
FROM system.billing.usage u
JOIN system.compute.clusters c
  ON u.usage_metadata.cluster_id = c.cluster_id
 AND u.workspace_id = c.workspace_id
ORDER BY u.usage_start_time DESC;
```

### Auditing permission changes (Day 18 callback)
```sql
SELECT event_time, user_identity.email, action_name, request_params
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
ORDER BY event_time DESC;
```

### Job reliability over the last week
```sql
SELECT workspace_id, COUNT(DISTINCT run_id) AS job_count, to_date(period_start_time) AS date
FROM system.lakeflow.job_run_timeline
WHERE period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
GROUP BY ALL;
```

### Tracing what feeds a table (lineage)
```sql
SELECT source_table_full_name, target_table_full_name, entity_type, event_time
FROM system.access.table_lineage
WHERE target_table_full_name = 'main.gold.revenue_summary'
ORDER BY event_time DESC;
```
Join `statement_id` from `table_lineage`/`column_lineage` back to `system.query.history` to get the **exact SQL text** that produced a lineage edge — a powerful "who wrote this and with what query" audit trail.

---

## Part 4 — Query Profile UI and Spark UI (cross-reference + the observability angle)

Day 8 covers the mechanics of reading the Spark UI (Jobs/Stages/Tasks/Executors/Storage) and the Query Profile's plan graph in depth — review that material for the "how do I read this" skill. For **this** objective bullet, the exam angle is *monitoring*, specifically:

- **Query Profile's automatic insights**: the UI proactively flags likely bottlenecks — data skew, disk spill, inefficient join strategy (e.g., a `SortMergeJoin` that could have been a broadcast), and poor data skipping (a scan reading far more bytes than the query's filter should require). Recognize these insight categories by name — a scenario describing symptoms (one task much slower, a spill metric appearing, or a "shuffle" step dominating total time) maps directly back to one of these categories.
- **Query Profile ↔ `system.query.history` link**: every query run on a SQL warehouse has a `statement_id`; Query Profile is the **visual** rendering of that statement's execution, while `system.query.history` is the **programmatic** record — join them via `statement_id` to build automated dashboards/alerts on top of what the UI shows visually one query at a time.
- **Spark UI for notebook/job clusters vs. Query Profile for SQL warehouses**: Spark UI is the general-purpose Spark execution view (any cluster, any workload); Query Profile is the DBSQL-specific, Photon-aware view purpose-built for SQL warehouse queries — know which one a scenario is pointing at based on whether the workload runs on a cluster (Spark UI) or a SQL warehouse (Query Profile).

---

## Part 5 — REST API and CLI for Monitoring Jobs and Pipelines

### Jobs (v2.1 API)

| Action | REST endpoint | CLI equivalent |
|---|---|---|
| List runs | `GET /api/2.1/jobs/runs/list` | `databricks jobs list-runs` |
| Get one run's details | `GET /api/2.1/jobs/runs/get?run_id=...` | `databricks jobs get-run <run-id>` |
| Get a run's output (notebook result, logs) | `GET /api/2.1/jobs/runs/get-output?run_id=...` | `databricks jobs get-run-output <run-id>` |
| Trigger a run | `POST /api/2.1/jobs/run-now` | `databricks jobs run-now <job-id>` |
| Cancel a run | `POST /api/2.1/jobs/runs/cancel` | `databricks jobs cancel-run <run-id>` |

### Pipelines API

| Action | REST endpoint |
|---|---|
| Get pipeline details/status | `GET /api/2.0/pipelines/{pipeline_id}` |
| List pipeline update history | `GET /api/2.0/pipelines/{pipeline_id}/updates` |
| List pipeline events | `GET /api/2.0/pipelines/{pipeline_id}/events` |

```python
import requests

resp = requests.get(
    f"{host}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"run_id": run_id},
)
run_state = resp.json()["state"]["result_state"]   # e.g., SUCCESS, FAILED, TIMEDOUT
```

**Why this matters for the exam:** this is the mechanism behind **custom monitoring dashboards, third-party integrations (Prometheus, Datadog, PagerDuty), and automated remediation scripts** — any scenario describing "build a custom dashboard of job health outside the Databricks UI" or "trigger an automated response when a run fails" points to the REST API/CLI, not the UI. This connects directly to **Day 23/24** (debugging and job-repair automation) — those days use these same endpoints to *act* on failures; this day is about *observing* them.

**Exam trap:** pagination — `runs/list` defaults to a limited page size and returns a `has_more` field; a script that doesn't loop on `has_more` will silently miss older runs. Similarly, `pipelines/{id}/events` caps `max_results` (documented ceiling of 250 per call) — a scenario about "my monitoring script is missing pipeline events" is testing whether you know to paginate via `next_page_token`, not a bug in the API itself.

---

## Part 6 — Lakeflow Declarative Pipelines Event Log

Every Lakeflow Declarative Pipeline automatically writes a structured event log capturing **audit events, data quality (expectation) results, pipeline/flow progress, and lineage** — this is the pipeline-specific complement to the generic system tables above.

### Querying it
```sql
-- As the pipeline owner, using the pipeline's UUID (dashes become underscores in the hidden table name)
SELECT * FROM event_log('<pipeline_id>');

-- Or, once you've published/pinned it to a view for reuse:
CREATE VIEW event_log_raw AS SELECT * FROM event_log('<pipeline_id>');
```
By default the event log is a **hidden Delta table** in the pipeline's default catalog/schema, queryable by sufficiently privileged users but only the **pipeline owner** can call `event_log()` directly (or a view built on top of it, unless explicitly published/shared). You can also choose to **publish** the event log to a named table via the pipeline's Advanced settings, making it easier to grant broader read access the normal Unity Catalog way.

### Schema highlights
| Field | Purpose |
|---|---|
| `event_type` | Category of event, e.g. `flow_progress`, `update_progress`, `dataset_definition`, `create_maintenance_flow` |
| `level` | `INFO`, `WARN`, `ERROR`, `METRICS` |
| `origin` | Struct with `pipeline_id`, `pipeline_name`, `update_id`, cloud/region metadata |
| `details` | JSON payload — the primary field for analysis (e.g., `details:flow_progress.status`, data quality metrics per expectation) |
| `message` | Human-readable summary |
| `maturity_level` | `STABLE` / `EVOLVING` / `DEPRECATED` — whether the event's schema is safe to build long-term automation against |

### Example: pipeline update history and duration
```sql
WITH last_status_per_update AS (
  SELECT
    origin.update_id AS update_id,
    details:update_progress.state AS state,
    timestamp,
    ROW_NUMBER() OVER (PARTITION BY origin.update_id ORDER BY timestamp DESC) AS rn
  FROM event_log_raw
  WHERE event_type = 'update_progress'
  QUALIFY rn = 1
)
SELECT * FROM last_status_per_update ORDER BY timestamp DESC;
```

### Example: data quality (expectation) metrics — ties to Day 15/17
```sql
SELECT
  timestamp,
  details:flow_progress.metrics.expectations AS expectation_metrics
FROM event_log_raw
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```
This is exactly how you'd monitor the `@dlt.expect`/`expect_or_drop` pass/fail counts from Day 15/17 in production, without manually instrumenting your pipeline code.

**Exam trap:** the event log is the correct answer whenever a scenario asks specifically about **debugging or monitoring a Lakeflow Declarative Pipeline's internal behavior** (data quality metrics, flow-level progress, per-update duration) — `system.lakeflow.pipelines`/`pipeline_update_timeline` give you the **account-wide/cross-pipeline** cost and scheduling view, while the **event log** gives you the **inside-one-pipeline** operational detail. Don't reach for `system.lakeflow.*` when a question is really asking about one pipeline's expectation failures.

---

## Part 7 — Exam Traps Recap

1. System tables need **either admin (metastore+account) or explicit `USE`+`SELECT` grants** on the schema — and some schemas need to be **enabled** before they populate at all.
2. `system.lakeflow.jobs`, `system.lakeflow.pipelines`, `system.compute.clusters` are **SCD2** — always filter to the latest row per entity with `ROW_NUMBER() ... QUALIFY rn = 1`.
3. Query Profile = SQL warehouse workloads (DBSQL/Photon-aware); Spark UI = general cluster workloads — pick based on the compute type in the scenario.
4. `statement_id` links `system.query.history` to the lineage tables and to Query Profile — the join key for building programmatic dashboards.
5. Jobs/Pipelines REST APIs are **paginated** — `has_more`/`next_page_token` must be handled in a loop, or a monitoring script will silently miss records.
6. The Lakeflow **event log** is for **inside-one-pipeline** operational/data-quality detail; `system.lakeflow.*` tables are for **cross-pipeline, account-wide** cost/scheduling visibility — pick the right tool for the question's scope.
7. Only the pipeline **owner** can query `event_log()` directly unless the log has been explicitly published to a shared table.

---

## Cross-References
- Day 8: Spark UI and Query Profile internals — how to read Jobs/Stages/Tasks, shuffle, skew, and spill metrics in detail.
- Day 15/17: `@dlt.expect*` data quality expectations — surfaced via the event log's `flow_progress` metrics here.
- Day 18: `system.access.audit` for permission-change history — introduced there, detailed here.
- Day 22: SQL Alerts and Lakeflow Jobs notifications — built on top of the same signals this day teaches you to observe.
- Day 23/24: Debugging failed runs and remediating via the Jobs API — the "act on it" counterpart to this day's "observe it."
