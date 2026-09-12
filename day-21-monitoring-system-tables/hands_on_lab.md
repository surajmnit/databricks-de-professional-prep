# Day 21 — Hands-On Lab: System Tables, REST API Monitoring, and Lakeflow Event Logs

## Lab Objectives

1. Query core system tables (`billing.usage`, `access.audit`, `compute.clusters`, `lakeflow.job_run_timeline`) and correctly handle SCD2 "latest state" queries.
2. Build a cost-attribution query joining `system.billing.usage` with job/pipeline metadata.
3. Reproduce and fix a system-table permission-denied error.
4. Call the Jobs REST API from a notebook to monitor a run programmatically.
5. Query a Lakeflow Declarative Pipeline's event log for update history and data-quality metrics.
6. Understand pagination for the Jobs/Pipelines REST APIs.

**Environment note:** system tables require the `system` catalog to be enabled for your metastore (workspace/account admin action) and specific schemas may need individual enablement. Community Edition and many trial workspaces either lack system tables entirely or have only a subset enabled. If you can't run the live queries, treat Steps 1–3 as **syntax practice** and read the "what to observe" notes — the SQL is exam-identical either way. Steps 4–6 need at least one real job and one real Lakeflow pipeline in your workspace; if you don't have one, reuse a job/pipeline from an earlier day's lab.

---

## Step 1 — Inspect what system schemas you can see

```sql
SHOW SCHEMAS IN system;
SHOW TABLES IN system.billing;
SHOW TABLES IN system.lakeflow;
```
**What to observe:** if a schema is empty or missing from `SHOW SCHEMAS`, it likely hasn't been enabled at the account level yet — this is a common real-world gotcha distinct from a permissions problem.

---

## Step 2 — Query billing and compute system tables

```sql
-- Raw usage, last 7 days
SELECT sku_name, usage_quantity, usage_start_time, workspace_id
FROM system.billing.usage
WHERE usage_start_time > current_timestamp() - INTERVAL 7 DAYS
ORDER BY usage_start_time DESC
LIMIT 50;

-- SCD2 "latest state" pattern for clusters — do NOT just SELECT * (you'd get every historical version)
SELECT *
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, cluster_id ORDER BY change_time DESC) AS rn
  FROM system.compute.clusters
) WHERE rn = 1;
```
**Predict then verify:** if you ran `SELECT * FROM system.compute.clusters WHERE cluster_id = '<some-id>'` without the `ROW_NUMBER()` filter, would you get one row or several? **Answer:** potentially several — one per configuration change (SCD2 behavior) — which is exactly the trap this step is designed to catch.

---

## Step 3 — 💥 Break it on purpose: query without the right grant

```python
try:
    spark.sql("SELECT * FROM system.access.audit LIMIT 5").display()
except Exception as e:
    print("Expected if you lack USE+SELECT on system.access and aren't an admin:")
    print(str(e)[:400])
```
**Fix (as an admin, granting to yourself/your group):**
```sql
GRANT USE SCHEMA, SELECT ON SCHEMA system.access TO `your_group_or_user`;
```
This is the exact Day 18 grant pattern applied to a system schema — system tables are governed by the same Unity Catalog ACL model as any other schema, just pre-populated and pre-managed by Databricks.

---

## Step 4 — Cost attribution query

```sql
WITH latest_jobs AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
  FROM system.lakeflow.jobs QUALIFY rn = 1
)
SELECT j.name AS job_name, SUM(u.usage_quantity) AS total_dbus
FROM system.billing.usage u
JOIN latest_jobs j
  ON u.workspace_id = j.workspace_id
 AND u.usage_metadata.job_id = j.job_id
GROUP BY j.name
ORDER BY total_dbus DESC
LIMIT 10;
```
**What to observe:** this is the standard FinOps pattern for the exam objective "resource utilization, cost... monitoring" — join `billing.usage` to whichever entity table (`lakeflow.jobs`, `lakeflow.pipelines`, `compute.clusters`) matches the `usage_metadata` struct's populated ID field.

---

## Step 5 — Monitor a job run via the REST API

```python
import requests

host = "https://<your-workspace-url>"
token = dbutils.secrets.get(scope="monitoring", key="pat_token")  # never hardcode a token

# Trigger a run (use a job_id from a previous day's lab)
job_id = "<your-job-id>"
run_resp = requests.post(
    f"{host}/api/2.1/jobs/run-now",
    headers={"Authorization": f"Bearer {token}"},
    json={"job_id": job_id},
)
run_id = run_resp.json()["run_id"]
print(f"Triggered run_id: {run_id}")

# Poll status
status_resp = requests.get(
    f"{host}/api/2.1/jobs/runs/get",
    headers={"Authorization": f"Bearer {token}"},
    params={"run_id": run_id},
)
print(status_resp.json()["state"])
```

### List runs with pagination handled correctly
```python
all_runs = []
params = {"job_id": job_id, "limit": 25}
while True:
    resp = requests.get(
        f"{host}/api/2.1/jobs/runs/list",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
    ).json()
    all_runs.extend(resp.get("runs", []))
    if not resp.get("has_more"):
        break
    params["page_token"] = resp["next_page_token"]

print(f"Total runs retrieved: {len(all_runs)}")
```
**Break it on purpose:** remove the `while` loop's `has_more` check and hardcode a single call — confirm you silently get only the first page. This is the exact "monitoring script missing records" trap from the notes.

---

## Step 6 — Query a Lakeflow Declarative Pipeline's event log

```sql
-- Use a pipeline_id from an earlier day's lab (Day 15/17 material)
CREATE OR REPLACE VIEW event_log_raw AS SELECT * FROM event_log('<your-pipeline-id>');

-- Pipeline update history
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

-- Data quality / expectation metrics
SELECT timestamp, details:flow_progress.metrics.expectations AS expectation_metrics
FROM event_log_raw
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 20;
```
**What to observe:** the expectation pass/fail counts here should match what you saw in the Lakeflow Pipelines UI's Data Quality tab from Day 15/17 — this confirms the event log is the same underlying data source as the UI, just queryable with SQL.

---

## Stretch Task

Build a single notebook/dashboard that combines all three monitoring surfaces for one pipeline:
1. **Cost** — this pipeline's DBU spend from `system.billing.usage` joined to `system.lakeflow.pipelines`.
2. **Reliability** — its update success/failure history from `system.lakeflow.pipeline_update_timeline`.
3. **Data quality** — its expectation metrics from its own `event_log()`.

This three-way combination (account-wide cost + account-wide scheduling + inside-pipeline detail) is exactly the kind of composite monitoring view a production data platform team builds, and mirrors how the exam likes to test "which table/API would you use for X" by making you first identify which of the three observability layers a requirement belongs to.

---

## Lab Checklist

- [ ] Listed and inspected available system schemas
- [ ] Queried `billing.usage` and correctly applied the SCD2 "latest state" pattern to `compute.clusters`
- [ ] Reproduced and fixed a system-table permission error using a Day 18-style grant
- [ ] Built a cost-attribution query joining `billing.usage` to `lakeflow.jobs`
- [ ] Triggered and polled a job run via the REST API
- [ ] Implemented correct pagination for `jobs/runs/list` and observed the failure mode when pagination is skipped
- [ ] Queried a pipeline's event log for update history and expectation metrics
- [ ] (Stretch) Built a combined cost + reliability + data-quality view for one pipeline

---

## Cross-References
- Day 8: Spark UI / Query Profile deep-dive.
- Day 15/17: The `@dlt.expect*` expectations whose metrics you queried in Step 6.
- Day 18: The `GRANT USE SCHEMA, SELECT` pattern reused in Step 3.
- Day 22: SQL Alerts and job notifications, built on these same signals.
- Day 23/24: Using these same REST endpoints to actively repair failed runs, not just observe them.
