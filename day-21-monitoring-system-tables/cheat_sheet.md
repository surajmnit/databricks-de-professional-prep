# Day 21 — Cheat Sheet: Monitoring — System Tables, Spark UI, Event Logs, REST API

## System Table Access

```sql
GRANT USE SCHEMA, SELECT ON SCHEMA system.billing TO `finops_team`;
```
Bypass: metastore admin **AND** account admin. Some schemas need explicit **enablement** first (zero rows ≠ permission error).

---

## Core System Tables

| Table | What it's for |
|---|---|
| `system.billing.usage` | DBU consumption, per SKU/workspace/job/pipeline/cluster |
| `system.billing.list_prices` | SKU pricing history — join with `usage` for $ cost |
| `system.access.audit` | All audit events incl. grant/revoke (Day 18) |
| `system.access.table_lineage` / `column_lineage` | Read/write lineage; join `statement_id` → `query.history` |
| `system.access.outbound_network` | Denied outbound network events |
| `system.compute.clusters` | **SCD2** cluster config history |
| `system.compute.node_types` | Static hardware reference |
| `system.compute.node_timeline` | Minute-by-minute node CPU/mem utilization |
| `system.lakeflow.jobs` | **SCD2** job config history |
| `system.lakeflow.job_run_timeline` / `job_task_run_timeline` | Job/task run history |
| `system.lakeflow.pipelines` | **SCD2** pipeline config history |
| `system.lakeflow.pipeline_update_timeline` | Pipeline update (run) history |
| `system.query.history` | Every SQL warehouse statement — text, duration, status |

**SCD2 tables** (`lakeflow.jobs`, `lakeflow.pipelines`, `compute.clusters`) → always:
```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY workspace_id, id_col ORDER BY change_time DESC) AS rn
  FROM system.<table>
) WHERE rn = 1;
```

---

## Query Profile vs. Spark UI

| | Query Profile | Spark UI |
|---|---|---|
| Compute type | SQL warehouse (DBSQL/Photon) | Any cluster |
| Auto-flags | Skew, spill, join strategy, poor data skipping | Manual inspection (Day 8) |
| Programmatic twin | `system.query.history` (join on `statement_id`) | — |

---

## REST API — Jobs (v2.1)

| Action | Endpoint |
|---|---|
| List runs | `GET /api/2.1/jobs/runs/list` |
| Get run | `GET /api/2.1/jobs/runs/get` |
| Get run output | `GET /api/2.1/jobs/runs/get-output` |
| Trigger run | `POST /api/2.1/jobs/run-now` |
| Cancel run | `POST /api/2.1/jobs/runs/cancel` |

**Pagination:** always loop on `has_more` / `next_page_token` — a single call silently misses older records.

## REST API — Pipelines (v2.0)

| Action | Endpoint |
|---|---|
| Get pipeline | `GET /api/2.0/pipelines/{id}` |
| List updates | `GET /api/2.0/pipelines/{id}/updates` |
| List events | `GET /api/2.0/pipelines/{id}/events` |

---

## Lakeflow Event Log

```sql
SELECT * FROM event_log('<pipeline_id>');
```
- Default: **hidden Delta table**, owner-only unless published.
- Key fields: `event_type` (`flow_progress`, `update_progress`, ...), `level`, `origin`, `details` (JSON), `maturity_level`.
- Use for: **inside-one-pipeline** detail — update duration, data-quality/expectation pass-fail metrics.
- `system.lakeflow.*` = **account-wide, cross-pipeline** cost/scheduling view. Don't confuse the two scopes.

---

## Exam Trap Shortlist

1. System table zero-rows → schema not enabled, not (necessarily) a permission error.
2. SCD2 tables need `ROW_NUMBER() ... QUALIFY rn=1` for "current state."
3. Query Profile = SQL warehouse; Spark UI = general cluster compute.
4. `statement_id` is the join key: `query.history` ↔ lineage tables ↔ Query Profile.
5. REST API pagination must be handled explicitly (`has_more`/`next_page_token`).
6. Event log = inside one pipeline; `system.lakeflow.*` = across all pipelines/account-wide.
7. Only the pipeline owner can query `event_log()` directly unless published.
8. `system.access.audit` (not a fictional `audit_logs` table) is the real permission-change history source.
