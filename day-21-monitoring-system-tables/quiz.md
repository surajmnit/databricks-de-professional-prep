# Day 21 — Quiz: Monitoring — System Tables, Spark UI, Event Logs, REST API

**Objective coverage:** Section 5 (10%) — Monitoring half.

---

## Question 1
**Objective:** System table access requirements.

A user who is neither a metastore admin nor an account admin wants to query `system.billing.usage`. What must be true for their query to succeed?

A. Nothing — system tables are readable by all authenticated users by default
B. They must hold `USE` and `SELECT` privileges on the `system.billing` schema, granted via normal `GRANT` statements
C. They must be a workspace admin specifically (not account admin)
D. System tables can never be queried by non-admins under any configuration

---

## Question 2
**Objective:** SCD2 system tables.

A query runs `SELECT * FROM system.compute.clusters WHERE cluster_id = 'abc123'` and unexpectedly returns 6 rows instead of 1. What's the most likely explanation?

A. A bug in the system table
B. The cluster's configuration changed 6 times; `compute.clusters` is an SCD2 table recording full history, and the query didn't filter to the latest row
C. The cluster was cloned 6 times
D. `cluster_id` is not a valid filter column

---

## Question 3
**Objective:** Cost attribution pattern.

To determine which Lakeflow Declarative Pipeline consumed the most DBUs last month, which two system tables should be joined, and on which fields?

A. `system.access.audit` and `system.compute.node_timeline`, joined on `event_time`
B. `system.billing.usage` and `system.lakeflow.pipelines`, joined on `workspace_id` and the pipeline ID in `usage_metadata`
C. `system.query.history` and `system.lakeflow.jobs`, joined on `statement_id`
D. `system.compute.clusters` and `system.access.table_lineage`, joined on `cluster_id`

---

## Question 4
**Objective:** Query Profile vs. Spark UI.

A data analyst's dashboard query, run on a SQL warehouse, is running slowly. Which tool is the most direct way to visually diagnose the bottleneck?

A. Spark UI Executors tab
B. Query Profile
C. `system.compute.node_timeline`
D. The Lakeflow pipeline event log

---

## Question 5
**Objective:** Linking Query Profile to programmatic monitoring.

A team wants to build an automated dashboard that flags any SQL warehouse query exceeding 60 seconds, without manually checking Query Profile for each one. Which system table should they query, and what's the join key back to lineage tables if they also need to trace affected tables?

A. `system.access.audit`; join key is `action_name`
B. `system.query.history`; join key is `statement_id`
C. `system.compute.clusters`; join key is `cluster_id`
D. `system.billing.usage`; join key is `usage_metadata`

---

## Question 6
**Objective:** REST API endpoints for job monitoring.

Which REST API call retrieves the notebook output/result of a completed job run?

A. `GET /api/2.1/jobs/runs/get`
B. `GET /api/2.1/jobs/runs/get-output`
C. `GET /api/2.1/jobs/runs/list`
D. `POST /api/2.1/jobs/run-now`

---

## Question 7
**Objective:** Pagination pitfalls.

A custom monitoring script calls `GET /api/2.1/jobs/runs/list` once and assumes it has retrieved every run for a job with a long history. What is the likely result?

A. The script correctly retrieves every run; no issue
B. The script silently misses older runs because the response is paginated and `has_more`/pagination tokens were not handled
C. The API returns an error because too much data was requested
D. The API automatically retrieves all pages internally, so this is fine

---

## Question 8
**Objective:** Lakeflow event log access.

By default, who can query a Lakeflow Declarative Pipeline's event log directly via `event_log(pipeline_id)`?

A. Any user with `SELECT` on the pipeline's default catalog
B. Only the pipeline owner, unless the event log has been explicitly published/shared
C. Only account admins
D. Any member of the pipeline's associated cluster policy group

---

## Question 9
**Objective:** Event log vs. system tables scope.

A team needs to see the pass/fail counts for a specific pipeline's `@dlt.expect_or_drop` data quality rules on its most recent run. Which source should they query?

A. `system.lakeflow.pipelines` — filter to the pipeline's latest row
B. The pipeline's own event log, filtering `event_type = 'flow_progress'` and reading the `details` expectation metrics
C. `system.access.audit`
D. `system.compute.node_timeline`

---

## Question 10
**Objective:** Event log vs. system tables scope (inverse case).

A platform team needs to compare DBU cost and update-success-rate across all 40 Lakeflow pipelines in the account, not the internal details of any single one. Which source(s) should they use?

A. Each pipeline's individual event log, queried 40 separate times
B. `system.billing.usage` joined with `system.lakeflow.pipelines`/`pipeline_update_timeline` — the account-wide view
C. Spark UI, checked manually for each pipeline's cluster
D. `system.access.table_lineage`

---

## Question 11
**Objective:** Lineage system tables.

Which system table would you query to find every job, notebook, or dashboard that has read from or written to a specific Unity Catalog table?

A. `system.access.audit`
B. `system.access.table_lineage`
C. `system.compute.clusters`
D. `system.lakeflow.job_run_timeline`

---

## Question 12
**Objective:** Auditing permission changes (Day 18 callback).

A compliance team wants to see every `GRANT`/`REVOKE` issued in the last 30 days. Which system table and filter combination is correct?

A. `system.access.audit` filtering `service_name = 'unityCatalog'` and `action_name = 'updatePermissions'`
B. `system.billing.usage` filtering `sku_name = 'PERMISSIONS'`
C. `system.lakeflow.jobs` filtering `event_type = 'grant'`
D. `system.compute.clusters` filtering `cluster_source = 'ACL'`

---

## Question 13
**Objective:** System schema enablement gap.

A newly onboarded workspace admin queries `system.lakeflow.pipelines` and consistently gets zero rows, even though pipelines are actively running. What's the most likely cause?

A. The admin lacks `SELECT` — they'd get a permission error, not zero rows
B. The `lakeflow` system schema has not yet been enabled for this metastore/account
C. `system.lakeflow.pipelines` does not track Lakeflow Declarative Pipelines, only classic jobs
D. Zero rows is expected — this table only populates after a pipeline is deleted

---

## Question 14
**Objective:** Compute-layer utilization monitoring.

Which system table provides minute-by-minute CPU/memory utilization for compute nodes?

A. `system.compute.clusters`
B. `system.compute.node_timeline`
C. `system.compute.node_types`
D. `system.billing.usage`

---

## Question 15
**Objective:** Choosing the right monitoring surface for a scenario.

A scenario states: "Build a Slack notification that fires whenever any job run fails, without a human checking the Jobs UI." Which mechanism is the foundation for this capability?

A. Spark UI, screen-shared periodically
B. The Jobs REST API (`runs/list`/`runs/get`) or CLI, polled or event-driven, feeding an external notification integration
C. `system.compute.node_types`, since it lists hardware
D. Query Profile, manually checked per run

---

## Answer Key

### Q1: B
System tables are governed by the same Unity Catalog ACL model as any other schema — `USE`+`SELECT` on the relevant system schema is sufficient without being an admin.
**Wrong options:** A overstates default access. C is incorrect — the actual bypass is *metastore admin AND account admin*, not workspace admin alone. D contradicts the documented grant-based access path.

### Q2: B
`system.compute.clusters` is a slowly changing dimension (SCD2) table — every configuration change emits a new row. Without filtering to the latest `change_time` per `cluster_id`, you get full history, not current state.
**Wrong options:** A/C/D invent explanations; the real cause is the well-documented SCD2 design.

### Q3: B
`system.billing.usage` carries the DBU consumption records; `system.lakeflow.pipelines` carries pipeline metadata (name, ID). Joining on `workspace_id` plus the pipeline ID found in `usage_metadata.dlt_pipeline_id` is the standard cost-attribution pattern.
**Wrong options:** A, C, D pair unrelated tables that don't share the needed join semantics for this specific question.

### Q4: B
Query Profile is the DBSQL/Photon-aware visual diagnostic tool purpose-built for SQL warehouse query performance, including automatic insight flags (skew, spill, join strategy, data skipping).
**Wrong options:** A (Spark UI) is the general cluster-workload tool, not the SQL-warehouse-specific one. C and D don't provide the per-query visual breakdown needed here.

### Q5: B
`system.query.history` is the programmatic record of every SQL warehouse statement; `statement_id` is the shared key used to join into `table_lineage`/`column_lineage` for tracing affected tables.
**Wrong options:** A, C, D name tables that don't carry query-duration data or the correct join key for this use case.

### Q6: B
`runs/get-output` specifically retrieves a run's output (notebook result, logs) — distinct from `runs/get`, which returns run metadata/state but not output content.
**Wrong options:** A returns state/metadata only. C lists runs, doesn't fetch output. D triggers a new run.

### Q7: B
Both the Jobs and Pipelines REST APIs are paginated; a script that checks only the first response and ignores `has_more`/`next_page_token` will silently miss older records — a common real-world and exam-tested gotcha.
**Wrong options:** A and D assume automatic exhaustive retrieval, which is false. C is not the documented failure mode.

### Q8: B
By default only the pipeline owner (or a view built on top of a shared/published event log) can query `event_log()` — it is not open to arbitrary users with catalog-level `SELECT`.
**Wrong options:** A, C, D describe access models that don't match the documented default behavior.

### Q9: B
Per-pipeline data quality/expectation metrics live in that pipeline's own event log under `flow_progress` events — this is inside-one-pipeline operational detail, not something the account-wide `system.lakeflow.*` tables expose.
**Wrong options:** A, C, D are account-wide or unrelated tables that don't carry expectation-level metrics.

### Q10: B
Cross-pipeline, account-wide cost and reliability comparisons are exactly what `system.billing.usage` and `system.lakeflow.pipelines`/`pipeline_update_timeline` are built for — a single set of queries across all 40 pipelines, no per-pipeline event log querying needed.
**Wrong options:** A is needlessly manual and doesn't scale; C is manual and slow; D is for lineage, not cost/reliability.

### Q11: B
`system.access.table_lineage` records read/write events against Unity Catalog tables/paths, including the job/notebook/pipeline/dashboard responsible.
**Wrong options:** A is general audit events, not specifically table-touch lineage. C and D don't carry lineage information.

### Q12: A
`system.access.audit` is the real, documented audit log; permission changes appear there under `service_name = 'unityCatalog'` with `action_name` values like `updatePermissions`.
**Wrong options:** B, C, D reference tables/filters that don't track permission changes.

### Q13: B
Some system schemas require explicit enablement at the account level before their tables begin populating — "table exists, always zero rows" is the classic signature of an unenabled schema, not a permissions or coverage gap.
**Wrong options:** A mischaracterizes the failure mode (a missing grant produces an error, not silent empty results). C is factually wrong — this table does track Lakeflow Declarative Pipelines. D invents a deletion-triggered behavior that isn't how the table works.

### Q14: B
`system.compute.node_timeline` records minute-by-minute utilization metrics per node.
**Wrong options:** A is SCD2 configuration history, not utilization time series. C is a static hardware reference table. D is billing, not utilization.

### Q15: B
Automated, event-driven or polled monitoring that feeds an external system (Slack, PagerDuty, Datadog) is built on the Jobs REST API/CLI — the UI is not a foundation for automation.
**Wrong options:** A and D require a human in the loop, defeating the automation requirement. C is unrelated to job run status.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | System table grant model |
| 2 | Medium | SCD2 "latest row" pattern |
| 3 | Medium | Cost-attribution join pattern |
| 4 | Easy | Query Profile vs. Spark UI scope |
| 5 | Medium | `system.query.history` + `statement_id` join key |
| 6 | Medium | Jobs API endpoint distinctions |
| 7 | Hard | REST API pagination pitfall |
| 8 | Medium | Event log default access (owner-only) |
| 9 | Medium | Event log for per-pipeline data quality detail |
| 10 | Medium | System tables for account-wide comparison |
| 11 | Easy | `table_lineage` purpose |
| 12 | Medium | `system.access.audit` for permission history |
| 13 | Hard | System schema enablement gap |
| 14 | Easy | `node_timeline` vs. `clusters` vs. `node_types` |
| 15 | Easy | REST API as the automation foundation |
