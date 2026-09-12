# Day 22 — Alerting: SQL Alerts, Lakeflow Jobs Notifications, Performance Alerts

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 5: Monitoring and Alerting (10%)** — the Alerting half (Monitoring was Day 21):
- "Use SQL Alerts to monitor data quality."
- "Use the Lakeflow Jobs UI and Jobs API to set up notifications for job status and performance issues."

*(This day is the "act on what Day 21 taught you to observe" counterpart — system tables, Query Profile, and event logs are the signals; SQL Alerts and Job notifications are how you get told about them without watching a dashboard.)*

---

## Part 1 — SQL Alerts: Mechanics

A SQL Alert periodically runs a saved query, evaluates a condition against the result, and sends a notification when the condition is met.

### The alert editor (current unified version)
| Component | Purpose |
|---|---|
| **Query editor** | The SQL query the alert runs |
| **Compute** | Which SQL warehouse executes it |
| **Schedule** | How often the alert runs — **independent of any schedule on the underlying saved query itself** |
| **Condition** | The threshold check against the query result |
| **Notifications** | Users/notification destinations to alert |
| **Share** | Who else in the workspace can view/manage the alert |

**Note:** Databricks unified query setup, condition, schedule, and notifications into a single editor in a newer alert version; **legacy alerts** (the original, separate-steps version) still exist side by side — a scenario referencing "the older alert experience" is talking about legacy alerts, not a different product.

### The condition — critical mechanics
- The condition evaluates **only the first row** of the query result.
- The evaluated column **must be numeric or boolean** — `total_errors > 0`, `missing_data = 1`, `value IS NULL`, etc.
- Supported comparisons: greater than, less than, equal to, not equal, is null, is not null.

**Exam trap:** if your query returns multiple rows (e.g., a per-region breakdown), the alert only ever looks at row one — a scenario where "the alert isn't firing even though some regions clearly have a problem" is testing whether you know to **pre-aggregate to a single row** (e.g., `SELECT MAX(error_count) ...` or `SELECT COUNT(*) FROM ... HAVING ...`) rather than relying on the alert to scan every row itself.

### Advanced settings
| Setting | Effect |
|---|---|
| **Notify on OK** | By default, alerts only notify when they transition into a triggered state; enabling this also notifies when the condition returns to OK — useful for "all clear" messages |
| **Empty result state** | Defines what state to report when the query returns **zero rows** (ambiguous by default) — set this explicitly for "missing data" style checks |
| **Notification template** | Customize the alert message content/formatting sent to destinations |

**Exam trap — the "missing data" alert pattern:** a query like `SELECT COUNT(*) FROM bronze_events WHERE event_date = current_date()` returns **zero rows only if the underlying `GROUP BY` produces no groups**, but `SELECT COUNT(*) ...` without a `GROUP BY` always returns exactly one row (with value `0` if nothing matched) — so "detect data didn't arrive today" is correctly implemented as:
```sql
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS missing_data
FROM bronze_events WHERE event_date = current_date();
```
with condition `missing_data = 1` — not by relying on the query returning an empty result set, which is why "Empty result state" and the `CASE WHEN COUNT(*) = 0` rewrite are the two tools for this exact scenario, and a scenario testing "why didn't my missing-data alert ever fire" is almost always because the query silently returns a row with `0` instead of zero rows.

---

## Part 2 — Using SQL Alerts for Data Quality Monitoring

This is the direct exam objective — SQL Alerts are the mechanism for turning a data-quality check into a proactive notification instead of something someone has to remember to query.

```sql
-- Alert: quarantined/rejected record count spiked (Day 17 quarantine pattern callback)
SELECT COUNT(*) AS quarantined_today
FROM bronze.quarantine_table
WHERE quarantine_date = current_date();
-- Condition: quarantined_today > 100

-- Alert: a Lakeflow pipeline's expectation failure rate (Day 15/17/21 callback)
SELECT SUM(details:flow_progress.metrics.expectations[0].failed_records) AS failed_count
FROM event_log_raw
WHERE event_type = 'flow_progress' AND timestamp > current_timestamp() - INTERVAL 1 HOUR;
-- Condition: failed_count > 0

-- Alert: freshness/staleness — has this table been updated recently?
SELECT CASE WHEN MAX(update_timestamp) < current_timestamp() - INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_stale
FROM silver.orders;
-- Condition: is_stale = 1
```

These three patterns — **threshold breach, quality-check failure, and staleness** — cover the large majority of "use a SQL Alert to monitor data quality" exam scenarios.

---

## Part 3 — Notification Destinations

A **Notification Destination** is a workspace-level object representing an external endpoint — email, Slack, Microsoft Teams, PagerDuty, or a generic webhook — that both **SQL Alerts and Jobs** can send to.

**Exam trap:** only **workspace admins** can create, update, or delete notification destinations. A regular user creating an alert or job can *select* an existing destination but cannot provision a new Slack/PagerDuty integration themselves — a scenario where "a data engineer can't add a new Slack channel as a destination" is testing this admin-only provisioning restriction, not a bug.

```python
# Conceptual: destinations are shared, reusable objects — create once, reference from many alerts/jobs
{
  "display_name": "Data Platform Slack Alerts",
  "config": {"slack": {"url": "https://hooks.slack.com/services/..."}}
}
```

---

## Part 4 — Lakeflow Jobs Notifications

Jobs support two notification mechanisms, configurable at the **job level** and independently at the **task level**:

| Type | Sends to |
|---|---|
| `email_notifications` | A list of email addresses |
| `webhook_notifications` | A list of **notification destination IDs** (Part 3) — email, Slack, Teams, PagerDuty, generic webhook |

### Event types (both blocks support the same set)
| Event | Fires when |
|---|---|
| `on_start` | The run starts |
| `on_success` | The run completes successfully |
| `on_failure` | The run completes unsuccessfully (`INTERNAL_ERROR`, `FAILED`, or `TIMED_OUT`) |
| `on_duration_warning_threshold_exceeded` | The run's duration exceeds a configured threshold — **requires a `health` rule to be defined; without one, this notification is silently never sent even if configured** |
| `on_streaming_backlog_exceeded` | A streaming task's backlog metric exceeds a configured threshold (Public Preview) |

```json
{
  "email_notifications": {
    "on_failure": ["oncall@company.com"],
    "on_duration_warning_threshold_exceeded": ["oncall@company.com"]
  },
  "webhook_notifications": {
    "on_failure": [{"id": "<notification-destination-id>"}]
  },
  "health": {
    "rules": [
      {"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 3600}
    ]
  }
}
```

**Exam trap — the health-rule dependency:** `on_duration_warning_threshold_exceeded` does nothing on its own. It fires only when the job's `health.rules` array defines a `RUN_DURATION_SECONDS` rule with a threshold `value`. A scenario describing "we added the duration-warning notification but never get alerted on slow runs" is testing whether you know the `health` block is a **separate, required** piece of configuration, not an implicit default.

### Streaming backlog alerting
- Configurable health metrics: `STREAMING_BACKLOG_BYTES`, `STREAMING_BACKLOG_RECORDS`, `STREAMING_BACKLOG_SECONDS`, `STREAMING_BACKLOG_FILES`.
- Alerting is based on the **10-minute average** of the chosen metric, not an instantaneous spike — this smooths out normal micro-batch variance.
- If the backlog issue persists, notifications are **resent every 30 minutes** rather than repeating continuously — a scenario about "why am I only getting one alert every half hour during a sustained backlog" is describing this by-design throttling, not a missed alert.

### Limits and filtering
- A **maximum of 3 destinations** can be specified per event type (e.g., at most 3 `on_failure` webhook destinations).
- `notification_settings` lets you suppress notifications for **skipped** or **canceled** runs at the job level — but this filtering must **also be applied at the task level** if task-level notifications are separately configured, or skipped/canceled task runs will still notify even though the job-level setting suggests they shouldn't.

### Webhook payload shape
```json
{
  "event_type": "jobs.on_failure",
  "workspace_id": "...",
  "run": {"run_id": "..."},
  "job": {"job_id": "...", "name": "..."}
}
```
Event type codes follow the `jobs.on_*` naming pattern (`jobs.on_start`, `jobs.on_success`, `jobs.on_failure`, `jobs.on_duration_warning_threshold_exceeded`) — useful for routing logic on the receiving end (e.g., a Lambda/Azure Function that only acts on `jobs.on_failure`).

---

## Part 5 — Combining Day 21's Observability Signals Into Custom Alerts

Not every monitoring need is covered by native job/pipeline notification types. When it isn't, **build a SQL Alert on top of a Day 21 system table or event log query**:

```sql
-- Custom alert: any job hasn't had a successful run in the expected window
-- (native notifications only tell you about the CURRENT run, not "it never ran at all")
WITH latest_success AS (
  SELECT job_id, MAX(period_end_time) AS last_success
  FROM system.lakeflow.job_run_timeline
  WHERE result_state = 'SUCCESS'
  GROUP BY job_id
)
SELECT job_id, last_success
FROM latest_success
WHERE last_success < current_timestamp() - INTERVAL 4 HOURS;
-- Condition: row count > 0 (or wrap in a CASE WHEN for a single-value condition, per Part 1's trap)
```

**Exam framing:** whenever a scenario asks for something native job/pipeline notifications *can't* express directly — "alert me if a job hasn't run successfully at all in N hours" (as opposed to "alert me when the current run fails/is slow") — the answer is a **SQL Alert built on `system.lakeflow.*` or an `event_log()` query**, not a job-level notification setting.

---

## Part 6 — Choosing the Right Alerting Mechanism

| Requirement | Mechanism |
|---|---|
| Data quality threshold on a table (row counts, null rates, freshness) | **SQL Alert** on a query |
| A specific job run failed/started/succeeded | **Job `email_notifications`/`webhook_notifications`, `on_failure`/`on_start`/`on_success`** |
| A job run is taking too long | **Job notification `on_duration_warning_threshold_exceeded` + a `health` rule for `RUN_DURATION_SECONDS`** |
| A streaming task's backlog is growing | **Job notification `on_streaming_backlog_exceeded` + a `health` rule for a `STREAMING_BACKLOG_*` metric** |
| A Lakeflow pipeline's expectations are failing | **SQL Alert on the pipeline's `event_log()`**, or the pipeline UI's Data Quality tab for manual inspection |
| "This job/pipeline hasn't run successfully in a while" (absence, not failure) | **SQL Alert on `system.lakeflow.job_run_timeline`/`pipeline_update_timeline`** |
| Route notifications to Slack/PagerDuty/Teams | **Notification Destination** (admin-provisioned), referenced from either SQL Alerts or Jobs |

---

## Part 7 — Exam Traps Recap

1. Alert conditions evaluate **only the first row** and require a **numeric/boolean** value — pre-aggregate multi-row checks into one row.
2. **Empty result set is ambiguous by default** — use "Empty result state" or rewrite the query (`CASE WHEN COUNT(*) = 0 ...`) to reliably detect missing data.
3. **Legacy alerts and the new unified alert editor coexist** — don't assume one replaced the other for exam purposes.
4. **Notification Destinations are admin-provisioned only** — regular users select from existing ones, they don't create new Slack/PagerDuty integrations themselves.
5. `on_duration_warning_threshold_exceeded` and `on_streaming_backlog_exceeded` **require a `health` rule** to be defined — configuring the notification alone does nothing.
6. Streaming backlog alerts use a **10-minute rolling average** and **resend every 30 minutes** while the condition persists — not instantaneous, not continuous spam.
7. **Maximum 3 destinations per event type** for job notifications.
8. Job-level "no alert for skipped/canceled runs" settings do **not automatically apply to task-level** notifications — both must be configured if both are in use.
9. For "did this ever run successfully" (absence-based) questions, native job notifications aren't the answer — build a **SQL Alert on `system.lakeflow.*`** instead.

---

## Cross-References
- Day 17: Quarantine pattern — the queries this day's data-quality alerts are typically built on top of.
- Day 15/17: `@dlt.expect*` expectations — the source of the Lakeflow event log metrics alerted on here.
- Day 21: `system.lakeflow.job_run_timeline`/`pipeline_update_timeline` and `event_log()` — the query sources for the custom alerts in Part 5.
- Day 23/24: Once an alert fires, these days cover how to actually diagnose and repair the failure.
