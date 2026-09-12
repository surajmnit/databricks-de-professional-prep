# Day 22 — Hands-On Lab: SQL Alerts and Lakeflow Jobs Notifications

## Lab Objectives

1. Create a SQL Alert for a data-quality threshold and correctly handle the "first row, numeric/boolean" evaluation rule.
2. Reproduce and fix the "missing data" alert false-negative caused by an ambiguous empty result.
3. Configure job-level `email_notifications`/`webhook_notifications` for `on_failure` and `on_start`.
4. Reproduce and fix the "duration warning notification configured but never fires" failure by adding the required `health` rule.
5. Configure a streaming backlog health rule and notification.
6. Build a custom "job hasn't run successfully in N hours" alert on top of Day 21's system tables.

**Environment note:** SQL Alerts require a SQL warehouse and Databricks SQL access. Notification Destinations (Slack/PagerDuty/Teams/webhook) require workspace admin rights to provision — if you don't have admin access, use **email** as your destination throughout this lab (available to any user with `SELECT`/ownership on the alert) and read the destination-provisioning steps as reference material.

---

## Step 1 — Create a data-quality SQL Alert

```sql
-- Save this as a query first
SELECT COUNT(*) AS quarantined_today
FROM main.default.quarantine_table
WHERE quarantine_date = current_date();
```
In the SQL Editor: **Create Alert** → select this query → **Condition**: `quarantined_today > 100` → **Schedule**: every 15 minutes → **Notifications**: your email.

**What to observe:** the schedule you set here runs *independently* of any schedule on the underlying saved query — the alert always re-runs the query itself at its own cadence.

---

## Step 2 — 💥 Break it on purpose: multi-row query, alert never reacts correctly

```sql
-- This looks like it should alert per-region, but it won't work as expected
SELECT region, COUNT(*) AS quarantined_today
FROM main.default.quarantine_table
WHERE quarantine_date = current_date()
GROUP BY region;
```
Create an alert on this query with condition `quarantined_today > 100`.

**Predict then verify:** if region `EU` has 500 quarantined rows but region `US` (returned first) has only 5, does the alert fire?
**Answer:** it depends entirely on **row order** — the condition only evaluates the **first row** of the result set. This is unreliable for a per-region check. **Fix:**
```sql
SELECT MAX(region_count) AS max_quarantined
FROM (
  SELECT region, COUNT(*) AS region_count
  FROM main.default.quarantine_table
  WHERE quarantine_date = current_date()
  GROUP BY region
);
-- Condition: max_quarantined > 100 — now a single, reliable row
```

---

## Step 3 — 💥 Break it on purpose: the "missing data" alert that never fires

```sql
-- Intended to detect "no data arrived today" — but this ALWAYS returns exactly one row with value 0,
-- it never returns an EMPTY result set, so relying on "Empty result state" alone won't catch this case
SELECT COUNT(*) AS row_count
FROM main.default.bronze_events
WHERE event_date = current_date();
```
**Predict then verify:** if zero events arrived today, what does this query return — an empty result, or one row with `row_count = 0`?
**Answer:** one row with `row_count = 0` — `COUNT(*)` without a `GROUP BY` never produces an empty result set. **Fix the condition, not the result-state setting:**
```sql
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS missing_data
FROM main.default.bronze_events
WHERE event_date = current_date();
-- Condition: missing_data = 1
```

---

## Step 4 — Configure job notifications with `on_failure` and `on_start`

Via the Jobs UI (Edit → Notifications) or the API:
```json
{
  "email_notifications": {
    "on_start": ["you@company.com"],
    "on_failure": ["you@company.com"]
  }
}
```
Trigger a run of a job from an earlier day's lab and confirm you receive the `on_start` notification, then intentionally break the job (e.g., point a task at a nonexistent table) and confirm the `on_failure` notification arrives.

---

## Step 5 — 💥 Break it on purpose: duration warning notification with no health rule

```json
{
  "email_notifications": {
    "on_duration_warning_threshold_exceeded": ["you@company.com"]
  }
}
```
Run a job that takes several minutes. **Predict then verify:** does the notification ever arrive, no matter how long the job runs?
**Answer:** No — without a `health` rule defining `RUN_DURATION_SECONDS`, this notification type is configured but inert. **Fix:**
```json
{
  "email_notifications": {
    "on_duration_warning_threshold_exceeded": ["you@company.com"]
  },
  "health": {
    "rules": [
      {"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 60}
    ]
  }
}
```
Re-run the same job and confirm the notification now arrives once the run exceeds 60 seconds.

---

## Step 6 — Streaming backlog health rule

If you have a streaming task from an earlier day's lab:
```json
{
  "email_notifications": {
    "on_streaming_backlog_exceeded": ["you@company.com"]
  },
  "health": {
    "rules": [
      {"metric": "STREAMING_BACKLOG_SECONDS", "op": "GREATER_THAN", "value": 300}
    ]
  }
}
```
**What to observe (or read, if you can't reproduce a real backlog):** this evaluates a **10-minute rolling average** of the backlog metric, and if it stays above threshold, you'll get a notification roughly every **30 minutes**, not continuously — don't expect (or design monitoring dashboards around) instant, per-microbatch alerting from this mechanism.

---

## Step 7 — Custom "hasn't run successfully" alert using Day 21 system tables

```sql
WITH latest_success AS (
  SELECT job_id, MAX(period_end_time) AS last_success
  FROM system.lakeflow.job_run_timeline
  WHERE result_state = 'SUCCESS'
  GROUP BY job_id
)
SELECT COUNT(*) AS stale_job_count
FROM latest_success
WHERE last_success < current_timestamp() - INTERVAL 4 HOURS;
```
Create a SQL Alert on this with condition `stale_job_count > 0`. **What to observe:** this catches a failure mode native job notifications cannot — a job that simply **never triggered at all** (e.g., a broken schedule trigger) rather than one that ran and failed.

---

## Stretch Task

Combine everything from Day 21 and today: build a SQL Alert on a Lakeflow pipeline's `event_log()` that fires when the sum of failed expectation records over the last hour exceeds a threshold, and route it to a Slack notification destination (or email, if you don't have admin rights to provision Slack). This mirrors a real production "data quality regression" alert a data platform team would run continuously.

---

## Lab Checklist

- [ ] Created a working SQL Alert on a single-row, numeric condition
- [ ] Reproduced and fixed the multi-row "wrong row evaluated" failure
- [ ] Reproduced and fixed the "missing data never triggers" false negative
- [ ] Configured job `on_start`/`on_failure` notifications and confirmed both fire
- [ ] Reproduced and fixed the "duration warning never fires without a health rule" failure
- [ ] Configured a streaming backlog health rule and understood its 10-min-average/30-min-resend cadence
- [ ] Built a custom "job hasn't run successfully in N hours" SQL Alert on `system.lakeflow.job_run_timeline`
- [ ] (Stretch) Built an event-log-based data quality alert routed to an external destination

---

## Cross-References
- Day 17: The quarantine table queried in Step 1.
- Day 21: `system.lakeflow.job_run_timeline` and `event_log()` reused in Step 7 and the stretch task.
- Day 23/24: What to do once one of these alerts actually fires.
