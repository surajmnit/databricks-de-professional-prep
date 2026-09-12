# Day 22 — Cheat Sheet: SQL Alerts and Lakeflow Jobs Notifications

## SQL Alert Editor Components

```
Query → Compute (warehouse) → Schedule (independent of query's own schedule)
→ Condition → Notifications → Share
```
Legacy alerts and the newer unified editor **coexist** — both valid.

---

## Condition Rules

- Evaluates **only the first row** of the query result.
- Value must be **numeric or boolean**.
- Multi-row checks → pre-aggregate: `MAX(...)`, `SUM(...)`, or `COUNT(*)` — never rely on row order.

**Reliable single-row patterns:**
```sql
SELECT COUNT(*) AS c FROM t WHERE ...;                          -- always 1 row (0 if none match)
SELECT MAX(x) AS worst FROM (SELECT ..., COUNT(*) AS x GROUP BY ...);
SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS is_missing FROM t WHERE ...;
```

## Advanced Settings

| Setting | Effect |
|---|---|
| Notify on OK | Off by default — enable to get "all clear" notifications |
| Empty result state | Only matters for queries that **can** legitimately return zero rows (e.g., `GROUP BY`-based) |
| Notification template | Customize message content |

---

## Notification Destinations

- Email, Slack, Teams, PagerDuty, generic webhook.
- **Admin-only** to create/update/delete. Regular users select existing ones.
- Shared object — same destination usable by SQL Alerts **and** Jobs.

---

## Job Notification Event Types

| Event | Fires when |
|---|---|
| `on_start` | Run starts |
| `on_success` | Run succeeds |
| `on_failure` | Run ends `INTERNAL_ERROR` / `FAILED` / `TIMED_OUT` |
| `on_duration_warning_threshold_exceeded` | Duration exceeds `health` rule (⚠️ **requires** a `health.rules` `RUN_DURATION_SECONDS` entry — inert without it) |
| `on_streaming_backlog_exceeded` | Streaming backlog metric exceeds `health` rule |

```json
{
  "email_notifications": {"on_failure": ["a@b.com"]},
  "webhook_notifications": {"on_failure": [{"id": "<dest-id>"}]},
  "health": {"rules": [{"metric": "RUN_DURATION_SECONDS", "op": "GREATER_THAN", "value": 3600}]}
}
```

## Streaming Backlog Metrics

`STREAMING_BACKLOG_BYTES` / `_RECORDS` / `_SECONDS` / `_FILES`
- Based on **10-minute rolling average** (not instantaneous).
- Resent every **~30 minutes** while condition persists (not continuous).

## Limits

- **Max 3 destinations** per event type.
- Job-level `notification_settings` (skip/cancel suppression) does **not** auto-apply to task-level notifications — configure both if both are used.

## Webhook Payload
```json
{"event_type": "jobs.on_failure", "run": {"run_id": "..."}, "job": {"job_id": "...", "name": "..."}}
```

---

## Choosing the Mechanism

| Need | Use |
|---|---|
| Data quality threshold / freshness check | SQL Alert |
| This run failed/started/succeeded | Job `on_failure`/`on_start`/`on_success` |
| This run is too slow | Job `on_duration_warning_threshold_exceeded` + `health` rule |
| Streaming backlog growing | Job `on_streaming_backlog_exceeded` + `health` rule |
| Pipeline expectations failing | SQL Alert on pipeline `event_log()` |
| Job never ran at all (absence) | SQL Alert on `system.lakeflow.job_run_timeline` |
| Route to Slack/PagerDuty/Teams | Notification Destination (admin-provisioned) |

---

## Exam Trap Shortlist

1. Condition = **first row only**, numeric/boolean — pre-aggregate multi-row checks.
2. Plain `COUNT(*)` (no `GROUP BY`) always returns 1 row — reliable for missing-data checks without needing "Empty result state."
3. "Empty result state" matters only for queries that **can** return zero rows (`GROUP BY`-based).
4. "Notify on OK" is **opt-in**, not default.
5. Legacy and current alert editors **coexist** — not a deprecated/current split.
6. Notification Destinations are **admin-only** to create.
7. `on_duration_warning_threshold_exceeded` / `on_streaming_backlog_exceeded` require a matching **`health` rule** — configuring the notification alone does nothing.
8. Streaming backlog: **10-min average**, **~30-min resend** — not instant, not continuous.
9. **Max 3 destinations** per event type.
10. Job-level and task-level notification settings are **independent** — mirror suppression settings at both levels.
11. Absence ("never ran") requires a **custom SQL Alert** on system tables — native job notifications can't detect it.
