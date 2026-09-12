# Day 22 — Quiz: SQL Alerts and Lakeflow Jobs Notifications

**Objective coverage:** Section 5 (10%) — Alerting half.

---

## Question 1
**Objective:** Alert condition evaluation mechanics.

A SQL Alert's query returns 5 rows, each with a `region` and an `error_count` column. The condition is `error_count > 50`. Row 3 (not row 1) has `error_count = 200`; all other rows are below 50. Does the alert fire?

A. Yes — the alert scans all rows for a match
B. No — the condition only evaluates the first row of the result set, which is below 50
C. Yes, but only if the rows are sorted descending by `error_count`
D. The alert fails with an error because multiple rows were returned

---

## Question 2
**Objective:** Condition data type requirement.

Which of the following queries is suitable to drive a SQL Alert condition, without modification?

A. `SELECT region, error_count FROM errors_by_region;`
B. `SELECT MAX(error_count) AS max_errors FROM errors_by_region;`
C. `SELECT * FROM errors_by_region;`
D. `SELECT region FROM errors_by_region;`

---

## Question 3
**Objective:** The "missing data" alert pattern.

A team wants an alert to fire when no rows have landed in a bronze table today. They write `SELECT COUNT(*) AS row_count FROM bronze_events WHERE event_date = current_date();` with condition `row_count = 0`. Does this reliably detect the "no data arrived" case?

A. Yes — `COUNT(*)` without `GROUP BY` always returns exactly one row (value 0 if nothing matched), so the condition can evaluate it correctly
B. No — this query returns an empty result set when there's no data, which the condition can't evaluate
C. No — `COUNT(*)` cannot be used in alert queries at all
D. Yes, but only if "Empty result state" is also configured

---

## Question 4
**Objective:** Notify on OK / Empty result state settings.

By default, does a SQL Alert send a notification when its condition returns to a non-triggered (OK) state after having been triggered?

A. Yes, always
B. No — only if "Notify on OK" is explicitly enabled in advanced settings
C. Yes, but only for legacy alerts
D. This setting doesn't exist; alerts always fire once per schedule regardless of state change

---

## Question 5
**Objective:** Legacy vs. current alert versions.

A colleague mentions they're using "the old alert experience" with separate steps for query, condition, and notifications, while your workspace also has the newer unified alert editor available. What does this indicate?

A. Their workspace is on an unsupported, deprecated product that no longer works
B. Legacy alerts and the newer unified alert editor coexist in the same product — both are valid, functioning alert mechanisms
C. This is impossible — Databricks only supports one alert version at a time
D. They must migrate immediately or lose all existing alerts

---

## Question 6
**Objective:** Notification Destination provisioning.

A data engineer (not a workspace admin) wants to add a brand-new Slack channel as a notification destination for their SQL Alert. Can they do this themselves?

A. Yes, any user can create a new notification destination
B. No — only workspace admins can create, update, or delete notification destinations; the engineer can only select from existing ones
C. Yes, but only for email destinations, not Slack
D. No — notification destinations can only be created via Terraform, never through the UI

---

## Question 7
**Objective:** Job notification event types.

Which job notification event type fires only when a run ends in `INTERNAL_ERROR`, `FAILED`, or `TIMED_OUT`?

A. `on_start`
B. `on_success`
C. `on_failure`
D. `on_duration_warning_threshold_exceeded`

---

## Question 8
**Objective:** Duration warning notification dependency.

A job is configured with `email_notifications.on_duration_warning_threshold_exceeded`, but no `health` block is defined. A run takes far longer than expected. Does the notification fire?

A. Yes — the notification type alone is sufficient
B. No — `on_duration_warning_threshold_exceeded` requires a `health` rule for `RUN_DURATION_SECONDS` to be defined; without it, the notification is never sent
C. Yes, but only after the run completes, never during
D. No — this notification type requires a Premium workspace tier

---

## Question 9
**Objective:** Streaming backlog alerting behavior.

A job task has a `health` rule for `STREAMING_BACKLOG_SECONDS` and a corresponding notification configured. The backlog spikes briefly for 2 minutes then recovers. Does this reliably trigger a notification?

A. Yes, immediately on any spike
B. Not necessarily — alerting is based on a 10-minute rolling average of the metric, so a brief 2-minute spike may not exceed the average threshold
C. No, streaming backlog alerting is not based on averages at all
D. Yes, but only if it happens twice in a row

---

## Question 10
**Objective:** Streaming backlog resend cadence.

A streaming backlog issue persists for 2 hours continuously above threshold. Roughly how many notifications should the team expect to receive during that period?

A. One, at the very start
B. Continuous, one for every microbatch
C. Approximately 4 — resent roughly every 30 minutes while the condition persists
D. None — streaming backlog notifications only fire once per job, ever

---

## Question 11
**Objective:** Destination limits.

What is the maximum number of notification destinations that can be specified for a single event type (e.g., `on_failure`) on a job?

A. 1
B. 3
C. 10
D. Unlimited

---

## Question 12
**Objective:** Task-level vs. job-level notification filtering.

A job has `notification_settings` configured at the job level to suppress notifications for skipped and canceled runs. A specific task within that job also has its own separate notification configuration. Does the job-level suppression automatically apply to that task's notifications too?

A. Yes, task-level notifications always inherit job-level settings
B. No — task-level notification filtering must be configured separately, or skipped/canceled task runs will still send notifications
C. Yes, but only for email notifications, not webhooks
D. This scenario is not possible — jobs and tasks cannot have separate notification configurations

---

## Question 13
**Objective:** Webhook payload structure.

A webhook notification is received with `"event_type": "jobs.on_failure"`. What triggered this payload?

A. The job run started
B. The job run completed successfully
C. The job run ended in an unsuccessful state
D. The job's duration exceeded a warning threshold

---

## Question 14
**Objective:** Choosing the right alerting mechanism for "absence" scenarios.

A team wants to be alerted if a scheduled job's trigger silently breaks and the job never runs at all for several hours (as opposed to running and failing). Which mechanism correctly detects this?

A. `on_failure` job notification — it will fire once the job eventually fails
B. A SQL Alert built on `system.lakeflow.job_run_timeline`, checking for the absence of a recent successful run
C. `on_duration_warning_threshold_exceeded` — a job that never runs has an infinite duration
D. This scenario cannot be monitored in Databricks

---

## Question 15
**Objective:** SQL Alerts for data quality monitoring.

Which query/condition pair correctly implements "alert if more than 100 rows were quarantined today," robust to the "first row only" evaluation rule?

A. `SELECT region, COUNT(*) AS c FROM quarantine GROUP BY region;` with condition `c > 100`
B. `SELECT COUNT(*) AS quarantined_today FROM quarantine WHERE quarantine_date = current_date();` with condition `quarantined_today > 100`
C. `SELECT * FROM quarantine WHERE quarantine_date = current_date();` with condition `quarantine_date IS NOT NULL`
D. `SELECT quarantine_date FROM quarantine;` with condition `quarantine_date = current_date()`

---

## Answer Key

### Q1: B
The condition only evaluates the first row of the result set — with unordered/arbitrary row order, a match anywhere other than row one is silently ignored.
**Wrong options:** A/C misunderstand that Databricks doesn't scan or require sorting; the alert engine simply reads row one. D is false — no error is raised for multi-row results.

### Q2: B
`MAX(error_count)` collapses the result to a single numeric row, satisfying both the "first row only" and "numeric/boolean" requirements.
**Wrong options:** A, C, D either return multiple rows or non-numeric/non-single-value results not suitable for a threshold condition.

### Q3: A
`SELECT COUNT(*) FROM ... WHERE ...` without a `GROUP BY` always returns exactly one row — with the value `0` if nothing matched — so the condition `row_count = 0` evaluates correctly and reliably against a real, present row. This is the safe, standard pattern precisely because it avoids the ambiguous-empty-result problem entirely.
**Wrong options:** B incorrectly assumes this query can return zero rows — it cannot, since there's no `GROUP BY` to eliminate. C is false; `COUNT(*)` is a completely standard and common alert-query aggregate. D is unnecessary here — "Empty result state" matters for queries that *can* legitimately return zero rows (e.g., a `GROUP BY`-based query with no matching groups), which this query is not.

### Q4: B
"Notify on OK" is an opt-in advanced setting — by default, alerts only notify on entering a triggered state, not on returning to OK.
**Wrong options:** A overstates default behavior. C is false — this setting isn't legacy-specific. D denies that state-transition notification exists as a concept at all.

### Q5: B
Databricks explicitly maintains legacy alerts alongside the newer unified alert editor — both are valid, currently functioning mechanisms, not a deprecated-vs-current split requiring migration.
**Wrong options:** A, C, D all incorrectly frame this as a deprecation/incompatibility issue.

### Q6: B
Notification destination creation/management is restricted to workspace admins; other users can only select from destinations that already exist.
**Wrong options:** A and C overstate regular-user permissions. D is false — destinations can be created via UI or API, not only Terraform.

### Q7: C
`on_failure` is specifically defined as firing for `INTERNAL_ERROR`, `FAILED`, or `TIMED_OUT` result states — distinct from `on_start`/`on_success`/duration-based events.
**Wrong options:** A/B/D describe different, non-failure event types.

### Q8: B
`on_duration_warning_threshold_exceeded` is inert without a `health.rules` entry for `RUN_DURATION_SECONDS` — the notification type and the health rule are two separate, both-required pieces of configuration.
**Wrong options:** A overstates what the notification type alone accomplishes. C and D invent unrelated conditions/tier restrictions.

### Q9: B
Streaming backlog alerting evaluates a 10-minute rolling average, not instantaneous values — a brief spike that doesn't sustain long enough to move the average past threshold may not trigger a notification.
**Wrong options:** A and D mischaracterize the mechanism as instant/repetitive-count-based. C denies the documented averaging behavior outright.

### Q10: C
With a persistent issue and a documented 30-minute resend interval, roughly 4 notifications would be expected across 2 hours (120 minutes / 30).
**Wrong options:** A undercounts a sustained, ongoing issue. B overstates frequency — it is not per-microbatch. D denies the resend mechanism entirely.

### Q11: B
A maximum of 3 destinations can be specified per event type for job notifications.
**Wrong options:** A, C, D don't match the documented limit.

### Q12: B
Job-level and task-level notification configurations are independent; suppression settings configured at one level must be mirrored at the other if both are used, or the unfiltered level will still send notifications.
**Wrong options:** A and C overstate automatic inheritance. D denies that independent job/task notification configuration is even possible, which it is.

### Q13: C
`jobs.on_failure` corresponds to the run ending in an unsuccessful state — matching the `on_failure` event definition.
**Wrong options:** A/B/D map to different event types (`on_start`, `on_success`, duration warning).

### Q14: B
Native job notifications only react to events on an **actual run** (start, success, failure, duration) — they cannot detect a run that never happened at all. A SQL Alert querying `system.lakeflow.job_run_timeline` for the absence of a recent successful run is the correct mechanism for this "silence" failure mode.
**Wrong options:** A requires a run to occur, which by definition doesn't happen here. C misapplies duration-warning semantics to a job with no run at all. D is simply false — this is a well-established monitoring pattern.

### Q15: B
A plain `COUNT(*)` with a `WHERE` filter reliably returns one numeric row regardless of how many quarantine records exist, satisfying the alert engine's evaluation rules.
**Wrong options:** A returns multiple rows (order-dependent evaluation risk). C and D don't produce a meaningful numeric/boolean threshold check.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | First-row-only condition evaluation |
| 2 | Easy | Numeric/boolean condition requirement |
| 3 | Hard | `COUNT(*)` reliability vs. `GROUP BY` empty-result nuance |
| 4 | Easy | "Notify on OK" opt-in behavior |
| 5 | Medium | Legacy vs. current alert coexistence |
| 6 | Easy | Admin-only destination provisioning |
| 7 | Easy | `on_failure` event definition |
| 8 | Hard | Health-rule dependency for duration warnings |
| 9 | Medium | 10-minute rolling average for streaming backlog |
| 10 | Medium | 30-minute resend cadence |
| 11 | Easy | Max 3 destinations per event type |
| 12 | Medium | Job-level vs. task-level notification independence |
| 13 | Easy | Webhook `event_type` payload mapping |
| 14 | Hard | Absence-detection requires a custom SQL Alert, not native notifications |
| 15 | Medium | Robust single-row data-quality alert pattern |
