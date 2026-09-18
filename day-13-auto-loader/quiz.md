# Day 13 — Quiz: Auto Loader Configuration, Schema Evolution, and Quarantine

**Objective coverage:** Section 2 (7%) and the classic-jobs half of Section 3 (10%)

---

## Question 1
**Objective:** `cloudFiles.schemaLocation` requirement.

A team configures Auto Loader with `.option("cloudFiles.format", "json")` and no `.schema(...)` call, but omits `cloudFiles.schemaLocation`. What happens?

A. Auto Loader infers the schema fresh on every micro-batch, with no persisted history
B. The stream fails to start — `schemaLocation` is required for Auto Loader to infer and track schema across restarts
C. Auto Loader falls back to using the checkpoint location automatically
D. Schema inference works, but `_rescued_data` is disabled

---

## Question 2
**Objective:** `addNewColumns` default behavior.

A stream is configured with no explicit `.schema(...)` and no `cloudFiles.schemaEvolutionMode` set. A new column appears in the source. What happens?

A. The new column is silently ignored
B. The stream stops with `UnknownFieldException`; the schema at `schemaLocation` has already been updated to include the new column before the exception is raised
C. The new column's values are automatically routed to `_rescued_data` and the stream continues
D. The stream fails permanently and requires a manual schema update before it can ever resume

---

## Question 3
**Objective:** `failOnNewColumns` vs. `addNewColumns`.

A governed bronze table has a strict schema contract — new fields must never be silently absorbed, even after a restart. Which `schemaEvolutionMode` fits?

A. `addNewColumns`
B. `rescue`
C. `failOnNewColumns`
D. `none`

---

## Question 4
**Objective:** Distinguish the "restart resumes automatically" trap.

Both `addNewColumns` and `failOnNewColumns` cause the stream to fail when a new column appears. What is the actual operational difference between them?

A. There is no difference — both behave identically
B. `addNewColumns` resumes successfully on a simple restart; `failOnNewColumns` requires a manual schema update or file removal before it can resume
C. `failOnNewColumns` never fails; only `addNewColumns` does
D. `addNewColumns` requires manual intervention; `failOnNewColumns` resumes automatically

---

## Question 5
**Objective:** `_rescued_data` contents.

A record has a field whose value doesn't match the inferred type (e.g., a string where a number was expected). Under the default `addNewColumns` mode, what happens to that field?

A. The stream fails immediately, since type mismatches aren't tolerated in `addNewColumns`
B. The column's value is set to NULL and the original mismatched value is captured in `_rescued_data`
C. The value is silently dropped with no trace
D. Auto Loader automatically casts the value to fit the inferred type

---

## Question 6
**Objective:** `schemaHints` behavior.

A team sets `cloudFiles.schemaHints = "amount DOUBLE"` to force a column's type. A record arrives with `amount` as a non-numeric string. What happens?

A. Auto Loader casts the string to the nearest valid double
B. The row is dropped entirely
C. `amount` is set to NULL for that row and the original value is captured in `_rescued_data` — schemaHints tells the reader what type to expect, it doesn't coerce non-conforming values
D. The stream fails, since schemaHints values must always be valid

---

## Question 7
**Objective:** Directory listing vs. file notification — switching modes.

A production Auto Loader stream currently uses directory listing (`cloudFiles.useNotifications = false`). The team wants to switch to file notification mode as file volume grows. What must they do?

A. Rebuild the entire pipeline from scratch, since discovery mode can't change mid-pipeline
B. Simply change the option and restart the stream — this is a safe, supported operation that preserves exactly-once guarantees
C. Manually reprocess all historical files after switching
D. File notification mode cannot be enabled after a stream has already started

---

## Question 8
**Objective:** `maxFilesPerTrigger` and `maxBytesPerTrigger` together.

A stream sets both `cloudFiles.maxFilesPerTrigger = "1000"` and `cloudFiles.maxBytesPerTrigger = "10g"`. In a given micro-batch, which limit governs?

A. Whichever is specified first in the code
B. The two are averaged together
C. Whichever limit is reached first — Auto Loader honors the lower effective bound
D. `maxBytesPerTrigger` always takes precedence over `maxFilesPerTrigger`

---

## Question 9
**Objective:** Classic-jobs quarantine pattern mechanism.

A team needs to split one Auto Loader stream into a "clean" Delta table and a "quarantine" Delta table based on whether `_rescued_data` is populated, without using Lakeflow Declarative Pipelines. Which mechanism enables writing one micro-batch to two separate destinations with custom logic?

A. `writeStream.format("delta").option("path", ...)`
B. `foreachBatch`
C. `Trigger.AvailableNow`
D. `cloudFiles.schemaHints`

---

## Question 10
**Objective:** Classic jobs vs. Lakeflow Declarative Pipelines quarantine — same goal, different mechanism.

A scenario describes implementing a quarantine pattern **inside a Lakeflow Declarative Pipeline**. Which mechanism is the correct fit, instead of manually filtering on `_rescued_data` in `foreachBatch`?

A. `@dlt.expect_or_drop` / `@dlt.expect_all_or_drop` constraints
B. `cloudFiles.useNotifications`
C. `spark.sql.adaptive.enabled`
D. `Trigger.ProcessingTime`

---

## Question 11
**Objective:** `addNewColumnsWithTypeWidening` and its version requirement.

A team wants Auto Loader to automatically widen a column from `INT` to `LONG` when the source data changes, without a full manual schema rewrite, while still failing (and requiring restart) on genuinely new columns. Which mode fits, and what's a key prerequisite?

A. `rescue` mode; no version prerequisite
B. `addNewColumnsWithTypeWidening`; requires DBR 16.4 or above
C. `none`; requires an explicit `.schema(...)` call
D. `failOnNewColumns`; requires Unity Catalog to be disabled

---

## Question 12
**Objective:** Auto Loader is a Structured Streaming source, not an "always streaming" tool.

A team wants Auto Loader's incremental, exactly-once, schema-evolving ingestion, but needs the job to run on a nightly schedule rather than as an always-on cluster. Is this possible with Auto Loader?

A. No — Auto Loader can only run as a continuously running stream
B. Yes — Auto Loader is a Structured Streaming source (`format("cloudFiles")`) and can be run with `Trigger.AvailableNow` to process the backlog and then terminate, on any schedule
C. No — Auto Loader requires `Trigger.ProcessingTime` and cannot use `Trigger.AvailableNow`
D. Yes, but only if `cloudFiles.useNotifications` is disabled

---

## Answer Key

### Q1: B
`cloudFiles.schemaLocation` is required whenever Auto Loader is asked to infer and evolve schema (i.e., no fixed `.schema(...)` is supplied) — it's where the inferred schema and its evolution history persist across stream restarts. Without it, the stream fails to start rather than silently degrading.
**Wrong options:** A ignores that persistence across restarts is the entire point. C invents a fallback that doesn't exist. D is unrelated — `_rescued_data` availability doesn't depend on `schemaLocation` being set.

### Q2: B
`addNewColumns` (the default with no explicit schema) fails the stream via `UnknownFieldException` on a new column, but Auto Loader has already inferred and persisted the updated schema to `schemaLocation` *before* raising the exception — a restart resumes immediately with the new column active.
**Wrong options:** A describes `none` mode with an explicit schema. C describes `rescue` mode. D describes `failOnNewColumns`, not `addNewColumns`.

### Q3: C
`failOnNewColumns` is the strict-contract option: the stream fails and does **not** auto-resume on a simple restart — a manual schema update or removal of the offending file is required first, which is exactly the behavior a governed table needing zero silent absorption of new fields wants.
**Wrong options:** A (`addNewColumns`) resumes automatically on restart — not strict enough for this requirement. B (`rescue`) never fails and absorbs unexpected fields into `_rescued_data`, the opposite of the stated need. D (`none`) silently ignores new columns entirely.

### Q4: B
Both modes fail the stream on a new column, but `addNewColumns` resumes cleanly on a plain restart (the schema was already updated), while `failOnNewColumns` requires deliberate manual intervention (update the schema or remove the offending file) before the stream can proceed again.
**Wrong options:** A denies a real, exam-tested distinction. C and D each invert which mode does what.

### Q5: B
Type mismatches against the inferred/current schema are one of the core categories `_rescued_data` captures, regardless of which schema evolution mode governs *new columns* — the column is set to NULL and the actual mismatched value is preserved in `_rescued_data` alongside the source file path.
**Wrong options:** A confuses type-mismatch handling with new-column handling — they're different failure categories. C denies the rescue mechanism exists. D is false — Auto Loader does not silently coerce mismatched types.

### Q6: C
`schemaHints` instructs the reader to interpret a column *as* a given type — it does not cast or validate existing values against that type. A value that doesn't fit is rescued into `_rescued_data`, exactly like any other type mismatch.
**Wrong options:** A and D both assume `schemaHints` performs casting/validation, which it doesn't. B ignores the rescue mechanism entirely.

### Q7: B
Switching `cloudFiles.useNotifications` between `true` and `false` across stream restarts (same checkpoint) is documented as a safe, supported operation — Auto Loader preserves exactly-once processing guarantees across the switch.
**Wrong options:** A, C, and D all invent unnecessary rebuild/reprocessing requirements that don't reflect the actual supported behavior — this is the exact trap the exam sets up.

### Q8: C
When both `maxFilesPerTrigger` and `maxBytesPerTrigger` are set, Auto Loader processes up to whichever limit is reached first in a given micro-batch — effectively honoring the lower of the two effective bounds.
**Wrong options:** A, B, and D all invent tie-breaking rules that don't reflect how the two limits actually interact.

### Q9: B
`foreachBatch` is the standard Structured Streaming mechanism for writing one micro-batch to multiple sinks with custom per-batch logic — exactly what a clean/quarantine split requires, since a plain `writeStream.format(...).save()` can only target one destination.
**Wrong options:** A only supports a single sink per query. C controls scheduling cadence, not sink routing. D is a schema-typing option, unrelated to routing rows to destinations.

### Q10: A
Inside a Lakeflow Declarative Pipeline, the equivalent quarantine goal is achieved declaratively via `@dlt.expect_or_drop`/`@dlt.expect_all_or_drop` constraints — same objective (separate good data from bad) as the classic `_rescued_data`/`foreachBatch` pattern, but a different, declarative mechanism appropriate to that execution context.
**Wrong options:** B, C, and D are unrelated configuration options with no connection to data-quality routing.

### Q11: B
`addNewColumnsWithTypeWidening` automatically widens compatible type changes (e.g., `INT`→`LONG`, `FLOAT`→`DOUBLE`) while still requiring a restart on genuinely new columns, exactly like `addNewColumns`. This mode requires **Databricks Runtime 16.4 or above** per current documentation.
**Wrong options:** A (`rescue`) never evolves the schema at all, so it wouldn't widen types either. C (`none`) requires an explicit schema but doesn't widen types automatically. D invents an unrelated, incorrect Unity Catalog prerequisite.

### Q12: B
Auto Loader is fundamentally a Structured Streaming source (`format("cloudFiles")`), so it inherits all of Structured Streaming's trigger options, including `Trigger.AvailableNow` — which processes everything currently available and then terminates, making it perfectly suited to a scheduled Job rather than an always-on cluster.
**Wrong options:** A and C both incorrectly assume Auto Loader is locked into continuous execution. D invents an unrelated dependency on file discovery mode.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | `schemaLocation` requirement for inference/evolution |
| 2 | Medium | `addNewColumns` default — schema updated before the exception |
| 3 | Medium | `failOnNewColumns` for strict schema contracts |
| 4 | Hard | `addNewColumns` vs. `failOnNewColumns` — auto-resume vs. manual intervention |
| 5 | Medium | `_rescued_data` captures type mismatches regardless of new-column mode |
| 6 | Medium | `schemaHints` does not cast — mismatches are rescued |
| 7 | Hard | Switching discovery modes mid-pipeline is safe and supported |
| 8 | Medium | `maxFilesPerTrigger` + `maxBytesPerTrigger` — lower limit wins |
| 9 | Easy | `foreachBatch` for multi-sink routing |
| 10 | Medium | Classic quarantine (`_rescued_data`) vs. Lakeflow (`@dlt.expect*`) |
| 11 | Hard | `addNewColumnsWithTypeWidening` and its DBR 16.4+ requirement |
| 12 | Medium | Auto Loader as a Structured Streaming source, not "always streaming" |
