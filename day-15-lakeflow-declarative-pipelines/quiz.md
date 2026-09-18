# Day 15 — Quiz: Lakeflow Declarative Pipelines, Expectations, Control Flow, and AUTO CDC

**Objective coverage:** Section 1 (Developing Code for Data Processing, 22%), Section 3 (Data Transformation, Cleansing, and Quality, 10%).

---

## Question 1
**Objective:** Lakeflow declarative model — what is managed for you.

Which of the following does Lakeflow Declarative Pipelines manage automatically, compared to raw Structured Streaming, that best answers the exam objective?

A. Lakeflow writes all the data to your target database
B. Lakeflow manages trigger scheduling, checkpoint locations, dependency ordering between tables, and compute provisioning — you never write `.option("checkpointLocation", ...)` yourself
C. Lakeflow replaces the need to write any PySpark code at all
D. Lakeflow automatically upgrades the Delta table protocol on every pipeline run

---

## Question 2
**Objective:** `dlt.read` vs. `dlt.read_stream` — the mechanical distinction.

A data engineer writes two Lakeflow table functions. One uses `dlt.read_stream("bronze_orders")` inside the function body; the other uses `dlt.read("bronze_orders")`. Both are decorated with `@dlt.table`. What is the actual difference between the two result tables?

A. `dlt.read_stream` creates a materialized view; `dlt.read` creates a streaming table
B. `dlt.read_stream` creates a streaming table (incremental append processing); `dlt.read` creates a materialized view (full static snapshot each refresh)
C. Both produce identical results — the read call has no effect on the table type
D. `dlt.read_stream` requires `@dlt.view` instead of `@dlt.table`

---

## Question 3
**Objective:** Expectations — three violation levels.

A pipeline has an expectation that `order_id IS NOT NULL`. Three violations arrive simultaneously: a row with a null `order_id`, a row with `amount = -100`, and a row that is perfectly valid. The table has both `@dlt.expect("valid_order_id", "order_id IS NOT NULL")` and `@dlt.expect_or_drop("positive_amount", "amount > 0")`. How many of the three rows appear in the output?

A. All three — expectations only log violations, never filter
B. Two — the row with null `order_id` is logged but kept (soft warning); the row with `amount = -100` is dropped; the valid row passes through
C. One — the null `order_id` row triggers a pipeline halt due to the violation
D. Two — the null `order_id` row is dropped; the negative amount row is logged but kept

---

## Question 4
**Objective:** `@dlt.expect_or_fail` behavior.

A Lakeflow pipeline update processes a batch containing a row that violates an `@dlt.expect_or_fail` constraint. What is the immediate outcome?

A. The violating row is dropped; processing continues with the remaining rows
B. The violating row is logged; the pipeline update completes successfully
C. The pipeline update fails immediately, halting before any downstream tables run
D. The pipeline pauses and waits for manual approval before continuing

---

## Question 5
**Objective:** Lakeflow vs. classic-jobs quarantine mechanism.

A team runs a classic Structured Streaming job (not a Lakeflow pipeline) and wants to separate valid rows from bad rows. Which quarantine mechanism is correct for that execution context?

A. `@dlt.expect_or_drop` — works in any execution context
B. `@dlt.expect_or_drop` is only for Lakeflow pipelines; for classic jobs use `_rescued_data`/`IS NOT NULL` filter inside `foreachBatch`
C. Lakeflow and classic Structured Streaming jobs share the same quarantine API
D. Quarantine is only possible in Production mode pipelines

---

## Question 6
**Objective:** Python `for` loop to generate multiple tables — closure bug.

A data engineer writes the following to generate bronze tables for three source systems:

```python
for table_name in ["customers", "products", "orders"]:
    @dlt.table(name=f"bronze_{table_name}")
    def _make_bronze():
        return spark.readStream.format("cloudFiles").load(
            f"/Volumes/main/landing/{table_name}/"
        )
```

What is the actual result when Lakeflow runs these functions?

A. Three separate bronze tables (`bronze_customers`, `bronze_products`, `bronze_orders`), each reading from its correct source path
B. Three separate bronze tables, but all reading from `/Volumes/main/landing/orders/` because the closure captures the final loop value of `table_name`
C. Only one bronze table named `bronze_orders` is created
D. The pipeline fails to start because `@dlt.table` cannot be used inside a loop

---

## Question 7
**Objective:** Python closure bug — correct fix.

How is the closure bug in the previous question correctly fixed?

A. Use `@dlt.view` instead of `@dlt.table` inside the loop
B. Capture the loop variable as a default function argument: `def _make_bronze(table_name=table_name)`
C. Replace the `for` loop with three separate `@dlt.table` definitions
D. Rename the function inside the loop to have a unique name per iteration

---

## Question 8
**Objective:** AUTO CDC — what it removes.

A team previously implemented CDC ingestion using `foreachBatch` + `MERGE` (Day 10 pattern) with manual out-of-order event handling and a two-statement SCD Type 2 implementation. They migrate to AUTO CDC. Which burdens does AUTO CDC eliminate, and which does it not?

A. Eliminates both out-of-order event handling AND the need to choose a correct `sequence_by` column
B. Eliminates out-of-order event handling (via `sequence_by`) and the manual two-statement SCD2 pattern — but does NOT eliminate the need to correctly identify `keys` and `sequence_by` columns
C. Eliminates all manual effort — no configuration needed at all
D. Eliminates the need for Delta tables; AUTO CDC works with any sink

---

## Question 9
**Objective:** AUTO CDC — `sequence_by` column selection.

A CDC source table has two timestamp columns: `ingestion_time` (when Databricks received the event) and `source_change_time` (when the source system actually changed the record). A team incorrectly uses `ingestion_time` as the `sequence_by` column. What is the consequence?

A. The pipeline fails to start — AUTO CDC validates that `sequence_by` points to a source-change-time column
B. AUTO CDC ignores the column and automatically selects the correct one
C. Changes are applied in ingestion-time order rather than true change-time order — out-of-order source changes can result in the wrong final state in the target
D. `ingestion_time` is always correct since it represents when the change was observed

---

## Question 10
**Objective:** AUTO CDC — `stored_as_scd_type=2`.

After running `stored_as_scd_type=2` on an AUTO CDC flow, a downstream analyst queries the target table. Which column is generated by SCD Type 2 that does NOT appear in a plain overwrite (SCD Type 1) target?

A. `_change_type` — marking insert/update/delete
B. `__START_AT` and `__END_AT` — timestamps bounding when each version of a row was current
C. `operation` — the CDC operation type
D. `sequence_num` — the order in which changes were applied

---

## Question 11
**Objective:** Control flow operators — what they actually mean.

The exam objective mentions "control flow operators (e.g., if/else, for/each, etc.)" in the context of Lakeflow pipelines. What do they actually refer to?

A. A runtime per-row branching feature that routes individual rows down different pipeline paths based on their values
B. Ordinary Python `if`/`for` statements that run at pipeline-authoring time to programmatically generate the set of tables in the pipeline graph
C. A special Lakeflow DSL for conditional data routing at runtime
D. Spark DataFrame `when`/`otherwise` expressions inside a transformation

---

## Question 12
**Objective:** Development vs. Production mode — retry behavior.

A data engineer notices that during pipeline development iteration, a broken transformation immediately causes the pipeline update to fail — without any retry attempts. In Production mode, the same broken transformation triggers retries with backoff before ultimately failing. What explains this difference?

A. A bug in the pipeline configuration — retries should happen in Development mode
B. Development mode deliberately disables automatic retries to surface errors immediately during authoring; Production mode enables them with exponential backoff for resilience
C. The cluster configuration is different between the two modes
D. Retries are only available for streaming tables, not materialized views

---

## Question 13
**Objective:** Streaming table — append-only assumption.

A Lakeflow pipeline reads from a source table that now receives `UPDATE` operations from an upstream process. The pipeline uses `dlt.read_stream("source")` to build a streaming table. What happens?

A. The streaming table correctly reflects the `UPDATE` operations automatically
B. The streaming table won't correctly reflect the `UPDATE` operations — streaming tables assume an append-only source
C. Lakeflow automatically converts the streaming table to a materialized view
D. The pipeline fails immediately upon the first `UPDATE`

---

## Question 14
**Objective:** Triggered vs. Continuous pipeline mode.

A pipeline is configured with `continuous: true`. Which Structured Streaming trigger behavior does this most closely correspond to?

A. `Trigger.AvailableNow()` — processes all available data, then stops
B. `Trigger.Once()` — processes one large batch and stops
C. Default/`ProcessingTime` trigger — runs continuously, processing new data as it arrives
D. `Trigger.ProcessingTime("1 second")` with no upper bound on runtime

---

## Question 15
**Objective:** Expectation metrics — where they go.

A team adds an `@dlt.expect_or_drop` constraint to a table in their Lakeflow pipeline. A row violates the constraint and is dropped. Where can they observe this violation?

A. The violation is only visible in the Spark executor logs
B. The violation count is written to the pipeline's event log as `flow_progress` events and visible in the Pipeline UI's Data Quality tab without any extra instrumentation
C. The violation is only visible if you add a custom logging callback
D. The Pipeline UI shows the violation only for `@dlt.expect_or_fail`, not for `@dlt.expect_or_drop`

---

## Answer Key

### Q1: B
Lakeflow manages trigger scheduling, checkpoint locations, dependency ordering, and compute provisioning automatically. You declare the tables you want; Lakeflow figures out how to build and maintain them — the core declarative model distinction from raw Structured Streaming.

### Q2: B
`dlt.read_stream` creates a **streaming table** (incremental, append-only processing); `dlt.read` creates a **materialized view** (full static snapshot each refresh). The `@dlt.table` decorator is the same in both cases — the read call inside the function body determines the result table type.

### Q3: B
`@dlt.expect` logs the violation but keeps the row (soft warning). `@dlt.expect_or_drop` removes the row from the output. Two rows survive: the null `order_id` row (logged but kept) and the valid row (passes through). The negative-amount row is dropped.

### Q4: C
`@dlt.expect_or_fail` halts the pipeline update the moment a violation occurs — the failure surfaces immediately in the Pipeline UI, and downstream tables that depend on this one do not run. This is the hard data-quality gate, distinct from the soft-warning and drop-only options.

### Q5: B
The two quarantine mechanisms are context-dependent: Lakeflow uses `@dlt.expect*` constraints; classic Structured Streaming jobs use the `_rescued_data` column from Auto Loader (or manually populated rescue columns) filtered inside `foreachBatch`. `@dlt.expect*` does not work outside a Lakeflow pipeline.

### Q6: B
The classic Python late-binding closure bug: without capturing `table_name` as a default argument, all three generated functions close over the same loop variable, which holds its **final** value (`"orders"`) after the loop ends. All three tables incorrectly read from `/Volumes/main/landing/orders/`.

### Q7: B
`def _make_bronze(table_name=table_name)` captures the current value of `table_name` as a default argument at each iteration, avoiding the late-binding closure trap. This is the standard, well-known fix for this Python gotcha.

### Q8: B
AUTO CDC eliminates manual out-of-order-event handling (via `sequence_by`) and the two-statement SCD2 close-then-insert pattern (via `stored_as_scd_type=2`). It does NOT eliminate the need to correctly choose `keys` and `sequence_by` — get the sequencing column wrong and AUTO CDC will apply changes in the wrong logical order.

### Q9: C
`ingestion_time` records when Databricks received the event, not when the source system changed the record. Using it as `sequence_by` means changes are ordered by arrival time rather than true change order. If source-system changes arrive out of order (e.g., a late-arriving change from an earlier source timestamp), they will overwrite the correct state with the wrong version.

### Q10: B
SCD Type 2 generates `__START_AT` and `__END_AT` columns to track the validity window of each historical version. These columns do not appear in a plain overwrite (SCD Type 1) target, where only the current state is stored.

### Q11: B
"Control flow operators" in the Lakeflow exam objective means ordinary Python `if`/`for` statements running at pipeline-authoring time (when Lakeflow imports your module to build the DAG) to programmatically generate the set of tables. It is NOT a runtime per-row branching feature — ordinary Spark DataFrame `when`/`otherwise` logic handles per-row conditional logic inside a single table's transform.

### Q12: B
Development mode deliberately disables automatic retries so errors surface immediately during authoring iteration. Production mode enables retries with exponential backoff for resilience. This is the exact behavior the "auto-optimization to disallow retries" exam bullet describes — it is a feature, not a bug.

### Q13: B
Streaming tables assume their source is append-only. If the upstream source receives `UPDATE`/`DELETE` operations, a streaming table won't correctly reflect them. This is the same limitation Day 10's Change Data Feed material addressed at the raw Structured Streaming level, and Day 16 covers in the Lakeflow context.

### Q14: C
Pipeline `continuous: true` maps to Structured Streaming's default/continuous execution model — runs indefinitely, processing new data as it arrives. `Triggered` mode maps to `Trigger.AvailableNow()` (process backlog, then stop).

### Q15: B
Every expectation's pass/fail counts are written to the pipeline's **event log** as `flow_progress` events (Day 21 covers querying this via `event_log()`). They are also visible in the Pipeline UI's Data Quality tab automatically, with no extra instrumentation required. Both `@dlt.expect_or_drop` and `@dlt.expect_or_fail` violations are recorded this way.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Lakeflow declarative model — what it manages |
| 2 | Easy | `dlt.read` vs `dlt.read_stream` mechanical distinction |
| 3 | Medium | Three expectation violation levels |
| 4 | Easy | `@dlt.expect_or_fail` halts pipeline update |
| 5 | Medium | Lakeflow vs. classic-jobs quarantine mechanism |
| 6 | Hard | Python closure bug in `for`-loop table generation |
| 7 | Medium | Fixing the closure bug with default-argument capture |
| 8 | Medium | AUTO CDC removes what burden, not which |
| 9 | Hard | `sequence_by` wrong column = wrong results |
| 10 | Medium | SCD Type 2 `__START_AT`/`__END_AT` generation |
| 11 | Medium | "Control flow operators" = Python at authoring time |
| 12 | Medium | Development mode retry suppression — intentional |
| 13 | Medium | Streaming table append-only limitation |
| 14 | Easy | Pipeline continuous mode maps to default trigger |
| 15 | Easy | Expectation metrics go to event log + Pipeline UI |
