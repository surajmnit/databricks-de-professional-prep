# Day 15 — Lakeflow Declarative Pipelines: Expectations, Control Flow, and AUTO CDC

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 1: Developing Code for Data Processing using Python and SQL (22%)**:
- "Build and manage reliable, production-ready data pipelines for batch and streaming data using Lakeflow Spark Declarative Pipelines and Autoloader."
- "Use AUTO CDC APIs (formerly APPLY CHANGES) to simplify CDC in Lakeflow Spark Declarative Pipelines."
- "Create a pipeline component that uses control flow operators (e.g., if/else, for/each, etc.)."
- "Choose the appropriate configs for environments and dependencies, high memory for notebook tasks, and auto-optimization to disallow retries."
- "Compare Spark Structured Streaming and Lakeflow Spark Declarative Pipelines to determine the optimal approach for building scalable ETL pipelines." *(Day 14 Part 7 previewed this; this day completes the comparison now that Lakeflow's own vocabulary is on the table.)*

And the **Lakeflow half** of **Section 3: Data Transformation, Cleansing, and Quality (10%)**:
- "Develop a quarantining process for bad data with Lakeflow Spark Declarative Pipelines, or Autoloader in classic jobs." *(Day 13 covered the classic-jobs `_rescued_data`/`foreachBatch` half; this day covers the declarative `@dlt.expect*` half.)*

**Terminology reminder:** this feature was formerly called **Delta Live Tables (DLT)**; the exam guide's current name is **Lakeflow Spark Declarative Pipelines**. The `@dlt` decorator namespace itself is unchanged (still `import dlt`) — only the product name changed. `APPLY CHANGES` is now **AUTO CDC**.

---

## Part 1 — What Lakeflow Declarative Pipelines Is

### The Declarative Model

Instead of writing imperative `readStream`/`writeStream` code and managing triggers/checkpoints yourself (Day 14), you **declare the tables you want** and Lakeflow figures out how to build and maintain them:

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(
    comment="Raw orders landing zone"
)
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/main/landing/orders/"))

@dlt.table(
    comment="Cleaned, validated orders"
)
def silver_orders():
    return (dlt.read_stream("bronze_orders")
        .filter(col("amount") > 0))
```

**What Lakeflow manages for you that Structured Streaming doesn't:**
- Trigger scheduling and checkpoint locations (you never write `.option("checkpointLocation", ...)` yourself)
- Dependency ordering between tables (`dlt.read`/`dlt.read_stream` builds the DAG automatically — `silver_orders` runs after `bronze_orders`)
- Cluster/compute provisioning for the pipeline run
- Data-quality enforcement (expectations — Part 2) and lineage tracking, surfaced in the pipeline's event log (Day 21)

### `dlt.read` vs. `dlt.read_stream`

| Call | Reads as | Result table type |
|---|---|---|
| `dlt.read_stream("upstream_table")` | Incremental, append-only | Streaming Table (Part 5) |
| `dlt.read("upstream_table")` | Full, static snapshot each refresh | Materialized View (Part 5) |

**Exam trap:** the function decorated with `@dlt.table` looks identical either way — it's the **read call inside the function body** (`dlt.read` vs. `dlt.read_stream`) that determines whether Lakeflow treats the result as a streaming table or a materialized view, not the decorator itself.

---

## Part 2 — Expectations: Declarative Data Quality

Expectations are SQL boolean conditions attached to a table that Lakeflow evaluates against every row, with three possible responses to a violation:

| Decorator | On violation |
|---|---|
| `@dlt.expect("name", "condition")` | Row is **kept**, violation is only recorded in metrics — a soft warning |
| `@dlt.expect_or_drop("name", "condition")` | Row is **dropped** from the output, violation recorded in metrics |
| `@dlt.expect_or_fail("name", "condition")` | The entire pipeline **update fails** the moment a violation occurs |

For multiple constraints at once, the `_all` variants take a dictionary of `{name: condition}` pairs:

```python
import dlt
from pyspark.sql.functions import col

@dlt.table
@dlt.expect_all_or_drop({
    "valid_amount": "amount > 0",
    "valid_customer": "customer_id IS NOT NULL",
})
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

**Where the metrics go:** every expectation's pass/fail counts are written to the pipeline's **event log** as `flow_progress` events (Day 21 covers querying this via `event_log()`), and are visible in the pipeline UI's Data Quality tab without any extra instrumentation.

**Exam trap — this vs. Day 13's classic pattern:** both achieve the same goal (separate good data from bad), but the mechanism differs by execution context:

| Context | Quarantine mechanism |
|---|---|
| Classic job / raw Structured Streaming (Day 13) | Manually filter on `_rescued_data IS NULL`/`IS NOT NULL` inside `foreachBatch`, write to two tables yourself |
| Lakeflow Declarative Pipeline (this day) | Declare `@dlt.expect_or_drop`/`@dlt.expect_all_or_drop` constraints — Lakeflow handles the routing and metrics for you |

A scenario naming "Lakeflow Declarative Pipelines" wants the expectations answer; a scenario naming "classic job" or "Structured Streaming" wants Day 13's `_rescued_data`/`foreachBatch` answer. Don't cross the two.

---

## Part 3 — Control Flow in Pipeline Definitions

**This is the single most misunderstood bullet in this objective.** "Control flow operators (if/else, for/each, etc.)" does **not** mean Lakeflow has a special runtime branching DSL for data flowing through the pipeline. The pipeline's DAG is fixed once it's built — there's no "if this row's value is X, route it down a different path" mechanism at the pipeline-graph level (ordinary `CASE WHEN`/`.filter()` logic inside a single table's transformation already handles that).

**What it actually means:** pipeline definitions are plain Python modules. `@dlt.table`-decorated functions are just Python functions, so **ordinary Python `if`/`for` statements run at pipeline-authoring time** (when Lakeflow imports your source files to build the graph), letting you *generate* the set of tables/flows programmatically instead of hand-writing each one.

### `for` — generating repetitive tables

```python
import dlt

source_tables = ["customers", "products", "regions"]

for name in source_tables:
    @dlt.table(name=f"bronze_{name}")
    def _make_bronze(table_name=name):  # default-arg capture avoids late-binding bugs
        return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .load(f"/Volumes/main/landing/{table_name}/"))
```

**Exam trap — the closure bug:** if you write `def _make_bronze(): return spark.readStream...load(f"/Volumes/main/landing/{name}/")` **without** capturing `name` as a default argument, every generated table function closes over the *same* loop variable `name`, and by the time Lakeflow actually calls these functions, `name` holds its **final** loop value for all of them — all three tables would incorrectly read from the same last source. The `table_name=name` default-argument pattern is the standard fix, and this is a real, common Python gotcha the exam can test independent of Lakeflow-specific knowledge.

### `if`/`else` — conditionally including a table

```python
import dlt

include_audit_table = spark.conf.get("pipelines.includeAudit", "false") == "true"

if include_audit_table:
    @dlt.table
    def audit_log():
        return dlt.read_stream("bronze_orders").selectExpr("current_timestamp() as logged_at", "*")
```

This decides **which tables exist in the graph at all** (e.g., a dev-only debugging table), evaluated once when the pipeline graph is constructed — not per-row, and not re-evaluated mid-run.

**Exam framing:** if a scenario describes "the pipeline should produce a bronze table for each of N source systems without duplicating code," that's the `for`-loop generation pattern. If it describes "the pipeline behaves differently based on which row's value" within one table, that's ordinary Spark SQL/DataFrame logic (`when`/`otherwise`, `filter`), not a "control flow operator" in the exam-objective sense.

---

## Part 4 — AUTO CDC (formerly APPLY CHANGES)

AUTO CDC is Lakeflow's declarative replacement for Day 10's hand-written `foreachBatch` + `MERGE` CDC/SCD pattern.

```python
import dlt

dlt.create_streaming_table("customers_silver")

dlt.create_auto_cdc_flow(
    target="customers_silver",
    source="customers_cdc_bronze",
    keys=["customer_id"],
    sequence_by="change_timestamp",
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "change_timestamp"],
    stored_as_scd_type=2,   # or 1
)
```

| Parameter | Purpose |
|---|---|
| `keys` | Column(s) identifying a unique logical row — the merge key |
| `sequence_by` | Column establishing correct event order — **this is what replaces the manual out-of-order-event handling** Day 10 flagged as a burden of hand-rolled CDC |
| `apply_as_deletes` | A condition identifying delete events, so they're applied as deletes rather than upserts |
| `stored_as_scd_type` | `1` (overwrite in place) or `2` (preserve full history with generated `__START_AT`/`__END_AT` columns) |
| `except_column_list` | Source columns to exclude from the target (e.g., CDC-metadata-only columns like `operation`) |

**Exam trap — what AUTO CDC removes vs. what it doesn't:** it removes the burden of (1) manually sorting/deduping out-of-order events before merging, and (2) hand-writing SCD Type 2's two-statement close-then-insert pattern (Day 10) into one declarative call. It does **not** remove the need to correctly identify `keys` and `sequence_by` — get the sequencing column wrong (e.g., use ingestion time instead of the source system's true change timestamp) and AUTO CDC will apply changes in the wrong logical order just as confidently as a hand-written pipeline would.

---

## Part 5 — Streaming Tables vs. Materialized Views (Preview)

Day 16 is the full trade-off comparison; for now, the mechanical distinction:

| | Streaming Table | Materialized View |
|---|---|---|
| Built from | `dlt.read_stream(...)` — incremental, append-only source | `dlt.read(...)` — full/static query |
| Refresh behavior | Only processes new data since last run | Lakeflow decides full recompute vs. incremental maintenance |
| Typical use | Bronze ingestion, append-heavy silver transforms | Aggregates, joins, anything needing to reflect upstream updates/deletes correctly |

**Exam trap (forward-referenced from Day 10):** a streaming table assumes its source is append-only — if the upstream table receives `UPDATE`/`DELETE`s, a streaming table won't correctly reflect them. This is exactly the gap Day 10's Change Data Feed material addressed at the Structured Streaming level; Day 16 covers the Lakeflow-native version of that same trade-off in depth.

---

## Part 6 — Pipeline Configuration

### Development vs. Production Mode

| Mode | Cluster behavior | Retry behavior |
|---|---|---|
| **Development** | Reuses the same cluster across pipeline updates (faster iteration, no cold-start per run) | **Automatic retries are disabled** — a failure surfaces immediately instead of being silently retried, so you can see and fix the actual error while iterating |
| **Production** | Provisions a fresh cluster per update by default (isolation) | Automatic retries **with exponential backoff** are enabled for resilience |

**This is the exam objective's "auto-optimization to disallow retries" bullet:** it refers to Development mode's deliberate retry-suppression — the point is to surface errors during authoring rather than have them masked by an automatic retry succeeding on the second attempt. A scenario about "why did my broken pipeline update immediately show an error instead of retrying automatically" is describing Development mode, not a bug.

### Continuous vs. Triggered Execution

| Mode | Behavior | Analogous to (Day 14) |
|---|---|---|
| **Triggered** | Processes all currently available data, then stops — run on a schedule | `Trigger.AvailableNow()` |
| **Continuous** | Runs indefinitely, processing new data as it arrives | Default/`ProcessingTime` trigger |

### Channel

`channel: CURRENT` (default, stable) vs. `channel: PREVIEW` (early access to upcoming Lakeflow runtime features) — set at the pipeline level, a single string value, not a list.

### Environments, Dependencies, and Compute Sizing

Pipelines support specifying library dependencies (PyPI packages, wheels) at the pipeline level so every notebook/file in the pipeline's graph has consistent access to them, following the same install-scope principles from Day 2 (a dependency needed by pipeline code must be available to the cluster the pipeline provisions, not just installed ad hoc in one notebook cell). For notebook tasks that need more memory than a pipeline's default node type provides, the fix is the same lever as any other Databricks compute — choose a memory-optimized node type / cluster policy for that workload, not a pipeline-specific "memory flag."

---

## Part 7 — Exam Traps Recap

1. `dlt.read` vs. `dlt.read_stream` inside the function body — not the `@dlt.table` decorator itself — determines streaming table vs. materialized view.
2. `@dlt.expect` warns only; `@dlt.expect_or_drop` drops the row; `@dlt.expect_or_fail` fails the whole update. Know which one a scenario's tolerance for bad data implies.
3. Lakeflow's quarantine mechanism is `@dlt.expect*`; Day 13's classic-jobs quarantine mechanism is `_rescued_data`/`foreachBatch` — match the mechanism to the stated execution context.
4. "Control flow operators" means ordinary **Python** `if`/`for` running at **pipeline-authoring time** to generate the graph — not a runtime per-row branching feature.
5. The classic Python closure-over-loop-variable bug is a real risk when using `for` to generate multiple `@dlt.table` functions — capture the loop variable as a default argument.
6. AUTO CDC's `sequence_by` replaces manual out-of-order-event handling; `stored_as_scd_type=2` replaces the manual two-statement SCD2 pattern from Day 10 — but choosing the wrong `sequence_by` column still produces wrong results.
7. Development mode disables automatic retries on purpose (surface errors while iterating); Production mode enables them with backoff — this is the "auto-optimization to disallow retries" objective bullet.
8. Streaming tables assume an append-only source; if the source gets updates/deletes, a streaming table won't reflect them correctly (Day 16 continues this).

---

## Cross-References
- Day 2: Library/dependency installation scope — the same principles apply to pipeline-level dependencies.
- Day 10: The manual `foreachBatch`+`MERGE` CDC pattern and SCD Type 2's two-statement burden — exactly what AUTO CDC automates.
- Day 13: Auto Loader as a pipeline source, and the classic-jobs `_rescued_data`/`foreachBatch` quarantine pattern this day's expectations replace declaratively.
- Day 14: Trigger types and the imperative Structured Streaming model that Lakeflow wraps declaratively; Continuous/Triggered pipeline modes map directly onto Day 14's trigger concepts.
- Day 16: The full Streaming Tables vs. Materialized Views trade-off comparison.
- Day 21: Querying a pipeline's event log for expectation metrics and update history.
