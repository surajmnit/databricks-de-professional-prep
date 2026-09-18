# Day 15 — Hands-On Lab: Lakeflow Declarative Pipelines, Expectations, Control Flow, and AUTO CDC

## Lab Objectives

1. Build a minimal two-table Lakeflow pipeline using `@dlt.table` and observe the auto-managed DAG/checkpoints.
2. Add expectations at all three violation levels (`@dlt.expect`, `@dlt.expect_or_drop`, `@dlt.expect_or_fail`) and observe the different outcomes.
3. Demonstrate `dlt.read` vs `dlt.read_stream` inside the function body — the mechanical distinction between materialized view and streaming table.
4. Use a Python `for` loop to generate multiple `@dlt.table` functions, demonstrating the closure-capture default-argument pattern.
5. Implement an AUTO CDC flow with `stored_as_scd_type=2`.
6. Observe Development vs. Production mode behavior (retries suppressed vs. enabled).
7. Break it on purpose: trigger `@dlt.expect_or_fail` and confirm the pipeline update halts.

**Environment note:** Lakeflow Declarative Pipelines require a **Databricks workspace with pipeline compute** — this lab **cannot** be run on Databricks Community Edition or in a plain Spark notebook without a live pipeline cluster. All code in this lab is syntactically correct and runnable once you have a workspace with pipeline compute enabled. The concepts are what matter here — if you can't run it live, read through each step and confirm you can predict the expected output before and after each change.

---

## Step 1 — Minimal Two-Table Pipeline: Observe the Auto-DAG

```python
import dlt
from pyspark.sql.functions import col

@dlt.table(comment="Bronze landing zone for orders")
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/landing/orders/_schema")
        .load("/Volumes/main/landing/orders/"))

@dlt.table(comment="Silver orders — amount must be positive")
def silver_orders():
    return (dlt.read_stream("bronze_orders")
        .filter(col("amount") > 0))
```

**What you observe in the Pipeline UI:** Lakeflow automatically infers that `silver_orders` depends on `bronze_orders` — you never wrote a `.option("checkpointLocation", ...)` or a `.trigger(...)` yourself. Both table functions run on the same pipeline cluster, and Lakeflow manages their update ordering.

**Predict:** if you introduce a row with `amount = NULL` into the source, does it land in `silver_orders`? **Answer:** No — `filter(col("amount") > 0)` evaluates `NULL > 0` as `UNKNOWN` (not TRUE), so NULL rows are silently dropped by the filter. This is ordinary Spark filter semantics, not a data-quality constraint.

---

## Step 2 — Expectations at All Three Violation Levels

Replace `silver_orders` with an expectations-based version:

```python
@dlt.table
@dlt.expect("positive_amount", "amount > 0")                      # soft warning
@dlt.expect_or_drop("non_null_customer", "customer_id IS NOT NULL")  # drop the row
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

**Now feed a row with `amount = -50`** and observe the pipeline event log (Day 21 covers `event_log()` queries):

| Scenario | What happens |
|---|---|
| Row with `amount = -50` | `positive_amount` violation recorded; row kept (soft warning) |
| Row with `customer_id = NULL` | Row dropped; `non_null_customer` violation recorded |
| `@dlt.expect_or_fail` violation | Pipeline update halts immediately — failure surfaces in the UI |

### 2b. Add a fail-level constraint

```python
@dlt.table
@dlt.expect("order_id_present", "order_id IS NOT NULL")   # warning only
@dlt.expect_or_drop("valid_amount", "amount > 0")          # drop
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

Now introduce a row with `order_id = NULL` and `amount = 100`. The pipeline continues — it's a warning only. Change to `@dlt.expect_or_fail` and the pipeline stops the moment that row is encountered.

---

## Step 3 — `dlt.read` vs. `dlt.read_stream`: The Mechanical Distinction

```python
# Materialized View — reads a full static snapshot each refresh
@dlt.table
def gold_user_counts_mv():
    return (dlt.read("silver_orders")
        .groupBy("region")
        .count())

# Streaming Table — incremental append processing
@dlt.table
def silver_orders_live():
    return (dlt.read_stream("bronze_orders")
        .filter("status = 'completed'"))
```

**Predict and verify:** in the Pipeline UI, which table shows a refresh timestamp that moves forward incrementally? Which shows a full recompute? **Answer:** `silver_orders_live` (streaming table) processes only new rows; `gold_user_counts_mv` (materialized view) is read as a full snapshot each refresh.

**Exam trap to confirm:** does changing the `@dlt.table` decorator on `gold_user_counts_mv` to `@dlt.view` change how Lakeflow reads `silver_orders`? **Answer:** No — `@dlt.view` makes the result a view (not a materialized table), but the read call inside the function body still determines whether it's full (`dlt.read`) or incremental (`dlt.read_stream`).

---

## Step 4 — Python `for` Loop to Generate Multiple Tables

**Correct pattern (default-argument capture):**

```python
import dlt

source_tables = ["customers", "products", "orders"]

for table_name in source_tables:
    @dlt.table(name=f"bronze_{table_name}")
    def _make_bronze(table_name=table_name):  # default-arg captures current value
        return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"/Volumes/main/landing/{table_name}/"))
```

**What to observe:** in the Pipeline UI, you should see three separate bronze tables (`bronze_customers`, `bronze_products`, `bronze_orders`), each pointing to a distinct source path. No duplication of code.

### 4b. Break it on purpose: The closure bug

Replace the function with this broken version:

```python
for table_name in source_tables:
    @dlt.table(name=f"bronze_{table_name}")
    def _make_bronze():  # MISSING: table_name=table_name default argument
        return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"/Volumes/main/landing/{table_name}/"))
```

**Predict:** after the loop finishes, `table_name` holds `"orders"`. When Lakeflow calls all three `_make_bronze` functions, they each close over the **same** `table_name` variable — now set to `"orders"`. **All three tables will read from `/Volumes/main/landing/orders/`** — this is the classic Python late-binding closure bug, not a Lakeflow bug.

---

## Step 5 — AUTO CDC Flow with SCD Type 2

```python
import dlt

# First, create the target streaming table
dlt.create_streaming_table("customers_scd2")

# Wire in the CDC source with AUTO CDC
dlt.create_auto_cdc_flow(
    target="customers_scd2",
    source="customers_cdc_bronze",      # upstream CDC table with operation + change_timestamp cols
    keys=["customer_id"],
    sequence_by="change_timestamp",      # determines correct logical ordering
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "change_timestamp"],
    stored_as_scd_type=2,               # generates __START_AT / __END_AT columns
)
```

**What to observe in the target table after processing:**
- `__START_AT`: the timestamp from which this version of the row became current
- `__END_AT`: NULL for the current version; a timestamp for historical versions (SCD2 closure)
- `operation = 'DELETE'` rows applied as actual deletes in the target
- `operation`-type columns excluded from the target via `except_column_list`

**Predict:** what happens if `change_timestamp` is ingestion time rather than the source system's true change time? **Answer:** changes applied out of logical order, even though AUTO CDC handled the SCD2 mechanics correctly — the wrong sequencing column is a data-correctness problem, not an AUTO CDC configuration failure.

---

## Step 6 — Development vs. Production Mode: Retry Behavior

```python
# Development mode settings
pipeline_config = {
    "development": True,
    "channel": "CURRENT",
}

# Production mode settings
pipeline_config = {
    "development": False,   # defaults to False (Production)
    "channel": "CURRENT",
    "autopilot": {
        "pipelineAutoCompactionEnabled": True,
    }
}
```

**What to observe:**
- In Development mode, the pipeline uses a **reused cluster** across updates (faster iteration) and **disables automatic retries** — a failure surfaces immediately, helping you see and fix the actual error.
- In Production mode, the pipeline provisions a **fresh cluster** per update and **enables automatic retries with exponential backoff** for resilience.

**Break it on purpose:** while in Development mode, introduce a row that violates an `@dlt.expect_or_fail` constraint. Confirm the pipeline update fails immediately rather than retrying. Now switch to Production mode and introduce the same violation — observe whether the pipeline retries before ultimately failing (it should, with backoff).

---

## Step 7 — Break It on Purpose: `expect_or_fail` Halts the Update

```python
@dlt.table
@dlt.expect_or_fail("order_total_positive", "total > 0")
def silver_orders_strict():
    return dlt.read_stream("bronze_orders")
```

Introduce a row: `{"order_id": 999, "customer_id": "c1", "total": -999.0, "status": "pending"}`

**Expected behavior:** the pipeline update enters a `FAILED` state immediately. In the Pipeline UI, the violation count for `order_total_positive` increments, and the update halts before any downstream tables that depend on `silver_orders_strict` run.

**What to check afterward:** query the pipeline event log for the failure event:
```python
spark.sql("""
    SELECT * FROM event_log('pipeline_id_here')
    WHERE event_type = 'flow_progress'
""").display()
```

This is the scenario behind "develop a quarantining process" — the pipeline halts rather than silently propagating bad data downstream.

---

## Stretch Task — Conditional Table Inclusion with Python `if`

```python
import dlt

# Read a pipeline-level configuration at graph-construction time
include_debug = spark.conf.get("pipelines.includeDebugTable", "false") == "true"

if include_debug:
    @dlt.table
    def debug_audit():
        return dlt.read_stream("silver_orders").selectExpr(
            "current_timestamp() as audit_ts", "*"
        )
```

Deploy the pipeline twice — once with `pipelines.includeDebugTable = false` (no `debug_audit` table in the graph) and once with `true` (the table appears in the graph). The pipeline graph itself changes based on the configuration, not the data flowing through it.

---

## Lab Checklist

- [ ] Built a two-table Lakeflow pipeline (`bronze_orders` → `silver_orders`) and confirmed Lakeflow manages the DAG and checkpoints automatically
- [ ] Confirmed ordinary `filter(col("amount") > 0)` silently drops NULLs (not a data-quality constraint)
- [ ] Applied all three expectation levels and observed soft-warning vs. drop vs. fail behaviors
- [ ] Confirmed `dlt.read` produces a full-snapshot refresh (materialized view); `dlt.read_stream` produces incremental append (streaming table)
- [ ] Generated three bronze tables via a Python `for` loop with correct default-argument closure capture
- [ ] Broke Step 4's pattern intentionally — confirmed all three tables read from the same path due to late-binding closure
- [ ] Implemented AUTO CDC with `stored_as_scd_type=2` and confirmed `__START_AT`/`__END_AT` column generation
- [ ] Observed Development mode's retry suppression vs. Production mode's backoff retries
- [ ] Triggered `@dlt.expect_or_fail` and confirmed the pipeline update halted immediately

---

## Cross-References
- Day 10: Manual `foreachBatch`+`MERGE` CDC/SCD2 pattern — AUTO CDC replaces this.
- Day 13: Auto Loader as the source; classic-jobs quarantine pattern (`foreachBatch` + `_rescued_data`) vs. Lakeflow's `@dlt.expect*`.
- Day 14: Structured Streaming execution model that Lakeflow wraps — triggers, checkpoints, output modes.
- Day 16: Full streaming table vs. materialized view trade-off.
- Day 21: Querying a pipeline's event log for expectation metrics.
