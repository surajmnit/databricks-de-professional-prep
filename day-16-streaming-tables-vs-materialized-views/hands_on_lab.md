# Day 16 — Hands-On Lab: Streaming Tables vs. Materialized Views — Trade-Offs and Use Cases

## Lab Objectives

1. Create a streaming table from an append-only source and confirm it processes only new rows incrementally.
2. Create a materialized view with an aggregation and confirm it recomputes correctly when source rows change.
3. Observe the streaming table's append-only limitation by introducing an `UPDATE` to the source and confirming the streaming table doesn't reflect it.
4. Observe the materialized view correctly reflecting the same `UPDATE`.
5. Confirm AUTO CDC targets a streaming table, not a materialized view.
6. Observe that expectations work identically on both table types.
7. Query the pipeline event log to see refresh type differentiation.

**Environment note:** Lakeflow Declarative Pipelines require pipeline compute — this lab **cannot** run on Community Edition or a plain notebook without a live pipeline cluster. All code is syntactically correct and runnable with pipeline compute. If you cannot run live, read through each step and confirm you can predict the expected state of each table before and after each action.

---

## Step 1 — Create a Streaming Table from an Append-Only Source

```python
import dlt

@dlt.table(comment="Bronze orders — append-only landing")
def bronze_orders():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/landing/orders/_schema")
        .load("/Volumes/main/landing/orders/"))
```

**SQL equivalent (for pipelines using SQL pipeline definition):**
```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT * FROM STREAM read_files('/Volumes/main/landing/orders/', format => 'json');
```

**What to observe in the Pipeline UI:** the streaming table updates continuously (or on the configured trigger cadence) by processing only the new rows that arrived since the last update — the full source table is never re-read.

**Feed initial data:**
```python
# Three orders arrive
initial = [
    {"order_id": 1, "customer": "alice", "amount": 100.0, "status": "pending"},
    {"order_id": 2, "customer": "bob", "amount": 200.0, "status": "pending"},
    {"order_id": 3, "customer": "carol", "amount": 300.0, "status": "pending"},
]
# (write to /Volumes/main/landing/orders/ via Auto Loader)
```

Confirm all three rows land in `bronze_orders`. Confirm the **checkpoint** — not the full source — tracks progress.

---

## Step 2 — Create a Materialized View with an Aggregation

```python
import dlt

@dlt.table
def daily_revenue_mv():
    return (dlt.read("bronze_orders")
        .groupBy("status")
        .agg(
            sparkf.count("*").alias("order_count"),
            sparkf.sum("amount").alias("total_amount")
        ))
```

**SQL equivalent:**
```sql
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue_mv
AS SELECT status,
       COUNT(*) AS order_count,
       SUM(amount) AS total_amount
FROM LIVE.bronze_orders
GROUP BY status;
```

**What to observe in the Pipeline UI:** the materialized view shows a **refresh timestamp**. Unlike the streaming table, it re-evaluates the query against the full source snapshot on each refresh — even though Lakeflow may incrementalize the recompute when the query pattern allows.

**Predict before running:** after the initial data, `daily_revenue_mv` should show `status=pending` with `order_count=3` and `total_amount=600.0`. Confirm this before proceeding.

---

## Step 3 — Observe the Streaming Table's Append-Only Limitation

**This is the core demonstration for the exam.**

Now apply an `UPDATE` directly to `bronze_orders` (simulating an upstream mutation):
```sql
UPDATE bronze_orders SET amount = 250.0 WHERE order_id = 1;
UPDATE bronze_orders SET status = 'completed' WHERE order_id = 1;
```

**Refresh the pipeline** (or wait for the next trigger if running continuously).

**What to observe:**
- The streaming table `bronze_orders` — did it reflect the `UPDATE`? **No.** A streaming table only sees rows that arrived *after* the last processed offset. Rows already committed before the `UPDATE` are invisible to it — the stream has moved on.
- A `SELECT * FROM bronze_orders WHERE order_id = 1` still shows `amount = 100.0` and `status = 'pending'`.

**Break it on purpose:** try to "fix" the streaming table by re-filtering — adding `WITH (UPDTECT)` or a special watermark. **Predict:** no streaming table mechanism corrects for already-processed mutations from the source. The streaming table is correct *given its model*, but that model only holds if the source is genuinely append-only. The fix is to use a materialized view or CDF + merge.

---

## Step 4 — Confirm the Materialized View Correctly Reflects the Update

```sql
SELECT * FROM daily_revenue_mv;
```

**What to observe:** `daily_revenue_mv` now shows the updated `amount = 250.0` for `order_id = 1` reflected in `total_amount = 750.0` (correctly sum of 250 + 200 + 300 = 750). The materialized view correctly reflects the current state of `bronze_orders` because it re-evaluates the query against the full source on each refresh.

**Compare:**

| | Streaming Table | Materialized View |
|---|---|---|
| After initial load | Shows `amount = 100.0` for order 1 | Shows `amount = 100.0` for order 1 |
| After `UPDATE order_id=1 SET amount=250` | **Still shows 100.0** (missed the mutation) | **Shows 250.0** (reflects mutation correctly) |

This is the concrete, demonstrable difference the exam wants you to reason through.

---

## Step 5 — Confirm AUTO CDC Targets a Streaming Table

```python
import dlt

# AUTO CDC creates a streaming table as its target — not a materialized view
dlt.create_streaming_table("customers_cdc_silver")

dlt.create_auto_cdc_flow(
    target="customers_cdc_silver",
    source="customers_cdc_bronze",
    keys=["customer_id"],
    sequence_by="change_timestamp",
    apply_as_deletes="operation = 'DELETE'",
    stored_as_scd_type=2,
)
```

**What to observe:** the pipeline UI shows `customers_cdc_silver` as a **streaming table** type, not a materialized view. AUTO CDC applies an incremental stream of CDC change events — exactly the append-only model a streaming table handles natively. Applying CDC events as a materialized view would not work (views expect to re-evaluate against a full snapshot, not receive row-level mutations).

**Predict:** if you tried to use `dlt.create_auto_cdc_flow` targeting a table created via `dlt.create_materialized_view()`, would it work? **Answer:** No — the API only accepts a streaming table as the target.

---

## Step 6 — Expectations Work Identically on Both Table Types

```python
import dlt
from pyspark.sql.functions import col

# Expectation on a streaming table
@dlt.table
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_orders_stream():
    return dlt.read_stream("bronze_orders")

# Same expectation on a materialized view
@dlt.table
@dlt.expect_or_drop("positive_amount", "amount > 0")
def silver_orders_mv():
    return dlt.read("bronze_orders")
```

**What to observe:** introduce a row with `amount = -50` into the source. Both tables drop the row from their output — data-quality enforcement is orthogonal to which refresh model the table uses. Expectation metrics for both appear in the pipeline event log (Day 21) and Pipeline UI Data Quality tab.

---

## Step 7 — Query the Pipeline Event Log to See Refresh Differentiation

```python
# Query event log to see actual refresh type for each table
spark.sql("""
    SELECT
        flows.flow_name,
        flows.status,
        flows.update_id,
        flows.processed_records
    FROM event_log('<pipeline_id>') AS events,
         LATERAL VIEW inline(events.event_log) AS flows
    WHERE events.event_type = 'flow_progress'
    ORDER BY events.timestamp DESC
""").display()
```

**What to observe:**
- Streaming table `bronze_orders` shows many small `processed_records` counts (new rows per micro-batch)
- Materialized view `daily_revenue_mv` shows fewer, larger `processed_records` counts (full/diffed recompute per refresh)

This is the operational telemetry that confirms which model is active for each table.

---

## Stretch Task — The CDF + Incremental Merge Bridge

Enable CDF on `bronze_orders`, then consume its changes via a streaming table + `MERGE`:

```python
# Step 1: Enable CDF on the source
spark.sql("""
    ALTER TABLE bronze_orders
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Step 2: Create a streaming table that reads CDF changes
dlt.create_streaming_table("bronze_orders_cdf")

# Step 3: Use a streaming table + MERGE to apply changes incrementally
# (requires a downstream target table where the MERGE lives)
# This pattern gets materialized-view-level correctness
# at streaming-table-level latency — the CDF bridge from Day 10
```

**What to observe:** the downstream target now reflects `UPDATE`s to `bronze_orders` correctly, despite being built on an incremental stream — CDF's row-level change events carry the before/after state that the `MERGE` uses to apply the correct target state.

---

## Lab Checklist

- [ ] Created a streaming table from an append-only source and confirmed incremental-only processing
- [ ] Created a materialized view with an aggregation and confirmed it recomputes correctly
- [ ] Applied an `UPDATE` directly to the source and confirmed the streaming table **did not** reflect it (append-only limitation)
- [ ] Confirmed the materialized view **correctly reflected** the same `UPDATE`
- [ ] Confirmed AUTO CDC targets a streaming table, not a materialized view
- [ ] Confirmed expectations work identically on both table types
- [ ] Queried the event log to see streaming vs. materialized view refresh differentiation
- [ ] (Stretch) Implemented the CDF + incremental merge bridge for correctness + low latency

---

## Cross-References
- Day 9: Delta transaction log `add`/`remove` actions — the mechanism CDF's change events are built from.
- Day 10: Change Data Feed mechanics — the exact "materialized-view correctness at streaming-table latency" pattern.
- Day 14: Structured Streaming's output mode trade-offs — the imperative analog of this day's comparison.
- Day 15: `dlt.read` vs `dlt.read_stream`, AUTO CDC, expectations — the building blocks this day sits on top of.
- Day 21: Querying the pipeline event log for refresh metrics and expectation counts.
