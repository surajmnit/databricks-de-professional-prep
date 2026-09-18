# Day 15 — Cheat Sheet: Lakeflow Declarative Pipelines, Expectations, Control Flow, and AUTO CDC

## Core API: `@dlt.table` and the Two Read Calls

```python
import dlt

@dlt.table(comment="...")
def target_table():
    return dlt.read_stream("upstream")   # Streaming Table — incremental append
    # or
    return dlt.read("upstream")          # Materialized View — full snapshot each refresh
```

**Key distinction:** `dlt.read` vs `dlt.read_stream` inside the function body determines the table type — not the `@dlt.table` decorator itself.

---

## Expectations: Three Violation Levels

| Decorator | On violation | Use case |
|---|---|---|
| `@dlt.expect("name", "condition")` | Row **kept**, violation logged | Monitor but don't block |
| `@dlt.expect_or_drop("name", "condition")` | Row **dropped** from output, violation logged | Quarantine bad rows |
| `@dlt.expect_or_fail("name", "condition")` | Pipeline update **halts immediately** | Hard data-quality gates |

Multiple constraints: `@dlt.expect_all_or_drop({"name1": "cond1", "name2": "cond2"})`

**Exam trap:** Lakeflow quarantine = `@dlt.expect*`; Classic-jobs quarantine (Day 13) = `_rescued_data`/`foreachBatch`. Match mechanism to stated execution context.

---

## AUTO CDC (formerly APPLY CHANGES)

```python
dlt.create_streaming_table("target")

dlt.create_auto_cdc_flow(
    target="target",
    source="cdc_source",
    keys=["id_col"],                    # merge key
    sequence_by="change_timestamp",     # establishes correct event ordering
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "change_timestamp"],
    stored_as_scd_type=2,               # 1=overwrite, 2=full history (__START_AT/__END_AT)
)
```

| Parameter | Purpose |
|---|---|
| `keys` | Unique row identifier — the merge key |
| `sequence_by` | Column for correct event ordering — **replaces manual out-of-order handling** |
| `apply_as_deletes` | Condition identifying a delete event |
| `stored_as_scd_type` | 1 = Type 1 (overwrite); 2 = Type 2 (history with `__START_AT`/`__END_AT`) |
| `except_column_list` | Source columns to exclude from target (e.g., CDC metadata columns) |

---

## Control Flow: Python at Pipeline-Authoring Time

**Correct `for` loop — default-argument capture:**
```python
for name in ["customers", "products"]:
    @dlt.table(name=f"bronze_{name}")
    def _make(table_name=name):   # captures current value, avoids late-binding bug
        return spark.readStream...
```

**Incorrect — late-binding closure bug:** all generated functions close over the same `table_name` loop variable, which holds its **final** value after the loop ends.

**Conditional table inclusion via `if`:**
```python
include_debug = spark.conf.get("pipelines.includeDebugTable", "false") == "true"
if include_debug:
    @dlt.table def debug_audit(): ...
```
Evaluated once at graph-construction time — determines **which tables exist**, not per-row routing.

---

## Development vs. Production Mode

| | Development | Production |
|---|---|---|
| Cluster | Reused across updates (fast iteration) | Fresh cluster per update (isolation) |
| Automatic retries | **Disabled** — failures surface immediately | Enabled with exponential backoff |
| Purpose | Catch errors during authoring | Resilient production operation |

---

## Triggered vs. Continuous Pipeline Mode

| Pipeline mode | Analogous to (Day 14) | Behavior |
|---|---|---|
| **Triggered** | `Trigger.AvailableNow()` | Process all available data, then stop |
| **Continuous** | Default/`ProcessingTime` trigger | Runs indefinitely, processes new data as it arrives |

---

## Exam Trap Shortlist

1. `dlt.read` vs `dlt.read_stream` inside the function body — NOT the decorator — determines streaming table vs. materialized view.
2. `@dlt.expect` = warn only; `@dlt.expect_or_drop` = drop row; `@dlt.expect_or_fail` = halt pipeline update.
3. Lakeflow quarantine = `@dlt.expect*`; Classic jobs = `_rescued_data`/`foreachBatch` (Day 13).
4. "Control flow operators" = ordinary Python `if`/`for` at **pipeline-authoring time** — not runtime per-row branching.
5. Python `for`-loop closure bug: capture loop variable as default argument, not late-binding closure.
6. AUTO CDC's `sequence_by` replaces manual out-of-order-event sorting — wrong column still produces wrong results.
7. `stored_as_scd_type=2` generates `__START_AT`/`__END_AT` — replaces manual two-statement SCD2 pattern from Day 10.
8. Development mode suppresses retries **deliberately** — "why did my broken pipeline fail immediately" is not a bug, it's the feature.
9. Streaming tables assume append-only source; if source gets updates/deletes, streaming table won't reflect them (Day 16 continues).
