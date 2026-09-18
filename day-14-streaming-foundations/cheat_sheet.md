# Day 14 — Cheat Sheet: Streaming Foundations — Structured Streaming, Triggers, Watermarking, and State

## Micro-Batch Execution Model

Structured Streaming = same Spark/Catalyst engine, run repeatedly:
```
Trigger fires → checks source for new offsets → processes as ordinary Spark batch
→ writes to sink → commits offset + state to checkpoint → waits for next trigger
```
**Inside each micro-batch:** Jobs/Stages/Tasks from Day 4/5 apply unchanged — Spark UI diagnostics work identically.

---

## Trigger Types

| Trigger | Behavior | Use when |
|---|---|---|
| Default (no `.trigger()`) | Next batch runs immediately after previous finishes — effectively continuous | Lowest latency, simplest |
| `Trigger.ProcessingTime("X seconds")` | Fixed-interval batches; waits if early, starts immediately if late | Predictable throttled cadence |
| `Trigger.Once()` (legacy, deprecated) | **One giant batch** — processes all backlog, then stops | **Avoid** — large backlog risks OOM |
| `Trigger.AvailableNow()` | All currently available data across **multiple bounded micro-batches**, then stops | **Modern replacement** for Once — backfill on a schedule |
| Continuous processing (experimental) | True ~1ms continuous execution, no micro-batch | Only narrow map-like ops; rarely right answer |

**Official exam sample (retired, still valid):** Peak hours cause 30s processing with 10s trigger. Requirement: <10s latency. **Answer:** decrease trigger interval to 5s — tighter interval improves executor utilization during peak backlog.

---

## Output Modes

| Mode | What it writes | Valid for |
|---|---|---|
| **Append** | Only new rows since last trigger | Stateless transforms; aggregations **only with watermark** |
| **Update** | New or changed rows since last trigger | Streaming aggregations — emits updated running totals |
| **Complete** | Entire result table rewritten each trigger | Aggregations with small/bounded cardinality only |

**Exam trap:** `outputMode("append")` on `groupBy().count()` **without watermark** → `AnalysisException` at query start. Spark needs the watermark to know when a group's count is "final" and safe to emit once.

---

## Watermarking

```python
from pyspark.sql.functions import window

counts = (df
    .withWatermark("event_time", "10 minutes")  # BEFORE the aggregation
    .groupBy(window("event_time", "5 minutes"), "device_id")
    .count())
```

- Watermark column **must be the same column** used in the window function.
- Spark advances watermark to `max(event_time seen) - threshold` after each batch.
- Data older than current watermark = excluded from further aggregation.
- **Bound of state** — without it, state grows unbounded forever.

**Exam trap:** watermarking is **event-time only**; does nothing for processing-time logic. Must be declared **before** the aggregation, not after.

**Stream-stream joins — both sides need watermarks AND a time range condition:**
```python
joined = (stream_a.withWatermark("a_time", "10 minutes")
    .join(
        stream_b.withWatermark("b_time", "10 minutes"),
        expr("a_key = b_key AND a_time BETWEEN b_time - INTERVAL 5 MINUTES AND b_time + INTERVAL 5 MINUTES")
    ))
```

---

## State Store

**What holds state:** running aggregates, dedup sets, join buffers, `mapGroupsWithState` logic.

| Problem | Fix |
|---|---|
| State growing unbounded → executor OOM | **Add or tighten watermark** to bound how long state is retained |
| State too large for JVM heap | **Switch to RocksDB state store** (`spark.sql.streaming.stateStore.providerClass`) |

**Custom stateful logic:** `mapGroupsWithState` / `applyInPandasWithState` with explicit timeout policy (`GroupStateTimeout.ProcessingTimeTimeout()` or `EventTimeTimeout()`).

---

## Exactly-Once Semantics

Requires **both** halves cooperating:

| Half | Requirement |
|---|---|
| Source | **Replayable** — Spark can re-request the same offset range after failure (Auto Loader, Kafka, Delta, files) |
| Sink | **Idempotent** — writing same micro-batch twice must not duplicate data (Delta gets this free via transaction log) |

**Exam trap:** duplicates after restart = non-idempotent sink, not a Structured Streaming bug.

---

## Structured Streaming vs. Lakeflow Declarative Pipelines

| | Structured Streaming | Lakeflow Declarative Pipelines |
|---|---|---|
| Model | Imperative — you manage triggers/checkpoints | Declarative — describe tables, Lakeflow manages execution |
| Data quality | Manual (filter, foreachBatch quarantine) | Built-in `@dlt.expect*` constraints |
| Multi-hop | Wire notebooks/jobs yourself | Native bronze → silver → gold in one pipeline |
| Best fit | Custom stateful logic, non-standard sinks, fine-grained control | Standard ETL with data-quality rules |

---

## Exam Trap Shortlist

1. Micro-batch = ordinary Spark execution — Day 4/8 Spark UI diagnostics apply unchanged.
2. `Trigger.Once()` deprecated — `AvailableNow()` is the current safe choice for batch-style runs.
3. Official retired sample: **decrease trigger interval** for better executor utilization under peak backlog.
4. `outputMode("append")` on aggregation needs watermark — query fails without it, not silently wrong.
5. Watermark declared **before** aggregation, on the **same event-time column** used in window.
6. Stream-stream join: watermarks on **both sides** + time-range condition = bounded state. Either alone insufficient.
7. Unbounded state = add/tighten watermark OR switch to RocksDB — not "add executor memory."
8. Exactly-once = replayable source + idempotent sink + checkpoint — not automatic regardless of sink.
