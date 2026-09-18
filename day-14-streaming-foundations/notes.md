# Day 14 — Streaming Foundations: Structured Streaming, Triggers, Watermarking, and State

## Exam Objectives (Exam Guide, July 2026)

This day builds the foundational Structured Streaming knowledge that **Section 1** and **Section 2** both assume, and that Day 15/16 build directly on top of:

**Section 1: Developing Code for Data Processing using Python and SQL**
- "Compare Spark Structured Streaming and Lakeflow Spark Declarative Pipelines to determine the optimal approach for building scalable ETL pipelines." *(This day covers the Structured Streaming side of that comparison in depth; Day 15/16 cover the Lakeflow side and complete the comparison.)*
- "Build and manage reliable, production-ready data pipelines for batch and streaming data using Lakeflow Spark Declarative Pipelines and Autoloader." *(Auto Loader itself was Day 13; this day is the streaming execution model underneath any streaming source, Auto Loader included.)*

**Section 2: Data Ingestion & Acquisition**
- "Create an append-only data pipeline capable of handling both batch and streaming data using Delta." *(Day 12 covered the Delta transaction-log mechanics that make this safe; this day covers the streaming-side trigger/output-mode choices that determine correctness.)*

*(Structured Streaming here means the raw `readStream`/`writeStream` DataFrame API — not Lakeflow Declarative Pipelines' declarative wrapper around it. Don't duplicate Day 15/16's `@dlt`/streaming-table material here; this day is the execution model those higher-level tools sit on top of.)*

---

## Part 1 — The Micro-Batch Execution Model

### How Structured Streaming Actually Executes

Structured Streaming is not a fundamentally different engine from batch Spark — it's the **same Catalyst/Spark SQL engine**, re-run repeatedly on new data:

```
Trigger fires
  -> Spark checks the source for new data since the last committed offset
  -> Processes that increment as one ordinary Spark batch (Jobs/Stages/Tasks — Day 4/5)
  -> Writes results to the sink
  -> Commits the new offset + any state to the checkpoint
  -> Waits for the next trigger
```

**Exam-relevant consequence:** everything from Day 4–8 (Jobs/Stages/Tasks, shuffle, memory, Spark UI) applies unchanged *inside* each micro-batch. A slow streaming job is diagnosed exactly like a slow batch job — check the Spark UI for that micro-batch's stage.

### The Core API

```python
# Source: any streaming-capable format (Auto Loader, Kafka, Delta, rate, socket)
streaming_df = (spark.readStream
    .format("cloudFiles")  # or "kafka", "delta", "rate", etc.
    .option("cloudFiles.format", "json")
    .load("/path/to/source"))

# Transformations: identical DataFrame API to batch — filter, select, join, groupBy all work
transformed = streaming_df.filter("status = 'active'")

# Sink: write the incremental result somewhere
query = (transformed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/chk/my_stream")
    .trigger(processingTime="30 seconds")
    .toTable("bronze.events"))
```

**Checkpointing is what makes this safe to restart.** The checkpoint directory records: (1) which offsets have been processed from the source, (2) the state store's versioned contents for any stateful operator, and (3) metadata about the query's operators. Deleting or corrupting the checkpoint forces a full reprocess from the source's earliest available offset — a common real-world incident.

---

## Part 2 — Trigger Types

| Trigger | Behavior | Use When |
|---|---|---|
| **Default** (no `.trigger()` call) | Runs the next micro-batch immediately after the previous one finishes — effectively continuous, but still micro-batch under the hood | Lowest-latency micro-batch processing; simplest to reason about |
| **`Trigger.ProcessingTime("X seconds")`** | Fixed-interval micro-batches — if processing finishes early, waits out the rest of the interval; if it runs long, the next batch starts immediately after (no overlap) | Predictable, throttled cadence; smoothing out bursty sources |
| **`Trigger.Once()`** (legacy, deprecated) | Processes **all currently available data in a single micro-batch**, then stops | Superseded — a large backlog in one batch risks OOM and long recovery on failure |
| **`Trigger.AvailableNow()`** (current, Day 12) | Processes all currently available data across **as many micro-batches as needed** (respecting `maxFilesPerTrigger`/`maxBytesPerTrigger` — Day 13), then stops | The modern replacement for `Trigger.Once()` — same "process backlog then stop" goal, without the single-giant-batch risk |
| **Continuous processing** (experimental) | True low-latency (~1ms) continuous execution, not micro-batch at all | Only supports a narrow set of map-like operations (no aggregations); rarely the right exam answer given its limitations |

**Exam trap — `Trigger.Once()` vs. `Trigger.AvailableNow()`:** both express "run like a batch job, then stop," but `Once()` forces the entire backlog through **one** micro-batch — on a large backlog this can spill, OOM, or make a single failed batch expensive to retry. `AvailableNow()` is the documented, current-recommended replacement specifically because it chunks the same backlog into multiple bounded micro-batches. A scenario naming either explicitly is testing whether you know `AvailableNow()` is the safer, current choice.

### The Official Retired Sample Question (Exam Guide) — Worked Through

> A Structured Streaming job's microbatch normally processes in under 3 seconds with a 10-second trigger interval. During peak hours, microbatch processing time sometimes exceeds 30 seconds, causing a backlog. Records must process in under 10 seconds. Holding all other variables constant, which adjustment meets the requirement?

The exam guide's official answer is: **decrease the trigger interval to 5 seconds** — reasoning that more frequent triggering allows idle executors to begin pulling in the next batch's available data sooner rather than sitting idle while a straggler task from the current batch finishes, improving overall executor utilization during the exact peak-hour window where processing time balloons.

**Exam trap:** it's tempting to argue "if processing already takes 30 seconds, a shorter interval can't possibly help — the bottleneck is processing time, not trigger frequency." That reasoning is intuitive but is **not** the tested answer — the exam guide's own answer key credits the tighter interval with better executor utilization under peak load, and explicitly rejects `Trigger.Once()`-on-a-schedule (treats backlog reprocessing as a workaround, not a fix) and "just increase shuffle partitions" (addresses parallelism, not the trigger-driven backlog itself) as the wrong moves here. Know this exact question — it is retired official exam content, not a third-party guess.

---

## Part 3 — Output Modes

| Mode | Behavior | Supported Operations |
|---|---|---|
| **Append** | Only *new* rows since the last trigger are written to the sink; existing output rows are never changed | Default for stateless transforms (filter, select, map); for aggregations, **requires a watermark** so Spark knows when a group is "final" and safe to emit once |
| **Update** | Only rows that are new or **changed** since the last trigger are written | The typical mode for streaming aggregations — emits updated running totals as they change, without replaying the whole result each time |
| **Complete** | The **entire** result table is rewritten to the sink on every trigger | Only feasible when the aggregated result is small and bounded (e.g., a handful of groups) — state and output size grow with cardinality, not data volume |

**Exam trap:** `outputMode("append")` on a plain `groupBy().count()` **with no watermark** raises an `AnalysisException` at query start — Spark refuses to run it, because without a watermark it can never be sure a group's count is "done" and safe to append once. Add `withWatermark(...)` on the event-time column before the aggregation to make append mode valid, or use `update`/`complete` mode if a watermark genuinely doesn't fit the use case.

---

## Part 4 — Watermarking and Late Data

### What a Watermark Does

A watermark tells Spark: *"I don't expect to see event-time data more than N behind the latest event time I've observed — anything later than that, drop or exclude from further aggregation."* This bounds how long Spark must keep state around for a given window.

```python
from pyspark.sql.functions import window

windowed_counts = (streaming_df
    .withWatermark("event_time", "10 minutes")   # define the watermark BEFORE the aggregation
    .groupBy(window("event_time", "5 minutes"), "device_id")
    .count())
```

- The watermark column (`event_time` here) must be the **same event-time column** used in the subsequent windowed aggregation.
- Spark's internal watermark advances to `max(event_time seen so far) - threshold` after each micro-batch.
- Data arriving with an event time **older than the current watermark** is treated as too late — it's excluded from further updates to that window's aggregate (and, depending on mode, may simply be dropped from the result).

### Why It Matters — Bounding State

Without a watermark, a stateful aggregation (or a stream-stream join) must **keep state for every group/key forever**, since Spark has no signal for when it's safe to discard old state. This is an unbounded-state memory leak waiting to happen on a long-running production stream.

**Exam trap:** watermarking is an **event-time** mechanism — it does nothing for processing-time-based logic, and it must be declared on the DataFrame **before** the `groupBy`/window operation that uses it, not after. Declaring it after the aggregation, or on a different column than the one used in the window function, silently fails to bound state.

### Stream-Stream Joins Need Watermarks on Both Sides

```python
joined = (stream_a.withWatermark("a_time", "10 minutes")
    .join(
        stream_b.withWatermark("b_time", "10 minutes"),
        expr("""
            a_key = b_key AND
            a_time BETWEEN b_time - INTERVAL 5 MINUTES AND b_time + INTERVAL 5 MINUTES
        """)
    ))
```

A stream-stream join must have **both** a watermark on each side **and** a time-range constraint in the join condition — together, these bound how long Spark keeps unmatched rows buffered waiting for a match on the other side. Without both, join state also grows unbounded.

---

## Part 5 — Stateful Operations and the State Store

### What the State Store Holds

Any operator that needs to remember information across micro-batches (a running aggregate, a dedup set, a join buffer, custom `mapGroupsWithState` logic) keeps that memory in a **state store** — a versioned, checkpointed key-value store tied to the query's checkpoint location.

- Default implementation: an in-memory HDFS-backed state store, checkpointed to the configured `checkpointLocation` after each micro-batch.
- **RocksDB state store** (`spark.sql.streaming.stateStore.providerClass`) is available for large-state workloads where the default in-memory store would pressure executor memory — an important lever for the exam's "state grew too large / executor OOM during a stateful streaming job" scenario, alongside adding/tightening a watermark.

### Arbitrary Stateful Processing

For custom logic beyond built-in aggregations, `mapGroupsWithState`/`flatMapGroupsWithState` (and PySpark's `applyInPandasWithState`) let you maintain and update arbitrary per-key state across batches, with an explicit timeout policy (`GroupStateTimeout.ProcessingTimeTimeout()` or `EventTimeTimeout()`) governing when Spark should expire a key's state if no new data arrives for it.

**Exam trap:** state store growth is one of the most common real-world streaming-job memory issues, and the fix is almost never "just add more executor memory" — it's **add or tighten a watermark** (bound how long state is kept) or **switch to RocksDB** (bound how much of that state must live in JVM heap at once).

---

## Part 6 — Fault Tolerance and Exactly-Once Semantics

End-to-end exactly-once requires **both** halves to cooperate:

| Half | Requirement |
|---|---|
| Source | Must be **replayable** — Spark can re-request the same offset range after a failure (Auto Loader, Kafka, Delta, files all qualify; a source that can't be re-read from an arbitrary offset breaks this guarantee) |
| Sink | Must be **idempotent** — writing the same micro-batch's output twice (e.g., after a restart re-processes an in-flight batch) must not duplicate data |

Delta as a sink gets this largely for free: each micro-batch's write is one atomic transaction-log commit (Day 9), and the checkpoint's recorded offsets ensure a restarted query resumes from the correct point rather than reprocessing a committed batch. A `foreachBatch` sink writing to a non-transactional system must implement its own idempotency (e.g., an upsert keyed by a batch ID) to get the same guarantee.

**Exam trap:** "exactly-once" is a property of the **source + sink + checkpoint working together**, not something Structured Streaming grants automatically regardless of what you write to. A scenario describing duplicate rows after a job restart is almost always pointing at a non-idempotent sink, not a Structured Streaming bug.

---

## Part 7 — Structured Streaming vs. Lakeflow Declarative Pipelines (Preview)

This is the exam's named comparison bullet; Day 15/16 cover it in full once Lakeflow's own concepts (expectations, `AUTO CDC`, streaming tables vs. materialized views) are on the table. For now, the headline distinction:

| | Structured Streaming (this day) | Lakeflow Declarative Pipelines (Day 15/16) |
|---|---|---|
| Programming model | Imperative — you write `readStream`/`writeStream` and manage triggers/checkpoints yourself | Declarative — you describe the desired tables; Lakeflow manages triggers, checkpoints, and dependency ordering |
| Data quality | Manual (`filter`, `foreachBatch` routing — Day 13's classic quarantine pattern) | Built-in `@dlt.expect*` constraints |
| Multi-hop orchestration | You wire notebooks/jobs together yourself | Native support for chained bronze → silver → gold flows in one pipeline definition |
| Best fit | Fine-grained custom control, arbitrary stateful logic, non-Databricks-native sinks | Standard multi-hop ETL where declarative data-quality and orchestration outweigh the need for custom control |

**Exam framing:** a scenario needing custom stateful logic (`mapGroupsWithState`), a non-standard sink, or very fine-grained trigger/checkpoint control points toward raw Structured Streaming; a scenario about a standard bronze/silver/gold pipeline with data-quality rules points toward Lakeflow Declarative Pipelines.

---

## Part 8 — Exam Traps Recap

1. Micro-batch execution is ordinary Spark batch execution repeated per trigger — Day 4–8's Jobs/Stages/Tasks/Spark UI diagnostics apply unchanged inside each batch.
2. `Trigger.Once()` (single giant batch) is superseded by `Trigger.AvailableNow()` (bounded multiple batches) — know which one a scenario is actually describing.
3. The official retired trigger-interval sample question's answer is **decrease the interval** for better executor utilization during peak load — memorize this exact question and answer.
4. `outputMode("append")` on an aggregation **requires a watermark** — without one, the query fails to start, not just behaves oddly.
5. A watermark must be declared **before** the windowed aggregation, on the **same** event-time column used in that aggregation.
6. Stream-stream joins need watermarks on **both** sides plus a time-range join condition to bound state — either alone is not sufficient.
7. Unbounded stateful growth is fixed with a watermark (bound retention) or RocksDB (bound in-heap footprint) — not "add executor memory" as a first move.
8. Exactly-once requires a replayable source **and** an idempotent sink working together with the checkpoint — it is not automatic regardless of what you write to.

---

## Cross-References
- Day 4/5: Jobs/Stages/Tasks and the DAG — the execution model running inside every micro-batch.
- Day 7/8: Memory and Spark UI diagnostics — apply unchanged to a slow or OOMing streaming micro-batch.
- Day 9: Delta's transaction log — what makes Delta an idempotent, exactly-once-friendly streaming sink.
- Day 12: `Trigger.AvailableNow()` introduced as the batch/streaming bridge; the format landscape for streaming sources.
- Day 13: Auto Loader as a concrete streaming source — everything here about triggers, checkpoints, and output modes applies directly to a `cloudFiles` stream.
- Day 15/16: Lakeflow Declarative Pipelines, `AUTO CDC`, and streaming tables vs. materialized views — the declarative layer built on top of the concepts in this day.
