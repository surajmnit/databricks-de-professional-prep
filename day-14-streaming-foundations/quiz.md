# Day 14 — Quiz: Streaming Foundations — Structured Streaming, Triggers, Watermarking, and State

**Objective coverage:** Section 1 (Developing Code for Data Processing, 22%), Section 2 (Data Ingestion & Acquisition, 7%).

---

## Question 1
**Objective:** Micro-batch execution model — Spark UI applicability.

A streaming job's micro-batch is running slowly. An engineer opens the Spark UI to diagnose the bottleneck. Which statement is correct about what they will find?

A. The Spark UI is not available for streaming workloads — streaming uses a completely separate monitoring interface
B. Inside each micro-batch, the execution model is identical to an ordinary Spark batch job — Jobs/Stages/Tasks and shuffle behavior from Day 4/5 apply unchanged
C. Spark UI shows only the aggregate of all micro-batches, not the current one individually
D. Streaming micro-batches bypass the Spark scheduler entirely, so the Spark UI shows no useful information

---

## Question 2
**Objective:** Trigger.Once() vs. Trigger.AvailableNow().

A team needs to backfill a large historical backlog from a source system on a nightly schedule. They want the job to process everything available, then terminate until the next night. Which trigger is the current, documented best practice for this use case?

A. `Trigger.Once()` — processes all available data and stops, exactly matching the requirement
B. `Trigger.AvailableNow()` — processes all currently available data across multiple bounded micro-batches, then terminates automatically — the safe, current replacement for `Trigger.Once()`
C. Default trigger with no interval specified — runs continuously but will naturally terminate when the backlog is cleared
D. `Trigger.ProcessingTime("24 hours")` — runs once per day, matching the schedule

---

## Question 3
**Objective:** Official retired sample question — trigger interval adjustment.

*(This is a retired official exam sample question. The exam guide's own answer is the correct choice.)*

A Structured Streaming job's microbatch normally processes in under 3 seconds with a 10-second trigger interval. During peak hours, microbatch processing time sometimes exceeds 30 seconds, causing a backlog. Records must process in under 10 seconds. Holding all other variables constant, which adjustment meets the requirement?

A. Increase the trigger interval to 20 seconds to allow more time per batch
B. Use `Trigger.Once()` to process the entire backlog in one large batch
C. Decrease the trigger interval to 5 seconds — tighter interval improves executor utilization during peak backlog
D. Increase shuffle partitions to increase parallelism within each micro-batch

---

## Question 4
**Objective:** Output mode — append mode on aggregation requires watermark.

A data engineer writes the following streaming aggregation:
```python
counts = (streaming_df
    .groupBy("user_id")
    .count()
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/chk/users")
    .toTable("counts"))
```
What happens when this query is started?

A. The query starts successfully and emits updated counts on each trigger
B. Spark throws an `AnalysisException` — `outputMode("append")` on a streaming aggregation requires a watermark so Spark knows when a group's result is "final" and safe to emit once
C. The query starts but silently drops all output until a watermark is added
D. Append mode is automatically promoted to `complete` mode for aggregations

---

## Question 5
**Objective:** Watermark declaration timing.

A data engineer writes the following query, believing it will bound state by watermarking after the aggregation:
```python
counts = (streaming_df
    .groupBy("user_id")
    .count()
    .withWatermark("event_time", "10 minutes")  # declared after aggregation
    .writeStream ...)
```
What is the actual behavior?

A. This correctly bounds state — watermarks can be declared anywhere in the chain
B. Spark throws an error — watermarks must be declared before the operation that uses them
C. The watermark is silently ignored — it must be declared on the DataFrame before the aggregation, not after
D. The watermark works but has no effect because `event_time` is not a window function column

---

## Question 6
**Objective:** Stream-stream join watermarks.

A streaming pipeline joins two streams — `orders` and `shipments` — on `order_id` and a time tolerance. The engineer adds a watermark on the `orders` side but not on the `shipments` side. What is the consequence?

A. The join works correctly — a watermark on one side is sufficient to bound join state
B. Join state on the `shipments` side grows unbounded — stream-stream joins require watermarks on **both** sides plus a time-range condition to bound state
C. Spark automatically infers the watermark on `shipments` based on the join condition
D. The join fails immediately with a validation error

---

## Question 7
**Objective:** State store — unbounded growth diagnosis.

A long-running streaming aggregation job is consuming more and more memory on its executors over time, eventually causing an OOM. The engineer asks what the correct first fix is, assuming a watermark is already in use. What is the most appropriate recommendation?

A. Increase `spark.executor.memory` to give the state store more heap space
B. Add or tighten the watermark to bound how long state is retained per key
C. Switch the sink from Delta to a plain Parquet format
D. Reduce the checkpoint interval to commit state more frequently

---

## Question 8
**Objective:** RocksDB state store.

A streaming job maintains a deduplication set with millions of keys. The in-memory HDFS-backed state store is causing executor memory pressure. Which state store provider addresses this?

A. `spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider` — stores state on disk instead of JVM heap, bounding the in-heap footprint
B. `spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.HDFSStateStoreProvider` — same provider but with increased memory allocation
C. RocksDB is only available for batch jobs, not streaming
D. The solution is to disable state checkpointing entirely

---

## Question 9
**Objective:** Exactly-once semantics — source and sink cooperation.

A streaming pipeline writes to a custom REST API sink via `foreachBatch`. After a job restart, the analyst notices duplicate records in the downstream system. What is the most likely cause?

A. The checkpoint is corrupted and must be deleted to force a clean restart
B. The REST API sink is not idempotent — writing the same micro-batch's output twice after a restart re-processes data, duplicating records
C. Kafka is not a replayable source and must be replaced with Auto Loader
D. Exactly-once is only achievable when writing to Delta tables

---

## Question 10
**Objective:** Exactly-once — Delta as sink.

A streaming job writes to a Delta table using `toTable("output_table")`. The job restarts after a failure. Which combination of features ensures exactly-once semantics without any additional code?

A. Delta's transaction log is automatically exactly-once; no checkpoint needed
B. Delta's transaction log (idempotent sink) + checkpoint tracking processed offsets (replayable resume point) together provide exactly-once
C. Exactly-once requires a custom `foreachBatch` implementation regardless of sink type
D. Delta tables are only once-delivery, not exactly-once, for streaming writes

---

## Question 11
**Objective:** Output mode — update vs. complete.

A streaming aggregation produces running click counts per user. On each trigger, only a small number of users' counts have changed since the last trigger, but there are millions of users total. Which output mode is most appropriate?

A. `complete` — always writes the full result
B. `append` — only writes new rows
C. `update` — only writes rows that changed since the last trigger — efficient for running aggregates with millions of groups where most don't change per batch
D. `append` with watermark — only applicable for this use case

---

## Question 12
**Objective:** Watermark — event-time mechanism scope.

A pipeline uses `processing_time()` as its time function rather than event time from the data. A watermark is added to the pipeline to bound state. What happens?

A. The watermark correctly bounds state for processing-time-based operations
B. The watermark has no effect — watermarking is an **event-time** mechanism that operates on event-time columns, not processing time
C. Spark automatically switches to event-time mode when a watermark is detected
D. A watermark on processing time causes Spark to fail the query immediately

---

## Question 13
**Objective:** Structured Streaming vs. Lakeflow — model distinction.

A team needs to implement a complex deduplication window that uses `mapGroupsWithState` with a custom timeout policy. Which approach is better suited for this requirement?

A. Lakeflow Declarative Pipelines — built-in `@dlt` expectations handle all stateful logic automatically
B. Raw Structured Streaming — `mapGroupsWithState` gives arbitrary per-key state management with explicit timeout policies that declarative pipelines do not expose directly
C. Lakeflow Declarative Pipelines support all stateful operations via SQL syntax
D. Either approach is equally capable for arbitrary stateful logic

---

## Question 14
**Objective:** Continuous processing trigger.

A team is evaluating true low-latency processing (~1ms) for a use case that involves a simple filter operation with no aggregations. They ask about `Trigger.Continuous("1 second")`. What is the correct guidance?

A. Continuous processing supports all operations including aggregations, joins, and windowed functions
B. Continuous processing supports only a narrow set of map-like operations — aggregations and joins are not supported; in practice it is rarely the right answer
C. Continuous processing is the default trigger for all streaming jobs on Databricks
D. Continuous processing is faster than micro-batch processing for all operation types

---

## Question 15
**Objective:** mapGroupsWithState — timeout policies.

A streaming job uses `mapGroupsWithState` with a business rule that a user's session state should be expired if no new events arrive within 30 minutes of the last event (event time, not processing time). Which timeout configuration is correct?

A. `GroupStateTimeout.ProcessingTimeTimeout()` — expires state after 30 minutes of processing time
B. `GroupStateTimeout.EventTimeTimeout()` with a `EventTimeTimeout` configured at 30 minutes on the watermark — expires state when the watermark passes 30 minutes beyond the last event time seen for that key
C. `GroupStateTimeout.NoTimeout()` — no timeout is needed for session-based state
D. Both timeout types work identically for session-based state management

---

## Answer Key

### Q1: B
Inside each micro-batch, Structured Streaming runs an ordinary Spark batch — Jobs, Stages, Tasks, shuffle, and memory behavior from Days 4–8 apply unchanged. A slow micro-batch is diagnosed exactly like a slow batch job using the Spark UI.

### Q2: B
`Trigger.AvailableNow()` is the documented, current-recommended replacement for `Trigger.Once()`. It processes all available data across **multiple bounded micro-batches** (not one giant batch), then terminates automatically. This gives streaming-grade correctness (checkpoint-tracked, exactly-once) on a batch-economy schedule.

### Q3: C
This is a retired official exam sample question. The exam guide's answer is **decrease the trigger interval to 5 seconds** — tighter intervals improve executor utilization under peak backlog by preventing idle time between batches. The intuition that "shorter interval can't help if processing already takes 30s" is explicitly rejected by the official answer key.

### Q4: B
`outputMode("append")` on a streaming aggregation requires a watermark — without one, Spark cannot know when a group's result is "final" and safe to emit once, so it throws an `AnalysisException` at query start. The fix is to add `withWatermark("event_time_col", "N minutes")` before the aggregation, or switch to `update`/`complete` mode.

### Q5: C
The watermark is silently ignored — it must be declared on the DataFrame **before** the aggregation that uses it. Declaring it after the aggregation, or on a different column than the one used in the window function, provides no state bounding.

### Q6: B
Stream-stream joins require watermarks on **both** sides plus a time-range constraint in the join condition to bound state. A watermark on one side alone leaves the other side's unmatched rows unconstrained, growing unbounded. Neither alone is sufficient.

### Q7: B
Adding or tightening the watermark is the first and most effective fix for unbounded state — it tells Spark how long to retain state per key, directly reducing what the state store must hold. "Add executor memory" is almost never the right first move for a state growth problem.

### Q8: A
`RocksDBStateStoreProvider` stores state on disk rather than in JVM heap, bounding the in-heap footprint while still providing fast state access. This is the correct lever for large-state streaming workloads where the default in-memory HDFS state store causes executor OOM.

### Q9: B
Exactly-once requires a **replayable source** + **idempotent sink** + checkpoint all working together. A custom REST API sink is almost certainly not idempotent — writing the same micro-batch twice after a restart produces duplicates. The fix is to implement idempotency in the `foreachBatch` logic (e.g., an upsert keyed by batch ID).

### Q10: B
Delta as a sink provides an idempotent commit via the transaction log. The checkpoint provides the replayable resume point. Together with a replayable source (Auto Loader, Kafka, etc.), exactly-once is achieved without any additional code.

### Q11: C
`update` mode writes only rows that changed since the last trigger — efficient for running aggregates where most groups don't change per batch. `complete` mode rewrites the entire result each trigger (impractical for millions of groups). `append` without watermark is invalid for aggregations.

### Q12: B
Watermarking is an **event-time** mechanism — it operates on event-time columns extracted from the data itself, not on processing time (when Spark processes the data). A watermark applied to processing time has no meaningful effect on state bounding.

### Q13: B
`mapGroupsWithState` / `applyInPandasWithState` give arbitrary per-key state management with explicit timeout policies — this level of custom stateful control is not exposed through Lakeflow Declarative Pipelines' declarative model. A scenario needing custom stateful logic points toward raw Structured Streaming.

### Q14: B
Continuous processing (experimental) supports only a narrow set of map-like stateless operations — aggregations, joins, and windowed functions are **not supported**. In practice it is rarely the right answer given these constraints.

### Q15: B
Session-based state that should expire based on event gaps uses `GroupStateTimeout.EventTimeTimeout()` configured via the watermark — state expires when the watermark advances past the timeout threshold beyond the last event time seen for that key. `ProcessingTimeTimeout()` would expire based on wall-clock time since the state was last updated, which is a different semantic.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Micro-batch = ordinary Spark execution |
| 2 | Easy | AvailableNow replaces Once |
| 3 | Hard | Official retired trigger-interval sample question |
| 4 | Medium | Append mode on aggregation requires watermark |
| 5 | Medium | Watermark declaration timing — before aggregation |
| 6 | Hard | Stream-stream joins — watermarks on both sides |
| 7 | Medium | Unbounded state = add/tighten watermark first |
| 8 | Medium | RocksDB state store for large state |
| 9 | Medium | Exactly-once = replayable source + idempotent sink |
| 10 | Medium | Delta + checkpoint = exactly-once for free |
| 11 | Easy | Update mode for running aggregates |
| 12 | Medium | Watermark = event-time only, not processing time |
| 13 | Medium | Custom stateful logic → raw Structured Streaming |
| 14 | Medium | Continuous processing limitations |
| 15 | Hard | EventTimeTimeout vs. ProcessingTimeTimeout for mapGroupsWithState |
