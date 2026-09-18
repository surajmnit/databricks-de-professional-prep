# Day 16 — Quiz: Streaming Tables vs. Materialized Views — Trade-Offs and Use Cases

**Objective coverage:** Section 1 (Developing Code for Data Processing, 22%), Section 6 (Cost & Performance Optimization, 13%).

---

## Question 1
**Objective:** Core distinction — refresh model.

A Lakeflow pipeline processes an upstream table that receives a mix of `INSERT`, `UPDATE`, and `DELETE` operations. Which table type correctly reflects all three operation types on each refresh?

A. Streaming Table — processes all change types incrementally
B. Materialized View — re-evaluates the query against the full source snapshot on each refresh, correctly reflecting inserts, updates, and deletes
C. Streaming Table if `continuous: true` is set
D. Neither — both table types only reflect `INSERT` operations

---

## Question 2
**Objective:** Streaming table — append-only limitation (silently missing updates).

A team uses a streaming table to ingest from an upstream table. The upstream process recently introduced a correction pipeline that `UPDATE`s rows that had incorrect values at ingestion time. The downstream analyst notices their streaming table still shows the old incorrect values. What is the most precise explanation?

A. Streaming tables only process `INSERT` operations; `UPDATE` and `DELETE` are simply not supported in Lakeflow
B. The streaming table's source assumption is append-only — it only sees rows that arrived after the last processed offset; mutations to already-committed rows are invisible to the stream and are silently missed
C. The pipeline has a bug and should be restarted to pick up the corrections
D. The streaming table requires a watermark to see `UPDATE` operations

---

## Question 3
**Objective:** Materialized view — not always full recompute.

A data engineer chooses a materialized view for an aggregation that must stay correct under source mutations. They are concerned about performance, expecting a full recompute of billions of source rows on every refresh. What is the accurate description of what actually happens?

A. Materialized views always do a full table scan and recompute from scratch on every refresh
B. Materialized views never do a full recompute — they are always incremental
C. Lakeflow decides whether to do a full recompute or an incremental maintenance based on whether the query pattern is incrementalizable — it is a spectrum, not a hard guarantee either way
D. Materialized views use a streaming table under the hood and are always incremental

---

## Question 4
**Objective:** Decision framework — append-only + low latency.

A data pipeline reads from a raw S3 ingestion bucket where files arrive continuously. The source system never modifies or deletes files that have already landed — it only ever appends new ones. Latency is the top priority. Which table type is most appropriate, and why?

A. Materialized View — always correct and handles any file arrival pattern
B. Streaming Table — designed for append-only incremental processing with lowest latency
C. Neither — you must use raw Structured Streaming without Lakeflow for this use case
D. Materialized View with `continuous: true` — gives both streaming latency and full correctness

---

## Question 5
**Objective:** Decision framework — updates/deletes + correctness + low latency.

A team needs to build a downstream table from a source that receives `UPDATE` and `DELETE` operations. Correctness is critical (errors cannot propagate), and the team has a strict low-latency requirement that rules out a full recompute approach. What is the recommended pattern?

A. Streaming Table — handles all change types with incremental processing
B. Materialized View with `continuous: true` — gives the correctness of a full refresh with streaming latency
C. Enable Change Data Feed (CDF) on the source and consume its row-level change events via a streaming table + incremental `MERGE` — getting materialized-view-level correctness at streaming-table-level latency
D. This requirement is impossible to satisfy with Lakeflow — the team must use raw Structured Streaming

---

## Question 6
**Objective:** Streaming table advantage — lowest incremental cost.

A streaming table and a materialized view both process the same source table. On a given update where only 50 new rows arrived, which statement is most accurate?

A. Both run identically — they both re-process the entire source
B. The streaming table processes only the 50 new rows; the materialized view processes either the 50 affected rows (if incrementalizable) or the full source (if not)
C. The materialized view always runs faster because it uses a different execution engine
D. The streaming table processes only the 50 new rows; the materialized view always processes the full source

---

## Question 7
**Objective:** AUTO CDC — targets streaming table, not materialized view.

A team uses `dlt.create_auto_cdc_flow` to apply CDC events from a source table into a downstream target. They attempt to use a materialized view as the `target`. What happens?

A. It works — AUTO CDC can write to either a streaming table or a materialized view
B. AUTO CDC only accepts a streaming table as the target — CDC change events are an incremental append stream that maps to the streaming table's refresh model
C. AUTO CDC writes to a materialized view but only reflects `INSERT` operations
D. The choice of target type is irrelevant — Lakeflow automatically selects the right one

---

## Question 8
**Objective:** CDF + streaming table + MERGE — the bridge.

A data engineer enables CDF on a source table and wants to consume those change events to keep a downstream target correct, even though the source receives `UPDATE` operations. Which combination of mechanisms correctly achieves this?

A. Streaming table alone — reads CDF changes incrementally and reflects them correctly
B. Materialized view alone — re-evaluates the full source including CDF changes
C. Streaming table + incremental `MERGE` into the downstream target — the streaming table reads CDF change events row-by-row; the `MERGE` applies each change to the target, achieving correctness at low latency
D. Streaming table + `GROUP BY` — aggregates CDF change events directly

---

## Question 9
**Objective:** Streaming table — no "go back and fix" mechanism.

A streaming table processed a row with an incorrect value last week. The upstream source has since corrected the value with an `UPDATE`. The analyst expects the streaming table to show the corrected value. What happens?

A. The streaming table automatically reprocesses the corrected row
B. The streaming table cannot "go back" — it has already committed past that record's offset and only processes new rows; it does not have a built-in mechanism to retroactively fix already-processed data
C. A watermark allows the streaming table to see the `UPDATE`
D. The pipeline must be restarted from the beginning to fix the historical value

---

## Question 10
**Objective:** Materialized view — latency trade-off.

A materialized view is configured in a pipeline running on a 5-minute trigger cadence. A data analyst queries the materialized view at 2:47 PM and sees revenue data from orders through 2:45 PM. Another analyst queries the same view at 2:53 PM and sees data through 2:50 PM. What explains the difference?

A. The materialized view is continuously updated row-by-row
B. The materialized view shows data "as of the last refresh" — between refreshes, it shows stale data from the last successful refresh, not real-time data
C. The pipeline has a bug causing inconsistent data between queries
D. Streaming tables and materialized views show identical freshness

---

## Question 11
**Objective:** `dlt.read_stream` vs. `dlt.read` — which creates which table type.

A data engineer writes a Lakeflow table function that uses `dlt.read("upstream")` inside the function body. What type of table does this create?

A. Streaming Table — `dlt.read` reads incrementally
B. Materialized View — `dlt.read` reads a full static snapshot each refresh
C. Streaming Table if the source is append-only; otherwise a materialized view
D. Neither — `@dlt.table` with `dlt.read` creates a view, not a table

---

## Question 12
**Objective:** Continuous execution — different meaning for each table type.

A Lakeflow pipeline is set to `continuous: true`. A streaming table and a materialized view are both in this pipeline. Which statement accurately describes how each behaves?

A. Both behave identically — "continuous" means the same thing for both table types
B. The streaming table genuinely processes rows as they arrive in near-real-time; the materialized view still refreshes on a cadence, not row-by-row — continuous for a view still means refresh-based maintenance, not streaming execution
C. Only the streaming table processes continuously; the materialized view is not affected by the continuous setting
D. Continuous mode is not supported for pipelines with materialized views

---

## Question 13
**Objective:** Expectations — orthogonal to table type.

A team applies `@dlt.expect_or_drop("amount > 0")` to both a streaming table and a materialized view. On a trigger where a row with `amount = -100` arrives, what happens in each table?

A. Both tables drop the row — data-quality enforcement (expectations) works identically regardless of which refresh model the table uses
B. Only the streaming table drops the row; materialized views don't support expectations
C. The streaming table logs the violation and drops the row; the materialized view logs it but keeps the row
D. Only the materialized view drops the row; streaming tables never filter data

---

## Question 14
**Objective:** Both table types backed by real Delta tables.

A downstream BI tool runs `SELECT * FROM silver_orders`. The analyst doesn't know whether `silver_orders` is a streaming table or a materialized view. What can they observe from the query result itself?

A. The query result is different depending on table type
B. Both are real Delta tables once published — they are queried identically with ordinary `SELECT`, and the result reflects current data, not data at a specific past version
C. Streaming tables show only data from the last micro-batch; materialized views show data from all historical micro-batches
D. Materialized views are not queryable — only streaming tables support `SELECT`

---

## Question 15
**Objective:** The exact exam-named trade-off — "materialized view correctness at streaming table latency."

A team needs to consume CDC events from a source table that receives `UPDATE` and `DELETE` operations, with a latency requirement of under 30 seconds and a requirement to never reflect an incorrect value downstream. Which description of the correct solution is most precise?

A. Streaming table alone — handles all change types with minimal latency
B. Materialized View alone — guarantees correctness but with higher latency
C. Neither streaming table nor materialized view alone satisfies both requirements simultaneously — the solution is to enable CDF on the source, consume its change events via a streaming table, and apply them to the downstream target using an incremental `MERGE` — this combines materialized-view-level correctness with streaming-table-level latency
D. CDF is not compatible with Lakeflow pipelines

---

## Answer Key

### Q1: B
A materialized view re-evaluates the query against the full source snapshot on each refresh, correctly reflecting inserts, updates, and deletes regardless of the source's mutation pattern. A streaming table only sees newly appended rows — mutations to already-committed rows are invisible to it.

### Q2: B
Streaming tables assume an append-only source — they only process rows that arrived after the last committed offset. Mutations to rows already processed are invisible to the stream. There is no error, no watermark that fixes this — it is a silent correctness gap when the append-only assumption is violated.

### Q3: C
Lakeflow decides whether to full-recompute or incrementally maintain a materialized view based on whether the query pattern is incrementalizable. This is a spectrum — some queries can be maintained by processing only affected rows; others require a broader recompute. It is not guaranteed either way the way a streaming table's incremental behavior is.

### Q4: B
A streaming table is the natural fit for an append-only source with latency as the top priority. It processes only newly arrived rows, runs at lowest incremental cost, and can run continuously for near-real-time freshness.

### Q5: C
This is the exact named trade-off from Day 10's Section 6 objective bullet: enable CDF on the source → consume change events via a streaming table → `MERGE` them into the downstream target. This combines materialized-view-level correctness (sees all change types) with streaming-table-level latency (incremental, no full recompute).

### Q6: B
The streaming table processes only the 50 new rows. The materialized view processes either the 50 affected rows (if Lakeflow can incrementalize the query) or the full source (if it cannot). The streaming table's incremental behavior is guaranteed; the materialized view's is a spectrum.

### Q7: B
`dlt.create_auto_cdc_flow` only accepts a streaming table as the target. CDC change events are an incremental append stream — exactly the model a streaming table handles natively. A materialized view expects to re-evaluate against a full snapshot, which is incompatible with row-level CDC event consumption.

### Q8: C
The streaming table reads CDF's row-level change events incrementally. The downstream target is kept correct via an incremental `MERGE` that applies each change event — `insert` as an insert, `update_postimage` as an update, `delete` as a delete. This is the CDF bridge pattern that satisfies both correctness and low latency.

### Q9: B
Streaming tables have no built-in mechanism to retroactively fix already-processed rows. The stream has moved past that record's offset. The only ways to get a corrected view are: (1) use a materialized view, or (2) use CDF + incremental merge so the `UPDATE` event is consumed and applied.

### Q10: B
A materialized view always shows data "as of the last refresh" — between refreshes, it shows stale data from the last successful refresh cycle. Streaming tables process rows continuously and show near-real-time data. A materialized view is not continuously updated in the row-by-row sense.

### Q11: B
`dlt.read` creates a materialized view — it reads a full static snapshot of the upstream table each refresh. `dlt.read_stream` creates a streaming table. The read call inside the function body, not the decorator, determines the table type.

### Q12: B
`continuous: true` means something different for each table type in the same pipeline. The streaming table genuinely processes rows as they arrive. The materialized view still refreshes on a cadence — its correctness-preserving recompute isn't something you'd want triggered by every single incoming row, so "continuous" for a view still means refresh-based, not streaming.

### Q13: A
Data-quality enforcement via `@dlt.expect*` is orthogonal to which refresh model a table uses. Both streaming tables and materialized views support expectations identically, and both drop the violating row when `@dlt.expect_or_drop` is violated.

### Q14: B
Both streaming tables and materialized views are physically backed by real Delta tables once published. They are queried identically with ordinary `SELECT`, and the result reflects the current state of the data. The refresh model difference is invisible to the consumer — it only matters for how the pipeline keeps the table up to date.

### Q15: C
Neither table type alone satisfies both requirements: a streaming table misses upstream mutations; a materialized view's correctness comes with higher latency. The CDF bridge — enable CDF, read via streaming table, `MERGE` into target — is the exact pattern that delivers materialized-view-level correctness (sees updates/deletes) at streaming-table-level latency (incremental, not full recompute).

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Materialized view correctly reflects all change types |
| 2 | Medium | Streaming table silently misses upstream mutations |
| 3 | Medium | MV not always full recompute — spectrum behavior |
| 4 | Easy | Append-only + low latency → streaming table |
| 5 | Hard | Correctness + low latency → CDF + incremental MERGE |
| 6 | Easy | Streaming table always processes only new rows |
| 7 | Easy | AUTO CDC targets streaming table, not materialized view |
| 8 | Medium | CDF + streaming table + MERGE = the bridge |
| 9 | Medium | Streaming table cannot go back to fix historical rows |
| 10 | Easy | Materialized view = as-of-last-refresh freshness |
| 11 | Easy | `dlt.read` = materialized view |
| 12 | Medium | Continuous mode means different things for each type |
| 13 | Easy | Expectations work identically on both table types |
| 14 | Easy | Both backed by real Delta tables, queried identically |
| 15 | Hard | Exact exam-named trade-off — CDF bridge pattern |
