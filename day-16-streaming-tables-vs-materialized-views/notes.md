# Day 16 — Streaming Tables vs. Materialized Views: Trade-Offs and Use Cases

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 1: Developing Code for Data Processing using Python and SQL (22%)**:
- "Explain the advantages and disadvantages of streaming tables compared to materialized views."

And connects directly to **Section 6: Cost & Performance Optimization (13%)**:
- "Apply Change Data Feed (CDF) to address specific limitations of streaming tables and enhance latency." *(Day 10 covered CDF's mechanics in depth — this day places that trade-off explicitly inside the streaming-table-vs-materialized-view decision, without re-deriving CDF itself.)*

*(Day 15 introduced the mechanical distinction — `dlt.read_stream` vs. `dlt.read` inside a `@dlt.table` function — as a preview. This day is the full advantages/disadvantages comparison the exam names explicitly.)*

---

## Part 1 — The Core Distinction

| | Streaming Table | Materialized View |
|---|---|---|
| Built from | `dlt.read_stream(...)` / `STREAM read_files(...)` — an incremental, append-only read | `dlt.read(...)` / a plain batch query — a static snapshot read |
| Refresh model | **Always incremental** — only processes rows that arrived since the last update | Lakeflow **decides** full recompute vs. incremental maintenance, depending on whether the query pattern is incrementalizable |
| Source assumption | **Append-only.** Upstream `UPDATE`/`DELETE`s are not correctly reflected | Any change pattern — inserts, updates, deletes are all correctly reflected on refresh |
| Execution mode | Can run **continuously** (processes new rows as they land) or triggered | **Refresh-based only** — even inside a "continuous" pipeline, a materialized view updates on a refresh cadence rather than processing row-by-row |
| Typical use | Bronze ingestion, append-heavy silver transforms, anything where the source genuinely never mutates existing rows | Aggregates, joins, deduplication, anything that must stay correct when upstream rows change or disappear |

Both are physically backed by real Delta tables once published — you query either one with ordinary `SELECT`. The difference is entirely in **how Lakeflow keeps them up to date**, not how you read them.

```sql
CREATE OR REFRESH STREAMING TABLE raw_orders
AS SELECT * FROM STREAM read_files('/Volumes/main/landing/orders/', format => 'json');

CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
AS SELECT order_date, SUM(amount) AS total
FROM LIVE.silver_orders
GROUP BY order_date;
```

---

## Part 2 — Advantages and Disadvantages (the exam's exact framing)

### Streaming Table

**Advantages:**
- Lowest latency and lowest incremental cost — a trigger only touches the rows that actually arrived since last time, never the whole table
- Can run continuously for near-real-time freshness
- Natural fit for the append-only ingestion layer (bronze) of a medallion architecture

**Disadvantages:**
- **Cannot correctly reflect `UPDATE`/`DELETE`s in the source.** If an upstream table starts receiving mutations, a streaming table reading it will simply miss them — it only ever sees newly appended rows, not modifications to rows it already processed
- No built-in mechanism to "go back and fix" a row it already processed incorrectly — correctness depends entirely on the append-only assumption holding

### Materialized View

**Advantages:**
- **Always correct** with respect to the current state of its source(s), regardless of whether the source has inserts, updates, or deletes
- The right tool for aggregations, deduplication, and joins, where "just append the new rows" isn't sufic to keep the result accurate

**Disadvantages:**
- Higher latency and cost per refresh — even when Lakeflow can incrementalize the maintenance, diffing/recomputing affected rows is inherently more expensive than a pure append
- Not continuously updated in the row-by-row sense — you're always looking at "as of the last refresh," not "as of right now"

**Exam trap:** don't assume "materialized view = always full recompute." Lakeflow's incremental-maintenance engine can often update a materialized view by processing only the rows affected by upstream changes rather than rebuilding the whole result from scratch — but this is a spectrum (how incrementalizable the query is), not a hard guarantee the way a streaming table's incremental behavior is.

---

## Part 3 — The Decision Framework

| Requirement | Correct choice |
|---|---|
| Source is genuinely append-only, lowest latency matters most | **Streaming Table** |
| Source receives updates/deletes, correctness matters, latency is not critical | **Materialized View** |
| Source receives updates/deletes, correctness matters, **and** low latency is required | **Neither alone** — enable Change Data Feed (Day 10) on the source and consume its row-level change events incrementally via a streaming table + `MERGE`, getting materialized-view-level correctness at streaming-table-level latency |

This third row is the exact scenario Day 10 built toward and the exam guide names explicitly in Section 6 ("apply CDF to address specific limitations of streaming tables"). The limitation being addressed is precisely Part 2's streaming-table disadvantage above — CDF is the bridge, not a third table type.

**Exam framing:** a scenario describing "our streaming table stopped reflecting corrections/deletes from the source" is testing whether you recognize the append-only limitation (Part 2) and reach for either a materialized view (if latency allows) or CDF + incremental merge (if it doesn't) — not "just add a watermark" or some other Day 14 streaming fix, since this isn't a lateness problem, it's a mutation-visibility problem.

---

## Part 4 — Mechanical Notes Worth Knowing

- **AUTO CDC targets are streaming tables, not materialized views.** `dlt.create_auto_cdc_flow(...)` writes into a table created via `dlt.create_streaming_table(...)` (Day 15) — this makes sense given AUTO CDC's job is to apply an incremental stream of change events, which is exactly a streaming table's native refresh model.
- Both object types support Lakeflow **expectations** (`@dlt.expect*`, Day 15) identically — data-quality enforcement is orthogonal to which refresh model the table uses.
- Continuous pipeline execution is meaningful for a streaming table (it can genuinely process rows as they arrive); for a materialized view in the same pipeline, "continuous" still means "refresh on a cadence," not row-by-row processing, since a view's correctness-preserving recompute isn't something you'd want triggered by every single incoming row.

---

## Part 5 — Exam Traps Recap

1. The exam's core ask is "advantages **and** disadvantages" — expect a scenario question, not a definition question. Match the stated requirement (latency vs. correctness-under-mutation) to the right table type.
2. Streaming tables assume append-only sources — silently missing upstream updates/deletes is the single most-tested limitation.
3. Materialized views are not always full recompute — Lakeflow incrementalizes when the query pattern allows it, but this is a spectrum, not a guarantee.
4. The "need both correctness and low latency" scenario points to **CDF + incremental merge**, not a third magic table type — this is the direct link to Day 10's Section 6 objective bullet.
5. AUTO CDC (Day 15) targets streaming tables specifically, never materialized views.
6. Both table types are queried identically once published — the refresh-model difference is invisible at query time; it only matters for freshness/correctness reasoning.

---

## Cross-References
- Day 9: Delta transaction log `add`/`remove` actions — the mechanism CDF's change events are built from.
- Day 10: Change Data Feed mechanics and the exact "materialized-view correctness at streaming-table latency" trade-off this day applies directly to the ST-vs-MV decision.
- Day 14: Structured Streaming's own append/update/complete output-mode trade-offs — the imperative-API analog of this day's declarative comparison.
- Day 15: `dlt.read` vs. `dlt.read_stream`, expectations, and AUTO CDC — the mechanical building blocks this day's comparison sits on top of.
- Day 21: Querying a pipeline's event log to observe actual refresh behavior and expectation metrics for either table type.
