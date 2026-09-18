# Day 16 — Cheat Sheet: Streaming Tables vs. Materialized Views — Trade-Offs and Use Cases

## Core Comparison

| | Streaming Table | Materialized View |
|---|---|---|
| Built from | `dlt.read_stream(...)` / `STREAM read_files(...)` | `dlt.read(...)` / plain batch query |
| Refresh model | **Always incremental** — only rows arrived since last update | Lakeflow decides full recompute vs. incremental maintenance |
| Source assumption | **Append-only** — upstream `UPDATE`/`DELETE`s NOT reflected | Any change pattern — inserts, updates, deletes all reflected on refresh |
| Execution | Continuous or triggered — processes rows as they land | Refresh-based only — "as of last refresh," not row-by-row |
| Typical use | Bronze ingestion, append-heavy silver transforms | Aggregates, joins, deduplication, anything needing correctness under source mutations |

Both are real Delta tables once published — queried identically with `SELECT`.

---

## Decision Framework

| Requirement | Correct choice |
|---|---|
| Source is append-only, lowest latency matters most | **Streaming Table** |
| Source has updates/deletes, correctness matters, latency not critical | **Materialized View** |
| Source has updates/deletes, correctness matters, AND low latency required | **CDF + incremental `MERGE`** — not a third table type; CDF bridges the gap |

**CDF bridge:** Enable CDF on the source → consume change events via a streaming table → `MERGE` them into the downstream target. This gets materialized-view-level correctness at streaming-table-level latency.

---

## Streaming Table Advantages / Disadvantages

**Advantages:**
- Lowest latency, lowest incremental cost — only new rows processed
- Can run continuously for near-real-time freshness
- Natural fit for the append-only bronze layer

**Disadvantages:**
- **Cannot reflect `UPDATE`/`DELETE`s in the source** — silently misses mutations to already-processed rows
- No mechanism to "go back and fix" a row — correctness entirely dependent on append-only assumption holding

---

## Materialized View Advantages / Disadvantages

**Advantages:**
- **Always correct** with respect to current source state — inserts, updates, deletes all reflected
- Right tool for aggregations/joins where append-only isn't sufficient for accuracy

**Disadvantages:**
- Higher latency and cost per refresh — diffing/recomputing affected rows is more expensive than pure append
- Not row-by-row continuously updated — always "as of last refresh"

**Exam trap:** materialized view ≠ always full recompute. Lakeflow incrementalizes when the query pattern allows, but this is a spectrum, not a hard guarantee.

---

## SQL Syntax

```sql
-- Streaming Table
CREATE OR REFRESH STREAMING TABLE raw_orders
AS SELECT * FROM STREAM read_files('/path/', format => 'json');

-- Materialized View
CREATE OR REFRESH MATERIALIZED VIEW daily_revenue
AS SELECT order_date, SUM(amount) AS total
FROM LIVE.silver_orders
GROUP BY order_date;
```

---

## AUTO CDC — Targets Streaming Tables Only

`dlt.create_auto_cdc_flow(target=..., ...)` only accepts a streaming table as the target. CDC change events are an incremental append stream — exactly what a streaming table handles natively.

---

## Exam Trap Shortlist

1. Streaming tables **silently miss** upstream `UPDATE`/`DELETE`s — no error, no warning, just incorrect data. This is the single most-tested limitation.
2. Materialized views are not always full recompute — Lakeflow incrementalizes when possible, but not guaranteed the way streaming tables are.
3. "Need correctness AND low latency" scenario → **CDF + incremental `MERGE`**, not a third magic table type.
4. AUTO CDC targets streaming tables, never materialized views.
5. Both table types support expectations identically — data quality is orthogonal to refresh model.
6. `dlt.read_stream` = streaming table; `dlt.read` = materialized view — determined inside the function body, not by the decorator.
