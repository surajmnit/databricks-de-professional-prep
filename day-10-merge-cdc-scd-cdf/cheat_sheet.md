# Day 10 — Cheat Sheet: MERGE, CDC, SCD, and Change Data Feed

## MERGE INTO Clauses

```sql
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET ...
WHEN MATCHED AND <cond> THEN DELETE
WHEN NOT MATCHED [BY TARGET] THEN INSERT ...
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET ...   -- reacts to rows MISSING from source
```
**Multiple-match error:** if `ON` matches >1 source row to 1 target row -> `UnsupportedOperationException`. **Fix:** dedupe source with `ROW_NUMBER()` first, never change `ON` to compensate.

---

## SCD Type 1 vs. Type 2

| | Type 1 | Type 2 |
|---|---|---|
| History | None -- overwrite | Full -- every version kept |
| Implementation | Single `MERGE ... UPDATE` | **Two statements**: close old row (`end_date`, `is_current=false`), then `INSERT` new current row |
| Tracking columns | None needed | `effective_date`, `end_date`, `is_current` |
| Automated version | -- | `AUTO CDC ... STORED AS SCD TYPE 2` (Day 15) |

---

## Change Data Feed (CDF)

**Enable:**
```sql
CREATE TABLE t (...) TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;  -- session default
```
Warning: **no backfill** -- only changes after enabling are captured.

**Read (batch):**
```sql
SELECT * FROM table_changes('t', start_version [, end_version]);
```
```python
spark.read.format("delta").option("readChangeFeed","true")
  .option("startingVersion", n).table("t")
```

**Read (stream):**
```python
spark.readStream.option("readChangeFeed","true").table("t")
```
Default: no `startingVersion` -> returns current snapshot as `insert` rows first, then future changes.

**Extra columns:**
| Column | Values |
|---|---|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | Delta table version of the change |
| `_commit_timestamp` | Commit time |

Warning: **One `UPDATE` = TWO CDF rows** (`update_preimage` + `update_postimage`), not one.

---

## The Exam-Named Trade-off (memorize this exactly)

| Requirement | Tool |
|---|---|
| Append-only source, low latency | Streaming Table |
| Source has updates/deletes, correctness matters, latency not critical | Materialized View (full recompute) |
| Source has updates/deletes, correctness matters, AND low latency required | **CDF + incremental `MERGE`** -- materialized-view correctness at streaming-table latency |

---

## Retention Interaction (cross-ref Day 9/19)

- CDF change data governed by the **same retention** as time travel (`delta.deletedFileRetentionDuration` / `delta.logRetentionDuration`).
- `VACUUM` past retention -> a `startingVersion` depending on removed files **fails to read**, doesn't silently skip.
- A long-paused CDF/streaming consumer can permanently lose its resume point.

---

## Exam Trap Shortlist

1. `MERGE` multiple-match -> hard error, fix by deduping source, not by changing `ON`.
2. `WHEN NOT MATCHED BY SOURCE` = target row with no source match (opposite of the common clause).
3. Manual CDC (`foreachBatch`+`MERGE`) = you own ordering + SCD2 windowing; `AUTO CDC` (Day 15) automates both.
4. SCD Type 2 by hand = **two statements**, not one `MERGE`.
5. CDF must be explicitly enabled; **never backfills**.
6. `UPDATE` -> 2 CDF rows (preimage + postimage).
7. CDF's purpose, exact exam wording: correctness of a Materialized View + latency of a Streaming Table.
8. CDF version reads are bounded by the same `VACUUM`/retention rules as time travel.
9. Merge-key skew -> fix via good key distribution + Liquid Clustering/Z-Ordering (Day 11), not more driver memory.
