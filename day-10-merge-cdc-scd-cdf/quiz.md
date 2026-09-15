# Day 10 — Quiz: MERGE, CDC, SCD, and Change Data Feed

**Objective coverage:** Section 6 (13%), Section 3 (10%), Section 10 (6%).

---

## Question 1
**Objective:** `MERGE` multiple-match behavior.

A `MERGE` statement's `ON` condition matches two source rows to a single target row. What happens?

A. The target row is updated twice, silently, with the last write winning
B. Spark throws an error — a single target row cannot be matched by multiple source rows
C. Both source rows are inserted as new rows instead
D. The `MERGE` silently skips the ambiguous row and continues

---

## Question 2
**Objective:** Fixing the multiple-match error.

What is the correct fix when a `MERGE` fails due to multiple source rows matching one target row?

A. Change the `ON` condition to use `OR` instead of `AND`
B. Deduplicate the source data (e.g., via `ROW_NUMBER()`) to one row per key before the `MERGE`
C. Add a `WHEN NOT MATCHED BY SOURCE` clause
D. Switch from `MERGE` to a plain `INSERT`

---

## Question 3
**Objective:** `WHEN NOT MATCHED BY SOURCE`.

Which `MERGE` clause specifically reacts to target rows that no longer have a corresponding row in the source?

A. `WHEN MATCHED`
B. `WHEN NOT MATCHED`
C. `WHEN NOT MATCHED BY SOURCE`
D. `WHEN NOT MATCHED BY TARGET`

---

## Question 4
**Objective:** Manual CDC vs. `AUTO CDC`.

A team hand-writes a `foreachBatch` + `MERGE` CDC consumer. Which of the following must they handle themselves that a declarative `AUTO CDC` flow would handle automatically?

A. Reading from a streaming source at all
B. Out-of-order event sequencing and SCD Type 2 validity-window management
C. Writing to a Delta table
D. Nothing — the two approaches require identical manual effort

---

## Question 5
**Objective:** SCD Type 1 vs. Type 2.

A correction is made to a customer's misspelled name, and the business explicitly does not need to retain the old (incorrect) spelling. Which SCD pattern fits?

A. SCD Type 2 — always preserve history
B. SCD Type 1 — overwrite in place, no history needed
C. Neither — this requires Change Data Feed
D. SCD Type 2, but with `is_current` always set to false

---

## Question 6
**Objective:** SCD Type 2 mechanics by hand.

Implementing SCD Type 2 manually with `MERGE` to both close out a changed record and insert its new version for the same key typically requires:

A. A single `MERGE` statement with `WHEN MATCHED` and `WHEN NOT MATCHED` clauses only
B. Two separate statements — one to close/update the old current row, another to insert the new current row
C. A single `INSERT OVERWRITE` statement
D. `DROP TABLE` followed by `CREATE TABLE AS SELECT`

---

## Question 7
**Objective:** Enabling Change Data Feed.

Which correctly enables CDF on an already-existing Delta table?

A. `ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);`
B. `CREATE CHANGE FEED ON TABLE t;`
C. `ENABLE CDF t;`
D. CDF is always on by default for every Delta table and cannot be toggled

---

## Question 8
**Objective:** CDF backfill behavior.

A table has existed for 6 months with 500 versions of history. CDF is enabled today. Querying `table_changes()` from version 0, do you see change records for the earlier 5 months?

A. Yes, Delta reconstructs full history automatically
B. No — CDF only captures changes made after it was enabled; it does not backfill
C. Yes, but only if `VACUUM` has never been run
D. Only the most recent 30 days of history are backfilled automatically

---

## Question 9
**Objective:** CDF row semantics for `UPDATE`.

An `UPDATE` changes one row's `status` column. How many rows does the change feed produce for this event?

A. One row, showing only the new value
B. Two rows — one `update_preimage` (old value) and one `update_postimage` (new value)
C. Zero rows — updates are not tracked by CDF
D. Three rows — one for each of insert, update, and delete semantics

---

## Question 10
**Objective:** CDF metadata columns.

Which column in a CDF result set indicates whether a row is an `insert`, `update_preimage`, `update_postimage`, or `delete`?

A. `_operation`
B. `_change_type`
C. `_row_type`
D. `_action`

---

## Question 11
**Objective:** CDF's exam-tested purpose (the named limitation it addresses).

A streaming table's upstream source now occasionally receives `UPDATE`s. A plain streaming table read against this source will:

A. Correctly reflect the updates, since Delta always propagates all changes
B. Fail to reflect the updates — streaming tables assume an append-only source and won't see modifications to existing rows
C. Automatically convert itself into a materialized view
D. Throw an error immediately upon the first `UPDATE`

---

## Question 12
**Objective:** CDF vs. Materialized View trade-off.

Given the scenario in Q11, and a strict low-latency requirement that rules out full recompute, what is the recommended fix?

A. Switch to a Materialized View — it handles updates/deletes correctly regardless of latency cost
B. Enable Change Data Feed on the source and consume it incrementally via `readChangeFeed` + `MERGE` downstream
C. Ignore the updates; streaming tables are only meant for append-only data anyway
D. Increase the streaming trigger interval to allow more time for updates to propagate

---

## Question 13
**Objective:** CDF streaming read default behavior.

When a CDF-enabled streaming read (`readStream.option("readChangeFeed", "true")`) first starts with no `startingVersion` specified, what does it return initially?

A. Nothing, until the first future change occurs
B. The table's current snapshot as `insert` records, then future changes as change events
C. Only rows changed in the last hour
D. An error — a starting version must always be explicitly specified

---

## Question 14
**Objective:** CDF and retention interaction.

A CDF consumer has been paused for a long time, and `VACUUM` has since removed files older than the retention window. What happens when the consumer tries to resume from its old checkpoint version?

A. It resumes seamlessly; retention never affects CDF
B. It may fail — a `startingVersion` that depended on now-vacuumed files is no longer readable
C. It automatically skips ahead to the current version silently, with no error
D. `VACUUM` is blocked automatically while any CDF consumer exists

---

## Question 15
**Objective:** MERGE performance consideration.

A `MERGE` against a very large target table is slow due to a skewed join key. Which Day 6/9-linked concept most directly helps?

A. Disabling schema enforcement
B. Ensuring the merge key is well-distributed, and considering Liquid Clustering/Z-Ordering on that key to reduce files scanned
C. Switching the `MERGE` to a `DELETE` followed by an `INSERT`
D. Increasing `spark.driver.memory`

---

## Answer Key

### Q1: B
`MERGE` raises `UnsupportedOperationException` when the `ON` condition allows more than one source row to match a single target row — this is a hard failure, not a silent behavior.

### Q2: B
Deduplicating the source to exactly one row per key (typically via `ROW_NUMBER()`/`QUALIFY` keeping the latest) before the `MERGE` resolves the ambiguity at its root.

### Q3: C
`WHEN NOT MATCHED BY SOURCE` is the clause for target rows with no corresponding source row — distinct from `WHEN NOT MATCHED [BY TARGET]`, which is for source rows with no corresponding target row.

### Q4: B
Ordering and SCD2 windowing are the two things a hand-written `foreachBatch`+`MERGE` pipeline must manage explicitly; `AUTO CDC` (Day 15) automates both via `sequence_by` and `stored_as_scd_type`.

### Q5: B
No retention requirement for history is the defining signal for SCD Type 1 — simple overwrite is appropriate and simpler.

### Q6: B
Manual SCD Type 2 is a two-statement pattern: close out the old current row (update its `end_date`/`is_current`), then insert a new current row — a single `MERGE` cannot cleanly do both when the insert needs data tied to the row being closed.

### Q7: A
`ALTER TABLE ... SET TBLPROPERTIES (delta.enableChangeDataFeed = true)` is the correct, documented syntax for an existing table.

### Q8: B
CDF captures only changes made after being enabled — it does not reconstruct or backfill history from before that point, regardless of how much table history otherwise exists.

### Q9: B
An `UPDATE` produces exactly two CDF rows for the changed row: `update_preimage` (before) and `update_postimage` (after) — not a single combined row.

### Q10: B
`_change_type` is the documented column carrying `insert`/`update_preimage`/`update_postimage`/`delete`.

### Q11: B
Streaming tables assume append-only sources; they will not correctly reflect `UPDATE`/`DELETE` mutations to existing rows in the upstream source.

### Q12: B
This is the exact, named exam trade-off: CDF + incremental `MERGE` gives materialized-view-level correctness (sees updates/deletes) at streaming-table-level latency (no full recompute) — the option a strict low-latency requirement calls for.
**Wrong options:** A ignores the stated latency constraint. C is factually wrong and ignores a real data-correctness problem. D doesn't address correctness at all.

### Q13: B
By default, a CDF stream with no starting point returns the table's current snapshot as `insert` rows first, then emits real future changes from that point forward.

### Q14: B
If `VACUUM` has removed the files a given `startingVersion` depends on, resuming a CDF read from that version fails — the same retention/`VACUUM` interaction that governs time travel applies to CDF.

### Q15: B
Merge-key skew and file-scan volume are addressed by ensuring good key distribution and by clustering the target table on the merge key (Liquid Clustering/Z-Ordering, Day 11) to reduce the files that must be scanned/joined against.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | `MERGE` multiple-match error |
| 2 | Easy | Fixing multiple-match via dedup |
| 3 | Medium | `WHEN NOT MATCHED BY SOURCE` |
| 4 | Medium | Manual CDC burden vs. `AUTO CDC` |
| 5 | Easy | SCD Type 1 use case |
| 6 | Medium | SCD Type 2 two-statement pattern |
| 7 | Easy | Enabling CDF syntax |
| 8 | Medium | CDF no-backfill behavior |
| 9 | Medium | CDF `UPDATE` = 2 rows |
| 10 | Easy | `_change_type` column |
| 11 | Hard | Streaming table append-only limitation |
| 12 | Hard | CDF vs. Materialized View latency/correctness trade-off (core exam objective) |
| 13 | Medium | CDF streaming default snapshot-as-insert behavior |
| 14 | Medium | CDF + `VACUUM` retention interaction |
| 15 | Medium | `MERGE` performance and clustering |
