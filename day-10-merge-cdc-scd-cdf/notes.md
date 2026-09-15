# Day 10 — MERGE, CDC, SCD, and Change Data Feed (CDF)

## Exam Objectives (Exam Guide, July 2026)

Primary bullet — **Section 6: Cost & Performance Optimization (13%)**:
- "Apply Change Data Feed (CDF) to address specific limitations of streaming tables and enhance latency."

Supporting bullets this day builds toward:
- **Section 3**: "Write efficient Spark SQL and PySpark code to apply advanced data transformations... to manipulate and analyze large datasets" — `MERGE INTO` is the core mechanism.
- **Section 10**: "Design and implement scalable data models using Delta Lake" — SCD Type 1/2 dimensional modeling patterns.

*(The **declarative**, automated version of CDC/SCD — `AUTO CDC` inside Lakeflow Declarative Pipelines — is Day 15's material. This day teaches the underlying manual mechanics first, so Day 15's automation is understood as "the thing that replaces what you're about to hand-write here," not a black box.)*

---

## Part 1 — `MERGE INTO`: The Upsert Primitive Everything Else Builds On

```sql
MERGE INTO target_table AS t
USING source_updates AS s
ON t.id = s.id
WHEN MATCHED AND s.is_deleted = true THEN DELETE
WHEN MATCHED THEN UPDATE SET t.value = s.value, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (id, value, updated_at) VALUES (s.id, s.value, s.updated_at)
WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.is_active = false;  -- optional 4th clause type
```

| Clause | Fires when |
|---|---|
| `WHEN MATCHED` | A row exists in both target and source, matched by the `ON` condition — can `UPDATE` or `DELETE`. Multiple `WHEN MATCHED` clauses are allowed with different conditions, evaluated in order. |
| `WHEN NOT MATCHED [BY TARGET]` | A source row has no matching target row — typically `INSERT` |
| `WHEN NOT MATCHED BY SOURCE` | A target row has no matching source row — lets you react to rows that *disappeared* from the source (e.g., soft-delete/deactivate) |

**Exam trap — the "multiple match" error:** if the `ON` condition matches **more than one source row to the same target row**, `MERGE` throws `UnsupportedOperationException: ... the ON search condition ... matched a single row from the target table with multiple rows of the source table`. This is a very common real-world (and exam-scenario) bug — the fix is to **deduplicate the source** (e.g., keep only the latest row per key via `ROW_NUMBER()`/`QUALIFY`) *before* the `MERGE`, not to change the `ON` condition.

**Performance note:** `MERGE` still triggers a shuffle/join to find matches — for very large targets, ensure the join key is well-distributed (not skewed) and consider Liquid Clustering/Z-Ordering (Day 11) on the merge key to reduce files scanned.

---

## Part 2 — Manual CDC: The "Roll Your Own" Pattern (before you see the automated version on Day 15)

A classic hand-written CDC consumer pattern, applying a batch of upstream change events to a target table inside a streaming `foreachBatch`:

```python
def upsert_to_target(microbatch_df, batch_id):
    from delta.tables import DeltaTable
    target = DeltaTable.forName(spark, "target_table")
    (target.alias("t")
        .merge(microbatch_df.alias("s"), "t.id = s.id")
        .whenMatchedDelete(condition="s.operation = 'DELETE'")
        .whenMatchedUpdateAll(condition="s.operation != 'DELETE'")
        .whenNotMatchedInsertAll(condition="s.operation != 'DELETE'")
        .execute()
    )

(spark.readStream.table("cdc_source")
    .writeStream
    .foreachBatch(upsert_to_target)
    .option("checkpointLocation", "/chk/cdc_target")
    .start())
```

**What you have to handle manually here that Day 15's `AUTO CDC` does for you:**
- **Out-of-order events** — if two changes for the same key arrive in the same microbatch out of timestamp order, you must pre-sort/dedupe by a sequence column yourself before the `merge()` call, or you can apply an older event *after* a newer one and corrupt state.
- **SCD Type 2 windowing** — tracking `__START_AT`/`__END_AT` validity windows is entirely your own logic (Part 3 below).
- **Delete semantics** — deciding what a "delete" event means downstream (hard delete vs. soft-delete flag) is your own condition to write.

This is exactly why the exam objective in Section 1 (Day 15) frames `AUTO CDC` as "simplifying CDC" — it replaces this hand-rolled `foreachBatch` + `MERGE` pattern with a declarative one-liner that also handles ordering and SCD windowing for you.

---

## Part 3 — SCD Type 1 vs. Type 2, Implemented by Hand

### SCD Type 1 — overwrite in place, no history
```sql
MERGE INTO dim_customer AS t
USING customer_updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET t.email = s.email, t.address = s.address
WHEN NOT MATCHED THEN INSERT (customer_id, email, address) VALUES (s.customer_id, s.email, s.address);
```
Simple — the old value is gone the moment it's overwritten. Use when history genuinely doesn't matter (e.g., correcting a typo).

### SCD Type 2 — preserve full history with validity windows
Requires extra tracking columns: `effective_date`, `end_date`, `is_current`.

```sql
-- Step 1: close out the old "current" record if its tracked attributes changed
MERGE INTO dim_customer AS t
USING customer_updates AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.address != s.address OR t.email != s.email) THEN
  UPDATE SET t.end_date = s.effective_date, t.is_current = false;

-- Step 2: insert the new current record for anyone who was just closed out, or is brand new
INSERT INTO dim_customer (customer_id, email, address, effective_date, end_date, is_current)
SELECT s.customer_id, s.email, s.address, s.effective_date, NULL, true
FROM customer_updates s
LEFT ANTI JOIN dim_customer t
  ON s.customer_id = t.customer_id AND t.is_current = true AND t.email = s.email AND t.address = s.address;
```

**Exam trap:** SCD Type 2 by hand is a **two-statement pattern** (close the old row, then insert the new row) — a single `MERGE` cannot both update an existing row *and* insert a brand-new row **for the same logical key** in one pass when the "insert" needs data from the row that was just closed (like its new `effective_date`). This two-step nature is precisely the tedium `AUTO CDC ... STORED AS SCD TYPE 2` (Day 15) eliminates — it manages `__START_AT`/`__END_AT` for you automatically from a single declarative statement.

---

## Part 4 — Change Data Feed (CDF): Reading Row-Level Changes From a Delta Table

CDF lets you query **exactly which rows changed, how, and when** for a Delta table — instead of only being able to see the table's current (or a fully time-traveled) state.

### Enabling it
```sql
-- New table
CREATE TABLE sales.orders (order_id BIGINT, status STRING, amount DECIMAL(18,2))
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Existing table
ALTER TABLE sales.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- All new tables in a session
SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
```

**Exam trap:** CDF only captures changes made **after** it's enabled — enabling it on an existing table does **not** backfill historical change history for versions before that point.

### Reading changes — batch
```sql
-- SQL: table_changes(table_name, starting_version [, ending_version])
SELECT * FROM table_changes('sales.orders', 120, 125);
```
```python
# PySpark
changes_df = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 120)
    .option("endingVersion", 125)
    .table("sales.orders"))
```
You can specify version **or** timestamp bounds (`startingTimestamp`/`endingTimestamp`), inclusive on both ends. Omitting the ending bound reads through to the latest version.

### Reading changes — streaming
```python
(spark.readStream
    .option("readChangeFeed", "true")
    .table("sales.orders")
    .writeStream
    ...
)
```
**Default behavior:** when a CDF stream **first starts**, it returns the table's current snapshot as `INSERT` rows, then emits real future changes as change events from that point forward. Databricks recommends CDF + Structured Streaming together specifically because Structured Streaming automatically tracks which version has already been processed via its checkpoint — you don't manage `startingVersion` bookkeeping by hand.

### The extra columns CDF adds to every row
| Column | Meaning |
|---|---|
| `_change_type` | `insert`, `update_preimage` (the row's value *before* an update), `update_postimage` (the row's value *after* an update), or `delete` |
| `_commit_version` | The Delta table version the change belongs to |
| `_commit_timestamp` | When that commit happened |

**Exam trap:** an `UPDATE` produces **two** CDF rows per changed row — one `update_preimage`, one `update_postimage` — not a single "changed" row. A scenario asking "how would you compute what a value changed *from* and *to*" is pointing at joining/comparing these two row types for the same key within the same `_commit_version`.

---

## Part 5 — How CDF Specifically Addresses the Exam's Named Limitation

The exam objective is precise: *"address specific limitations of streaming tables and enhance latency."* Here's the exact mechanism:

**The limitation:** a Lakeflow Declarative Pipelines **streaming table** assumes its source is **append-only**. If the upstream source Delta table experiences `UPDATE`s or `DELETE`s (not just new appended rows), a plain streaming table read against it will **not correctly reflect** those mutations — it only ever sees new rows landing, not modifications to old ones (cross-reference Day 16's Streaming Table vs. Materialized View comparison).

**Without CDF, the "fix" is a Materialized View** — which handles updates/deletes correctly by **recomputing** the result, but recomputation is more expensive and higher-latency than an incremental append-only read.

**With CDF, you get a third option:** read the upstream table's **row-level change events** (including its updates and deletes) incrementally via `readChangeFeed`, and apply them downstream with a `MERGE` (Part 1/3 pattern) — this achieves **materialized-view-equivalent correctness** (updates/deletes are properly reflected) while keeping **streaming-table-equivalent low latency** (only the actual changed rows are processed, not a full recompute).

**Exam framing:** "A source table now receives occasional updates/deletes, and downstream latency requirements rule out a full materialized view recompute" is the canonical scenario pointing to **enabling CDF on the source and consuming it via `readChangeFeed` + `MERGE`**, not simply "switch to a materialized view" (too slow/expensive for the stated latency need) and not "keep using a plain streaming table" (incorrect — it can't see the updates/deletes at all).

---

## Part 6 — CDF's Relationship to Retention and VACUUM (cross-reference Days 9 & 19)

CDF change data is stored as small Parquet files (Databricks may also reconstruct some changes from existing `add`/`remove` actions when efficient to do so) governed by the **same log/file retention** as time travel. If `VACUUM` removes files past `delta.deletedFileRetentionDuration`, a `startingVersion` that depended on those files is **no longer readable** — you'll get an error rather than silently missing rows. A production CDF consumer that falls too far behind (e.g., a paused streaming job) can permanently lose its ability to resume from its last checkpoint if retention has already passed — this is why monitoring consumer lag matters operationally.

---

## Part 7 — Exam Traps Recap

1. `MERGE` throws an error (not a silent wrong result) when the `ON` condition matches multiple source rows to one target row — fix by deduplicating the source first.
2. `WHEN NOT MATCHED BY SOURCE` reacts to rows **missing from the source**, distinct from the more common `WHEN NOT MATCHED [BY TARGET]`.
3. Hand-written CDC/SCD via `foreachBatch` + `MERGE` requires you to manage event ordering and SCD2 windowing yourself — `AUTO CDC` (Day 15) automates exactly this.
4. SCD Type 2 by hand is fundamentally a **two-statement pattern** (close old row, then insert new row) — not a single `MERGE`.
5. CDF must be **explicitly enabled**, and it **never backfills** history from before it was turned on.
6. An `UPDATE` produces **two** CDF rows (`update_preimage` + `update_postimage`), not one.
7. CDF's exam-tested purpose: let a consumer get **materialized-view-level correctness** (sees updates/deletes) at **streaming-table-level latency** (incremental, not a full recompute) — this is the precise trade-off the objective is testing.
8. CDF version availability is bounded by the same retention/`VACUUM` rules as time travel (Day 9/19) — a lagging consumer can lose its resume point.

---

## Cross-References
- Day 9: Transaction log `add`/`remove` actions and retention settings that CDF's version-bounded reads depend on.
- Day 15: `AUTO CDC` — the declarative, automated replacement for the manual `foreachBatch`+`MERGE` CDC/SCD2 pattern taught here.
- Day 16: Streaming Tables vs. Materialized Views — the exact append-only limitation CDF is designed to work around.
- Day 19: `VACUUM` retention and its interaction with time-travel/CDF version availability.
- Day 26: Dimensional modeling and SCD patterns in a broader data-modeling context.
