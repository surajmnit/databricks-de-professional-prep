# Day 13 — Cheat Sheet: Auto Loader Configuration, Schema Evolution, and Quarantine

## Why Auto Loader Over Day 12's Batch Tools

| | `spark.read`/`COPY INTO` | Auto Loader (`cloudFiles`) |
|---|---|---|
| Discovery | One-time listing at query time | Incremental, checkpoint-tracked |
| Scale | Bounded/moderate file counts | Millions of files, no re-listing |
| Schema evolution | None / inference only | Full inference **and** evolution + `_rescued_data` safety net |

Auto Loader is a **Structured Streaming source** — not "always streaming." Run continuously, or on a schedule via `Trigger.AvailableNow`.

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/bronze/_schemas/orders")
    .load("s3://landing/orders/"))
```

**`cloudFiles.schemaLocation` is required** for inference/evolution — omit it and the stream fails to start.

---

## `cloudFiles.schemaEvolutionMode`

| Mode | New column? | Type mismatch? | Notes |
|---|---|---|---|
| `addNewColumns` (default, no schema) | **Fails** (`UnknownFieldException`), resumes on restart | Rescued | Schema at `schemaLocation` already updated *before* the exception |
| `addNewColumnsWithTypeWidening` | Same as above | Rescued unless widen-compatible | Auto-widens `int→long`, `float→double`; **requires DBR 16.4+** |
| `rescue` | Never fails — rescued | Rescued | Schema never evolves; safest for uptime |
| `failOnNewColumns` | **Fails, stays failed** | Rescued | Manual schema update or file removal required — the strict-contract option |
| `none` (default **only** with explicit `.schema(...)`) | Silently ignored | Not rescued unless `rescuedDataColumn` set | Stream never fails on schema changes |

**Exam trap:** `addNewColumns` ≠ zero-downtime. It genuinely fails the stream; "seamless" evolution comes from a **Lakeflow Job auto-restart policy**, not from Auto Loader avoiding failure.

**Exam trap:** `addNewColumns` vs `failOnNewColumns` — both fail on a new column. Difference is whether a plain **restart** fixes it (`addNewColumns`: yes) or **manual intervention** is required first (`failOnNewColumns`: yes).

---

## `_rescued_data` — The Default Safety Net

Captures (as a JSON blob + source file path):
- Fields missing from the current schema
- Type mismatches vs. inferred/provided schema
- Column name case mismatches
- Entirely empty structs (Parquet can't represent them)

```python
.option("rescuedDataColumn", "_rescued_data")   # on by default with this name
```

**`schemaHints` does NOT cast.** It tells the reader what type to expect; anything that doesn't fit is **rescued**, never coerced or silently dropped.

**Active by default** unless `schemaEvolutionMode = "none"` *and* you haven't set `rescuedDataColumn` explicitly.

---

## File Discovery

| Mode | Mechanism | Best for |
|---|---|---|
| Directory listing (default, `useNotifications=false`) | Lists directory via storage APIs each trigger | Fast setup; small/moderate file counts |
| File notification (`useNotifications=true`) | Cloud-native events (AWS SNS+SQS / Azure Event Grid+Queue Storage / GCP Pub/Sub) | Production scale — millions of files/hour |

**Exam trap:** switching between the two modes across restarts is **safe** and preserves exactly-once — no pipeline rebuild required.

---

## Rate Limiting

```python
.option("cloudFiles.maxFilesPerTrigger", "1000")
.option("cloudFiles.maxBytesPerTrigger", "10g")
```
Both set → Auto Loader honors whichever limit is **lower** per micro-batch. Pair with `Trigger.AvailableNow` for bounded backfill chunks.

---

## Classic-Jobs Quarantine Pattern (Section 3 objective)

```python
def route_batch(batch_df, batch_id):
    clean = batch_df.filter("_rescued_data IS NULL")
    bad   = batch_df.filter("_rescued_data IS NOT NULL")
    clean.write.format("delta").mode("append").saveAsTable("bronze.orders_clean")
    bad.write.format("delta").mode("append").saveAsTable("bronze.orders_quarantine")

(raw.writeStream
    .foreachBatch(route_batch)
    .option("checkpointLocation", "/chk/orders_quarantine_pattern")
    .start())
```
`foreachBatch` = the mechanism for writing **one micro-batch to multiple sinks**.

**Classic job vs. Lakeflow Declarative Pipeline — same goal, different mechanism:**
| Context | Quarantine mechanism |
|---|---|
| Classic job / Structured Streaming | Filter on `_rescued_data IS NULL`/`NOT NULL` in `foreachBatch` |
| Lakeflow Declarative Pipeline | `@dlt.expect_or_drop` / `@dlt.expect_all_or_drop` (Day 15/17) |

---

## Exam Trap Shortlist

1. `schemaLocation` required for any inference/evolution — no exceptions.
2. `addNewColumns` fails the stream — restart resumes with new schema, but it's not automatic without a Job retry policy.
3. `failOnNewColumns` ≠ `addNewColumns` — one auto-resumes, the other needs manual fixing.
4. `_rescued_data` is on by default — not an opt-in error log.
5. `schemaHints` shapes reads, never casts existing data.
6. Discovery mode (listing ↔ notification) is switchable anytime without breaking exactly-once.
7. Two trigger-rate options together → **lower** limit wins.
8. `addNewColumnsWithTypeWidening` needs **DBR 16.4+**.
9. Auto Loader is a streaming *source* — `Trigger.AvailableNow` makes it schedule-friendly, not always-on.
10. Classic-job quarantine = `_rescued_data` + `foreachBatch`; Lakeflow quarantine = `@dlt.expect*` — match the mechanism to the stated execution context.
