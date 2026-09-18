# Day 13 — Auto Loader: Configuration, Schema Evolution, and the Quarantine Pattern

## Exam Objectives (Exam Guide, July 2026)

Continues **Section 2: Data Ingestion & Acquisition (7%)**:
- "Design and implement data ingestion pipelines to efficiently ingest a variety of data formats... from diverse sources such as message buses and cloud storage." *(Day 12 covered the format landscape and batch tools; this day is Auto Loader's own configuration in depth.)*

And the **classic-jobs half** of **Section 3: Data Transformation, Cleansing, and Quality (10%)**:
- "Develop a quarantining process for bad data with Lakeflow Spark Declarative Pipelines, **or Autoloader in classic jobs**." *(The Lakeflow Declarative Pipelines `@dlt.expect*` half of this bullet is Day 15/17's material — don't duplicate it here.)*

---

## Part 1 — Why Auto Loader Exists (vs. Day 12's Batch Tools)

| Tool | Discovery | Scale | Schema evolution |
|---|---|---|---|
| `spark.read` / `COPY INTO` (Day 12) | One-time listing at query time | Fine for bounded, moderate file counts | `COPY INTO`: none |
| `read_files()` batch (Day 12) | One-time listing | Same as above | Inference only, no evolution tracking |
| **Auto Loader (`cloudFiles`)** | **Incremental** — only sees genuinely new files each trigger, checkpoint-tracked, exactly-once | **Scales to millions of files** without re-listing everything each run | **Full inference AND evolution**, with a safety net (`_rescued_data`) for anything unexpected |

Auto Loader is a **Structured Streaming source** (`format("cloudFiles")`), so it can run continuously *or* be triggered on a schedule via `Trigger.AvailableNow` (Day 12) — it is not itself "always streaming."

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/main/bronze/_schemas/orders")
    .load("s3://landing/orders/"))
```

`cloudFiles.schemaLocation` is **required** whenever you want Auto Loader to infer and track schema (as opposed to supplying a fixed `.schema(...)`) — it's where the inferred schema and its evolution history are persisted between stream restarts.

---

## Part 2 — Schema Evolution Modes (`cloudFiles.schemaEvolutionMode`)

Auto Loader detects a genuinely new column mid-stream via an **`UnknownFieldException`**. Before throwing it, Auto Loader has already inferred the updated schema from the latest micro-batch and written it to `schemaLocation` — the exception is the signal that a **restart** is needed to pick up the new schema; existing column data types are never changed by this process.

| Mode | Behavior on a new column |
|---|---|
| **`addNewColumns`** (default when no schema is supplied) | Stream **fails** with `UnknownFieldException`; the new column is already merged into the schema at `schemaLocation`. Restarting the stream resumes with the new column active. |
| **`addNewColumnsWithTypeWidening`** | Same restart-required behavior as `addNewColumns`, **plus** automatically widens compatible type changes (`int`→`long`, `float`→`double`). Incompatible type changes (`int`→`string`) still go to `_rescued_data`. |
| **`rescue`** | The stream **never fails** for schema drift. New/unexpected columns are captured in `_rescued_data` instead of being added to the schema. |
| **`failOnNewColumns`** | Stream fails and **does not auto-restart** — you must manually update the schema or remove the offending file before it can proceed again. |
| **`none`** (default **only** when you supply an explicit `.schema(...)`) | New columns are silently ignored; nothing is rescued unless you separately set `rescuedDataColumn`. |

**Exam trap — the restart requirement:** `addNewColumns` is *not* "seamless, zero-downtime evolution" — the stream genuinely stops with an exception. **Databricks recommends running Auto Loader inside a Lakeflow Job configured to auto-restart on failure**, so the "seamlessness" comes from job-level retry, not from Auto Loader avoiding the failure. A scenario describing "the stream unexpectedly stopped after a new field appeared in the source, and it needs to resume automatically" is pointing at Job-level restart configuration, not a different Auto Loader setting.

**Exam trap — `failOnNewColumns` vs. `addNewColumns`:** both *fail* on a new column — the difference is whether restarting alone fixes it (`addNewColumns`: yes) or requires manual schema intervention first (`failOnNewColumns`: yes, manual step required). A scenario emphasizing "a governed table under a strict schema contract, new fields must never be silently absorbed" points to `failOnNewColumns`.

---

## Part 3 — The Rescued Data Column (`_rescued_data`)

This is arguably the single most exam-relevant Auto Loader behavior: **by default, every Auto Loader stream can capture data that doesn't cleanly fit the schema**, without ever failing the pipeline for a merely-malformed record.

**What lands in `_rescued_data`:**
- A field present in the incoming record but **missing from the current schema** (when in `rescue` mode, or a genuine type mismatch even in evolving modes)
- **Type mismatches** against the inferred/provided schema
- **Case mismatches** in column names
- Records that are **entirely empty structs** (`{}` in JSON, or a zero-field Avro record) — Parquet cannot represent an empty struct, so these are redirected here rather than dropped or erroring

**Format:** a JSON blob containing the rescued field(s) **plus the source file path** of the record — so you always know exactly which file a rescued value came from.

```python
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")   # can rename; on by default with this name
    .load(source_path))
```

**`schemaHints` interaction:** if you set `cloudFiles.schemaHints` to force a column to a specific type, Auto Loader does **not** cast existing data to match — it tells the reader to read the column *as* that type. Any value that doesn't fit is **rescued**, not silently coerced or dropped.

**Exam trap:** `_rescued_data` is not an error log you have to opt into after the fact — it's Auto Loader's **default safety net**, active automatically unless you've set `schemaEvolutionMode = "none"` without also setting `rescuedDataColumn` explicitly.

---

## Part 4 — File Discovery: Directory Listing vs. File Notification

| Mode | How it finds new files | Best for |
|---|---|---|
| **Directory listing** (`cloudFiles.useNotifications = false`, the default) | Lists the target directory via cloud storage list APIs each trigger; Databricks optimizes this with a flattened-response strategy | Fast to set up (only needs storage read access); fine for small-to-moderate file counts; **performance degrades as file counts grow** |
| **File notification** (`cloudFiles.useNotifications = true`) | Auto Loader provisions cloud-native event infrastructure (AWS: SNS+SQS; Azure: Event Grid+Queue Storage; GCP: Pub/Sub) so new-file events arrive directly, no repeated listing | **Recommended for high-throughput/production** — scales to millions of files/hour without the listing-cost growth curve |

**Exam trap:** you can **switch between the two modes across stream restarts and still keep exactly-once processing guarantees** — a scenario implying you'd need to rebuild the pipeline from scratch to change discovery modes is testing whether you know this is a safe, supported operation.

---

## Part 5 — Rate Limiting a Trigger

```python
.option("cloudFiles.maxFilesPerTrigger", "1000")
.option("cloudFiles.maxBytesPerTrigger", "10g")
```
When **both** are set, Auto Loader processes up to the **lower** of the two limits in a given micro-batch — useful for controlling cluster sizing/cost during an initial large backfill, often combined with `Trigger.AvailableNow` (Day 12) to process a big backlog in bounded, predictable chunks rather than one enormous first micro-batch.

---

## Part 6 — The Quarantine Pattern: Auto Loader in Classic Jobs

This is the exact "Autoloader in classic jobs" half of the Section 3 objective bullet. Since `_rescued_data` already flags every problematic record, the classic (non-Lakeflow) quarantine pattern is simply: **split the stream on whether `_rescued_data` is populated.**

```python
raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(source_path))

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

**Why `foreachBatch` here:** it's the standard mechanism for **writing one streaming micro-batch to multiple sinks** with custom per-batch logic — exactly what a two-destination (clean/quarantine) split requires.

**Contrast with the Lakeflow Declarative Pipelines version (forward reference, Day 15/17):** inside a Declarative Pipeline, the equivalent pattern uses `@dlt.expect_or_drop`/`@dlt.expect_all_or_drop` constraints instead of manually filtering on `_rescued_data` — same *goal* (separate good data from bad), different *mechanism* (declarative constraints vs. imperative `foreachBatch` routing). A scenario naming "Lakeflow Declarative Pipelines" wants the expectations answer; a scenario naming "classic job" or "Structured Streaming" wants this `_rescued_data`/`foreachBatch` answer.

---

## Part 7 — Exam Traps Recap

1. `cloudFiles.schemaLocation` is **required** for inference/evolution tracking — it's where evolving schema history persists across restarts.
2. `addNewColumns` (the default with no explicit schema) still **fails the stream** on a new column — restart resumes with the new schema; it's not zero-downtime by itself. Pair with a Job configured to auto-restart.
3. `failOnNewColumns` fails **and stays failed** until a manual schema update or file removal — the strict-contract option.
4. `_rescued_data` is Auto Loader's **default safety net** — captures missing fields, type mismatches, case mismatches, and empty-struct records as a JSON blob plus source file path.
5. `schemaHints` **does not cast** data — mismatches against the hinted type are rescued, not coerced.
6. Directory listing (default) vs. file notification (recommended at scale) — **switchable at any time** without breaking exactly-once guarantees.
7. `maxFilesPerTrigger` + `maxBytesPerTrigger` together → Auto Loader honors whichever limit is **lower**.
8. The classic-jobs quarantine pattern = filter on `_rescued_data IS NULL`/`IS NOT NULL` inside `foreachBatch`, writing to two separate sinks — the Lakeflow Declarative Pipelines equivalent uses `@dlt.expect*` instead (Day 15/17).

---

## Cross-References
- Day 12: Batch ingestion tools (`COPY INTO`, `read_files()`) and `Trigger.AvailableNow` — Auto Loader is the incremental, schema-evolving upgrade over those for continuous/high-volume ingestion.
- Day 14: Structured Streaming triggers, watermarking, and `foreachBatch` mechanics in more depth.
- Day 15/17: `@dlt.expect_or_drop`/`expect_all_or_fail` — the Lakeflow Declarative Pipelines half of the quarantine objective bullet.
- Day 9: Schema enforcement/evolution (`mergeSchema`) at the Delta table level — the sink-side counterpart to Auto Loader's source-side schema evolution.
