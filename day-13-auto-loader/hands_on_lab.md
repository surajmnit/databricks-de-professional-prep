# Day 13 — Hands-On Lab: Auto Loader Schema Evolution, Rescue Mode, and Quarantine

## Lab Objectives

1. Configure Auto Loader with `cloudFiles.schemaLocation` and observe schema inference on first read.
2. Trigger a genuine `UnknownFieldException` by introducing a new column mid-stream, then confirm a restart resumes cleanly with the evolved schema.
3. Switch to `schemaEvolutionMode = "rescue"` and observe `_rescued_data` capturing a type mismatch and a new field without failing the stream.
4. Implement the classic-jobs quarantine pattern with `foreachBatch`, writing to a clean table and a quarantine table from one stream.
5. Observe `maxFilesPerTrigger`/`maxBytesPerTrigger` rate limiting across multiple micro-batches.
6. Understand `schemaHints` behavior — hints direct how a column is read, they don't cast existing data.

**Environment note:** Auto Loader works against any storage Spark can read from, including DBFS paths — you don't need real cloud storage to complete Steps 1–5. Step 6 (file notification mode) needs actual cloud-native event infrastructure (SNS+SQS, Event Grid+Queue Storage, or Pub/Sub) and **cannot** be run on Community Edition or a workspace without cloud admin access to provision those resources — that step is read-only reference material.

---

## Step 1 — Configure Auto Loader with Schema Inference

```python
import json

base_path = "/tmp/day13_autoloader"
source_path = f"{base_path}/source"
schema_path = f"{base_path}/_schema"
checkpoint_path = f"{base_path}/_checkpoint"

dbutils.fs.rm(base_path, recurse=True)
dbutils.fs.mkdirs(source_path)

# Batch 1: three fields, no surprises
initial_records = [
    {"order_id": 1, "customer": "alice", "amount": 100.0},
    {"order_id": 2, "customer": "bob", "amount": 250.0},
]
dbutils.fs.put(
    f"{source_path}/batch_1.json",
    "\n".join(json.dumps(r) for r in initial_records),
    overwrite=True,
)

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path))

query = (df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("main.default.orders_autoloader_day13"))
query.awaitTermination()

spark.sql("SELECT * FROM main.default.orders_autoloader_day13").show()
```

**What to observe:** check `dbutils.fs.ls(schema_path)` — Auto Loader has written an inferred schema file here, independent of the checkpoint. This is the file that gets updated (not overwritten with unrelated data) whenever a new column is detected.

---

## Step 2 — 💥 Break It on Purpose: Trigger `UnknownFieldException`

```python
# Introduce a genuinely new column: "region"
new_records = [
    {"order_id": 3, "customer": "carol", "amount": 300.0, "region": "US"},
]
dbutils.fs.put(
    f"{source_path}/batch_2.json",
    "\n".join(json.dumps(r) for r in new_records),
    overwrite=True,
)

try:
    query_fail = (df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable("main.default.orders_autoloader_day13"))
    query_fail.awaitTermination()
except Exception as e:
    print("Expected — UnknownFieldException on new column 'region':")
    print(str(e)[:400])
```

**Predict then verify:** has `schema_path` already been updated to include `region`, even though the stream just failed? Check:
```python
spark.read.json(f"{schema_path}").show(truncate=False)  # or inspect the schema log file directly
```
**Answer:** Yes — per the notes, Auto Loader infers the updated schema and writes it to `schemaLocation` *before* raising the exception. The exception exists purely to force a restart, not because the schema update itself failed.

### 2b. Restart and confirm automatic resumption

```python
# Re-create the read stream — same schemaLocation, same checkpoint
df_restarted = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .load(source_path))

query_resumed = (df_restarted.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("main.default.orders_autoloader_day13"))
query_resumed.awaitTermination()

spark.sql("SELECT * FROM main.default.orders_autoloader_day13").show()
```

**What to observe:** `batch_2.json`'s row now lands successfully with `region = 'US'`; the two original rows show `region = NULL` (existing column data types were never touched — only a new column was appended). This is the exact mechanic behind "the stream unexpectedly stopped after a new field appeared" — in production this restart would be handled by a Lakeflow Job's automatic-retry policy, not a person manually re-running a cell.

---

## Step 3 — Switch to Rescue Mode: No Failures, Ever

```python
rescue_source = f"{base_path}/source_rescue"
rescue_schema_path = f"{base_path}/_schema_rescue"
rescue_checkpoint = f"{base_path}/_checkpoint_rescue"
dbutils.fs.mkdirs(rescue_source)

dbutils.fs.put(f"{rescue_source}/batch_1.json", json.dumps({"id": 1, "amount": 50.0}), overwrite=True)

df_rescue = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", rescue_schema_path)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(rescue_source))

(df_rescue.writeStream
    .format("delta")
    .option("checkpointLocation", rescue_checkpoint)
    .trigger(availableNow=True)
    .toTable("main.default.rescue_demo_day13")
).awaitTermination()

# Now: a type mismatch (id as a string) AND a brand-new field, in the same record
dbutils.fs.put(
    f"{rescue_source}/batch_2.json",
    json.dumps({"id": "not-a-number", "amount": 75.0, "extra_field": "surprise"}),
    overwrite=True,
)

(df_rescue.writeStream
    .format("delta")
    .option("checkpointLocation", rescue_checkpoint)
    .trigger(availableNow=True)
    .toTable("main.default.rescue_demo_day13")
).awaitTermination()  # Does NOT raise — this is the whole point of rescue mode

spark.sql("SELECT * FROM main.default.rescue_demo_day13").show(truncate=False)
```

**What to observe:** the stream never fails. The second row shows `id = NULL` (the mismatched string value didn't fit the inferred `LONG` type) and a populated `_rescued_data` column containing both the mismatched `id` value and the entirely new `extra_field`, plus the source file path — exactly matching Part 3 of the notes.

---

## Step 4 — The Classic-Jobs Quarantine Pattern

```python
def route_batch(batch_df, batch_id):
    clean = batch_df.filter("_rescued_data IS NULL")
    bad = batch_df.filter("_rescued_data IS NOT NULL")

    print(f"Batch {batch_id}: {clean.count()} clean rows, {bad.count()} quarantined rows")

    clean.write.format("delta").mode("append").saveAsTable("main.default.orders_clean_day13")
    bad.write.format("delta").mode("append").saveAsTable("main.default.orders_quarantine_day13")

query_quarantine = (df_rescue.writeStream
    .foreachBatch(route_batch)
    .option("checkpointLocation", f"{base_path}/_checkpoint_quarantine")
    .trigger(availableNow=True)
    .start())
query_quarantine.awaitTermination()

print("Clean table:")
spark.sql("SELECT * FROM main.default.orders_clean_day13").show()
print("Quarantine table:")
spark.sql("SELECT * FROM main.default.orders_quarantine_day13").show(truncate=False)
```

**What to observe:** the same source stream now lands in two separate tables based purely on whether `_rescued_data` is populated — no `@dlt.expect*` constraints involved, since this is the classic-Structured-Streaming half of the quarantine objective bullet, not the Lakeflow Declarative Pipelines half (Day 15/17).

---

## Step 5 — Rate Limiting a Trigger

```python
rate_source = f"{base_path}/source_rate"
dbutils.fs.mkdirs(rate_source)

# Write 5 separate small files to simulate 5 files landing
for i in range(5):
    dbutils.fs.put(f"{rate_source}/file_{i}.json", json.dumps({"n": i}), overwrite=True)

batch_sizes = []

def record_batch_size(batch_df, batch_id):
    n = batch_df.count()
    batch_sizes.append(n)
    print(f"Micro-batch {batch_id}: {n} rows")

df_rate = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{base_path}/_schema_rate")
    .option("cloudFiles.maxFilesPerTrigger", "1")   # process one file at a time
    .load(rate_source))

(df_rate.writeStream
    .foreachBatch(record_batch_size)
    .option("checkpointLocation", f"{base_path}/_checkpoint_rate")
    .trigger(availableNow=True)
    .start()
).awaitTermination()

print(f"Total micro-batches: {len(batch_sizes)} (expect 5, one file each)")
```

**What to observe:** even though `Trigger.AvailableNow` processes the *entire* backlog before stopping, it does so across **multiple micro-batches** honoring `maxFilesPerTrigger` — this is exactly the "bounded, predictable chunks" behavior from Part 5 of the notes, useful for controlling cluster sizing during a large backfill.

**Break it on purpose:** set both `cloudFiles.maxFilesPerTrigger = "1"` and `cloudFiles.maxBytesPerTrigger = "1g"` together, then reason about which one actually governs a micro-batch of five tiny files. **Answer:** the file-count limit (1) is far more restrictive than the byte limit here, so Auto Loader honors the **lower** effective limit — you'd still see 5 micro-batches, not fewer.

---

## Step 6 — (Read-Only Reference) Directory Listing vs. File Notification

This step cannot be executed without real cloud credentials and permissions to provision event infrastructure — read through it rather than running it.

```python
# Directory listing (default) — no extra setup, but lists the whole directory each trigger
df_listing = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.useNotifications", "false")   # default
    .load(source_path))

# File notification — Auto Loader provisions cloud-native event infra on first run
df_notification = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.region", "us-east-1")   # required for notification mode on AWS
    .load(source_path))
```

**What to understand even without running it:** switching `cloudFiles.useNotifications` between `true`/`false` across stream restarts (same `checkpointLocation`) is a **safe, supported** operation that preserves exactly-once guarantees — you are not required to rebuild the pipeline to change discovery modes.

---

## Stretch Task — `schemaHints` Does Not Cast

Force a column to a specific type via `schemaHints`, then feed it a value that doesn't fit, and confirm the value is rescued rather than coerced or silently dropped.

```python
hints_source = f"{base_path}/source_hints"
dbutils.fs.mkdirs(hints_source)
dbutils.fs.put(f"{hints_source}/batch_1.json", json.dumps({"id": 1, "score": "ninety-five"}), overwrite=True)

df_hints = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{base_path}/_schema_hints")
    .option("cloudFiles.schemaHints", "score DOUBLE")   # force score to DOUBLE
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(hints_source))

(df_hints.writeStream
    .format("delta")
    .option("checkpointLocation", f"{base_path}/_checkpoint_hints")
    .trigger(availableNow=True)
    .toTable("main.default.hints_demo_day13")
).awaitTermination()

spark.sql("SELECT * FROM main.default.hints_demo_day13").show(truncate=False)
```

**Predict then verify:** does `score` get cast from `"ninety-five"` to some numeric approximation, or dropped silently? **Answer:** neither — `score` shows `NULL`, and the literal string `"ninety-five"` appears inside `_rescued_data`. `schemaHints` tells the reader what type to *expect*, not how to coerce values that don't fit it.

---

## Lab Checklist

- [ ] Configured Auto Loader with `cloudFiles.schemaLocation` and confirmed schema inference
- [ ] Triggered a real `UnknownFieldException` and confirmed the schema was already updated before the exception was raised
- [ ] Restarted the stream and confirmed the new column resumed processing automatically
- [ ] Switched to `rescue` mode and confirmed a type mismatch + new field both land in `_rescued_data` without failing the stream
- [ ] Implemented the classic-jobs quarantine pattern with `foreachBatch`, splitting into clean/quarantine tables
- [ ] Observed `maxFilesPerTrigger` producing multiple micro-batches under `Trigger.AvailableNow`
- [ ] (Read-only) Reviewed the directory listing vs. file notification configuration difference
- [ ] (Stretch) Confirmed `schemaHints` rescues non-conforming values instead of casting or dropping them

---

## Cross-References
- Day 12: `Trigger.AvailableNow` mechanics and the batch-ingestion tool comparison Auto Loader improves on.
- Day 14: Structured Streaming triggers, watermarking, and `foreachBatch` in more depth.
- Day 15/17: The Lakeflow Declarative Pipelines `@dlt.expect_or_drop` equivalent of Step 4's quarantine pattern.
- Day 9: `mergeSchema` at the Delta table (sink) level — the write-side counterpart to Auto Loader's read-side schema evolution.
