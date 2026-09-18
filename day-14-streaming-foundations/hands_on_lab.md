# Day 14 — Hands-On Lab: Structured Streaming Triggers, Watermarking, and State

## Lab Objectives

1. Observe the micro-batch execution model in the Spark UI's Structured Streaming tab.
2. Compare `Trigger.AvailableNow()` against the deprecated `Trigger.Once()` and confirm which one actually honors `maxFilesPerTrigger`.
3. Reproduce the "append mode + aggregation without a watermark" failure, then fix it.
4. Build a windowed aggregation with a watermark and prove that genuinely late data gets dropped once the watermark has advanced past it.
5. Observe state-store size via `lastProgress` and understand why unbounded key cardinality with no watermark is a real production risk.
6. Build a stream-stream join with watermarks on both sides, then break it on purpose without watermarks and observe that Spark does **not** block the query at start (unlike the append-mode case).
7. Confirm checkpoint-based exactly-once behavior across a query restart.

**Environment note:** Every step uses either the built-in `rate` source or plain local/DBFS JSON files — no cloud storage, Kafka, or Auto Loader required. Fully runnable on Community Edition.

---

## Step 1 — Observe the Micro-Batch Model

```python
from pyspark.sql import functions as F
import time

stream_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
stream_df.printSchema()  # timestamp: TIMESTAMP, value: LONG

query = (stream_df.writeStream
    .format("memory")
    .queryName("rate_demo")
    .outputMode("append")
    .start())

time.sleep(10)
spark.sql("SELECT * FROM rate_demo ORDER BY timestamp DESC LIMIT 5").show()
query.stop()
```

**What to observe:** Open the Spark UI's **Structured Streaming** tab and click into `rate_demo`. Each row in the batch history is one micro-batch — click one and you land on an ordinary Spark **Jobs/Stages/Tasks** view, exactly like a batch job (Day 4/5). The "Input Rate" and "Process Rate" graphs are streaming-specific, but everything below the batch level is unchanged batch Spark.

---

## Step 2 — `Trigger.AvailableNow()` vs. the Deprecated `Trigger.Once()`

```python
import json
from pyspark.sql.types import StructType, StructField, IntegerType

base = "/tmp/day14_streaming"
source = f"{base}/source"
dbutils.fs.rm(base, recurse=True)
dbutils.fs.mkdirs(source)

for i in range(5):
    dbutils.fs.put(f"{source}/file_{i}.json", json.dumps({"id": i, "val": i * 10}), overwrite=True)

schema = StructType([StructField("id", IntegerType()), StructField("val", IntegerType())])

file_stream = (spark.readStream
    .format("json")
    .schema(schema)
    .option("maxFilesPerTrigger", 1)   # process one file per micro-batch
    .load(source))
```

### 2a. `Trigger.AvailableNow()` — honors `maxFilesPerTrigger`

```python
batch_log_available = []

def log_available(batch_df, batch_id):
    n = batch_df.count()
    batch_log_available.append((batch_id, n))
    print(f"[AvailableNow] Batch {batch_id}: {n} rows")

q_available = (file_stream.writeStream
    .foreachBatch(log_available)
    .option("checkpointLocation", f"{base}/_chk_availablenow")
    .trigger(availableNow=True)
    .start())
q_available.awaitTermination()

print(f"Total micro-batches (AvailableNow): {len(batch_log_available)}")  # expect 5
```

### 2b. `Trigger.Once()` — ignores `maxFilesPerTrigger`

```python
batch_log_once = []

def log_once(batch_df, batch_id):
    n = batch_df.count()
    batch_log_once.append((batch_id, n))
    print(f"[Once] Batch {batch_id}: {n} rows")

q_once = (file_stream.writeStream
    .foreachBatch(log_once)
    .option("checkpointLocation", f"{base}/_chk_once")   # separate checkpoint — fresh comparison
    .trigger(once=True)
    .start())
q_once.awaitTermination()

print(f"Total micro-batches (Once): {len(batch_log_once)}")  # expect 1
```

**Predict then verify:** with `maxFilesPerTrigger=1` set on the same source, why does `AvailableNow()` produce 5 micro-batches while `Once()` produces only 1? **Answer:** `Trigger.Once()` forces the entire currently-available backlog through a single micro-batch regardless of rate-limiting options — this is exactly why it was deprecated in favor of `AvailableNow()`, which respects `maxFilesPerTrigger`/`maxBytesPerTrigger` and spreads the same backlog across bounded batches.

---

## Step 3 — 💥 Break It on Purpose: Append Mode Without a Watermark

```python
events = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .withColumnRenamed("timestamp", "event_time"))

windowed = events.groupBy(F.window("event_time", "10 seconds")).count()

try:
    bad_query = (windowed.writeStream
        .format("memory")
        .queryName("bad_append")
        .outputMode("append")   # no watermark defined on a streaming aggregation
        .start())
except Exception as e:
    print("Expected — append mode on an aggregation without a watermark:")
    print(str(e)[:400])
```

**What to observe:** the query fails to even **start** — this is an `AnalysisException` at query-planning time, not a runtime error partway through. Spark refuses to run it because it has no way to know when a window is "final" and safe to append exactly once.

**Fix it:**

```python
windowed_fixed = (events
    .withWatermark("event_time", "10 seconds")   # declared BEFORE the aggregation
    .groupBy(F.window("event_time", "10 seconds"))
    .count())

good_query = (windowed_fixed.writeStream
    .format("memory")
    .queryName("good_append")
    .outputMode("append")
    .start())

time.sleep(30)
spark.sql("SELECT * FROM good_append ORDER BY window DESC").show(truncate=False)
good_query.stop()
```

**What to observe:** the query now starts successfully, but a given window's row only appears in `good_append` once the watermark has advanced far enough past that window's end to consider it final — expect a short delay before the first row shows up.

---

## Step 4 — Watermarks Actually Dropping Late Data

This step uses hand-crafted event times (not the `rate` source's auto-generated ones) so you can control exactly which record is "late."

```python
from datetime import datetime, timedelta
from pyspark.sql.types import StringType
from pyspark.sql.functions import col

late_source = f"{base}/late_demo/source"
dbutils.fs.mkdirs(late_source)

t0 = datetime(2026, 1, 1, 12, 0, 0)

def rec(offset_sec, id_):
    return {"id": id_, "event_time": (t0 + timedelta(seconds=offset_sec)).isoformat()}

# Batch 1: on-time events spanning two 10-second windows: [0,10) and [10,20)
dbutils.fs.put(f"{late_source}/b1.json", "\n".join(
    json.dumps(r) for r in [rec(0, 1), rec(5, 2), rec(12, 3)]
), overwrite=True)

schema2 = StructType([StructField("id", IntegerType()), StructField("event_time", StringType())])

late_stream = (spark.readStream.format("json").schema(schema2).load(late_source)
    .withColumn("event_time", col("event_time").cast("timestamp")))

late_windowed = (late_stream
    .withWatermark("event_time", "10 seconds")
    .groupBy(F.window("event_time", "10 seconds"))
    .count())

q_late = (late_windowed.writeStream
    .format("memory")
    .queryName("late_demo")
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .start())

time.sleep(8)
print("--- After batch 1 ---")
spark.sql("SELECT * FROM late_demo ORDER BY window").show(truncate=False)

# Batch 2: an event far ahead in time — this is what pushes the watermark forward
dbutils.fs.put(f"{late_source}/b2.json", json.dumps(rec(60, 4)), overwrite=True)
time.sleep(8)
print("--- After batch 2 (watermark now ~50s) ---")
spark.sql("SELECT * FROM late_demo ORDER BY window").show(truncate=False)

# Batch 3: a genuinely late event for the FIRST window (event_time = 2s, watermark is ~50s)
dbutils.fs.put(f"{late_source}/b3.json", json.dumps(rec(2, 5)), overwrite=True)
time.sleep(8)
print("--- After batch 3 (late event for window [0,10)) ---")
spark.sql("SELECT * FROM late_demo ORDER BY window").show(truncate=False)

q_late.stop()
```

**Predict then verify:** does the count for the `[00:00:00, 00:00:10)` window change after batch 3 lands? **Answer:** No — after batch 2, the watermark advanced to roughly `60s - 10s = 50s`, well past that window's close. The batch-3 event (`event_time = 2s`) arrives after the watermark has already passed its window, so Spark drops it from that window's aggregate rather than updating an already-finalized count. This is watermarking's core guarantee in action: bounded lateness tolerance, not unlimited correction.

---

## Step 5 — Observe State-Store Size via `lastProgress`

```python
no_wm = (spark.readStream.format("rate").option("rowsPerSecond", 20).load()
    .groupBy((F.col("value") % 50).alias("bucket"))
    .count())

q_state = (no_wm.writeStream
    .format("memory")
    .queryName("state_growth_demo")
    .outputMode("update")
    .start())

time.sleep(15)
progress = q_state.lastProgress
print(progress["stateOperators"])   # numRowsTotal = current state store size (rows of state held)
q_state.stop()
```

**What to observe:** `numRowsTotal` reflects how many distinct groups' state Spark is currently holding. Here it plateaus quickly because the key space (`value % 50`) is artificially bounded to 50 buckets. In production, grouping by a genuinely unbounded key (a raw user ID, a session ID with no watermark to expire old sessions) means this number climbs **without limit** for the life of the query — the exact "state grew until the executor OOMed" incident the exam tests, and the fix is a watermark (bound retention) or the RocksDB state store provider (bound in-heap footprint), not more executor memory as a first move.

---

## Step 6 — Stream-Stream Join With Watermarks, Then Break It

### 6a. Correct version — watermarks on both sides + a time-range join condition

```python
orders = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .selectExpr("value as order_id", "timestamp as order_time")
    .withWatermark("order_time", "30 seconds")
    .alias("o"))

shipments = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .selectExpr("value as order_id", "timestamp as ship_time")
    .withWatermark("ship_time", "30 seconds")
    .alias("s"))

joined = orders.join(
    shipments,
    F.expr("""
        o.order_id = s.order_id AND
        s.ship_time BETWEEN o.order_time AND o.order_time + INTERVAL 20 SECONDS
    """)
)

q_join = (joined.writeStream
    .format("memory")
    .queryName("join_demo")
    .outputMode("append")
    .start())

time.sleep(15)
spark.sql("SELECT * FROM join_demo LIMIT 10").show(truncate=False)
q_join.stop()
```

**What to observe:** matched rows appear as both sides advance in lockstep. The dual watermarks plus the `BETWEEN` time-range condition together bound how long Spark buffers an unmatched row from either side waiting for its partner.

### 6b. 💥 Break it on purpose — remove both watermarks

```python
orders_no_wm = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .selectExpr("value as order_id", "timestamp as order_time").alias("o"))

shipments_no_wm = (spark.readStream.format("rate").option("rowsPerSecond", 5).load()
    .selectExpr("value as order_id", "timestamp as ship_time").alias("s"))

joined_unbounded = orders_no_wm.join(shipments_no_wm, F.expr("o.order_id = s.order_id"))

q_unbounded = (joined_unbounded.writeStream
    .format("memory")
    .queryName("unbounded_join_demo")
    .outputMode("append")
    .start())

time.sleep(10)
print("Query is still RUNNING — no exception was raised.")
q_unbounded.stop()
```

**What to observe — the actual trap:** unlike Step 3's append-mode-aggregation-without-a-watermark, Spark does **not** refuse to start this query. It runs quite happily, silently buffering every unmatched row from both sides forever, since nothing tells it when it's safe to give up waiting for a match. This is the more dangerous failure mode precisely because there's no upfront error to catch it — the state simply grows for the life of the query, unnoticed until memory pressure shows up in production.

---

## Step 7 — Checkpoint Recovery: Confirm Exactly-Once Across a Restart

```python
chk_path = f"{base}/_chk_recovery"
recovery_source = f"{base}/recovery_source"
dbutils.fs.rm(chk_path, recurse=True)
dbutils.fs.mkdirs(recovery_source)

schema3 = StructType([StructField("id", IntegerType())])
rec_stream = spark.readStream.format("json").schema(schema3).load(recovery_source)

dbutils.fs.put(f"{recovery_source}/f1.json", json.dumps({"id": 1}), overwrite=True)

q1 = (rec_stream.writeStream
    .format("delta")
    .option("checkpointLocation", chk_path)
    .trigger(availableNow=True)
    .toTable("main.default.recovery_demo_day14"))
q1.awaitTermination()

print("--- After first run ---")
spark.sql("SELECT * FROM main.default.recovery_demo_day14").show()

# Land a NEW file, then "restart" using the SAME checkpoint location
dbutils.fs.put(f"{recovery_source}/f2.json", json.dumps({"id": 2}), overwrite=True)

q2 = (rec_stream.writeStream
    .format("delta")
    .option("checkpointLocation", chk_path)   # same checkpoint — this is the "restart"
    .trigger(availableNow=True)
    .toTable("main.default.recovery_demo_day14"))
q2.awaitTermination()

print("--- After second run (simulated restart) ---")
spark.sql("SELECT * FROM main.default.recovery_demo_day14").show()
# Expect exactly 2 rows total — f1.json was NOT reprocessed
```

**What to observe:** the table ends up with exactly two rows (`id=1` and `id=2`), not three or four. The checkpoint recorded that `f1.json` was already committed, so the "restarted" query only picked up the genuinely new file — this is the offset-tracking half of exactly-once in action.

---

## Stretch Task

Combine Steps 3 and 4: build a windowed aggregation in `update` mode (not `append`) over the hand-crafted late-data source, and confirm that `update` mode does **not** require a watermark to start (unlike `append`) — then add a watermark anyway and observe how it still bounds state growth even though it's not strictly required for the query to run. Write down, in your own words, the difference between "a watermark Spark enforces before letting you start" and "a watermark you should still add for operational health."

---

## Lab Checklist

- [ ] Observed a micro-batch's Jobs/Stages/Tasks in the Spark UI's Structured Streaming tab
- [ ] Confirmed `AvailableNow()` honors `maxFilesPerTrigger`, `Once()` does not
- [ ] Reproduced the append-mode-without-watermark `AnalysisException` and fixed it
- [ ] Proved a genuinely late event gets dropped once the watermark has advanced past its window
- [ ] Inspected state-store size via `lastProgress["stateOperators"]`
- [ ] Built a correct stream-stream join with watermarks + time-range condition
- [ ] Confirmed an unwatermarked stream-stream join starts without error (the dangerous silent case)
- [ ] Verified checkpoint-based exactly-once behavior across a simulated restart
- [ ] (Stretch) Compared watermark-required-to-start vs. watermark-required-for-good-hygiene

---

## Cross-References

- Day 4/5: Jobs/Stages/Tasks — what you see inside each micro-batch in the Structured Streaming UI.
- Day 9: Delta's transaction log — why Delta as a sink makes Step 7's exactly-once behavior straightforward.
- Day 12/13: `Trigger.AvailableNow()` and Auto Loader — this lab's file-source triggers apply directly to a real `cloudFiles` stream.
- Day 15/16: Lakeflow Declarative Pipelines — how these same trigger/watermark/state concepts are managed declaratively instead of by hand.
