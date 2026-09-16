# Day 12 — Cheat Sheet: Data Ingestion — Formats, Sources, and Batch/Streaming Append Pipelines

## Format Landscape

| Format | Type | Schema | Key notes |
|---|---|---|---|
| **Delta Lake** | Parquet-based + transaction log | Embedded + enforced | ACID, schema enforcement, time travel — the destination, not usually the source |
| **Parquet** | Columnar | Embedded | Splittable, compressed, embedded min/max stats in file footers |
| **ORC** | Columnar | Embedded | Hive-lineage; legacy source format from Hive/Hadoop systems |
| **Avro** | Row-based | Schema registry (separate) | Schema/data separation — dominant format for Kafka payloads |
| **JSON** | Semi-structured, text | Inferred by default | `multiLine=true` for pretty-printed files; slower parsing; type-guessing risk |
| **CSV** | Flat, text | Inferred by default | No embedded schema; delimiter/quote/escape misconfig silently produces bad data |
| **XML** | Semi-structured, text | Requires `rowTag` option | DBR 14.1+ native support; older: `com.databricks:spark-xml` library |
| **Text** | Unstructured | None | Each line = one row, single `value: STRING` column |
| **Binary** | Unstructured | None | Returns `path`, `modificationTime`, `length`, `content` (raw bytes) — for images/docs |

**Exam trap:** Parquet/ORC/Avro carry embedded schema — no inference, no guessing risk. CSV/JSON infer schema — prone to type-coercion errors (e.g., leading zeros lost, numeric IDs inferred as INT).

## Reading Syntax

```python
df = spark.read.format("parquet").load("/path/")
df = spark.read.format("orc").load("/path/")
df = spark.read.format("avro").load("/path/")
df = spark.read.format("json").option("multiLine", "true").load("/path/")
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/path/")
df = spark.read.format("xml").option("rowTag", "record").load("/path/")
df = spark.read.format("text").load("/path/")
df = spark.read.format("binaryFile").load("/path/")
```

## Diverse Sources

**Message buses:**
```python
raw = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders-topic")
    .option("startingOffsets", "earliest")
    .load())
# Kafka key/value = BINARY — must deserialize explicitly:
parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
```
**Exam trap:** Auto Loader (`cloudFiles`, Day 13) reads **cloud storage files only** — NOT Kafka/Kinesis/Event Hubs.

## Batch Ingestion Tool Comparison

| Tool | Idempotent? | Schema evolution | Best for |
|---|---|---|---|
| `spark.read` | No — re-runs reprocess everything | Manual via `mergeSchema` | Ad-hoc exploration |
| `read_files()` | No (batch mode) / Yes (STREAM mode) | Auto-detects; evolution in streaming/pipeline mode | SQL batch reads; Lakeflow pipeline entry point |
| `COPY INTO` | **Yes** — tracks ingested files in table metadata | **No** — needs predefined schema | Bounded batch loads, backfills, periodic refreshes with stable schema |
| Auto Loader (`cloudFiles`) | **Yes** — checkpoint-tracked, exactly-once | **Yes** — inference + evolution + quarantine | High-volume continuous ingestion, evolving schemas |

**Exam trap:** `COPY INTO` for stable-schema bounded loads — Auto Loader for evolving schemas or continuous arrival.

## Append-Only Batch + Streaming with Delta

Delta's transaction log is what makes batch + streaming writers safely append to the **same target table**.

```python
# BATCH
batch_df.write.format("delta").mode("append").saveAsTable("bronze.events")

# STREAMING
(stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/chk/bronze_events")
    .toTable("bronze.events"))
```

## Trigger Modes

| Trigger | Behavior |
|---|---|
| Default (no trigger) | Continuously running micro-batches — "always-on" |
| `Trigger.ProcessingTime('X seconds')` | Fixed-interval micro-batches — still continuously running |
| `Trigger.AvailableNow` | Process all currently available data, then **terminate automatically** |

**Exam trap:** Scenario about "minimize cost, don't leave cluster running continuously, but need exactly-once incremental ingestion" = `Trigger.AvailableNow` on a scheduled job — NOT plain batch (no tracking) and NOT continuously running streaming (ongoing billing).

## Exam Trap Shortlist

1. CSV/JSON infer schema — type coercion risk; Parquet/ORC/Avro embed schema — no inference.
2. XML requires `rowTag` to define record boundaries.
3. Kafka `key`/`value` = binary — always requires explicit `from_json`/`from_avro` deserialization.
4. Auto Loader reads cloud storage files only — NOT message buses.
5. `COPY INTO` is idempotent but has **no schema evolution** — wrong tool once evolution or continuous arrival is in scope.
6. `read_files()` is batch by default; `STREAM read_files(...)` inside a Lakeflow pipeline gives Auto Loader behavior.
7. Delta's transaction log is why batch + streaming can append to the same table safely.
8. `Trigger.AvailableNow` = streaming-grade correctness with batch-economy compute (shuts down when caught up).
