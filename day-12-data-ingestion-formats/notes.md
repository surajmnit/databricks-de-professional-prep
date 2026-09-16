# Day 12 — Data Ingestion: Formats, Sources, and Unified Batch/Streaming Append Pipelines

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 2: Data Ingestion & Acquisition (7%)**:
- "Design and implement data ingestion pipelines to efficiently ingest a variety of data formats including Delta Lake, Parquet, ORC, AVRO, JSON, CSV, XML, Text and Binary from diverse sources such as message buses and cloud storage."
- "Create an append-only data pipeline capable of handling both batch and streaming data using Delta."

*(Auto Loader's own configuration — schema inference/evolution, `cloudFiles` options, the quarantine pattern — is Day 13's dedicated deep dive. This day owns the format landscape, the batch-ingestion tool comparison, and the batch/streaming unification pattern with Delta as the sink.)*

---

## Part 1 — The Format Landscape

| Format | Type | Key characteristics | Typical source |
|---|---|---|---|
| **Delta Lake** | Columnar (Parquet-based) + transaction log | ACID, schema enforcement, time travel — the target format for virtually every ingestion pipeline | The destination, not usually the source |
| **Parquet** | Columnar | Splittable, compressed, embeds its own schema + column statistics in file footers; what Delta physically stores under the hood | Cloud storage, upstream Spark/Hadoop pipelines |
| **ORC** | Columnar | Hive-lineage columnar format, similar goals to Parquet (splittable, compressed, embedded stats); less commonly the *target* on Databricks but frequently a legacy *source* format from Hive-based systems | Legacy Hive/Hadoop sources |
| **Avro** | Row-based | Compact binary format with **schema stored separately** (often in a schema registry) — this schema/data separation is *why* Avro is the dominant format for **Kafka** payloads, where schema evolution needs to be tracked independently of any one message | Kafka and other message buses |
| **JSON** | Semi-structured, text | Flexible/nested, but slower to parse than binary columnar formats; `multiLine=true` needed for pretty-printed (non-one-record-per-line) files | APIs, logs, event payloads |
| **CSV** | Flat, text | Simple but fragile — no embedded schema or types; wrong delimiter/quote/escape handling silently produces bad data | Exports from external/legacy systems |
| **XML** | Semi-structured, text | Requires a **`rowTag`** option to tell Spark which element demarcates a record — native XML support is built into Databricks Runtime 14.1+; older workspaces relied on the separate `spark-xml` library (`com.databricks:spark-xml`) | Legacy enterprise systems, SOAP APIs |
| **Text** | Unstructured | Each line becomes one row in a single `value: STRING` column — you parse/structure it yourself downstream | Raw logs |
| **Binary** | Unstructured | `binaryFile` format — returns `path`, `modificationTime`, `length`, and `content` (raw bytes) columns; used for images, PDFs, and other non-tabular files you'll process with a UDF or external library | Documents, images, ML input data |

### Representative read syntax
```python
df_parquet = spark.read.format("parquet").load("/mnt/raw/parquet/")
df_orc     = spark.read.format("orc").load("/mnt/raw/orc/")
df_avro    = spark.read.format("avro").load("/mnt/raw/avro/")
df_json    = spark.read.format("json").option("multiLine", "true").load("/mnt/raw/json/")
df_csv     = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/raw/csv/")
df_xml     = spark.read.format("xml").option("rowTag", "record").load("/mnt/raw/xml/")
df_text    = spark.read.format("text").load("/mnt/raw/logs/")
df_binary  = spark.read.format("binaryFile").load("/mnt/raw/documents/")
```

**Exam trap:** `CSV` and `JSON` **infer** schema by default (an extra pass over the data, and prone to guessing wrong on edge cases — e.g., a numeric-looking ID column inferred as `INT` when it should be `STRING` to preserve leading zeros). Parquet, ORC, and Avro carry their schema **embedded in the file itself**, so no inference pass is needed and no guessing risk exists. A scenario about "unexpected type coercion / lost leading zeros after ingesting a CSV" is testing this exact CSV/JSON-vs-binary-format distinction.

---

## Part 2 — Diverse Sources: Cloud Storage vs. Message Buses

### Cloud storage (files)
Standard `spark.read`/`spark.readStream` against S3/ADLS/GCS paths, or the tools in Part 3 below (`read_files`, `COPY INTO`, Auto Loader).

### Message buses (Kafka, Kinesis, Event Hubs, Pulsar)
```python
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092")
    .option("subscribe", "orders-topic")
    .option("startingOffsets", "earliest")   # or "latest"
    .load())

# Kafka's key/value columns are BINARY — you must deserialize them yourself
from pyspark.sql.functions import col, from_json
parsed = raw_stream.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")
```

**Exam trap:** the Kafka source's `value` (and `key`) columns arrive as **raw binary** — you always need an explicit deserialization step (`from_json` for JSON payloads, `from_avro` with a schema registry for Avro payloads) before the data is usable; Spark does not auto-parse message-bus payload contents the way it can auto-infer a CSV/JSON file's schema.

**Exam trap:** Auto Loader (`cloudFiles`, Day 13) is a **cloud-storage-files** tool — it does **not** read directly from Kafka/Kinesis/Event Hubs. For message-bus sources, you use Structured Streaming's native connectors (`kafka`, `kinesis`, `eventhubs`, `pulsar` formats) instead.

---

## Part 3 — Batch Ingestion Tool Comparison

| Tool | Interface | Idempotent re-runs? | Schema evolution | Best for |
|---|---|---|---|---|
| **`spark.read`** (direct) | PySpark/Scala | No — re-running reprocesses everything you point it at | Manual (`mergeSchema` option on read, if supported) | Ad-hoc exploration, one-off loads |
| **`read_files()`** (SQL table-valued function) | SQL, or `STREAM read_files(...)` inside a Lakeflow Declarative Pipeline | Batch mode: no built-in tracking; Streaming mode (via `STREAM`): yes, leverages Auto Loader under the hood | Auto-detects format; schema inference across files; evolution only when used in streaming/pipeline mode | Quick SQL-based batch reads; the SQL-native entry point into Auto Loader inside Lakeflow pipelines |
| **`COPY INTO`** | SQL | **Yes** — tracks which files have already been loaded into the target table's metadata; re-running against the same file set skips already-ingested files | **No** — requires a predefined target schema, does not evolve it automatically | Bounded, well-understood batch loads; ad-hoc backfills; simple periodic refreshes with a stable schema |
| **Auto Loader (`cloudFiles`)** | Structured Streaming source | Yes — checkpoint-tracked, exactly-once | **Yes** — schema inference AND evolution, plus a rescued-data column for unexpected fields | Continuous or high-volume incremental ingestion (millions+ files), evolving schemas — Day 13's focus |

**Exam trap:** `COPY INTO` is a great answer for "we need to backfill a known, bounded set of files into a table with a stable, predefined schema" — but it is the **wrong** answer the moment the scenario mentions schema evolution or files arriving continuously/indefinitely; that phrasing points to Auto Loader instead.

---

## Part 4 — Creating an Append-Only Pipeline That Handles Both Batch and Streaming (via Delta)

This is the exam objective's second bullet, and the key insight is: **Delta's ACID transaction log is what makes it safe for a batch job and a streaming job to append to the *same* target table** — something a plain Parquet/CSV sink cannot safely guarantee (concurrent writers to raw files can produce corrupted or inconsistent results).

```python
# BATCH append — e.g., a daily backfill job
batch_df.write.format("delta").mode("append").saveAsTable("bronze.events")

# STREAMING append — e.g., a continuously running ingestion job
(stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/chk/bronze_events")
    .toTable("bronze.events"))
```
Both writers commit through the same `_delta_log` (Day 9) — Delta's optimistic concurrency control resolves any overlap safely, so the two ingestion paradigms can coexist against one physical table without you building any additional coordination logic.

### Bridging batch and streaming: `Trigger.AvailableNow`
```python
(stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)     # process everything currently available, then STOP
    .option("checkpointLocation", "/chk/bronze_events")
    .toTable("bronze.events"))
```
- **Default trigger:** runs micro-batches as fast as possible, continuously — a genuinely "always-on" streaming job.
- **`Trigger.ProcessingTime('X seconds')`:** fixed-interval micro-batches — still continuously running.
- **`Trigger.AvailableNow`:** processes all data that is currently available (across as many micro-batches as needed), then **terminates automatically** — this gives you Structured Streaming's exactly-once, checkpoint-tracked correctness guarantees, but running it **on a schedule inside a Databricks Job** rather than as an always-on cluster. This is the mechanism that most directly satisfies "capable of handling both batch and streaming data" with one code path: the same `writeStream` code serves a scheduled/batch-like cadence (`AvailableNow`) or a continuously running one (`ProcessingTime`/default), just by changing the trigger.

**Exam trap:** a scenario emphasizing "minimize cost by not leaving a cluster running continuously, but still needs exactly-once, incremental (not full-reprocess) ingestion" is describing **`Trigger.AvailableNow`** on a scheduled job cluster — not a plain batch `spark.read` (which would need you to track what's already been processed yourself) and not a continuously running streaming job (which would leave compute running/billing between scheduled arrivals).

---

## Part 5 — Exam Traps Recap

1. Parquet/ORC/Avro carry embedded schema (no inference risk); CSV/JSON require inference and are prone to type-guessing mistakes (e.g., losing leading zeros).
2. XML requires the `rowTag` option to define record boundaries; native support exists in DBR 14.1+, otherwise the `spark-xml` library is needed.
3. `binaryFile` format returns `path`/`modificationTime`/`length`/`content` — the standard way to ingest unstructured documents/images for downstream UDF processing.
4. Kafka's `key`/`value` columns are **binary** — always requires explicit `from_json`/`from_avro` deserialization; Auto Loader does **not** read from Kafka (it's a cloud-storage-files tool only).
5. `COPY INTO` is idempotent (tracks ingested files) but has **no schema evolution** and needs a predefined schema — wrong tool once evolution or truly continuous arrival is in scope (that's Auto Loader/Day 13).
6. `read_files()` is batch by default; wrapping it in `STREAM read_files(...)` inside a Lakeflow Declarative Pipeline gives it Auto Loader's incremental/evolving behavior.
7. Delta's transaction log is *why* batch and streaming writers can safely append to the same target table — this is the mechanical answer behind the "append-only, batch AND streaming" objective bullet.
8. `Trigger.AvailableNow` is the bridge between the two paradigms: streaming-grade correctness, run on a batch-like schedule, with compute that shuts down when caught up.

---

## Cross-References
- Day 9: The transaction log/optimistic concurrency mechanics that make concurrent batch+streaming appends to one Delta table safe.
- Day 13: Auto Loader (`cloudFiles`) deep dive — schema inference/evolution options, the rescued-data column, and the quarantine pattern.
- Day 14: Structured Streaming foundations — watermarking and stateful operations that build on the trigger concepts introduced here.
- Day 15/16: Lakeflow Declarative Pipelines' own `read_files`/Auto Loader integration and streaming tables vs. materialized views.
