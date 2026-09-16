# Day 12 — Quiz: Data Ingestion — Formats, Sources, and Batch/Streaming Append Pipelines

**Objective coverage:** Section 2 (Data Ingestion & Acquisition, 7%).

---

## Question 1
**Objective:** Format schema handling — embedded vs. inferred.

A team ingests a CSV file containing `user_id` values like `00012345`. Downstream, they notice these IDs have been converted to `12345`, losing the leading zeros. What is the root cause?

A. Delta cannot store leading zeros for numeric-looking columns
B. CSV format infers schema by default, and a numeric-looking column is inferred as an integer type, losing the leading zeros
C. The `header` option strips leading zeros from the first column
D. Avro would solve this, but Delta cannot preserve leading zeros from any text-based format

---

## Question 2
**Objective:** Format schema handling — embedded schema formats.

A data pipeline reads from a legacy Hive system that stores data in ORC format. The upstream team periodically changes column order in the source files. Which statement about how Spark handles this is correct?

A. Spark always rejects ORC files when column order changes
B. Spark reads the embedded schema directly from ORC file footers — no inference needed, and column-order changes in the source are handled safely
C. ORC format requires schema inference and will guess the new schema incorrectly
D. ORC format is read-only and cannot be used as a source for Spark pipelines

---

## Question 3
**Objective:** XML format — rowTag requirement.

A pipeline needs to ingest an XML file where each record is wrapped in a `<Transaction>` element. What option is required, and why?

A. `option("rowTag", "Transaction")` — tells Spark which XML element demarcates a record boundary
B. `option("rootTag", "Transaction")` — names the root document element
C. `option("recordTag", "Transaction")` — marks the start of each data record
D. No special option is needed; Spark auto-detects the record element in XML

---

## Question 4
**Objective:** Binary file format.

A team needs to ingest image files stored in cloud storage and process them with a custom Python UDF. Which format and read syntax correctly returns the raw bytes?

A. `spark.read.format("text").load("/path/")` — text handles any file type
B. `spark.read.format("binaryFile").load("/path/")` — returns `path`, `modificationTime`, `length`, and `content` (raw bytes) columns
C. `spark.read.format("csv").load("/path/")` — CSV works for binary content
D. Binary files cannot be read directly by Spark; a separate tool is required

---

## Question 5
**Objective:** Avro format — schema separation.

A team uses Kafka to stream events and wants to use Avro for serialization. Unlike Parquet, what architectural characteristic of Avro is most important for Kafka workloads?

A. Avro embeds its schema in each binary file footer, enabling self-describing records
B. Avro stores its schema separately (typically in a schema registry), decoupling schema evolution from message payload delivery
C. Avro compresses better than Parquet for streaming data
D. Avro automatically infers schema from incoming messages

---

## Question 6
**Objective:** Kafka source — binary payload deserialization.

A pipeline reads from Kafka. The analyst notices the `value` column shows binary-looking characters instead of readable JSON. What is the correct diagnosis and fix?

A. Kafka has a bug — convert the output format to CSV instead
B. The `value` column arrives as raw binary — you must explicitly deserialize it with `from_json` (for JSON) or `from_avro` (for Avro) before the data is usable
C. Spark automatically deserializes Kafka payloads; use `value` directly in your transforms
D. Set `spark.sql.kafkaConsumerEnabled=true` to enable automatic deserialization

---

## Question 7
**Objective:** Batch ingestion tool comparison — COPY INTO vs. Auto Loader.

A data team has 10,000 Parquet files in cloud storage representing a one-time historical backfill. The schema is stable and well-understood. The team wants idempotent behavior (re-running shouldn't duplicate data). Which tool best fits?

A. `spark.read.format("parquet").load("/path/").write` — direct batch read
B. `COPY INTO` — idempotent (tracks which files have been loaded), no schema evolution needed, perfect for stable-schema bounded loads
C. Auto Loader — provides exactly-once guarantees but is overkill for a one-time backfill
D. `read_files()` table-valued function — handles this without any special options

---

## Question 8
**Objective:** Batch ingestion tool comparison — schema evolution scenarios.

A team receives new files daily from a partner API. The schema evolves slowly over time as the partner adds new fields. What is the correct tool choice and why?

A. `COPY INTO` — tracks ingested files and handles schema evolution automatically
B. `spark.read` with `mergeSchema=true` — enables schema merging on read
C. Auto Loader (`cloudFiles`) — schema inference + evolution handles new fields gracefully with the rescued-data column for unexpected fields
D. Binary format — will preserve any new fields automatically

---

## Question 9
**Objective:** Auto Loader — source type (cloud storage vs. message bus).

A team wants to ingest data directly from Kafka topics into their Databricks pipeline. They ask about Auto Loader. What is the correct guidance?

A. Auto Loader supports Kafka as a source — just use `cloudFiles` format with Kafka bootstrap servers
B. Auto Loader reads cloud storage files only; for Kafka, use Structured Streaming's native `kafka` format connector
C. Auto Loader supports Kafka, Kinesis, and Event Hubs equally well
D. Auto Loader cannot be used with message bus sources; use `spark.read` instead

---

## Question 10
**Objective:** read_files() — batch vs. streaming behavior.

Which statement correctly describes `read_files()` behavior in different contexts?

A. `read_files()` always behaves identically regardless of context — batch or streaming
B. `read_files()` in batch mode has no built-in file tracking; wrapping it in `STREAM read_files(...)` inside a Lakeflow Declarative Pipeline gives Auto Loader's incremental/evolving behavior
C. `read_files()` is always incremental and never needs a pipeline wrapper
D. `read_files()` requires explicit checkpoint configuration to be idempotent

---

## Question 11
**Objective:** Delta enables batch + streaming coexistence.

A team runs a daily batch backfill job AND a continuously running streaming ingestion job that both write to the same `bronze.events` table. Why is this safe with Delta but not with raw Parquet files?

A. Delta's transaction log serializes concurrent writes, preventing conflicts
B. Databricks automatically detects batch vs. streaming jobs and allocates separate resources
C. Parquet cannot be used as a write target, only as a read source
D. Streaming jobs cannot write to Parquet by definition

---

## Question 12
**Objective:** Trigger.AvailableNow — batch-economy streaming.

A team needs exactly-once, incremental ingestion (not full reprocess) on a daily schedule, but wants to minimize compute costs by not leaving a cluster running continuously. Which Structured Streaming trigger pattern satisfies these requirements?

A. Default trigger (continuous micro-batches) — processes incrementally and shuts down when caught up
B. `Trigger.ProcessingTime('1 hour')` — fixed interval with lower cost than continuous processing
C. `Trigger.AvailableNow` — processes all currently available data, then terminates automatically — streaming-grade correctness with batch-economy compute
D. No trigger option — use a plain batch write instead

---

## Question 13
**Objective:** COPY INTO — schema evolution limitation.

A team uses `COPY INTO` for a daily ingestion job. One morning, the incoming files have two new columns that weren't in yesterday's files. What happens?

A. `COPY INTO` auto-detects the new columns and adds them to the target table
B. `COPY INTO` ingests the new columns into a `rescued_data` column
C. `COPY INTO` fails — it requires a predefined target schema and does not support schema evolution; new columns are rejected
D. `COPY INTO` silently ignores the new columns and continues ingestion

---

## Question 14
**Objective:** Kafka — startingOffsets option.

A streaming job is being deployed for a new Kafka topic. It should process all existing historical messages first before processing new messages. What `startingOffsets` value achieves this?

A. `"latest"` — starts from the newest available messages
B. `"earliest"` — starts from the oldest available offset, processing all historical messages first
C. `"none"` — requires explicit offset specification
D. `"auto"` — Databricks decides based on available cluster resources

---

## Question 15
**Objective:** Combined batch + streaming architecture.

A data platform ingests from two sources: (1) a daily batch job processing yesterday's full exports from an SFTP server, and (2) a real-time stream from Kafka for intraday updates. Both write to the same `silver.transactions` Delta table. What Delta feature most directly enables this safe coexistence?

A. Delta's ACID transaction log and optimistic concurrency control allow both paradigms to commit to the same table without additional coordination logic
B. Databricks automatically creates two separate physical tables for batch and streaming
C. The batch job and streaming job are automatically serialized by the cluster manager
D. This pattern is not supported by Delta; batch and streaming must write to separate tables

---

## Answer Key

### Q1: B
CSV and JSON formats infer schema by default — an extra pass that guesses column types from the data. A numeric-looking string like `00012345` gets inferred as `INT`, losing the leading zeros. Parquet/ORC/Avro carry their schema embedded in the file and avoid this guessing entirely.

### Q2: B
ORC (like Parquet) carries its schema embedded in the file footer. Spark reads it directly without inference, and schema changes handled correctly because the schema is stored with each file independently.

### Q3: A
`option("rowTag", "Transaction")` tells Spark which XML element represents one record boundary — without it, Spark has no way to know how to split the XML document into rows.

### Q4: B
`spark.read.format("binaryFile")` returns exactly four columns: `path`, `modificationTime`, `length`, and `content` (raw bytes). This is the standard entry point for ingesting unstructured files for downstream UDF processing.

### Q5: B
Avro's defining architectural characteristic for Kafka is schema/data separation — the schema lives in a schema registry and evolves independently from any individual message payload. This enables schema evolution without breaking existing consumers and is precisely why Avro dominates Kafka serialization.

### Q6: B
Kafka's `value` (and `key`) columns arrive as raw binary in Spark. You must explicitly deserialize them: `from_json` for JSON payloads, `from_avro` with a schema registry reference for Avro payloads. Spark does not auto-parse message-bus payloads the way it can auto-infer a CSV file's schema.

### Q7: B
`COPY INTO` is designed for exactly this scenario: bounded, stable-schema files where idempotent re-runs matter. It tracks which files have been ingested in the table's metadata and skips already-loaded files on re-run. `spark.read` has no built-in tracking; Auto Loader is overkill for a bounded one-time load.

### Q8: C
Auto Loader (`cloudFiles`) is built for evolving schemas: it infers the schema from incoming files, handles new fields via the rescued-data column (for unexpected fields that don't match the target schema), and continues processing without failing on schema drift. `COPY INTO` has no schema evolution support.

### Q9: B
Auto Loader is a cloud-storage-files ingestion tool — it does not read from Kafka/Kinesis/Event Hubs. For message bus sources, use Structured Streaming's native connectors: `kafka`, `kinesis`, `eventhubs`, `pulsar` formats. This is a common exam trap.

### Q10: B
`read_files()` is batch by default (no built-in tracking). Wrapping it inside `STREAM read_files(...)` inside a Lakeflow Declarative Pipeline gives it Auto Loader's incremental, evolving, checkpoint-tracked behavior — the key distinction the exam tests.

### Q11: A
Delta's transaction log and optimistic concurrency control are what allow both batch and streaming writers to append to the same table safely. Each writer commits through the `_delta_log`; Delta's concurrency mechanism resolves any overlap. Raw Parquet files have no such coordination — concurrent appends can produce corrupted or inconsistent results.

### Q12: C
`Trigger.AvailableNow` processes all data that is currently available (across as many micro-batches as needed), then terminates automatically. This gives you Structured Streaming's exactly-once, checkpoint-tracked correctness guarantees while running on a batch-like schedule inside a Databricks Job — compute shuts down when caught up, eliminating continuous-billing cluster time.

### Q13: C
`COPY INTO` requires a predefined target schema and does not support schema evolution. New columns in incoming files are rejected (the statement fails), not silently ignored. Auto Loader is the tool designed for scenarios with evolving schemas.

### Q14: B
`startingOffsets` = `"earliest"` tells Spark to start from the oldest available offset in the topic, processing all historical messages before moving to new ones. `"latest"` starts from the newest messages and skips all history.

### Q15: A
Delta's transaction log and optimistic concurrency control are the mechanical foundation that allows batch and streaming writers to coexist against one physical table. Both writers commit through the same `_delta_log`; Delta handles the coordination. This is the answer behind the "append-only, batch AND streaming" exam objective bullet.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | CSV schema inference — leading zeros lost |
| 2 | Easy | ORC embedded schema — column order changes handled |
| 3 | Easy | XML rowTag requirement |
| 4 | Easy | Binary file format syntax |
| 5 | Medium | Avro schema separation for Kafka |
| 6 | Medium | Kafka binary payload deserialization |
| 7 | Medium | COPY INTO for idempotent stable-schema loads |
| 8 | Medium | Auto Loader for schema evolution |
| 9 | Medium | Auto Loader reads cloud storage, not message buses |
| 10 | Medium | read_files() batch vs. STREAM mode behavior |
| 11 | Easy | Delta transaction log enables batch+streaming coexistence |
| 12 | Hard | Trigger.AvailableNow — batch-economy streaming |
| 13 | Medium | COPY INTO no schema evolution limitation |
| 14 | Easy | Kafka startingOffsets = earliest for historical |
| 15 | Medium | Delta ACID + optimistic concurrency for batch+streaming |
