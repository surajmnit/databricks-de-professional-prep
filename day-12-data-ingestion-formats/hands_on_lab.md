# Day 12 — Hands-On Lab: Multi-Format Ingestion and Unified Batch/Streaming Appends

## Lab Objectives

1. Ingest sample data in Parquet, JSON, CSV, and Text formats and compare schema inference behavior.
2. Ingest unstructured files with the `binaryFile` format.
3. Compare `read_files()`, `COPY INTO`, and direct `spark.read` for batch ingestion.
4. Reproduce `COPY INTO`'s lack of schema evolution on purpose.
5. Build a single Delta table that receives both a batch append and a streaming append.
6. Use `Trigger.AvailableNow` to run a streaming write in a scheduled, batch-like fashion.

**Environment note:** Kafka/message-bus steps are provided as **reference code** since Community Edition and most personal workspaces don't have a running Kafka cluster — read them rather than executing them unless you have a broker available.

---

## Step 1 — Ingest multiple formats and compare schema behavior

```python
# Write small sample files in different formats to compare read-side behavior
sample_data = [(1, "Alice", "00123"), (2, "Bob", "00456")]
cols = ["id", "name", "zip_code"]
df = spark.createDataFrame(sample_data, cols)

df.write.mode("overwrite").parquet("/tmp/day12/parquet/")
df.write.mode("overwrite").json("/tmp/day12/json/")
df.write.mode("overwrite").option("header", "true").csv("/tmp/day12/csv/")

# Read them back
df_parquet = spark.read.parquet("/tmp/day12/parquet/")
df_json = spark.read.json("/tmp/day12/json/")
df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv("/tmp/day12/csv/")

df_parquet.printSchema()
df_json.printSchema()
df_csv.printSchema()
```
**What to observe:** check the `zip_code` column's inferred type in each. Parquet preserves the exact type you wrote (`string`, since it was written from a DataFrame with that schema). CSV, re-inferring from plain text, may or may not correctly preserve `zip_code` as a string depending on the values — this is the "lost leading zeros" trap in practice. Try adding a row with `zip_code = "00789"` and re-reading the CSV with `inferSchema=true` to see if it gets coerced to an integer (dropping the leading zeros).

---

## Step 2 — Ingest unstructured files with `binaryFile`

```python
# Write a couple of plain text files to simulate "documents"
dbutils.fs.put("/tmp/day12/docs/doc1.txt", "This is document one.", overwrite=True)
dbutils.fs.put("/tmp/day12/docs/doc2.txt", "This is document two.", overwrite=True)

df_binary = spark.read.format("binaryFile").load("/tmp/day12/docs/")
df_binary.select("path", "modificationTime", "length").show(truncate=False)

# The actual bytes are in the `content` column — decode to inspect
from pyspark.sql.functions import col
df_binary.select(col("path"), col("content").cast("string").alias("text_preview")).show(truncate=False)
```

---

## Step 3 — Compare `read_files()`, `COPY INTO`, and direct `spark.read`

```sql
-- read_files: SQL, batch, auto-detects format
SELECT * FROM read_files('/tmp/day12/csv/', format => 'csv', header => true);
```
```sql
-- COPY INTO: idempotent, tracked, needs a predefined target table
CREATE TABLE IF NOT EXISTS main.default.copy_into_target_day12 (id INT, name STRING, zip_code STRING);

COPY INTO main.default.copy_into_target_day12
FROM '/tmp/day12/csv/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');

SELECT * FROM main.default.copy_into_target_day12;
```
**Predict then verify:** run the exact same `COPY INTO` statement a second time. Does it duplicate the rows?
**Answer:** No — `COPY INTO` tracks which files it has already ingested into the target table's metadata and skips them on re-run. This is its idempotency guarantee.

---

## Step 4 — 💥 Break it on purpose: `COPY INTO` has no schema evolution

```python
# Add a NEW column to the source that the target table doesn't have
new_data = [(3, "Carol", "00789", "Sales")]
new_cols = ["id", "name", "zip_code", "department"]
spark.createDataFrame(new_data, new_cols).write.mode("append").option("header", "true").csv("/tmp/day12/csv_v2/")

try:
    spark.sql("""
        COPY INTO main.default.copy_into_target_day12
        FROM '/tmp/day12/csv_v2/'
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true')
    """)
    spark.sql("SELECT * FROM main.default.copy_into_target_day12").show()
except Exception as e:
    print("Expected — COPY INTO does not evolve the target schema automatically:")
    print(str(e)[:300])
```
**What to observe:** the new `department` column is either rejected or silently dropped depending on configuration — either way, `COPY INTO` does **not** automatically widen the target schema the way Auto Loader's `mergeSchema`/schema evolution would (Day 13). Fix would require an explicit `ALTER TABLE ... ADD COLUMNS` first.

---

## Step 5 — Unified batch + streaming appends into one Delta table

```python
spark.sql("CREATE TABLE IF NOT EXISTS main.default.bronze_events_day12 (id INT, event STRING)")

# BATCH append
batch_df = spark.createDataFrame([(1, "batch_insert")], ["id", "event"])
batch_df.write.format("delta").mode("append").saveAsTable("main.default.bronze_events_day12")

# STREAMING append (using rate source to simulate a stream, since no Kafka is available)
stream_df = (spark.readStream.format("rate").option("rowsPerSecond", 1).load()
    .selectExpr("CAST(value AS INT) as id", "'stream_insert' as event"))

query = (stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .trigger(availableNow=True)      # process what's available, then stop — batch-like execution
    .option("checkpointLocation", "/tmp/day12/chk/bronze_events")
    .toTable("main.default.bronze_events_day12"))

query.awaitTermination()
```
**What to observe:**
```sql
SELECT * FROM main.default.bronze_events_day12;
DESCRIBE HISTORY main.default.bronze_events_day12;
```
Both the batch `WRITE` operation and the streaming `STREAMING UPDATE` operation appear as separate, safely-coexisting commits in the same transaction log — this is the concrete demonstration of the "append-only pipeline capable of handling both batch and streaming data using Delta" objective.

---

## Step 6 — (Reference only, unless you have a Kafka broker) Message-bus ingestion pattern

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

order_schema = StructType().add("order_id", IntegerType()).add("status", StringType())

kafka_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "<broker>:9092")
    .option("subscribe", "orders-topic")
    .option("startingOffsets", "earliest")
    .load())

parsed = kafka_stream.select(
    from_json(col("value").cast("string"), order_schema).alias("data")
).select("data.*")

(parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/chk/orders_from_kafka")
    .toTable("bronze.orders"))
```
**Key point to notice even without running this:** the `from_json(col("value").cast("string"), ...)` step is mandatory — Kafka never hands you already-parsed columns.

---

## Stretch Task

Ingest an XML sample file using the `rowTag` option:
```python
dbutils.fs.put("/tmp/day12/sample.xml", """
<records>
  <record><id>1</id><name>Alice</name></record>
  <record><id>2</id><name>Bob</name></record>
</records>
""", overwrite=True)

df_xml = spark.read.format("xml").option("rowTag", "record").load("/tmp/day12/sample.xml")
df_xml.show()
```
If native XML isn't available on your runtime, install the `com.databricks:spark-xml_2.12` Maven library on your cluster first and retry.

---

## Lab Checklist

- [ ] Ingested Parquet/JSON/CSV and compared schema-inference behavior, including the leading-zeros trap
- [ ] Ingested unstructured files via `binaryFile` and inspected `path`/`content`
- [ ] Compared `read_files()`, `COPY INTO`, and direct `spark.read`
- [ ] Confirmed `COPY INTO` idempotency (no duplicate rows on re-run)
- [ ] Reproduced `COPY INTO`'s lack of automatic schema evolution
- [ ] Built one Delta table receiving both a batch append and a `Trigger.AvailableNow` streaming append
- [ ] Confirmed both operation types appear safely in `DESCRIBE HISTORY`
- [ ] (Reference) Reviewed the Kafka `from_json` deserialization pattern
- [ ] (Stretch) Ingested an XML file using `rowTag`

---

## Cross-References
- Day 9: Transaction log commits — what makes Step 5's coexisting batch/streaming writes safe.
- Day 13: Auto Loader's schema inference/evolution — the tool that fixes Step 4's `COPY INTO` limitation.
- Day 14: Structured Streaming triggers and watermarking, building on the trigger concepts used here.
