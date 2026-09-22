# Day 17 — Data Transformation, Cleansing, and Quality: Deduplication, Validation, and the Complete Quarantine Pattern

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 3: Data Transformation, Cleansing, and Quality (10%)**:
- "Write efficient Spark SQL and PySpark code to apply advanced data transformations, including window functions, joins, and aggregations, to manipulate and analyze large datasets." *(Day 3 covered the syntax and mechanics of window functions and joins in depth — this day applies those same tools specifically to cleansing and data-quality tasks, not general transformation teaching. Don't re-read Day 3's ROWS/RANGE or LEFT SEMI/ANTI mechanics here — this day assumes them.)*
- "Develop a quarantining process for bad data with Lakeflow Spark Declarative Pipelines, or Autoloader in classic jobs." *(Day 13 built the classic `_rescued_data`/`foreachBatch` mechanism; Day 15 built the Lakeflow `@dlt.expect*` mechanism. This day is the synthesis — a complete, production-shaped quarantine design using either, plus the layers of validation that neither mechanism catches on its own.)*

---

## Part 1 — Deduplication Patterns That Actually Hold Up

### `dropDuplicates()` vs. `ROW_NUMBER()` — a real correctness gap, not just a style choice

```python
# dropDuplicates: keeps an ARBITRARY row among duplicates — no control over which one
df.dropDuplicates(["customer_id"])

# ROW_NUMBER: deterministic — you choose exactly which row "wins"
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("customer_id").orderBy(col("last_updated").desc())
deduped = (df
    .withColumn("rn", row_number().over(window))
    .filter(col("rn") == 1)
    .drop("rn"))
```

**Exam trap:** `dropDuplicates()` does not let you express "keep the most recent record" — it has no ordering concept at all, so which duplicate survives is not guaranteed to be stable across reruns or Spark versions. Any scenario mentioning "keep the latest/most recent record per key" requires the `ROW_NUMBER()` pattern (Day 3), not `dropDuplicates()`.

### Tie-breaking matters

If two duplicate rows share the exact same `last_updated` value, `ROW_NUMBER()`'s `ORDER BY` alone doesn't guarantee which one gets `rn = 1` — Spark's tie-breaking among equal sort keys is not itself deterministic across partitions/reruns. Add a genuinely unique tiebreaker column to the `ORDER BY` (an ingestion sequence number, a surrogate key, or the file-modification timestamp) whenever the "primary" ordering column can plausibly tie:

```python
window = Window.partitionBy("customer_id").orderBy(
    col("last_updated").desc(), col("ingestion_seq").desc()
)
```

---

## Part 2 — Referential Integrity Validation via Joins

A cleansing pipeline often needs to confirm that foreign keys in an incoming fact table actually exist in a dimension/reference table before letting rows into silver. This reuses Day 3's `LEFT ANTI JOIN`, applied to a validation use case rather than a set-exclusion one:

```python
# Rows in the incoming fact batch whose customer_id has NO match in the dimension table
orphaned_rows = incoming_orders.join(dim_customers, "customer_id", "leftanti")

# Route: valid rows continue to silver, orphaned rows go to quarantine
valid_orders = incoming_orders.join(dim_customers, "customer_id", "leftsemi")
```

**Exam trap — formatting bugs masquerading as real orphans:** a very common false-positive source for this exact check is inconsistent formatting between the two sides of the join — mismatched casing, leading/trailing whitespace, or inconsistent zero-padding on the key column. Standardize both sides before joining:

```python
from pyspark.sql.functions import upper, trim

incoming_orders = incoming_orders.withColumn("customer_id", upper(trim(col("customer_id"))))
dim_customers = dim_customers.withColumn("customer_id", upper(trim(col("customer_id"))))
```

A scenario describing "referential integrity violations spiked after a source system change, but the actual customer records exist" is almost always pointing at a formatting mismatch like this, not genuinely missing dimension data.

---

## Part 3 — Anomaly Detection with Window Aggregates

Beyond schema-level validity (Day 13/15's territory), a cleansing layer can flag **statistically unusual** values using the same window-function machinery from Day 3:

```python
from pyspark.sql.functions import avg, stddev, abs as sql_abs

stats_window = Window.partitionBy("account_id").orderBy("txn_date").rowsBetween(-30, -1)

flagged = (transactions
    .withColumn("trailing_avg", avg("amount").over(stats_window))
    .withColumn("trailing_stddev", stddev("amount").over(stats_window))
    .withColumn(
        "is_outlier",
        sql_abs(col("amount") - col("trailing_avg")) > (3 * col("trailing_stddev"))
    ))
```

**Why `rowsBetween(-30, -1)` and not `(-30, 0)`:** excluding the current row (`-1` as the upper bound, not `0`) means today's transaction is compared against the **preceding** 30 days' distribution, not a distribution that includes and is skewed by the very value being evaluated. This is the same off-by-one attentiveness Day 3 flagged for moving averages, applied here to an anomaly baseline instead.

This kind of check is a **business-rule** layer of data quality — it catches values that are syntactically perfectly valid (a well-formed number, correct type) but semantically suspicious, which neither `_rescued_data` (Day 13) nor a schema-level expectation (Day 15) would ever flag.

---

## Part 4 — Null Handling and Type Coercion in Cleansing Logic

| Tool | Behavior |
|---|---|
| `df.na.fill(value)` / `.fillna(...)` | Replace nulls with a specified default, column-by-column or globally |
| `df.na.drop()` / `.dropna(...)` | Drop rows with nulls (in any or specified columns) |
| `coalesce(col1, col2, ...)` | Return the first non-null value across several columns/expressions — useful for "prefer this source, fall back to that one" cleansing |

**Exam trap — implicit type coercion in filters:** comparing columns of different types (an `INT` column against a `STRING` literal, for example) can silently produce a different result than intended depending on whether ANSI SQL mode is enabled — under non-ANSI behavior Spark may coerce one side without complaint; under ANSI mode the same comparison can raise an error instead. Cleansing logic should **cast explicitly** (`col("id").cast("string")`) rather than relying on implicit coercion, precisely because its behavior isn't guaranteed to be identical across Spark/DBR versions and configurations (Day 6's `hands_on_lab.md` demonstrates this exact ambiguity in a filter comparison).

---

## Part 5 — The Complete Quarantine Pattern: Classic and Lakeflow, Side by Side

Neither Day 13's `_rescued_data` nor Day 15's `@dlt.expect*` alone constitutes "data quality" — they catch **schema/format-level** problems (a field that doesn't parse, a type mismatch, a missing value). Parts 1–3 above are the complementary **business-rule** layer (duplicates, orphaned keys, statistical outliers) that neither mechanism was ever designed to catch. A production-grade cleansing pipeline needs both layers.

### Decision: which quarantine mechanism to reach for

| Execution context | Mechanism | Covered in |
|---|---|---|
| Classic job / raw Structured Streaming | Filter on `_rescued_data IS NULL`/`IS NOT NULL` inside `foreachBatch`, write to two sinks | Day 13 |
| Lakeflow Declarative Pipeline | `@dlt.expect_or_drop` / `@dlt.expect_all_or_drop` constraints | Day 15 |

### The layered design

```
Bronze (raw, as-ingested)
   │
   ├─ Layer 1 — Format/schema validity
   │     Classic: _rescued_data IS NULL check
   │     Lakeflow: @dlt.expect_or_drop("valid_schema", ...)
   │
   ├─ Layer 2 — Business-rule validity (this day's material)
   │     Referential integrity (Part 2), statistical outliers (Part 3),
   │     deduplication (Part 1) — expressed as ordinary filter/join/window
   │     logic, wrapped in the SAME expectation or foreachBatch-routing
   │     mechanism as Layer 1 so violations land in the same quarantine sink
   │
   ▼
Silver (clean) + Quarantine (everything Layer 1 or Layer 2 rejected)
```

```python
# Lakeflow: combining a schema-level expectation with a business-rule expectation
# in one declarative table — both feed the same violation metrics/routing
@dlt.table
@dlt.expect_all_or_drop({
    "no_rescued_fields": "_rescued_data IS NULL",         # Layer 1 (Day 13/15)
    "known_customer": "customer_id IN (SELECT customer_id FROM LIVE.dim_customers)",  # Layer 2 (this day)
})
def silver_orders():
    return dlt.read_stream("bronze_orders")
```

**Exam framing:** a scenario combining "malformed records" language (points to Layer 1 — Day 13/15's mechanisms) with "duplicate/orphaned/suspicious records" language (points to Layer 2 — this day's transformations) is testing whether you recognize these as two different, stackable layers rather than assuming one mechanism should catch everything.

### Reprocessing quarantined data

Quarantining is not a dead end — a complete design defines what happens to quarantined rows: manual review and correction, then re-ingestion through the same pipeline (relying on Day 9's idempotent `MERGE`/upsert semantics so reprocessing doesn't create duplicates), or an automated retry once an upstream fix (e.g., a corrected dimension load) resolves the original violation.

---

## Part 6 — Exam Traps Recap

1. `dropDuplicates()` gives no control over *which* duplicate survives — "keep the most recent" requires `ROW_NUMBER()` with an explicit, tie-broken `ORDER BY`.
2. Apparent referential-integrity violations are frequently formatting mismatches (case/whitespace), not real orphaned data — standardize both join sides before validating.
3. Window-based anomaly detection should exclude the current row from its own baseline (`rowsBetween(-30, -1)`, not `(-30, 0)`).
4. Implicit type coercion in filters is configuration-dependent (ANSI mode) — cast explicitly in cleansing logic rather than relying on it.
5. Schema-level quarantine (`_rescued_data`, `@dlt.expect*`) and business-rule quarantine (dedup, referential integrity, outliers) are **complementary layers** — a scenario mixing both kinds of "bad data" wants both layers, stacked into one routing mechanism, not a choice between them.
6. Quarantining isn't the end state — a complete design includes a reprocessing/remediation path back into the pipeline.

---

## Cross-References
- Day 3: `ROW_NUMBER()`, `LEFT SEMI`/`LEFT ANTI` joins, and window frame mechanics — the syntax this day applies to cleansing tasks.
- Day 6: The ANSI-mode int-vs-string comparison ambiguity referenced in Part 4.
- Day 9: Delta `MERGE`/upsert idempotency — what makes safe reprocessing of corrected quarantined data possible.
- Day 13: The classic-jobs `_rescued_data`/`foreachBatch` quarantine mechanism (Layer 1).
- Day 15: Lakeflow `@dlt.expect*` constraints (Layer 1) and how they compose with the business-rule expectations added here (Layer 2).
- Day 19: PII detection patterns that build on this same validation-layering approach before data reaches a governed table.
