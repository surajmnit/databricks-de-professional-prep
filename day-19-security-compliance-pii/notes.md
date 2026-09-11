# Day 19 — Security, Compliance & PII: Row Filters, Column Masks, Anonymization & Data Purging

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 7: Ensuring Data Security and Compliance (10%)**:
- "Use row filters and column masks to filter and mask sensitive table data."
- "Apply anonymization and pseudonymization methods, such as Hashing, Tokenization, Suppression, and generalization, to confidential data."
- "Implement a compliant batch & streaming pipeline that detects and applies masking of PII to ensure data privacy."
- "Develop a data purging solution ensuring compliance with data retention policies."

*(ACLs, permission inheritance, and discoverability were Day 18 — this day builds directly on top of that grant model: everything below is enforced **after** a `SELECT` grant already succeeds.)*

---

## Part 1 — Row filters and column masks: how they fit with Day 18's ACL model

Row filters and column masks don't replace `GRANT`/`REVOKE` — they sit **on top of** a successful ACL check. A user must already have `SELECT` (plus the `USE CATALOG`/`USE SCHEMA` traversal chain from Day 18) before a row filter or column mask even gets a chance to run. Think of it as two sequential gates:

```
Query arrives → Gate 1: does the ACL grant SELECT? (Day 18) → Gate 2: row filter / column mask applied to what's returned (Day 19)
```

Both are implemented as **SQL user-defined functions attached to a table**, not as separate DDL objects with their own syntax family.

---

## Part 2 — Row filters

A row filter is a scalar SQL function returning `BOOLEAN` — rows where it evaluates to `FALSE` or `NULL` are silently excluded.

```sql
-- 1. Define the function
CREATE FUNCTION us_only_filter(region STRING)
RETURN IF(is_account_group_member('admin'), true, region = 'US');

-- 2. Attach it to a table
CREATE TABLE sales (region STRING, id INT, amount DOUBLE);
ALTER TABLE sales SET ROW FILTER us_only_filter ON (region);

-- Or attach at creation time:
CREATE TABLE sales (region STRING, id INT, amount DOUBLE)
WITH ROW FILTER us_only_filter ON (region);

-- Remove it
ALTER TABLE sales DROP ROW FILTER;
```

- Also attachable to **materialized views** and **streaming tables** via `CREATE`/`ALTER MATERIALIZED VIEW` / `CREATE`/`ALTER STREAMING TABLE` — this is explicitly documented, so don't assume row filters are table-only.
- The filter function typically inspects the caller's identity/group via `is_account_group_member('group_name')` or `current_user()`/`session_user()` — this is how "analysts see only their region" or "only HR sees HR rows" scenarios are implemented without maintaining separate tables per audience.

---

## Part 3 — Column masks

A column mask is a scalar SQL function whose return type matches the masked column; it replaces the column's actual value with the function's output.

```sql
CREATE FUNCTION ssn_mask(ssn STRING)
RETURN CASE WHEN is_account_group_member('hr_dept') THEN ssn ELSE '***-**-****' END;

-- Apply to an existing column
ALTER TABLE employees ALTER COLUMN ssn SET MASK ssn_mask;

-- Or at table creation
CREATE TABLE employees (name STRING, ssn STRING MASK ssn_mask);

-- A mask can also take OTHER columns as extra input (e.g., to compare against a caller's own department)
ALTER TABLE employees ALTER COLUMN salary SET MASK dept_salary_mask USING COLUMNS (department);

-- Remove
ALTER TABLE employees ALTER COLUMN ssn DROP MASK;
```

---

## Part 4 — Privilege requirements and compute compatibility (exam-tested checklist)

| Action | Privilege needed |
|---|---|
| Attach a row filter/mask function to a table | `EXECUTE` on the function **+** `USE SCHEMA` on the schema **+** `USE CATALOG` on the catalog |
| Add a filter/mask while creating a **new** table | `CREATE TABLE` on the schema |
| Add/remove a filter/mask on an **existing** table | Be the table owner, **or** hold `MANAGE` **and** `SELECT` on the table |

**Exam trap — drop order:** you must run `ALTER TABLE ... DROP ROW FILTER` / `DROP MASK` **before** dropping the underlying function. Drop the function first and the table becomes **inaccessible** (every query fails trying to resolve a function that no longer exists) until you either recreate the function or manually intervene.

**Exam trap — compute access mode:** reading a table with a row filter or column mask requires one of: a SQL warehouse, **Standard (formerly Shared) access mode** on DBR 12.2 LTS+, or **Dedicated (formerly Single User) access mode** on DBR 15.4 LTS+. **Dedicated access mode on DBR 15.3 or below cannot read filtered/masked tables at all.** A scenario where "the row filter works for some users' clusters but not others" is testing this exact access-mode/DBR-version gap, not a permissions bug.

**Exam trap — enforcement point:** the filter/mask is applied "as soon as the row is fetched from the data source" — i.e., at **read time**, uniformly, regardless of whether the underlying table is populated by batch writes or a streaming pipeline. This is the key fact for the "compliant batch & streaming pipeline" objective bullet (Part 6 below).

---

## Part 5 — Anonymization and pseudonymization techniques

| Technique | Reversible? | Databricks implementation | Typical use case |
|---|---|---|---|
| **Hashing** | No (one-way) | `sha2(col, 256)`, `hash(col)` — deterministic, same input always produces same output | Join keys across systems where you need consistent identifiers but never the original value; **beware** low-cardinality inputs (e.g., a hashed 2-digit age) are trivially reversible via a rainbow-table/brute-force attack unless salted |
| **Tokenization** | Yes, with the key/lookup | `aes_encrypt(col, key)` / `aes_decrypt(col, key)` (key pulled from a Databricks secret scope, never hardcoded), or an external token vault mapping token ↔ real value | Data that authorized downstream systems must be able to restore (e.g., a payment processor token that maps back to a real card number) |
| **Suppression** | No — data removed entirely | Column mask returning `NULL` or a constant (e.g., `'REDACTED'`) for unauthorized callers; or simply excluding the column from the view/table altogether | Fields with no legitimate secondary use once ingested (e.g., a scratch field only needed by one internal system) |
| **Generalization** | No — precision reduced | `date_trunc('YEAR', birth_date)`, `substring(zip, 1, 3)`, `round(salary, -4)` — reduces specificity while keeping the value useful for aggregate analysis | Fields you want usable for trend analysis (age bands, regional rollups) without exposing an individual-identifying exact value |

**Exam trap:** the question will often describe a requirement like *"downstream fraud detection must be able to recover the original card number when investigating a flagged transaction."* That need for reversibility is the signal for **tokenization** (`aes_encrypt`/`aes_decrypt`), not hashing — hashing can never be undone by design, so it's the wrong tool whenever the original value must be recoverable under any circumstance.

**Exam trap:** *"Analysts need to see the general age range but not exact birthdate."* That's **generalization**, not suppression — suppression would remove the field's analytical value entirely, generalization keeps it usable at reduced precision.

---

## Part 6 — Compliant PII pipeline: batch and streaming

The exam objective specifically says "detects **and** applies masking of PII" across **both** batch and streaming. Two decisions matter here:

**Decision 1 — mask before writing, or store raw + mask at read time?**

| Approach | When to use |
|---|---|
| **Mask/hash/tokenize the value in the transformation itself, before it's ever written to a table** (e.g., inside the bronze→silver `withColumn`) | Regulatory requirement says the raw value must **never exist in queryable/persisted form at all**, including for admins — the safest, most defensible posture |
| **Store the raw value; apply a column mask on the table for read-time control** | Some legitimate users/roles (compliance, security, HR) genuinely need to see the real value under audited conditions — the mask conditionally reveals based on group membership |

**Decision 2 — batch vs. streaming enforcement point**

Because row filters/column masks are enforced **at read time on the table object itself** (Part 4), the *same* filter/mask definition protects data regardless of whether the table is populated via a batch `INSERT`/`MERGE` or a Structured Streaming/Lakeflow Declarative Pipeline `writeStream`. You do **not** need separate masking logic for the streaming path if you're using table-level masks — this is a common wrong-answer bait ("you need to re-implement the mask inside your `foreachBatch` sink" — you don't, if the mask lives on the table).

Where you **do** need explicit code in the pipeline is **detection + transformation before persistence** (Decision 1's first row): e.g., in a Lakeflow Declarative Pipeline, apply the hashing/tokenizing transform as part of the silver-layer query itself, and optionally combine with `@dlt.expect_or_drop`/quarantine (Day 17 material) to catch malformed/unexpected PII patterns (like an email regex mismatch) before they reach a governed table at all.

```python
import dlt
from pyspark.sql.functions import sha2, col

@dlt.table
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .withColumn("email_hash", sha2(col("email"), 256))
        .drop("email")   # raw PII never persisted downstream of this point
    )
```

---

## Part 7 — Data purging: the full "right to be forgotten" lifecycle

**Exam trap — masking ≠ deletion.** For a genuine GDPR/CCPA erasure request, **complete deletion is preferred over obfuscation.** Masking/hashing/tokenizing still leaves the underlying value recoverable by someone with sufficient access or by re-identification techniques; if a scenario says "must guarantee the data can never be recovered by anyone, including admins," the answer is a hard delete + physical purge, not a column mask.

**The physical purge lifecycle** (this exact sequence is the tested pattern):

```sql
-- 1. Point delete (logical — with deletion vectors, this just marks rows, doesn't rewrite files)
DELETE FROM customers WHERE customer_id = 12345;

-- 2. Physically rewrite files to actually remove the marked rows (required when deletion vectors are enabled)
REORG TABLE customers APPLY (PURGE);

-- 3. Remove old file versions beyond the retention window so the deleted data
--    doesn't linger in cloud storage as a "previous version" accessible via time travel
VACUUM customers;

-- For an urgent compliance deadline where you cannot wait for the default retention window:
SET spark.databricks.delta.retentionDurationCheck.enabled = false;   -- override the safety guard
ALTER TABLE customers SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 0 hours');
VACUUM customers RETAIN 0 HOURS;
```

**Why each step is necessary:**
- `DELETE` alone, when **deletion vectors** are enabled (the modern default), only marks rows as deleted in a side-file — the actual bytes are still sitting in the data files.
- `REORG TABLE ... APPLY (PURGE)` rewrites the affected files, physically removing the marked rows.
- `VACUUM` removes now-unreferenced old file versions (governed by `delta.deletedFileRetentionDuration` / `delta.logRetentionDuration`) — until this runs, prior versions (including the "deleted" data) remain queryable via **time travel**, which is itself a compliance gap.
- `VACUUM RETAIN 0 HOURS` + disabling `retentionDurationCheck` is the emergency-compliance override — it breaks time travel entirely for that table, so use it deliberately, not by default.

**Propagation across the medallion architecture:** GDPR/CCPA applies to every layer, not just the source table. The recommended pattern is to **delete in the bronze layer first**, driven by a control table of deletion requests, then **propagate the delete downstream to silver/gold** — either via a full refresh of downstream tables or, more efficiently, via **Change Data Feed** (Day 10/11 material) so silver/gold pick up the delete as a CDC event without a full reprocessing pass.

```sql
-- A driving "deletion request" control table
MERGE INTO bronze_customers t
USING gdpr_deletion_requests d
ON t.customer_id = d.customer_id
WHEN MATCHED THEN DELETE;
```

**Exam trap:** GDPR/CCPA obligations extend to **upstream, non-Delta sources too** — Kafka topics, raw files in cloud storage landing zones, etc. A question implying "delete only from the Delta table and you're compliant" is incomplete; the raw ingestion source must also be purged or have its own retention policy.

**Exam trap:** materialized views and streaming tables are explicitly included in this purge lifecycle — don't assume `REORG TABLE ... APPLY (PURGE)` only applies to plain managed tables.

---

## Part 8 — Exam Traps Recap

1. Row filters/masks are enforced at **read time on the table**, uniformly for batch and streaming writers — you don't need duplicate masking logic per write path if the mask lives on the table.
2. **Drop order matters:** remove the `ROW FILTER`/`MASK` from the table before dropping the underlying function, or the table becomes inaccessible.
3. **Dedicated access mode below DBR 15.4** cannot read row-filtered/masked tables — a compute/version gap, not a permissions bug.
4. **Hashing is one-way; tokenization is reversible with a key.** Pick based on whether the original value must ever be recoverable.
5. **Masking/anonymization is not equivalent to deletion** for GDPR "right to be forgotten" — true erasure requires `DELETE` → `REORG TABLE ... APPLY (PURGE)` → `VACUUM`.
6. `DELETE` alone with deletion vectors enabled does **not** physically remove data — `REORG TABLE ... APPLY (PURGE)` is required for a true purge.
7. Time travel keeps "deleted" data queryable until `VACUUM` runs past the retention window — a compliance gap if not addressed.
8. Purge propagation must flow **bronze → silver → gold**, and also cover **non-Delta upstream sources**.
9. Generalization reduces precision (still analytically useful); suppression removes the value entirely — don't confuse the two when a scenario asks for "still usable for trend analysis."

---

## Cross-References

- Day 6/10/11: Change Data Feed — used to propagate deletes downstream efficiently.
- Day 17: Quarantine/expectations — for detecting malformed PII patterns before they land in a governed table.
- Day 18: The ACL/`SELECT` grant that must succeed before any row filter or column mask is even evaluated.
- Day 2/25: Declarative Automation Bundles — for scheduling the purge job itself.
