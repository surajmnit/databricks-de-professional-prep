# Day 19 — Hands-On Lab: Row Filters, Column Masks, Anonymization & Data Purging

## Lab Objectives

1. Create and apply a row filter that conditionally restricts rows by group membership.
2. Create and apply a column mask that conditionally reveals a sensitive column.
3. Reproduce the "wrong drop order" failure (dropping a masking function before removing the mask).
4. Practice all four anonymization/pseudonymization techniques side by side: hashing, tokenization, suppression, generalization.
5. Build a mini compliant pipeline that masks/hashes PII before persisting it downstream.
6. Execute the full physical data-purge lifecycle (`DELETE` → `REORG TABLE ... APPLY (PURGE)` → `VACUUM`) and observe the time-travel compliance gap it closes.
7. Trigger and understand the `VACUUM RETAIN 0 HOURS` safety guard.

**Environment note:** row filters, column masks, and `REORG TABLE`/`VACUUM` require a Unity-Catalog-enabled workspace (same caveat as Day 18) and a SQL warehouse or a Standard/Dedicated-mode cluster on a sufficiently recent DBR. If you're on a UC-limited Community Edition instance, run these as syntax practice against `main` and read the "what to observe" notes rather than expecting live results.

---

## Step 1 — Set up a table with PII columns

```sql
CREATE TABLE IF NOT EXISTS main.default.employees_day19 (
    employee_id INT,
    region STRING,
    email STRING,
    ssn STRING,
    birth_date DATE,
    salary DOUBLE
);

INSERT INTO main.default.employees_day19 VALUES
  (1, 'US', 'alice@example.com', '111-22-3333', DATE'1990-04-12', 95000),
  (2, 'EU', 'bob@example.com',   '222-33-4444', DATE'1985-09-01', 88000);
```

---

## Step 2 — Row filter by region

```sql
CREATE FUNCTION main.default.us_only_filter(region STRING)
RETURN IF(is_account_group_member('admin'), true, region = 'US');

ALTER TABLE main.default.employees_day19 SET ROW FILTER main.default.us_only_filter ON (region);

SELECT * FROM main.default.employees_day19;   -- non-admin callers only see region = 'US' rows
```

**Predict then verify:** if you are (or simulate being) an `admin` group member, do you see the EU row too? Check with `SELECT is_account_group_member('admin');` first to know which branch of the `IF` applies to you.

---

## Step 3 — Column mask for SSN

```sql
CREATE FUNCTION main.default.ssn_mask(ssn STRING)
RETURN CASE WHEN is_account_group_member('hr_dept') THEN ssn ELSE '***-**-****' END;

ALTER TABLE main.default.employees_day19 ALTER COLUMN ssn SET MASK main.default.ssn_mask;

SELECT employee_id, ssn FROM main.default.employees_day19;
```

---

## Step 4 — 💥 Break it on purpose: wrong drop order

```python
try:
    spark.sql("DROP FUNCTION main.default.ssn_mask")   # dropping the function BEFORE removing the mask
    spark.sql("SELECT * FROM main.default.employees_day19").display()
except Exception as e:
    print("Expected failure — table is now inaccessible because the mask function no longer exists:")
    print(str(e)[:400])
```

**Fix (the correct order):**
```sql
-- If you already broke it, recreate the function first to restore access, then:
ALTER TABLE main.default.employees_day19 ALTER COLUMN ssn DROP MASK;
DROP FUNCTION main.default.ssn_mask;
```

---

## Step 5 — Anonymization techniques, side by side

```sql
-- Hashing (one-way)
SELECT email, sha2(email, 256) AS email_hashed FROM main.default.employees_day19;

-- Tokenization (reversible, key-based) — store the key in a secret scope in production, never inline like this
SELECT ssn,
       base64(aes_encrypt(ssn, 'a-32-byte-demo-key-000000000000')) AS ssn_token
FROM main.default.employees_day19;

-- Suppression via a mask returning NULL
CREATE FUNCTION main.default.suppress_email(email STRING) RETURN CAST(NULL AS STRING);
ALTER TABLE main.default.employees_day19 ALTER COLUMN email SET MASK main.default.suppress_email;
SELECT email FROM main.default.employees_day19;   -- always NULL now

-- Generalization
SELECT birth_date, date_trunc('YEAR', birth_date) AS birth_year,
       salary, round(salary, -4) AS salary_band
FROM main.default.employees_day19;

-- Clean up the suppression demo before moving on
ALTER TABLE main.default.employees_day19 ALTER COLUMN email DROP MASK;
```

---

## Step 6 — Mini compliant pipeline: mask before persistence (batch)

```python
from pyspark.sql.functions import sha2, col

bronze_df = spark.table("main.default.employees_day19")
silver_df = bronze_df.withColumn("email_hash", sha2(col("email"), 256)).drop("email", "ssn")
silver_df.write.mode("overwrite").saveAsTable("main.default.employees_silver_day19")

spark.sql("SELECT * FROM main.default.employees_silver_day19").display()
print("Raw email/ssn never persisted downstream of this transform — the streaming equivalent")
print("would apply the identical .withColumn(sha2(...)) inside the streaming query itself.")
```

---

## Step 7 — Full purge lifecycle

```sql
-- Confirm the row exists
SELECT * FROM main.default.employees_day19 WHERE employee_id = 2;

-- Point delete
DELETE FROM main.default.employees_day19 WHERE employee_id = 2;

-- Time travel still shows it — the compliance gap before VACUUM
SELECT * FROM main.default.employees_day19 VERSION AS OF 0 WHERE employee_id = 2;
```

---

## Step 8 — 💥 Break it on purpose: VACUUM safety guard

```python
try:
    spark.sql("VACUUM main.default.employees_day19 RETAIN 0 HOURS")
except Exception as e:
    print("Expected — Databricks blocks an unsafely low retention period by default:")
    print(str(e)[:400])
```

**Override deliberately (understand what you're giving up: all time-travel history for this table):**
```sql
REORG TABLE main.default.employees_day19 APPLY (PURGE);
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM main.default.employees_day19 RETAIN 0 HOURS;
SET spark.databricks.delta.retentionDurationCheck.enabled = true;   -- turn the guard back on immediately after
```

**Verify the purge:**
```sql
SELECT * FROM main.default.employees_day19 VERSION AS OF 0 WHERE employee_id = 2;
-- Expected: no longer retrievable — the row is now physically gone, not just logically deleted
```

---

## Stretch Task

Design a scheduled GDPR-deletion job (as a Databricks Job / DAB task, from Day 2/25):
1. A control table `gdpr_deletion_requests(customer_id, requested_at)`.
2. A `MERGE ... WHEN MATCHED THEN DELETE` against the bronze layer.
3. A downstream step using **Change Data Feed** (Day 10/11) to propagate the delete into silver/gold without a full reprocess.
4. A scheduled `REORG TABLE ... APPLY (PURGE)` + `VACUUM` maintenance step, run on a cadence that satisfies your organization's regulatory deadline (e.g., within 30 days of a request) without leaving the retention guard permanently disabled.

---

## Lab Checklist

- [ ] Created and applied a row filter with a group-based admin bypass
- [ ] Created and applied a column mask with a group-based reveal condition
- [ ] Reproduced the "drop function before dropping the mask" failure and fixed it correctly
- [ ] Demonstrated hashing, tokenization, suppression, and generalization side by side
- [ ] Built a batch transform that hashes/drops PII before persisting to a silver table
- [ ] Ran the full purge lifecycle: `DELETE` → confirmed time-travel gap → `REORG TABLE ... APPLY (PURGE)` → `VACUUM`
- [ ] Triggered and understood the `VACUUM RETAIN 0 HOURS` safety guard
- [ ] (Stretch) Designed a scheduled, control-table-driven GDPR deletion job

---

## Cross-References

- Day 6/10/11: Change Data Feed — used to propagate deletes downstream efficiently.
- Day 17: Quarantine/expectations — for detecting malformed PII patterns before they land in a governed table.
- Day 18: The ACL/`SELECT` grant that must succeed before any row filter or column mask is even evaluated.
- Day 2/25: Declarative Automation Bundles — for scheduling the purge job itself.
