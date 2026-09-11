# Day 19 — Quiz: Row Filters, Column Masks, Anonymization & Data Purging

**Objective coverage:** Section 7 (10%)

---

## Question 1
**Objective:** Row filter mechanics.

A row filter function returns `NULL` for a given row. What happens to that row?

A. It is included, since `NULL` is treated as unknown/permit
B. It is excluded — `FALSE` or `NULL` both filter the row out
C. The query fails with an error
D. It is included only for admin users

---

## Question 2
**Objective:** Privilege requirements for filters/masks.

To attach an existing row filter function to an existing table you don't own, what must you additionally hold on the table itself?

A. Nothing — `EXECUTE` on the function is sufficient
B. `MANAGE` and `SELECT` on the table (or be the owner)
C. `ALL PRIVILEGES` on the schema
D. `CREATE TABLE` on the schema

---

## Question 3
**Objective:** Drop-order trap.

A team drops the masking function used by a table's column mask before removing the mask from the table. What happens?

A. Nothing — Unity Catalog automatically removes the mask reference
B. The table becomes inaccessible until the function is recreated or the mask reference is otherwise resolved
C. The column silently reverts to showing unmasked data
D. Only `SELECT *` queries fail; column-specific queries still work

---

## Question 4
**Objective:** Compute compatibility for masked/filtered tables.

A column-masked table works fine from a SQL warehouse but fails to read for a user on a Dedicated (single-user) access mode cluster running DBR 14.3. What's the most likely explanation?

A. The user lacks `SELECT` on the table
B. Dedicated access mode requires DBR 15.4 LTS or above to read row-filtered/masked tables; DBR 14.3 is below that threshold
C. Row filters/masks are SQL-warehouse-only and never work on any cluster
D. The masking function needs to be re-registered per cluster

---

## Question 5
**Objective:** Hashing vs. tokenization.

A fraud investigation team needs the ability to recover an original credit card number from a stored value when investigating a flagged transaction. Which anonymization technique should be used for that field?

A. Hashing with `sha2`
B. Tokenization with `aes_encrypt`/`aes_decrypt` and a securely stored key
C. Suppression
D. Generalization

---

## Question 6
**Objective:** Generalization vs. suppression.

A reporting team needs to analyze customer age distributions in 10-year bands but must not see exact birthdates. Which technique fits?

A. Suppression — remove the birthdate field entirely
B. Generalization — e.g., bucket into decades via a computed expression
C. Hashing the birthdate
D. Tokenizing the birthdate

---

## Question 7
**Objective:** Streaming + batch masking placement.

A table has a column mask applied. Does a streaming write path into that same table need separate masking logic inside its `foreachBatch`/write logic to enforce the mask for readers?

A. Yes — masks only apply to batch-inserted data
B. No — the mask is enforced at read time on the table object regardless of how the data was written (batch or streaming)
C. Yes, but only for Structured Streaming, not Lakeflow Declarative Pipelines
D. No, but only if Change Data Feed is enabled

---

## Question 8
**Objective:** Masking vs. deletion for GDPR compliance.

A regulator's erasure request requires that a data subject's information can never be recovered under any circumstance, including by internal admins. Which approach satisfies this?

A. Apply a column mask that hides the value from non-admin users
B. Hash the value with `sha2`
C. Physically delete and purge the data: `DELETE` → `REORG TABLE ... APPLY (PURGE)` → `VACUUM`
D. Tokenize the value so only authorized systems can decrypt it

---

## Question 9
**Objective:** Deletion vectors and physical purge.

A table has deletion vectors enabled. After running `DELETE FROM t WHERE id = 5`, is the row's data physically removed from the underlying files?

A. Yes, immediately
B. No — the row is only marked as deleted; `REORG TABLE ... APPLY (PURGE)` is required to physically rewrite the files
C. No, and there's no way to physically remove it later
D. Yes, but only after 30 days automatically

---

## Question 10
**Objective:** Time travel as a compliance gap.

After running `DELETE` and `REORG TABLE ... APPLY (PURGE)` on a table, can the deleted row still be retrieved via `SELECT ... VERSION AS OF <earlier version>`?

A. No, `REORG TABLE ... APPLY (PURGE)` alone fully resolves this
B. Yes, until `VACUUM` removes the old file versions containing that data, per the retention window
C. No, `DELETE` alone already prevents time travel from seeing it
D. Time travel is disabled automatically the moment `REORG TABLE` runs

---

## Question 11
**Objective:** VACUUM safety guard.

Running `VACUUM my_table RETAIN 0 HOURS` fails with an error by default. What must be done to proceed for an urgent compliance deadline?

A. Nothing can be done — 0-hour retention is never allowed
B. Set `spark.databricks.delta.retentionDurationCheck.enabled = false` to override the safety guard, understanding this breaks time travel for that table
C. Run `OPTIMIZE` first, which automatically permits 0-hour retention
D. Use `TRUNCATE TABLE` instead, which bypasses the guard

---

## Question 12
**Objective:** Purge propagation across layers.

A GDPR deletion request is processed by deleting the affected row from the bronze table only. Is the organization now compliant?

A. Yes — bronze is the system of record
B. No — the delete must propagate to silver/gold (e.g., via Change Data Feed) and also cover any non-Delta upstream sources (Kafka, raw files)
C. Yes, as long as `VACUUM` is run on bronze
D. No, but only because silver/gold need a full table drop and recreate

---

## Question 13
**Objective:** Row filter applicability across object types.

Which of the following can have a row filter attached, per Databricks documentation?

A. Managed tables only
B. Managed tables and views only
C. Tables, materialized views, and streaming tables
D. External tables only

---

## Question 14
**Objective:** Column mask with extra columns.

A masking function needs to compare the caller's department against the row's department to decide whether to reveal a salary value. Which syntax element allows passing an additional column into the mask function beyond the masked column itself?

A. `ALTER TABLE t ALTER COLUMN salary SET MASK dept_mask;` (no extra syntax needed)
B. `ALTER TABLE t ALTER COLUMN salary SET MASK dept_mask USING COLUMNS (department);`
C. `ALTER TABLE t ADD COLUMN MASK dept_mask REFERENCING department;`
D. Column masks cannot reference other columns — only the masked column itself

---

## Question 15
**Objective:** Suppression via masking.

A column mask function is defined as `RETURN CAST(NULL AS STRING);` with no conditional logic and applied to a `notes` column. What is the effect for every caller, including admins?

A. Admins see the real value; everyone else sees `NULL`
B. Every caller, with no exceptions in the function logic, sees `NULL` — this is a blunt suppression pattern, not a conditional mask
C. The mask is ignored because there's no `is_account_group_member` check
D. The column is dropped entirely from the schema

---

## Answer Key

### Q1: B
`FALSE` or `NULL` both cause the row to be filtered out — there's no "unknown = permit" behavior in row filter semantics.

### Q2: B
`EXECUTE`+`USE SCHEMA`+`USE CATALOG` get you access to attach the function, but altering an **existing** table also requires ownership, or `MANAGE` + `SELECT` on that table specifically.

### Q3: B
The correct order is always `ALTER TABLE ... DROP MASK` (or `DROP ROW FILTER`) **before** `DROP FUNCTION`; dropping the function first leaves the table unable to resolve its mask/filter reference.

### Q4: B
Dedicated access mode requires DBR 15.4 LTS or above to read row-filtered/masked tables; below that threshold, reads simply aren't supported on that compute type — this is a version gap, not a grant problem.

### Q5: B
Hashing is one-way and can never be reversed by design — wrong choice whenever recovery is a requirement. Tokenization with a securely stored key is exactly what allows controlled, auditable reversal.

### Q6: B
Suppression would remove analytical value entirely; the requirement explicitly wants *some* usable signal at reduced precision, which is generalization's definition.

### Q7: B
Row filters/column masks are enforced at read time on the table object itself, regardless of how the data got there — no separate masking logic is needed in a streaming write path if the mask lives on the table.

### Q8: C
Masking/hashing/tokenization all leave the underlying value recoverable by *someone* with sufficient access or technique — true "cannot be recovered by anyone" erasure requires physical deletion and purge, not obfuscation.

### Q9: B
With deletion vectors enabled, `DELETE` only marks the row as deleted in a side-file; the bytes remain in the data files until `REORG TABLE ... APPLY (PURGE)` rewrites them.

### Q10: B
Old file versions — including the "deleted" data — remain accessible via time travel until `VACUUM` physically removes them past the configured retention threshold.

### Q11: B
The retention-duration safety check must be explicitly disabled to allow an unsafely low retention period; doing so breaks time travel for that table, so it should be a deliberate, temporary override, not a default setting.

### Q12: B
GDPR/CCPA obligations apply across every layer the data touches, plus any non-Delta upstream sources — deleting only from bronze without propagating downstream or addressing upstream sources leaves the organization non-compliant.

### Q13: C
Row filters are documented as attachable to tables, materialized views, and streaming tables — not just plain managed tables.

### Q14: B
`USING COLUMNS (...)` is the syntax for passing additional columns into a masking function beyond the column being masked itself.

### Q15: B
Without a conditional branch (like an `is_account_group_member` check), the mask applies uniformly to every caller with no exceptions — a deliberate full-suppression pattern, useful when a field genuinely should never be visible to anyone via normal query access.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Easy | Row filter `FALSE`/`NULL` semantics |
| 2 | Medium | Privilege requirements to modify existing table's filter/mask |
| 3 | Hard | Drop-order trap (mask before function) |
| 4 | Hard | Compute access-mode/DBR version gate |
| 5 | Medium | Hashing vs. tokenization reversibility |
| 6 | Medium | Generalization vs. suppression |
| 7 | Medium | Read-time enforcement regardless of batch/streaming write path |
| 8 | Hard | Masking is not a substitute for physical deletion |
| 9 | Medium | Deletion vectors don't physically remove data alone |
| 10 | Medium | Time travel as a compliance gap before `VACUUM` |
| 11 | Easy | `VACUUM RETAIN 0 HOURS` safety guard override |
| 12 | Medium | Purge propagation across medallion layers and upstream sources |
| 13 | Medium | Row filter applicability to MVs and streaming tables |
| 14 | Easy | `USING COLUMNS` syntax for multi-column masks |
| 15 | Easy | Unconditional suppression mask behavior |
