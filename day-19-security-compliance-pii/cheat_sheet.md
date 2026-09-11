# Day 19 — Cheat Sheet: Row Filters, Column Masks, Anonymization & Data Purging

## Two-Gate Model

```
Query → Gate 1: ACL SELECT grant (Day 18) → Gate 2: row filter / column mask (Day 19)
```
Masks/filters never substitute for a missing grant — a grant must succeed first.

---

## Row Filter

```sql
CREATE FUNCTION f(col TYPE) RETURN {boolean expression};
ALTER TABLE t SET ROW FILTER f ON (col);
ALTER TABLE t DROP ROW FILTER;   -- do this BEFORE dropping the function
```
- `FALSE`/`NULL` → row excluded.
- Applies to: tables, materialized views, streaming tables.

## Column Mask

```sql
CREATE FUNCTION f(col TYPE) RETURN {expression, same type as col};
ALTER TABLE t ALTER COLUMN col SET MASK f [USING COLUMNS (extra_col, ...)];
ALTER TABLE t ALTER COLUMN col DROP MASK;   -- do this BEFORE dropping the function
```

---

## Privilege Checklist

| Action | Needs |
|---|---|
| Attach filter/mask to a table | `EXECUTE` (function) + `USE SCHEMA` + `USE CATALOG` |
| Add filter/mask on a **new** table | `CREATE TABLE` on schema |
| Modify filter/mask on an **existing** table | Owner, or `MANAGE` + `SELECT` |

## Compute Compatibility

| Compute | Can read masked/filtered tables? |
|---|---|
| SQL warehouse | ✅ |
| Standard access mode, DBR 12.2+ | ✅ |
| Dedicated access mode, DBR 15.4+ | ✅ |
| Dedicated access mode, DBR ≤15.3 | ❌ |

---

## Anonymization / Pseudonymization

| Technique | Reversible? | Function |
|---|---|---|
| Hashing | No | `sha2(col, 256)` |
| Tokenization | Yes (with key) | `aes_encrypt` / `aes_decrypt` |
| Suppression | No (removed) | Mask returning `NULL`/constant |
| Generalization | No (reduced precision) | `date_trunc`, `substring`, `round` |

**Rule of thumb:** need to recover the original value later? → tokenization. Need a stable but irreversible key? → hashing. Value has no downstream use? → suppression. Want reduced-precision analytics? → generalization.

---

## Compliant Batch + Streaming Masking

- Mask/hash **before** persisting if the raw value must never exist anywhere, even for admins.
- Use a **table-level mask** if some roles legitimately need the raw value under audit.
- Masks/filters enforce identically for batch and streaming writers — **no duplicate logic per write path** needed when the mask lives on the table.

---

## Data Purge Lifecycle (true GDPR/CCPA erasure)

```sql
DELETE FROM t WHERE id = ...;
REORG TABLE t APPLY (PURGE);
VACUUM t;                                   -- normal cadence

-- Urgent compliance override:
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM t RETAIN 0 HOURS;
SET spark.databricks.delta.retentionDurationCheck.enabled = true;
```
- Applies to tables, materialized views, and streaming tables.
- `DELETE` with deletion vectors ≠ physical removal.
- Time travel keeps "deleted" rows visible until `VACUUM` passes retention.
- Must propagate **bronze → silver → gold** (via CDF) and cover **non-Delta upstream sources**.

---

## Exam Trap Shortlist

1. Masking ≠ deletion — true erasure needs physical purge, not obfuscation.
2. `DELETE` with deletion vectors ≠ physical removal — needs `REORG TABLE ... APPLY (PURGE)`.
3. Time travel keeps "deleted" data visible until `VACUUM` passes retention.
4. Drop the mask/filter from the table before dropping the function.
5. Dedicated-mode clusters below DBR 15.4 can't read masked/filtered tables.
6. Hashing = one-way; tokenization = reversible with a key — pick based on recovery need.
7. Generalization keeps analytical value at reduced precision; suppression removes it.
8. Purge must propagate through every layer and every upstream non-Delta source.
9. Row filters/masks apply identically to batch and streaming writes — no per-path duplication needed.
