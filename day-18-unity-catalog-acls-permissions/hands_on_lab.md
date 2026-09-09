# Day 18 — Hands-On Lab: Unity Catalog ACLs and Permissions

> ⚠️ **Correction notice:** this lab has been corrected against current Databricks documentation. Two things in an earlier draft were wrong and have been fixed throughout: (1) **`DENY` does not exist in Unity Catalog** — it's a legacy Hive Metastore statement only, so every "DENY precedence" step below has been replaced with the correct structural/row-filter approach; (2) **the traversal privilege is `USE CATALOG` / `USE SCHEMA`**, not a generic `USAGE` — every `GRANT USAGE ...` example has been corrected.

## Lab Objectives

1. Explore the Unity Catalog hierarchy (catalog → schema → table).
2. Grant and revoke privileges on catalogs, schemas, and tables using correct UC syntax.
3. Reproduce the "missing link in the traversal chain" failure firsthand.
4. Use `SHOW GRANTS` correctly to verify effective permissions.
5. Confirm there is no `DENY` in Unity Catalog, and practice the correct alternative for restricting a subset of a broad grant.
6. Distinguish workspace ACLs from Unity Catalog ACLs.
7. Configure least-privilege service principal access.
8. Add and query metadata (`COMMENT`, `DESCRIBE`, tags, `information_schema`) for discoverability, and observe metadata loss on `DEEP CLONE`.

**Environment note:** this lab needs a Unity-Catalog-enabled workspace with either admin rights or a sandbox catalog you own. **Community Edition historically has limited or no Unity Catalog support** (run `SHOW CATALOGS;` — if you only see `hive_metastore`/`samples` and nothing you can create under, UC isn't available to you). If that's your situation, run every statement below anyway as **syntax practice** — they are valid Databricks SQL and Databricks Runtime commands; where a step needs admin rights you don't have, read the expected result rather than executing it, and treat `SHOW GRANTS` output for `main`/`samples` as your only live verification. Where possible, prefer actually running the read-only inspection steps (Steps 1–2) against whatever catalog you do have access to, even if it's just `main`.

---

## Step 1 — Explore the Unity Catalog Hierarchy

### 1a. List catalogs, schemas, tables
```python
spark.sql("SHOW CATALOGS").display()
spark.sql("SHOW SCHEMAS IN prod").display()
spark.sql("SHOW TABLES IN prod.sales").display()
```
**What to observe:** the three-level namespace (catalog → schema → table). Note the default catalogs available on any UC-enabled workspace: `main`, `system`, and each catalog's `information_schema`.

### 1b. Who am I, and what can I already reach?
```python
current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")

spark.sql("SHOW CATALOGS").display()   # catalogs you have at least USE CATALOG on
```

---

## Step 2 — Inspect Existing Grants

### 2a. Grants on a table
```python
spark.sql("SHOW GRANTS ON TABLE prod.sales.customers").display()
```
Look for: principal, `actionType` (the privilege, e.g. `SELECT`), `objectType`, `objectKey`.

### 2b. Grants on a schema and a catalog
```python
spark.sql("SHOW GRANTS ON SCHEMA prod.sales").display()
spark.sql("SHOW GRANTS ON CATALOG prod").display()
```
**What to observe:** catalog- and schema-level grants are what cascade to every current and future object beneath them — if a table shows no direct grants of its own, check the levels above it before assuming no one has access.

### 2c. Grants scoped to yourself
```python
# SHOW GRANTS requires an object — there is no bare "show everything for me" form.
# Check yourself against a specific object you care about:
spark.sql(f"SHOW GRANTS `{current_user}` ON CATALOG prod").display()
spark.sql(f"SHOW GRANTS `{current_user}` ON SCHEMA prod.sales").display()
```
**Correction vs. some older notes:** the real syntax is `SHOW GRANTS [principal] ON <securable_object>` — the `securable_object` is required. There is no standalone `SHOW GRANTS FOR <principal>` that lists every grant across every object account-wide; to get a full account-wide picture of one principal's access, you'd either check object-by-object like this, or query `system.access.audit` / `information_schema.*_privileges` views for the catalogs you care about (Day 21 territory).

---

## Step 3 — GRANT Syntax Practice

Run for syntax familiarity; these require ownership/`MANAGE`/admin rights to actually take effect.

```python
# Traversal + read for an analytics group
spark.sql("GRANT USE CATALOG ON CATALOG prod TO `analysts`")
spark.sql("GRANT USE SCHEMA ON SCHEMA prod.sales TO `analysts`")
spark.sql("GRANT SELECT ON SCHEMA prod.sales TO `analysts`")   # schema-level SELECT cascades to all current+future tables

# Write access for an ETL service principal, staging schema only
spark.sql("GRANT USE CATALOG ON CATALOG prod TO `etl-pipeline-sp`")
spark.sql("GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA prod.staging TO `etl-pipeline-sp`")

# Specific table-level grant
spark.sql("GRANT SELECT, MODIFY ON TABLE prod.sales.customers TO `etl-writers`")
```

---

## Step 4 — 💥 Break It On Purpose: The Missing Traversal Link

**Objective:** reproduce the #1 tested failure mode — a table-level grant that doesn't work because a level above it was never granted.

### 4a. Grant SELECT directly on a table, but skip USE SCHEMA
```python
spark.sql("GRANT SELECT ON TABLE prod.bronze.raw_orders TO `analysts`")
# Deliberately do NOT grant USE SCHEMA on prod.bronze yet
```
**Predict first:** can a member of `analysts` query `prod.bronze.raw_orders` right now?

**Verify:**
```python
spark.sql("SHOW GRANTS ON TABLE prod.bronze.raw_orders").display()
spark.sql("SHOW GRANTS ON SCHEMA prod.bronze").display()
# As a member of analysts (or reasoning through the grants shown above):
try:
    spark.sql("SELECT * FROM prod.bronze.raw_orders LIMIT 5").display()
except Exception as e:
    print("Expected failure — missing USE SCHEMA on prod.bronze:")
    print(str(e)[:400])
```
**Answer:** No — `SELECT` alone is not sufficient without `USE CATALOG` on `prod` and `USE SCHEMA` on `prod.bronze` also being granted. Fix it:
```python
spark.sql("GRANT USE SCHEMA ON SCHEMA prod.bronze TO `analysts`")
# Now the full chain is complete: USE CATALOG (Step 3) + USE SCHEMA (this) + SELECT (4a) = access works
```

### 4b. Confirm there's no `DENY` to fall back on
```python
# This statement is intentionally NOT valid for a Unity Catalog object —
# DENY only applies to the legacy hive_metastore catalog:
# spark.sql("DENY SELECT ON TABLE prod.hr.salaries TO `analysts`")   # DO NOT RUN — will fail on a UC object

spark.sql("SHOW GRANTS ON TABLE prod.hr.salaries").display()
```
**What to observe:** the only tools you have to remove or restrict access on a UC object are `REVOKE`, restructuring which schema/catalog something lives in, or row filters/column masks (Day 19). If a colleague's notes or a quiz question shows `DENY` being used against a Unity Catalog table, that's the same error this lab just corrected.

---

## Step 5 — Verify Forward-Looking Inheritance

```python
# Grant at schema level
spark.sql("GRANT SELECT ON SCHEMA prod.sales TO `analysts`")

# Create a brand-new table AFTER the grant
spark.sql("CREATE TABLE IF NOT EXISTS prod.sales.new_table (id INT, val STRING)")

# Does analysts have SELECT on it without any new GRANT statement?
spark.sql("SHOW GRANTS ON TABLE prod.sales.new_table").display()
```
**What to observe:** the schema-level grant applies automatically to `new_table` even though it didn't exist when the `GRANT` ran — this is the forward-looking inheritance behavior. No re-grant needed.

### 5a. Revoke scoping
```python
spark.sql("GRANT SELECT ON TABLE prod.sales.customers TO `analysts`")   # separate, explicit table-level grant
spark.sql("REVOKE SELECT ON SCHEMA prod.sales FROM `analysts`")          # revoke the schema-level grant only

spark.sql("SHOW GRANTS ON TABLE prod.sales.customers").display()
```
**Predict then verify:** does `analysts` still have `SELECT` on `customers` after the schema-level revoke?
**Answer:** Yes — the explicit table-level grant is a separate grant record and survives a revoke issued at a different (schema) level.

---

## Step 6 — Service Principal Least-Privilege Configuration

```python
# Read-only across prod, write access limited to staging
spark.sql("GRANT USE CATALOG ON CATALOG prod TO `pipeline-sp`")
spark.sql("GRANT USE SCHEMA, SELECT ON CATALOG prod TO `pipeline-sp`")
spark.sql("GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA prod.staging TO `pipeline-sp`")

spark.sql("SHOW GRANTS `pipeline-sp` ON CATALOG prod").display()
spark.sql("SHOW GRANTS `pipeline-sp` ON SCHEMA prod.staging").display()
```
**What to observe:** the service principal has broad read (`SELECT` at catalog level, cascading) but write (`MODIFY`, `CREATE TABLE`) scoped to only `staging` — the standard least-privilege pattern for a production ETL identity, and the correct answer whenever a scenario asks for a pipeline account that should "read everything, write only to its own landing zone."

---

## Step 7 — Workspace ACLs vs. Unity Catalog ACLs

### 7a. UC-managed objects — queryable via SQL
```python
spark.sql("""
    SELECT table_catalog, table_schema, table_name, table_type
    FROM prod.information_schema.tables
    LIMIT 20
""").display()
```

### 7b. Workspace-managed objects — not queryable via GRANT/SHOW GRANTS SQL
Notebooks, jobs, clusters, dashboards, and legacy MLflow experiments use **workspace permissions**, set via:
- Workspace UI → object → **Permissions** tab, or
- REST API: `PATCH /api/2.0/permissions/{request_object_type}/{request_object_id}`

There is no SQL `GRANT`/`SHOW GRANTS` equivalent for these — that's the core distinction to internalize. A user can have full workspace "Can Manage" on a notebook and still get an access-denied error the moment that notebook's code touches a UC table it has no grant on.

### 7c. External locations and credentials (admin-only, syntax practice)
```python
try:
    spark.sql("SHOW EXTERNAL LOCATIONS").display()
except Exception as e:
    print("Requires admin / not available on this tier:", str(e)[:200])
```

---

## Step 8 — Data Discoverability: Metadata, Comments, and Tags

### 8a. View existing metadata
```python
spark.sql("DESCRIBE CATALOG prod").display()
spark.sql("DESCRIBE SCHEMA prod.sales").display()
spark.sql("DESCRIBE TABLE prod.sales.customers").display()
spark.sql("DESCRIBE TABLE prod.sales.customers COLUMN email").display()
spark.sql("DESCRIBE DETAIL prod.sales.customers").display()   # includes TBLPROPERTIES, format, size
```

### 8b. Query information_schema for governance gaps
```python
spark.sql("""
    SELECT table_catalog, table_schema, table_name, comment
    FROM prod.information_schema.tables
    WHERE comment IS NULL OR comment = ''
    ORDER BY table_schema, table_name
""").display()
print("Rows returned = tables missing descriptions = governance gaps to close")
```

### 8c. Add comments and tags
```python
spark.sql("""
    ALTER TABLE prod.sales.customers
    SET COMMENT 'All customer records — updated nightly from CRM system'
""")
spark.sql("""
    ALTER TABLE prod.sales.customers
    ALTER COLUMN email SET COMMENT 'Customer email — PII, masked for analysts'
""")
spark.sql("ALTER TABLE prod.sales.customers SET TAGS ('pii' = 'true', 'department' = 'sales')")

spark.sql("""
    SELECT * FROM prod.information_schema.table_tags WHERE tag_name = 'pii'
""").display()
```

### 8d. 💥 Break it on purpose: tags/comments lost on clone
```python
spark.sql("DEEP CLONE prod.sales.customers TO prod.staging.customers_clone")   # or CREATE TABLE ... AS SELECT

spark.sql("""
    SELECT * FROM prod.information_schema.table_tags
    WHERE table_name = 'customers_clone'
""").display()
```
**Predict then verify:** does the cloned table show the `pii`/`department` tags?
**Answer:** No — empty result. Tags and comments are **not** preserved by `DEEP CLONE` or `CTAS`; only the structure/data is copied. This is a real governance gap teams hit in production — any pipeline step that clones or CTAS-es a sensitive table must explicitly reapply tags/comments as part of that same step, or the clone silently loses its PII classification.

---

## Step 9 — Common Permission-Error Scenarios

### 9a. Missing USE CATALOG
```python
try:
    spark.sql("SELECT * FROM some_catalog_you_cannot_use.some_schema.some_table LIMIT 1").display()
except Exception as e:
    print("Expected error — no USE CATALOG:")
    print(str(e)[:400])
```

### 9b. Attempting to grant without sufficient rights
```python
try:
    spark.sql("GRANT SELECT ON TABLE prod.sales.customers TO `random_group`")
except Exception as e:
    print("Expected — you need to be owner, have MANAGE, or be admin to grant:")
    print(str(e)[:300])
```

### 9c. Writing without MODIFY
```python
try:
    spark.sql("INSERT INTO prod.sales.customers VALUES (999, 'test@example.com')")
except Exception as e:
    print("Expected — no MODIFY privilege:")
    print(str(e)[:300])
```

---

## Stretch Task: Design a Full ACL Configuration for a New Schema

Scenario: design (write out the SQL, execute what you can) grants for `prod.analytics`:
- `analysts_group` — read-only across all of `prod`.
- `etl_pipeline_sp` — read all of `prod`, write only to `prod.analytics`.
- `data_science_team` — read `prod.analytics` only (not the rest of `prod`).
- `hr_admin_group` — full control of `prod.hr` only, no access elsewhere.

```sql
-- analysts_group: read-only everywhere
GRANT USE CATALOG ON CATALOG prod TO `analysts_group`;
GRANT USE SCHEMA, SELECT ON CATALOG prod TO `analysts_group`;

-- etl_pipeline_sp: read everywhere, write only to analytics
GRANT USE CATALOG ON CATALOG prod TO `etl_pipeline_sp`;
GRANT USE SCHEMA, SELECT ON CATALOG prod TO `etl_pipeline_sp`;
GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA prod.analytics TO `etl_pipeline_sp`;

-- data_science_team: analytics schema only — note it must NOT get USE CATALOG-level SELECT,
-- only schema-scoped access, to avoid leaking the rest of prod
GRANT USE CATALOG ON CATALOG prod TO `data_science_team`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.analytics TO `data_science_team`;

-- hr_admin_group: full control of hr only
GRANT USE CATALOG ON CATALOG prod TO `hr_admin_group`;
GRANT ALL PRIVILEGES ON SCHEMA prod.hr TO `hr_admin_group`;
```
**Think it through:** since there's no `DENY`, how do you guarantee `data_science_team` genuinely cannot see `prod.hr` or `prod.sales`? (Answer: by never granting them anything beyond `USE CATALOG` + the one schema — the absence of a grant *is* the restriction, since UC is additive-only.)

---

## Lab Checklist

- [ ] Explored the three-level UC hierarchy (catalog → schema → table)
- [ ] Used `SHOW GRANTS ON <object>` correctly (object is required — no bare "show all for me")
- [ ] Reproduced the missing-traversal-link failure and fixed it
- [ ] Confirmed `DENY` does not work against a Unity Catalog object
- [ ] Verified forward-looking inheritance on a newly created table
- [ ] Verified a schema-level revoke doesn't remove a separate table-level grant
- [ ] Configured least-privilege service principal grants
- [ ] Distinguished workspace ACLs from Unity Catalog ACLs
- [ ] Added comments, tags, and `TBLPROPERTIES`; queried `information_schema` for gaps
- [ ] Observed tags/comments not surviving `DEEP CLONE`
- [ ] Triggered and interpreted common permission errors
- [ ] (Stretch) Designed a full multi-group ACL configuration using only grants (no deny)

---

## Cross-References
- Day 19: Row filters and column masks — enforced *after* the ACL check on top of whatever `SELECT` allows through.
- Day 20: Delta Sharing / Lakehouse Federation permissions (`CREATE SHARE`, `USE CONNECTION`).
- Day 21: `system.access.audit` for auditing grant/revoke history over time.
- Day 2 / 25: Service principal `run_as` pattern in Declarative Automation Bundles.
