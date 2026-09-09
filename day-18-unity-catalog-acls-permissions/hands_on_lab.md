# Day 18 — Hands-On Lab: Unity Catalog ACLs and Permissions

## Lab Objectives

1. Explore the Unity Catalog hierarchy (catalog → schema → table)
2. Grant and revoke privileges on catalogs, schemas, and tables
3. Observe how inheritance flows through the hierarchy
4. Use SHOW GRANTS to verify effective permissions
5. Test DENY precedence over GRANT
6. Observe the difference between workspace ACLs and Unity Catalog ACLs
7. Add and query metadata (COMMENT, DESCRIBE, tags, information_schema) for data discoverability

**Note:** Full hands-on with real ACL changes requires admin access to a Unity Catalog-enabled workspace. This lab provides syntax that runs against your workspace's Unity Catalog objects. Community Edition has limited UC support (may not support creating new catalogs/schemas on CE). If CE does not support the full ACL syntax, treat steps as read-only exploration of existing objects.

---

## Step 1 — Explore the Unity Catalog Hierarchy

**Objective:** Map the three-level namespace in your workspace.

### 1a. List all catalogs

```python
catalogs = spark.sql("SHOW CATALOGS")
catalogs.display()
```

### 1b. List schemas within a catalog

```python
schemas = spark.sql("SHOW SCHEMAS IN prod")
schemas.display()
```

### 1c. List tables within a schema

```python
tables = spark.sql("SHOW TABLES IN prod.sales")
tables.display()
```

**What to observe:** The three-level hierarchy (catalog → schema → table) is visible in the results. Note the default catalogs: `main`, `system`, `information_schema`.

### 1d. View the current user's effective catalogs

```python
# Who am I?
current_user = spark.sql("SELECT current_user()").collect()[0][0]
print(f"Current user: {current_user}")

# What catalogs does this user have USAGE on?
spark.sql("SHOW CATALOGS").filter("catalogName IN ('prod', 'main', 'samples')").display()
```

---

## Step 2 — Inspect Existing Grants

**Objective:** Read existing ACLs to understand the security posture of a catalog.

### 2a. Show all grants on a specific table

```python
grants_table = spark.sql("SHOW GRANTS ON TABLE prod.sales.customers")
grants_table.display()
```

**What to look for:** Principals (users, groups, service principals), privileges (SELECT, MODIFY, etc.), and inherited grants.

### 2b. Show grants for a specific principal

```python
# Replace with your email or group name
my_principal = spark.sql("SELECT current_user()").collect()[0][0]
grants_user = spark.sql(f"SHOW GRANTS FOR {my_principal}")
grants_user.display()
```

### 2c. Show grants on a schema (to see inherited vs direct grants)

```python
grants_schema = spark.sql("SHOW GRANTS ON SCHEMA prod.sales")
grants_schema.display()
```

**What to observe:** Grants at the schema level apply to all tables in the schema. Table-level grants are explicit and shown separately.

### 2d. Show catalog-level grants

```python
grants_catalog = spark.sql("SHOW GRANTS ON CATALOG prod")
grants_catalog.display()
```

**What to observe:** Catalog-level grants cascade to all schemas and tables. If a table has no direct grants, check the catalog and schema levels.

---

## Step 3 — GRANT Syntax Practice (Syntax-Only — No Actual Changes in CE)

**Objective:** Familiarize yourself with the correct GRANT syntax.

These commands require admin privileges to execute. On CE, run them for syntax familiarity — they will error without admin permissions.

```python
# Grant SELECT on a catalog to a group
spark.sql("""
    GRANT SELECT ON CATALOG prod
    TO `groups/data-analysts`
""")

# Grant MODIFY (INSERT, UPDATE, DELETE) on a schema to a service account
spark.sql("""
    GRANT MODIFY ON SCHEMA prod.sales
    TO `service-principal:12345-abcd-6789`
""")

# Grant USAGE + CREATE TABLE on a schema for ETL pipeline
spark.sql("""
    GRANT USAGE ON CATALOG prod TO `service-principal:etl-pipeline-id`
""")
spark.sql("""
    GRANT CREATE TABLE ON SCHEMA prod.sales TO `service-principal:etl-pipeline-id`
""")

# Grant specific privileges
spark.sql("""
    GRANT SELECT, MODIFY ON TABLE prod.sales.customers
    TO `groups/etl-writers`
""")
```

**Note:** If you have admin access, execute these against your test workspace. If not, move to Step 4 to verify with SHOW GRANTS.

---

## Step 4 — DENY vs GRANT Precedence

**Objective:** Observe that DENY always overrides GRANT, even when the principal is in multiple groups.

### 4a. Scenario: Deny a sensitive table within an open catalog grant

```python
# Assumes: all_employees group has SELECT on CATALOG prod
# but needs to be denied access to prod.hr.salaries

# Syntax (requires admin):
spark.sql("""
    DENY SELECT ON TABLE prod.hr.salaries
    TO `groups/all_employees`
""")

# Check the effective grants on the denied table
spark.sql("SHOW GRANTS ON TABLE prod.hr.salaries").display()
```

### 4b. Verify DENY appears alongside GRANT

```python
# The table should show both the DENY and the inherited GRANT from catalog
grants_sensitive = spark.sql("SHOW GRANTS ON TABLE prod.hr.salaries")
grants_sensitive.display()
```

**What to observe:** DENY appears as a distinct privilege entry. Even though `all_employees` has SELECT on `prod` (catalog), the DENY on `prod.hr.salaries` takes precedence for that specific table.

### 4c. Test with a user in multiple groups

If you are in multiple groups (one granted, one denied), verify access:

```python
# Current user — check which groups they belong to
user_groups = spark.sql("SELECT is_member('groups/analysts') as in_analysts")
user_groups.display()
```

---

## Step 5 — Verify Inheritance Behavior

**Objective:** Confirm that catalog-level grants flow to all schemas and tables.

### 5a. Grant USAGE on catalog only — verify schema access works

```python
# This is implicit if USAGE on catalog is granted
# Check: can you list schemas in prod?
spark.sql("SHOW SCHEMAS IN prod").display()
```

### 5b. Without USAGE on catalog — verify failure scenario

If your user has explicit grants at schema level but NOT at catalog level:

```python
# Attempt to access a schema in a catalog you don't have USAGE on
# (replace catalog_name with one you may not have access to)
try:
    result = spark.sql("SHOW TABLES IN restricted_catalog.dont_read")
    result.display()
except Exception as e:
    print("Expected error — no USAGE on catalog:")
    print(str(e)[:300])
```

**What to observe:** Without USAGE on the catalog, traversal to the schema fails — even if the user has SELECT on the schema directly. Catalog-level USAGE is a prerequisite.

### 5c. Revoke and re-grant to observe behavior

```python
# Syntax only — requires admin
# Revoke a previously granted privilege
spark.sql("""
    REVOKE SELECT ON SCHEMA prod.sales
    FROM `user:analyst@example.com`
""")

# Verify the revoke worked
spark.sql("SHOW GRANTS ON SCHEMA prod.sales").display()
```

---

## Step 6 — Service Principal ACL Configuration

**Objective:** Set up a pipeline service account with minimal privileges.

### 6a. Create a service principal (syntax — account admin required)

```python
# Requires account-level permissions
spark.sql("""
    CREATE SERVICE PRINCIPAL IF NOT EXISTS pipeline_sp
    COMMENT 'Production ETL pipeline service account'
""")
```

### 6b. Grant minimal privileges to service principal

```python
# Read-only access for monitoring
spark.sql("""
    GRANT SELECT ON CATALOG prod TO service-principal:pipeline-sp-id
""")

# Write access only to staging schema
spark.sql("""
    GRANT USAGE ON CATALOG prod TO service-principal:pipeline-sp-id
""")
spark.sql("""
    GRANT MODIFY ON SCHEMA prod.staging TO service-principal:pipeline-sp-id
""")
spark.sql("""
    GRANT CREATE TABLE ON SCHEMA prod.staging TO service-principal:pipeline-sp-id
""")
```

### 6c. Verify the service principal's grants

```python
# Show all grants for the service principal
sp_grants = spark.sql("SHOW GRANTS FOR service-principal:pipeline-sp-id")
sp_grants.display()
```

**What to observe:** Service principals have narrow grants — SELECT on prod (read everything) but MODIFY only on staging (write to staging only). This follows the least-privilege principle.

---

## Step 7 — Compare Workspace ACLs vs Unity Catalog ACLs

**Objective:** Understand which objects use which ACL system.

### 7a. Unity Catalog-managed objects

```python
# These objects use Unity Catalog ACLs
# Tables, schemas, catalogs, volumes
uc_objects = spark.sql("""
    SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
    FROM information_schema.tables
    WHERE TABLE_CATALOG IN ('prod', 'main')
    LIMIT 20
""")
uc_objects.display()
```

### 7b. Workspace ACL objects (not directly queryable in SQL)

Notebooks and workspace objects use workspace-level permissions set via:
- Workspace UI → right-click → Permissions
- Workspace API `PATCH /api/2.0/workspace/permissions`

**Key distinction:**
- Unity Catalog ACLs: `GRANT SELECT ON TABLE ... TO ...`
- Workspace ACLs: Permissions set per workspace object via workspace permissions UI

### 7c. External locations and credentials

```python
# Show external locations (requires admin)
try:
    locations = spark.sql("SHOW EXTERNAL LOCATIONS")
    locations.display()
except Exception as e:
    print("External locations require admin access or are unavailable in CE")

# Show credentials
try:
    creds = spark.sql("SHOW CREDENTIALS")
    creds.display()
except Exception as e:
    print("Credentials require admin access or are unavailable in CE")
```

---

## Step 9 — Data Discoverability: Metadata, Comments, and Tags

**Objective:** Add and query metadata to make data self-documenting.

### 9a. View existing descriptions via DESCRIBE

```python
# View catalog description
spark.sql("DESCRIBE CATALOG prod").display()

# View schema description
spark.sql("DESCRIBE SCHEMA prod.sales").display()

# View table description
spark.sql("DESCRIBE TABLE prod.sales.customers").display()

# View column-level descriptions
spark.sql("DESCRIBE TABLE prod.sales.customers COLUMN email").display()
```

### 9b. Query information_schema for all commented objects

```python
# Find all tables in prod that have descriptions
commented_tables = spark.sql("""
    SELECT table_catalog, table_schema, table_name, comment
    FROM information_schema.tables
    WHERE table_catalog = 'prod'
      AND comment IS NOT NULL
    ORDER BY table_schema, table_name
""")
commented_tables.display()

# Find columns with descriptions
commented_cols = spark.sql("""
    SELECT table_catalog, table_schema, table_name, column_name, comment
    FROM information_schema.columns
    WHERE comment IS NOT NULL
    ORDER BY table_catalog, table_schema, table_name
    LIMIT 30
""")
commented_cols.display()
```

### 9c. Add descriptions to a table (requires admin/owner)

```python
# Add table-level comment
spark.sql("""
    ALTER TABLE prod.sales.customers
    SET COMMENT 'All customer records — updated nightly from CRM system'
""")

# Add column-level comments
spark.sql("""
    ALTER TABLE prod.sales.customers
    ALTER COLUMN email SET COMMENT 'Customer email — PII, see data team for access'
""")

spark.sql("""
    ALTER TABLE prod.sales.customers
    ALTER COLUMN created_at SET COMMENT 'Account creation timestamp — UTC'
""")

# Verify with DESCRIBE
spark.sql("DESCRIBE TABLE prod.sales.customers").display()
```

### 9d. Add tags for data classification

```python
# Tag a PII table
spark.sql("""
    ALTER TABLE prod.sales.customers
    SET TAG pii = 'true'
""")

# Set multiple tags at once
spark.sql("""
    ALTER TABLE prod.sales.customers
    SET TAGS (gdpr_classification = 'personal', department = 'crm')
""")

# Query tables by tag (if tags are available in your workspace config)
try:
    tagged_tables = spark.sql("""
        SELECT * FROM information_schema.table_tags
        WHERE tag_name = 'pii'
    """)
    tagged_tables.display()
except Exception as e:
    print("Tags may not be available in this workspace config:")
    print(str(e)[:200])
```

### 9e. View table details (properties + structure)

```python
# DESCRIBE DETAIL shows schema + table properties
spark.sql("DESCRIBE DETAIL prod.sales.customers").display()

# This includes: format, size, numFiles, partitionInfo, and custom TBLPROPERTIES
```

### 9f. Build a data catalog query (discoverability exercise)

```python
# Find all prod tables that lack descriptions (gaps in governance)
missing_descriptions = spark.sql("""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        table_type,
        comment
    FROM information_schema.tables
    WHERE table_catalog = 'prod'
      AND (comment IS NULL OR comment = '')
    ORDER BY table_schema, table_name
    LIMIT 20
""")
missing_descriptions.display()
print("Tables missing descriptions = governance gaps to address")
```

### Break it on purpose: Tags lost on clone

```python
# Create a table with tags
# spark.sql("ALTER TABLE prod.staging.clone_test SET TAG pii = 'true'")

# Deep clone — tags are NOT copied
# spark.sql("DEEP CLONE prod.sales.customers prod.staging.clone_test")

# Verify: tags are gone after clone
# spark.sql("SELECT * FROM information_schema.table_tags WHERE table_name = 'clone_test'")
# Expected: empty result — tags must be reapplied after clone
```

**What to observe:** Tags survive within Unity Catalog but are NOT preserved through DEEP CLONE or CTAS. Comments are also not copied. The clone creates a new table with no metadata. This is a common governance gap — reapply tags/comments programmatically after cloning.

---

## Step 8 — Break It on Purpose: Common Permission Errors

**Objective:** Observe common permission failure scenarios.

### 8a. Missing USAGE on catalog

```python
# Try to access a table in a catalog you don't have USAGE on
try:
    spark.sql("SELECT * FROM system.bogus_catalog.does_not_exist LIMIT 1").display()
except Exception as e:
    print("Error without USAGE on catalog:")
    print(str(e)[:400])
```

### 8b. Trying to grant without admin

```python
# Non-admin: attempt to grant — should fail
try:
    spark.sql("GRANT SELECT ON TABLE prod.sales TO user:random@example.com")
except Exception as e:
    print("Expected: permission denied for non-admin grant attempt")
    print(str(e)[:300])
```

### 8c. Modifying a table without MODIFY privilege

```python
# Try to INSERT without MODIFY
try:
    spark.sql("INSERT INTO prod.sales SELECT 1 as id, 'test' as name")
except Exception as e:
    print("Expected: no MODIFY privilege")
    print(str(e)[:300])
```

---

## Stretch Task: Build a Complete ACL Configuration for a New Schema

Design and (syntax-)document the ACL configuration for a new `prod.analytics` schema:

```python
# Scenario:
# - analysts_group: read-only access to all prod tables
# - etl_pipeline_sp: read all prod, write only to prod.analytics
# - data_science_team: read prod.analytics only
# - hr_data_admin: full access to prod.hr schema only

# 1. Analysts: read-only catalog
# GRANT SELECT ON CATALOG prod TO `groups/analysts_group`;

# 2. ETL pipeline: read all + write analytics
# GRANT USAGE ON CATALOG prod TO service-principal:etl-pipeline;
# GRANT SELECT ON CATALOG prod TO service-principal:etl-pipeline;
# GRANT MODIFY ON SCHEMA prod.analytics TO service-principal:etl-pipeline;
# GRANT CREATE TABLE ON SCHEMA prod.analytics TO service-principal:etl-pipeline;

# 3. Data science: read analytics only
# GRANT USAGE ON CATALOG prod TO `groups/data_science`;
# GRANT SELECT ON SCHEMA prod.analytics TO `groups/data_science`;

# 4. HR admin: full access to hr schema only
# GRANT USAGE ON CATALOG prod TO `groups/hr_admin`;
# GRANT ALL PRIVILEGES ON SCHEMA prod.hr TO `groups/hr_admin`;
```

---

## Lab Checklist

- [ ] Explored Unity Catalog three-level hierarchy (catalog → schema → table)
- [ ] Used SHOW GRANTS ON TABLE/SCHEMA/CATALOG to inspect ACLs
- [ ] Used SHOW GRANTS FOR to see all grants for a principal
- [ ] Understood GRANT syntax (catalog, schema, table levels)
- [ ] Observed DENY precedence over GRANT
- [ ] Verified catalog USAGE is required before accessing schemas
- [ ] Set up minimal service principal ACL configuration
- [ ] Distinguished Unity Catalog ACLs from workspace ACLs
- [ ] Triggered common permission errors (missing USAGE, no MODIFY)
- [ ] Used DESCRIBE/DESCRIBE DETAIL to view existing metadata
- [ ] Queried information_schema.tables and information_schema.columns for descriptions
- [ ] Added COMMENT to a table and column (syntax)
- [ ] Set tags on a table for classification
- [ ] Identified tables missing descriptions via information_schema
- [ ] (Stretch) Documented complete ACL configuration for a new schema

---

## Cross-References

- Day 17: Row filters and column masks build on these ACL concepts
- Day 19: PII anonymization and data purging with UC
- Day 20: Delta Sharing permission management
- Day 21: System tables auditing ACL changes
