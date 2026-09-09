# Day 18 — Unity Catalog ACLs and Permissions

## Exam Objectives (Exam Guide, July 2026)

This day maps to **Section 7: Ensuring Data Security and Compliance (10%)** and **Section 8: Data Governance (7%)**:

- "Use ACLs to secure Workspace Objects, enforcing the principle of least privilege, including enforcing principles like least privilege, policy enforcement."
- "Demonstrate understanding of Unity Catalog permission inheritance model."
- "Create and add descriptions/metadata about enterprise data to make it more discoverable."

---

## Part 1 — Unity Catalog Hierarchical Model

### Three-Layer Hierarchy

Unity Catalog uses a **three-level namespace** with inheritance flowing downward:

```
CATALOG
  └── SCHEMA
       └── TABLE / VIEW / MODEL / FUNCTION / etc.
```

**Inheritance rule:** Permissions granted at a higher level (catalog, schema) **cascade down** to all child objects, unless explicitly overridden.

### Catalog Types

| Type | Created By | Notes |
|---|---|---|
| **Unity Catalog managed** | Unity Catalog automatically | Storage location managed by UC |
| **External** | User (with `CREATE CATALOG`) | Points to external cloud storage (S3/ADLS/GCS) |
| **Foreign** | User | References a share from Delta Sharing |

### Default Catalog Behavior

On enabling Unity Catalog, a workspace gets:
- `main` catalog (managed by UC, tied to workspace's default storage)
- System catalog (`system`)
- Information schema (`information_schema`)

---

## Part 2 — Principal, Privilege, and Securable Model

### The Three-Component ACL Model

Every ACL entry is: **PRINCIPAL + PRIVILEGE + SECUREABLE**

```
GRANT <PRIVILEGE> ON <SECUREABLE> TO <PRINCIPAL>
```

### Principals (Who)

| Principal | Description |
|---|---|
| `user@example.com` | Individual user by email |
| `user:<user-id>` | Individual user by UUID |
| `groups/<group-name>` | Group (best practice — assign permissions to groups, not users) |
| `service:<service-principal-id>` | Service principal for applications |

**Best practice:** Use groups. Assign users to groups. Grant permissions to groups. This maps cleanly to organizational structures.

### Securables (What)

| Securable Type | Examples |
|---|---|
| Catalog | `CREATE CATALOG`, `USAGE` |
| Schema | `CREATE`, `USE SCHEMA` |
| Table / View / Model | `SELECT`, `MODIFY`, `CREATE TABLE` |
| External Location | `READ FILES`, `WRITE FILES` |
| Volume | `READ FILES`, `WRITE FILES` |
| Function | `EXECUTE` |
| Pipeline | `RUN` |
| Credential | `USE CREDENTIAL` |
| Share (provider side) | `CREATE SHARE`, `CREATE RECIPIENT` |
| Recipient (provider side) | `GRANT SHARE` |

### Privileges (What they can do)

| Privilege | Applies To | Description |
|---|---|---|
| `ALL PRIVILEGES` | Any | All applicable privileges |
| `SELECT` | Tables, views | Read data |
| `MODIFY` | Tables | INSERT, DELETE, UPDATE, TRUNCATE |
| `CREATE` | Schema, catalog | Create child objects |
| `USAGE` | Catalog, schema | Access the catalog/schema |
| `CREATE TABLE` | Schema | Create tables |
| `EXECUTE` | Functions | Run functions/UDFs |
| `READ FILES` | Volumes, external locations | Read files from storage |
| `WRITE FILES` | Volumes, external locations | Write files to storage |
| `RUN` | Pipelines | Trigger pipeline runs |
| `USE CREDENTIAL` | Credentials | Use credential for federation |
| `CREATE SHARE` | Account | Create shares |
| `GRANT SHARE` | Recipients | Share data |
| `BROWSE` | Catalog, schema | List child objects |
| `APPLY ON CLAIM` | Any | Delegate privileges (for row filters) |

---

## Part 3 — Privilege Inheritance

### How Inheritance Works

When a privilege is granted on a parent object, it applies to all descendants unless overridden:

```sql
-- Grant SELECT on the catalog → applies to all schemas and tables
GRANT SELECT ON CATALOG prod TO analyst_group;

-- This table now inherits SELECT from catalog level
SELECT * FROM prod.sales.customers;  -- analyst_group can read
```

### Explicit Override (DENY)

DENY blocks a granted privilege at a lower level:

```sql
GRANT SELECT ON CATALOG prod TO analyst_group;

-- Override: deny SELECT on a specific sensitive table
DENY SELECT ON prod.hr.salaries TO analyst_group;
```

Analyst group can read everything in `prod` except `prod.hr.salaries`.

### Inheritance Direction

**Bottom-up:** No. Parent permissions do NOT flow up. Granting on a table does NOT give access to the schema or catalog.

**Top-down:** Yes. Catalog → Schema → Table. Explicit grants at each level override inherited ones.

### Exam Trap — Inheritance Assumption

A common exam scenario: "A user has USAGE on schema but not the catalog — can they access the table?" Without USAGE on the catalog, they cannot traverse the path to the schema. The catalog USAGE is required first.

### System-Defined Roles

Unity Catalog provides predefined roles (simplest way to grant common permission sets):

| Role | SELECT | MODIFY | CREATE | USE SCHEMA |
|---|---|---|---|---|
| ` metastore-admin` | Yes | Yes | Yes | Yes |
| `catalog-owner` | Yes | Yes | Yes | Yes |
| `schema-owner` | No | Yes | Yes | Yes |
| `table-owner` | No | Yes | No | Yes |

Assign these via `GRANT` to delegate ownership.

---

## Part 4 — GRANT, SHOW, and DENY

### Basic GRANT Syntax

```sql
GRANT <privilege> ON <securable> TO <principal>
GRANT SELECT ON TABLE prod.sales.customers TO `analysts@example.com`
GRANT CREATE TABLE ON SCHEMA prod.sales TO `data_engineer_group`
GRANT USAGE ON CATALOG prod TO `service:app-id`
```

### Multiple Privileges

```sql
GRANT SELECT, MODIFY ON TABLE prod.sales TO analyst_group;
GRANT ALL PRIVILEGES ON SCHEMA prod.sales TO admin_group;
```

### Revoking Access

```sql
REVOKE SELECT ON TABLE prod.sales FROM `analysts@example.com`;
REVOKE ALL PRIVILEGES ON SCHEMA prod.sales FROM ex_employee;
```

### Viewing Permissions

```sql
-- Show grants on a specific object
SHOW GRANTS ON TABLE prod.sales.customers;

-- Show all grants for a principal
SHOW GRANTS FOR `analysts@example.com`;

-- Show all granted privileges (catalog level)
SHOW GRANTS ON CATALOG prod;
```

### DENY Precedence

DENY always wins over GRANT. The evaluation order:

1. DENY (blocked)
2. GRANT (allowed)

If a user belongs to two groups — one granted SELECT, one DENY'd — the DENY wins.

```sql
GRANT SELECT ON CATALOG prod TO all_employees;
DENY SELECT ON prod.hr.salaries TO all_employees;
-- all_employees can read prod.* except prod.hr.salaries
```

---

## Part 5 — Securing Workspace Objects

### Workspace vs Unity Catalog Scope

| Scope | Managed By | ACL Type |
|---|---|---|
| Workspace-level objects | Workspace ACLs | Workspace permissions (legacy) |
| Unity Catalog objects | Unity Catalog | Catalog ACLs |

When Unity Catalog is enabled, all data objects (tables, schemas, catalogs) are managed by Unity Catalog. Workspace ACLs still apply to notebooks, experiments, and other non-data objects.

### Objects Still Using Workspace ACLs (post-UC enablement)

- Notebooks
- Notebooks folders
- MLflow experiments and models (legacy)
- Dashboards
- Job definitions
- Legacy Hive metastore tables (migrated separately)

### Unity Catalog-Managed Objects

- All tables (managed and external)
- All schemas and catalogs
- Models registered to Unity Catalog
- Volumes
- Connections (federation)
- Shares and recipients
- External locations
- Credentials

### Least Privilege in Practice

**Principle:** Grant only the privileges required for a role's tasks. No broad grants.

```sql
-- BAD: Grant ALL PRIVILEGES on catalog
GRANT ALL PRIVILEGES ON CATALOG prod TO analyst_group;

-- GOOD: Grant only SELECT (read access)
GRANT SELECT ON CATALOG prod TO analyst_group;

-- BETTER for writers: SELECT + MODIFY
GRANT SELECT, MODIFY ON CATALOG prod TO etl_pipeline_service_account;
```

### Secure External Locations

External locations bind storage paths to Unity Catalog credentials:

```sql
-- Create external location with credential
CREATE EXTERNAL LOCATION IF NOT EXISTS landing_zone
    URL 's3://company-landing-bucket/'
    CREDENTIAL landing_zone_cred
    COMMENT 'Ingest landing zone';

-- Grant read/write to ETL group only
GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION landing_zone TO etl_group;
```

### Row Filters and Column Masks (covered Day 19, noted here for context)

Row filters and column masks are applied AFTER ACL checks. A user with SELECT on a table still sees filtered/masked data if a row filter or column mask is defined.

---

## Part 6 — Unity Catalog vs Legacy Hive Metastore ACLs

| Aspect | Unity Catalog | Legacy Hive Metastore |
|---|---|---|
| Scope | Account-level | Workspace-level |
| Hierarchy | Catalog → Schema → Table | Database → Table |
| Inheritance | Yes, top-down | No |
| DENY support | Yes | No |
| Row filters | Yes | No |
| Column masks | Yes | No |
| Cross-workspace sharing | Via Delta Sharing | Not natively |
| Audit | System tables | Limited |

**Migration:** Use the migration wizard or `CATALOG MIGRATION` commands to move from Hive metastore to Unity Catalog.

---

## Part 7 — Service Principals and Workload Identity

### Service Principals

Service principals represent applications or automated workloads (not human users):

```sql
CREATE SERVICE PRINCIPAL app_pipeline
    COMMENT 'ETL pipeline automation account';

GRANT SELECT ON CATALOG prod TO service-principal:app-pipeline-id;
GRANT CREATE TABLE ON SCHEMA prod.sales TO service-principal:app-pipeline-id;
```

### Credential-Based Authentication

For automated workloads, use **OAuth 2.0** or **service principal + secrets**:

```bash
# Databricks CLI with service principal
databricks configure --service-principal \
    --host https://$(databricks workspace configure --get-host) \
    --client-id $DATABRICKS_CLIENT_ID \
    --client-secret $DATABRICKS_CLIENT_SECRET
```

### Workspace vs Account-Level Permissions

Some permissions are granted at the **account level** (not workspace):

- `CREAT CATALOG` (requires account-level privilege)
- `CREATE SHARE`, `CREATE RECIPIENT`
- `CREATE EXTERNAL LOCATION`
- `CREATE CREDENTIAL`

Workspace-level ACLs cannot grant these — they must be done at the account level by an account admin.

---

## Part 8 — Exam Traps and Common Misconceptions

### Trap 1: ALL PRIVILEGES Does Not Mean ALL

`ALL PRIVILEGES` on a table does not include `OWNERSHIP`. Ownership is a separate privilege transferred via `OWNERSHIP` or when creating the object.

### Trap 2: USAGE Is Required Before Any Operation

Without `USAGE` on the catalog, users cannot access schemas inside it — even with explicit grants on the schema. The inheritance path must be open at each level.

### Trap 3: REVOKE Does Not Recursively Remove All Grants

REVOKE removes only the specifically granted privilege. If a user inherited a privilege from a group grant, revoking from the group removes it. But explicit grants on child objects remain.

### Trap 4: DENY Wins — Even From Multiple Groups

If a user is in group A (granted SELECT) and group B (denied SELECT), the DENY wins. This is a critical exam pattern.

### Trap 5: ACLs Apply to Views, Not Underlying Tables

When a user queries a view, Unity Catalog checks ACLs on the **view**, not the underlying tables. If a user has SELECT on the view but not the base table, they can still access data through the view — provided the view owner has the necessary permissions (which they do, since they created the view).

---

## Cross-References

---

## Part 9 — Data Governance: Metadata and Discoverability

### What Metadata Means in Unity Catalog

Unity Catalog stores descriptions and properties at every level of the hierarchy. This metadata appears in the Catalog Explorer UI, in `DESCRIBE`, and in the `information_schema`. Good metadata makes data self-documenting — users can search and understand data without needing to ask a data engineer.

### COMMENT / DESCRIBE Syntax

```sql
-- Add description to a catalog
ALTER CATALOG prod SET COMMENT 'Production data — all business units';

-- Add description to a schema
ALTER SCHEMA prod.sales SET COMMENT 'Sales transactions — orders, returns, subscriptions';

-- Add description to a table
ALTER TABLE prod.sales.customers SET COMMENT 'All customer records — updated nightly from CRM';

-- Add description to a column
ALTER TABLE prod.sales.customers ALTER COLUMN email SET COMMENT 'Customer email — PII, masked for analysts';

-- Add description via CREATE
CREATE TABLE prod.sales.orders (
    order_id BIGINT COMMENT 'Unique order identifier',
    customer_id BIGINT COMMENT 'FK to customer record',
    order_date DATE COMMENT 'Date order was placed — local timezone',
    total_amount DECIMAL(10,2) COMMENT 'Order total in USD'
) COMMENT 'All completed orders from the e-commerce platform';
```

### View Metadata

```sql
-- DESCRIBE (short form: DESC)
DESCRIBE CATALOG prod;
DESCRIBE SCHEMA prod.sales;
DESCRIBE TABLE prod.sales.customers;
DESCRIBE TABLE prod.sales.customers COLUMN email;

-- From information_schema
SELECT table_catalog, table_schema, table_name, comment
FROM information_schema.tables
WHERE table_catalog = 'prod';
```

### Data Classification and Tags

Beyond descriptions, Unity Catalog supports **tags** (key-value labels) for classification:

```sql
-- Set a tag on a table (requires admin or owner)
ALTER TABLE prod.hr.salaries SET TAG sensitivity = 'confidential';

-- Set multiple tags
ALTER TABLE prod.sales.customers SET TAGS (pii = 'true', department = 'sales');

-- Query tables by tag
SELECT * FROM information_schema.table_tags
WHERE tag_name = 'pii' AND tag_value = 'true';

-- Remove a tag
ALTER TABLE prod.sales.customers UNSET TAG pii;
```

### Tags vs Comments — When to Use Each

| Feature | Use Case | Persists after clone? |
|---|---|---|
| `COMMENT` | Human-readable descriptions (what, when, source) | No (only metadata) |
| `TAG` | Classification labels (PII, GDPR, department, cost center) | Yes (via Lakehouse monitoring) |

**Exam trap:** Tags set on a table are NOT preserved when you do a DEEP CLONE or CTAS. Comments are also not preserved in CTAS. Only the table structure is copied. Tags that need to survive cloning must be reapplied programmatically.

### Row-Level Metadata

Table properties can store custom key-value metadata:

```sql
-- Set custom table properties
ALTER TABLE prod.sales.orders SET TBLPROPERTIES ('source_system' = 'ecommerce', 'refresh_frequency' = 'daily');

-- View properties
DESCRIBE DETAIL prod.sales.orders;
```

`DESCRIBE DETAIL` returns schema plus table properties including format, size, and partition info.

### Information Schema — Programmatic Discovery

The `information_schema` provides SQL-native access to all catalog metadata:

```sql
-- Find all tables in prod that have comments
SELECT table_catalog, table_schema, table_name, comment
FROM information_schema.tables
WHERE table_catalog = 'prod'
  AND comment IS NOT NULL;

-- Find all columns with descriptions
SELECT table_catalog, table_schema, table_name, column_name, comment
FROM information_schema.columns
WHERE comment IS NOT NULL
ORDER BY table_catalog, table_schema, table_name;

-- Find tables by tag (if tags are surfaced in information_schema)
SELECT * FROM information_schema.table_tags
WHERE tag_name = 'pii';
```

### Governance Best Practices

1. **Add COMMENT to every catalog, schema, table, and column** — at minimum, the business purpose and data source.
2. **Tag PII columns** using tags like `pii=true`, `gdpr=true`, or `classification=restricted`.
3. **Document update cadence** in table comments (e.g., "updated hourly", "batch daily at 2am UTC").
4. **Document data ownership** — Unity Catalog does not have a native owner tag beyond ACLs; document in the schema or table comment.
5. **Use information_schema** for data lineage and discoverability tooling — query it to build data catalogs.

---

## Cross-References

- Day 17: Row filters and column masks build on these ACLs
- Day 19: Anonymization, pseudonymization, and data purging
- Day 20: Delta Sharing and Lakehouse Federation permissions
- Day 21: System tables for audit of ACL changes
