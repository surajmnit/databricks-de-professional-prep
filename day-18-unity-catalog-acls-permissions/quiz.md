# Day 18 — Quiz: Unity Catalog ACLs and Permissions

**Objective coverage:** Section 7 (10%) and Section 8 (7%)

---

## Question 1

**Objective:** Understand the Unity Catalog three-level hierarchy.

A data analyst is granted SELECT on `prod.sales.customers` (table level). They can read that table. Can they also read `prod.sales.orders` (a different table in the same schema)?

A. Yes, because USAGE on the schema is inherited from any table-level grant
B. Yes, because the analyst is in the same schema and schema-level grants flow up
C. No, because table-level grants do not flow up to schema or to sibling tables
D. No, because SELECT on one table blocks access to all other tables in the schema

---

## Question 2

**Objective:** Apply catalog-level grants correctly.

A data engineer needs to read all tables in `prod` and write to `prod.etl`. Which grant set is correct for the service account?

A. `GRANT ALL PRIVILEGES ON CATALOG prod TO service-principal:etl`
B. `GRANT SELECT, MODIFY ON CATALOG prod TO service-principal:etl`
C. `GRANT SELECT ON CATALOG prod TO service-principal:etl; GRANT MODIFY ON SCHEMA prod.etl TO service-principal:etl`
D. `GRANT USAGE ON SCHEMA prod.etl TO service-principal:etl`

---

## Question 3

**Objective:** Understand DENY precedence.

A user is in two groups: `analysts` (granted SELECT on `prod`) and `restricted_team` (DENIED SELECT on `prod.hr.salaries`). What happens when the user queries `prod.hr.salaries`?

A. The user can read `prod.hr.salaries` because the GROUP grant is broader
B. The user can read `prod.hr.salaries` because DENY only applies to the user directly, not through groups
C. The user is blocked from reading `prod.hr.salaries` because DENY wins over any GRANT
D. The query fails because conflicting DENY and GRANT from groups causes an error

---

## Question 4

**Objective:** Apply least-privilege principle.

A team of business analysts needs to view data only — no write access. Which privilege is most appropriate?

A. `MODIFY` — allows reading and writing
B. `SELECT` — read-only access
C. `USAGE` — access to traverse the hierarchy
D. `BROWSE` — list objects without reading data

---

## Question 5

**Objective:** Understand USAGE requirement in the hierarchy.

A user has explicit `GRANT SELECT ON SCHEMA prod.sales TO analyst`. When they run `SELECT * FROM prod.sales.customers`, they get an access denied error. What is missing?

A. SELECT on the table `prod.sales.customers`
B. USAGE on the schema `prod.sales`
C. USAGE on the catalog `prod`
D. Both USAGE on catalog `prod` AND USAGE on schema `prod.sales`

---

## Question 6

**Objective:** Distinguish workspace ACLs from Unity Catalog ACLs.

Which object type is managed by Unity Catalog ACLs (GRANT/REVOKE SQL commands)?

A. Notebooks
B. MLflow experiments (legacy)
C. Delta tables registered in Unity Catalog
D. Job definitions

---

## Question 7

**Objective:** Configure service principal access correctly.

A pipeline needs to read data from all `prod` tables and create new tables only in `prod.staging`. Which set of grants follows least privilege?

A. `GRANT SELECT ON CATALOG prod TO pipeline_sp; GRANT CREATE TABLE ON SCHEMA prod.staging TO pipeline_sp`
B. `GRANT ALL PRIVILEGES ON CATALOG prod TO pipeline_sp`
C. `GRANT SELECT ON SCHEMA prod.staging TO pipeline_sp; GRANT CREATE TABLE ON SCHEMA prod.staging TO pipeline_sp`
D. `GRANT USAGE ON CATALOG prod TO pipeline_sp; GRANT MODIFY ON CATALOG prod TO pipeline_sp`

---

## Question 8

**Objective:** Demonstrate understanding of permission inheritance.

A group is granted `SELECT ON CATALOG prod`. The security team then grants `DENY SELECT ON TABLE prod.finance.salaries TO the same group`. What is the effective permission on `prod.finance.salaries`?

A. Full SELECT access (catalog-level grant overrides table-level DENY)
B. No access (table-level DENY overrides catalog-level grant)
C. MODIFY access only (DENY blocks SELECT but not MODIFY)
D. USAGE only (DENY removes all table-level access)

---

## Question 9

**Objective:** Use SHOW GRANTS correctly for auditing.

An auditor wants to see all permissions granted to `user:alice@example.com` across all objects. Which command returns the complete list?

A. `SHOW GRANTS ON CATALOG prod FOR alice@example.com`
B. `SHOW GRANTS FOR alice@example.com`
C. `SHOW CATALOGS FOR alice@example.com`
D. `SHOW PERMISSIONS alice@example.com`

---

## Question 10

**Objective:** Understand system-defined roles.

Which system role grants USAGE on all catalogs and schemas and full CREATE/USE permissions but NOT read access by default?

A. `metastore-admin`
B. `catalog-owner`
C. `schema-owner`
D. `table-owner`

---

## Question 11

**Objective:** Apply ACLs to secure external locations.

A security policy requires that only the ETL team can write to the S3 landing bucket, and no one else can access it. Which Unity Catalog object should be used and how should it be configured?

A. Create an external schema and grant USAGE on it to etl_group only
B. Create an external location bound to a credential, and grant READ FILES + WRITE FILES on the external location to etl_group only
C. Create a managed table and grant MODIFY to etl_group
D. Create a volume and grant ALL PRIVILEGES to etl_group

---

## Question 12

**Objective:** Distinguish ALL PRIVILEGES from OWNERSHIP.

A user is granted `ALL PRIVILEGES ON TABLE prod.sales`. Can they transfer ownership of that table to another user?

A. Yes, ALL PRIVILEGES includes the ability to transfer ownership
B. No, OWNERSHIP is a separate privilege and must be explicitly granted
C. Yes, but only if the user is also a metastore-admin
D. Only if the table was created by the user (creator = owner)

---

## Question 13

**Objective:** Understand cross-workspace permission limitations.

A user in workspace A needs to access a table in workspace B. Workspace-level ACLs are used for workspace objects. Which Unity Catalog feature enables cross-workspace data access?

A. GRANT SELECT across workspaces using workspace names
B. Delta Sharing (D2D) with a recipient configuration
C. External tables pointing to shared S3 buckets
D. Service principals created in each workspace

---

## Question 14

**Objective:** Audit ACL changes using system tables.

A compliance team needs to track all permission changes (GRANT/DENY/REVOKE) in the last 30 days. Which system table or view contains this information?

A. `system.default.access_logs`
B. `system.default.audit_logs`
C. `information_schema.table_privileges`
D. `system.metadata.permissions_history`

---

## Question 15

**Objective:** Apply the correct privilege for a specific use case.

A data scientist needs to run a Python UDF that aggregates data. The UDF is registered in Unity Catalog. What privilege should be granted?

A. `SELECT` on the table used by the UDF
B. `EXECUTE` on the function/UDF
C. `USAGE` on the catalog containing the function
D. `MODIFY` on the table so the UDF can write results

---

## Question 16

**Objective:** Add metadata for data discoverability.

A data steward wants to document a table `prod.hr.salaries` so that analysts can understand its purpose, source system, and refresh cadence without asking the data team. Which command adds this description at the table level?

A. `CREATE COMMENT ON TABLE prod.hr.salaries AS 'HR salaries — updated weekly'`
B. `ALTER TABLE prod.hr.salaries SET COMMENT 'HR salaries — source Workday, updated every Monday'`
C. `ADD LABEL TO TABLE prod.hr.salaries DESCRIPTION 'HR salaries — updated weekly'`
D. `UPDATE information_schema.tables SET comment WHERE table_name = 'salaries'`

---

## Question 17

**Objective:** Use information_schema for data discovery.

A governance team wants to find all tables in `prod` that are missing column-level descriptions. Which query is correct?

A. `SELECT table_name FROM information_schema.tables WHERE table_catalog = 'prod' AND comment IS NULL`
B. `SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = 'prod' AND column.comment IS NULL`
C. `SELECT * FROM information_schema.tables WHERE comment IS NULL AND table_schema = 'prod'`
D. `SELECT table_name FROM information_schema.column_tags WHERE tag_name = 'description' AND tag_value IS NULL`

---

## Question 18

**Objective:** Understand tag behavior with cloning operations.

A data engineer clones a table using DEEP CLONE for a staging environment. The original table has PII tags set (`pii=true`, `gdpr=personal`). After cloning, the tags are no longer present on the cloned table. Which statement is most accurate?

A. Tags were not set correctly — tags should survive deep cloning
B. Tags are intentionally NOT preserved by DEEP CLONE — they must be reapplied after cloning
C. Tags survived but are not visible in the staging workspace's information_schema
D. Deep cloning is not supported for tagged tables

---

## Answer Key

### Q1: C — No, because table-level grants do not flow up to schema or to sibling tables

Unity Catalog inheritance flows top-down (catalog → schema → table). A table-level grant does not propagate up to the schema or sideways to sibling tables. The analyst would need explicit grants on each table they need to access.

**Why others are wrong:** A/B (inheritance wrong direction) — grants flow from parent to child, not child to parent or sibling. D (blocks all other tables) — each table has independent grants.

---

### Q2: C — Grant SELECT on catalog; grant MODIFY on schema for write access

Least privilege: SELECT on catalog gives read access to all tables. MODIFY (INSERT/UPDATE/DELETE) on `prod.etl` schema restricts write operations to that specific schema. This is the correct least-privilege configuration.

**Why others are wrong:** A = too broad (ALL PRIVILEGES). B = MODIFY on catalog is too broad for writes. D = USAGE alone does not allow data reading.

---

### Q3: C — The user is blocked from reading because DENY wins over any GRANT

DENY takes precedence over GRANT regardless of where the DENY originates. If any group the user belongs to has a DENY for a privilege, that DENY wins even if another group has a GRANT for the same privilege.

**Why others are wrong:** A = DENY does not lose to GROUP grants. B = DENY through groups still applies. D = no conflict error — DENY simply wins.

---

### Q4: B — SELECT — read-only access

SELECT grants the ability to read data without modification rights. This is the appropriate least-privilege permission for business analysts who only need to view data.

**Why others are wrong:** A = MODIFY allows writes (violates least privilege). C = USAGE allows traversal but not reading. D = BROWSE allows listing objects but not reading data.

---

### Q5: D — Both USAGE on catalog AND USAGE on schema

The user needs USAGE on `prod` (catalog) to enter the catalog, AND USAGE on `prod.sales` (schema) to enter the schema. Even with SELECT on the schema, without catalog USAGE the user cannot traverse to the schema. Without schema USAGE, they cannot traverse to the table.

**Why others are wrong:** A (SELECT on table) — the error occurs before reaching the table level. B/C (only one level) — both catalog and schema USAGE are required for traversal.

---

### Q6: C — Delta tables registered in Unity Catalog

Delta tables, schemas, and catalogs are managed by Unity Catalog and use GRANT/REVOKE SQL. Notebooks, MLflow experiments (legacy), and job definitions use workspace-level permissions.

**Why others are wrong:** A (notebooks) — workspace permissions. B (MLflow) — workspace permissions (legacy). D (jobs) — workspace permissions.

---

### Q7: A — SELECT on catalog + CREATE TABLE on staging schema

Least privilege: SELECT on catalog gives read access everywhere. CREATE TABLE on staging schema limits table creation to staging only. This satisfies both requirements with minimal scope.

**Why others are wrong:** B (ALL PRIVILEGES on catalog) — too broad. C (only SELECT on staging) — prevents reading other prod tables. D (USAGE + MODIFY on catalog) — MODIFY on catalog is too broad.

---

### Q8: B — No access (table-level DENY overrides catalog-level GRANT)

DENY always wins. The catalog-level grant gives SELECT on all tables including `prod.finance.salaries`, but the table-level DENY explicitly blocks SELECT on that table. The DENY takes precedence.

**Why others are wrong:** A = catalog-level grant does not override explicit table-level DENY. C/D = DENY blocks SELECT specifically; it does not reduce the permission to a different level.

---

### Q9: B — SHOW GRANTS FOR alice@example.com

`SHOW GRANTS FOR <principal>` lists all granted (and denied) privileges for that user or group across all securables in the account.

**Why others are wrong:** A = wrong syntax (no FOR clause on SHOW GRANTS ON). C = SHOW CATALOGS does not take a principal. D = no such command in Unity Catalog.

---

### Q10: C — schema-owner

`schema-owner` grants USAGE on all catalogs and schemas and full CREATE/USE permissions within schemas. It does not grant SELECT by default (that's the owner's implicit right to read their own schema tables).

**Why others are wrong:** A (metastore-admin) = full access to everything. B (catalog-owner) = USAGE + CREATE on catalog and children. D (table-owner) = USAGE + CREATE on owning schema.

---

### Q11: B — Create an external location bound to a credential, grant READ FILES + WRITE FILES on the external location to etl_group only

External locations bind storage paths to Unity Catalog credentials. Granting READ FILES + WRITE FILES on the external location restricts access to that bucket to etl_group only. This is the correct least-privilege pattern for securing raw storage.

**Why others are wrong:** A (external schema) — schema grants do not control raw storage access. C (managed table) — wrong mechanism. D (volume) — volumes are for structured file access, not raw bucket control.

---

### Q12: B — No, OWNERSHIP is a separate privilege

ALL PRIVILEGES grants all data-level privileges (SELECT, MODIFY, CREATE, etc.) but OWNERSHIP is a separate, object-level privilege. To transfer ownership, you must explicitly grant `OWNERSHIP ON <object> TO <principal>`.

**Why others are wrong:** A = OWNERSHIP is not included in ALL PRIVILEGES. C = metastore-admin is not required. D = creator does not automatically equal owner in UC.

---

### Q13: B — Delta Sharing (D2D) with a recipient configuration

Delta Sharing D2D allows one Databricks workspace to share data with another Databricks workspace. Workspace-level ACLs cannot grant cross-workspace table access.

**Why others are wrong:** A = no GRANT syntax supports cross-workspace references. C = external tables in one workspace point to that workspace's storage, not another workspace's tables. D = service principals are workspace-local.

---

### Q14: A — system.default.access_logs

The `system.default.access_logs` table (or `system.access.audit` depending on workspace config) records all access and permission change events including GRANT, DENY, and REVOKE operations.

**Why others are wrong:** B = no standard `audit_logs` table. C = information_schema only shows current grants, not change history. D = no standard `permissions_history` table.

---

### Q15: B — EXECUTE on the function/UDF

To run a Unity Catalog function or UDF, the user needs EXECUTE privilege on the function itself (not the underlying tables). This follows the principle of least privilege — the user can call the function without needing direct table access.

**Why others are wrong:** A (SELECT on table) — not required if the function handles access. C (USAGE on catalog) — does not grant function execution. D (MODIFY) — UDFs read by default, MODIFY is unnecessary and too broad.

---

### Q16: B — ALTER TABLE prod.hr.salaries SET COMMENT '...'

`ALTER TABLE ... SET COMMENT` is the correct syntax for adding a table-level description in Unity Catalog. This comment is visible via DESCRIBE, in the Catalog Explorer UI, and in information_schema.tables.comment.

**Why others are wrong:** A — no `CREATE COMMENT` syntax in Unity Catalog. C — no `ADD LABEL` syntax. D — information_schema tables are read-only; you cannot UPDATE them directly.

---

### Q17: B — SELECT ... FROM information_schema.columns WHERE ... AND column.comment IS NULL

The `information_schema.columns` view exposes the `comment` column for column-level descriptions. To find columns without descriptions, check `column.comment IS NULL` against the `information_schema.columns` view.

**Why others are wrong:** A — `information_schema.tables.comment` is the table-level comment, not column-level. C — `comment IS NULL` on `information_schema.tables` checks the table comment, not column comments. D — there is no `information_schema.column_tags` view in standard Unity Catalog.

---

### Q18: B — Tags are intentionally NOT preserved by DEEP CLONE — they must be reapplied after cloning

Tags are not copied by DEEP CLONE or CTAS. The cloned table is a new object with no metadata. This is a known behavior and a common governance gap. Tags and comments must be reapplied programmatically after cloning.

**Why others are wrong:** A — tags do NOT survive cloning by design. C — tags are not hidden; they are absent. D — deep cloning tagged tables is fully supported; tags are simply not copied.

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Hierarchy: table grants do not flow to siblings |
| 2 | Medium | Least-privilege service account grants |
| 3 | Hard | DENY precedence over GRANT from groups |
| 4 | Easy | SELECT = read-only for analysts |
| 5 | Hard | Both catalog AND schema USAGE required |
| 6 | Easy | UC ACL vs workspace ACL scope |
| 7 | Medium | Least-privilege pipeline grants |
| 8 | Hard | DENY overrides inherited GRANT |
| 9 | Easy | SHOW GRANTS FOR command syntax |
| 10 | Medium | System-defined roles and their scope |
| 11 | Medium | External location security |
| 12 | Medium | ALL PRIVILEGES vs OWNERSHIP |
| 13 | Hard | Cross-workspace access via Delta Sharing |
| 14 | Medium | System tables for ACL audit |
| 15 | Medium | EXECUTE privilege for UDFs |
| 16 | Easy | ALTER TABLE SET COMMENT syntax |
| 17 | Medium | information_schema.columns for column descriptions |
| 18 | Medium | Tags not preserved by DEEP CLONE |
