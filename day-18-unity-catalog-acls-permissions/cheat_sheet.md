# Day 18 — Cheat Sheet: Unity Catalog ACLs and Permissions

## Unity Catalog Three-Level Hierarchy

```
CATALOG → SCHEMA → TABLE/VIEW/MODEL/FUNCTION
```

**Inheritance:** Permissions granted at a higher level cascade down. NOT bottom-up. NOT sideways.

---

## GRANT Syntax

```sql
GRANT <privilege> ON <securable> TO <principal>
GRANT SELECT ON CATALOG prod TO `groups/analysts`;
GRANT MODIFY ON SCHEMA prod.sales TO service-principal:etl;
GRANT USAGE ON CATALOG prod TO `user:alice@example.com`;
```

---

## Key Privileges

| Privilege | Applies To | What It Does |
|---|---|---|
| USAGE | Catalog, schema | Traverse/enter the object |
| SELECT | Table, view | Read data |
| MODIFY | Table | INSERT, UPDATE, DELETE, TRUNCATE |
| CREATE | Schema, catalog | Create child objects |
| CREATE TABLE | Schema | Create tables in schema |
| READ FILES | External location, volume | Read raw files |
| WRITE FILES | External location, volume | Write raw files |
| EXECUTE | Function/UDF | Run the function |
| RUN | Pipeline | Trigger pipeline runs |
| ALL PRIVILEGES | Any | All data privileges — NOT ownership |

---

## Principals

| Principal Type | Example |
|---|---|
| User | `user:alice@example.com` or `alice@example.com` |
| Group | `groups/analysts` or `analyst_team` |
| Service Principal | `service-principal:pipeline-id` |

**Best practice:** Assign permissions to groups, not individual users.

---

## DENY vs GRANT — Rule

**DENY always wins.** Even if a user has GRANT from one group and DENY from another, the DENY takes precedence.

```sql
GRANT SELECT ON CATALOG prod TO all_employees;
DENY SELECT ON prod.hr.salaries TO all_employees;
-- all_employees: can read prod.* EXCEPT prod.hr.salaries
```

---

## USAGE — Critical Requirement

Both catalog AND schema USAGE are required to reach a table:

```
SELECT * FROM prod.sales.customers
  ↑ needs USAGE on prod     ↑
  needs USAGE on prod.sales ↑
  needs SELECT on customers
```

Grant at catalog level + specific MODIFY at schema level = typical least-privilege pattern.

---

## Workspace vs Unity Catalog ACLs

| Scope | ACL Type | Commands |
|---|---|---|
| Tables, schemas, catalogs | Unity Catalog ACLs | GRANT, REVOKE, DENY |
| Notebooks, jobs, MLflow (legacy) | Workspace ACLs | Workspace UI/API |

---

## System-Defined Roles

| Role | SELECT | MODIFY | CREATE | USAGE |
|---|---|---|---|---|
| metastore-admin | Yes | Yes | Yes | Yes |
| catalog-owner | Yes | Yes | Yes | Yes |
| schema-owner | No | Yes | Yes | Yes |
| table-owner | No | Yes | No | Yes |

---

## External Locations

```sql
CREATE EXTERNAL LOCATION landing_zone
    URL 's3://bucket/'
    CREDENTIAL landing_cred;

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION landing_zone TO etl_group;
```

External locations bind storage paths to credentials for least-privilege raw storage access.

---

## Key Exam Traps

1. **USAGE trap:** Must have USAGE on catalog AND schema before accessing table. No USAGE = access denied even with table SELECT.
2. **DENY wins trap:** DENY from any group overrides GRANT from another group — applies even through group membership.
3. **ALL PRIVILEGES trap:** Does NOT include OWNERSHIP. OWNERSHIP is separate.
4. **Direction trap:** Grants flow top-down (catalog → schema → table). NOT bottom-up or sideways.
5. **EXECUTE trap:** Running a UDF requires EXECUTE on the function, not SELECT on underlying tables.
6. **Workspace ACL trap:** Notebooks, jobs, MLflow experiments use workspace permissions — NOT GRANT/REVOKE SQL.
7. **Clone tag trap:** Tags and comments are NOT preserved by DEEP CLONE or CTAS — must be reapplied.

---

## Data Governance: Metadata and Discoverability

**Add descriptions:**
```sql
ALTER CATALOG prod SET COMMENT 'Production data';
ALTER SCHEMA prod.sales SET COMMENT 'Sales transactions';
ALTER TABLE prod.sales.customers SET COMMENT 'Customer records — nightly CRM sync';
ALTER TABLE prod.sales.customers ALTER COLUMN email SET COMMENT 'PII — masked for analysts';
```

**View metadata:**
```sql
DESCRIBE TABLE prod.sales.customers;
DESCRIBE TABLE prod.sales.customers COLUMN email;
SELECT table_catalog, table_schema, table_name, comment
FROM information_schema.tables WHERE comment IS NOT NULL;
SELECT table_catalog, table_schema, table_name, column_name, comment
FROM information_schema.columns WHERE comment IS NOT NULL;
```

**Tags (classification):**
```sql
ALTER TABLE prod.hr.salaries SET TAG pii = 'true';
ALTER TABLE prod.sales.customers SET TAGS (gdpr = 'personal', department = 'sales');
```
Tags NOT preserved by DEEP CLONE — must be reapplied.
