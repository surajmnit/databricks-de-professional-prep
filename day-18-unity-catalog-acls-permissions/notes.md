# Day 18 — Unity Catalog ACLs and Permissions

## Exam Objectives (Exam Guide, July 2026)

Maps to **Section 7: Ensuring Data Security and Compliance (10%)** and **Section 8: Data Governance (7%)**:
- "Use ACLs to secure Workspace Objects, enforcing the principle of least privilege, including policy enforcement."
- "Demonstrate understanding of Unity Catalog permission inheritance model."
- "Create and add descriptions/metadata about enterprise data to make it more discoverable."

*(Row filters, column masks, PII anonymization, and data purging are Day 19 — not duplicated here.)*

> ⚠️ **Correction notice:** An earlier draft of this content incorrectly stated that `DENY` is supported in Unity Catalog and that "DENY wins" is a UC precedence rule. **This has been verified against current Databricks documentation and is false.** `DENY` is a **legacy Hive Metastore statement only** — Databricks' own docs state plainly: *"This function is not supported by Unity Catalog"* and *"This statement applies only to the `hive_metastore` catalog and its objects."* Unity Catalog privileges are **purely additive** (grant/revoke only). This is corrected throughout below and is one of the highest-value traps to get right on the real exam.

---

## Part 1 — Unity Catalog Hierarchical Model

### Three-level namespace

```
CATALOG
  └── SCHEMA
        └── TABLE / VIEW / MATERIALIZED VIEW / VOLUME / FUNCTION / MODEL
```

**Inheritance rule:** privileges granted at a higher level (metastore, catalog, schema) apply to **current and future** child objects automatically. Inheritance flows **top-down only** — granting a privilege on a table does not grant anything at the schema or catalog level above it.

### Catalog types

| Type | Created by | Notes |
|---|---|---|
| **Unity Catalog managed** | User (`CREATE CATALOG`) | Storage location managed by UC |
| **External / foreign catalog** | User, via a `CONNECTION` | Points to an externally federated data source (Lakehouse Federation) |
| **Delta Sharing catalog** | User, from a share | References data shared in from another metastore/provider |

### Defaults on a new UC-enabled workspace
- A **workspace catalog** is created automatically and attached; **all users automatically get `USE CATALOG`** on it, plus `USE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`, `CREATE MODEL`, `CREATE FUNCTION`, and `CREATE MATERIALIZED VIEW` on its default schema.
- `system` catalog (system tables — Day 21) and each catalog's `information_schema` are also present.

---

## Part 2 — Principal, Privilege, and Securable Model

Every ACL entry is: **PRINCIPAL + PRIVILEGE + SECURABLE**
```sql
GRANT <privilege> ON <securable> TO <principal>
```

### Principals (who)

| Principal | Example |
|---|---|
| User | `` `alice@example.com` `` |
| Group | `` `analysts` `` (best practice: grant to groups, not individuals) |
| Service principal | `` `service-principal-id` `` (for jobs/pipelines/apps — never a personal account) |

### Securable objects and their real privilege sets (verified against current Databricks docs)

| Securable | Applicable privileges |
|---|---|
| **Metastore** | `CREATE CATALOG`, `CREATE CONNECTION`, `CREATE EXTERNAL LOCATION`, `CREATE PROVIDER`, `CREATE RECIPIENT`, `CREATE SHARE`, `CREATE STORAGE CREDENTIAL`, `READ METADATA` (inherits down to everything, unlike other metastore-level privileges) |
| **Catalog** | `ALL PRIVILEGES`, `APPLY TAG`, `BROWSE`, `CREATE SCHEMA`, `USE CATALOG`, plus (grantable here to cascade to children): `CREATE TABLE`, `CREATE VIEW`, `CREATE FUNCTION`, `CREATE MODEL`, `CREATE VOLUME`, `CREATE MATERIALIZED VIEW`, `USE SCHEMA`, `SELECT`, `MODIFY`, `EXECUTE`, `READ VOLUME`, `WRITE VOLUME`, `MANAGE` |
| **Schema** | Same child-level set as above, scoped to that schema |
| **Table** | `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `MODIFY`, `SELECT` |
| **View** | `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `SELECT` |
| **Materialized view** | `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `REFRESH`, `SELECT` |
| **Volume** | `ALL PRIVILEGES`, `APPLY TAG`, `MANAGE`, `READ VOLUME`, `WRITE VOLUME` |
| **Function / Model** (models are a function type) | `ALL PRIVILEGES`, `APPLY TAG`, `EXECUTE`, `MANAGE` |
| **External location** | `ALL PRIVILEGES`, `BROWSE`, `CREATE EXTERNAL TABLE`, `CREATE EXTERNAL VOLUME`, `EXTERNAL USE LOCATION`, `MANAGE`, `READ FILES`, `WRITE FILES` |
| **Connection** (Lakehouse Federation) | `ALL PRIVILEGES`, `CREATE FOREIGN CATALOG`, `MANAGE`, `USE CONNECTION` |
| **Share** | `SELECT` (grantable to a `RECIPIENT`) |

**Note:** there is **no generic `USAGE` privilege** in current Unity Catalog — the correct, exam-tested keywords are **`USE CATALOG`** and **`USE SCHEMA`** specifically. `USAGE` was Hive-Metastore-era terminology; using it in a UC `GRANT` statement is not valid syntax.

---

## Part 3 — Privilege Inheritance (the core tested behavior)

### How inheritance actually works

```sql
-- Grant SELECT at the catalog level → applies to every schema/table now AND created later
GRANT USE CATALOG ON CATALOG prod TO analyst_group;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO analyst_group;

-- analyst_group can now read this, and any FUTURE table added to prod.sales too:
SELECT * FROM prod.sales.customers;
```

### The traversal chain — the #1 tested scenario

> **To `SELECT` from `prod.sales.customers`, a principal needs `USE CATALOG` on `prod` AND `USE SCHEMA` on `prod.sales` AND `SELECT` on `prod.sales.customers`.** Missing any single link → access denied, even if the other two links are present.

**Exam trap:** "A user has `USE SCHEMA` on the schema and `SELECT` on the table, but not `USE CATALOG` on the catalog — can they query it?" **No.** Without `USE CATALOG`, they cannot even traverse into the catalog to reach the schema, regardless of what's granted below it.

### There is no DENY — how to actually restrict access

Because Unity Catalog grants are **purely additive**, you cannot broadly grant at a high level and then "carve out" an exception at a lower level with a deny statement. If you need `analysts` to read everything in `prod` **except** `prod.hr.salaries`, the correct patterns are:

1. **Structural separation** — keep `prod.hr` in its own schema/catalog that is never covered by the broad grant, and grant `analysts` only the schemas they should see individually (rather than one blanket catalog-level grant).
2. **Row filters / column masks** (Day 19) — grant `SELECT` normally, but attach a row filter or column mask function to the sensitive table/columns so restricted rows/values are hidden at query time regardless of the `SELECT` grant.

```sql
-- WRONG — this does not exist in Unity Catalog:
-- DENY SELECT ON TABLE prod.hr.salaries FROM analyst_group;

-- RIGHT — structural approach: don't include hr in the broad grant at all
GRANT USE CATALOG ON CATALOG prod TO analyst_group;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO analyst_group;      -- sales: yes
-- prod.hr is simply never granted to analyst_group
```

### Revoking within the inheritance model
```sql
REVOKE SELECT ON SCHEMA prod.sales FROM analyst_group;
```
- This removes the **inherited** access that came from this specific schema-level grant.
- If `analyst_group` also has a **separate, explicit** grant directly on one table in that schema, that grant is a distinct record and **survives** this revoke — revokes are scoped to the level at which the grant was issued.
- `REVOKE ALL PRIVILEGES` only revokes the `ALL PRIVILEGES` grant itself — any other privileges granted separately to that principal remain untouched.

### Ownership vs. privileges
- Every securable has exactly **one owner**. The owner automatically has all capabilities on the object (functionally equivalent to `ALL PRIVILEGES`, though Databricks doesn't literally list it that way in `SHOW GRANTS`).
- **`ALL PRIVILEGES` ≠ `OWNERSHIP`.** `ALL PRIVILEGES` is the union of grantable privileges; only the owner (or someone the owner promotes) can grant/revoke on the object, transfer ownership, or drop it.
- **Ownership does not cascade downward** — owning a catalog does not make you the owner of the schemas/tables inside it. You (or the object's actual owner) still need explicit grants on child objects to act on them, even as the parent's owner.
- **`MANAGE` privilege** lets a principal grant/revoke privileges on an object (like a delegated admin) without making them the owner — they don't automatically get data-access privileges like `SELECT` unless they grant it to themselves.
- Metastore admins and account admins bypass privilege checks entirely.

---

## Part 4 — GRANT, REVOKE, SHOW GRANTS

```sql
-- Grant
GRANT SELECT ON TABLE prod.sales.customers TO `analysts@example.com`;
GRANT CREATE TABLE ON SCHEMA prod.sales TO `data_engineers`;
GRANT USE CATALOG ON CATALOG prod TO `service-principal-id`;
GRANT SELECT, MODIFY ON TABLE prod.sales.orders TO etl_group;
GRANT ALL PRIVILEGES ON SCHEMA prod.sales TO admin_group;

-- Revoke
REVOKE SELECT ON TABLE prod.sales.customers FROM `analysts@example.com`;
REVOKE ALL PRIVILEGES ON SCHEMA prod.sales FROM ex_employee_group;

-- Inspect
SHOW GRANTS ON TABLE prod.sales.customers;
SHOW GRANTS `analysts@example.com` ON SCHEMA prod.sales;
SHOW GRANTS ON CATALOG prod;

SELECT grantee, privilege_type, table_schema, table_name
FROM prod.information_schema.table_privileges;
```

---

## Part 5 — Securing Workspace Objects (the other ACL plane)

Two independent planes — a very frequently tested distinction:

| Plane | Secures | Mechanism |
|---|---|---|
| **Workspace ACLs** | Notebooks, folders, jobs, clusters, DBSQL warehouses, dashboards, alerts, MLflow experiments (legacy), tokens, secret scopes | Permissions tab in UI / `permissions` REST API / `access_control_list` in DAB YAML — levels like Can View/Run/Edit/Manage |
| **Unity Catalog ACLs** | Catalogs, schemas, tables, views, volumes, functions, models, connections, shares, credentials, external locations | `GRANT`/`REVOKE` SQL, Catalog Explorer UI |

**Exam trap:** a user with "Can Manage" on a notebook (workspace ACL) but no UC grants on the tables it queries will still get an access-denied error at the data layer — fixing the notebook permission does nothing for the data permission, and vice versa.

### Least-privilege in practice
```sql
-- Avoid blanket ALL PRIVILEGES for read-only consumers:
-- BAD
GRANT ALL PRIVILEGES ON CATALOG prod TO analyst_group;
-- GOOD
GRANT USE CATALOG ON CATALOG prod TO analyst_group;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO analyst_group;
```

### Cluster policies — least privilege at the compute layer
- **Cluster policies** are the mechanism for the "policy enforcement" phrase in the objective bullet — an admin defines allowed node types, autoscaling bounds, Spark configs, and required access mode (e.g., must be Unity-Catalog-enabled/shared), then grants specific users/groups "Can Use" on that policy.
- Users without "Can Use" on any unrestricted policy cannot create clusters outside those guardrails. This is a **separate mechanism from UC data grants** — don't conflate "who can query this table" with "who can launch this cluster configuration."

### External locations (governed raw-storage access)
```sql
CREATE EXTERNAL LOCATION IF NOT EXISTS landing_zone
  URL 's3://company-landing-bucket/'
  CREDENTIAL landing_zone_cred
  COMMENT 'Ingest landing zone';

GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION landing_zone TO etl_group;
```
External locations bind a storage path to a credential, then privileges (`READ FILES`, `WRITE FILES`, `CREATE EXTERNAL TABLE`, `CREATE EXTERNAL VOLUME`) are granted on that named location rather than on raw cloud paths directly.

### Service principals for automated workloads
```sql
GRANT USE CATALOG, USE SCHEMA ON CATALOG prod TO `etl-service-principal-id`;
GRANT SELECT, MODIFY ON SCHEMA prod.sales TO `etl-service-principal-id`;
```
Production jobs/pipelines should authenticate and run **as a service principal**, never as a personal account — ties access to the workload's lifecycle, not an employee's. (Connects directly to the DAB `run_as: service_principal_name` pattern from Day 2/25.)

### Views vs. underlying tables
When a user queries a **view**, Unity Catalog checks ACLs on the **view itself**, not (necessarily) the base tables — a user can have `SELECT` on a view but nothing on the underlying table, and the query still succeeds, because the view's owner already has the rights needed to read the base table on the caller's behalf. This is a deliberate and useful pattern for exposing a curated slice of restricted data (e.g., pairs with row filters/column masks on Day 19 — those are enforced *after* the ACL check, on top of whatever the ACL allows through).

---

## Part 6 — Unity Catalog vs. Legacy Hive Metastore ACLs

| Aspect | Unity Catalog | Legacy Hive Metastore table ACLs |
|---|---|---|
| Scope | Account-level, cross-workspace | Workspace-level |
| Hierarchy | Catalog → Schema → Table | Database → Table |
| Inheritance | Yes, top-down, forward-looking | No |
| **`DENY` support** | **No — not supported** | **Yes** (this is the source of the common exam confusion) |
| Row filters / column masks | Yes | No |
| Fine-grained volumes | Yes | N/A |
| Cross-workspace sharing | Via Delta Sharing | Not native |
| Audit | `system` tables | Limited |

---

## Part 7 — Exam Traps Recap

1. **`SELECT` granted but the query still fails** → missing `USE CATALOG` and/or `USE SCHEMA` somewhere in the traversal chain. The single most-tested pattern for this objective.
2. **"How do I deny just this one user/table from a broad grant?"** → **there is no `DENY` in Unity Catalog.** The answer is always structural (separate schema) or row filter/column mask — never a deny statement. (`DENY` only exists for `hive_metastore`.)
3. **Workspace object permission ≠ UC data permission** — two fully independent checks; fixing one doesn't fix the other.
4. **New table created inside an already-granted schema** → access is automatic and immediate, no re-grant needed (forward-looking inheritance).
5. **`ALL PRIVILEGES` ≠ `OWNERSHIP`** — ownership is a distinct, non-cascading, per-object attribute.
6. **Cluster size/config restriction** → cluster policies + "Can Use," not UC grants (different plane entirely).
7. **"Most scalable / least-privilege" answer among options** → grant to **groups**, not individual users; use **service principals** for automated/production workloads.
8. **Revoking a schema-level grant** does not remove a separately-issued, explicit table-level grant to the same principal.
9. **Tags and comments are not preserved by `DEEP CLONE` or `CTAS`** (Part 8) — only the table structure/data is copied; metadata must be reapplied.

---

## Part 8 — Data Governance: Metadata and Discoverability (Section 8 objective)

Discoverability is a distinct governance objective from access control — it's about making *approved* data findable and self-explanatory, not about who can read it.

### Comments — at every level, including columns
```sql
ALTER CATALOG prod SET COMMENT 'Production data — all business units';
ALTER SCHEMA prod.sales SET COMMENT 'Sales transactions — orders, returns, subscriptions';
ALTER TABLE prod.sales.customers SET COMMENT 'All customer records — nightly CRM sync';
ALTER TABLE prod.sales.customers ALTER COLUMN email SET COMMENT 'PII — masked for analysts';

-- Or inline at creation time
CREATE TABLE prod.sales.orders (
  order_id     BIGINT  COMMENT 'Unique order identifier',
  customer_id  BIGINT  COMMENT 'FK to customer record',
  order_date   DATE    COMMENT 'Date order was placed — local timezone',
  total_amount DECIMAL(10,2) COMMENT 'Order total in USD'
) COMMENT 'All completed orders from the e-commerce platform';
```

### Tags — structured classification, not free text
```sql
ALTER TABLE prod.hr.salaries SET TAGS ('sensitivity' = 'confidential');
ALTER TABLE prod.sales.customers SET TAGS ('pii' = 'true', 'department' = 'sales');
ALTER TABLE prod.sales.customers UNSET TAG 'pii';

SELECT * FROM prod.information_schema.table_tags WHERE tag_name = 'pii';
```

**Tags vs. comments:**

| Feature | Purpose | Persists after `DEEP CLONE` / `CTAS`? |
|---|---|---|
| `COMMENT` | Human-readable description (what/when/source) | **No** — not copied by `CTAS`; `DEEP CLONE` copies table properties/comments at the table level in some cases, but **do not rely on this** — always verify and reapply |
| `TAG` | Structured classification (PII, GDPR, cost center) | **No** — not automatically carried over by `DEEP CLONE` or `CTAS`. Must be reapplied programmatically as part of the pipeline that creates the derived table. |

**Exam trap:** if a question describes cloning or CTAS-ing a tagged/commented table and asks whether the new table is still tagged/discoverable the same way, the answer is **no — metadata must be reapplied**, since only the data/schema structure is copied.

### Table properties and DESCRIBE DETAIL
```sql
ALTER TABLE prod.sales.orders SET TBLPROPERTIES (
  'source_system' = 'ecommerce',
  'refresh_frequency' = 'daily'
);

DESCRIBE DETAIL prod.sales.orders;   -- format, size, partitioning, properties, location
DESCRIBE TABLE prod.sales.customers;
DESCRIBE TABLE prod.sales.customers COLUMN email;
```

### Programmatic discovery via information_schema
```sql
SELECT table_catalog, table_schema, table_name, comment
FROM prod.information_schema.tables
WHERE comment IS NOT NULL;

SELECT table_catalog, table_schema, table_name, column_name, comment
FROM prod.information_schema.columns
WHERE comment IS NOT NULL;
```

### Governance best practices
1. Comment every catalog, schema, table, and PII-relevant column — at minimum, business purpose and source system.
2. Tag PII/GDPR-relevant columns consistently (`pii=true`, `classification=restricted`) so tag-based automation (row filters, masking policies) can key off them later.
3. Document refresh cadence in table comments or `TBLPROPERTIES`.
4. Reapply tags/comments explicitly in any pipeline step that clones or CTAS-es a table — don't assume they survive.
5. Use `information_schema` programmatically to power internal data-catalog tooling or completeness audits (e.g., "which tables have no comment yet").

---

## Cross-References
- Day 19: Row filters, column masks, PII anonymization/pseudonymization, data purging — builds directly on the ACL model above (filters/masks apply *after* the ACL check succeeds).
- Day 20: Delta Sharing and Lakehouse Federation permissions (`CREATE SHARE`, `USE CONNECTION`, etc. from the metastore-level privilege table above).
- Day 21: `system` tables for auditing grant/revoke history and access patterns.
- Day 2 / 25: Service principal `run_as` pattern in Declarative Automation Bundles ties directly into the service-principal grants above.
