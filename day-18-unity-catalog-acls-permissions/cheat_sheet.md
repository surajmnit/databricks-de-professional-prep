# Day 18 — Cheat Sheet: Unity Catalog ACLs and Permissions

> Corrected against current Databricks docs: **Unity Catalog does NOT support `DENY`.** `DENY` is a legacy Hive Metastore statement only. If you see conflicting notes elsewhere, trust this version.

## Three-Level Hierarchy

```
CATALOG → SCHEMA → TABLE / VIEW / MATERIALIZED VIEW / VOLUME / FUNCTION / MODEL
```
Inheritance: **top-down only**, applies to **current and future** child objects. No bottom-up, no sideways.

---

## GRANT / REVOKE Syntax

```sql
GRANT <privilege> ON <securable> TO <principal>;
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.sales TO `analysts`;
GRANT MODIFY ON TABLE prod.sales.orders TO `etl_service_principal`;

REVOKE SELECT ON SCHEMA prod.sales FROM `analysts`;
SHOW GRANTS ON TABLE prod.sales.customers;
SHOW GRANTS `analysts` ON SCHEMA prod.sales;
```

**There is no `DENY` in Unity Catalog.** To restrict a subset while granting broadly: split into separate schemas, or use row filters/column masks (Day 19) — never a deny statement.

---

## Key Privileges (verified names)

| Privilege | Applies to | What it does |
|---|---|---|
| `USE CATALOG` | Catalog | Traverse into the catalog (required, no substitute) |
| `USE SCHEMA` | Schema | Traverse into the schema (required, no substitute) |
| `SELECT` | Table, view, materialized view | Read data |
| `MODIFY` | Table | INSERT / UPDATE / DELETE / MERGE |
| `CREATE SCHEMA` | Catalog | Create schemas |
| `CREATE TABLE` / `CREATE VIEW` / `CREATE FUNCTION` / `CREATE VOLUME` / `CREATE MODEL` / `CREATE MATERIALIZED VIEW` | Schema (or catalog, to cascade) | Create that object type |
| `EXECUTE` | Function / model | Invoke |
| `READ VOLUME` / `WRITE VOLUME` | Volume | Read/write files in a managed volume |
| `READ FILES` / `WRITE FILES` | External location | Read/write raw cloud storage (Databricks recommends against direct use) |
| `MANAGE` | Any securable | Grant/revoke privileges on the object — **not** the same as ownership, doesn't imply `SELECT`/`MODIFY` |
| `ALL PRIVILEGES` | Any | Union of applicable non-ownership privileges |
| `OWNERSHIP` | Any (implicit, one owner per object) | Full control incl. grant/revoke, alter, drop; does **not** cascade to children |
| `APPLY TAG` | Most securables | Apply/remove tags |
| `BROWSE` | Catalog, schema, external location | List child objects without full access |

⚠️ **No generic `USAGE` privilege exists** — it's specifically `USE CATALOG` and `USE SCHEMA`. `USAGE` is Hive-Metastore-era terminology and is invalid UC syntax.

---

## The Traversal Chain (#1 exam pattern)

```
SELECT * FROM prod.sales.customers
              ↑              ↑         ↑
     needs USE CATALOG   needs USE   needs SELECT
       on prod            SCHEMA on   on customers
                           prod.sales
```
**All three required simultaneously.** Missing any one → access denied, regardless of the other two.

---

## Principals

| Type | Example | Best practice |
|---|---|---|
| User | `` `alice@example.com` `` | Avoid for grants — hard to maintain at scale |
| Group | `` `analysts` `` | **Preferred** — manage access via group membership |
| Service principal | `` `etl-sp-id` `` | **Required** for production jobs/pipelines — never a personal account |

---

## Ownership vs. ALL PRIVILEGES vs. MANAGE

| Concept | Cascades to children? | Can grant/revoke? | Includes data access (SELECT etc.)? |
|---|---|---|---|
| `OWNERSHIP` | No (per-object only) | Yes | Yes, implicitly, on that object |
| `ALL PRIVILEGES` | Yes, if granted at catalog/schema | No (not a grant-management right) | Yes |
| `MANAGE` | Depends on level granted | Yes | No — must self-grant separately |

---

## Two Independent ACL Planes

| Plane | Secures | Mechanism |
|---|---|---|
| **Workspace ACLs** | Notebooks, jobs, clusters, warehouses, dashboards, MLflow (legacy) | UI Permissions tab / `permissions` API — Can View/Run/Edit/Manage |
| **Unity Catalog ACLs** | Catalogs, schemas, tables, views, volumes, functions, models, shares, connections | `GRANT`/`REVOKE` SQL |

Fixing one does **not** fix the other — always two separate checks.

---

## Compute-Layer Least Privilege

Cluster policies + "Can Use" assignment — restricts node types, autoscaling, Spark configs, required access mode. **Separate mechanism from UC data grants.**

---

## Discoverability (Section 8)

```sql
COMMENT ON TABLE prod.sales.orders IS 'One row per order';       -- or ALTER TABLE ... SET COMMENT
ALTER TABLE prod.sales.orders SET TAGS ('pii' = 'false', 'layer' = 'gold');
ALTER TABLE prod.sales.orders SET TBLPROPERTIES ('refresh_frequency' = 'daily');
DESCRIBE DETAIL prod.sales.orders;
SELECT * FROM prod.information_schema.tables WHERE comment IS NOT NULL;
```
**Tags and comments are NOT automatically preserved by `DEEP CLONE` or `CTAS`** — only structure/data is copied; reapply metadata explicitly.

---

## Exam Trap Shortlist

1. `SELECT` granted, query still fails → missing `USE CATALOG`/`USE SCHEMA` in the chain.
2. "Deny just this one exception" → **no `DENY` in UC** — restructure schemas or use row filters/column masks.
3. Notebook "Can Manage" ≠ table `SELECT` — independent planes.
4. New table in an already-granted schema → access is automatic (forward-looking inheritance), no re-grant.
5. `ALL PRIVILEGES` ≠ `OWNERSHIP`; ownership never cascades to children.
6. Cluster size/config restriction → cluster policies, not UC grants.
7. "Most scalable/least-privilege" answer → groups over individual users; service principals over personal accounts for automation.
8. Revoking a schema-level grant leaves a separately-issued table-level grant to the same principal untouched.
9. Cloning/CTAS-ing a table loses its tags/comments — must be reapplied.
10. `DENY` (if it ever appears as an option) is only valid/meaningful for the legacy `hive_metastore` catalog — never the right answer for a Unity Catalog scenario.
