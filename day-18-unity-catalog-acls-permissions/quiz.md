# Day 18 — Quiz: Unity Catalog ACLs and Permissions

**Objective coverage:** Section 7 (10%) and Section 8 (7%)

> ⚠️ **Correction notice:** several questions in an earlier draft tested `DENY` as if it were a valid Unity Catalog precedence mechanism, used the invalid privilege name `USAGE`, referenced a `SHOW GRANTS FOR <principal>` syntax that doesn't exist, described fabricated "system-defined roles," and named the wrong audit system table. All of these have been corrected below against current Databricks documentation. If you've already drilled the old versions of Q3, Q5, Q8, Q9, Q10, or Q14, re-do them here.

---

## Question 1
**Objective:** Understand the Unity Catalog three-level hierarchy.

A data analyst is granted `SELECT` on `prod.sales.customers` (table level) only. Can they also read `prod.sales.orders`, a different table in the same schema?

A. Yes, because `USE SCHEMA` is implicitly inherited from any table-level grant
B. Yes, because schema-level access flows upward from table grants
C. No — table-level grants do not flow up to the schema or sideways to sibling tables
D. No — `SELECT` on one table blocks access to all other tables in the schema

---

## Question 2
**Objective:** Apply catalog-level grants correctly for least privilege.

A data engineer needs read access to every table in `prod` and write access limited to `prod.etl`. Which grant set is correct?

A. `GRANT ALL PRIVILEGES ON CATALOG prod TO etl_sp;`
B. `GRANT SELECT, MODIFY ON CATALOG prod TO etl_sp;`
C. `GRANT USE CATALOG ON CATALOG prod TO etl_sp; GRANT USE SCHEMA, SELECT ON CATALOG prod TO etl_sp; GRANT USE SCHEMA, MODIFY ON SCHEMA prod.etl TO etl_sp;`
D. `GRANT USE SCHEMA ON SCHEMA prod.etl TO etl_sp;`

---

## Question 3
**Objective:** Understand how Unity Catalog handles restricting a subset of a broad grant.

A group `all_employees` has `SELECT` granted at the catalog level on `prod` (with the necessary `USE CATALOG`/`USE SCHEMA` also granted). Leadership wants this group to be unable to read the single table `prod.hr.salaries`, while keeping their access to everything else in `prod`. What should the team do?

A. Run `DENY SELECT ON TABLE prod.hr.salaries TO all_employees;`
B. Restructure so `prod.hr` is never included in the broad grant to `all_employees` (i.e., grant `all_employees` only the specific schemas they should see, not the whole catalog), or apply a row filter/column mask on the sensitive table
C. Run `REVOKE SELECT ON TABLE prod.hr.salaries FROM all_employees;` even though it was never explicitly granted at the table level
D. Rename `prod.hr.salaries` so the group can't find it in Catalog Explorer

---

## Question 4
**Objective:** Apply the least-privilege principle to a read-only role.

A team of business analysts needs to view data only — no write access. Which privilege should they be granted on the relevant tables/schemas?

A. `MODIFY` — allows reading and writing
B. `SELECT` — read-only access
C. `USE SCHEMA` — access to traverse into the schema
D. `BROWSE` — list objects without reading their data

---

## Question 5
**Objective:** Understand the full traversal requirement for accessing a table.

A user has `GRANT SELECT ON SCHEMA prod.sales TO analyst;` only. When they run `SELECT * FROM prod.sales.customers`, they get an access-denied error. What is most likely missing?

A. `SELECT` on the table `prod.sales.customers` specifically
B. `USE SCHEMA` on `prod.sales`
C. `USE CATALOG` on `prod`
D. Both `USE CATALOG` on `prod` and `USE SCHEMA` on `prod.sales`

---

## Question 6
**Objective:** Distinguish workspace ACLs from Unity Catalog ACLs.

Which object type is governed by Unity Catalog's `GRANT`/`REVOKE` SQL commands rather than workspace-level permissions?

A. Notebooks
B. MLflow experiments (legacy)
C. Delta tables registered in Unity Catalog
D. Job definitions

---

## Question 7
**Objective:** Configure service principal access following least privilege.

A production pipeline needs to read every table in `prod` and create new tables only in `prod.staging`. Which grant set is correct and minimal?

A. `GRANT USE CATALOG ON CATALOG prod TO pipeline_sp; GRANT USE SCHEMA, SELECT ON CATALOG prod TO pipeline_sp; GRANT USE SCHEMA, CREATE TABLE ON SCHEMA prod.staging TO pipeline_sp;`
B. `GRANT ALL PRIVILEGES ON CATALOG prod TO pipeline_sp;`
C. `GRANT USE SCHEMA, SELECT, CREATE TABLE ON SCHEMA prod.staging TO pipeline_sp;` (nothing else)
D. `GRANT USE CATALOG, MODIFY ON CATALOG prod TO pipeline_sp;`

---

## Question 8
**Objective:** Understand inheritance in the absence of any deny mechanism.

A group is granted `SELECT ON CATALOG prod` (with the required `USE CATALOG`/`USE SCHEMA` also granted). No other grants or restrictions of any kind exist for this group. What is their effective access to `prod.finance.salaries`?

A. Full `SELECT` access — the catalog-level grant cascades down, and Unity Catalog has no mechanism to override it downward except restructuring the grant itself
B. No access, because sensitive tables are automatically excluded from catalog-level grants
C. `MODIFY` access only
D. Access depends on whether a `DENY` was separately issued

---

## Question 9
**Objective:** Use `SHOW GRANTS` correctly.

An auditor wants to see what privileges `alice@example.com` has been granted on the `prod.sales` schema specifically. Which command is syntactically correct?

A. `SHOW GRANTS FOR alice@example.com;`
B. `` SHOW GRANTS `alice@example.com` ON SCHEMA prod.sales; ``
C. `SHOW PERMISSIONS alice@example.com ON prod.sales;`
D. `SHOW CATALOGS FOR alice@example.com;`

---

## Question 10
**Objective:** Understand ownership vs. delegated management vs. broad privilege grants.

A user is granted the `MANAGE` privilege on a schema. What does this allow them to do, and what does it NOT automatically give them?

A. `MANAGE` makes them the owner of the schema, including the ability to drop it
B. `MANAGE` lets them grant and revoke privileges on the schema, but does not by itself give them data-access privileges like `SELECT` — they would need to grant that to themselves separately
C. `MANAGE` is identical to `ALL PRIVILEGES`
D. `MANAGE` only applies to workspace objects, not Unity Catalog securables

---

## Question 11
**Objective:** Apply ACLs to secure external locations (raw cloud storage).

A security policy requires that only the ETL team can read/write a specific S3 landing bucket, and no one else should have any access to it. What's the correct approach?

A. Create an external schema and grant `USE SCHEMA` on it to `etl_group` only
B. Create an external location bound to a storage credential, and grant `READ FILES`, `WRITE FILES` on that external location to `etl_group` only
C. Create a managed table over the bucket and grant `MODIFY` to `etl_group`
D. Create a volume and grant `ALL PRIVILEGES` to `etl_group`

---

## Question 12
**Objective:** Distinguish `ALL PRIVILEGES` from ownership.

A user is granted `ALL PRIVILEGES ON TABLE prod.sales.orders`. Can they transfer ownership of that table to another user?

A. Yes — `ALL PRIVILEGES` includes the ability to transfer ownership
B. No — ownership is a distinct, per-object attribute; only the current owner (or someone with sufficient admin rights) can transfer it
C. Yes, but only if they are also a metastore admin
D. Only if they originally created the table

---

## Question 13
**Objective:** Understand cross-workspace data access limitations.

A user in Workspace A needs governed, read-only access to a table that physically lives in a metastore attached to Workspace B. Workspace-level ACLs only apply within a single workspace. What Unity Catalog feature is designed for this?

A. A `GRANT SELECT` statement that names the remote workspace directly
B. Delta Sharing (Databricks-to-Databricks) with a recipient/share configuration
C. An external table pointing at Workspace B's underlying cloud storage path
D. Creating a duplicate service principal in Workspace B

---

## Question 14
**Objective:** Use system tables to audit permission changes.

A compliance team needs to review every Unity Catalog permission change (grants and revokes) from the last 30 days. Which system table should they query?

A. `system.access.audit` (filtering `service_name = 'unityCatalog'` and `action_name = 'updatePermissions'`)
B. `system.default.audit_logs`
C. `information_schema.table_privileges` (shows current state only, not history)
D. `system.metadata.permissions_history`

---

## Question 15
**Objective:** Apply the correct privilege for invoking a Unity Catalog function/UDF.

A data scientist needs to call a Python UDF registered in Unity Catalog. What privilege do they need on the function itself (separate from any privileges on tables it might touch internally)?

A. `SELECT` on the function
B. `EXECUTE` on the function
C. `USE CATALOG` on the catalog containing the function (alone, with nothing else)
D. `MODIFY` on the function

---

## Question 16
**Objective:** Add metadata for data discoverability.

A data steward wants to document `prod.hr.salaries` so analysts understand its purpose and refresh cadence without asking the data team. Which command adds a table-level description?

A. `CREATE COMMENT ON TABLE prod.hr.salaries AS '...';` (not valid syntax)
B. `ALTER TABLE prod.hr.salaries SET COMMENT 'HR salaries — source Workday, refreshed every Monday';`
C. `ADD LABEL TO TABLE prod.hr.salaries DESCRIPTION '...';` (not valid syntax)
D. `UPDATE information_schema.tables SET comment = '...' WHERE table_name = 'salaries';` (information_schema is read-only)

---

## Question 17
**Objective:** Use `information_schema` for governance discovery.

A governance team wants every column across `prod` that is missing a description. Which query is correct?

A. `SELECT table_name FROM prod.information_schema.tables WHERE comment IS NULL;` (table-level only, not column-level)
B. `SELECT table_name, column_name FROM prod.information_schema.columns WHERE comment IS NULL;`
C. `SELECT * FROM prod.information_schema.tables WHERE comment IS NULL;` (same issue as A)
D. `SELECT table_name FROM prod.information_schema.column_tags WHERE tag_name = 'description' AND tag_value IS NULL;` (tags ≠ comments, and this table doesn't track descriptions)

---

## Question 18
**Objective:** Understand metadata behavior under cloning operations.

A data engineer runs `DEEP CLONE` on `prod.sales.customers` (tagged `pii=true`) into a staging table for testing. After the clone, the new table shows no tags. What's the most accurate explanation?

A. The tags were set incorrectly — tags should survive `DEEP CLONE`
B. Tags (and comments) are intentionally not carried over by `DEEP CLONE` or `CTAS` — only structure/data is copied, and metadata must be reapplied explicitly
C. The tags survived but aren't visible from the schema the clone landed in
D. `DEEP CLONE` doesn't support tagged source tables at all

---

## Question 19
**Objective:** Understand the two independent ACL planes.

A user has "Can Manage" workspace permission on a notebook, but running it produces a Unity Catalog access-denied error on a `SELECT` against `prod.sales.customers`. What does this tell you?

A. "Can Manage" should have been sufficient — this indicates a bug
B. Workspace ACLs (governing the notebook) and Unity Catalog ACLs (governing the table) are independent; fixing the notebook permission won't fix the missing table grant
C. The notebook needs to be re-attached to a different cluster
D. Workspace admins automatically override Unity Catalog grants, so this shouldn't be possible

---

## Answer Key

### Q1: C
Inheritance flows top-down only (catalog → schema → table); a table-level grant never propagates up to the schema or sideways to sibling tables. The analyst needs an explicit grant on `orders` too.
**Wrong options:** A/B assume upward inheritance, which doesn't exist. D is false — each table's grants are independent; one grant doesn't block others.

### Q2: C
Full traversal + read (`USE CATALOG`, `USE SCHEMA`, `SELECT` at catalog level) plus scoped write (`USE SCHEMA`, `MODIFY` on `prod.etl` only) is both correct syntax and minimal scope.
**Wrong options:** A is far too broad. B grants catalog-wide `MODIFY`, which lets the engineer write anywhere in `prod`, not just `etl` — violates least privilege. D omits `USE CATALOG` and any read/write privilege entirely — non-functional.

### Q3: B
Since Unity Catalog has no `DENY`, the only ways to exclude one table from an otherwise-broad grant are structural (don't include that schema in the broad grant to begin with) or row filters/column masks at the data layer.
**Wrong options:** A doesn't work — `DENY` isn't supported on UC objects. C is nonsensical (revoking something never granted does nothing useful and doesn't create a restriction). D is security-by-obscurity, not an access control.

### Q4: B
`SELECT` is exactly read access with no write capability — the correct least-privilege grant for a view-only role.
**Wrong options:** A grants write too (`MODIFY`). C only allows traversal, not reading data. D only allows listing objects, not reading their contents.

### Q5: D
Both `USE CATALOG` on `prod` and `USE SCHEMA` on `prod.sales` are required before the `SELECT` on the schema (which does cascade to the table) can take effect — without traversal rights at both levels above, the query fails before it can even reach the table.
**Wrong options:** A is wrong because the schema-level `SELECT` already covers the table once traversal works. B/C are each individually necessary but not sufficient alone — both are required together.

### Q6: C
Tables (and catalogs, schemas, views, volumes, functions, models) registered in Unity Catalog use `GRANT`/`REVOKE` SQL. Notebooks, legacy MLflow experiments, and job definitions all use workspace-level permissions instead.
**Wrong options:** A, B, D are all workspace-ACL-governed objects.

### Q7: A
Full-catalog read plus schema-scoped create/write in staging is the minimal correct set.
**Wrong options:** B is far too broad (write access everywhere in `prod`). C omits catalog-wide read entirely — the pipeline couldn't read tables outside staging. D grants catalog-wide `MODIFY`, again too broad.

### Q8: A
With no deny mechanism in Unity Catalog and no other restriction in place, the catalog-level `SELECT` grant simply applies — the group can read `prod.finance.salaries` along with everything else in `prod`. This is precisely why the "no DENY" fact matters operationally: broad grants really do apply broadly unless you design your schema/catalog boundaries to prevent it up front.
**Wrong options:** B assumes an automatic sensitivity-based carve-out that doesn't exist. C misunderstands what `SELECT` vs. `MODIFY` control. D references a mechanism (`DENY`) that doesn't apply to Unity Catalog objects.

### Q9: B
`SHOW GRANTS [principal] ON <securable_object>` is the actual syntax — the object is required, and the principal (in backticks) is optional and scopes the result to that principal on that object.
**Wrong options:** A has no such bare "FOR" form for account-wide results. C isn't a real Databricks SQL statement. D is nonsensical — `SHOW CATALOGS` doesn't take a principal argument.

### Q10: B
`MANAGE` is a delegated-administration privilege — it lets the holder grant/revoke privileges on the object, but it does not itself confer `SELECT`/`MODIFY`/etc.; a `MANAGE` holder who wants data access must explicitly grant it to themselves.
**Wrong options:** A conflates `MANAGE` with ownership — they're different. C is false; `MANAGE` is not part of `ALL PRIVILEGES` in current UC (in fact `ALL PRIVILEGES` explicitly excludes `MANAGE`, `EXTERNAL USE SCHEMA`, and `EXTERNAL USE LOCATION` to prevent privilege escalation). D is false — `MANAGE` is very much a Unity Catalog concept.

### Q11: B
External locations bind a storage path to a credential; granting `READ FILES`/`WRITE FILES` on the named external location — not the raw bucket path — is the governed, least-privilege pattern.
**Wrong options:** A — "external schema" isn't the right construct for raw storage access control. C — wrapping in a managed table doesn't control the underlying bucket the way an external location does. D — volumes are for managed/external file access within the catalog, a different (though related) construct from external-location-level bucket access.

### Q12: B
Ownership is tracked as a distinct, per-object attribute independent of `ALL PRIVILEGES`; only the current owner (or an admin with sufficient rights) can transfer it via an explicit ownership-transfer action.
**Wrong options:** A overstates what `ALL PRIVILEGES` includes. C is unnecessary — ownership transfer doesn't require being a metastore admin if you're already the owner. D is false in general — the *creator* does become the initial owner, but that's a different fact from what `ALL PRIVILEGES` grants a *different* user.

### Q13: B
Delta Sharing (Databricks-to-Databricks) is purpose-built for securely sharing live data across metastores/workspaces/organizations via shares and recipients (Day 20 covers this in depth).
**Wrong options:** A — no such cross-workspace `GRANT` target exists. C — pointing at the same storage doesn't give you Unity Catalog-governed access control or auditability. D — duplicating a service principal doesn't solve cross-metastore data governance.

### Q14: A
`system.access.audit` is the real, documented audit log system table; permission changes appear there under `service_name = 'unityCatalog'` with action names like `updatePermissions`.
**Wrong options:** B, D name system tables that don't exist. C shows only the current grant state, not a history of changes over time.

### Q15: B
`EXECUTE` is the specific privilege required to invoke a Unity Catalog function/UDF — this exists precisely so callers don't need direct access to whatever tables the function touches internally.
**Wrong options:** A — `SELECT` isn't a valid/necessary privilege on a function object for this purpose. C — catalog-level `USE CATALOG` alone doesn't grant the ability to execute anything within it. D — `MODIFY` is a table-write privilege, unrelated to invoking a function.

### Q16: B
`ALTER TABLE ... SET COMMENT '...'` is the correct, valid syntax for a table-level description, visible via `DESCRIBE`, Catalog Explorer, and `information_schema.tables.comment`.
**Wrong options:** A and C are not valid Databricks SQL syntax. D is invalid — `information_schema` views are read-only; you alter metadata via `ALTER`/`COMMENT ON`, not `UPDATE`.

### Q17: B
`information_schema.columns` exposes column-level `comment`; filtering `WHERE comment IS NULL` there finds columns lacking descriptions.
**Wrong options:** A and C check `information_schema.tables.comment`, which is table-level, not column-level. D references a table that doesn't track descriptions this way.

### Q18: B
Tags and comments are not copied by `DEEP CLONE` or `CTAS` — only the table's structure and data are copied. This is by design, not a bug, and is a real governance gap teams need to guard against by reapplying metadata as part of any cloning pipeline step.
**Wrong options:** A is factually backwards. C is incorrect — the tags are genuinely absent, not merely hidden. D is false — cloning tagged tables works fine; it's the tags specifically that don't transfer.

### Q19: B
Workspace ACLs (governing notebooks/jobs/clusters) and Unity Catalog ACLs (governing tables/schemas/catalogs) are two fully independent security planes. A user can have maximal permission on one and none on the other.
**Wrong options:** A misdiagnoses this as a bug — it's expected, correct behavior. C is an unrelated, irrelevant fix. D is false — admins aren't automatically exempt from UC checks just by virtue of a workspace-level role (only actual metastore/account admins bypass UC checks, which is a different thing from workspace "Can Manage").

---

## Difficulty Ratings

| Q# | Difficulty | Topic |
|---|---|---|
| 1 | Medium | Hierarchy: table grants don't flow to siblings |
| 2 | Medium | Least-privilege catalog + scoped-schema grants |
| 3 | Hard | No `DENY` in UC — structural/row-filter alternative |
| 4 | Easy | `SELECT` = read-only |
| 5 | Hard | Full traversal chain (`USE CATALOG` + `USE SCHEMA`) required |
| 6 | Easy | UC ACL vs. workspace ACL scope |
| 7 | Medium | Least-privilege pipeline grants |
| 8 | Hard | Broad grants apply broadly — no automatic sensitive-data carve-out |
| 9 | Medium | Correct `SHOW GRANTS` syntax |
| 10 | Medium | `MANAGE` vs. ownership vs. `ALL PRIVILEGES` |
| 11 | Medium | External location security |
| 12 | Medium | `ALL PRIVILEGES` vs. ownership |
| 13 | Hard | Cross-workspace access via Delta Sharing |
| 14 | Medium | `system.access.audit` for permission-change history |
| 15 | Medium | `EXECUTE` privilege for functions/UDFs |
| 16 | Easy | `ALTER TABLE ... SET COMMENT` syntax |
| 17 | Medium | `information_schema.columns` for column-level gaps |
| 18 | Medium | Tags/comments not preserved by `DEEP CLONE`/CTAS |
| 19 | Easy | Two independent ACL planes |
