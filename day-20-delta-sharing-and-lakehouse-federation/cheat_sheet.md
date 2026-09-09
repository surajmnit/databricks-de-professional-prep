# Day 20 — Cheat Sheet: Delta Sharing and Lakehouse Federation

## Delta Sharing vs Lakehouse Federation

| | Delta Sharing | Lakehouse Federation |
|---|---|---|
| Purpose | Share your data with others | Query others' data from Databricks |
| Direction | Databricks → Outside | Outside → Databricks |
| Data Location | Provider's storage (no copy) | Source database (no copy) |
| Real-time | Yes | Yes |
| Recipient | Databricks workspace or any platform | Databricks workspace |

---

## Delta Sharing

**Types:**
- **D2D (Databricks-to-Databricks):** Recipient is a Databricks account. Uses Unity Catalog recipient identity.
- **D2O (Databricks-to-Other):** Recipient is any platform (Snowflake, Redshift, Power BI, etc.). Uses OPEN recipient + REST API.

**Key concept:** Access-based, not copy-based. Revoke = immediate loss of access. Data never leaves provider's storage.

**Recipient types:**
- Account-based recipient: Databricks workspace identity
- OPEN recipient: Non-Databricks platforms via REST API

---

## Lakehouse Federation

**Supported sources:** MySQL, PostgreSQL, Amazon Redshift, Snowflake, BigQuery, Azure Synapse, Databricks.

**Key concept:** Federated queries push computation to source DB. Only results returned to Databricks.

---

## Key Exam Traps

1. **Q5 direction trap:** Delta Sharing = Databricks OUT. Lakehouse Federation = Outside IN.
2. **Federated query = no data copy** — only results returned.
3. **Row filters/column masks work on federated tables** — UC applies uniformly.
4. **Delta Sharing revocation = immediate** — no data copy exists to delete.
5. **OPEN recipient = non-Databricks platforms.** Account recipient = Databricks workspaces.
