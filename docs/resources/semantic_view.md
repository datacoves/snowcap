---
description: >-
  Grant SELECT / REFERENCES / MONITOR on a Snowflake Semantic View.
---

# SemanticView

[Snowflake Documentation](https://docs.snowflake.com/en/user-guide/views-semantic/overview) | Snowcap CLI label: `semantic_view`

A Semantic View is a schema-scoped Snowflake object that defines business
metrics, dimensions, and relationships over one or more tables, used by
Cortex Analyst and other consumers to query data in business terms. Snowcap
supports granting access to existing semantic views declaratively. The
semantic view itself (the `CREATE SEMANTIC VIEW ... TABLES (...) FACTS (...)
DIMENSIONS (...) METRICS (...)` body) is **not** modeled as a concrete
resource — create it via DDL or dbt, then manage who can query it through
`grants:`.

## Examples

### YAML

```yaml
grants:
  # Required to query the semantic view (e.g. via Cortex Analyst).
  - priv: SELECT
    on: semantic view somedb.someschema.sales_metrics
    to: analyst_role

  # Required to inspect the semantic view's structure via the information schema.
  - priv: REFERENCES
    on: semantic view somedb.someschema.sales_metrics
    to: analyst_role

  # Required for get_ai_observability_events() / Cortex Analyst request logs.
  - priv: MONITOR
    on: semantic view somedb.someschema.sales_metrics
    to: search_observability_role

  # Schema-scope privilege to allow a role to create new semantic views.
  - priv: CREATE SEMANTIC VIEW
    on: schema somedb.someschema
    to: semantic_view_author_role

  # Bulk + future grants. FUTURE is what survives a rebuild: a semantic view
  # that dbt (or any CREATE OR REPLACE) recreates loses its object-level grants,
  # but a future grant on the schema re-applies SELECT to the new object.
  - priv: SELECT
    on: all semantic views in schema somedb.someschema
    to: analyst_role
  - priv: SELECT
    on: future semantic views in schema somedb.someschema
    to: analyst_role
```

### Python

```python
# Grant SELECT
grant = Grant(
    priv="SELECT",
    on_semantic_view="somedb.someschema.sales_metrics",
    to="analyst_role",
)

# Grant REFERENCES
grant = Grant(
    priv="REFERENCES",
    on_semantic_view="somedb.someschema.sales_metrics",
    to="analyst_role",
)

# Grant MONITOR
grant = Grant(
    priv="MONITOR",
    on_semantic_view="somedb.someschema.sales_metrics",
    to="search_observability_role",
)
```

## Privileges

| Privilege    | Purpose                                                                 |
|--------------|---------------------------------------------------------------------------|
| `SELECT`     | Query the semantic view — sufficient on its own, without `SELECT` on the underlying tables. |
| `REFERENCES` | Inspect the semantic view's structure via the information schema.       |
| `MONITOR`    | Read Cortex Analyst request logs via `get_ai_observability_events(...)`. |
| `OWNERSHIP`  | Standard ownership semantics — drop, alter, transfer.                  |
| `ALL`        | Convenience: expand to all of the above.                                |

The schema-scope privilege `CREATE SEMANTIC VIEW` is part of
[Grant](grant.md) under `SchemaPriv` — see the schema-privileges example
above.

## How snowcap resolves a semantic view

At plan time snowcap verifies that every grant target exists (`SHOW SEMANTIC
VIEWS ... IN SCHEMA <db>.<schema>`) and reads back the grant itself from `SHOW
GRANTS TO ROLE <role>` / `SHOW FUTURE GRANTS IN SCHEMA`, where Snowflake reports
the object type as `SEMANTIC_VIEW`. A grant on a semantic view that does not
exist yet fails the plan the same way a grant on a missing table does — create
the view (dbt, DDL) before declaring grants on it, or use a `future semantic
views in schema` grant, which needs only the schema to exist.

`snowcap export` lists semantic views for `sync_resources` the same way it lists
views and dynamic tables; the objects themselves are never created, altered, or
dropped by snowcap — only their grants are managed.

## See also

- [Snowflake — Semantic views overview](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Grant](grant.md) — for the underlying grant resource and YAML schema
