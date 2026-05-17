---
description: >-
  Grant USAGE / MONITOR on a Snowflake Cortex Search Service.
---

# CortexSearchService

[Snowflake Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview) | Snowcap CLI label: `cortex_search_service`

A Cortex Search Service is a schema-scoped Snowflake AI service that exposes
semantic + lexical search over a base table or view. Snowcap supports granting
access to existing services declaratively. The service itself (the
`CREATE CORTEX SEARCH SERVICE ... AS <query>` body, embedding model,
`target_lag`, attribute set, etc.) is **not** modeled as a concrete resource —
create it via DDL or dbt, then manage who can call it through `grants:`.

## Examples

### YAML

```yaml
grants:
  # Required to call SNOWFLAKE.CORTEX.SEARCH_PREVIEW() against the service.
  - priv: USAGE
    on: cortex search service somedb.someschema.transcript_search
    to: customer_support

  # Required for get_ai_observability_events() / Cortex Search request logs.
  - priv: MONITOR
    on: cortex search service somedb.someschema.transcript_search
    to: search_observability_role

  # Schema-scope privilege to allow a role to create new services.
  - priv: CREATE CORTEX SEARCH SERVICE
    on: schema somedb.someschema
    to: search_author_role
```

### Python

```python
# Grant USAGE
grant = Grant(
    priv="USAGE",
    on_cortex_search_service="somedb.someschema.transcript_search",
    to="customer_support",
)

# Grant MONITOR
grant = Grant(
    priv="MONITOR",
    on_cortex_search_service="somedb.someschema.transcript_search",
    to="search_observability_role",
)
```

## Privileges

| Privilege   | Purpose                                                                 |
|-------------|-------------------------------------------------------------------------|
| `USAGE`     | Call `SNOWFLAKE.CORTEX.SEARCH_PREVIEW(...)` against the service.        |
| `MONITOR`   | Read request logs via `get_ai_observability_events(...)`.              |
| `OWNERSHIP` | Standard ownership semantics — drop, alter, transfer.                  |
| `ALL`       | Convenience: expand to all of the above.                                |

The schema-scope privilege `CREATE CORTEX SEARCH SERVICE` is part of
[Grant](grant.md) under [SchemaPriv] — see the schema-privileges example
above.

## Minimal example: full Cortex access for a developer role

A common goal is "let this role use Cortex Code in Snowsight, call Cortex AI
SQL functions, and query our Cortex Search Service." Three pieces stack
together:

```yaml
# 1. Account-level privilege for Cortex AI SQL (AI_COMPLETE, AI_FILTER,
#    SUMMARIZE, embeddings, etc.). Granted to PUBLIC by default — declare it
#    explicitly so access survives a future PUBLIC revoke.
grants:
  - priv: USE AI FUNCTIONS
    on: ACCOUNT
    to: dbt_developer

  # 2. (Optional) USAGE on the search service itself
  - priv: USAGE
    on: cortex search service db_dev.cortex.faq_search
    to: dbt_developer

# 3. Database-role grants on the SNOWFLAKE shared database. COPILOT_USER is
#    required for the Cortex Code pane in Snowsight. CORTEX_USER (or
#    CORTEX_AGENT_USER) is required for Cortex AI SQL functions and Cortex
#    Code's underlying calls.
database_role_grants:
  - database_role: SNOWFLAKE.COPILOT_USER
    roles:
      - dbt_developer
  - database_role: SNOWFLAKE.CORTEX_USER
    roles:
      - dbt_developer
```

### Gotchas

- `SNOWFLAKE.CORTEX_USER` is granted to `PUBLIC` by default, so a role
  inherits it transitively unless your account has revoked that default.
  `SNOWFLAKE.COPILOT_USER` is **not** granted to `PUBLIC` — without an
  explicit grant the Cortex Code pane is hidden in Snowsight.
- Declaring a `database_role_grants` entry for a role that is already
  granted to another grantee (e.g. `ACCOUNTADMIN`) requires snowcap ≥ the
  release containing the multi-grantee fetch fix. Earlier versions emit a
  spurious `UpdateResource(to_role: ACCOUNTADMIN → <new>)` diff instead of
  a clean create.
- `USE AI FUNCTIONS ON ACCOUNT` is the account privilege, separate from
  the `SNOWFLAKE.CORTEX_USER` database role. Both are typically required
  for Cortex AI SQL calls; missing either produces a
  "Function requires X privilege" error at runtime.
- Querying a search service also requires `USAGE` on its parent database
  and schema. If those are absent the call fails before reaching the
  service-level USAGE check.

## See also

- [Snowflake — Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Snowflake — Cortex Code access control](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-snowsight#access-control-requirements)
- [Snowflake — Cortex AI SQL required privileges](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql#required-privileges)
- [Snowflake — Cortex Search Monitor / logs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-monitor)
- [DatabaseRole](database_role.md) — for granting `SNOWFLAKE.*` database roles
- [Grant](grant.md) — for the underlying grant resource and YAML schema
