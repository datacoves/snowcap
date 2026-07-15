---
description: >-
  Grant USAGE / MONITOR on a Snowflake dbt project object (dbt Projects on Snowflake).
---

# DbtProject

[Snowflake Documentation](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-access-control) | Snowcap CLI label: `dbt_project`

A dbt project object (part of *dbt Projects on Snowflake*) is a schema-scoped
object that packages a dbt project so it can be executed with `EXECUTE DBT
PROJECT`. Snowcap supports granting access to existing dbt project objects
declaratively. The object itself (`CREATE DBT PROJECT ... FROM <workspace|stage>`
with its profiles/target config) is **not** modeled as a concrete resource —
create it via a Snowflake Workspace or DDL, then manage who can execute and
monitor it through `grants:`.

## Examples

### YAML

```yaml
grants:
  # Required to EXECUTE DBT PROJECT and to list/retrieve the project's files.
  - priv: USAGE
    on: dbt project somedb.someschema.analytics_dbt
    to: transformer_role

  # Required to view the project (details + run history) in Snowsight.
  - priv: MONITOR
    on: dbt project somedb.someschema.analytics_dbt
    to: analytics_observer

  # Schema-scope privilege to allow a role to create new dbt project objects
  # (e.g. deploying from a Workspace).
  - priv: CREATE DBT PROJECT
    on: schema somedb.someschema
    to: dbt_author_role
```

### Python

```python
# Grant USAGE (execute the project + read its files)
grant = Grant(
    priv="USAGE",
    on_dbt_project="somedb.someschema.analytics_dbt",
    to="transformer_role",
)

# Grant MONITOR (Snowsight project details + run history)
grant = Grant(
    priv="MONITOR",
    on_dbt_project="somedb.someschema.analytics_dbt",
    to="analytics_observer",
)
```

## Privileges

| Privilege          | Purpose                                                                                     |
|--------------------|---------------------------------------------------------------------------------------------|
| `USAGE`            | Execute the dbt project (`EXECUTE DBT PROJECT`) and list/retrieve its files.                 |
| `MONITOR`          | View the project's details and run history in Snowsight.                                     |
| `OWNERSHIP`        | Full control of the object. Exclusive to a single role (like tasks).                         |
| `ALL`              | All privileges above.                                                                        |

The schema-scope privilege `CREATE DBT PROJECT` (see [Schema](#) grants) lets a
role create dbt project objects in that schema.

## Minimal example

Two roles, least privilege: one runs the project, one only watches it in
Snowsight. Neither owns it — ownership stays with the deploying role.

```yaml
grants:
  - priv: USAGE
    on: dbt project analytics.transforms.daily_models
    to: dbt_runner
  - priv: MONITOR
    on: dbt project analytics.transforms.daily_models
    to: analytics_viewer
```

> Note: in a managed-access schema, granting these privileges is restricted to
> the schema owner or a role with `MANAGE GRANTS` — the same constraint that
> applies to tasks.
