---
description: >-
  A Streamlit app in Snowflake.
---

# Streamlit

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit) | Snowcap CLI label: `streamlit`

Represents a Streamlit app in Snowflake, which is a schema-scoped resource for creating interactive applications using Python code.

## Examples

### YAML

```yaml
streamlits:
  # From a stage
  - name: my_db.my_schema.my_streamlit
    from: "@my_stage"
    main_file: app.py
    title: My Streamlit App
    query_warehouse: my_warehouse
    comment: A sample Streamlit app from a stage
    owner: SYSADMIN
    tags:
      project: demo

  # From a Git repository
  - name: my_streamlit
    from: https://github.com/user/repo.git
    version: main
    main_file: app.py
    title: Repo Streamlit App
```

### Python

```python
# Creating a Streamlit app from a stage
streamlit_stage = Streamlit(
    name="my_db.my_schema.my_streamlit",
    from_="@my_stage",
    main_file="app.py",
    title="My Streamlit App",
    query_warehouse="my_warehouse",
    comment="A sample Streamlit app from a stage",
    tags={"project": "demo"}
)

# Creating a Streamlit app from a Git repository
streamlit_repo = Streamlit(
    name="my_streamlit",
    from_="https://github.com/user/repo.git",
    version="main",
    main_file="app.py",
    title="Repo Streamlit App",
    owner="SYSADMIN"
)
```

## Fields

* `name` (string, required) - The name of the Streamlit app. Can be a fully qualified name (e.g., "database.schema.app_name").
* `from_` (string, required) - The source of the Streamlit app. This can be either a stage (e.g., '@mystage') or a repository URL (e.g., 'https://github.com/user/repo.git').
* `version` (string) - The version or branch of the repository to use. Only applicable if from_ is a repository URL.
* `main_file` (string) - The name of the main Python file for the Streamlit app (e.g., 'app.py').
* `title` (string) - The display title of the Streamlit app.
* `query_warehouse` (string) - The name of the warehouse to use for queries in the app.
* `comment` (string) - A comment or description for the Streamlit app.
* `owner` (string or Role) - The role that owns the Streamlit app. Defaults to "SYSADMIN".
* `tags` (dict) - A dictionary of tags to associate with the Streamlit app.

## Granting access

A Streamlit app is a schema-scoped object. Grant `USAGE` on the app to let a
role open and run it — this is the whole access story for viewers when the app
uses owner's rights (all app queries execute as the app owner, so viewers need
no privileges on the underlying tables):

```yaml
grants:
  # Let a viewer role open and run the app.
  - priv: USAGE
    on: streamlit my_db.my_schema.my_streamlit
    to: app_viewer_role

  # Schema-scope privilege to allow a role to create Streamlit apps.
  - priv: CREATE STREAMLIT
    on: schema my_db.my_schema
    to: app_developer_role
```

```python
# Grant USAGE so a role can open the app
grant = Grant(
    priv="USAGE",
    on_streamlit="my_db.my_schema.my_streamlit",
    to="app_viewer_role",
)
```

| Privilege         | Purpose                                                                  |
|-------------------|--------------------------------------------------------------------------|
| `USAGE`           | Open, view, and run the Streamlit app (and `DESCRIBE` it).               |
| `OWNERSHIP`       | Full control. Set at create/deploy time — Snowflake does not support transferring streamlit ownership via `GRANT OWNERSHIP`. |
| `ALL`             | All privileges above.                                                    |

The schema-scope privilege `CREATE STREAMLIT` lets a role create apps in that
schema; creating an app with a `ROOT_LOCATION` stage also needs `CREATE STAGE`.
