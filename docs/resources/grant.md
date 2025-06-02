---
description: >-
  Documentation for the Grant resource, which represents privilege grants in Snowflake.
---

# Grant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

The `Grant` resource represents a privilege grant, a future grant, or a grant of privileges on all resources of a specified type to a role in Snowflake.

## Examples

### Python

#### Object Grants

```python
# Global Privileges:
grant = Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")

# Warehouse Privileges:
grant = Grant(priv="OPERATE", on=Warehouse(name="foo"), to="somerole")
grant = Grant(priv="OPERATE", on_warehouse="foo", to="somerole")

# Schema Privileges:
grant = Grant(priv="CREATE TABLE", on=Schema(name="foo"), to="somerole")
grant = Grant(priv="CREATE TABLE", on_schema="foo", to="somerole")

# Table Privileges:
grant = Grant(priv=["SELECT", "INSERT", "DELETE"], on_table="sometable", to="somerole")
```

#### Future Grants

```python
# Database Object Privileges:
future_grant = Grant(
    priv="CREATE TABLE",
    on=["FUTURE", "SCHEMAS", Database(name="somedb")],
    to="somerole",
)
future_grant = Grant(
    priv="CREATE TABLE",
    on="future schemas in database somedb",
    to="somerole",
)

# Schema Object Privileges:
future_grant = Grant(
    priv=["SELECT", "INSERT"],
    on=["future", "tables", "in", Schema(name="someschema")],
    to="somerole",
)
future_grant = Grant(
    priv="READ",
    on="future image repositories in schema someschema",
    to="somerole",
)
```

#### Grants on All Resources

```python
# Schema Privileges:
grant_on_all = Grant(
    priv="CREATE TABLE",
    on="all schemas in database somedb",
    to="somerole",
)
grant_on_all = Grant(
    priv="CREATE VIEW",
    on=["all", "schemas", Database(name="somedb")],
    to="somerole",
)

# Schema Object Privileges:
grant_on_all = Grant(
    priv=["SELECT", "INSERT"],
    on="all tables in schema someschema",
    to="somerole",
)
grant_on_all = Grant(
    priv="SELECT",
    on="ALL VIEWS IN DATABASE SOMEDB",
    to="somerole",
)
```

### YAML

#### Object Grants

```yaml
grants:
  - priv:
      - SELECT
      - INSERT
    on_table: some_table
    to: some_role
    grant_option: true
  - priv: SELECT
    on: schema someschema
    to: some_role
    grant_option: true
```

#### Future Grants

```yaml
grants:
  - priv:
      - SELECT
      - INSERT
    on:
      - future tables in schema someschema
      - future views in schema someschema
    to: somerole
```

#### Grants on All Resources

```yaml
grants:
  - priv:
      - SELECT
      - INSERT
    on:
      - all tables in schema someschema
      - all views in schema someschema
    to: somerole
```

## Fields

- **`priv`** (`string` or `list`, required):  
  The privilege(s) to grant. Examples include `"SELECT"`, `"INSERT"`, `"CREATE TABLE"`.

- **`on`** (`string` or [Resource](resource.md), required):  
  The resource on which the privilege is granted.  
  - Can be a string like `"ACCOUNT"`, a specific resource object, or a list of strings containing:
    - **grant type**: `"FUTURE"` or `"ALL"`
    - **contained items type**: Any resource contained in a Database or Schema (e.g., `"tables"`, `"schemas"`)
    - **container object type**: `"Database"` or `"Schema"`
    - **container object name**: The name of the database or schema
  - The list items can be joined using whitespace and provided as a single string (e.g., `"future tables in schema someschema"`).
  - Also, multiple grants can be specified at once using a list of strings e.g.:
    - `"future tables in schema someschema"`
    - `"all views in schema someschema"`

- **`to`** (`string` or [Role](role.md), required):  
  The role to which the privileges are granted.

- **`grant_option`** (`bool`, optional):  
  Specifies whether the grantee can grant the privileges to other roles. Defaults to `false`.

- **`owner`** (`string` or [Role](role.md), optional):  
  The owner role of the grant. Defaults to `"SYSADMIN"`.
