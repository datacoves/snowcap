---
description: >-
  
---

# Grant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege)

Represents a grant, a future grant, or a grant of privileges on all resources of a specified type to a role in Snowflake.

## Examples

### Python

#### Object grants

```python
# Global Privs:
grant = Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")
# Warehouse Privs:
grant = Grant(priv="OPERATE", on=Warehouse(name="foo"), to="somerole")
grant = Grant(priv="OPERATE", on_warehouse="foo", to="somerole")
# Schema Privs:
grant = Grant(priv="CREATE TABLE", on=Schema(name="foo"), to="somerole")
grant = Grant(priv="CREATE TABLE", on_schema="foo", to="somerole")
# Table Privs:
grant = Grant(priv="SELECT", on_table="sometable", to="somerole")
```

#### Future grants

```python
# Database Object Privs:
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
# Schema Object Privs:
future_grant = Grant(
    priv="SELECT",
    on=["future", "tables", "in", Schema(name="someschema")],
    to="somerole",
)
future_grant = Grant(
    priv="READ",
    on="future image repositories in schema someschema",
    to="somerole",
)
```

#### Grant on All

```python
    # Schema Privs:
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
    # Schema Object Privs:
    grant_on_all = Grant(
        priv="SELECT",
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

#### Object grants

```yaml
- grants:
    priv: "SELECT"
    on_table: "some_table"
    to: "some_role"
    grant_option: true
```

#### Future grants

```yaml
grants:
  - priv: SELECT
    on: future tables in schema someschema
    to: somerole
```

#### Grant on All

```yaml
grants:
  - priv: SELECT
    on: all tables in schema someschema
    to: somerole
```


## Fields

* `priv` (string, required) - The privilege to grant. Examples include 'SELECT', 'INSERT', 'CREATE TABLE'.
* `on` (string or [Resource](resource.md), required) - The resource on which the privilege is granted. Can be a string like 'ACCOUNT', a specific resource object, a list of strings containing [`grant type`, `items type`, `object type`, `object name`], where:
  * `grant type` could be either `FUTURE` or `ALL`,
  * `items type` could be any resource contained on a Database or Schema, and
  * `object type` could be `Database` or `Schema`.
  The list items could be specified as a single string of its elements joined by whitespace.
* `to` (string or [Role](role.md), required) - The role to which the privileges are granted.
* `grant_option` (bool) - Specifies whether the grantee can grant the privileges to other roles. Defaults to False.
* `owner` (string or [Role](role.md)) - The owner role of the grant. Defaults to 'SYSADMIN'.


