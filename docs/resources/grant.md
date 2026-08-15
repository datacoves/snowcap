---
description: >-
  A privilege grant in Snowflake.
---

# Grant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege) | Snowcap CLI label: `grant`

The `Grant` resource represents a privilege grant, a future grant, an inherited grant, or a grant of privileges on all resources of a specified type to a role in Snowflake.

## Examples

### YAML

#### Object Grants

```yaml
grants:
  # Global privileges
  - priv: CREATE WAREHOUSE
    on: ACCOUNT
    to: somerole

  # Single privilege on a table
  - priv: SELECT
    on: table some_table
    to: some_role

  # Multiple privileges on a table
  - priv:
      - SELECT
      - INSERT
    on: table some_table
    to: some_role
    grant_option: true

  # Schema privileges
  - priv: USAGE
    on: schema somedb.someschema
    to: some_role

  # Warehouse privileges
  - priv: USAGE
    on: warehouse some_warehouse
    to: some_role

  # Apps: USAGE on a Streamlit app so a role can open and run it
  - priv: USAGE
    on: streamlit somedb.someschema.some_streamlit
    to: app_viewer_role

  # AI: account-level privilege for Cortex AI SQL functions
  - priv: USE AI FUNCTIONS
    on: ACCOUNT
    to: cortex_user_role

  # AI: USAGE on a Cortex Search Service to call SNOWFLAKE.CORTEX.SEARCH_PREVIEW
  - priv: USAGE
    on: cortex search service somedb.someschema.someservice
    to: search_consumer_role

  # AI: MONITOR on a Cortex Search Service to query observability logs
  - priv: MONITOR
    on: cortex search service somedb.someschema.someservice
    to: search_observability_role

  # AI: USAGE on a Cortex Agent Server, which backs Snowflake's MCP server
  # integration. Snowflake creates one when an account connects an MCP client.
  - priv: USAGE
    on: cortex agent server somedb.someschema.someserver
    to: mcp_client_role

  # AI: schema-scope privilege to allow a role to create semantic views
  - priv: CREATE SEMANTIC VIEW
    on: schema somedb.someschema
    to: semantic_view_author_role

  # AI: SELECT on a Semantic View to query it via Cortex Analyst
  - priv: SELECT
    on: semantic view somedb.someschema.somesv
    to: semantic_view_consumer_role

  # Data Engineering: USAGE on a dbt project object to EXECUTE DBT PROJECT
  - priv: USAGE
    on: dbt project somedb.someschema.analytics_dbt
    to: transformer_role

  # Data Engineering: MONITOR on a dbt project for Snowsight run history
  - priv: MONITOR
    on: dbt project somedb.someschema.analytics_dbt
    to: analytics_observer

  # AI: USAGE on an MCP Server so MCP clients can call its tools
  - priv: USAGE
    on: mcp server somedb.someschema.someserver
    to: mcp_client_role
  # IMPORTED PRIVILEGES on a shared database (see SharedDatabase)
  - priv: IMPORTED PRIVILEGES
    on_database: gong
    to: gong_r

  # SPCS: USAGE on a compute pool
  - priv: usage
    on_compute_pool: ds_gpu_pool
    to_role: data_engineer

  # SPCS: READ on an image repository
  - priv: read
    on_image_repository: sandbox.spcs.vllm_repo
    to_role: data_engineer

  # SPCS: MONITOR on a service
  - priv: monitor
    on_service: sandbox.spcs.lora_service
    to_role: data_engineer
```

#### Future Grants

```yaml
grants:
  - priv:
      - SELECT
      - INSERT
    on: future tables in schema someschema
    to: somerole

  # Multiple future grants
  - priv: SELECT
    on:
      - future tables in schema someschema
      - future views in schema someschema
    to: somerole

  # AI: future semantic views in schema
  - priv: SELECT
    on: future semantic views in schema somedb.someschema
    to: somerole
```

#### Grants on All Resources

```yaml
grants:
  - priv:
      - SELECT
      - INSERT
    on: all tables in schema someschema
    to: somerole

  # Multiple "all" grants
  - priv: SELECT
    on:
      - all tables in schema someschema
      - all views in schema someschema
    to: somerole

  # AI: all semantic views in schema
  - priv: SELECT
    on: all semantic views in schema somedb.someschema
    to: somerole
```

#### Inherited Grants

An inherited grant is a single grant on a container that covers every current **and
future** object of a type inside it, replacing an `all` + `future` pair.

```yaml
grants:
  - priv: SELECT
    on: inherited tables in schema somedb.someschema
    to: somerole

  # Multiple privileges expand to one statement each
  - priv:
      - SELECT
      - INSERT
    on: inherited tables in database somedb
    to: somerole

  # The account can only be the container of an inherited grant
  - priv: SELECT
    on: inherited tables in account
    to: somerole

  # Or turn a grant on all objects into an inherited one
  - priv: SELECT
    on: all tables in database somedb
    inherited: true
    to: somerole

  # Delegate to a role holding MANAGE GRANTS on the container
  - priv: SELECT
    on: inherited tables in database sales_db
    to: analyst
    owner: sales_db_admin
```

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

# MCP Server Privileges:
grant = Grant(priv="USAGE", on_mcp_server="someserver", to="mcp_client_role")
# IMPORTED PRIVILEGES on a shared database (see SharedDatabase):
grant = Grant(priv="IMPORTED PRIVILEGES", on_database="gong", to="gong_r")

# Snowpark Container Services (SPCS) Privileges:
grant = Grant(priv="USAGE", on_compute_pool="ds_gpu_pool", to="data_engineer")
grant = Grant(priv="READ", on_image_repository="sandbox.spcs.vllm_repo", to="data_engineer")
grant = Grant(priv="MONITOR", on_service="sandbox.spcs.lora_service", to="data_engineer")
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

#### Inherited Grants

```python
inherited_grant = Grant(
    priv="SELECT",
    on="INHERITED TABLES IN SCHEMA somedb.someschema",
    to="somerole",
)
inherited_grant = Grant(
    priv="SELECT",
    on=["INHERITED", "TABLES", Database(name="somedb")],
    to="somerole",
)

# The account can only be the container of an inherited grant
inherited_grant = Grant(priv="SELECT", on="INHERITED TABLES IN ACCOUNT", to="somerole")

# Or turn a grant on all objects into an inherited one
inherited_grant = Grant(
    priv="SELECT",
    on="ALL TABLES IN DATABASE somedb",
    inherited=True,
    to="somerole",
)
```

## Fields

- **`priv`** (`string` or `list`, required):  
  The privilege(s) to grant. Examples include `"SELECT"`, `"INSERT"`, `"CREATE TABLE"`.

- **`on`** (`string` or Resource, required):
  The resource on which the privilege is granted. Examples:
  - `"ACCOUNT"` - for account-level privileges
  - `"table my_table"` - for table privileges
  - `"schema my_db.my_schema"` - for schema privileges
  - `"warehouse my_wh"` - for warehouse privileges
  - `"database my_db"` - for database privileges
  - `"semantic view my_db.my_schema.my_sv"` - for semantic view privileges
  - `"compute pool my_pool"` - for compute pool privileges
  - `"image repository my_db.my_schema.my_repo"` - for image repository privileges
  - `"service my_db.my_schema.my_service"` - for service privileges
  - `"future tables in schema my_schema"` - for future grants
  - `"all tables in database my_db"` - for grants on all existing objects
  - `"inherited tables in database my_db"` - for inherited grants, covering existing and future objects
  - `"inherited tables in account"` - inherited grants are the only kind that can be scoped to the account

- **`to`** (`string` or [Role](role.md), required):  
  The role to which the privileges are granted.

- **`grant_option`** (`bool`, optional):  
  Specifies whether the grantee can grant the privileges to other roles. Defaults to `false`.

- **`owner`** (`string` or [Role](role.md), optional):  
  The owner role of the grant. Defaults to `"SYSADMIN"`. Grants are issued as
  `SECURITYADMIN`; for inherited grants, an explicit owner names the role holding
  `MANAGE GRANTS` on the container and is used to issue the grant instead.

- **`inherited`** (`bool`, optional):  
  Turns a grant on all objects in a container into an inherited grant, which also covers
  objects created later. Defaults to `false`.

**Note:** Inherited grants are a Snowflake preview feature, opted into with an account
parameter. Snowcap manages it with an [AccountParameter](account_parameter.md), applied
before any inherited grant that depends on it:

```yaml
account_parameters:
  - name: FEATURE_RBAC_INHERITED_GRANTS
    value: ENABLED
```

`snowcap plan` fails with a clear message if neither the account nor the config has opted
in. Snowflake does not allow inherited grants
to be combined with `WITH GRANT OPTION`, to carry `OWNERSHIP`, or to target shares and
integrations; `priv: ALL` is not supported either, so list privileges explicitly. See
[Managing access with inherited grants](https://docs.snowflake.com/en/user-guide/inherited-grants-intro).

**Note:** `IMPORTED PRIVILEGES` is only valid on a [SharedDatabase](shared_database.md)
(a database created `FROM SHARE`). It cannot be granted `WITH GRANT OPTION`
and can only be granted to account roles, not database roles. Snowflake's
`SHOW GRANTS` reports it as `USAGE` on shared databases — snowcap's fetch
logic handles this quirk transparently.

One `IMPORTED PRIVILEGES` grant also fans out in `SHOW GRANTS` into a row per object the
share exposes — every view, function, procedure, schema, database role, class, tag and
image repository in the database, which on the `SNOWFLAKE` database is several hundred
rows. Those rows are never in your config, so `--sync_resources grant` treats them as
covered by the declared grant rather than revoking them, the same way it treats the
per-object grants produced by an `ALL` or `INHERITED` grant.
