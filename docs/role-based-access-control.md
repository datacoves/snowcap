# Role-Based Access Control Pattern

This guide describes a recommended pattern for managing Snowflake permissions using Snowcap. The pattern uses **composite roles** to provide fine-grained, maintainable access control.

## Overview

Instead of granting privileges directly to users, this pattern creates a hierarchy of roles:

1. **Object Roles** - Grant specific privileges on individual objects (databases, schemas, warehouses, stages)
2. **Base/Composite Roles** - Combine multiple object roles into logical groupings
3. **Functional Roles** - End-user roles that combine base roles and are assigned to users

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              USERS                                          │
│                    noel, jose, svc_airflow                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FUNCTIONAL ROLES                                    │
│                    analyst, loader, transformer_dbt                         │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      BASE / COMPOSITE ROLES                                 │
│                           z_base__analyst                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           OBJECT ROLES                                      │
│   z_db__raw, z_schema__l1_loans, z_wh__wh_transforming, z_stage__...       │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE OBJECTS                                   │
│              databases, schemas, warehouses, stages, tables                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Role Naming Convention

!!! info "Recommended, not required"
    This naming convention is a recommendation to help organize roles. Snowcap does not enforce any specific naming pattern—use whatever works for your organization.

Object roles use a `z_` prefix followed by the object type and name:

| Role Type | Naming Pattern | Example |
|-----------|----------------|---------|
| Database | `z_db__<database_name>` | `z_db__raw` |
| Schema | `z_schema__<schema_name>` | `z_schema__l1_loans` |
| Warehouse | `z_wh__<warehouse_name>` | `z_wh__wh_transforming` |
| Stage (read) | `z_stage__<path>__read` | `z_stage__raw__artifacts__read` |
| Stage (write) | `z_stage__<path>__write` | `z_stage__raw__artifacts__write` |
| Account privileges | `z_account__<privilege>` | `z_account__create_database` |
| Tables/Views | `z_tables_views__<privilege>` | `z_tables_views__select` |
| Base/Composite | `z_base__<name>` | `z_base__analyst` |

The `z_` prefix ensures these object roles sort to the bottom of role lists, making functional roles more visible.

## Directory Structure

Organize your Snowcap configuration into logical files:

```
snowcap/
├── resources/
│   ├── databases.yml           # Database variables
│   ├── schemas.yml             # Schema variables
│   ├── warehouses.yml          # Warehouse variables
│   ├── stages.yml              # Stage definitions + roles + grants
│   ├── roles__base.yml         # Object-level roles + grants
│   ├── roles__functional.yml   # Functional roles + role hierarchy
│   ├── users.yml               # User-to-role assignments
│   └── object_templates/
│       ├── database.yml        # Template for databases + roles + grants
│       ├── schema.yml          # Template for schemas + roles + grants
│       └── warehouses.yml      # Template for warehouses + roles + grants
├── plan.sh
├── apply.sh
└── .env.sample
```

## Configuration Examples

### Define Variables (databases.yml)

Define your resources as variables that templates will iterate over:

```yaml
vars:
  - name: databases
    type: list
    default:
      - name: raw
        owner: loader
        max_data_extension_time_in_days: 10
      - name: analytics
        owner: transformer_dbt
        max_data_extension_time_in_days: 30
      - name: analytics_dev
        owner: transformer_dbt
        max_data_extension_time_in_days: 5
```

### Create Resources with Templates (object_templates/database.yml)

Use `for_each` to create resources, roles, and grants automatically:

```yaml
# Create databases
databases:
  - for_each: var.databases
    name: "{{ each.value.name }}"
    owner: "{{ each.value.owner }}"
    max_data_extension_time_in_days: "{{ each.value.max_data_extension_time_in_days }}"

# Create a role for each database
roles:
  - for_each: var.databases
    name: "z_db__{{ each.value.name }}"

# Grant USAGE on each database to its corresponding role
grants:
  - for_each: var.databases
    priv: USAGE
    on: "database {{ each.value.name }}"
    to: "z_db__{{ each.value.name }}"
```

### Schema Template (object_templates/schema.yml)

```yaml
# Create schemas
schemas:
  - for_each: var.schemas
    name: "{{ each.value.name.split('.')[1] }}"
    database: "{{ each.value.name.split('.')[0] }}"
    owner: "{{ each.value.get('owner', parent.owner) }}"

# Create a role for each schema
roles:
  - for_each: var.schemas
    name: "z_schema__{{ each.value.name.split('.')[1] }}"

# Grant USAGE on each schema to its corresponding role
grants:
  - for_each: var.schemas
    priv: USAGE
    on: "schema {{ each.value.name }}"
    to: "z_schema__{{ each.value.name.split('.')[1] }}"
```

### Warehouse Template (object_templates/warehouses.yml)

```yaml
# Create warehouses
warehouses:
  - for_each: var.warehouses
    name: "{{ each.value.name }}"
    warehouse_size: "{{ each.value.size }}"
    auto_suspend: "{{ each.value.auto_suspend }}"
    auto_resume: true
    initially_suspended: true

# Create a role for each warehouse
roles:
  - for_each: var.warehouses
    name: "z_wh__{{ each.value.name }}"

# Grant USAGE and MONITOR on each warehouse to its corresponding role
grants:
  - for_each: var.warehouses
    priv:
      - USAGE
      - MONITOR
    on: "warehouse {{ each.value.name }}"
    to: "z_wh__{{ each.value.name }}"
```

### Base Object Roles (roles__base.yml)

Define additional object-level roles and their grants:

```yaml
roles:
  - name: z_account__create_database
  - name: z_db__analytics_dev__create_schema
  - name: z_schemas__db__raw
  - name: z_tables_views__select

grants:
  # Grant CREATE DATABASE at account level
  - priv: "CREATE DATABASE"
    on: "ACCOUNT"
    to: z_account__create_database

  # Grant CREATE SCHEMA on a specific database
  - priv: "CREATE SCHEMA"
    on: "database analytics_dev"
    to: z_db__analytics_dev__create_schema

  # Grant USAGE on all current and future schemas in a database
  - priv: "USAGE"
    on:
      - "all schemas in database raw"
      - "future schemas in database raw"
    to: z_schemas__db__raw

  # Grant SELECT on all tables and views across databases
  - for_each: var.databases
    priv: "SELECT"
    on:
      - "all tables in database {{ each.value.name }}"
      - "all views in database {{ each.value.name }}"
      - "future tables in database {{ each.value.name }}"
      - "future views in database {{ each.value.name }}"
    to: z_tables_views__select
```

!!! warning "Database-level future grants can be silently ignored"

    When future grants exist on the **same object type** at both the database and the
    schema level, Snowflake gives the schema-level grant precedence and
    [ignores the database-level grant](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege#future-grants-on-database-or-schema-objects)
    for that schema. Objects created there never receive the privilege, and nothing
    fails — access is simply missing.

    This is easy to trip over with managed access schemas, where privilege management is
    centralized on the schema owner: the schema-level future grant that shadows the
    database-level one is often added later, by a different config or a different team.
    Managed access does not by itself disable database-level future grants (the one
    exception is future grants of `OWNERSHIP`, which Snowcap does not support), but it is
    where the conflict tends to appear.

    If your schemas use `managed_access: true`, declare the future grants at the schema
    level, in the same template that creates the schemas:

    ```yaml
    grants:
      - for_each: var.schemas
        priv: SELECT
        on:
          - "all tables in schema {{ each.value.name }}"
          - "all views in schema {{ each.value.name }}"
          - "future tables in schema {{ each.value.name }}"
          - "future views in schema {{ each.value.name }}"
        to: z_tables_views__select
    ```

    `snowcap plan` warns when it finds database-level future grants in a database that
    contains managed access schemas, and when a schema-level future grant already shadows
    a database-level one.

!!! tip "Inherited grants avoid this problem entirely"

    Snowflake's [inherited grants](https://docs.snowflake.com/en/user-guide/inherited-grants-intro)
    replace an `ALL` + `FUTURE` pair with a single container-level grant covering every
    current and future object of a type. They are **not** subject to the precedence rule
    above: a database-level and a schema-level inherited grant both apply, and managed
    access schemas do not change that.

    ```yaml
    grants:
      - priv: SELECT
        on: INHERITED TABLES IN DATABASE sales_db
        to: analyst
    ```

    See [Inherited grants](#inherited-grants) below for the full syntax, the account
    requirements, and how to migrate an existing `ALL` + `FUTURE` pair.

### Functional Roles and Hierarchy (roles__functional.yml)

Define functional roles and assemble the role hierarchy:

```yaml
roles:
  # Base composite role
  - name: z_base__analyst

  # Functional roles (assigned to users)
  - name: analyst
  - name: loader
  - name: transformer_dbt

role_grants:
  # Assemble the base analyst role from object roles
  - to_role: z_base__analyst
    roles:
      # Database access
      - z_db__raw
      - z_db__analytics

      # Schema access
      - z_schemas__db__raw
      - z_schema__l1_loans
      - z_schema__l2_loan_analytics

      # Warehouse access
      - z_wh__wh_transforming

  # Grant base role + SELECT privileges to analyst
  - to_role: analyst
    roles:
      - z_base__analyst
      - z_tables_views__select

  # Loader gets warehouse access for loading data
  - to_role: loader
    roles:
      - z_wh__wh_loading

  # Transformer gets elevated privileges
  - to_role: transformer_dbt
    roles:
      - z_account__create_database
      - z_db__raw
      - z_schemas__db__raw
      - z_wh__wh_transforming
      - z_tables_views__select
```

### User Assignments (users.yml)

Assign functional roles to users:

```yaml
role_grants:
  # Human users
  - to_user: alice
    roles:
      - analyst

  - to_user: bob
    roles:
      - analyst
      - loader
      - transformer_dbt
      - securityadmin

  # Service accounts
  - to_user: svc_airbyte
    roles:
      - loader

  - to_user: svc_airflow
    roles:
      - loader
      - transformer_dbt
```

### Stage Roles (stages.yml)

Stages often need separate read and write roles:

```yaml
stages:
  - name: raw.dbt_artifacts.artifacts
    type: internal
    owner: transformer_dbt
    directory:
      enable: true
    comment: Used to store dbt artifacts

roles:
  - name: z_stage__raw__dbt_artifacts__artifacts__read
  - name: z_stage__raw__dbt_artifacts__artifacts__write

grants:
  - priv: "READ"
    on: "stage raw.dbt_artifacts.artifacts"
    to: z_stage__raw__dbt_artifacts__artifacts__read

  - priv:
      - READ
      - WRITE
    on: "stage raw.dbt_artifacts.artifacts"
    to: z_stage__raw__dbt_artifacts__artifacts__write
```

## Inherited Grants

An [inherited grant](https://docs.snowflake.com/en/user-guide/inherited-grants-intro) is a
single grant on a container — an account, database, or schema — that applies to every
current **and future** object of a type inside it. One inherited grant replaces the
`ALL` + `FUTURE` pair this pattern would otherwise need:

```yaml
grants:
  # Instead of "all tables in ..." plus "future tables in ..."
  - priv: SELECT
    on: INHERITED TABLES IN DATABASE sales_db
    to: z_tables_views__r

  # Multiple privileges expand to one statement each
  - priv: [SELECT, INSERT, UPDATE, DELETE]
    on: INHERITED TABLES IN SCHEMA sales_db.us_west
    to: z_tables__rw

  # The account can only be the container of an inherited grant
  - priv: SELECT
    on: INHERITED TABLES IN ACCOUNT
    to: z_scanner

  # Or turn an existing grant on all objects into an inherited one
  - priv: SELECT
    on: "all tables in database sales_db"
    inherited: true
    to: z_tables_views__r
```

### Why it matters for this pattern

| | `ALL` + `FUTURE` | `INHERITED` |
|---|---|---|
| Covers objects created later | Only via the `FUTURE` half | Yes |
| Shadowed by a schema-level grant | Yes, silently | No |
| Compared against Snowflake on each run | No — `ALL` grants are reapplied every time | Yes |
| Grant records created | One per object, plus one future grant | One |

Because Snowflake reports an inherited grant back as a single durable record, `snowcap
plan` can compare it against your config. Grants on all objects cannot be compared, so
they are reapplied on every run.

### Requirements

Inherited grants are a Snowflake preview feature, opted into with an account parameter.
Snowcap manages that parameter like any other — declare it alongside the rest:

```yaml
# account.yml
account_parameters:
  - name: FEATURE_RBAC_INHERITED_GRANTS
    value: ENABLED
```

Snowcap applies the parameter before any inherited grant that depends on it, so a single
`snowcap apply` can enable the preview and create the grants in one run. `ALTER ACCOUNT`
requires `ACCOUNTADMIN`, which is the role Snowcap already uses for account parameters.
The equivalent SQL, if you would rather set it outside of Snowcap:

```sql
ALTER ACCOUNT SET FEATURE_RBAC_INHERITED_GRANTS = 'ENABLED';
```

Either way, `snowcap plan` fails with a clear message if your config declares inherited
grants and neither the account nor the config has opted in.

!!! note "If preview features are turned off account-wide"

    Preview access gates every preview feature at once and is
    [enabled by default for most accounts](https://docs.snowflake.com/en/release-notes/preview-features),
    so usually there is nothing to do. If it has been disabled, the parameter above will
    not take effect until an account admin re-enables it:

    ```sql
    SELECT SYSTEM$GET_PREVIEW_ACCESS_STATUS();  -- check
    SELECT SYSTEM$ENABLE_PREVIEW_ACCESS();      -- enable
    ```

    These are system function calls rather than resources, so Snowcap cannot manage them
    declaratively. It does detect the situation: if preview access is off, `snowcap plan`
    says so and points at the function to call, rather than suggesting the parameter that
    would not help.

Creating one requires `MANAGE GRANTS` on the container, not just ownership of it. By
default Snowcap issues grants as `SECURITYADMIN`. To delegate to a database or schema
admin instead, name that role as the grant's owner:

```yaml
grants:
  - priv: SELECT
    on: INHERITED TABLES IN DATABASE sales_db
    to: analyst
    owner: sales_db_admin   # holds MANAGE GRANTS ON DATABASE sales_db
```

### Migrating from ALL + FUTURE

Snowflake recommends adding the inherited grant first and revoking the originals only once
you have confirmed access is intact. A grant pair is safe to collapse when the privilege
and the grantee are the same on both halves, and no object in the container needs to be
excluded. If some objects need different access, keep the granular grants, or use
[masking policies](masking-policies.md) and [row access policies](row-access-policies.md)
for the exceptions.

Snowcap will not revoke per-object grants that a declared inherited grant covers, so you
can add the inherited grant and remove the old declarations in either order without an
access gap.

### Limitations

Snowflake does not allow inherited grants to be combined with `WITH GRANT OPTION`, to
carry `OWNERSHIP`, to target shares or integrations, or to be granted on shared databases.
Snowcap rejects these at plan time. `priv: ALL` is also rejected — list the privileges
explicitly.

Note that Snowflake's Information Schema does not currently account for inherited grants
when deciding whether an object is visible to a role, so an object a role can only reach
through one will not appear in `INFORMATION_SCHEMA` results.

## Running Snowcap

### Environment Setup

Create a `.env` file with your Snowflake credentials. This example uses [key-pair authentication](getting-started.md#quick-start-create-a-warehouse):

```bash
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_ROLE=SECURITYADMIN
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
```

See [Getting Started](getting-started.md) for all authentication options.

### Plan Script (plan.sh)

```bash
#!/bin/bash
if [ -f .env ]; then
    export $(cat .env | xargs)
else
    echo "File .env does not exist."
    exit 1
fi

snowcap plan \
    --config resources/ \
    --sync_resources role,grant,role_grant
```

!!! warning "About `--sync_resources`"
    By default, Snowcap only creates or updates resources—it never deletes anything.

    The `--sync_resources` flag enables **sync mode** for the specified resource types. This means resources of those types that exist in Snowflake but are **not** in your config will be **deleted**.

    In this example, `role,grant,role_grant` are synced, so any roles or grants in Snowflake that aren't defined in your config files will be removed. Use with caution.

### Apply Script (apply.sh)

```bash
#!/bin/bash
if [ -f .env ]; then
    export $(cat .env | xargs)
else
    echo "File .env does not exist."
    exit 1
fi

snowcap apply \
    --config resources/ \
    --sync_resources role,grant,role_grant
```

## Benefits of This Pattern

1. **Fine-grained control** - Each object has its own role, making it easy to grant or revoke access to specific resources.

2. **Composability** - Base roles combine object roles into logical groupings that can be reused across functional roles.

3. **Visibility** - The `z_` prefix keeps object roles organized and separate from user-facing functional roles.

4. **Maintainability** - Adding a new database, schema, or warehouse automatically creates the corresponding role and grant through templates.

5. **Auditability** - The role hierarchy clearly shows who has access to what resources.

6. **Separation of concerns** - Object roles handle "what can be accessed", functional roles handle "who can access it".

## Role Type Reference

| Role Type | What it grants | Example privileges |
|-----------|----------------|-------------------|
| Database | Visibility of database existence | USAGE |
| Schema | Visibility of schema existence | USAGE |
| Warehouse | Access to compute resources | USAGE, MONITOR |
| Stage (read) | Read from stage | READ |
| Stage (write) | Write to stage | READ, WRITE |
| Tables/Views | Query data | SELECT |
| Account | Account-level operations | CREATE DATABASE |
| Base/Composite | Combination of other roles | (via role_grants) |
| Functional | End-user grouping | (via role_grants) |

## Design Decisions

This section explains the reasoning behind the patterns recommended in this guide.

### Account-Level Roles vs Database Roles

Snowflake offers two types of roles:

| Type | Scope | Can Grant to Users | Included in Clones |
|------|-------|-------------------|-------------------|
| Account-level roles | Global across account | Yes | No |
| Database roles | Single database only | No (must grant to account role) | Yes |

**We recommend account-level roles** for most use cases because:

1. **Unified management** - All roles defined in one place, version-controlled in your Snowcap config
2. **Cross-database access** - One role can grant access to multiple databases (e.g., `z_tables_views__select` across all databases)
3. **Direct user assignment** - Roles can be granted directly to users without an extra layer
4. **Simpler hierarchy** - One inheritance tree to reason about

**Database roles are useful when:**

- **Data sharing** - Database roles can be included in shares to external accounts; account roles cannot
- **Database owner autonomy** - When a database owner needs to manage access independently

Snowcap supports both. See [DatabaseRole](resources/database_role.md) for database role configuration.

### Why Not Grant Custom Roles to SYSADMIN?

Snowflake's documentation suggests granting all custom roles to SYSADMIN so administrators can access all objects. We don't recommend this approach because:

1. **Violates least privilege** - SYSADMIN gains access to everything, even sensitive data it doesn't need
2. **Blurs responsibility** - SYSADMIN is meant for creating and managing objects, not accessing business data
3. **Complicates auditing** - When SYSADMIN can access everything, it's harder to track who accessed what and why
4. **PII/compliance concerns** - Regulatory requirements often mandate restricted access to sensitive data; granting SYSADMIN blanket access can violate these requirements

Instead, we recommend:

- Keep SYSADMIN focused on infrastructure (creating databases, warehouses, schemas)
- Use functional roles for data access, granted only to users who need it
- Grant SECURITYADMIN or a dedicated security role the ability to manage grants
- If admins need data access, grant them the appropriate functional role explicitly

### Managed Access Schemas

By default, object owners can grant privileges on objects they create. This can lead to ad-hoc grants that bypass your centralized RBAC.

**Managed access schemas** restrict grant authority to the schema owner (or roles with MANAGE GRANTS):

```yaml
schemas:
  - for_each: var.schemas
    name: "{{ each.value.name.split('.')[1] }}"
    database: "{{ each.value.name.split('.')[0] }}"
    owner: "{{ each.value.get('owner', parent.owner) }}"
    managed_access: true
```

With `managed_access: true`, even if an analyst creates a view, they cannot grant SELECT on it—only the schema owner can. This ensures all access flows through your defined role hierarchy.

### Cloned Databases (QA, blue-green, PR environments)

Cloning a database does two different things to grants:

| What | Happens to grants |
|------|-------------------|
| The database itself | **Not** copied — the clone starts with no grants on it |
| Schemas, tables and other child objects | **Copied** — each keeps the grants its source had |

So after `CREATE DATABASE BALBOA_QA CLONE BALBOA`, every `z_schema__<name>` role
already holds USAGE on the clone's copy of its schema, without anyone writing
that down. Only the database-level grant is missing, which is why a clone is
normally followed by a re-grant of `USAGE ON DATABASE` to `z_db__<name>`.

That is the behaviour you want — a role named for a schema keeps its meaning in
every copy of that schema — but Snowcap does not know about it. With
`--sync_resources grant`, those copied grants are remote state that no config
declares, so a plan proposes dropping them. Applying that leaves roles with
usage on the clone's database and no access to anything inside it.

Declare them with a filtered loop over the schema list you already keep:

```yaml
grants:
  - for_each: var.schemas
    where: "each.value.name.split('.')[0] == 'BALBOA'"
    priv: USAGE
    on: "schema BALBOA_QA.{{ each.value.name.split('.')[1] }}"
    to: "z_schema__{{ each.value.name.split('.')[1] }}"
```

The `where` keeps the block off schemas in databases that have no clone. Adding
a schema to the source layer covers its clone automatically, so the two cannot
drift apart. The clone's schemas themselves stay undeclared — the clone creates
them, and Snowcap only needs to describe the access.

**Do not reach for `all schemas in database` here.** It looks like less
configuration, but it grants every role that holds it the entire clone. If
roles are scoped by layer — an analyst role seeing L1 through L3 and a reporter
role seeing only L3 — a database-wide grant silently flattens that distinction
in the clone while leaving it intact in the source, which is the kind of gap
that survives review precisely because the source still looks correct.

## See Also

- [Grant](resources/grant.md)
- [Role](resources/role.md)
- [RoleGrant](resources/role_grant.md)
- [DatabaseRole](resources/database_role.md)
- [Blueprint](blueprint.md)
