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

## See Also

- [Grant](resources/grant.md)
- [Role](resources/role.md)
- [RoleGrant](resources/role_grant.md)
- [Blueprint](blueprint.md)
