# YAML Configuration

Snowcap uses YAML files to define your Snowflake resources. This page covers the templating features that make YAML configurations powerful and reusable.

## Variables (vars)

Variables let you parameterize your configuration, making it reusable across environments or dynamic based on input.

### Basic Usage

Use double curly braces to reference variables:

```yaml
# snowcap.yml
databases:
  - name: "db_{{ var.environment }}"
```

Pass values via CLI:

```sh
snowcap plan --config snowcap.yml --vars '{"environment": "prod"}'
```

Or via environment variables (must start with `SNOWCAP_VAR_` and be uppercase):

```sh
export SNOWCAP_VAR_ENVIRONMENT="prod"
snowcap plan --config snowcap.yml
```

### Defining Variables with Defaults

Use the top-level `vars:` key to define expected variables with types and optional defaults:

```yaml
vars:
  - name: environment
    type: string

  - name: warehouse_size
    type: string
    default: XSMALL

databases:
  - name: "analytics_{{ var.environment }}"

warehouses:
  - name: "wh_{{ var.environment }}"
    warehouse_size: "{{ var.warehouse_size }}"
```

### Variable Types

| Type | Description |
|------|-------------|
| `string` | Text value |
| `list` | Array of values (used with `for_each`) |
| `int` | Integer number |
| `bool` | Boolean (true/false) |

## Loops (for_each)

The `for_each` directive creates multiple resources from a list. This is the key to DRY (Don't Repeat Yourself) configurations.

### Basic Example

```yaml
vars:
  - name: databases
    type: list
    default:
      - name: raw
        owner: loader
      - name: analytics
        owner: transformer

databases:
  - for_each: var.databases
    name: "{{ each.value.name }}"
    owner: "{{ each.value.owner }}"
```

This creates two databases: `raw` (owned by `loader`) and `analytics` (owned by `transformer`).

### Loop Variables

Inside a `for_each` block, you have access to:

| Variable | Description |
|----------|-------------|
| `each.value` | The current item in the list |
| `each.value.<field>` | Access a field of the current item |
| `each.index` | The index of the current item (0-based) |

### Creating Roles and Grants

A common pattern is creating a role and grant for each resource:

```yaml
vars:
  - name: databases
    type: list
    default:
      - name: raw
      - name: analytics

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

### Multiple Grants Per Iteration

Use a list for the `on` parameter to create multiple grants:

```yaml
grants:
  - for_each: var.databases
    priv: SELECT
    on:
      - "all tables in database {{ each.value.name }}"
      - "all views in database {{ each.value.name }}"
      - "future tables in database {{ each.value.name }}"
      - "future views in database {{ each.value.name }}"
    to: z_tables_views__select
```

### String Manipulation

Use Jinja filters and Python methods for string operations:

```yaml
vars:
  - name: schemas
    type: list
    default:
      - name: RAW.FINANCE
      - name: RAW.MARKETING
      - name: ANALYTICS.REPORTS

schemas:
  - for_each: var.schemas
    name: "{{ each.value.name.split('.')[1] }}"
    database: "{{ each.value.name.split('.')[0] }}"
```

### Default Values

Use Jinja's `default` filter for optional fields:

```yaml
warehouses:
  - for_each: var.warehouses
    name: "{{ each.value.name }}"
    warehouse_size: "{{ each.value.size }}"
    auto_suspend: "{{ each.value.auto_suspend | default(60) }}"
```

Or use Python's `.get()` method:

```yaml
schemas:
  - for_each: var.schemas
    owner: "{{ each.value.get('owner', 'SYSADMIN') }}"
```

### Parent Attributes

Access parent resource attributes with `parent`:

```yaml
schemas:
  - for_each: var.schemas
    name: "{{ each.value.name.split('.')[1] }}"
    database: "{{ each.value.name.split('.')[0] }}"
    owner: "{{ each.value.get('owner', parent.owner) }}"
```

## Scope

!!! warning "Experimental"
    The scope feature is experimental and may change in future versions.

### What Scope Does

By default, Snowcap can manage any resource in your Snowflake account. The `scope` parameter **restricts** Snowcap to only manage resources within a specific database or schema.

**Why use scope?**

| Benefit | Description |
|---------|-------------|
| **Safety** | Prevents accidentally modifying resources outside your intended area |
| **Reusability** | Same config can be applied to different databases/schemas |
| **Team separation** | Different teams manage their own schemas without affecting others |
| **Focused configs** | Smaller, focused configs instead of one giant account-wide config |

### How It Works

When you set a scope:

1. **Resources outside the scope cause an error** - If you try to create an account-level resource (like a warehouse) in a schema-scoped config, Snowcap will reject it
2. **Resources are automatically placed in the scope** - Tables, views, etc. without explicit database/schema are assigned to the configured scope
3. **Sync mode only affects resources in scope** - Using `--sync_resources` won't delete resources outside your scope

### Database Scope

Limits Snowcap to resources within a single database:

```yaml
scope: DATABASE
database: RAW

# These are allowed (database-level or below)
schemas:
  - name: FINANCE
  - name: LEGAL

tables:
  - name: products
    schema: FINANCE
    columns:
      - name: product
        data_type: string

# This would ERROR - warehouses are account-level, not database-level
# warehouses:
#   - name: my_warehouse
```

### Schema Scope

Limits Snowcap to resources within a single schema:

```yaml
scope: SCHEMA
database: DEV
schema: SALESFORCE

# These are allowed (schema-level)
tables:
  - name: products
    columns:
      - name: product
        data_type: string

tags:
  - name: cost_center
    allowed_values: ["finance", "engineering"]

# This would ERROR - schemas are database-level, not schema-level
# schemas:
#   - name: another_schema
```

### Reusable Configs with CLI Overrides

The real power of scope is **reusability**. Define a config once and apply it to different targets:

```yaml
# dev_schema.yml
scope: SCHEMA
database: DEV

tables:
  - name: staging_orders
    columns:
      - name: id
        data_type: int
      - name: data
        data_type: variant

views:
  - name: v_orders
    as_: "SELECT * FROM staging_orders"
```

Apply to different engineer schemas:

```sh
snowcap apply --config dev_schema.yml --schema=SCH_ALICE
snowcap apply --config dev_schema.yml --schema=SCH_BOB
snowcap apply --config dev_schema.yml --schema=SCH_CAROL
```

Each engineer gets identical tables and views in their own schema.

### Combining Scope with Variables

Use variables to apply the same schema structure to different environments:

```yaml
# finance.yml
scope: SCHEMA
database: "ANALYTICS_{{ var.env }}"
schema: FINANCE

tables:
  - name: revenue
    columns:
      - name: date
        data_type: date
      - name: amount
        data_type: number
```

```sh
# Deploy to staging
snowcap apply --config finance.yml --vars '{"env": "STAGE"}'

# Deploy to production
snowcap apply --config finance.yml --vars '{"env": "PROD"}'
```

This creates `ANALYTICS_STAGE.FINANCE.revenue` and `ANALYTICS_PROD.FINANCE.revenue`.

## See Also

- [Getting Started](getting-started.md) - Basic setup and first config
- [Role-Based Access Control](role-based-access-control.md) - Real-world YAML patterns
- [Blueprint](blueprint.md) - Python API reference
