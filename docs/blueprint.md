# Blueprint

The Blueprint class validates and deploys a set of resources as a group. It provides a structured way to manage your Snowflake resources through two methods: `plan` and `apply`.

Blueprint provides options to customize how resources are deployed to Snowflake, including `run_mode`, `allowlist`, and `dry_run`.

In Python, you utilize the `Blueprint` class to create and manage blueprints. When using the CLI or GitHub Action with YAML configurations, a Blueprint is created automatically.

## Example

```Python
from snowcap.blueprint import Blueprint
from snowcap.resources import Database, Schema

bp = Blueprint(
    run_mode='create-or-update',
    resources=[
        Database('my_database'),
        Schema('my_schema', database='my_database'),
    ],
    allowlist=["database", "schema"],
    dry_run=False,
)
plan = bp.plan(session)
bp.apply(session, plan)
```

## Blueprint parameters
**run_mode** `str`
- Defines how the blueprint interacts with the Snowflake account
  - **create-or-update** (*default*): Resources are either created or updated, no resources are destroyed
  - **sync**:
    - `⚠️ WARNING` Sync mode will drop resources.
    - Snowcap will update Snowflake to match the blueprint exactly. Must be used with `allowlist`.

**resources** `list[Resource]`
- List of resources initialized in the blueprint.

**allowlist** `list[str]`
- Specifies the allowed resource types in the blueprint.

**dry_run** `bool`
- `apply()` will return a list of SQL commands that would be executed without applying them.

**vars** `dict`
- A key-value dictionary that specifies the names and values of vars.

**vars_spec** `list[dict]`
- A list of dictionaries defining the `name`, `type` and `default` (optional) of all expected vars.

**scope** `str`
- Limit Snowcap's scope to a single database or schema. Must be one of "DATABASE" or "SCHEMA". If not specified, Snowcap will manage any resource.

**database** `str`
- The name of a database to limit Snowcap's scope to. Must be used with `scope`.

**schema** `str`
- The name of a schema to limit Snowcap's scope to. Must be used with `scope` and `database`.

## Methods

### `plan(session)`

The plan method analyzes your Snowflake account to determine how it is different from your configuration. It identifies what resources need to be added, changed, or removed to achieve the desired state.

#### Parameters:
- **session** (`SnowflakeConnection`): The session object used to connect to Snowflake

#### Returns:

- `list[ResourceChange]`: The list of changes that need to be made to the Snowflake account


### `apply(session, [plan])`

The apply method executes the SQL commands required to update your Snowflake account according to the plan generated. Apply returns a list of SQL commands that were executed.

#### Parameters:
- **session** (`SnowflakeConnection`): The session object used to connect to Snowflake
- **plan** (`list[ResourceChange]`, *optional*): The list of changes to apply. If not provided, the plan is generated automatically.

#### Returns:

- `list[str]`: A list of SQL commands that were executed.


### `add(resource: Resource)`

Alternate uses:
- `add(resource_1, resource_2, ...)`
- `add([resource_1, resource_2, ...])`

The add method allows you to add a resource to the blueprint.

## Using vars

### Vars in YAML
In YAML, vars are specified with double curly braces.
```YAML
-- snowcap.yml
databases:
  - name: "db_{{ var.fruit }}"
```

In the CLI, use the `--vars` flag to pass values to Snowcap
```sh
# Specify values as a key-value JSON string
snowcap plan --config snowcap.yml \
  --vars '{"fruit": "banana"}'
```

Alternatively, use environment variables to pass values to Snowcap. Vars environment variables must start with `SNOWCAP_VAR_` and must be in uppercase.

```sh
export SNOWCAP_VAR_FRUIT="peach"
snowcap plan --config snowcap.yml
```

### Vars defaults in YAML

Use the top-level `vars:` key to define a list of expected vars. You must specify a `type`, you can optionally specify a `default`.

```YAML
vars:
  - name: color
    type: string

  - name: fruit
    type: string
    default: apple

databases:
  - name: "db_{{ var.color }}_{{ var.fruit }}"
```


### Vars in Python

```Python
from snowcap.blueprint import Blueprint
from snowcap.resources import Database

# In Python, a var can be specifed using Snowcap's var module
from snowcap import var
db1 = Database(name=var.db1_name)

# Alternatively, a var can be specified inside a string with double curly braces. This is Jinja-style template syntax, not an f-string.
db2 = Database(name="db_{{ var.db2_name }}")

# Use the vars parameter to pass values to Snowcap
Blueprint(
  resources=[db1, db2],
  vars={
    "db1_name": "pineapple",
    "db2_name": "durian",
  },
)
```

### Vars defaults in Python

```Python
from snowcap.blueprint import Blueprint
from snowcap.resources import Database

# Use the vars_spec parameter to define a list of expected vars. You must specify a `type`, you can optionally specify a `default`.
Blueprint(
  resources=[Database(name="db_{{ var.color }}_{{ var.fruit }}")],
  vars={"color": "blue"},
  vars_spec=[
    {
      "name": "color",
      "type": "string",
    },
    {
      "name": "fruit",
      "type": "string",
      "default": "apple",
    }

  ]
)
```

## Using for_each

The `for_each` directive allows you to create multiple resources from a list of values. This is useful for creating resources, roles, and grants programmatically.

### Basic for_each example

```yaml
vars:
  - name: databases
    type: list
    default:
      - name: raw
        owner: loader
      - name: analytics
        owner: transformer_dbt

databases:
  - for_each: var.databases
    name: "{{ each.value.name }}"
    owner: "{{ each.value.owner }}"
```

This creates two databases: `raw` (owned by `loader`) and `analytics` (owned by `transformer_dbt`).

### Accessing loop values

Inside a `for_each` block, you have access to:

- `each.value` - The current item in the list
- `each.value.<field>` - Access fields of the current item
- `each.index` - The index of the current item (0-based)

### Using for_each with grants

Create a role and grant for each database:

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

### Multiple grants per iteration

Use a list for the `on` parameter to create multiple grants per iteration:

```yaml
grants:
  - for_each: var.databases
    priv: "SELECT"
    on:
      - "all tables in database {{ each.value.name }}"
      - "all views in database {{ each.value.name }}"
      - "future tables in database {{ each.value.name }}"
      - "future views in database {{ each.value.name }}"
    to: z_tables_views__select
```

### Accessing nested values

For complex data structures, use dot notation or Jinja filters:

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

### Using default values

Use Jinja's `default` filter or `.get()` method for optional fields:

```yaml
warehouses:
  - for_each: var.warehouses
    name: "{{ each.value.name }}"
    warehouse_size: "{{ each.value.size }}"
    statement_timeout_in_seconds: "{{ each.value.statement_timeout_in_seconds | default(3600) }}"
```

Or with `.get()`:

```yaml
schemas:
  - for_each: var.schemas
    owner: "{{ each.value.get('owner', 'SYSADMIN') }}"
```

### Using parent attributes

When a resource has a parent (like a schema in a database), you can access the parent's attributes:

```yaml
schemas:
  - for_each: var.schemas
    name: "{{ each.value.name.split('.')[1] }}"
    database: "{{ each.value.name.split('.')[0] }}"
    owner: "{{ each.value.get('owner', parent.owner) }}"
```

## Using scope

`🔬 EXPERIMENTAL`

When the `scope` parameter is used, Snowcap will limit which resources are allowed and limit where those resources are located within Snowflake.

### Using database scope

```YAML
-- raw.yml
scope: DATABASE
database: RAW

schemas:
  - name: FINANCE
  - name: LEGAL
  - name: MARKETING

tables:
  - name: products
    schema: FINANCE
    columns:
      - name: product
        data_type: string
```


### Using schema scope
```YAML
-- salesforce.yml
scope: SCHEMA
database: DEV
schema: SALESFORCE

tables:
  - name: products
    columns:
      - name: product
        data_type: string

tags:
  - name: cost_center
    allowed_values: ["finance", "engineering"]
```

### Scope example: re-use the same schema setup for multiple engineers

```YAML
-- dev_schema.yml
scope: SCHEMA
database: DEV

tables: ...
views: ...
procedures: ...
```

```sh
snowcap apply --config dev_schema.yml --schema=SCH_TEEJ
snowcap apply --config dev_schema.yml --schema=SCH_ALLY
snowcap apply --config dev_schema.yml --schema=SCH_DAVE
```

### Scope example: combine scope with vars

```YAML
-- finance.yml
scope: SCHEMA
database: "ANALYTICS_{{ vars.env }}"
schema: FINANCE

tables: ...
views: ...
procedures: ...
```

```sh
snowcap apply --config finance.yml --vars='{"env": "stage"}'
snowcap apply --config finance.yml --vars='{"env": "prod"}'
```