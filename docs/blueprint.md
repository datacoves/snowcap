# Blueprint

A Blueprint is the core engine that compares your configuration to Snowflake and generates the SQL to make them match. It validates resources, generates execution plans, and applies changes.

## YAML vs Python

**Most users should use YAML with the CLI.** When you run `snowcap plan` or `snowcap apply`, a Blueprint is created automatically from your YAML files. This approach is:

- Declarative and easy to read
- Version-controlled in git
- No Python knowledge required

**The Python API is for advanced use cases:**

- Building custom tooling or automation
- Integrating Snowcap into other Python applications
- Dynamic resource generation from external data (APIs, databases)
- Complex logic that YAML templates can't express
- Testing and CI/CD pipelines

## Python API Example

```python
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

For a complete Python example, see [Python API](python-api.md).

## Blueprint Parameters

### run_mode

Defines how the blueprint interacts with the Snowflake account.

| Value | Description |
|-------|-------------|
| `create-or-update` | *(default)* Resources are created or updated, never deleted |
| `sync` | Snowflake is updated to match the blueprint exactly. **Will delete resources not in config.** Must be used with `allowlist`. |

### resources

List of resources to manage.

```python
resources=[
    Database('my_database'),
    Schema('my_schema', database='my_database'),
]
```

### allowlist

Limits which resource types the blueprint can manage. Required when using `sync` mode.

```python
allowlist=["database", "schema", "role"]
```

### dry_run

When `True`, `apply()` returns SQL commands without executing them.

```python
dry_run=True
```

### vars

A dictionary of variable values for templating.

```python
vars={
    "environment": "prod",
    "owner": "analytics_team",
}
```

### vars_spec

Defines expected variables with types and optional defaults.

```python
vars_spec=[
    {"name": "environment", "type": "string"},
    {"name": "size", "type": "string", "default": "XSMALL"},
]
```

### scope, database, schema

Limits Snowcap to managing resources within a specific database or schema.

```python
scope="DATABASE",
database="RAW",
```

### use_account_usage

Controls whether Snowcap uses `SNOWFLAKE.ACCOUNT_USAGE` views for fetching grants. Defaults to `False`.

```python
use_account_usage=False  # default
```

When enabled, Snowcap fetches all grants with a single bulk query to `ACCOUNT_USAGE.GRANTS_TO_ROLES` instead of per-role `SHOW GRANTS` commands. This can significantly reduce the number of queries for accounts with many roles.

**When to enable:**
- Your manifest manages **50+ roles** with grants
- You're seeing many `SHOW GRANTS TO ROLE` queries in the logs
- The bulk query time (typically 30-60 seconds) is less than the cumulative time of individual `SHOW GRANTS` commands

**When to keep disabled (default):**
- Smaller manifests with fewer roles
- You want faster apply times for simple configurations
- Your account has many grants but your manifest only references a few roles

Requires `IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE`. If unavailable, Snowcap falls back to `SHOW GRANTS` automatically.

See [Getting Started - Optimizing Grant Fetching](getting-started.md#optimizing-grant-fetching-with-account_usage) for setup instructions.

## Methods

### plan(session)

Compares your configuration to Snowflake and returns a list of changes needed.

```python
plan = bp.plan(session)
```

**Parameters:**

- `session` - Snowflake connection object

**Returns:**

- `list[ResourceChange]` - Changes needed to reach desired state

### apply(session, plan)

Executes SQL commands to apply the plan to Snowflake.

```python
results = bp.apply(session, plan)
```

**Parameters:**

- `session` - Snowflake connection object
- `plan` *(optional)* - List of changes. If not provided, generates a plan automatically.

**Returns:**

- `list[str]` - SQL commands that were executed

### add(resource)

Adds resources to the blueprint after initialization.

```python
bp.add(Database('another_db'))
bp.add(role1, role2, role3)
bp.add([schema1, schema2])
```

## Using Variables in Python

```python
from snowcap.blueprint import Blueprint
from snowcap.resources import Database
from snowcap import var

# Reference a variable
db = Database(name=var.db_name)

# Or use Jinja-style syntax in strings
db = Database(name="db_{{ var.environment }}")

# Pass values when creating the blueprint
bp = Blueprint(
    resources=[db],
    vars={"db_name": "analytics", "environment": "prod"},
)
```

## See Also

- [YAML Configuration](yaml-configuration.md) - Variables, loops, and scope in YAML
- [Python API](python-api.md) - Complete Python example
- [Working With Resources](working-with-resources.md) - Resource classes and relationships
