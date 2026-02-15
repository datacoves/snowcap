# Python API

For programmatic control, use the Python API directly instead of the CLI.

## Basic Example

```python
import os
import snowflake.connector

from snowcap.blueprint import Blueprint, print_plan
from snowcap.resources import Grant, Role, Warehouse

# Configure resources by instantiating Python objects
role = Role(name="transformer")

warehouse = Warehouse(
    name="transforming",
    warehouse_size="large",
    auto_suspend=60,
)

usage_grant = Grant(priv="usage", to=role, on=warehouse)

# Connect to Snowflake
connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": "SYSADMIN",
}
session = snowflake.connector.connect(**connection_params)

# Create a Blueprint and pass your resources into it
bp = Blueprint(resources=[
    role,
    warehouse,
    usage_grant,
])

# Generate a plan (like Terraform)
plan = bp.plan(session)
print_plan(plan)

# Apply changes to Snowflake
bp.apply(session, plan)
```

## Plan Output

The `print_plan()` function displays changes that will be made:

```
» snowcap
» Plan: 3 to add, 0 to change, 0 to destroy.

+ urn::ABCD123:warehouse/transforming {
  + name                = "transforming"
  + owner               = "SYSADMIN"
  + warehouse_type      = "STANDARD"
  + warehouse_size      = "LARGE"
  ...
}

+ urn::ABCD123:role/transformer {
  + name    = "transformer"
  + owner   = "USERADMIN"
}

+ urn::ABCD123:grant/TRANSFORMER?priv=USAGE&on=warehouse/TRANSFORMING {
  + priv    = "USAGE"
  + on      = "transforming"
  + to      = TRANSFORMER
}
```

## Apply Output

The `apply()` function executes the SQL commands:

```
[SNOWCAP_USER:SYSADMIN]  > USE SECONDARY ROLES ALL
[SNOWCAP_USER:SYSADMIN]  > CREATE WAREHOUSE TRANSFORMING warehouse_type = STANDARD ...
[SNOWCAP_USER:SYSADMIN]  > USE ROLE USERADMIN
[SNOWCAP_USER:USERADMIN] > CREATE ROLE TRANSFORMER
[SNOWCAP_USER:USERADMIN] > USE ROLE SYSADMIN
[SNOWCAP_USER:SYSADMIN]  > GRANT USAGE ON WAREHOUSE transforming TO TRANSFORMER
```

## Loading from Environment

Use [python-dotenv](https://pypi.org/project/python-dotenv/) to load credentials from a `.env` file:

```python
from dotenv import load_dotenv
load_dotenv()

# Now os.environ has values from .env
```

## Next Steps

- [Blueprint](blueprint.md) - Advanced deployment customization
- [Working With Resources](working-with-resources.md) - Resource configuration options
