# Getting Started

If you're new, the best place to start is with the Python package.

### Install from PyPi (MacOS, Linux)

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install snowcap
```

### Install from PyPi (Windows)

```bat
python -m venv .venv
.\.venv\Scripts\activate
python -m pip install snowcap
```

## Using the Python package

```Python
import os
import snowflake.connector

from snowcap.blueprint import Blueprint, print_plan
from snowcap.resources import Grant, Role, Warehouse

# Configure resources by instantiating Python objects.

role = Role(name="transformer")

warehouse = Warehouse(
    name="transforming",
    warehouse_size="large",
    auto_suspend=60,
)

usage_grant = Grant(priv="usage", to=role, on=warehouse)

# Snowcap compares your config to a Snowflake account. Create a Snowflake
# connection to allow Snowcap to connect to your account.

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": "SYSADMIN",
}
session = snowflake.connector.connect(**connection_params)

# Create a Blueprint and pass your resources into it. A Blueprint helps you
# validate and deploy a set of resources.

bp = Blueprint(resources=[
    role,
    warehouse,
    usage_grant,
])

# Blueprint works like Terraform. Calling plan(...) will compare your config
# to the state of your Snowflake account and return a list of changes.

plan = bp.plan(session)
print_plan(plan) # =>
"""
» snowcap
» Plan: 4 to add, 0 to change, 0 to destroy.

+ urn::ABCD123:warehouse/transforming {
  + name                                = "transforming"
  + owner                               = "SYSADMIN"
  + warehouse_type                      = "STANDARD"
  + warehouse_size                      = "LARGE"
  ...
}

+ urn::ABCD123:role/transformer {
  + name    = "transformer"
  + owner   = "USERADMIN"
  + tags    = None
  + comment = None
}

+ urn::ABCD123:grant/TRANSFORMER?priv=USAGE&on=warehouse/TRANSFORMING {
  + priv         = "USAGE"
  + on           = "transforming"
  + on_type      = "WAREHOUSE"
  + to           = TRANSFORMER
  ...
}
"""

# Calling apply(...) will convert your plan into the right set of SQL commands
# and run them against your Snowflake account.
bp.apply(session, plan) # =>
"""
[SNOWCAP_USER:SYSADMIN]  > USE SECONDARY ROLES ALL
[SNOWCAP_USER:SYSADMIN]  > CREATE WAREHOUSE TRANSFORMING warehouse_type = STANDARD ...
[SNOWCAP_USER:SYSADMIN]  > USE ROLE USERADMIN
[SNOWCAP_USER:USERADMIN] > CREATE ROLE TRANSFORMER
[SNOWCAP_USER:USERADMIN] > USE ROLE SYSADMIN
[SNOWCAP_USER:SYSADMIN]  > GRANT USAGE ON WAREHOUSE transforming TO TRANSFORMER
"""
```

For more advanced usage, see [Blueprint](blueprint.md).

## Using the CLI

You can use the CLI to generate a plan, apply a plan, or export resources. To use the CLI, install the Python package and call `snowcap` from the command line.

The CLI allows you to `plan` and `apply` a Snowcap YAML config. You can specify a single input file or a directory of configs.

In addition to `plan` and `apply`, the CLI also allows you to `export` resources. This makes it easy to generate a config for an existing Snowflake environment.

To connect with Snowflake, the CLI uses environment variables. These environment variables are supported:

* `SNOWFLAKE_ACCOUNT`
* `SNOWFLAKE_USER`
* `SNOWFLAKE_PASSWORD`
* `SNOWFLAKE_DATABASE`
* `SNOWFLAKE_SCHEMA`
* `SNOWFLAKE_ROLE`
* `SNOWFLAKE_WAREHOUSE`
* `SNOWFLAKE_MFA_PASSCODE`
* `SNOWFLAKE_AUTHENTICATOR`


### CLI Example

Show the help message

```sh
snowcap --help

# Usage: snowcap [OPTIONS] COMMAND [ARGS]...
#
#   snowcap helps you manage your Snowflake environment.
#
# Options:
#   --help  Show this message and exit.
#
# Commands:
#   apply    Apply a resource config to a Snowflake account
#   connect  Test the connection to Snowflake
#   export   Generate a resource config for existing Snowflake resources
#   plan     Compare a resource config to the current state of Snowflake
```

Apply a resource config to Snowflake

```sh
# Create a resource config file
cat <<EOF > snowcap.yml
roles:
  - name: transformer

warehouses:
  - name: transforming
    warehouse_size: LARGE
    auto_suspend: 60

grants:
  - to_role: transformer
    priv: usage
    on_warehouse: transforming
EOF

# Set connection variables
export SNOWFLAKE_ACCOUNT="my-account"
export SNOWFLAKE_USER="my-user"
export SNOWFLAKE_PASSWORD="my-password"

# Generate a plan
snowcap plan --config snowcap.yml

# Apply the config
snowcap apply --config snowcap.yml
```

Export existing Snowflake resources to YAML.

```sh
snowcap export \
  --resource=warehouse,grant,role \
  --out=snowcap.yml
```

The Snowcap Python package installs the CLI script `snowcap`. You can alternatively use Python CLI module syntax if you need fine-grained control over the Python environment.

```sh
python -m snowcap plan --config snowcap.yml
```

## Using the GitHub Action
The Snowcap GitHub Action allows you to automate the deployment of Snowflake resources using a git-based workflow.

### GitHub Action Example

```YAML
-- .github/workflows/snowcap.yml
name: Deploy to Snowflake with Snowcap
on:
  push:
    branches: [ main ]
    paths:
    - 'snowcap/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Deploy to Snowflake
        uses: datacoves/snowcap-action@main
        with:
          run-mode: 'create-or-update'
          resource-path: './snowcap'
          allowlist: 'warehouse,role,grant'
          dry-run: 'false'
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
```

For in-depth documentation, see [Snowcap GitHub Action](snowcap-github-action.md).