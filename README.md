# `snowcap` - Snowflake infrastructure as code

[![PyPI](https://img.shields.io/pypi/v/snowcap)](https://pypi.org/project/snowcap/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

*formerly Titan Core*

## Table of Contents

- [Why Snowcap?](#why-snowcap)
- [Key Features](#key-features)
- [Real-World Pattern: Modular RBAC](#real-world-pattern-modular-rbac)
- [Getting Started](#getting-started)
- [Using the CLI](#using-the-cli)
- [Using the GitHub Action](#using-the-github-action)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Support](#support)

## Brought to you by Datacoves

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/datacoves-dark.png">
  <img alt="Datacoves" src="images/datacoves-light.png" width="150">
</picture>

Snowcap helps you provision, deploy, and secure resources in Snowflake. Datacoves takes it further: a managed DataOps platform for dbt and Airflow, deployable in your private cloud or available as SaaS.

- **Private cloud or SaaS** - your data, your choice
- **Managed dbt + Airflow** - production-ready from day one
- **In-browser VS Code** - onboard developers in minutes
- **Bring your own tools** - integrates with your existing stack, no lock-in
- **AI-assisted development** - connect your organization's approved LLM (Anthropic, OpenAI, Azure, Gemini, and more)
- **Built-in governance** - CI/CD, guardrails, and best practices included

Snowcap is the power tools. Datacoves is the workshop.

[Explore the platform →](https://datacoves.com)

---

## Why Snowcap?

Snowcap replaces tools like Terraform, Schemachange, or Permifrost with a single, purpose-built tool for Snowflake.

| Feature | Snowcap | Terraform | Permifrost |
|---------|---------|-----------|------------|
| Snowflake-native | Yes | No (generic) | Yes |
| State file | No | Yes | No |
| YAML + Python | Yes | HCL only | YAML only |
| Speed | 50-90% faster | Baseline | Medium |
| All resource types | Yes | Most | Roles/grants only |
| `for_each` templating | Yes | Yes | No |
| Export existing resources | Yes | Import only | No |

Deploy any Snowflake resource, including users, roles, schemas, databases, integrations, pipes, stages, functions, stored procedures, and more. Convert adhoc, bug-prone SQL management scripts into simple, repeatable configuration.

### Who is Snowcap for?

* **DevOps engineers** looking to automate and manage Snowflake infrastructure
* **Analytics engineers** working with dbt who want to manage Snowflake resources without macros
* **Data platform teams** who need to reliably manage Snowflake with CI/CD
* **Organizations** that prefer a git-based workflow for infrastructure management
* **Teams** seeking to replace Terraform for Snowflake-related tasks

```
    ╔══════════╗                                           ╔═══════════╗
    ║  CONFIG  ║                                           ║ SNOWFLAKE ║
    ╚══════════╝                                           ╚═══════════╝
  ┏━━━━━━━━━━━┓                                        ┏━━━━━━━━━━━┓
┌─┫ WAREHOUSE ┣─────┐                                ┌─┫ WAREHOUSE ┣───────────┐
│ ┗━━━━━━━━━━━┛     │                    ALTER       │ ┗━━━━━━━━━━━┛           │
│ name:         ETL │─────┐           ┌─ WAREHOUSE ─▶│ name:         ETL       │
│ auto_suspend: 60  │     │           │              │ auto_suspend: 300 -> 60 │
└───────────────────┘  ╔══▼═══════════╩═╗            └─────────────────────────┘
                       ║                ║
                       ║    SNOWCAP     ║
  ┏━━━━━━┓             ║                ║              ┏━━━━━━┓
┌─┫ ROLE ┣──────────┐  ╚══▲═══════════╦═╝            ┌─┫ ROLE ┣────────────────┐
│ ┗━━━━━━┛          │     │           │              │ ┗━━━━━━┛                │
│ name: TRANSFORMER │─────┘           └─ CREATE ────▶│ name: TRANSFORMER       │
└───────────────────┘                    ROLE        └─────────────────────────┘
```


## Key Features

 * **Declarative** — Generates the right SQL to make your config and account match
 * **Comprehensive** — Nearly every Snowflake resource is supported
 * **Flexible** — Write resource configuration in YAML or Python
 * **Fast** — Snowcap runs 50-90% faster than Terraform and Permifrost
 * **Migration-friendly** — Generate config automatically with the export CLI


## Real-World Pattern: Modular RBAC

Snowcap's `for_each` templating makes it easy to implement composable role architectures used in production environments.

### The Problem

Managing Snowflake permissions typically leads to:
- Scattered SQL scripts that drift from reality
- No audit trail for "who granted what to whom"
- Copy-paste errors when adding new resources
- Overly permissive roles because granular management is painful

### The Solution: Atomic Building Blocks

Create atomic roles that grant a single privilege on a single resource type, then compose them into functional roles.

```
┌─────────────────────────────────────────────────────────────┐
│                    FUNCTIONAL ROLES                         │
│            (analyst, loader, transformer_dbt)               │
│                  Users are assigned here                    │
└──────────────────────────┬──────────────────────────────────┘
                           │ inherits
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    ATOMIC ROLES                             │
│  z_db__<database>       → USAGE on database                 │
│  z_schema__<schema>     → USAGE on schema                   │
│  z_wh__<warehouse>      → USAGE + MONITOR on warehouse      │
│  z_stage__<stage>       → READ/WRITE on stage               │
│  z_tables_views__select → SELECT on tables/views            │
└─────────────────────────────────────────────────────────────┘
```

### Implementation with Snowcap

**Step 1: Define your databases**

```yaml
# databases.yml
vars:
  - name: databases
    type: list
    default:
      - name: raw
        owner: loader
      - name: analytics
        owner: transformer
      - name: reporting
        owner: transformer
```

**Step 2: Auto-generate atomic roles with templates**

```yaml
# object_templates/database.yml

# Databases
databases:
  - for_each: var.databases
    name: "{{ each.value.name }}"
    owner: "{{ each.value.owner }}"

# Database roles
roles:
  - for_each: var.databases
    name: "z_db__{{ each.value.name }}"

# Database grants
grants:
  - for_each: var.databases
    priv: USAGE
    on: "database {{ each.value.name }}"
    to: "z_db__{{ each.value.name }}"
```

**Step 3: Compose functional roles**

```yaml
# roles__functional.yml
roles:
  - name: analyst
  - name: loader
  - name: transformer

role_grants:
  - to_role: analyst
    roles:
      - z_db__analytics
      - z_db__reporting
      - z_schema__marts
      - z_schema__reports
      - z_wh__querying
      - z_tables_views__select

  - to_role: transformer
    roles:
      - z_db__raw
      - z_db__analytics
      - z_wh__transforming
```

### Why This Pattern Works

| Benefit | Description |
|---------|-------------|
| **Auditable** | Each atomic role does one thing—easy to audit and understand |
| **DRY** | Add a database to the list, templates auto-create the role + grants |
| **Least Privilege** | Users get functional roles, never atomic roles directly |
| **Composable** | Mix and match atomic roles to create new personas in minutes |
| **Git-native** | Full audit trail via version control |


## Getting Started

**Requirements:** Python 3.9 or higher

### Install from PyPI

```sh
# MacOS / Linux
python -m venv .venv
source .venv/bin/activate
pip install snowcap

# Windows
python -m venv .venv
.\.venv\Scripts\activate
pip install snowcap
```

### Quick Start: Create a Warehouse

The simplest way to get started—define a single resource and deploy it:

```yaml
# snowcap.yml
warehouses:
  - name: analytics
    warehouse_size: xsmall
    auto_suspend: 60
```

Create a `.env` file for credentials (add `.env` to `.gitignore`!):

```sh
# .env
SNOWFLAKE_ACCOUNT=my-account
SNOWFLAKE_USER=my-user
SNOWFLAKE_PASSWORD=my-password
SNOWFLAKE_ROLE=SYSADMIN
```

Run snowcap:

```sh
# Load environment variables
export $(cat .env | xargs)

# Preview changes
snowcap plan --config snowcap.yml

# Apply changes
snowcap apply --config snowcap.yml
```

That's it. Snowcap compares your config to Snowflake and generates the SQL to make them match.

### Scaling Up: Directory Structure with Templates

As your infrastructure grows, organize configs into directories and use templates for scalability:

```
snowcap/
├── resources/
│   ├── databases.yml            # Database definitions
│   ├── schemas.yml              # Schema definitions
│   ├── warehouses.yml           # Warehouse definitions
│   ├── stages.yml               # Stage definitions
│   ├── users.yml                # User definitions
│   ├── roles__base.yml          # Atomic privilege roles
│   └── roles__functional.yml    # Functional roles + grants
│
└── object_templates/            # Auto-generate resources with for_each
    ├── database.yml
    ├── schema.yml
    └── warehouses.yml
```

**databases.yml** - Define your databases:

```yaml
vars:
  - name: databases
    type: list
    default:
      - name: raw
        owner: loader
      - name: analytics
        owner: transformer
      - name: analytics_dev
        owner: transformer
```

**object_templates/database.yml** - Auto-generate databases, roles, and grants:

```yaml
# Databases
databases:
  - for_each: var.databases
    name: "{{ each.value.name }}"
    owner: "{{ each.value.owner }}"

# Database roles
roles:
  - for_each: var.databases
    name: "z_db__{{ each.value.name }}"

# Database grants
grants:
  - for_each: var.databases
    priv: USAGE
    on: "database {{ each.value.name }}"
    to: "z_db__{{ each.value.name }}"
```

**roles__functional.yml** - Compose into functional roles:

```yaml
roles:
  - name: analyst
  - name: loader
  - name: transformer

role_grants:
  - to_role: analyst
    roles:
      - z_db__analytics
      - z_schema__marts
      - z_wh__querying
      - z_tables_views__select

  - to_role: transformer
    roles:
      - z_db__raw
      - z_db__analytics
      - z_wh__transforming
```

**Run snowcap:**

```sh
# Load environment variables from .env
export $(cat .env | xargs)

# Preview all changes
snowcap plan --config ./snowcap/

# Apply all changes
snowcap apply --config ./snowcap/
```

Adding a new database? Just add one line to `databases.yml`—the template auto-creates the database, role, and grant.

### Export Existing Resources

Already have a Snowflake environment? Generate config from your existing setup:

```sh
snowcap export \
  --resource=warehouse,role,grant \
  --out=snowcap.yml
```

### Python Example

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


## Using the CLI

You can use the CLI to generate a plan, apply a plan, or export resources. To use the CLI, install the Python package and call `python -m snowcap` from the command line.

The CLI allows you to `plan` and `apply` a Snowcap YAML config. You can specify a single input file or a directory of configs.

### CLI Commands

```sh
snowcap --help

# Commands:
#   apply    Apply a resource config to a Snowflake account
#   connect  Test the connection to Snowflake
#   export   Generate a resource config for existing Snowflake resources
#   plan     Compare a resource config to the current state of Snowflake
```

### Environment Variables

To connect with Snowflake, the CLI uses environment variables:

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier |
| `SNOWFLAKE_USER` | Username |
| `SNOWFLAKE_PASSWORD` | Password |
| `SNOWFLAKE_DATABASE` | Default database (optional) |
| `SNOWFLAKE_SCHEMA` | Default schema (optional) |
| `SNOWFLAKE_ROLE` | Role to use |
| `SNOWFLAKE_WAREHOUSE` | Warehouse to use |
| `SNOWFLAKE_MFA_PASSCODE` | MFA passcode (if required) |
| `SNOWFLAKE_AUTHENTICATOR` | Authentication method |

For [key-pair auth](https://docs.snowflake.com/en/user-guide/key-pair-auth), use these instead of `SNOWFLAKE_PASSWORD`:
* `SNOWFLAKE_PRIVATE_KEY_PATH`
* `PRIVATE_KEY_PASSPHRASE` (if using encrypted key)

Set `SNOWFLAKE_AUTHENTICATOR` to `SNOWFLAKE_JWT` when using key-pair auth.


## Using the GitHub Action

Automate Snowflake deployments with GitHub Actions. Here's an example workflow:

```yaml
# .github/workflows/snowcap.yml
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
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install Snowcap
        run: pip install snowcap

      - name: Plan changes
        run: snowcap plan --config ./snowcap/
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}

      - name: Apply changes
        if: github.ref == 'refs/heads/main'
        run: snowcap apply --config ./snowcap/
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
```


## Documentation

Full documentation is available at [datacoves.github.io/snowcap](https://datacoves.github.io/snowcap).


## Open Source

This project is a fork of [Titan Core](https://github.com/Titan-Systems/titan), originally created by [Titan Systems](https://github.com/Titan-Systems). The original project appears to be unmaintained, so Datacoves has forked it to continue development, fix bugs, and add new features.

We are grateful to the Titan Systems team for creating this project and releasing it under an open source license.

This project is licensed under the Apache 2.0 License - see [LICENSE](LICENSE) for details.


## Contributing

We welcome contributions! Please see our [contributing guidelines](CONTRIBUTING.md) for details.


## Support

- [Documentation](https://datacoves.github.io/snowcap)
- [GitHub Issues](https://github.com/datacoves/snowcap/issues)
