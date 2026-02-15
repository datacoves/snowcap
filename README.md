# `snowcap` - Snowflake infrastructure as code

[![PyPI](https://img.shields.io/pypi/v/snowcap)](https://pypi.org/project/snowcap/)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Brought to you by Datacoves

<a href="https://datacoves.com">
  <img alt="Datacoves" src="https://raw.githubusercontent.com/datacoves/snowcap/main/images/datacoves-light.png" width="150">
</a>

Snowcap helps you provision, deploy, and secure resources in Snowflake. Datacoves takes it further: a managed DataOps platform for dbt and Airflow where governance and best practices are built into every layer.

- **Private cloud or SaaS** – your data, your choice
- **Managed dbt + Airflow** – production-ready from day one
- **In-browser VS Code** – onboard developers in minutes
- **Bring your own tools** – integrates with your existing stack, no lock-in
- **AI-assisted development** – connect your organization's approved LLM (Anthropic, OpenAI, Azure, Gemini, and more)
- **Built-in governance** – CI/CD, guardrails, and best practices included

Snowcap is the power tools. Datacoves is the workshop.

[Explore the platform →](https://datacoves.com)

---

## Why Snowcap?

Snowcap replaces Terraform, Schemachange, or Permifrost with a single, purpose-built tool for Snowflake.

| Feature | Snowcap | Terraform | Permifrost |
|---------|---------|-----------|------------|
| Snowflake-native | Yes | No | Yes |
| State file | No | Yes | No |
| YAML + Python | Yes | HCL only | YAML only |
| Speed | 50-90% faster | Baseline | Medium |
| All resource types | Yes | Most | Roles/grants only |
| `for_each` templating | Yes | Yes | No |
| Export existing resources | Yes | Import only | No |

## Key Features

- **Declarative** — Generates the right SQL to make your config match your account
- **Comprehensive** — 60+ Snowflake resource types supported
- **Flexible** — Write configuration in YAML or Python
- **Fast** — 50-90% faster than Terraform
- **Migration-friendly** — Export existing resources with the CLI

## Quick Start

```sh
pip install snowcap
```

Create `snowcap.yml`:

```yaml
warehouses:
  - name: analytics
    warehouse_size: xsmall
    auto_suspend: 60
```

Run:

```sh
# Set credentials
export SNOWFLAKE_ACCOUNT=my-account
export SNOWFLAKE_USER=my-user
export SNOWFLAKE_PASSWORD=my-password
export SNOWFLAKE_ROLE=SYSADMIN

# Preview changes
snowcap plan --config snowcap.yml

# Apply changes
snowcap apply --config snowcap.yml
```

## Documentation

Full documentation, examples, and resource reference at **[datacoves.github.io/snowcap](https://datacoves.github.io/snowcap)**

- [Getting Started](https://datacoves.github.io/snowcap/getting-started/)
- [YAML Configuration](https://datacoves.github.io/snowcap/yaml-configuration/)
- [Role-Based Access Control](https://datacoves.github.io/snowcap/role-based-access-control/)
- [Resource Reference](https://datacoves.github.io/snowcap/resources/database/)

## Support

- [Documentation](https://datacoves.github.io/snowcap)
- [GitHub Issues](https://github.com/datacoves/snowcap/issues)
