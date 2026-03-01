# `snowcap` - Snowflake infrastructure as code

## Brought to you by Datacoves

<img src="images/datacoves-light.png" alt="Datacoves" width="150" class="light-mode-only">
<img src="images/datacoves-dark.png" alt="Datacoves" width="150" class="dark-mode-only">

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

Snowcap replaces tools like Terraform, Schemachange, or Permifrost.

Deploy any Snowflake resource, including users, roles, schemas, databases, integrations, pipes, stages, functions, stored procedures, and more. Convert adhoc, bug-prone SQL management scripts into simple, repeatable configuration.

## Snowcap is for

* DevOps engineers looking to automate and manage Snowflake infrastructure.
* Analytics engineers working with dbt who want to manage Snowflake resources without macros.
* Data platform teams who need to reliably manage Snowflake with CI/CD.
* Organizations that prefer a git-based workflow for infrastructure management.
* Teams seeking to replace Terraform for Snowflake-related tasks.


## Key Features

 * **Declarative** » Generates the right SQL to make your config and account match

 * **Comprehensive** » Nearly every Snowflake resource is supported

 * **Flexible** » Write resource configuration in YAML or Python

 * **Fast** » Snowcap runs 50-90% faster than Terraform and Permifrost

 * **Migration-friendly** » Generate config automatically with the export CLI

 * **LLM-friendly** » [llms.txt](https://llmstxt.org/) support for AI-assisted development

## Contents

* [Getting Started](getting-started.md) - Installation, authentication, and first config
* [Role-Based Access Control](role-based-access-control.md) - Best practices for managing permissions
* [YAML Configuration](yaml-configuration.md) - Variables, loops, and scope
* [GitHub Action](snowcap-github-action.md) - Automate deployments with CI/CD
* [Export Existing Resources](export.md) - Generate config from your current Snowflake setup

### Advanced Usage

* [Python API](python-api.md) - Programmatic control with Python
* [Blueprint](blueprint.md) - Python API reference and parameters
* [Working With Resources](working-with-resources.md) - Resource classes and relationships

