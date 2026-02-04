# `snowcap` GitHub Action

> **Note:** The Snowcap GitHub Action (`datacoves/snowcap-action`) will be available in a future release.

## Using the GitHub action

To add the Snowcap GitHub action to your repository, follow these steps:

### Create a Snowcap workflow file

Create a file in the GitHub workflows directory of your repo (`.github/workflows/snowcap.yml`)

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

### Configure your Snowflake connection

Go to your GitHub repository settings, navigate to `Secrets`. There, add a secret for `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and whatever other connection settings you need.


### Create a `snowcap` directory in your repository

Add YAML resource configs to the `snowcap` directory.

```YAML
# snowcap/warehouses.yml
warehouses:
  - name: reporting
    warehouse_size: XSMALL
    auto_suspend: 60
    auto_resume: true
```

```YAML
# snowcap/rbac.yml

roles:
  - name: reporter
    comment: "Has permissions on the analytics database..."

grants:
  - to_role: reporter
    priv: usage
    on_warehouse: reporting
  - to_role: reporter
    priv: usage
    on_database: analytics

role_grants:
  - role: reporter
    roles:
      - SYSADMIN
```

### Commit and push your changes

When you push to `main` changes to files in the `snowcap/` directory, the Github Action will deploy them to Snowflake.

## Configuration options

**run-mode** `string`

Defines how the blueprint interacts with the Snowflake account

- Default: `"create-or-update"`
- **create-or-update**
  - Resources are either created or updated, no resources are destroyed
- **sync**:
  - `⚠️ WARNING` Sync mode will drop resources.
  - Snowcap will update Snowflake to match the blueprint exactly. Must be used with `allowlist`.

**resource-path** `string`

Defines the file or directory where Snowcap will look for the resource configs

- Default: `"."`

**allowlist** `list[string] or "all"`

Defines which resource types are allowed

 - Default: `"all"`

**dry_run** `bool`

**vars** `dict`

**vars_spec** `list[dict]`

**scope** `str`

**database** `str`

**schema** `str`

## Ignore files with `.snowcapignore`

If you specify a directory as the `resource-path`, Snowcap will recursively look for all files with a `.yaml` or `.yml` file extension. You can tell Snowcap to exclude files or directories with a `.snowcapignore` file. This file uses [gitignore syntax](https://git-scm.com/docs/gitignore).

### `.snowcapignore` example

```
# .snowcapignore

# Ignore dbt config
dbt_project.yml
```
