# Command Line Interface

Snowcap provides a CLI for managing Snowflake resources.

## Installation

```bash
pip install snowcap
```

## Commands

### plan

Compare resource config to the current state of Snowflake and show what changes would be made.

```bash
snowcap plan --config <path>
```

| Option | Description |
|--------|-------------|
| `--config <path>` | Path to resource YAML files or directory |
| `--out <file>` | Save plan to JSON file (for later use with `apply --plan`) |
| `--json` | Output JSON to stdout instead of human-readable text |
| `--vars <json>` | Dynamic values as a JSON dictionary |
| `--sync_resources <types>` | Sync these resource types (config becomes source of truth; resources in Snowflake but not in config will be deleted) |
| `--exclude <types>` | Exclude resource types from the plan |
| `--database <name>` | Limit scope to this database |
| `--schema <name>` | Limit scope to this schema (requires --database) |
| `--use-account-usage` | Use ACCOUNT_USAGE views (faster for 50+ roles) |
| `--debug` | Enable debug mode |

**Examples:**

```bash
# Basic plan
snowcap plan --config resources/

# Plan with sync mode for roles
snowcap plan --config resources/ --sync_resources role,grant,role_grant

# Exclude enterprise features for standard accounts
snowcap plan --config resources/ --exclude masking_policy,row_access_policy

# Output as JSON
snowcap plan --config resources/ --json --out plan.json
```

### apply

Apply a resource config to a Snowflake account.

```bash
snowcap apply --config <path>
```

| Option | Description |
|--------|-------------|
| `--config <path>` | Path to resource YAML files or directory |
| `--plan <file>` | Path to a saved plan JSON file (from `snowcap plan --out`) |
| `--vars <json>` | Dynamic values as a JSON dictionary |
| `--sync_resources <types>` | Sync these resource types (config becomes source of truth; resources in Snowflake but not in config will be deleted) |
| `--exclude <types>` | Exclude resource types from the plan |
| `--database <name>` | Limit scope to this database |
| `--schema <name>` | Limit scope to this schema (requires --database) |
| `--dry-run` | Preview changes without applying |
| `--use-account-usage` | Use ACCOUNT_USAGE views (faster for 50+ roles) |
| `--debug` | Enable debug mode |

**Examples:**

```bash
# Apply from config
snowcap apply --config resources/

# Apply from saved plan
snowcap apply --plan plan.json

# Dry run (preview only)
snowcap apply --config resources/ --dry-run
```

### export

Generate a resource config from existing Snowflake resources.

```bash
snowcap export --resource <types>
snowcap export --all
```

| Option | Description |
|--------|-------------|
| `--resource <types>` | Resource types to export (comma-separated) |
| `--all` | Export all resources |
| `--exclude <types>` | Exclude resource types (used with --all) |
| `--out <file>` | Write exported config to a file |
| `--format [json\|yml]` | Output format (default: yml) |

**Examples:**

```bash
# Export databases
snowcap export --resource database --out databases.yml

# Export all resources
snowcap export --all --out snowcap.yml

# Export all except users and roles
snowcap export --all --exclude user,role --out snowcap.yml
```

### connect

Test the connection to Snowflake.

```bash
snowcap connect
```

Displays the connection parameters and verifies connectivity.

### generate

Generate helper files for integrations.

#### generate dbt-macros

Generate dbt macros for applying Snowflake governance policies.

```bash
snowcap generate dbt-macros
```

| Option | Description |
|--------|-------------|
| `--dbt-path <path>` | Path to dbt project (auto-detects from `DATACOVES__DBT_HOME` or `DBT_HOME`) |
| `--macros-path <path>` | Macros directory name (default: `macros`) |
| `--tag-database <name>` | Database where tags are defined |
| `--tag-schema <name>` | Schema where tags are defined |
| `--policy-database <name>` | Database where row access policies are defined |
| `--policy-schema <name>` | Schema where row access policies are defined |

**Example:**

```bash
snowcap generate dbt-macros \
  --dbt-path ./transform \
  --tag-database GOVERNANCE \
  --tag-schema TAGS \
  --policy-database GOVERNANCE \
  --policy-schema POLICIES
```

## CI/CD Workflow

For production deployments, use a two-step process: generate a plan, review it, then apply the exact plan that was reviewed.

### 1. Generate and save the plan

```bash
snowcap plan --config resources/ --out plan.json
```

This saves the plan to `plan.json` for review.

### 2. Review the plan

Options for reviewing the plan:

- **Download the artifact** from GitHub Actions and inspect the JSON locally
- **Print to workflow logs** by adding `cat plan.json` or using `--json` without `--out` to display the plan inline
- **Post as PR comment** using `gh pr comment` to make changes visible in the pull request

### 3. Apply the saved plan

```bash
snowcap apply --plan plan.json
```

This applies the exact changes from the saved plan—no re-computation, no surprises.

### Example GitHub Actions workflow

```yaml
jobs:
  plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install snowcap
      - run: snowcap plan --config resources/ --out plan.json
      - uses: actions/upload-artifact@v4
        with:
          name: plan
          path: plan.json

  apply:
    needs: plan
    runs-on: ubuntu-latest
    environment: production  # Requires approval
    steps:
      - uses: actions/download-artifact@v4
        with:
          name: plan
      - run: pip install snowcap
      - run: snowcap apply --plan plan.json
```

**Note:** The `environment: production` line pauses the job for approval, but only if you configure the environment in GitHub first. Go to **Settings → Environments → New environment**, create `production`, and enable **Required reviewers**. See [GitHub's environment documentation](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment) for details.

## Resource Type Labels

The `--sync_resources`, `--exclude`, and `--resource` options accept resource type labels in snake_case format.

**Common resource types:**

| Label | Resource |
|-------|----------|
| `database` | Database |
| `schema` | Schema |
| `role` | Role |
| `grant` | Grant |
| `role_grant` | Role Grant |
| `user` | User |
| `warehouse` | Warehouse |
| `table` | Table |
| `view` | View |
| `masking_policy` | Masking Policy |
| `row_access_policy` | Row Access Policy |
| `tag` | Tag |

Each resource documentation page shows its CLI label. See [Resources](resources/database.md) for the full list.

## Environment Variables

Snowcap uses standard Snowflake environment variables for connection:

| Variable | Description |
|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier |
| `SNOWFLAKE_USER` | Username |
| `SNOWFLAKE_PASSWORD` | Password |
| `SNOWFLAKE_ROLE` | Role to use |
| `SNOWFLAKE_WAREHOUSE` | Warehouse to use |
| `SNOWFLAKE_DATABASE` | Default database |

For dbt integration:

| Variable | Description |
|----------|-------------|
| `DATACOVES__DBT_HOME` | Path to dbt project |
| `DBT_HOME` | Alternative path to dbt project |

## Wrapper Scripts

For production use, a wrapper script can validate environment variables before running snowcap. This catches configuration errors early with clear messages instead of cryptic Snowflake connection failures.

**snowcap-apply.sh:**

```bash
#!/bin/bash

# Load .env if it exists (also works if vars are already in environment)
if [ -f .env ]; then
    set -a
    . ./.env
    set +a
fi

# Validate required variables
missing=()
[ -z "$SNOWFLAKE_ACCOUNT" ] && missing+=("SNOWFLAKE_ACCOUNT")
[ -z "$SNOWFLAKE_USER" ] && missing+=("SNOWFLAKE_USER")
[ -z "$SNOWFLAKE_ROLE" ] && missing+=("SNOWFLAKE_ROLE")

if [ ${#missing[@]} -gt 0 ]; then
    echo "Error: Missing required environment variables:"
    for var in "${missing[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Set these in your environment or create a .env file."
    echo "See: https://snowcap.datacoves.com/cli/#environment-variables"
    exit 1
fi

snowcap apply --config resources/ "$@"
```

**Usage:**

```bash
chmod +x snowcap-apply.sh
./snowcap-apply.sh                    # Apply changes
./snowcap-apply.sh --dry-run          # Preview only
```

Create a similar `snowcap-plan.sh` for planning. The `"$@"` passes any additional arguments to snowcap.

## See Also

- [Getting Started](getting-started.md)
- [YAML Configuration](yaml-configuration.md)
