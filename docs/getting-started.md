# Getting Started

**Requirements:** Python 3.10 or higher

## Install from PyPI

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

## Quick Start: Create a Warehouse

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

??? note "All Environment Variables"
    | Variable | Description |
    |----------|-------------|
    | `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier |
    | `SNOWFLAKE_USER` | Username |
    | `SNOWFLAKE_PASSWORD` | Password (for password auth) |
    | `SNOWFLAKE_ROLE` | Role to use |
    | `SNOWFLAKE_WAREHOUSE` | Warehouse to use (optional) |
    | `SNOWFLAKE_DATABASE` | Default database (optional) |
    | `SNOWFLAKE_SCHEMA` | Default schema (optional) |
    | `SNOWFLAKE_AUTHENTICATOR` | Authentication method (see below) |
    | `SNOWFLAKE_MFA_PASSCODE` | TOTP passcode from authenticator app |
    | `SNOWFLAKE_PRIVATE_KEY_PATH` | Path to private key file (for key-pair auth) |
    | `PRIVATE_KEY_PASSPHRASE` | Passphrase for encrypted private key |

    **Authenticator options:**

    | Value | Description |
    |-------|-------------|
    | *(default)* | Username and password |
    | `SNOWFLAKE_JWT` | Key-pair authentication |
    | `externalbrowser` | SSO via web browser |
    | `oauth` | OAuth with access token |
    | `username_password_mfa` | Password with MFA (push notification) |

??? example "Key-Pair Authentication"
    For [key-pair auth](https://docs.snowflake.com/en/user-guide/key-pair-auth), use `SNOWFLAKE_JWT` instead of password:

    ```sh
    # .env
    SNOWFLAKE_ACCOUNT=my-account
    SNOWFLAKE_USER=my-user
    SNOWFLAKE_ROLE=SECURITYADMIN
    SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private-key.pem
    SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
    ```

    If your private key is encrypted, also set:

    ```sh
    PRIVATE_KEY_PASSPHRASE=your-passphrase
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

## Scaling Up: Directory Structure with Templates

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

## CLI Commands

```sh
snowcap --help

# Commands:
#   apply    Apply a resource config to a Snowflake account
#   connect  Test the connection to Snowflake
#   export   Generate a resource config for existing Snowflake resources
#   plan     Compare a resource config to the current state of Snowflake
```

## Optimizing Grant Fetching with ACCOUNT_USAGE

Snowcap uses Snowflake's `ACCOUNT_USAGE` views to fetch grant information efficiently. This reduces API calls from O(N) to O(1) where N is the number of roles—a 90%+ reduction in grant-related queries for accounts with many roles.

### Enabling ACCOUNT_USAGE Access

To enable this optimization, grant `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database to your role:

```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
```

Replace `<your_role>` with the role you use for Snowcap (e.g., `SYSADMIN` or a custom deployment role).

??? note "About ACCOUNT_USAGE Latency"
    ACCOUNT_USAGE views have up to 2 hours of latency—data may not reflect very recent changes. This is acceptable for grants because:

    - **GRANT statements are idempotent**: Re-granting an existing privilege succeeds without error
    - **REVOKE has IF EXISTS semantics**: Revoking a non-existent grant won't fail
    - **Worst case**: The plan shows a grant change that's already applied, and re-applies it harmlessly

### Disabling ACCOUNT_USAGE

If you encounter issues or prefer the traditional per-role `SHOW GRANTS` approach, you can disable ACCOUNT_USAGE:

**CLI (via environment variable or config file):**
```yaml
# snowcap.yml
use_account_usage: false
```

**Python API:**
```python
bp = Blueprint(
    resources=[...],
    use_account_usage=False,
)
```

When disabled (or when `IMPORTED PRIVILEGES` is not granted), Snowcap falls back automatically to the traditional `SHOW GRANTS` approach with a warning.

## Next Steps

- [Export Existing Resources](export.md) - Generate config from your current Snowflake setup
- [Python API](python-api.md) - Programmatic control with Python
- [Working With Resources](working-with-resources.md) - Resource configuration options
- [Role-Based Access Control](role-based-access-control.md) - Best practices for managing permissions
- [Blueprint](blueprint.md) - Advanced deployment customization
- [GitHub Action](snowcap-github-action.md) - Automate deployments with CI/CD
