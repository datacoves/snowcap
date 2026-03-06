---
description: >-
  Managing secrets and environment variables in Snowcap.
---

# Secrets and Environment Variables

Snowcap provides a secure way to manage sensitive values like passwords, API keys, and tokens without storing them in your configuration files or repository.

## How It Works

Snowcap automatically loads environment variables prefixed with `SNOWCAP_VAR_` and makes them available in your YAML configurations using Jinja2 templating syntax.

**Pattern:**
1. Set environment variable: `SNOWCAP_VAR_MY_SECRET="sensitive-value"`
2. Reference in YAML: `{{ var.my_secret }}`

The variable name is converted to lowercase when referenced. For example:
- `SNOWCAP_VAR_DB_PASSWORD` → `{{ var.db_password }}`
- `SNOWCAP_VAR_API_KEY` → `{{ var.api_key }}`

## Using .env Files

For local development, you can store environment variables in a `.env` file and source it before running Snowcap.

### Create a .env file

```bash
# .env (add to .gitignore!)
SNOWCAP_VAR_DB_PASSWORD="your-secret-password"
SNOWCAP_VAR_API_KEY="your-api-key"
SNOWCAP_VAR_OAUTH_TOKEN="your-oauth-token"
```

### Load and run Snowcap

Use `export` with a subshell to load the variables:

```bash
export $(cat .env | xargs)
snowcap plan --config snowcap.yml
```

Or use a tool like [direnv](#using-direnv) for automatic loading.

!!! warning "Important"
    Always add `.env` to your `.gitignore` file to prevent accidentally committing secrets to your repository.

```bash
# .gitignore
.env
.env.*
```

## Using direnv

[direnv](https://direnv.net/) is a shell extension that automatically loads environment variables when you enter a directory.

### Setup

1. Install direnv:
   ```bash
   # macOS
   brew install direnv

   # Ubuntu/Debian
   sudo apt install direnv
   ```

2. Add to your shell (e.g., `~/.zshrc` or `~/.bashrc`):
   ```bash
   eval "$(direnv hook zsh)"  # or bash
   ```

3. Create a `.envrc` file in your project:
   ```bash
   # .envrc
   export SNOWCAP_VAR_DB_PASSWORD="your-secret-password"
   export SNOWCAP_VAR_API_KEY="your-api-key"
   ```

4. Allow the directory:
   ```bash
   direnv allow
   ```

Now environment variables are automatically loaded when you `cd` into the project directory.

## YAML Configuration Examples

### Secrets

```yaml
secrets:
  # Generic secret (API key)
  - name: external_api_key
    secret_type: GENERIC_STRING
    secret_string: "{{ var.api_key }}"
    comment: API key for external service

  # Password secret
  - name: database_credentials
    secret_type: PASSWORD
    username: "{{ var.db_username }}"
    password: "{{ var.db_password }}"
    comment: Credentials for external database

  # OAuth secret
  - name: oauth_secret
    secret_type: OAUTH2
    api_authentication: my_security_integration
    oauth_refresh_token: "{{ var.oauth_token }}"
    oauth_refresh_token_expiry_time: 2049-01-06 20:00:00
```

### Other Resources

Environment variables work with any resource field:

```yaml
users:
  - name: service_account
    password: "{{ var.service_account_password }}"
    default_role: SERVICE_ROLE
```

## CI/CD Integration

### GitHub Actions

Use GitHub Secrets to securely pass environment variables:

```yaml
# .github/workflows/snowcap.yml
name: Snowcap Deploy

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Run Snowcap
        uses: datacoves/snowcap-action@v1
        with:
          config: snowcap.yml
          command: apply
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWCAP_VAR_API_KEY: ${{ secrets.API_KEY }}
          SNOWCAP_VAR_DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
```

### GitLab CI

```yaml
# .gitlab-ci.yml
snowcap-deploy:
  image: python:3.11
  script:
    - pip install snowcap
    - snowcap apply --config snowcap.yml
  variables:
    SNOWCAP_VAR_API_KEY: $API_KEY
    SNOWCAP_VAR_DB_PASSWORD: $DB_PASSWORD
```

### Azure DevOps

```yaml
# azure-pipelines.yml
steps:
  - script: |
      pip install snowcap
      snowcap apply --config snowcap.yml
    env:
      SNOWCAP_VAR_API_KEY: $(API_KEY)
      SNOWCAP_VAR_DB_PASSWORD: $(DB_PASSWORD)
```

## Best Practices

1. **Never commit secrets** - Always use environment variables for sensitive values and add `.env` files to `.gitignore`.

2. **Use secret managers in CI/CD** - GitHub Secrets, GitLab CI Variables, Azure Key Vault, AWS Secrets Manager, etc.

3. **Rotate secrets regularly** - Update tokens and passwords periodically.

4. **Use different secrets per environment** - Production, staging, and development should have separate credentials.

5. **Limit secret scope** - Only expose secrets to the services and pipelines that need them.

## Troubleshooting

### Variable not found

If you get an error that a variable is not defined:

1. Verify the environment variable is set:
   ```bash
   echo $SNOWCAP_VAR_MY_SECRET
   ```

2. Check the prefix is correct (`SNOWCAP_VAR_` in uppercase)

3. Remember the variable name is lowercased in templates:
   - `SNOWCAP_VAR_MY_SECRET` → `{{ var.my_secret }}`

### Testing variable substitution

Use `snowcap plan` to preview the configuration before applying:

```bash
source .env && snowcap plan --config snowcap.yml
```

This will show you the resolved configuration without making changes to Snowflake.
