# GitHub Action

Automate Snowflake deployments with GitHub Actions using a review-then-apply workflow.

## Workflow Pattern

The recommended pattern is **plan on PR, apply on merge**:

1. **Pull Request opened** → Run `snowcap plan` to show what changes will be made
2. **Reviewers** → See the planned changes in the PR, approve or request changes
3. **PR merged to main** → Run `snowcap apply` to execute the changes

This prevents accidental changes - nothing is applied to Snowflake until the PR is reviewed and merged.

## Authentication

GitHub Actions require **key-pair authentication** since service accounts can't use passwords or MFA.

Set up key-pair auth in Snowflake:
1. [Generate a key pair](https://docs.snowflake.com/en/user-guide/key-pair-auth#generate-the-private-key)
2. Assign the public key to your service user
3. Store the private key as a GitHub secret

## Example Workflow

```yaml
# .github/workflows/snowcap.yml
name: Snowcap

on:
  pull_request:
    paths:
      - 'snowcap/**'
  push:
    branches: [main]
    paths:
      - 'snowcap/**'

jobs:
  plan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: pip install snowcap

      - name: Write private key
        run: echo "${{ secrets.SNOWFLAKE_PRIVATE_KEY }}" > /tmp/rsa_key.pem

      - name: Plan changes
        run: snowcap plan --config ./snowcap/
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PRIVATE_KEY_PATH: /tmp/rsa_key.pem
          SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}

  apply:
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: pip install snowcap

      - name: Write private key
        run: echo "${{ secrets.SNOWFLAKE_PRIVATE_KEY }}" > /tmp/rsa_key.pem

      - name: Apply changes
        run: snowcap apply --config ./snowcap/
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PRIVATE_KEY_PATH: /tmp/rsa_key.pem
          SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
          SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
```

## Configure Secrets

Go to your GitHub repository **Settings → Secrets and variables → Actions** and add:

| Secret | Description |
|--------|-------------|
| `SNOWFLAKE_ACCOUNT` | Your Snowflake account identifier |
| `SNOWFLAKE_USER` | Service account username |
| `SNOWFLAKE_PRIVATE_KEY` | Contents of your private key file (PEM format) |
| `SNOWFLAKE_ROLE` | Role to use for deployments (e.g., `SECURITYADMIN`) |

## How It Works

1. **Developer** creates a branch and modifies files in `snowcap/`
2. **Opens PR** → GitHub runs `snowcap plan`, showing planned changes
3. **Reviewer** approves the PR after reviewing the plan output
4. **Merge to main** → GitHub runs `snowcap apply`, changes are made to Snowflake