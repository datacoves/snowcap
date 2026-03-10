---
description: >-
  A password secret in Snowflake.
---

# PasswordSecret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret)

A Secret defines a set of sensitive data that can be used for authentication or other purposes.
This class defines a password secret.


## Examples

### YAML

For sensitive values like passwords, use environment variables to avoid storing secrets in your repository. Set environment variables prefixed with `SNOWCAP_VAR_` and reference them using `{{ var.variable_name }}` syntax.

```bash
# Set in your environment or .env file (loaded before running snowcap)
export SNOWCAP_VAR_DB_USERNAME="service_account"
export SNOWCAP_VAR_DB_PASSWORD="your-secret-password"
```

```yaml
secrets:
  - name: some_secret
    secret_type: PASSWORD
    username: "{{ var.db_username }}"
    password: "{{ var.db_password }}"
    comment: Credentials for external database
    owner: SYSADMIN
```


### Python

```python
import os

secret = PasswordSecret(
    name="some_secret",
    username=os.environ.get("DB_USERNAME"),
    password=os.environ.get("DB_PASSWORD"),
    comment="Credentials for external database",
    owner="SYSADMIN",
)
```


## Fields

* `name` (string, required) - The name of the secret.
* `username` (string) - The username for the secret.
* `password` (string) - The password for the secret.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


