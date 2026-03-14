---
description: >-
  A generic secret in Snowflake.
---

# GenericSecret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret) | Snowcap CLI label: `generic_secret`

A Secret defines a set of sensitive data that can be used for authentication or other purposes.
This class defines a generic secret.


## Examples

### YAML

For sensitive values, use environment variables to avoid storing secrets in your repository. Set environment variables prefixed with `SNOWCAP_VAR_` and reference them using `{{ var.variable_name }}` syntax.

```bash
# Set in your environment or .env file (loaded before running snowcap)
export SNOWCAP_VAR_API_KEY="your-secret-api-key"
```

```yaml
secrets:
  - name: some_secret
    secret_type: GENERIC_STRING
    secret_string: "{{ var.api_key }}"
    comment: API key for external service
    owner: SYSADMIN
```


### Python

```python
import os

secret = GenericSecret(
    name="some_secret",
    secret_string=os.environ.get("API_KEY"),
    comment="API key for external service",
    owner="SYSADMIN",
)
```


## Fields

* `name` (string, required) - The name of the secret.
* `secret_string` (string) - The secret string.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


