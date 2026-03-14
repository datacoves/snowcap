---
description: >-
  A secret in Snowflake.
---

# Secret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret) | Snowcap CLI label: `secret`

A Secret defines a set of sensitive data that can be used for authentication or other purposes.


## Examples

For sensitive values, use environment variables to avoid storing secrets in your repository. Set environment variables prefixed with `SNOWCAP_VAR_` and reference them using `{{ var.variable_name }}` syntax.

For specific secret types, see:

- [GenericSecret](generic_secret.md) - For generic string secrets
- [PasswordSecret](password_secret.md) - For username/password credentials
- [OAuthSecret](oauth_secret.md) - For OAuth2 authentication

### YAML

```bash
# Set in your environment or .env file
export SNOWCAP_VAR_OAUTH_TOKEN="your-secret-token"
```

```yaml
secrets:
  - name: some_secret
    type: OAUTH2
    api_authentication: some_security_integration
    oauth_scopes:
      - scope1
      - scope2
    oauth_refresh_token: "{{ var.oauth_token }}"
    oauth_refresh_token_expiry_time: 2049-01-06 20:00:00
    comment: OAuth secret for API access
    owner: SYSADMIN
```

### Python

```python
import os

secret = Secret(
    name="some_secret",
    type="OAUTH2",
    api_authentication="some_security_integration",
    oauth_scopes=["scope1", "scope2"],
    oauth_refresh_token=os.environ.get("OAUTH_TOKEN"),
    oauth_refresh_token_expiry_time="2049-01-06 20:00:00",
    comment="OAuth secret for API access",
    owner="SYSADMIN",
)
```


## Fields

* `name` (string, required) - The name of the secret.
* `type` (string or SecretType, required) - The type of the secret.
* `api_authentication` (string) - The security integration name for API authentication.
* `oauth_scopes` (list) - The OAuth scopes for the secret.
* `oauth_refresh_token` (string) - The OAuth refresh token.
* `oauth_refresh_token_expiry_time` (string) - The expiry time of the OAuth refresh token.
* `username` (string) - The username for the secret.
* `password` (string) - The password for the secret.
* `secret_string` (string) - The secret string.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


