---
description: >-
  An OAuth secret in Snowflake.
---

# OAuthSecret

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-secret)

A Secret defines a set of sensitive data that can be used for authentication or other purposes.
This class defines an OAuth secret.


## Examples

### YAML

For sensitive values like tokens, use environment variables to avoid storing secrets in your repository. Set environment variables prefixed with `SNOWCAP_VAR_` and reference them using `{{ var.variable_name }}` syntax.

```bash
# Set in your environment or .env file (loaded before running snowcap)
export SNOWCAP_VAR_OAUTH_REFRESH_TOKEN="your-secret-token"
```

```yaml
secrets:
  # OAuth with client credentials flow:
  - name: some_secret
    secret_type: OAUTH2
    api_authentication: some_security_integration
    oauth_scopes:
      - scope1
      - scope2
    comment: OAuth secret for API access
    owner: SYSADMIN

  # OAuth with authorization code grant flow (using environment variable):
  - name: another_secret
    secret_type: OAUTH2
    api_authentication: some_security_integration
    oauth_refresh_token: "{{ var.oauth_refresh_token }}"
    oauth_refresh_token_expiry_time: 2049-01-06 20:00:00
    comment: OAuth secret with refresh token
    owner: SYSADMIN
```


### Python

```python
import os

# OAuth with client credentials flow:
secret = OAuthSecret(
    name="some_secret",
    api_authentication="some_security_integration",
    oauth_scopes=["scope1", "scope2"],
    comment="OAuth secret for API access",
    owner="SYSADMIN",
)

# OAuth with authorization code grant flow (token from environment):
secret = OAuthSecret(
    name="another_secret",
    api_authentication="some_security_integration",
    oauth_refresh_token=os.environ.get("OAUTH_REFRESH_TOKEN"),
    oauth_refresh_token_expiry_time="2049-01-06 20:00:00",
    comment="OAuth secret with refresh token",
    owner="SYSADMIN",
)
```


## Fields

* `name` (string, required) - The name of the secret.
* `api_authentication` (string) - The security integration name for API authentication.
* `oauth_scopes` (list) - The OAuth scopes for the secret.
* `oauth_refresh_token` (string) - The OAuth refresh token.
* `oauth_refresh_token_expiry_time` (string) - The expiry time of the OAuth refresh token.
* `comment` (string) - A comment for the secret.
* `owner` (string or [Role](role.md)) - The owner of the secret. Defaults to SYSADMIN.


