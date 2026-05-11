---
description: >-
  An API integration in Snowflake.
---

# APIIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration) | Snowcap CLI label: `api_integration`

Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
This class supports creating, replacing, and checking the existence of API integrations across multiple cloud providers
and Git HTTPS providers.

## Supported `api_provider` values

| `api_provider` | Required fields | Used for |
|---|---|---|
| `AWS_API_GATEWAY`, `AWS_PRIVATE_API_GATEWAY`, `AWS_GOV_API_GATEWAY`, `AWS_GOV_PRIVATE_API_GATEWAY` | `api_aws_role_arn` | External functions calling AWS API Gateway |
| `AZURE_API_MANAGEMENT` | `azure_tenant_id`, `azure_ad_application_id` | External functions calling Azure API Management |
| `GOOGLE_API_GATEWAY` | `google_audience` | External functions calling Google API Gateway |
| `GIT_HTTPS_API` | (none beyond `api_allowed_prefixes`) | Snowflake [Git repositories](git_repository.md) connecting to GitHub/GitLab/Bitbucket/etc. |

## Examples

### YAML — AWS API Gateway

```yaml
api_integrations:
  - name: some_api_integration
    api_provider: AWS_API_GATEWAY
    api_aws_role_arn: "arn:aws:iam::123456789012:role/MyRole"
    enabled: true
    api_allowed_prefixes: ["/prod/", "/dev/"]
    api_blocked_prefixes: ["/test/"]
    api_key: "ABCD1234"
    comment: "Example AWS API integration"
```

### YAML — GitHub (used by GitRepository)

```yaml
api_integrations:
  - name: github_api_integration
    api_provider: GIT_HTTPS_API
    api_allowed_prefixes: ["https://github.com/some-org/"]
    enabled: true
    comment: "GitHub integration for git repos"
```

### Python

```python
api_integration = APIIntegration(
    name="some_api_integration",
    api_provider="AWS_API_GATEWAY",
    api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
    enabled=True,
    api_allowed_prefixes=["/prod/", "/dev/"],
    api_blocked_prefixes=["/test/"],
    api_key="ABCD1234",
    comment="Example API integration",
)
```


## Fields

* `name` (string, required) - The unique name of the API integration.
* `api_provider` (string or ApiProvider, required) - The provider of the API service. See table above for supported values.
* `api_aws_role_arn` (string) - The AWS IAM role ARN. Required for AWS providers; omit for AZURE/GOOGLE/GIT_HTTPS_API.
* `azure_tenant_id` (string) - Azure AD tenant ID. Required for `AZURE_API_MANAGEMENT`.
* `azure_ad_application_id` (string) - Azure AD application registration ID. Required for `AZURE_API_MANAGEMENT`.
* `google_audience` (string) - GCP audience identifier. Required for `GOOGLE_API_GATEWAY`.
* `api_key` (string) - Optional API key used for authentication.
* `api_allowed_prefixes` (list) - The list of allowed prefixes for the API endpoints.
* `api_blocked_prefixes` (list) - The list of blocked prefixes for the API endpoints.
* `enabled` (bool, required) - Specifies if the API integration is enabled. Defaults to TRUE.
* `comment` (string) - A comment or description for the API integration.


## Granting on an integration

Snowflake's `GRANT USAGE ON INTEGRATION <name>` SQL is valid for any subtype. In YAML you may use either the
concrete subtype (`on: api integration <fqn>`) — preferred — or the generic umbrella (`on: integration <fqn>`):

```yaml
grants:
  - priv: USAGE
    on: api integration github_api_integration   # preferred — explicit subtype
    to: some_role
  - priv: USAGE
    on: integration github_api_integration       # also supported (umbrella)
    to: another_role
```
