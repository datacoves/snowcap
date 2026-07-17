---
description: >-
  A Snowflake OAuth security integration for a custom OAuth client, with credentials generated and managed by Snowflake.
---

# SnowflakeCustomOAuthSecurityIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake) | Snowcap CLI label: `snowflake_custom_oauth_security_integration`

A security integration in Snowflake for a custom OAuth client that Snowflake itself
issues and manages. Unlike the partner integrations, Snowflake generates the OAuth
client_id and client_secret for you; retrieve them with
SYSTEM$SHOW_OAUTH_CLIENT_SECRETS, they cannot be set through this resource.
ACCOUNTADMIN, ORGADMIN, GLOBALORGADMIN, and SECURITYADMIN are always blocked by
Snowflake and must not be listed in blocked_roles_list.
Every property except oauth_client_type is applied with ALTER SECURITY INTEGRATION
and never recreates the integration. oauth_client_type cannot be changed after
creation: snowcap fails the plan rather than recreating the integration, because
recreation would rotate the Snowflake-issued client_id/client_secret and break live
OAuth clients. To change it, recreate the integration manually and migrate clients.
oauth_alternate_redirect_uris is CREATE-ONLY in snowcap: it is not fetchable, so
editing it in YAML after creation produces no diff and no error. Changes must be
applied out-of-band with
ALTER SECURITY INTEGRATION <name> SET OAUTH_ALTERNATE_REDIRECT_URIS = (...).


## Examples

### YAML

```yaml
security_integrations:
  - name: claude_mcp_oauth
    enabled: true
    oauth_client_type: CONFIDENTIAL
    oauth_redirect_uri: https://claude.ai/api/mcp/auth_callback
    oauth_issue_refresh_tokens: true
    oauth_refresh_token_validity: 7776000
    oauth_use_secondary_roles: NONE
    oauth_enforce_pkce: true
    blocked_roles_list:
      - SYSADMIN
    comment: OAuth client for the Claude MCP connector
```


### Python

```python
claude_mcp_oauth = SnowflakeCustomOAuthSecurityIntegration(
    name="claude_mcp_oauth",
    enabled=True,
    oauth_client_type="CONFIDENTIAL",
    oauth_redirect_uri="https://claude.ai/api/mcp/auth_callback",
    oauth_issue_refresh_tokens=True,
    oauth_refresh_token_validity=7776000,
    oauth_use_secondary_roles="NONE",
    oauth_enforce_pkce=True,
    blocked_roles_list=["SYSADMIN"],
    comment="OAuth client for the Claude MCP connector"
)
```


## Fields

* `name` (string, required) - The name of the security integration.
* `enabled` (bool) - Specifies if the security integration is enabled. Defaults to True.
* `oauth_client_type` (string or OAuthClientType, required) - The type of OAuth client. Supported values are 'CONFIDENTIAL' and 'PUBLIC'. Cannot be changed after creation.
* `oauth_redirect_uri` (string, required) - The redirect URI the client uses to complete the OAuth flow.
* `oauth_alternate_redirect_uris` (list) - Additional allowed redirect URIs, set at creation only.
* `oauth_issue_refresh_tokens` (bool) - Indicates if refresh tokens should be issued. Defaults to True.
* `oauth_refresh_token_validity` (int) - The validity period of the refresh token in seconds. Defaults to 7776000.
* `oauth_use_secondary_roles` (string or OAuthUseSecondaryRoles) - Whether secondary roles are activated for OAuth sessions. Supported values are 'IMPLICIT' and 'NONE'. Defaults to 'NONE'.
* `oauth_enforce_pkce` (bool) - Requires clients to use PKCE during the OAuth flow. Defaults to False.
* `network_policy` (string) - The network policy enforced for requests made with this integration's tokens.
* `pre_authorized_roles_list` (list) - Roles granted access without displaying a consent screen to the user.
* `blocked_roles_list` (list) - Roles that are not allowed to use this integration.
* `comment` (string) - A comment about the security integration.


