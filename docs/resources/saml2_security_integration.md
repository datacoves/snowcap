---
description: >-
  A SAML2 security integration in Snowflake, which federates authentication to an external identity provider for SSO.
---

# SAML2SecurityIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-saml2) | Snowcap CLI label: `saml2_security_integration`

A SAML2 security integration, which federates Snowflake authentication to an external
identity provider (Okta, ADFS, or any SAML2 IdP). With one in place, clients can connect
with `authenticator = externalbrowser`.
Snowflake matches the IdP's NameID to the user's `login_name`, so every federated user
needs a `login_name` that matches what the IdP sends.
To stop the browser from opening on every new connection, also set the `ALLOW_ID_TOKEN`
account parameter to `true`, which lets the client cache the SSO token in the local
OS keystore. Caching also depends on the driver: the Python connector caches on Linux
from 5.0 on, earlier versions need the `secure-local-storage` extra.
Creating the integration and setting `ALLOW_ID_TOKEN` both require ACCOUNTADMIN.
`saml2_x509_cert` takes either the bare base64 body of the IdP signing certificate or a
full PEM block; the PEM header, footer and whitespace are stripped, because that is how
Snowflake stores it.


## Examples

### Python

```python
sso_saml = SAML2SecurityIntegration(
    name="sso_saml",
    enabled=True,
    saml2_provider="OKTA",
    saml2_issuer="http://www.okta.com/exk1abcdefg",
    saml2_sso_url="https://example.okta.com/app/snowflake/exk1abcdefg/sso/saml",
    saml2_x509_cert="MIIDpDCCAoygAwIBAgIGAV2ka...",
    saml2_enable_sp_initiated=True,
    saml2_sp_initiated_login_page_label="Datacoves SSO",
)
```


### YAML

```yaml
security_integrations:
  - name: sso_saml
    type: SAML2
    enabled: true
    saml2_provider: OKTA
    saml2_issuer: http://www.okta.com/exk1abcdefg
    saml2_sso_url: https://example.okta.com/app/snowflake/exk1abcdefg/sso/saml
    saml2_x509_cert: "{{ var.saml2_x509_cert }}"
    saml2_enable_sp_initiated: true
    saml2_sp_initiated_login_page_label: Datacoves SSO
```


## Fields

* `name` (string, required) - The name of the security integration.
* `saml2_issuer` (string, required) - The EntityID or Issuer of the IdP.
* `saml2_sso_url` (string, required) - The IdP SSO URL where Snowflake sends the SAML request.
* `saml2_provider` (string or SAML2Provider, required) - The IdP vendor. One of 'OKTA', 'ADFS', or 'CUSTOM'.
* `saml2_x509_cert` (string, required) - The IdP signing certificate, base64 encoded or as a PEM block.
* `enabled` (bool) - Specifies if the security integration is enabled. Defaults to True.
* `saml2_enable_sp_initiated` (bool) - Shows the IdP button on the Snowflake login page for SP-initiated login. Defaults to False.
* `saml2_sp_initiated_login_page_label` (string) - The label of that button on the Snowflake login page.
* `saml2_snowflake_acs_url` (string) - The Snowflake assertion consumer service URL the IdP posts to. Defaults to the account's own URL.
* `saml2_snowflake_issuer_url` (string) - The Snowflake EntityID sent to the IdP. Defaults to the account's own URL.
* `saml2_force_authn` (bool) - Forces the user to re-authenticate at the IdP on every login. Defaults to False.
* `saml2_requested_nameid_format` (string) - The NameID format Snowflake requests from the IdP.
* `saml2_post_logout_redirect_url` (string) - Where the user lands after logging out of Snowflake.
* `saml2_sign_request` (bool) - Signs the SAML request Snowflake sends to the IdP. Defaults to False.
* `allowed_user_domains` (list) - Email domains allowed to authenticate through this integration.
* `allowed_email_patterns` (list) - Regular expressions for email addresses allowed to authenticate through this integration.
* `comment` (string) - A comment about the security integration.


