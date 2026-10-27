---
description: >-
  A SAML2 security integration in Snowflake, which federates authentication to an external identity provider for SSO.
---

# SAML2SecurityIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-saml2) | Snowcap CLI label: `saml2_security_integration`

A SAML2 security integration, which federates Snowflake authentication to an external
identity provider (Okta, ADFS, or any SAML2 IdP). With one in place, clients can connect
with `authenticator = externalbrowser`.
Describe the IdP one of two ways: set `metadata_url`, which Snowflake supports for
Okta and Microsoft Entra ID only, or set all four of `saml2_issuer`, `saml2_sso_url`,
`saml2_provider` and `saml2_x509_cert`. The two are mutually exclusive. `metadata_url`
is CREATE-only in snowcap: Snowflake reads the metadata once and reports the derived
values, so editing the URL later produces no diff and no error, and the change has to
be applied out of band with
`ALTER SECURITY INTEGRATION <name> SET METADATA_URL = '...'`.
Microsoft Entra ID uses `saml2_provider: CUSTOM`. Take `saml2_issuer` from the
enterprise app's Microsoft Entra Identifier, `saml2_sso_url` from its Login URL, and
`saml2_x509_cert` from the Base64 SAML signing certificate. Both URLs embed the
tenant id, so pass them as vars rather than committing them.
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
# The IdP values identify your tenant and rotate independently of this config,
# so keep them out of the repo: SNOWCAP_VAR_SAML2_X509_CERT becomes
# {{ var.saml2_x509_cert }}. Declaring them here with no default makes a missing
# value fail the run before it reaches Snowflake.
vars:
  - name: saml2_issuer
    type: string
  - name: saml2_sso_url
    type: string
  - name: saml2_x509_cert
    type: string
security_integrations:
  - name: sso_saml
    type: SAML2
    enabled: true
    saml2_provider: OKTA
    saml2_issuer: "{{ var.saml2_issuer }}"
    saml2_sso_url: "{{ var.saml2_sso_url }}"
    saml2_x509_cert: "{{ var.saml2_x509_cert }}"
    saml2_enable_sp_initiated: true
    saml2_sp_initiated_login_page_label: Datacoves SSO
```


## Fields

* `name` (string, required) - The name of the security integration.
* `metadata_url` (string) - The IdP metadata URL, in place of the four saml2_* IdP fields. Okta and Microsoft Entra ID only. Set at creation only.
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


