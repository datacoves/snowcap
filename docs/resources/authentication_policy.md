---
description: >-
  An authentication policy in Snowflake.
---

# AuthenticationPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy) | Snowcap CLI label: `authentication_policy`

Defines the rules and constraints for authentication within the system, ensuring they meet specific security standards.


## Examples

### Python

```python
authentication_policy = AuthenticationPolicy(
    name="some_authentication_policy",
    authentication_methods=["PASSWORD", "SAML", "PROGRAMMATIC_ACCESS_TOKEN"],
    mfa_authentication_methods=["PASSWORD"],
    mfa_enrollment="REQUIRED",
    client_types=["SNOWFLAKE_UI"],
    security_integrations=["ALL"],
    pat_policy={
        "network_policy_evaluation": "ENFORCED_NOT_REQUIRED",
        "default_expiry_in_days": 30,
        "max_expiry_in_days": 180,
    },
    comment="Policy for secure authentication."
)
```


### YAML

```yaml
authentication_policies:
  - name: some_authentication_policy
    authentication_methods:
      - PASSWORD
      - SAML
      - PROGRAMMATIC_ACCESS_TOKEN
    mfa_authentication_methods:
      - PASSWORD
    mfa_enrollment: REQUIRED
    client_types:
      - SNOWFLAKE_UI
    security_integrations:
      - ALL
    pat_policy:
      network_policy_evaluation: ENFORCED_NOT_REQUIRED
      default_expiry_in_days: 30
      max_expiry_in_days: 180
    comment: Policy for secure authentication.
```


## Fields

* `name` (string, required) - The name of the authentication policy.
* `authentication_methods` (list) - A list of allowed authentication methods.
* `mfa_authentication_methods` (list) - A list of authentication methods that enforce multi-factor authentication (MFA).
* `mfa_enrollment` (string) - Determines whether a user must enroll in multi-factor authentication. Defaults to OPTIONAL.
* `client_types` (list) - A list of clients that can authenticate with Snowflake.
* `security_integrations` (list) - A list of security integrations the authentication policy is associated with.
* `pat_policy` (dict) - Controls programmatic access token issuance: network_policy_evaluation, default_expiry_in_days, and max_expiry_in_days must all be given or all omitted; declaring exactly the Snowflake defaults (ENFORCED_REQUIRED, 15, 365) compares as unset.
* `comment` (string) - A comment or description for the authentication policy.
* `owner` (string or [Role](role.md)) - The owner role of the authentication policy. Defaults to SECURITYADMIN.


