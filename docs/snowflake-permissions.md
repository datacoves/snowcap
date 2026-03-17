# Snowflake Permissions

Snowcap runs SQL on your behalf. Whatever role your session uses, that's the role Snowcap uses. It has no elevated access of its own. This means a human operator running `snowcap apply` is doing exactly what they could do by hand in a SQL worksheet, just with reproducibility and version control.

The permissions question becomes more important with **service accounts**. When Snowcap runs in a CI/CD pipeline, it operates unattended under a dedicated user. That user's role determines the blast radius if something goes wrong, or if the account is ever compromised. For that reason, Snowflake recommends (and Snowcap supports) running pipelines under a purpose-built role rather than ACCOUNTADMIN.

## Human operators

No special setup is required. Use whatever Snowflake role you already have. If your role can create a warehouse manually, `snowcap apply` can create it too.

If your team wants consistent behavior across all operators (for example, to ensure no one accidentally sets account parameters through Snowcap), you can have everyone set `SNOWFLAKE_ROLE=SNOWCAP_ADMIN` in their local environment. That's an organizational choice, not a security requirement.

## Service accounts and CI/CD

This is where role design matters. A service account used in CI/CD should hold the minimum privileges Snowcap needs and nothing more. ACCOUNTADMIN can do things well outside Snowcap's scope (modify billing configuration, alter SSO/SAML settings, change encryption key management, suspend the account), none of which your pipeline should ever need.

The scripts below walk through creating a `SNOWCAP_ADMIN` role scoped to what Snowcap actually uses.

### Step 1: Core privileges

The base privileges cover what most teams manage with Snowcap: databases, schemas, warehouses, roles, users, and grants. These map to Snowflake's standard admin roles (SYSADMIN, SECURITYADMIN, USERADMIN).

```sql
USE ROLE ACCOUNTADMIN;

-- Create the role
CREATE ROLE IF NOT EXISTS SNOWCAP_ADMIN;

-- Infrastructure (SYSADMIN equivalent)
GRANT CREATE DATABASE ON ACCOUNT TO ROLE SNOWCAP_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE SNOWCAP_ADMIN;

-- Identity and access control (SECURITYADMIN + USERADMIN equivalent)
GRANT CREATE ROLE ON ACCOUNT TO ROLE SNOWCAP_ADMIN;
GRANT CREATE USER ON ACCOUNT TO ROLE SNOWCAP_ADMIN;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SNOWCAP_ADMIN;

-- Place the role in the standard hierarchy so SYSADMIN
-- can see and manage objects that SNOWCAP_ADMIN creates
GRANT ROLE SNOWCAP_ADMIN TO ROLE SYSADMIN;
```

> **On MANAGE GRANTS:** This privilege allows the holder to grant or revoke privileges on any object in the account, even objects it doesn't own. Snowcap needs it for ownership transfers and to manage grants on behalf of the roles it controls. It's effectively equivalent to SECURITYADMIN for access control purposes. Treat the service account credentials accordingly.

### Step 2: Additional privileges

Add these based on which resources your configuration includes:

```sql
-- Integrations (storage, API, external access, etc.)
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SNOWCAP_ADMIN;

-- Network policies and rules
GRANT CREATE NETWORK POLICY ON ACCOUNT TO ROLE SNOWCAP_ADMIN;

-- Snowpark Container Services (compute pools, image repositories, services)
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE SNOWCAP_ADMIN;

-- ACCOUNT_USAGE optimization for large deployments (50+ roles)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SNOWCAP_ADMIN;
```

See [Optimizing Grant Fetching with ACCOUNT_USAGE](getting-started.md#optimizing-grant-fetching-with-account_usage) for when the ACCOUNT_USAGE optimization is worth enabling.

### Step 3: Create the service user

```sql
-- Generate your key pair first:
-- openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowcap_key.p8 -nocrypt
-- openssl rsa -in snowcap_key.p8 -pubout -out snowcap_key.pub

CREATE USER IF NOT EXISTS SNOWCAP_SVC
    TYPE = SERVICE
    RSA_PUBLIC_KEY = '<contents_of_snowcap_key.pub>'
    DEFAULT_ROLE = SNOWCAP_ADMIN
    COMMENT = 'Snowcap CI/CD service account';

GRANT ROLE SNOWCAP_ADMIN TO USER SNOWCAP_SVC;
```

Service users with `TYPE = SERVICE` cannot log into Snowsight and do not require MFA, which makes them appropriate for unattended automation.

## Resources that require ACCOUNTADMIN

Three resource types in Snowcap are locked to ACCOUNTADMIN by Snowflake. No privilege can be granted that would allow a custom role to create or modify them. This is a platform constraint, not a Snowcap limitation.

| Resource | Why ACCOUNTADMIN is required |
|---|---|
| [ResourceMonitor](resources/resource_monitor.md) | `CREATE RESOURCE MONITOR` is not a grantable privilege |
| [AccountParameter](resources/account_parameter.md) | `ALTER ACCOUNT SET` requires ACCOUNTADMIN for most parameters |
| [FailoverGroup](resources/failover_group.md) / [ReplicationGroup](resources/replication_group.md) | Failover and replication management is restricted to ACCOUNTADMIN |

If your configuration includes any of these, you have two options:

**Option A:** Run the pipeline with ACCOUNTADMIN for the full resource set. Simpler, but the service account holds more privilege than it needs for everything else.

**Option B:** Run the pipeline with SNOWCAP_ADMIN for all standard resources, and handle resource monitors, account parameters, and failover groups through a separate process (a restricted manual workflow, a separate pipeline step that uses ACCOUNTADMIN only for those resource types, etc.).

Most teams choose Option B as they mature their setup.

## What ACCOUNTADMIN can do that SNOWCAP_ADMIN cannot

This is the full list of ACCOUNTADMIN-exclusive capabilities that have nothing to do with Snowcap. It's useful context when explaining the custom role to a security team.

- **Billing and usage:** view credit consumption, manage payment methods, access billing dashboards
- **Encryption key management:** configure Tri-Secret Secure, switch between Snowflake-managed and customer-managed key hierarchies (AWS KMS, Azure Key Vault, GCP Cloud KMS)
- **Federated authentication:** `ALTER ACCOUNT SET SAML_IDENTITY_PROVIDER`, SCIM integration setup; an ACCOUNTADMIN compromise could silently redirect authentication to an attacker-controlled identity provider
- **Trust Center:** enable, disable, and configure compliance scanner packages (these can drive significant unexpected credit consumption if misconfigured)
- **Account suspension:** suspend or resume the account itself
- **Cross-account replication targets:** approve an account as a replication target on the receiving side

None of these are actions a Snowcap pipeline should ever need to take. Keeping them out of the service account's reach is the main reason to use a custom role.

## Environment variables

```bash
SNOWFLAKE_ACCOUNT=<orgname>-<accountname>
SNOWFLAKE_USER=SNOWCAP_SVC
SNOWFLAKE_ROLE=SNOWCAP_ADMIN
SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/snowcap_key.p8
```

See [Getting Started](getting-started.md) for the full list of supported authenticators.
