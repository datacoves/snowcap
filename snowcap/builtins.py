# The blueprint uses an account named ACCOUNT as the root container that every
# account-scoped resource hangs off. It is a sentinel, not a real Snowflake
# account, so it must never be looked up or altered.
ROOT_ACCOUNT_NAME = "ACCOUNT"

SYSTEM_DATABASES = [
    "SNOWFLAKE",
    "WORKSHEETS_APP",
]

SYSTEM_SCHEMAS = [
    "PUBLIC",
    "INFORMATION_SCHEMA",
]

SYSTEM_ROLES = [
    "ACCOUNTADMIN",
    "ORGADMIN",
    "PUBLIC",
    "SECURITYADMIN",
    "SYSADMIN",
    "USERADMIN",
]

SYSTEM_USERS = [
    "SNOWFLAKE",
]

SYSTEM_SECURITY_INTEGRATIONS = [
    "APPLICA",
]
