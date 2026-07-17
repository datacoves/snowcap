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

# Snowflake always blocks these roles from a CUSTOM OAuth integration, regardless of
# what's in BLOCKED_ROLES_LIST, and echoes them back in DESC. Shared by the spec (which
# rejects them in blocked_roles_list) and the fetch layer (which strips them from state
# fetched from Snowflake), so both sides agree on what "always blocked" means.
ALWAYS_BLOCKED_OAUTH_ROLES = [
    "ACCOUNTADMIN",
    "ORGADMIN",
    "GLOBALORGADMIN",
    "SECURITYADMIN",
]
