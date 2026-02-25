import datetime
import inspect
import json
import logging
import sys
from functools import cache
from typing import Any, Optional, TypedDict, Union

import pytz
from inflection import pluralize
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from .builtins import (
    SYSTEM_DATABASES,
    SYSTEM_ROLES,
    SYSTEM_SECURITY_INTEGRATIONS,
    SYSTEM_USERS,
)
from .client import (
    ACCESS_CONTROL_ERR,
    DOES_NOT_EXIST_ERR,
    INVALID_IDENTIFIER,
    OBJECT_DOES_NOT_EXIST_ERR,
    UNSUPPORTED_FEATURE,
    execute,
    execute_in_parallel,
)
from .enums import AccountEdition, GrantType, ResourceType, WarehouseSize
from .identifiers import FQN, URN, parse_FQN, resource_type_for_label
from .parse import (
    _parse_column,
    _parse_dynamic_table_text,
    parse_collection_string,
    parse_region,
    parse_view_ddl,
)
from .privs import GrantedPrivilege
from .resource_name import (
    ResourceName,
    attribute_is_resource_name,
    resource_name_from_snowflake_metadata,
)

__this__ = sys.modules[__name__]

logger = logging.getLogger("snowcap")

# Cache for inspect.signature() results to avoid repeated introspection
_SIGNATURE_CACHE: dict[str, inspect.Signature] = {}


def _get_cached_signature(func_name: str) -> inspect.Signature:
    """Get the signature for a function, caching the result."""
    if func_name not in _SIGNATURE_CACHE:
        func = getattr(__this__, func_name)
        _SIGNATURE_CACHE[func_name] = inspect.signature(func)
    return _SIGNATURE_CACHE[func_name]


class SessionContext(TypedDict):
    account_edition: AccountEdition
    account_grant_map: dict[str, list[ResourceName]]
    account_locator: str
    account: str
    available_roles: list[ResourceName]
    cloud_region: str
    cloud: str
    database: str
    role: ResourceName
    schemas: list[str]
    secondary_roles: list[str]
    user: str
    version: str
    warehouse: str


def _quote_snowflake_identifier(identifier: Union[str, ResourceName]) -> str:
    return str(resource_name_from_snowflake_metadata(identifier))


def _get_owner_identifier(data: dict) -> str:
    if "owner_role_type" not in data:
        return _quote_snowflake_identifier(data["owner"])
    if data["owner"] == "":
        return ""
    if data["owner_role_type"] == "DATABASE_ROLE":
        return _quote_snowflake_identifier(data["database_name"]) + "." + _quote_snowflake_identifier(data["owner"])
    elif data["owner_role_type"] == "ROLE":
        return _quote_snowflake_identifier(data["owner"])
    else:
        raise Exception(f"Unsupported owner role type: {data['owner_role_type']}, {data}")


def _desc_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        property = row["property"]
        if lower_properties:
            property = property.lower()
        result[property] = row["value"]
    return result


def _desc_type2_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        property = row["property"]
        if lower_properties:
            property = property.lower()
        value = row["property_value"]
        if row["property_type"] == "Boolean":
            value = value == "true"
        elif row["property_type"] == "Long":
            value = value or None
        elif row["property_type"] == "Integer":
            value = int(value)
        elif row["property_type"] == "String":
            value = value or None
        elif row["property_type"] == "List":
            value = _parse_list_property(value)
        # Not sure this is correct. External Access Integration uses this
        elif row["property_type"] == "Object":
            value = _parse_list_property(value)
        result[property] = value
    return result


def _desc_type3_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        parent_property = row["parent_property"]
        property = row["property"]
        if lower_properties:
            parent_property = parent_property.lower()
            property = property.lower()
        value = row["property_value"]
        if row["property_type"] == "Boolean":
            value = value == "true"
        elif row["property_type"] == "Long":
            value = value or None
        elif row["property_type"] == "Integer":
            value = int(value)
        elif row["property_type"] == "String":
            value = value or None
        elif row["property_type"] == "List":
            value = _parse_list_property(value)

        if parent_property:
            if parent_property not in result:
                result[parent_property] = {}
            result[parent_property][property] = value
        else:
            result[property] = value
    return result


def _desc_type4_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        property = row["name"]
        if lower_properties:
            property = property.lower()
        result[property] = row["value"]

    return result


def _fail_if_not_granted(result, *args):
    if len(result) == 0:
        raise Exception("Failed to create grant")
    if len(result) == 1 and result[0]["status"] == "Grant not executed: Insufficient privileges.":
        raise Exception(result[0]["status"], *args)


def _fetch_grant_to_role(
    session: SnowflakeConnection,
    grant_type: GrantType,
    role: ResourceName,
    granted_on: str,
    on_name: str,
    privilege: str,
    role_type: ResourceType = ResourceType.ROLE,
):
    grants = (
        _show_future_grants_to_role(session, role, cacheable=True)
        if grant_type == GrantType.FUTURE
        else _show_grants_to_role(session, role, role_type=role_type, cacheable=True)
    )
    for grant in grants:
        name = "ACCOUNT" if grant["granted_on"] == "ACCOUNT" else grant["name"]
        # Use ResourceName for comparison to handle quoted identifiers correctly
        name_matches = ResourceName(name) == ResourceName(on_name) if name != "ACCOUNT" else name == on_name
        if grant["granted_on"] == granted_on and grant["privilege"] == privilege and name_matches:
            return grant
    return None


def _filter_result(result, **kwargs):

    filtered = []
    predicates = {key: value for key, value in kwargs.items() if value is not None}
    for row in result:
        for key, value in predicates.items():
            # Roughly match any names. `name`, `database_name`, `schema_name`, etc.
            if attribute_is_resource_name(key):
                if resource_name_from_snowflake_metadata(row[key]) != ResourceName(value):
                    # if ResourceName(value) != f'"{row[key]}"':
                    break
            else:
                if row[key] != value:
                    break
        else:
            filtered.append(row)
    return filtered


# def _urn_from_grant(row, session_ctx):
#     account_scoped_resources = {"user", "role", "warehouse", "database", "task"}
#     granted_on = row["granted_on"].lower()
#     if granted_on == "account":
#         return URN.from_session_ctx(session_ctx)
#     else:
#         if granted_on == "procedure" or granted_on == "function":
#             # This needs a special function because Snowflake gives an incorrect FQN for functions/sprocs
#             # eg. TITAN_DEV.PUBLIC."FETCH_DATABASE(NAME VARCHAR):OBJECT"
#             # The correct FQN is TITAN_DEV.PUBLIC."FETCH_DATABASE"(VARCHAR)
#             id_parts = list(FullyQualifiedIdentifier.parse_string(row["name"], parse_all=True))
#             name = parse_function_name(id_parts[-1])
#             fqn = FQN(database=id_parts[0], schema=id_parts[1], name=name)
#         elif granted_on in account_scoped_resources:
#             # This is probably all account-scoped resources
#             fqn = FQN(name=ResourceName(row["name"]))
#         else:
#             # Scoped resources
#             fqn = parse_FQN(row["name"], is_db_scoped=(granted_on == "schema"))
#         return URN(
#             resource_type=ResourceType(granted_on),
#             account_locator=session_ctx["account_locator"],
#             fqn=fqn,
#         )


def _convert_to_gmt(dt: datetime.datetime, fmt_str: str = "%Y-%m-%d %H:%M:%S") -> Optional[str]:
    """
    datetime.datetime(2049, 1, 6, 12, 0, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>)

    =>

    2049-01-06 20:00
    """
    if not dt:
        return None
    gmt = pytz.timezone("GMT")
    dt_gmt = dt.astimezone(gmt)
    return dt_gmt.strftime(fmt_str)


def _parse_cluster_keys(cluster_keys_str: str) -> Optional[list[str]]:
    """
    Assume cluster key statement is in the form of:
        LINEAR(C1, C3)
        LINEAR(SUBSTRING(C2, 5, 15), CAST(C1 AS DATE))
    """
    if cluster_keys_str is None or cluster_keys_str == "":
        return None
    cluster_keys_str = cluster_keys_str[len("LINEAR") :]
    cluster_keys_str = cluster_keys_str.strip("()")
    return [key.strip(" ") for key in cluster_keys_str.split(",")]


def _parse_function_arguments_2023_compat(arguments_str: str) -> tuple:
    """
    Input
    -----
        FETCH_DATABASE(OBJECT [, BOOLEAN]) RETURN OBJECT

    Output
    ------
        identifier => FETCH_DATABASE(OBJECT, BOOLEAN)
        returns => OBJECT

    """

    header, returns = arguments_str.split(" RETURN ")
    header = header.replace("[", "").replace("]", "")
    identifier = parse_FQN(header)
    return (identifier, returns)


def _parse_function_arguments(arguments_str: str) -> tuple[FQN, str]:
    """
    Input
    -----
        FETCH_DATABASE(VARCHAR) RETURN OBJECT

    Output
    ------
        identifier => FETCH_DATABASE(VARCHAR)
        returns => OBJECT

    """

    header, returns = arguments_str.split(" RETURN ")
    identifier = parse_FQN(header)
    return (identifier, returns)


def _parse_list_property(property_str: str) -> Optional[list]:
    if property_str is None or property_str == "":
        return None
    property_str = property_str.strip("[]")
    if property_str:
        return [item.strip(" ") for item in property_str.split(",")]
    return []


def _parse_signature(signature: str) -> list:
    signature = signature.strip("()")

    if signature:
        return [_parse_column(col.strip(" ")) for col in signature.split(",")]
    return []


def _parse_comma_separated_values(values: str) -> Optional[list]:
    if values is None or values == "":
        return None
    return [value.strip(" ") for value in values.split(",")]


def _parse_packages(packages_str: str) -> Optional[list]:
    if packages_str is None or packages_str == "":
        return None
    return json.loads(packages_str.replace("'", '"'))


def _parse_storage_location(storage_location_str: str) -> Optional[dict]:
    if storage_location_str is None or storage_location_str == "":
        return None
    raw_dict = json.loads(storage_location_str)
    storage_location = {}
    for key, value in raw_dict.items():
        key = key.lower()
        if key == "encryption_type":
            storage_location["encryption"] = {"type": value}
        elif key in (
            "name",
            "storage_provider",
            "storage_base_url",
            "storage_aws_role_arn",
            "storage_aws_external_id",
        ):
            storage_location[key] = value
    return storage_location


def _cast_param_value(raw_value: str, param_type: str) -> Any:
    if param_type == "BOOLEAN":
        return raw_value == "true"
    elif param_type == "NUMBER":
        try:
            return int(raw_value)
        except ValueError:
            try:
                return float(raw_value)
            except ValueError:
                raise Exception(f"Unsupported number type: {raw_value}")
    elif param_type == "STRING":
        return str(raw_value) if raw_value else None
    else:
        return raw_value


def params_result_to_dict(params_result):
    params = {}
    for param in params_result:
        typed_value = _cast_param_value(param["value"], param["type"])
        params[param["key"].lower()] = typed_value
    return params


def options_result_to_list(options_result):
    return [option.strip(" ") for option in options_result.split(",")]


def remove_none_values(d):
    new_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            new_dict[k] = remove_none_values(v)
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            new_dict[k] = [remove_none_values(item) for item in v if item is not None]
        elif v is not None:
            new_dict[k] = v
    return new_dict


def _fetch_columns_for_table(session: SnowflakeConnection, fqn: FQN):
    info_schema_result = execute(session, f"SELECT * FROM {fqn.database}.INFORMATION_SCHEMA.COLUMNS", cacheable=True)
    columns = []
    for col in info_schema_result:

        if (
            resource_name_from_snowflake_metadata(col["TABLE_SCHEMA"]) != fqn.schema
            or resource_name_from_snowflake_metadata(col["TABLE_NAME"]) != fqn.name
        ):
            continue

        data_type = None
        default = col["COLUMN_DEFAULT"]

        if col["DATA_TYPE"] == "NUMBER":
            data_type = f"NUMBER({col['NUMERIC_PRECISION']}, {col['NUMERIC_SCALE']})"
        elif col["DATA_TYPE"] == "TEXT":
            data_type = f"VARCHAR({col['CHARACTER_MAXIMUM_LENGTH']})"
            if col["COLUMN_DEFAULT"]:
                default = col["COLUMN_DEFAULT"].strip("'")
        else:
            data_type = col["DATA_TYPE"]

        columns.append(
            {
                "name": col["COLUMN_NAME"],
                "data_type": data_type,
                "not_null": col["IS_NULLABLE"] == "NO",
                "default": default,
                "comment": col["COMMENT"] or None,
                "constraint": None,
                "collate": None,
            }
        )
    return columns


def _fetch_owner(session: SnowflakeConnection, type_str: str, fqn: FQN) -> Optional[str]:
    show_grants = execute(session, f"SHOW GRANTS ON {type_str} {fqn}")
    ownership_grant = _filter_result(show_grants, privilege="OWNERSHIP")
    if len(ownership_grant) == 0:
        return None
    return ownership_grant[0]["grantee_name"]


def _show_resources(session: SnowflakeConnection, type_str, fqn: FQN, cacheable: bool = True) -> list[dict]:
    try:
        in_account = " IN ACCOUNT"
        if "INTEGRATIONS" in type_str:
            in_account = ""
        initial_fetch = execute(session, f"SHOW {type_str}{in_account}", cacheable=cacheable)
        if len(initial_fetch) == 0:
            return []
        elif len(initial_fetch) < 1000:
            container_kwargs = {}
            show_columns = initial_fetch[0].keys()
            if "database" in show_columns:
                container_kwargs["database"] = fqn.database
            elif "database_name" in show_columns:
                container_kwargs["database_name"] = fqn.database

            if "schema" in show_columns:
                container_kwargs["schema"] = fqn.schema
            elif "schema_name" in show_columns:
                container_kwargs["schema_name"] = fqn.schema
            filtered_fetch = _filter_result(
                initial_fetch,
                name=fqn.name,
                **container_kwargs,
            )
            return filtered_fetch
        else:
            name = str(fqn.name).replace('"', "")
            if fqn.database is None and fqn.schema is None:
                return execute(session, f"SHOW {type_str} LIKE '{name}'", cacheable=cacheable)
            elif fqn.database is None:
                return execute(session, f"SHOW {type_str} LIKE '{name}' IN SCHEMA {fqn.schema}", cacheable=cacheable)
            elif fqn.schema is None:
                return execute(
                    session, f"SHOW {type_str} LIKE '{name}' IN DATABASE {fqn.database}", cacheable=cacheable
                )
            else:
                return execute(
                    session,
                    f"SHOW {type_str} LIKE '{name}' IN SCHEMA {fqn.database}.{fqn.schema}",
                    cacheable=cacheable,
                )
    except ProgrammingError as err:
        if err.errno == OBJECT_DOES_NOT_EXIST_ERR or err.errno == DOES_NOT_EXIST_ERR:
            return []
        else:
            raise


def _show_resource_parameters(session: SnowflakeConnection, type_str: str, fqn: FQN, cacheable: bool = True) -> dict:
    result = execute(session, f"SHOW PARAMETERS IN {type_str} {fqn}", cacheable=cacheable)
    return params_result_to_dict(result)


def _show_users(session) -> list[dict]:
    # SHOW USERS requires the MANAGE GRANTS privilege
    # Other roles can see the list of users but don't get access to other metadata such as login_name.
    # This causes incorrect drift

    session_ctx = fetch_session(session)
    execution_role = None
    current_role = session_ctx["role"]

    eligible_roles = [ResourceName("SECURITYADMIN"), ResourceName("ACCOUNTADMIN")]
    if (
        "MANAGE GRANTS" in session_ctx["account_grant_map"]
        and len(session_ctx["account_grant_map"]["MANAGE GRANTS"]) > 0
    ):
        eligible_roles.extend(session_ctx["account_grant_map"]["MANAGE GRANTS"])

    if current_role in eligible_roles:
        return execute(session, "SHOW USERS", cacheable=True)
    else:
        execution_role = None
        for role in eligible_roles:
            if role in session_ctx["available_roles"]:
                execution_role = role
                break
        else:
            raise RuntimeError("Managing users requires the MANAGE GRANTS privilege")

        use_role(session, execution_role)
        users = execute(session, "SHOW USERS", cacheable=True)
        use_role(session, current_role)

    return users


def _get_account_privilege_roles(session: SnowflakeConnection) -> dict[str, list[ResourceName]]:
    grant_map: dict[str, list[ResourceName]] = {}
    grants = execute(session, "SHOW GRANTS ON ACCOUNT")
    for grant in grants:
        # Skip system grants
        if grant["granted_by"] == "":
            continue

        if grant["privilege"] in ["MANAGE GRANTS", "APPLY TAG"]:
            priv = grant["privilege"]
            role = resource_name_from_snowflake_metadata(grant["grantee_name"])
            if priv not in grant_map:
                grant_map[priv] = []
            grant_map[priv].append(role)
    return grant_map


def _show_grants_to_role(
    session: SnowflakeConnection,
    role: ResourceName,
    role_type: ResourceType = ResourceType.ROLE,
    cacheable: bool = False,
    use_account_usage: bool = False,
) -> list[dict[str, Any]]:
    """
    Get grants to a role, using ACCOUNT_USAGE cache when available.

    When use_account_usage is True and ACCOUNT_USAGE data is cached, filters the
    cached data instead of issuing a SHOW GRANTS command. Falls back to SHOW GRANTS
    when ACCOUNT_USAGE is unavailable or for database roles.

    Returns:
    {
        'created_on': datetime.datetime(2024, 2, 28, 20, 5, 32, 166000, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
        'privilege': 'USAGE',
        'granted_on': 'DATABASE',
        'name': 'STATIC_DATABASE',
        'granted_to': 'ROLE',
        'grantee_name': 'THATROLE',
        'grant_option': 'false',
        'granted_by': 'ACCOUNTADMIN'
    }
    """
    # Try to use ACCOUNT_USAGE cache for regular roles
    if use_account_usage and role_type == ResourceType.ROLE:
        session_id = id(session)
        if session_id in _ACCOUNT_USAGE_GRANTS_CACHE:
            # Filter cached grants by role name (case-insensitive)
            role_upper = str(role).upper()
            filtered_grants = [
                grant for grant in _ACCOUNT_USAGE_GRANTS_CACHE[session_id]
                if grant["grantee_name"].upper() == role_upper and grant["granted_to"] == "ROLE"
            ]
            logger.debug(f"Using ACCOUNT_USAGE cache for grants to role {role} ({len(filtered_grants)} grants)")
            return filtered_grants

    # Fall back to SHOW GRANTS
    grants = execute(
        session,
        f"SHOW GRANTS TO {role_type} {role}",
        cacheable=cacheable,
        empty_response_codes=[DOES_NOT_EXIST_ERR],
    )
    return grants


def _show_future_grants_to_role(
    session: SnowflakeConnection, role: ResourceName, cacheable: bool = False
) -> list[dict[str, Any]]:
    """
    {
        'created_on': datetime.datetime(2024, 2, 28, 20, 5, 32, 166000, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
        'privilege': 'USAGE',
        'grant_on': 'SCHEMA',
        'name': 'STATIC_DATABASE.<SCHEMA>',
        'grant_to': 'ROLE',
        'grantee_name': 'THATROLE',
        'grant_option': 'false'
    }
    """
    grants = execute(
        session,
        f"SHOW FUTURE GRANTS TO ROLE {role}",
        cacheable=cacheable,
        empty_response_codes=[DOES_NOT_EXIST_ERR],
    )
    for grant in grants:
        grant["granted_on"] = "DATABASE" if len(grant["name"].split(".")) == 2 else "SCHEMA"
    return grants


def use_secondary_roles(session: SnowflakeConnection, all: bool = False):
    """
    Set the secondary roles for the current session.
    """
    secondary_roles = "ALL" if all else "NONE"
    execute(session, f"USE SECONDARY ROLES {secondary_roles}")


def use_role(session: SnowflakeConnection, role_name: ResourceName):
    """
    Set the active role for the current session.
    """
    execute(session, f"USE ROLE {role_name}")


# Fields that come from SHOW PARAMETERS queries (expensive to fetch)
# If manifest doesn't specify these fields, we can skip the SHOW PARAMETERS query
# Only includes fields that are actually returned by the fetch functions
PARAMETER_FIELDS = {
    "database": {"max_data_extension_time_in_days", "external_volume", "catalog", "default_ddl_collation"},
    "schema": {"max_data_extension_time_in_days", "default_ddl_collation"},
    "user": {"network_policy"},
    "warehouse": {"max_concurrency_level", "statement_queued_timeout_in_seconds", "statement_timeout_in_seconds"},
    "table": {"default_ddl_collation"},
    "task": {"suspend_task_after_num_failures", "user_task_managed_initial_warehouse_size", "user_task_timeout_ms"},
    "iceberg_table": {
        "catalog_sync",
        "storage_serialization_policy",
        "data_retention_time_in_days",
        "max_data_extension_time_in_days",
        "default_ddl_collation",
    },
}


def fetch_resource(session: SnowflakeConnection, urn: URN, include_params: bool = True, existence_only: bool = False) -> Optional[dict]:
    """
    Fetch a resource from Snowflake.

    Args:
        session: Snowflake connection
        urn: Resource URN
        include_params: If False, skip expensive SHOW PARAMETERS queries.
                       Use False when manifest doesn't specify parameter fields.
        existence_only: If True, only check if resource exists (skip detailed queries like DESC USER).
                       Use True for reference validation where we just need to verify existence.
    """
    try:
        func_name = f"fetch_{urn.resource_label}"
        fetch_fn = getattr(__this__, func_name)
        # Check which optional parameters the fetch function accepts (cached)
        sig = _get_cached_signature(func_name)
        kwargs = {}
        if "include_params" in sig.parameters:
            kwargs["include_params"] = include_params
        if "existence_only" in sig.parameters:
            kwargs["existence_only"] = existence_only
        if kwargs:
            return fetch_fn(session, urn.fqn, **kwargs)
        else:
            return fetch_fn(session, urn.fqn)
    except ProgrammingError as err:
        # This try/catch block fixes a cache-inconsistency issue where _show_resources returns the object as it existed at the start of the cache window,
        # but _show_resource_parameters returns the object as it exists right now. If the object was dropped in between the cache window and the query execution,
        # we should assume the database no longer exists.

        # This is only likely to happen for long-running commands like export
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise


def fetch_account_locator(session: SnowflakeConnection):
    locator = execute(session, "SELECT CURRENT_ACCOUNT() as account_locator")[0]["ACCOUNT_LOCATOR"]
    return locator


def fetch_region(session: SnowflakeConnection):
    region = execute(session, "SELECT CURRENT_REGION()")[0]
    return region


@cache
def fetch_session(session: SnowflakeConnection) -> SessionContext:
    session_obj = execute(
        session,
        """
        SELECT
            CURRENT_ACCOUNT_NAME() as account,
            CURRENT_ACCOUNT() as account_locator,
            CURRENT_USER() as user,
            CURRENT_ROLE() as role,
            CURRENT_AVAILABLE_ROLES() as available_roles,
            CURRENT_SECONDARY_ROLES() as secondary_roles,
            CURRENT_DATABASE() as database,
            CURRENT_SCHEMAS() as schemas,
            CURRENT_WAREHOUSE() as warehouse,
            CURRENT_VERSION() as version,
            CURRENT_REGION() as region,
            SYSTEM$BOOTSTRAP_DATA_REQUEST('ACCOUNT') as account_data
        """,
    )[0]

    account_data = json.loads(session_obj["ACCOUNT_DATA"])
    available_roles = [ResourceName(role) for role in json.loads(session_obj["AVAILABLE_ROLES"])]
    region = parse_region(session_obj["REGION"])
    account_grant_map = _get_account_privilege_roles(session)

    return {
        "account_edition": AccountEdition(account_data["accountInfo"]["serviceLevelName"]),
        "account_grant_map": account_grant_map,
        "account_locator": session_obj["ACCOUNT_LOCATOR"],
        "account": session_obj["ACCOUNT"],
        "available_roles": available_roles,
        "cloud": region["cloud"],
        "cloud_region": region["cloud_region"],
        "database": session_obj["DATABASE"],
        "role": ResourceName(session_obj["ROLE"]),
        "schemas": json.loads(session_obj["SCHEMAS"]),
        "secondary_roles": json.loads(session_obj["SECONDARY_ROLES"]),
        "user": session_obj["USER"],
        "version": session_obj["VERSION"],
        "warehouse": session_obj["WAREHOUSE"],
    }


def fetch_role_privileges(
    session: SnowflakeConnection,
    roles: list[ResourceName],
    cacheable: bool = True,
    use_account_usage: bool = False,
) -> dict[ResourceName, list[GrantedPrivilege]]:
    role_privileges: dict[ResourceName, list[GrantedPrivilege]] = {}

    # Filter out roles we skip (ACCOUNTADMIN and SNOWFLAKE.* roles)
    processable_roles = [
        role for role in roles if role != "ACCOUNTADMIN" and not role.startswith("SNOWFLAKE.")
    ]

    # Initialize empty lists for all processable roles
    for role in processable_roles:
        role_privileges[role] = []

    if not processable_roles:
        return role_privileges

    # Try ACCOUNT_USAGE if enabled and accessible
    if _should_use_account_usage(session, use_account_usage):
        logger.debug("fetch_role_privileges: Using ACCOUNT_USAGE for role privileges")
        role_name_set = {role.upper() for role in processable_roles}
        all_grants = _fetch_grants_from_account_usage(session)

        # If ACCOUNT_USAGE query failed, fall back to SHOW queries
        if all_grants is not None:
            for grant in all_grants:
                grantee_name = grant["grantee_name"]
                # Only process grants for roles we care about (case-insensitive)
                if grantee_name.upper() not in role_name_set:
                    continue

                # Find the original role name (preserve case)
                role_match = next(
                    (role for role in processable_roles if role.upper() == grantee_name.upper()), None
                )
                if role_match is None:
                    continue

                try:
                    granted_priv = GrantedPrivilege.from_grant(
                        privilege=grant["privilege"],
                        granted_on=grant["granted_on"].replace("_", " "),
                        name=grant["name"],
                    )
                    role_privileges[role_match].append(granted_priv)
                # If snowcap isnt aware of the privilege, ignore it
                except ValueError:
                    continue

            return role_privileges
        # Fall through to SHOW queries if ACCOUNT_USAGE failed

    # Fallback: Use SHOW GRANTS for each role
    logger.debug("fetch_role_privileges: Using SHOW GRANTS for role privileges")
    for role in processable_roles:
        grants = _show_grants_to_role(session, role, cacheable=cacheable)
        for grant in grants:
            try:
                granted_priv = GrantedPrivilege.from_grant(
                    privilege=grant["privilege"],
                    granted_on=grant["granted_on"].replace("_", " "),
                    name=grant["name"],
                )
                role_privileges[role].append(granted_priv)
            # If snowcap isnt aware of the privilege, ignore it
            except ValueError:
                continue
    return role_privileges


# ------------------------------
# ACCOUNT_USAGE Access Check
# ------------------------------

# Cache for ACCOUNT_USAGE access check results (keyed by session id)
_ACCOUNT_USAGE_ACCESS_CACHE: dict[int, bool] = {}

# Cache for tracking sessions where ACCOUNT_USAGE queries failed at runtime
# (distinct from permission errors - these are unexpected query failures)
# When True, that session should fall back to SHOW queries
_ACCOUNT_USAGE_FALLBACK_CACHE: dict[int, bool] = {}

# Cache for ACCOUNT_USAGE grants data (keyed by session id)
# Stores the normalized grant list from GRANTS_TO_ROLES
_ACCOUNT_USAGE_GRANTS_CACHE: dict[int, list[dict[str, Any]]] = {}

# Cache for ACCOUNT_USAGE role-to-user grants (keyed by session id)
# Stores the normalized grant list from GRANTS_TO_USERS
_ACCOUNT_USAGE_USER_GRANTS_CACHE: dict[int, list[dict[str, Any]]] = {}


def reset_account_usage_caches() -> None:
    """
    Clear all ACCOUNT_USAGE caches.

    This should be called when you need to force a fresh query of grant data,
    such as after applying changes that create new grants.
    """
    global _ACCOUNT_USAGE_ACCESS_CACHE, _ACCOUNT_USAGE_FALLBACK_CACHE
    global _ACCOUNT_USAGE_GRANTS_CACHE, _ACCOUNT_USAGE_USER_GRANTS_CACHE

    _ACCOUNT_USAGE_ACCESS_CACHE.clear()
    _ACCOUNT_USAGE_FALLBACK_CACHE.clear()
    _ACCOUNT_USAGE_GRANTS_CACHE.clear()
    _ACCOUNT_USAGE_USER_GRANTS_CACHE.clear()


def _mark_account_usage_fallback(session: SnowflakeConnection) -> None:
    """
    Mark that ACCOUNT_USAGE queries failed for this session and we should fall back to SHOW queries.
    This is used when a query unexpectedly fails (not permission error).
    """
    session_id = id(session)
    _ACCOUNT_USAGE_FALLBACK_CACHE[session_id] = True
    logger.warning("ACCOUNT_USAGE query failed - falling back to SHOW queries for this session")


def _should_use_account_usage(session: SnowflakeConnection, use_account_usage: bool) -> bool:
    """
    Determine whether to use ACCOUNT_USAGE queries for grants.

    Checks:
    1. use_account_usage config flag is True
    2. Session has ACCOUNT_USAGE access (IMPORTED PRIVILEGES)
    3. Session hasn't had previous ACCOUNT_USAGE query failures

    Returns:
        True if ACCOUNT_USAGE should be used, False otherwise.
    """
    if not use_account_usage:
        return False

    session_id = id(session)
    if _ACCOUNT_USAGE_FALLBACK_CACHE.get(session_id, False):
        logger.debug("Skipping ACCOUNT_USAGE: previous query failure for this session")
        return False

    return _has_account_usage_access(session)


def populate_account_usage_caches(session: SnowflakeConnection) -> bool:
    """
    Pre-populate ACCOUNT_USAGE caches for grants data.

    This should be called early in the process when use_account_usage is True
    to ensure the caches are populated before individual fetch functions are called.

    Returns:
        True if caches were populated successfully, False otherwise.
    """
    session_id = id(session)

    # Skip if already populated
    if session_id in _ACCOUNT_USAGE_GRANTS_CACHE and session_id in _ACCOUNT_USAGE_USER_GRANTS_CACHE:
        return True

    # Populate GRANTS_TO_ROLES cache
    grants = _fetch_grants_from_account_usage(session)
    if grants is None:
        return False

    # Populate GRANTS_TO_USERS cache
    user_grants = _fetch_role_grants_to_users_from_account_usage(session)
    if user_grants is None:
        return False

    logger.debug(f"Pre-populated ACCOUNT_USAGE caches: {len(grants)} role grants, {len(user_grants)} user grants")
    return True


def _has_account_usage_access(session: SnowflakeConnection) -> bool:
    """
    Check if the current session has IMPORTED PRIVILEGES on the SNOWFLAKE database.
    This is required to query ACCOUNT_USAGE views.

    Result is cached for the session to avoid repeated permission checks.

    Returns:
        True if query against ACCOUNT_USAGE succeeds, False if permission error.
    """
    session_id = id(session)
    if session_id in _ACCOUNT_USAGE_ACCESS_CACHE:
        return _ACCOUNT_USAGE_ACCESS_CACHE[session_id]

    try:
        execute(
            session,
            "SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES LIMIT 1",
            cacheable=True,
        )
        _ACCOUNT_USAGE_ACCESS_CACHE[session_id] = True
        logger.debug("ACCOUNT_USAGE access check: access granted")
        return True
    except ProgrammingError as err:
        if err.errno == ACCESS_CONTROL_ERR:
            logger.debug("ACCOUNT_USAGE access check: access denied (missing IMPORTED PRIVILEGES)")
            _ACCOUNT_USAGE_ACCESS_CACHE[session_id] = False
            return False
        # Re-raise unexpected errors
        raise


# ------------------------------
# ACCOUNT_USAGE Grant Fetching
# ------------------------------


def _fetch_grants_from_account_usage(session: SnowflakeConnection) -> list[dict[str, Any]] | None:
    """
    Fetch all role grants from SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES in a single query.

    Returns a list of grant dictionaries with keys matching SHOW GRANTS output:
        - created_on: datetime when grant was created
        - privilege: name of the privilege
        - granted_on: object type (e.g., 'DATABASE', 'TABLE')
        - name: object name
        - granted_to: grantee type ('ROLE' or 'DATABASE_ROLE')
        - grantee_name: name of the role receiving the grant
        - grant_option: whether grant can be passed to others ('true' or 'false')
        - granted_by: role that granted the privilege

    Note: ACCOUNT_USAGE returns uppercase column names and slightly different values
    (e.g., 'ACCOUNT ROLE' instead of 'ROLE'). This function normalizes the output
    to match the SHOW GRANTS structure.

    Results are cached per session to avoid repeated queries.

    Returns:
        List of grant dictionaries, or None if the query fails (signaling fallback needed).
    """
    # Check cache first
    session_id = id(session)
    if session_id in _ACCOUNT_USAGE_GRANTS_CACHE:
        return _ACCOUNT_USAGE_GRANTS_CACHE[session_id]

    query = """
        SELECT
            CREATED_ON,
            PRIVILEGE,
            GRANTED_ON,
            NAME,
            TABLE_CATALOG,
            TABLE_SCHEMA,
            GRANTED_TO,
            GRANTEE_NAME,
            GRANT_OPTION,
            GRANTED_BY
        FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
        WHERE DELETED_ON IS NULL
    """
    try:
        results = execute(session, query, cacheable=True)
    except ProgrammingError as err:
        if err.errno == ACCESS_CONTROL_ERR:
            logger.warning("ACCOUNT_USAGE query failed: access denied - falling back to SHOW queries")
        else:
            logger.warning(f"ACCOUNT_USAGE query failed with error {err.errno}: {err.msg} - falling back to SHOW queries")
        _mark_account_usage_fallback(session)
        return None
    except Exception as err:
        logger.warning(f"ACCOUNT_USAGE query failed unexpectedly: {err} - falling back to SHOW queries")
        _mark_account_usage_fallback(session)
        return None

    # Normalize results to match SHOW GRANTS structure
    normalized_grants = []
    for row in results:
        # ACCOUNT_USAGE returns 'ACCOUNT ROLE' but SHOW returns 'ROLE'
        # Similarly 'DATABASE_ROLE' -> 'DATABASE ROLE' for consistency
        granted_to = row["GRANTED_TO"]
        if granted_to == "ACCOUNT ROLE":
            granted_to = "ROLE"
        elif granted_to == "DATABASE_ROLE":
            granted_to = "DATABASE ROLE"

        # GRANT_OPTION is boolean in ACCOUNT_USAGE but string in SHOW
        grant_option = "true" if row["GRANT_OPTION"] else "false"

        # Construct fully qualified name to match SHOW GRANTS output
        # ACCOUNT_USAGE NAME column only has object name, not full path
        granted_on = row["GRANTED_ON"]
        name = row["NAME"]
        table_catalog = row.get("TABLE_CATALOG")
        table_schema = row.get("TABLE_SCHEMA")

        if granted_on == "ACCOUNT":
            # Account grants don't need qualification
            pass
        elif granted_on == "DATABASE":
            # Database grants: NAME is already the database name
            pass
        elif granted_on == "SCHEMA":
            # Schema grants: need DATABASE.SCHEMA
            if table_catalog:
                name = f"{table_catalog}.{name}"
        elif granted_on in ("DATABASE ROLE",):
            # Database role grants: need DATABASE.ROLE
            if table_catalog:
                name = f"{table_catalog}.{name}"
        else:
            # Schema-scoped objects (TABLE, VIEW, FUNCTION, etc.): need DATABASE.SCHEMA.OBJECT
            if table_catalog and table_schema:
                name = f"{table_catalog}.{table_schema}.{name}"

        normalized_grants.append(
            {
                "created_on": row["CREATED_ON"],
                "privilege": row["PRIVILEGE"],
                "granted_on": granted_on,
                "name": name,
                "granted_to": granted_to,
                "grantee_name": row["GRANTEE_NAME"],
                "grant_option": grant_option,
                "granted_by": row["GRANTED_BY"],
            }
        )

    logger.debug(f"Fetched {len(normalized_grants)} grants from ACCOUNT_USAGE.GRANTS_TO_ROLES")

    # Cache the results
    _ACCOUNT_USAGE_GRANTS_CACHE[session_id] = normalized_grants

    # Also mark that we have ACCOUNT_USAGE access (query succeeded)
    _ACCOUNT_USAGE_ACCESS_CACHE[session_id] = True

    return normalized_grants


def _fetch_role_grants_to_users_from_account_usage(session: SnowflakeConnection) -> list[dict[str, Any]] | None:
    """
    Fetch all role-to-user grants from SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS in a single query.

    Returns a list of grant dictionaries with keys matching SHOW GRANTS OF ROLE output:
        - created_on: datetime when grant was created
        - role: name of the role being granted
        - granted_to: always 'USER'
        - grantee_name: name of the user receiving the grant
        - granted_by: role that granted the privilege

    Results are cached per session to avoid repeated queries.

    Returns:
        List of grant dictionaries, or None if the query fails (signaling fallback needed).
    """
    # Check cache first
    session_id = id(session)
    if session_id in _ACCOUNT_USAGE_USER_GRANTS_CACHE:
        return _ACCOUNT_USAGE_USER_GRANTS_CACHE[session_id]

    query = """
        SELECT
            CREATED_ON,
            ROLE,
            GRANTED_TO,
            GRANTEE_NAME,
            GRANTED_BY
        FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
        WHERE DELETED_ON IS NULL
    """
    try:
        results = execute(session, query, cacheable=True)
    except ProgrammingError as err:
        if err.errno == ACCESS_CONTROL_ERR:
            logger.warning("ACCOUNT_USAGE GRANTS_TO_USERS query failed: access denied - falling back to SHOW queries")
        else:
            logger.warning(f"ACCOUNT_USAGE GRANTS_TO_USERS query failed with error {err.errno}: {err.msg} - falling back to SHOW queries")
        _mark_account_usage_fallback(session)
        return None
    except Exception as err:
        logger.warning(f"ACCOUNT_USAGE GRANTS_TO_USERS query failed unexpectedly: {err} - falling back to SHOW queries")
        _mark_account_usage_fallback(session)
        return None

    # Normalize results to match SHOW GRANTS OF ROLE structure
    normalized_grants = []
    for row in results:
        normalized_grants.append(
            {
                "created_on": row["CREATED_ON"],
                "role": row["ROLE"],
                "granted_to": row["GRANTED_TO"],
                "grantee_name": row["GRANTEE_NAME"],
                "granted_by": row["GRANTED_BY"],
            }
        )

    logger.debug(f"Fetched {len(normalized_grants)} role grants to users from ACCOUNT_USAGE.GRANTS_TO_USERS")

    # Cache the results
    _ACCOUNT_USAGE_USER_GRANTS_CACHE[session_id] = normalized_grants

    return normalized_grants


def _fetch_future_grants_for_all_roles(
    session: SnowflakeConnection, role_names: list[ResourceName]
) -> dict[str, list[dict[str, Any]]]:
    """
    Fetch future grants for all specified roles using SHOW FUTURE GRANTS commands.

    Note: Future grants are NOT available in SNOWFLAKE.ACCOUNT_USAGE views.
    They represent templates for privileges that will be applied when new objects
    are created, not actual granted privileges. Therefore, SHOW FUTURE GRANTS
    commands must be used.

    Args:
        session: Snowflake connection
        role_names: List of role names to fetch future grants for

    Returns:
        Dictionary mapping role names (as strings) to their list of future grant dicts.
        Each future grant dict has keys matching _show_future_grants_to_role() output:
            - created_on: datetime when future grant was created
            - privilege: name of the privilege
            - grant_on: 'SCHEMA' (from SHOW output)
            - granted_on: 'DATABASE' or 'SCHEMA' (inferred from name pattern)
            - name: object pattern (e.g., 'DB_NAME.<SCHEMA>' or 'DB_NAME.SCHEMA_NAME.<TABLE>')
            - grant_to: 'ROLE'
            - grantee_name: name of the role receiving the future grant
            - grant_option: 'true' or 'false'
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    future_grants_by_role: dict[str, list[dict[str, Any]]] = {}

    def fetch_for_role(role_name: ResourceName) -> tuple[str, list[dict[str, Any]]]:
        grants = _show_future_grants_to_role(session, role_name, cacheable=True)
        return str(role_name), grants

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_for_role, role_name): role_name for role_name in role_names}
        for future in as_completed(futures):
            role_name_str, grants = future.result()
            future_grants_by_role[role_name_str] = grants

    total_grants = sum(len(g) for g in future_grants_by_role.values())
    logger.debug(f"Fetched {total_grants} future grants for {len(role_names)} roles using SHOW FUTURE GRANTS")
    return future_grants_by_role


# ------------------------------
# Fetch Resources
# ------------------------------


def fetch_account(session: SnowflakeConnection, fqn: FQN):
    return {
        "name": None,
        "locator": None,
    }


def fetch_account_parameter(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, "SHOW PARAMETERS IN ACCOUNT", cacheable=True)
    account_parameters = _filter_result(show_result, key=fqn.name, level="ACCOUNT")
    if len(account_parameters) == 0:
        return None
    if len(account_parameters) > 1:
        raise Exception(f"Found multiple account parameters matching {fqn}")
    data = account_parameters[0]
    return {
        "name": ResourceName(data["key"]),
        "value": _cast_param_value(data["value"], data["type"]),
    }


def fetch_aggregation_policy(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "AGGREGATION POLICIES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple aggregation policies matching {fqn}")
    data = show_result[0]
    desc_result = execute(session, f"DESC AGGREGATION POLICY {fqn}")
    properties = desc_result[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "body": properties["body"],
        "owner": _get_owner_identifier(data),
    }


def fetch_alert(session: SnowflakeConnection, fqn: FQN):
    alerts = _show_resources(session, "ALERTS", fqn)
    if len(alerts) == 0:
        return None
    if len(alerts) > 1:
        raise Exception(f"Found multiple alerts matching {fqn}")
    data = alerts[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "warehouse": data["warehouse"],
        "schedule": data["schedule"],
        "comment": data["comment"] or None,
        "condition": data["condition"],
        "then": data["action"],
        "owner": _get_owner_identifier(data),
    }


def fetch_api_integration(session: SnowflakeConnection, fqn: FQN):
    integrations = _show_resources(session, "API INTEGRATIONS", fqn)
    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple api integrations matching {fqn}")
    data = integrations[0]
    desc_result = execute(session, f"DESC API INTEGRATION {fqn}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "api_provider": properties["api_provider"],
        "api_aws_role_arn": properties["api_aws_role_arn"],
        "enabled": properties["enabled"],
        "api_allowed_prefixes": properties["api_allowed_prefixes"],
        "api_blocked_prefixes": properties["api_blocked_prefixes"],
        "owner": owner,
        "comment": data["comment"] or None,
    }


def fetch_authentication_policy(session: SnowflakeConnection, fqn: FQN):
    policies = _show_resources(session, "AUTHENTICATION POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple authentication policies matching {fqn}")
    data = policies[0]
    desc_result = execute(session, f"DESC AUTHENTICATION POLICY {fqn}")
    properties = _desc_result_to_dict(desc_result, lower_properties=True)

    # mfa_authentication_methods is deprecated as of Snowflake 2025_06 bundle.
    # Snowflake returns a default value ['PASSWORD'] even when not set.
    # Return None when it's the default to avoid false drift detection.
    mfa_auth_methods = _parse_list_property(properties["mfa_authentication_methods"])
    if mfa_auth_methods == ["PASSWORD"]:
        mfa_auth_methods = None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "authentication_methods": _parse_list_property(properties["authentication_methods"]),
        "mfa_authentication_methods": mfa_auth_methods,
        "mfa_enrollment": properties["mfa_enrollment"],
        "client_types": _parse_list_property(properties["client_types"]),
        "security_integrations": _parse_list_property(properties["security_integrations"]),
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_catalog_integration(session: SnowflakeConnection, fqn: FQN):
    integrations = _show_resources(session, "CATALOG INTEGRATIONS", fqn)
    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple catalog integrations matching {fqn}")

    data = integrations[0]
    desc_result = execute(session, f"DESC CATALOG INTEGRATION {fqn}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if properties["catalog_source"] == "GLUE":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "catalog_source": properties["catalog_source"],
            "catalog_namespace": properties["catalog_namespace"],
            "table_format": properties["table_format"],
            "glue_aws_role_arn": properties["glue_aws_role_arn"],
            "glue_catalog_id": properties["glue_catalog_id"],
            "glue_region": properties["glue_region"],
            "enabled": properties["enabled"],
            "owner": owner,
            "comment": data["comment"] or None,
        }
    elif properties["catalog_source"] == "OBJECT_STORE":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "catalog_source": properties["catalog_source"],
            "table_format": properties["table_format"],
            "enabled": properties["enabled"],
            "owner": owner,
            "comment": data["comment"] or None,
        }
    else:
        raise Exception(f"Unsupported catalog integration: {properties['catalog_source']}")


def fetch_columns(session: SnowflakeConnection, resource_type: str, fqn: FQN):
    desc_result = execute(session, f"DESC {resource_type} {fqn}")
    columns = []
    for col in desc_result:
        if col["kind"] != "COLUMN":
            raise Exception(f"Unexpected kind {col['kind']} in desc result")
        columns.append(
            {
                "name": col["name"],
                "data_type": col["type"],
                "not_null": col["null?"] == "N",
                "default": col["default"],
                "comment": col["comment"] or None,
                "constraint": None,
                "collate": None,
            }
        )
    return columns


def fetch_compute_pool(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW COMPUTE POOLS LIKE '{fqn.name}'", cacheable=True)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple compute pools matching {fqn}")

    data = show_result[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "min_nodes": data["min_nodes"],
        "max_nodes": data["max_nodes"],
        "instance_family": data["instance_family"],
        "auto_resume": data["auto_resume"] == "true",
        "auto_suspend_secs": data["auto_suspend_secs"],
        "comment": data["comment"] or None,
    }


def fetch_database(session: SnowflakeConnection, fqn: FQN, include_params: bool = True):
    show_result = _show_resources(session, "DATABASES", fqn)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple databases matching {fqn}")

    data = show_result[0]

    is_standard_db = data["kind"] in ["STANDARD", "IMPORTED DATABASE"]
    is_snowflake_builtin = data["kind"] == "APPLICATION" and data["name"] in SYSTEM_DATABASES

    if not (is_standard_db or is_snowflake_builtin):
        return None

    options = options_result_to_list(data["options"])

    # Only fetch parameters if needed (expensive SHOW PARAMETERS query)
    if include_params:
        params = _show_resource_parameters(session, "DATABASE", fqn)
        max_data_extension = params.get("max_data_extension_time_in_days")
        external_volume = params.get("external_volume")
        catalog = params.get("catalog")
        default_ddl_collation = params["default_ddl_collation"]
    else:
        max_data_extension = None
        external_volume = None
        catalog = None
        default_ddl_collation = None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "data_retention_time_in_days": int(data["retention_time"]),
        "comment": data["comment"] or None,
        "transient": "TRANSIENT" in options,
        "owner": _get_owner_identifier(data),
        "max_data_extension_time_in_days": max_data_extension,
        "external_volume": external_volume,
        "catalog": catalog,
        "default_ddl_collation": default_ddl_collation,
    }


def fetch_database_role(session: SnowflakeConnection, fqn: FQN):
    try:
        show_result = execute(session, f"SHOW DATABASE ROLES IN DATABASE {fqn.database}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise

    roles = _filter_result(show_result, name=fqn.name)
    if len(roles) == 0:
        return None
    if len(roles) > 1:
        raise Exception(f"Found multiple database roles matching {fqn}")
    data = roles[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "database": fqn.database,
        "comment": data["comment"] or None,
    }


def fetch_database_role_grant(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW GRANTS OF DATABASE ROLE {fqn.database}.{fqn.name}", cacheable=True)

    subject, subject_name = next(iter(fqn.params.items()))

    role_grants = _filter_result(show_result, granted_to=subject.upper(), grantee_name=subject_name)
    if len(role_grants) == 0:
        return None
    if len(role_grants) > 1:
        raise Exception(f"Found multiple database role grants matching {fqn}")

    data = show_result[0]

    to_role = None
    to_database_role = None
    if data["granted_to"] == "ROLE":
        to_role = _quote_snowflake_identifier(data["grantee_name"])
    elif data["granted_to"] == "DATABASE_ROLE":
        to_database_role = data["grantee_name"]

    return {
        "database_role": data["role"],
        "to_role": to_role,
        "to_database_role": to_database_role,
    }


def fetch_dynamic_table(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "DYNAMIC TABLES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple dynamic tables matching {fqn}")

    columns = fetch_columns(session, "DYNAMIC TABLE", fqn)
    columns = [{"name": col["name"], "comment": col["comment"]} for col in columns]

    data = show_result[0]
    refresh_mode, initialize, as_ = _parse_dynamic_table_text(data["text"])
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "warehouse": data["warehouse"],
        "refresh_mode": refresh_mode,
        "initialize": initialize,
        "target_lag": data["target_lag"],
        "comment": data["comment"] or None,
        "columns": columns,
        "as_": as_,
    }


def fetch_event_table(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, "SHOW EVENT TABLES IN ACCOUNT")

    tables = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)

    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple tables matching {fqn}")

    data = tables[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "comment": data["comment"] or None,
        "cluster_by": _parse_cluster_keys(data["cluster_by"]),
        "data_retention_time_in_days": int(data["retention_time"]),
        "change_tracking": data["change_tracking"] == "ON",
        "owner": _get_owner_identifier(data),
    }


def fetch_external_access_integration(session: SnowflakeConnection, fqn: FQN):
    integrations = _show_resources(session, "EXTERNAL ACCESS INTEGRATIONS", fqn)
    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple external access integrations matching {fqn}")

    data = integrations[0]
    desc_result = execute(session, f"DESC EXTERNAL ACCESS INTEGRATION {fqn}", cacheable=True)
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "allowed_network_rules": properties["allowed_network_rules"],
        "allowed_api_authentication_integrations": properties["allowed_api_authentication_integrations"] or None,
        "allowed_authentication_secrets": properties["allowed_authentication_secrets"] or None,
        "enabled": data["enabled"] == "true",
        "owner": owner,
        "comment": data["comment"] or None,
    }


def fetch_external_volume(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "EXTERNAL VOLUMES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple external volumes matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC EXTERNAL VOLUME {fqn}", cacheable=True)
    properties = _desc_type3_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "VOLUME", fqn)

    storage_locations = []
    index = 1
    while True:
        storage_location = properties["storage_locations"].get(f"storage_location_{index}")
        if storage_location is None:
            break
        storage_locations.append(_parse_storage_location(storage_location))
        index += 1

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": owner,
        "storage_locations": storage_locations,
        "allow_writes": data["allow_writes"] == "true",
        "comment": data["comment"] or None,
    }


def fetch_file_format(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "FILE FORMATS", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple file formats matching {fqn}")

    data = show_result[0]
    format_options = json.loads(data["format_options"])

    if data["type"] == "CSV":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "owner": _get_owner_identifier(data),
            "field_delimiter": format_options["FIELD_DELIMITER"],
            "skip_header": format_options["SKIP_HEADER"],
            "null_if": format_options["NULL_IF"] or None,
            "empty_field_as_null": format_options["EMPTY_FIELD_AS_NULL"],
            "compression": format_options["COMPRESSION"],
            "record_delimiter": format_options["RECORD_DELIMITER"],
            "file_extension": format_options["FILE_EXTENSION"],
            "parse_header": format_options["PARSE_HEADER"],
            "skip_blank_lines": format_options["SKIP_BLANK_LINES"],
            "date_format": format_options["DATE_FORMAT"],
            "time_format": format_options["TIME_FORMAT"],
            "timestamp_format": format_options["TIMESTAMP_FORMAT"],
            "binary_format": format_options["BINARY_FORMAT"],
            "escape": format_options["ESCAPE"] if format_options["ESCAPE"] != "NONE" else None,
            "escape_unenclosed_field": format_options["ESCAPE_UNENCLOSED_FIELD"],
            "trim_space": format_options["TRIM_SPACE"],
            "field_optionally_enclosed_by": (
                format_options["FIELD_OPTIONALLY_ENCLOSED_BY"]
                if format_options["FIELD_OPTIONALLY_ENCLOSED_BY"] != "NONE"
                else None
            ),
            "error_on_column_count_mismatch": format_options["ERROR_ON_COLUMN_COUNT_MISMATCH"],
            "replace_invalid_characters": format_options["REPLACE_INVALID_CHARACTERS"],
            "skip_byte_order_mark": format_options["SKIP_BYTE_ORDER_MARK"],
            "encoding": format_options["ENCODING"],
            "comment": data["comment"] or None,
        }
    elif data["type"] == "PARQUET":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "owner": _get_owner_identifier(data),
            "comment": data["comment"] or None,
            "compression": format_options["COMPRESSION"],
            "binary_as_text": format_options["BINARY_AS_TEXT"],
            "trim_space": format_options["TRIM_SPACE"],
            "replace_invalid_characters": format_options["REPLACE_INVALID_CHARACTERS"],
            "null_if": format_options["NULL_IF"] or None,
        }
    elif data["type"] == "JSON":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "owner": _get_owner_identifier(data),
            "comment": data["comment"] or None,
            "compression": format_options["COMPRESSION"],
            "date_format": format_options["DATE_FORMAT"],
            "time_format": format_options["TIME_FORMAT"],
            "timestamp_format": format_options["TIMESTAMP_FORMAT"],
            "binary_format": format_options["BINARY_FORMAT"],
            "trim_space": format_options["TRIM_SPACE"],
            "null_if": format_options["NULL_IF"] or None,
            "file_extension": format_options["FILE_EXTENSION"],
            "enable_octal": format_options["ENABLE_OCTAL"],
            "allow_duplicate": format_options["ALLOW_DUPLICATE"],
            "strip_outer_array": format_options["STRIP_OUTER_ARRAY"],
            "strip_null_values": format_options["STRIP_NULL_VALUES"],
            "replace_invalid_characters": format_options["REPLACE_INVALID_CHARACTERS"],
            "ignore_utf8_errors": format_options["IGNORE_UTF8_ERRORS"],
            "skip_byte_order_mark": format_options["SKIP_BYTE_ORDER_MARK"],
        }
    else:
        raise Exception(f"Unsupported file format type: {data['type']}")


def fetch_function(session: SnowflakeConnection, fqn: FQN):
    udfs = _show_resources(session, "USER FUNCTIONS", fqn)
    if len(udfs) == 0:
        return None
    if len(udfs) > 1:
        raise Exception(f"Found multiple functions matching {fqn}")

    data = udfs[0]
    _, returns = data["arguments"].split(" RETURN ")
    try:
        desc_result = execute(session, f"DESC FUNCTION {fqn}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise
    properties = _desc_result_to_dict(desc_result)
    owner = _fetch_owner(session, "FUNCTION", fqn)

    if data["language"] == "PYTHON":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secure": data["is_secure"] == "Y",
            "args": _parse_signature(properties["signature"]),
            "returns": returns,
            "language": data["language"],
            "comment": None if data["description"] == "user-defined function" else data["description"],
            "volatility": properties["volatility"],
            "as_": properties["body"],
            "owner": owner,
        }
    elif data["language"] == "JAVASCRIPT":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secure": data["is_secure"] == "Y",
            "args": _parse_signature(properties["signature"]),
            "returns": returns,
            "language": data["language"],
            "comment": None if data["description"] == "user-defined function" else data["description"],
            "volatility": properties["volatility"],
            "as_": properties["body"],
            "owner": owner,
        }


def fetch_grant(session: SnowflakeConnection, fqn: FQN):
    priv = fqn.params["priv"]
    on_type, on = fqn.params["on"].split("/", 1)
    on_type = on_type.upper()
    to_type, to = fqn.params["to"].split("/", 1)
    to_type = resource_type_for_label(to_type)
    # Default to OBJECT grant type if not specified
    grant_type = fqn.params.get("grant_type", GrantType.OBJECT)

    if priv == "ALL":

        filters = {
            "granted_on": on_type,
        }

        if on_type != "ACCOUNT":
            filters["name"] = on

        grants = _show_grants_to_role(session, to, role_type=to_type, cacheable=True)
        grants = _filter_result(grants, **filters)

        if len(grants) == 0:
            return None

        data = grants[0]
        privs = sorted([g["privilege"] for g in grants])

    else:
        data = _fetch_grant_to_role(
            session,
            grant_type=grant_type,
            role=to,
            granted_on=on_type,
            on_name=on,
            privilege=priv,
            role_type=to_type,
        )
        if data is None:
            return None
        privs = [priv]

    # elif len(grants) > 1 and priv != "ALL":
    #     # This is likely to happen when a grant has been issued by ACCOUNTADMIN
    #     # and some other role with MANAGE GRANTS or OWNERSHIP. It needs to be properly
    #     # handled in the future.
    #     raise Exception(f"Found multiple grants matching {fqn}")

    items_type = None
    if grant_type == GrantType.FUTURE:
        collection = parse_collection_string(on)
        items_type = collection["items_type"].upper()
        on_type = collection["on_type"]
        on = collection["on"]
        to_type = resource_type_for_label(data["grant_to"])
        owner = ""
    else:
        to_type = resource_type_for_label(data["granted_to"])
        owner = data["granted_by"]

    return {
        "priv": priv,
        "on": "ACCOUNT" if on_type == "ACCOUNT" else on,
        "on_type": on_type.replace("_", " "),
        "to": to,
        "to_type": to_type,
        "grant_option": data["grant_option"] == "true",
        "owner": owner,
        "_privs": privs,
        "items_type": items_type.replace("_", " ") if items_type else None,
        "grant_type": grant_type,
    }


def fetch_grant_on_all(session: SnowflakeConnection, fqn: FQN):
    # All grants are expensive to fetch, so we will assume they are always out of date
    return None


def fetch_iceberg_table(session: SnowflakeConnection, fqn: FQN, include_params: bool = True):
    tables = _show_resources(session, "ICEBERG TABLES", fqn)
    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple iceberg tables matching {fqn}")

    data = tables[0]
    columns = fetch_columns(session, "ICEBERG TABLE", fqn)

    # Only fetch parameters if needed (expensive query)
    if include_params:
        show_params_result = execute(session, f"SHOW PARAMETERS FOR TABLE {fqn}")
        params = params_result_to_dict(show_params_result)
    else:
        params = {}

    return {
        "name": fqn.name,
        "owner": data["owner"],
        "columns": columns,
        "external_volume": data["external_volume_name"],
        "catalog": data["catalog_name"],
        "base_location": data["base_location"].rstrip("/"),
        "catalog_sync": params.get("catalog_sync") or None,
        "storage_serialization_policy": params.get("storage_serialization_policy"),
        "data_retention_time_in_days": params.get("data_retention_time_in_days"),
        "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days"),
        # "change_tracking": data["change_tracking"],
        "default_ddl_collation": params.get("default_ddl_collation") or None,
        "comment": data["comment"] or None,
    }


def fetch_image_repository(session: SnowflakeConnection, fqn: FQN):
    repos = _show_resources(session, "IMAGE REPOSITORIES", fqn)

    if len(repos) == 0:
        return None
    if len(repos) > 1:
        raise Exception(f"Found multiple image repositories matching {fqn}")

    data = repos[0]

    return {"name": fqn.name, "owner": _get_owner_identifier(data)}


def fetch_masking_policy(session: SnowflakeConnection, fqn: FQN):
    policies = _show_resources(session, "MASKING POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple masking policies matching {fqn}")

    data = policies[0]
    options = json.loads(data["options"]) if data["options"] else {}
    desc_result = execute(session, f"DESC MASKING POLICY {fqn}", cacheable=True)
    properties = desc_result[0]

    return {
        "name": data["name"],
        "owner": _get_owner_identifier(data),
        "args": _parse_signature(properties["signature"]),
        "returns": properties["return_type"],
        "body": properties["body"],
        "comment": data["comment"] or None,
        "exempt_other_policies": options.get("exempt_other_policies", "false") == "true",
    }


def fetch_materialized_view(session: SnowflakeConnection, fqn: FQN):
    materialized_views = _show_resources(session, "MATERIALIZED VIEWS", fqn)
    if len(materialized_views) == 0:
        return None
    if len(materialized_views) > 1:
        raise Exception(f"Found multiple materialized views matching {fqn}")

    data = materialized_views[0]
    columns = fetch_columns(session, "VIEW", fqn)

    return {
        "name": fqn.name,
        "owner": _get_owner_identifier(data),
        "secure": data["is_secure"] == "true",
        "columns": columns,
        "cluster_by": _parse_cluster_keys(data["cluster_by"]),
        "comment": data["comment"] or None,
        "as_": parse_view_ddl(data["text"]),
    }


def fetch_network_policy(session: SnowflakeConnection, fqn: FQN):
    policies = _show_resources(session, "NETWORK POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple network policies matching {fqn}")

    data = policies[0]
    desc_result = execute(session, f"DESC NETWORK POLICY {fqn}", cacheable=True)
    properties = _desc_type4_result_to_dict(desc_result, lower_properties=True)

    allowed_network_rule_list = None
    if "allowed_network_rule_list" in properties:
        allowed_network_rule_list = [
            rule["fullyQualifiedRuleName"] for rule in json.loads(properties["allowed_network_rule_list"])
        ]
    blocked_network_rule_list = None
    if "blocked_network_rule_list" in properties:
        blocked_network_rule_list = [
            rule["fullyQualifiedRuleName"] for rule in json.loads(properties["blocked_network_rule_list"])
        ]
    allowed_ip_list = None
    if "allowed_ip_list" in properties:
        allowed_ip_list = properties["allowed_ip_list"].split(",")
    blocked_ip_list = None
    if "blocked_ip_list" in properties:
        blocked_ip_list = properties["blocked_ip_list"].split(",")

    owner = _fetch_owner(session, "NETWORK POLICY", fqn)

    return {
        "name": data["name"],
        "allowed_network_rule_list": allowed_network_rule_list,
        "blocked_network_rule_list": blocked_network_rule_list,
        "allowed_ip_list": allowed_ip_list,
        "blocked_ip_list": blocked_ip_list,
        "comment": data["comment"] or None,
        "owner": owner,
    }


def fetch_network_rule(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "NETWORK RULES", fqn)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple network rules matching {fqn}")

    desc_result = execute(session, f"DESC NETWORK RULE {fqn}", cacheable=True)
    properties = desc_result[0]

    data = show_result[0]
    return {
        "name": fqn.name,
        "owner": _get_owner_identifier(data),
        "type": data["type"],
        "value_list": _parse_comma_separated_values(properties["value_list"]),
        "mode": data["mode"],
        "comment": data["comment"] or None,
    }


def fetch_notebook(session: SnowflakeConnection, fqn: FQN):
    notebooks = _show_resources(session, "NOTEBOOKS", fqn)
    if len(notebooks) == 0:
        return None
    if len(notebooks) > 1:
        raise Exception(f"Found multiple notebooks matching {fqn}")

    data = notebooks[0]
    desc_result = execute(session, f"DESC NOTEBOOK {fqn}", cacheable=True)
    properties = desc_result[0]
    return {
        "name": data["name"],
        "main_file": None if properties["main_file"] == "notebook_app.ipynb" else properties["main_file"],
        "query_warehouse": data["query_warehouse"],
        "comment": data["comment"],
        "owner": _get_owner_identifier(data),
        # "version": data["version"],
    }


def fetch_notification_integration(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW NOTIFICATION INTEGRATIONS LIKE '{fqn.name}'")
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple notification integrations matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC NOTIFICATION INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)

    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if data["type"] == "EMAIL":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "allowed_recipients": properties["allowed_recipients"],
            "owner": owner,
            "comment": data["comment"] or None,
        }
    elif data["type"].startswith("QUEUE"):
        # QUEUE type notifications have format like "QUEUE - GCP_PUBSUB" or "QUEUE - AZURE_STORAGE_QUEUE"
        # The direction field may or may not exist depending on the notification type
        type_parts = data["type"].split(" - ")
        notification_provider = type_parts[1] if len(type_parts) > 1 else None
        direction = data.get("direction") or properties.get("direction", "INBOUND")

        base_result = {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": "QUEUE",
            "direction": direction,
            "notification_provider": notification_provider,
            "enabled": data["enabled"] == "true",
            "owner": owner,
            "comment": data["comment"] or None,
        }

        # Add provider-specific fields
        if notification_provider == "GCP_PUBSUB":
            if direction == "INBOUND":
                base_result["gcp_pubsub_subscription_name"] = properties.get("gcp_pubsub_subscription_name")
            else:  # OUTBOUND
                base_result["gcp_pubsub_topic_name"] = properties.get("gcp_pubsub_topic_name")
        elif notification_provider == "AZURE_STORAGE_QUEUE":
            base_result["azure_storage_queue_primary_uri"] = properties.get("azure_storage_queue_primary_uri")
            base_result["azure_tenant_id"] = properties.get("azure_tenant_id")
        elif notification_provider == "AZURE_EVENT_GRID":
            base_result["azure_event_grid_topic_endpoint"] = properties.get("azure_event_grid_topic_endpoint")
            base_result["azure_tenant_id"] = properties.get("azure_tenant_id")
        elif notification_provider == "AWS_SNS":
            base_result["aws_sns_topic_arn"] = properties.get("aws_sns_topic_arn")
            base_result["aws_sns_role_arn"] = properties.get("aws_sns_role_arn")

        return base_result
    else:
        raise Exception(f"Unsupported notification integration type: {data['type']}")


def fetch_packages_policy(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "PACKAGES POLICIES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple packages policies matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC PACKAGES POLICY {fqn}")
    properties = desc_result[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "language": properties["language"],
        "allowlist": _parse_packages(properties["allowlist"]),
        "blocklist": _parse_packages(properties["blocklist"]),
        "additional_creation_blocklist": _parse_packages(properties["additional_creation_blocklist"]),
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_password_policy(session: SnowflakeConnection, fqn: FQN):
    policies = _show_resources(session, "PASSWORD POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple password policies matching {fqn}")

    data = policies[0]
    desc_result = execute(session, f"DESC PASSWORD POLICY {fqn}")
    properties = _desc_result_to_dict(desc_result)

    comment = properties["COMMENT"] if properties["COMMENT"] != "null" else None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "password_min_length": int(properties["PASSWORD_MIN_LENGTH"]),
        "password_max_length": int(properties["PASSWORD_MAX_LENGTH"]),
        "password_min_upper_case_chars": int(properties["PASSWORD_MIN_UPPER_CASE_CHARS"]),
        "password_min_lower_case_chars": int(properties["PASSWORD_MIN_LOWER_CASE_CHARS"]),
        "password_min_numeric_chars": int(properties["PASSWORD_MIN_NUMERIC_CHARS"]),
        "password_min_special_chars": int(properties["PASSWORD_MIN_SPECIAL_CHARS"]),
        "password_min_age_days": int(properties["PASSWORD_MIN_AGE_DAYS"]),
        "password_max_age_days": int(properties["PASSWORD_MAX_AGE_DAYS"]),
        "password_max_retries": int(properties["PASSWORD_MAX_RETRIES"]),
        "password_lockout_time_mins": int(properties["PASSWORD_LOCKOUT_TIME_MINS"]),
        "password_history": int(properties["PASSWORD_HISTORY"]),
        "comment": comment,
        "owner": properties["OWNER"],
    }


def fetch_pipe(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "PIPES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple pipes matching {fqn}")

    data = show_result[0]

    # desc_result = execute(session, f"DESC PIPE {fqn}", cacheable=True)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "as_": data["definition"],
        "owner": _get_owner_identifier(data),
        "error_integration": data["error_integration"],
        # "aws_sns_topic": data["aws_sns_topic"],
        "integration": data["integration"],
        "comment": data["comment"],
    }


def fetch_procedure(session: SnowflakeConnection, fqn: FQN):
    # SHOW PROCEDURES IN SCHEMA {}.{}
    # FIXME: This will fail if the database doesnt exist
    show_result = execute(session, f"SHOW PROCEDURES IN SCHEMA {fqn.database}.{fqn.schema}", cacheable=True)
    sprocs = _filter_result(show_result, name=fqn.name)
    if len(sprocs) == 0:
        return None
    if len(sprocs) > 1:
        raise Exception(f"Found multiple stored procedures matching {fqn}")

    data = sprocs[0]

    identifier, returns = _parse_function_arguments(data["arguments"])
    desc_result = execute(session, f"DESC PROCEDURE {fqn.database}.{fqn.schema}.{str(identifier)}", cacheable=True)
    properties = _desc_result_to_dict(desc_result)

    owner = _fetch_owner(session, "PROCEDURE", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "args": _parse_signature(properties["signature"]),
        "comment": data["description"],
        "execute_as": properties["execute as"],
        "external_access_integrations": data["external_access_integrations"] or None,
        "handler": properties["handler"],
        "imports": _parse_list_property(properties["imports"]) or None,
        "language": properties["language"],
        "null_handling": properties["null handling"],
        "owner": owner,
        "packages": _parse_packages(properties["packages"]),
        "returns": returns,
        "runtime_version": properties["runtime_version"],
        "secure": data["is_secure"] == "Y",
        "as_": properties["body"],
    }


def fetch_role(session: SnowflakeConnection, fqn: FQN):
    roles = _show_resources(session, "ROLES", fqn)

    if len(roles) == 0:
        return None
    if len(roles) > 1:
        raise Exception(f"Found multiple roles matching {fqn}")

    data = roles[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_role_grant(session: SnowflakeConnection, fqn: FQN, use_account_usage: bool = False):
    """
    Fetch a role grant (role granted to another role or user).

    Uses ACCOUNT_USAGE cache when available to avoid SHOW GRANTS OF ROLE commands.
    Falls back to SHOW GRANTS OF ROLE if grant not found in cache (to handle latency).
    """
    subject, grantee = fqn.params.copy().popitem()
    subject = ResourceName(subject)
    grantee = ResourceName(grantee)
    role_name = str(fqn.name).upper()
    grantee_upper = str(grantee).upper()

    # Try to use ACCOUNT_USAGE cache
    session_id = id(session)
    if use_account_usage:
        # For role-to-role grants, check GRANTS_TO_ROLES cache
        if str(subject).upper() == "ROLE" and session_id in _ACCOUNT_USAGE_GRANTS_CACHE:
            for grant in _ACCOUNT_USAGE_GRANTS_CACHE[session_id]:
                if (
                    grant["privilege"] == "USAGE"
                    and grant["granted_on"] == "ROLE"
                    and grant["name"].upper() == role_name
                    and grant["grantee_name"].upper() == grantee_upper
                    and grant["granted_to"] == "ROLE"
                ):
                    return {
                        "role": fqn.name,
                        "to_role": _quote_snowflake_identifier(grant["grantee_name"]),
                    }
            # Not found in cache - will fall through to SHOW GRANTS fallback
            logger.debug(f"Role grant {fqn.name} to role {grantee} not found in ACCOUNT_USAGE cache, trying SHOW GRANTS")

        # For role-to-user grants, check GRANTS_TO_USERS cache
        if str(subject).upper() == "USER" and session_id in _ACCOUNT_USAGE_USER_GRANTS_CACHE:
            for grant in _ACCOUNT_USAGE_USER_GRANTS_CACHE[session_id]:
                if (
                    grant["role"].upper() == role_name
                    and grant["grantee_name"].upper() == grantee_upper
                ):
                    return {
                        "role": fqn.name,
                        "to_user": _quote_snowflake_identifier(grant["grantee_name"]),
                    }
            # Not found in cache - will fall through to SHOW GRANTS fallback
            logger.debug(f"Role grant {fqn.name} to user {grantee} not found in ACCOUNT_USAGE cache, trying SHOW GRANTS")

    # Fall back to SHOW GRANTS OF ROLE (either cache not available or grant not found in cache)
    try:
        show_result = execute(session, f"SHOW GRANTS OF ROLE {fqn.name}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise

    if len(show_result) == 0:
        return None

    for data in show_result:
        if (
            resource_name_from_snowflake_metadata(data["granted_to"]) == subject
            and resource_name_from_snowflake_metadata(data["grantee_name"]) == grantee
        ):
            if data["granted_to"] == "ROLE":
                return {
                    "role": fqn.name,
                    "to_role": _quote_snowflake_identifier(data["grantee_name"]),
                    # "owner": data["granted_by"],
                }
            elif data["granted_to"] == "USER":
                return {
                    "role": fqn.name,
                    "to_user": _quote_snowflake_identifier(data["grantee_name"]),
                    # "owner": data["granted_by"],
                }
            else:
                raise Exception(f"Unexpected role grant for role {fqn.name}")

    return None


def fetch_scanner_package(session: SnowflakeConnection, fqn: FQN):
    scanner_packages = execute(
        session,
        f"select * from snowflake.trust_center.scanner_packages where ID = '{fqn.name}' and STATE = 'TRUE'",
        cacheable=True,
    )
    if len(scanner_packages) == 0:
        return None
    if len(scanner_packages) > 1:
        raise Exception(f"Found multiple scanner packages matching {fqn}")

    data = scanner_packages[0]

    return {
        "name": _quote_snowflake_identifier(data["ID"]),
        "enabled": data["STATE"] == "TRUE",
        "schedule": data["SCHEDULE"][11:],
    }


def fetch_schema(session: SnowflakeConnection, fqn: FQN, include_params: bool = True):
    if fqn.database is None:
        raise Exception(f"Schema {fqn} is missing a database name")
    try:
        show_result = _show_resources(session, "SCHEMAS", fqn)
    except ProgrammingError:
        return None

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple schemas matching {fqn}")

    data = show_result[0]

    options = options_result_to_list(data["options"])

    # Only fetch parameters if needed (expensive SHOW PARAMETERS query)
    if include_params:
        params = _show_resource_parameters(session, "SCHEMA", fqn)
        max_data_extension = params.get("max_data_extension_time_in_days")
        default_ddl_collation = params["default_ddl_collation"]
    else:
        max_data_extension = None
        default_ddl_collation = None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "transient": "TRANSIENT" in options,
        "owner": _get_owner_identifier(data),
        "managed_access": "MANAGED ACCESS" in options,
        "data_retention_time_in_days": int(data["retention_time"]),
        "max_data_extension_time_in_days": max_data_extension,
        "default_ddl_collation": default_ddl_collation,
        "comment": data["comment"] or None,
    }


def fetch_secret(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "SECRETS", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple secrets matching {fqn}")
    data = show_result[0]
    desc_result = execute(session, f"DESC SECRET {fqn}")
    properties = desc_result[0]
    if data["secret_type"] == "PASSWORD":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secret_type": data["secret_type"],
            "username": properties["username"],
            "comment": data["comment"] or None,
            "owner": _get_owner_identifier(data),
        }
    elif data["secret_type"] == "GENERIC_STRING":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secret_type": data["secret_type"],
            "comment": data["comment"] or None,
            "owner": _get_owner_identifier(data),
        }
    elif data["secret_type"] == "OAUTH2":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "api_authentication": properties["integration_name"],
            "secret_type": data["secret_type"],
            "oauth_scopes": data["oauth_scopes"],
            "oauth_refresh_token_expiry_time": _convert_to_gmt(properties["oauth_refresh_token_expiry_time"]),
            "comment": data["comment"] or None,
            "owner": _get_owner_identifier(data),
        }
    else:
        raise NotImplementedError(f"Unsupported secret type {data['secret_type']}")


def fetch_security_integration(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, "SHOW SECURITY INTEGRATIONS", cacheable=True)

    show_result = _filter_result(show_result, name=fqn.name)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple security integrations matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC SECURITY INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)

    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if data["type"] == "API_AUTHENTICATION":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "auth_type": properties["auth_type"],
            "enabled": data["enabled"] == "true",
            "oauth_token_endpoint": properties["oauth_token_endpoint"],
            "oauth_client_auth_method": properties["oauth_client_auth_method"],
            "oauth_client_id": properties["oauth_client_id"],
            "oauth_grant": properties["oauth_grant"],
            "oauth_access_token_validity": int(properties["oauth_access_token_validity"]),
            "oauth_allowed_scopes": _parse_list_property(properties["oauth_allowed_scopes"]),
            "comment": data["comment"] or None,
            "owner": owner,
        }

    elif data["type"].startswith("OAUTH"):
        type_, oauth_client = data["type"].split(" - ")
        if oauth_client == "SNOWSERVICES_INGRESS":
            return {
                "name": _quote_snowflake_identifier(data["name"]),
                "type": type_,
                "oauth_client": oauth_client,
                "enabled": data["enabled"] == "true",
                "owner": owner,
            }
    raise Exception(f"Unsupported security integration type {data['type']}")

    # return {
    #     "name": _quote_snowflake_identifier(data["name"]),
    #     "type": type_,
    #     "enabled": data["enabled"] == "true",
    #     "oauth_client": oauth_client,
    #     # "oauth_client_secret": None,
    #     # "oauth_redirect_uri": None,
    #     "oauth_issue_refresh_tokens": properties["oauth_issue_refresh_tokens"] == "true",
    #     "oauth_refresh_token_validity": properties["oauth_refresh_token_validity"],
    #     "comment": data["comment"] or None,
    # }


def fetch_sequence(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW SEQUENCES LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}")
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple sequences matching {fqn}")

    data = show_result[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "start": data["next_value"],
        "increment": data["interval"],
        "comment": data["comment"] or None,
    }


def fetch_service(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(
        session, f"SHOW SERVICES LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}", cacheable=True
    )

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple services matching {fqn}")

    data = show_result[0]

    return {
        "name": fqn.name,
        "compute_pool": data["compute_pool"],
        "external_access_integrations": None,
        "auto_resume": data["auto_resume"] == "true",
        "min_instances": data["min_instances"],
        "max_instances": data["max_instances"],
        "query_warehouse": data["query_warehouse"],
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_share(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW SHARES LIKE '{fqn.name}'")
    shares = _filter_result(show_result, kind="OUTBOUND")

    if len(shares) == 0:
        return None
    if len(shares) > 1:
        raise Exception(f"Found multiple shares matching {fqn}")

    data = shares[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "comment": data["comment"] or None,
    }


def fetch_shared_database(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, "SELECT SYSTEM$SHOW_IMPORTED_DATABASES()", cacheable=True)
    show_result = json.loads(show_result[0]["SYSTEM$SHOW_IMPORTED_DATABASES()"])

    shares = _filter_result(show_result, name=fqn.name)

    if len(shares) == 0:
        return None
    if len(shares) > 1:
        raise Exception(f"Found multiple shares matching {fqn}")

    data = shares[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "from_share": data["origin"],
        "owner": _get_owner_identifier(data),
    }


def fetch_stage(session: SnowflakeConnection, fqn: FQN):
    show_result = _show_resources(session, "STAGES", fqn)
    stages = _filter_result(show_result, name=fqn.name)

    if len(stages) == 0:
        return None
    if len(stages) > 1:
        raise Exception(f"Found multiple stages matching {fqn}")

    data = stages[0]
    # desc_result = execute(session, f"DESC STAGE {fqn}")
    # properties = _desc_type3_result_to_dict(desc_result, lower_properties=True)

    if data["type"] == "EXTERNAL":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "url": data["url"],
            "owner": _get_owner_identifier(data),
            "type": data["type"],
            "storage_integration": data["storage_integration"],
            "directory": {"enable": data["directory_enabled"] == "Y"},
            "comment": data["comment"] or None,
        }
    elif data["type"] in ("INTERNAL", "INTERNAL NO CSE"):
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "owner": _get_owner_identifier(data),
            "type": "INTERNAL",
            "directory": {"enable": data["directory_enabled"] == "Y"},
            "comment": data["comment"] or None,
        }
    else:
        raise Exception(f"Unsupported stage type {data['type']}")


def fetch_storage_integration(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, "SHOW INTEGRATIONS")
    integrations = _filter_result(show_result, name=fqn.name, category="STORAGE")

    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple storage integrations matching {fqn}")

    data = integrations[0]

    desc_result = execute(session, f"DESC INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)

    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if properties["storage_provider"] == "S3":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "comment": data["comment"] or None,
            "owner": owner,
            "storage_provider": properties["storage_provider"],
            "storage_aws_role_arn": properties.get("storage_aws_role_arn"),
            "storage_allowed_locations": properties.get("storage_allowed_locations") or None,
            "storage_blocked_locations": properties.get("storage_blocked_locations") or None,
            "storage_aws_object_acl": properties.get("storage_aws_object_acl"),
        }
    elif properties["storage_provider"] == "GCS":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "comment": data["comment"] or None,
            "owner": owner,
            "storage_provider": properties["storage_provider"],
            "storage_allowed_locations": properties.get("storage_allowed_locations") or None,
            "storage_blocked_locations": properties.get("storage_blocked_locations") or None,
        }
    elif properties["storage_provider"] == "AZURE":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "comment": data["comment"] or None,
            "owner": owner,
            "storage_provider": properties["storage_provider"],
            "storage_allowed_locations": properties.get("storage_allowed_locations") or None,
            "azure_tenant_id": properties["azure_tenant_id"],
        }
    else:
        raise Exception(f"Unsupported storage provider {properties['storage_provider']}")


def fetch_stream(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, "SHOW STREAMS IN ACCOUNT", cacheable=True)

    streams = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)

    if len(streams) == 0:
        return None
    if len(streams) > 1:
        raise Exception(f"Found multiple streams matching {fqn}")

    data = streams[0]
    if data["source_type"] == "Table":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "comment": data["comment"] or None,
            "append_only": data["mode"] == "APPEND_ONLY",
            "on_table": data["table_name"],
            "owner": _get_owner_identifier(data),
        }
    elif data["source_type"] == "View":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "comment": data["comment"] or None,
            "append_only": data["mode"] == "APPEND_ONLY",
            "on_view": data["table_name"],
            "owner": _get_owner_identifier(data),
        }
    elif data["source_type"] == "Stage":
        # Snowflake only returns the stage name without the fully qualified path.
        # We need to construct it from the stream's database/schema.
        stage_name = data["table_name"]
        if "." not in stage_name:
            stage_name = f"{data['database_name']}.{data['schema_name']}.{stage_name}"
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "on_stage": stage_name,
            "owner": _get_owner_identifier(data),
            "comment": data["comment"] or None,
        }
    else:
        raise NotImplementedError(f"Unsupported stream source type {data['source_type']}")


def fetch_tag(session: SnowflakeConnection, fqn: FQN):
    try:
        show_result = execute(session, "SHOW TAGS IN ACCOUNT", cacheable=True)
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return None
        raise
    tags = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)
    if len(tags) == 0:
        return None
    if len(tags) > 1:
        raise Exception(f"Found multiple tags matching {fqn}")
    data = tags[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "comment": data["comment"] or None,
        "allowed_values": json.loads(data["allowed_values"]) if data["allowed_values"] else None,
    }


def fetch_task(session: SnowflakeConnection, fqn: FQN, include_params: bool = True):
    show_result = _show_resources(session, "TASKS", fqn)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple tasks matching {fqn}")

    data = show_result[0]
    task_details_result = execute(session, f"DESC TASK {fqn.database}.{fqn.schema}.{fqn.name}", cacheable=True)
    if len(task_details_result) == 0:
        raise Exception(f"Failed to fetch task details for {fqn}")
    task_details = task_details_result[0]

    # Only fetch parameters if needed (expensive query)
    if include_params:
        show_params_result = execute(session, f"SHOW PARAMETERS FOR TASK {fqn}")
        params = params_result_to_dict(show_params_result)
    else:
        params = {}

    error_integration = None
    if data["error_integration"] != "null":
        error_integration = _quote_snowflake_identifier(data["error_integration"])

    task_relations = json.loads(task_details["task_relations"])
    after = task_relations["Predecessors"]

    suspend_task_after_num_failures = None
    if len(after) == 0:
        suspend_task_after_num_failures = params.get("suspend_task_after_num_failures", None)

    user_task_managed_initial_warehouse_size = None
    if not data["warehouse"]:
        user_task_managed_initial_warehouse_size = params.get("user_task_managed_initial_warehouse_size", None)
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "warehouse": data["warehouse"],
        "schedule": data["schedule"],
        "config": data["config"],
        "allow_overlapping_execution": data["allow_overlapping_execution"] == "true",
        "user_task_managed_initial_warehouse_size": user_task_managed_initial_warehouse_size,
        "user_task_timeout_ms": params.get("user_task_timeout_ms", None),
        "suspend_task_after_num_failures": suspend_task_after_num_failures,
        "error_integration": error_integration,
        "state": str(data["state"]).upper(),
        "owner": _get_owner_identifier(data),
        "comment": task_details["comment"] or None,
        "after": after or None,
        "as_": task_details["definition"],
    }


def fetch_replication_group(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW REPLICATION GROUPS LIKE '{fqn.name}'", cacheable=True)

    replication_groups = _filter_result(show_result, is_primary="true")
    if len(replication_groups) == 0:
        return None
    if len(replication_groups) > 1:
        raise Exception(f"Found multiple replication groups matching {fqn}")

    data = replication_groups[0]
    show_databases_result = execute(session, f"SHOW DATABASES IN REPLICATION GROUP {fqn.name}")
    databases = [row["name"] for row in show_databases_result]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "object_types": data["object_types"].split(","),
        "allowed_integration_types": (
            None if data["allowed_integration_types"] == "" else data["allowed_integration_types"].split(",")
        ),
        "allowed_accounts": None if data["allowed_accounts"] == "" else data["allowed_accounts"].split(","),
        "allowed_databases": databases,
        "replication_schedule": data["replication_schedule"],
        "owner": _get_owner_identifier(data),
    }


def fetch_resource_monitor(session: SnowflakeConnection, fqn: FQN):
    show_result = execute(session, f"SHOW RESOURCE MONITORS LIKE '{fqn.name}'")
    resource_monitors = _filter_result(show_result)
    if len(resource_monitors) == 0:
        return None
    if len(resource_monitors) > 1:
        raise Exception(f"Found multiple resource monitors matching {fqn}")
    data = resource_monitors[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "credit_quota": int(float(data["credit_quota"])) if data["credit_quota"] else None,
        "frequency": data["frequency"],
        "start_timestamp": _convert_to_gmt(data["start_time"], "%Y-%m-%d %H:%M"),
        "end_timestamp": _convert_to_gmt(data["end_time"], "%Y-%m-%d %H:%M"),
        "notify_users": data["notify_users"] or None,
    }


def fetch_resource_tags(session: SnowflakeConnection, resource_type: ResourceType, fqn: FQN):
    """
    +----------------------+------------+-------------+-----------+--------+----------------------+---------------+-------------+--------+-------------+
    |     TAG_DATABASE     | TAG_SCHEMA |  TAG_NAME   | TAG_VALUE | LEVEL  |   OBJECT_DATABASE    | OBJECT_SCHEMA | OBJECT_NAME | DOMAIN | COLUMN_NAME |
    +----------------------+------------+-------------+-----------+--------+----------------------+---------------+-------------+--------+-------------+
    | TITAN                | SOMESCH    | TASTY_TREAT | muffin    | SCHEMA | TEST_DB_RUN_13287C56 |               | SOMESCH     | SCHEMA |             |
    | TEST_DB_RUN_13287C56 | PUBLIC     | TRASH       | true      | SCHEMA | TEST_DB_RUN_13287C56 |               | SOMESCH     | SCHEMA |             |
    +----------------------+------------+-------------+-----------+--------+----------------------+---------------+-------------+--------+-------------+

    """

    database = f"{fqn.database}." if fqn.database else ""

    try:
        tag_refs = execute(
            session,
            f"""
                SELECT *
                FROM table({database}information_schema.tag_references(
                    '{fqn}', '{str(resource_type)}'
                ))""",
        )
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return None
        raise

    if len(tag_refs) == 0:
        return None

    tag_map = {}
    for tag_ref in tag_refs:
        in_same_database = tag_ref["TAG_DATABASE"] == tag_ref["OBJECT_DATABASE"]
        in_same_schema = tag_ref["TAG_SCHEMA"] == tag_ref["OBJECT_SCHEMA"]
        tag_in_public_schema = tag_ref["TAG_SCHEMA"] == "PUBLIC"

        if in_same_database and (in_same_schema or tag_in_public_schema):
            tag_name = tag_ref["TAG_NAME"]
        else:
            tag_name = f"{tag_ref['TAG_DATABASE']}.{tag_ref['TAG_SCHEMA']}.{tag_ref['TAG_NAME']}"
        tag_map[tag_name] = tag_ref["TAG_VALUE"]
    return tag_map


def fetch_table(session: SnowflakeConnection, fqn: FQN, include_params: bool = True):
    show_result = execute(session, "SHOW TABLES IN ACCOUNT", cacheable=True)

    tables = _filter_result(
        show_result,
        name=fqn.name,
        database_name=fqn.database,
        schema_name=fqn.schema,
    )

    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple tables matching {fqn}")

    columns = fetch_columns(session, "TABLE", fqn)

    data = tables[0]

    # Only fetch parameters if needed (expensive query)
    if include_params:
        show_params_result = execute(session, f"SHOW PARAMETERS FOR TABLE {fqn}")
        params = params_result_to_dict(show_params_result)
    else:
        params = {}

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "columns": columns,
        "cluster_by": _parse_cluster_keys(data["cluster_by"]),
        "transient": data["kind"] == "TRANSIENT",
        "owner": _get_owner_identifier(data),
        "comment": data["comment"] or None,
        "enable_schema_evolution": data["enable_schema_evolution"] == "Y",
        # "data_retention_time_in_days": int(data["retention_time"]),
        # "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days", None),
        "default_ddl_collation": params.get("default_ddl_collation", None),
        "change_tracking": data["change_tracking"] == "ON",
    }


def fetch_tag_reference(session: SnowflakeConnection, fqn: FQN):
    object_domain = fqn.params["domain"]
    # TODO: this is a hacky fix
    name = str(fqn).split("?")[0]
    resource_fqn = parse_FQN(name, is_db_scoped=(object_domain == "SCHEMA"))

    tag_db = resource_fqn.database if resource_fqn.database else resource_fqn

    # Another hacky fix
    if str(resource_fqn) == "DATABASE":
        resource_fqn = '"DATABASE"'  # type: ignore[assignment]

    try:
        tag_refs = execute(
            session,
            f"""
                SELECT *
                FROM table({tag_db}.information_schema.tag_references(
                    '{resource_fqn}', '{object_domain}'
                ))""",
        )
    except ProgrammingError as err:
        if err.errno in (INVALID_IDENTIFIER, UNSUPPORTED_FEATURE):
            return None
        raise

    if len(tag_refs) == 0:
        return None

    tag_map = {}
    for tag_ref in tag_refs:
        tag_name = f"{tag_ref['TAG_DATABASE']}.{tag_ref['TAG_SCHEMA']}.{tag_ref['TAG_NAME']}"
        tag_map[tag_name] = tag_ref["TAG_VALUE"]
    return {
        "object_name": name,
        "object_domain": object_domain,
        "tags": tag_map,
    }


def fetch_user(session: SnowflakeConnection, fqn: FQN, include_params: bool = True, existence_only: bool = False) -> Optional[dict]:
    show_result = _show_users(session)
    users = _filter_result(show_result, name=fqn.name)

    if len(users) == 0:
        return None
    if len(users) > 1:
        raise Exception(f"Found multiple users matching {fqn}")

    data = users[0]

    # For existence checks (reference validation), skip expensive DESC USER
    if existence_only:
        return {"name": _quote_snowflake_identifier(data["name"]), "owner": ""}

    desc_result = execute(session, f"DESC USER {fqn}")
    properties = _desc_result_to_dict(desc_result, lower_properties=True)

    # Only fetch parameters if needed (expensive SHOW PARAMETERS query)
    if include_params:
        show_params_result = execute(session, f"SHOW PARAMETERS FOR USER {fqn}")
        params = params_result_to_dict(show_params_result)
        network_policy = params["network_policy"]
    else:
        network_policy = None

    user_type = properties["type"].upper()

    display_name = None
    login_name = None
    must_change_password = None
    if user_type != "SERVICE":
        display_name = data["display_name"]
        login_name = data["login_name"]
        must_change_password = data["must_change_password"] == "true"

    rsa_public_key = properties["rsa_public_key"] if properties["rsa_public_key"] != "null" else None
    middle_name = properties["middle_name"] if properties["middle_name"] != "null" else None

    default_secondary_roles = json.loads(data["default_secondary_roles"]) if data["default_secondary_roles"] else None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "login_name": login_name,
        "display_name": display_name,
        "first_name": data["first_name"] or None,
        "middle_name": middle_name,
        "last_name": data["last_name"] or None,
        "email": data["email"] or None,
        "comment": data["comment"] or None,
        "disabled": data["disabled"] == "true",
        "must_change_password": must_change_password,
        "default_warehouse": data["default_warehouse"] or None,
        "default_namespace": data["default_namespace"] or None,
        "default_role": data["default_role"] or None,
        "default_secondary_roles": default_secondary_roles,
        "type": user_type,
        "rsa_public_key": rsa_public_key,
        "network_policy": network_policy,
        "owner": _get_owner_identifier(data),
    }


def fetch_view(session: SnowflakeConnection, fqn: FQN):
    if fqn.schema is None:
        raise Exception(f"View fqn must have a schema {fqn}")
    try:
        views = _show_resources(session, "VIEWS", fqn)
    except ProgrammingError:
        return None

    if len(views) == 0:
        return None
    if len(views) > 1:
        raise Exception(f"Found multiple views matching {fqn}")

    data = views[0]

    if data["is_materialized"] == "true":
        return None

    columns = fetch_columns(session, "VIEW", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "secure": data["is_secure"] == "true",
        "columns": columns,
        "change_tracking": data["change_tracking"] == "ON",
        "comment": data["comment"] or None,
        "as_": parse_view_ddl(data["text"]),
    }


def fetch_warehouse(session: SnowflakeConnection, fqn: FQN, include_params: bool = True):
    try:
        show_result = _show_resources(session, "WAREHOUSES", fqn)
    except ProgrammingError:
        return None

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple warehouses matching {fqn}")

    data = show_result[0]

    # Only fetch parameters if needed (expensive SHOW PARAMETERS query)
    if include_params:
        show_params_result = execute(session, f"SHOW PARAMETERS FOR WAREHOUSE {fqn}")
        params = params_result_to_dict(show_params_result)
        max_concurrency_level = params["max_concurrency_level"]
        statement_queued_timeout = params["statement_queued_timeout_in_seconds"]
        statement_timeout = params["statement_timeout_in_seconds"]
    else:
        max_concurrency_level = None
        statement_queued_timeout = None
        statement_timeout = None

    resource_monitor = None if data["resource_monitor"] == "null" else data["resource_monitor"]

    # Enterprise edition features
    query_accel = data.get("enable_query_acceleration")
    if query_accel:
        query_accel = query_accel == "true"
    else:
        query_accel = False

    warehouse_dict = {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "warehouse_type": data["type"],
        "warehouse_size": str(WarehouseSize(data["size"])),
        "auto_suspend": data["auto_suspend"],
        "auto_resume": data["auto_resume"] == "true",
        "comment": data["comment"] or None,
        "resource_monitor": resource_monitor,
        "enable_query_acceleration": query_accel,
        "query_acceleration_max_scale_factor": data.get("query_acceleration_max_scale_factor", None),
        "max_cluster_count": data.get("max_cluster_count", None),
        "min_cluster_count": data.get("min_cluster_count", None),
        "scaling_policy": data.get("scaling_policy", None),
        "max_concurrency_level": max_concurrency_level,
        "statement_queued_timeout_in_seconds": statement_queued_timeout,
        "statement_timeout_in_seconds": statement_timeout,
    }

    return warehouse_dict


################ List functions

######## List helpers


def list_resource(session: SnowflakeConnection, resource_label: str, **kwargs) -> list[FQN]:
    func_name = f"list_{pluralize(resource_label)}"
    list_func = getattr(__this__, func_name)
    # Pass through kwargs (e.g., use_account_usage) to functions that support them (cached)
    sig = _get_cached_signature(func_name)
    supported_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return list_func(session, **supported_kwargs)


def list_account_scoped_resource(session: SnowflakeConnection, resource) -> list[FQN]:
    show_result = execute(session, f"SHOW {resource}", cacheable=True)
    resources = []
    for row in show_result:
        resources.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return resources


def list_schema_scoped_resource(session: SnowflakeConnection, resource) -> list[FQN]:
    show_result = execute(session, f"SHOW {resource} IN ACCOUNT", cacheable=True)
    resources = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES:
            continue
        resources.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return resources


######## List functions by resource


def list_account_parameters(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW PARAMETERS IN ACCOUNT", cacheable=True)
    account_parameters = []
    for row in show_result:
        # Skip system parameters and unset parameters
        if row["level"] != "ACCOUNT":
            continue
        account_parameters.append(FQN(name=ResourceName(row["key"])))
    return account_parameters


def list_alerts(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "ALERTS")


def list_api_integrations(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "API INTEGRATIONS")


def list_authentication_policies(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "AUTHENTICATION POLICIES")


def list_catalog_integrations(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "CATALOG INTEGRATIONS")


def list_compute_pools(session: SnowflakeConnection) -> list[FQN]:
    try:
        show_result = execute(session, "SHOW COMPUTE POOLS")
    except ProgrammingError as err:
        logger.warning(f"Error listing compute pools: {err}")
        return []
    return [FQN(name=resource_name_from_snowflake_metadata(row["name"])) for row in show_result]


def _list_databases(session: SnowflakeConnection) -> list[ResourceName]:
    show_result = execute(session, "SHOW DATABASES", cacheable=True)
    databases = []
    for row in show_result:
        # Exclude system databases like SNOWFLAKE
        if row["name"] in SYSTEM_DATABASES:
            continue
        # Exclude database shares
        if row["kind"] != "STANDARD":
            continue
        databases.append(resource_name_from_snowflake_metadata(row["name"]))
    return databases


def list_databases(session: SnowflakeConnection) -> list[FQN]:
    databases = _list_databases(session)
    return [FQN(name=database) for database in databases]


def list_database_roles(session: SnowflakeConnection, database=None) -> list[FQN]:
    databases: list[ResourceName]
    if database:
        databases = [ResourceName(database)]
    else:
        databases = _list_databases(session)

    roles = []
    for database_name in databases:
        try:
            # A rare case where we need to always quote the identifier. Snowflake chokes if the database name
            # is DATABASE, but this will work if quoted
            if database_name == "DATABASE":
                database_name._quoted = True
            database_roles = execute(session, f"SHOW DATABASE ROLES IN DATABASE {database_name}", cacheable=True)
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_ERR:
                continue
            raise
        for role in database_roles:
            roles.append(
                FQN(
                    name=resource_name_from_snowflake_metadata(role["name"]),
                    database=database_name,
                )
            )
    return roles


def list_database_role_grants(
    session: SnowflakeConnection, database=None, use_account_usage: bool = False
) -> list[FQN]:
    """
    List all database role grants (database role granted to roles or other database roles).

    When use_account_usage is True and ACCOUNT_USAGE access is available, uses:
    - GRANTS_TO_ROLES view for database role grants (privilege=USAGE, granted_on=DATABASE ROLE)

    Falls back to SHOW GRANTS OF DATABASE ROLE commands when ACCOUNT_USAGE is unavailable.

    Args:
        session: Snowflake connection
        database: Optional database name to filter grants for. If None, returns grants for all databases.
        use_account_usage: Whether to attempt using ACCOUNT_USAGE (default True)

    Returns:
        List of FQN objects representing database role grants, with params indicating
        whether the grantee is a 'role' or 'database_role'.
    """
    databases: list[ResourceName]
    if database:
        databases = [ResourceName(database)]
    else:
        databases = _list_databases(session)

    # Build a set of database names for filtering (uppercase for case-insensitive matching)
    database_name_set = {str(db).upper() for db in databases}

    role_grants: list[FQN] = []

    # Try ACCOUNT_USAGE if enabled and accessible
    use_au = _should_use_account_usage(session, use_account_usage)
    if use_au:
        logger.debug("Using ACCOUNT_USAGE for list_database_role_grants()")

        # Fetch all grants and filter for database role grants
        # Database role grants have privilege=USAGE and granted_on=DATABASE ROLE
        all_grants = _fetch_grants_from_account_usage(session)

        # If ACCOUNT_USAGE query succeeded, process results
        if all_grants is not None:
            for grant in all_grants:
                # Filter for database role grants (USAGE on DATABASE ROLE)
                if grant["privilege"] != "USAGE" or grant["granted_on"] != "DATABASE ROLE":
                    continue

                # The name field contains the fully qualified database role (e.g., "DB.ROLE_NAME")
                db_role_name = grant["name"]
                if "." not in db_role_name:
                    continue

                db_name, role_name = db_role_name.split(".", 1)

                # Filter by database if specified
                if db_name.upper() not in database_name_set:
                    continue

                # Determine subject based on grantee type
                subject = "role" if grant["granted_to"] == "ROLE" else "database_role"

                role_grants.append(
                    FQN(
                        name=resource_name_from_snowflake_metadata(role_name),
                        database=resource_name_from_snowflake_metadata(db_name),
                        params={subject: grant["grantee_name"]},
                    )
                )
            # If we got results or no specific database was filtered, return
            # If specific database was filtered and we got 0 results, fall back to SHOW
            # (handles ACCOUNT_USAGE latency for newly created grants)
            if role_grants or database is None:
                return role_grants
            logger.debug("ACCOUNT_USAGE returned 0 results for specific database, falling back to SHOW")
        # Fall through to SHOW queries if ACCOUNT_USAGE failed or returned empty for filtered query

    # Fallback to SHOW GRANTS OF DATABASE ROLE commands
    logger.debug("Using SHOW GRANTS OF DATABASE ROLE for list_database_role_grants() (ACCOUNT_USAGE unavailable or disabled)")

    for database_name in databases:
        try:
            # A rare case where we need to always quote the identifier. Snowflake chokes if the database name
            # is DATABASE, but this will work if quoted
            if database_name == "DATABASE":
                database_name._quoted = True
            database_roles = execute(session, f"SHOW DATABASE ROLES IN DATABASE {database_name}", cacheable=True)
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_ERR:
                continue
            raise
        for role in database_roles:
            show_result = execute(
                session, f"SHOW GRANTS OF DATABASE ROLE {database_name}.{role['name']}", cacheable=True
            )
            for data in show_result:
                subject = "role" if data["granted_to"] == "ROLE" else "database_role"
                db, name = data["role"].split(".")
                role_grants.append(
                    FQN(
                        name=resource_name_from_snowflake_metadata(name),
                        database=resource_name_from_snowflake_metadata(db),
                        params={subject: data["grantee_name"]},
                    )
                )
    return role_grants


def list_dynamic_tables(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "DYNAMIC TABLES")


def list_external_access_integrations(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "EXTERNAL ACCESS INTEGRATIONS")


def list_external_volumes(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "EXTERNAL VOLUMES")


def list_file_formats(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "FILE FORMATS")


def list_functions(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW USER FUNCTIONS IN ACCOUNT", cacheable=True)
    functions = []
    for row in show_result:
        # Skip functions with empty database/schema and system databases
        if not row["catalog_name"] or not row["schema_name"]:
            continue
        if row["catalog_name"] in SYSTEM_DATABASES:
            continue
        fqn, returns = _parse_function_arguments(row["arguments"])
        fqn.database = row["catalog_name"]
        fqn.schema = row["schema_name"]
        functions.append(fqn)
    return functions


def list_grants(
    session: SnowflakeConnection,
    use_account_usage: bool = False,
    include_future_grants: bool = True,
    future_grant_roles: Optional[set] = None,
) -> list[FQN]:
    grants: list[FQN] = []

    # Get all non-system role names for processing
    # Use "SHOW ROLES IN ACCOUNT" to match _show_resources for cache consistency
    roles_result = execute(session, "SHOW ROLES IN ACCOUNT", cacheable=True)
    role_names = [
        resource_name_from_snowflake_metadata(role["name"])
        for role in roles_result
        if resource_name_from_snowflake_metadata(role["name"]) not in SYSTEM_ROLES
    ]

    # Determine whether to use ACCOUNT_USAGE or SHOW queries
    use_au = _should_use_account_usage(session, use_account_usage)
    au_succeeded = False

    if use_au:
        logger.debug("list_grants: using ACCOUNT_USAGE for regular grants")
        # Fetch all grants in a single ACCOUNT_USAGE query
        all_grants = _fetch_grants_from_account_usage(session)

        # If ACCOUNT_USAGE query succeeded, process results
        if all_grants is not None:
            au_succeeded = True
            # Build a set of non-system role names for filtering
            role_name_set = {str(rn) for rn in role_names}

            for data in all_grants:
                # Only process grants to non-system roles (ROLE type, not DATABASE ROLE)
                if data["granted_to"] != "ROLE":
                    continue
                grantee = data["grantee_name"]
                if grantee not in role_name_set:
                    continue

                # Skip role grants (hierarchy handled by list_role_grants)
                if data["granted_on"] == "ROLE":
                    continue

                # Snowcap Grants don't support OWNERSHIP privilege
                if data["privilege"] == "OWNERSHIP":
                    continue

                # Skip undocumented privs
                if data["privilege"] in ["CREATE CORTEX SEARCH SERVICE", "CANCEL QUERY"]:
                    continue

                name = data["name"]
                if data["granted_on"] == "ACCOUNT":
                    name = "ACCOUNT"
                on = f"{data['granted_on'].lower()}/{name}"
                to = f"role/{grantee}"
                grants.append(
                    FQN(
                        name=ResourceName("GRANT"),
                        params={
                            "grant_type": "OBJECT",
                            "priv": data["privilege"],
                            "on": on,
                            "to": to,
                        },
                    )
                )
        # Fall through to SHOW queries if ACCOUNT_USAGE failed

    if not au_succeeded:
        logger.debug("list_grants: using SHOW GRANTS per role (ACCOUNT_USAGE disabled or unavailable)")
        # Fall back to per-role SHOW queries
        for role_name in role_names:
            grant_data = _show_grants_to_role(session, role_name, role_type=ResourceType.ROLE, cacheable=True, use_account_usage=False)
            for data in grant_data:
                if data["granted_on"] == "ROLE":
                    continue

                # Snowcap Grants don't support OWNERSHIP privilege
                if data["privilege"] == "OWNERSHIP":
                    continue

                # Skip undocumented privs
                if data["privilege"] in ["CREATE CORTEX SEARCH SERVICE", "CANCEL QUERY"]:
                    continue

                name = data["name"]
                if data["granted_on"] == "ACCOUNT":
                    name = "ACCOUNT"
                on = f"{data['granted_on'].lower()}/{name}"
                to = f"role/{role_name}"
                grants.append(
                    FQN(
                        name=ResourceName("GRANT"),
                        params={
                            "grant_type": "OBJECT",
                            "priv": data["privilege"],
                            "on": on,
                            "to": to,
                        },
                    )
                )

    # Future grants always use SHOW commands (not available in ACCOUNT_USAGE)
    # Only fetch if include_future_grants is True (manifest has future grants)
    if include_future_grants:
        # If future_grant_roles is provided, only fetch for those specific roles
        # This optimization avoids querying all roles when only a few have future grants
        if future_grant_roles:
            roles_to_query = [
                rn for rn in role_names
                if str(rn).upper() in future_grant_roles
            ]
            logger.debug(f"list_grants: fetching future grants for {len(roles_to_query)} roles (filtered by manifest)")
        else:
            roles_to_query = role_names
            logger.debug("list_grants: fetching future grants using SHOW FUTURE GRANTS")

        future_grants_by_role = _fetch_future_grants_for_all_roles(session, roles_to_query)
        for role_name in roles_to_query:
            role_name_str = str(role_name)
            grant_data = future_grants_by_role.get(role_name_str, [])
            for data in grant_data:
                on_type = data["granted_on"].lower()
                collection = data["name"]
                to = f"role/{role_name}"
                grants.append(
                    FQN(
                        name=ResourceName("GRANT"),
                        params={
                            "grant_type": "FUTURE",
                            "priv": data["privilege"],
                            "on": f"{on_type}/{collection}",
                            "to": to,
                        },
                    )
                )
    else:
        logger.debug("list_grants: skipping future grants (none in manifest)")

    return grants


def list_iceberg_tables(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "ICEBERG TABLES")


def list_image_repositories(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "IMAGE REPOSITORIES")


def list_masking_policies(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "MASKING POLICIES")


def list_network_policies(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "NETWORK POLICIES")


def list_network_rules(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "NETWORK RULES")


def list_notification_integrations(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "NOTIFICATION INTEGRATIONS")


def list_packages_policies(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "PACKAGES POLICIES")


def list_password_policies(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "PASSWORD POLICIES")


def list_pipes(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "PIPES")


def list_procedures(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW PROCEDURES IN ACCOUNT", cacheable=True)
    procedures = []
    for row in show_result:
        # Skip system procedures (empty database/schema) and system databases
        if not row["catalog_name"] or not row["schema_name"]:
            continue
        if row["catalog_name"] in SYSTEM_DATABASES:
            continue
        fqn, returns = _parse_function_arguments(row["arguments"])
        fqn.database = row["catalog_name"]
        fqn.schema = row["schema_name"]
        procedures.append(fqn)
    return procedures


def list_resource_monitors(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "RESOURCE MONITORS")


def list_roles(session: SnowflakeConnection) -> list[FQN]:
    # Use "SHOW ROLES IN ACCOUNT" to match _show_resources for cache consistency
    show_result = execute(session, "SHOW ROLES IN ACCOUNT", cacheable=True)
    return [
        FQN(name=resource_name_from_snowflake_metadata(row["name"]))
        for row in show_result
        if row["name"] not in SYSTEM_ROLES
    ]


def list_role_grants(session: SnowflakeConnection, use_account_usage: bool = False) -> list[FQN]:
    """
    List all role grants (role-to-role and role-to-user) in the account.

    When use_account_usage is True and ACCOUNT_USAGE access is available, uses:
    - GRANTS_TO_ROLES view for role-to-role grants (privilege=USAGE, granted_on=ROLE)
    - GRANTS_TO_USERS view for role-to-user grants

    Falls back to SHOW GRANTS OF ROLE commands when ACCOUNT_USAGE is unavailable.

    Returns:
        List of FQN objects representing role grants, with params indicating
        whether the grantee is a 'user' or 'role'.
    """
    # Use "SHOW ROLES IN ACCOUNT" to match _show_resources for cache consistency
    roles = execute(session, "SHOW ROLES IN ACCOUNT", cacheable=True)

    # Build set of non-system role names for filtering
    role_name_set: set[str] = set()
    role_names: list[ResourceName] = []
    for role in roles:
        role_name = resource_name_from_snowflake_metadata(role["name"])
        if role_name in SYSTEM_ROLES:
            continue
        role_names.append(role_name)
        role_name_set.add(str(role_name).upper())

    grants: list[FQN] = []

    # Try ACCOUNT_USAGE if enabled and accessible
    use_au = _should_use_account_usage(session, use_account_usage)
    au_succeeded = False

    if use_au:
        logger.debug("Using ACCOUNT_USAGE views for list_role_grants()")

        # Get role-to-role grants from GRANTS_TO_ROLES
        # These are grants where privilege=USAGE and granted_on=ROLE
        all_grants = _fetch_grants_from_account_usage(session)

        # If ACCOUNT_USAGE query succeeded, process results
        if all_grants is not None:
            for grant in all_grants:
                # Filter for role grants (USAGE on ROLE)
                if grant["privilege"] != "USAGE" or grant["granted_on"] != "ROLE":
                    continue

                role_being_granted = grant["name"]  # The role being granted
                grantee_name = grant["grantee_name"]  # Who receives the grant

                # Skip system roles (the role being granted)
                if role_being_granted.upper() in SYSTEM_ROLES:
                    continue

                # Only include roles we're tracking
                if role_being_granted.upper() not in role_name_set:
                    continue

                # The granted_to field indicates if grantee is ROLE or DATABASE ROLE
                if grant["granted_to"] == "ROLE":
                    grants.append(
                        FQN(
                            name=resource_name_from_snowflake_metadata(role_being_granted),
                            params={"role": grantee_name},
                        )
                    )

            # Get role-to-user grants from GRANTS_TO_USERS
            user_grants = _fetch_role_grants_to_users_from_account_usage(session)

            # If user grants query also succeeded, mark as successful
            if user_grants is not None:
                au_succeeded = True
                for grant in user_grants:
                    role_being_granted = grant["role"]

                    # Skip system roles
                    if role_being_granted.upper() in SYSTEM_ROLES:
                        continue

                    # Only include roles we're tracking
                    if role_being_granted.upper() not in role_name_set:
                        continue

                    grants.append(
                        FQN(
                            name=resource_name_from_snowflake_metadata(role_being_granted),
                            params={"user": grant["grantee_name"]},
                        )
                    )
        # Fall through to SHOW queries if ACCOUNT_USAGE failed

    if not au_succeeded:
        # Fallback to SHOW GRANTS OF ROLE commands
        # Clear any partial results from failed ACCOUNT_USAGE attempt
        grants = []
        logger.debug("Using SHOW GRANTS OF ROLE for list_role_grants() (ACCOUNT_USAGE unavailable or disabled)")

        def error_handler(err: Exception, sql: str):
            if isinstance(err, ProgrammingError) and err.errno == DOES_NOT_EXIST_ERR:
                return
            raise err

        for name, result in execute_in_parallel(
            session,
            [(f"SHOW GRANTS OF ROLE {role_name}", role_name) for role_name in role_names],
            error_handler=error_handler,
            cacheable=True,
        ):
            for data in result:
                subject = "user" if data["granted_to"] == "USER" else "role"
                grants.append(FQN(name=name, params={subject: data["grantee_name"]}))

    return grants


def list_scanner_packages(session: SnowflakeConnection) -> list[FQN]:
    try:
        scanner_packages = execute(
            session, "select * from snowflake.trust_center.scanner_packages WHERE state = 'TRUE'", cacheable=True
        )
    except ProgrammingError as err:
        # Trust Center may not be available on all accounts
        logger.debug(f"Could not query trust_center.scanner_packages: {err}")
        return []
    user_packages = []
    for pkg in scanner_packages:
        if pkg["ID"] == "SECURITY_ESSENTIALS":
            continue
        user_packages.append(FQN(name=resource_name_from_snowflake_metadata(pkg["ID"])))
    return user_packages


def list_schemas(session: SnowflakeConnection, database=None) -> list[FQN]:
    if database:
        in_ctx = f"DATABASE {database}"
        user_databases = None
    else:
        in_ctx = "ACCOUNT"
        user_databases = _list_databases(session)
    try:
        show_result = execute(session, f"SHOW SCHEMAS IN {in_ctx}", cacheable=True)
        schemas = []
        for row in show_result:
            # Skip system databases
            if row["database_name"] in SYSTEM_DATABASES:
                continue
            # Skip system schemas
            if row["name"] == "INFORMATION_SCHEMA":
                continue
            # Skip database shares
            if database is None and row["database_name"] not in (user_databases or []):
                continue
            schemas.append(
                FQN(
                    database=resource_name_from_snowflake_metadata(row["database_name"]),
                    name=resource_name_from_snowflake_metadata(row["name"]),
                )
            )
        return schemas
    except ProgrammingError as err:
        if err.errno == OBJECT_DOES_NOT_EXIST_ERR:
            return []
        raise


def list_secrets(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "SECRETS")


def list_security_integrations(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW SECURITY INTEGRATIONS", cacheable=True)
    integrations = []
    for row in show_result:
        if row["name"] in SYSTEM_SECURITY_INTEGRATIONS:
            continue
        integrations.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return integrations


def list_sequences(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "SEQUENCES")


def list_shares(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW SHARES", cacheable=True)
    shares = []
    for row in show_result:
        if row["kind"] == "INBOUND":
            continue
        shares.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return shares


def list_stages(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW STAGES IN ACCOUNT", cacheable=True)
    stages = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES:
            continue
        if row["type"] not in ("EXTERNAL", "INTERNAL", "INTERNAL NO CSE"):
            continue
        stages.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return stages


def list_storage_integrations(session: SnowflakeConnection) -> list[FQN]:
    return list_account_scoped_resource(session, "STORAGE INTEGRATIONS")


def list_streams(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "STREAMS")


def list_tables(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW TABLES IN ACCOUNT", cacheable=True)
    user_databases = _list_databases(session)
    tables = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES:
            continue
        if row["schema_name"] == "INFORMATION_SCHEMA":
            continue
        if row["database_name"] not in user_databases:
            continue
        if (
            row["is_external"] == "Y"
            or row["is_hybrid"] == "Y"
            or row["is_iceberg"] == "Y"
            or row["is_dynamic"] == "Y"
            or row["is_event"] == "Y"
        ):
            continue
        tables.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return tables


def list_tag_references(session: SnowflakeConnection) -> list[FQN]:
    # FIXME
    # This function previously relied on a system table function with 2 hours of latency.

    try:
        # show_result = execute(session, "SHOW TAGS IN ACCOUNT")
        tag_references: list[FQN] = []
        # for tag in show_result:
        #     if tag["database_name"] in SYSTEM_DATABASES or tag["schema_name"] == "INFORMATION_SCHEMA":
        #         continue

        #     tag_refs = execute(
        #         session,
        #         f"""
        #             SELECT *
        #             FROM table(snowflake.account_usage.tag_references_with_lineage(
        #                 '{tag['database_name']}.{tag['schema_name']}.{tag['name']}'
        #             ))
        #         """,
        #     )

        #     for ref in tag_refs:
        #         if ref["OBJECT_DELETED"] is not None:
        #             continue

        #         tag_references.append(
        #             FQN(
        #                 database=resource_name_from_snowflake_metadata(ref["TAG_DATABASE"]),
        #                 schema=resource_name_from_snowflake_metadata(ref["TAG_SCHEMA"]),
        #                 name=resource_name_from_snowflake_metadata(ref["TAG_NAME"]),
        #             )
        #         )

        return tag_references

    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return []
        else:
            raise


def list_tags(session: SnowflakeConnection) -> list[FQN]:
    try:
        show_result = execute(session, "SHOW TAGS IN ACCOUNT", cacheable=True)
        tags = []
        for row in show_result:
            if row["database_name"] in SYSTEM_DATABASES or row["schema_name"] == "INFORMATION_SCHEMA":
                continue
            tags.append(
                FQN(
                    database=resource_name_from_snowflake_metadata(row["database_name"]),
                    schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                    name=resource_name_from_snowflake_metadata(row["name"]),
                )
            )
        return tags
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return []
        else:
            raise


def list_tasks(session: SnowflakeConnection) -> list[FQN]:
    return list_schema_scoped_resource(session, "TASKS")


def list_users(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW USERS", cacheable=True)
    users = []
    for row in show_result:
        if row["name"] in SYSTEM_USERS:
            continue
        users.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return users


def list_views(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW VIEWS IN ACCOUNT", cacheable=True)
    views = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES or row["schema_name"] == "INFORMATION_SCHEMA":
            continue
        if row["is_materialized"] == "true":
            continue
        views.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return views


def list_warehouses(session: SnowflakeConnection) -> list[FQN]:
    show_result = execute(session, "SHOW WAREHOUSES", cacheable=True)
    warehouses = []
    for row in show_result:
        if row["name"].startswith("SYSTEM$"):
            continue
        warehouses.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return warehouses
