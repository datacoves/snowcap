import json
import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import (
    Any,
    Generator,
    Optional,
    Sequence,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)

import snowflake.connector
from inflection import pluralize

from . import data_provider, lifecycle
from .blueprint_config import BlueprintConfig
from .builtins import SYSTEM_ROLES
from .client import (
    ALREADY_EXISTS_ERR,
    DOES_NOT_EXIST_ERR,
    INVALID_GRANT_ERR,
    execute,
    reset_cache,
)
from .data_provider import SessionContext
from .enums import (
    INHERITED_GRANTS_FEATURE_FLAG,
    OWNER_EXECUTED_RESOURCE_TYPES,
    AccountEdition,
    BlueprintScope,
    GrantType,
    ResourceType,
    resource_type_is_grant,
)
from .error_formatting import (
    format_missing_container_error,
    format_missing_pointer_error,
    format_missing_resource_error,
)
from .exceptions import (
    DuplicateResourceException,
    InvalidResourceException,
    MarkedForReplacementException,
    MissingPrivilegeException,
    MissingResourceException,
    NonConformingPlanException,
    NotADAGException,
    OrphanResourceException,
)
from .identifiers import URN, parse_identifier, parse_URN, resource_label_for_type, smart_split
from .privs import AccountPriv, CREATE_PRIV_FOR_RESOURCE_TYPE, system_role_for_priv
from .resource_name import ResourceName
from .resource_tags import ResourceTags
from .resources import Database, Grant, RoleGrant, Schema
from .resources.database import public_schema_urn
from .resources.grant import INHERITED_GRANT_DOCS, _Grant, grant_on_clause
from .resources.resource import (
    RESOURCE_SCOPES,
    NamedResource,
    Resource,
    ResourceContainer,
    ResourceLifecycleConfig,
    ResourcePointer,
    infer_role_type_from_name,
)
from .resources.role import Role
from .resources.shared_database import SharedDatabase
from .resources.tag import Tag, TaggableResource
from .scope import (
    AccountScope,
    DatabaseScope,
    OrganizationScope,
    SchemaScope,
    TableScope,
)

T = TypeVar("T")
ResourceRef = Union[tuple[ResourceType, str, tuple[tuple[str, str], ...]], str]


logger = logging.getLogger("snowcap")


def resource_urn_needs_params(urn: URN, manifest: "Manifest") -> bool:
    """
    Check if a specific URN needs parameter fields fetched.

    This checks if the resource at this URN has any parameter fields with non-None values.
    If it doesn't, we can skip the expensive SHOW PARAMETERS query.

    Args:
        urn: The URN to check
        manifest: The manifest to check

    Returns:
        True if this resource needs parameter data, False otherwise
    """
    resource_label = resource_label_for_type(urn.resource_type)
    param_fields = data_provider.PARAMETER_FIELDS.get(resource_label, set())
    if not param_fields:
        return True  # No optimization for this resource type

    # Check if this specific URN is in the manifest with param fields
    if urn not in manifest.urns:
        return False  # Not in manifest, skip params

    item = manifest[urn]
    if isinstance(item, ManifestResource):
        # Skip implicit resources (like PUBLIC schema created by database)
        if item.implicit:
            return False
        # Only need params if this resource has non-None param field values
        for field in param_fields:
            if field in item.data and item.data[field] is not None:
                return True
    return False


def resource_type_needs_params(resource_type: ResourceType, manifest: "Manifest") -> bool:
    """
    Check if any resource of this type in the manifest specifies parameter fields
    with non-None values.

    This is used to optimize fetch_remote_state by skipping expensive SHOW PARAMETERS
    queries when no resource of that type in the manifest needs the parameter data.

    Args:
        resource_type: The type of resource to check
        manifest: The manifest to check

    Returns:
        True if any resource of this type needs parameter data, False otherwise
    """
    resource_label = resource_label_for_type(resource_type)
    param_fields = data_provider.PARAMETER_FIELDS.get(resource_label, set())
    if not param_fields:
        return True  # No optimization for this resource type

    # For schemas, we use per-URN optimization via schema_urn_needs_params() instead
    # of type-level optimization. Return True here to avoid the type-level cache blocking
    # individual schema checks.
    if resource_type == ResourceType.SCHEMA:
        return True  # Delegate to per-URN check

    # Check all resources of this type in manifest
    for urn in manifest.urns:
        if urn.resource_type != resource_type:
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            # Skip implicit resources (like PUBLIC schema created by database)
            if item.implicit:
                continue
            # Only consider fields that have non-None values
            # Fields with None values indicate the user didn't explicitly set them
            for field in param_fields:
                if field in item.data and item.data[field] is not None:
                    return True  # At least one resource specifies a non-None parameter field
    return False  # No resources of this type specify parameter fields with values


def databases_with_param_fields(manifest: "Manifest") -> set:
    """
    Return the set of database names that have param fields set in the manifest.
    Used to determine which PUBLIC schemas need param fetching (they inherit from database).
    """
    db_param_fields = data_provider.PARAMETER_FIELDS.get("database", set())
    databases = set()
    for urn in manifest.urns:
        if urn.resource_type != ResourceType.DATABASE:
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            for field in db_param_fields:
                if field in item.data and item.data[field] is not None:
                    databases.add(str(urn.fqn.name).upper())
                    break
    return databases


def schema_urn_needs_params(urn: URN, manifest: "Manifest", db_with_params: set) -> bool:
    """
    Check if a specific schema URN needs parameter fields fetched.

    A schema needs params if:
    1. The schema is in the manifest with param fields set, OR
    2. The schema is PUBLIC and its parent database has param fields (inheritance)

    Args:
        urn: The schema URN to check
        manifest: The manifest to check against
        db_with_params: Set of database names that have param fields set

    Returns:
        True if this schema needs params fetched, False otherwise
    """
    schema_param_fields = data_provider.PARAMETER_FIELDS.get("schema", set())

    # Check if this schema is in manifest with param fields
    if urn in manifest.urns:
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            for field in schema_param_fields:
                if field in item.data and item.data[field] is not None:
                    return True

    # Check if this is a PUBLIC schema whose database has param fields
    schema_name = str(urn.fqn.name).upper()
    if schema_name == "PUBLIC":
        db_name = str(urn.fqn.database).upper() if urn.fqn.database else None
        if db_name and db_name in db_with_params:
            return True

    return False


def manifest_has_future_grants(manifest: "Manifest") -> bool:
    """
    Check if the manifest contains any future grants.

    This is used to optimize list_grants by skipping expensive SHOW FUTURE GRANTS
    queries when the manifest doesn't define any future grants.
    """
    for urn in manifest.urns:
        if urn.resource_type != ResourceType.GRANT:
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            if item.data.get("grant_type") == "FUTURE":
                return True
    return False


def manifest_future_grant_roles(manifest: "Manifest") -> set:
    """
    Return the set of account role names that have future grants in the manifest.

    This is used to optimize SHOW FUTURE GRANTS by only querying roles
    that actually have future grants defined in the manifest.

    Note: This only returns account roles (not database roles).
    Use manifest_future_grant_database_roles() for database roles.
    """
    roles = set()
    for urn in manifest.urns:
        if urn.resource_type != ResourceType.GRANT:
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            if item.data.get("grant_type") == "FUTURE":
                # The "to" field contains the role name (FQN string)
                to = item.data.get("to", "")
                if to:
                    # Handle both formats: "role/SOME_ROLE" or "database_role/DB.ROLE"
                    if "/" in to:
                        prefix, role_name = to.split("/", 1)
                        # Only include account roles, not database roles
                        if prefix.lower() == "database_role":
                            continue
                    else:
                        role_name = to
                    roles.add(role_name.upper())
    return roles


FUTURE_GRANT_PRECEDENCE_DOCS = (
    "https://docs.snowflake.com/en/sql-reference/sql/grant-privilege#future-grants-on-database-or-schema-objects"
)


def _future_grant_scopes(entries) -> tuple[set[str], list[dict], set[tuple[str, str]]]:
    """
    Bucket resources into the three things the managed-access check needs:
    managed access schemas, database-level future grants, and schema-level future grants.

    `entries` is an iterable of (urn, resource_type, data) so the check can run over a
    manifest, over remote state, or over a plan without caring which it was handed.
    """
    managed_access_schemas: set[str] = set()
    database_future_grants: list[dict] = []
    schema_future_grants: set[tuple[str, str]] = set()

    for urn, resource_type, data in entries:
        if resource_type == ResourceType.SCHEMA:
            if data.get("managed_access") and urn.fqn.database:
                managed_access_schemas.add(f"{urn.fqn.database}.{urn.fqn.name}".upper())
        elif resource_type == ResourceType.GRANT:
            if data.get("grant_type") != GrantType.FUTURE.value:
                continue
            items_type = str(data.get("items_type") or "").upper()
            on_type = str(data.get("on_type") or "").upper()
            on = str(data.get("on") or "").upper()
            if not items_type or not on:
                continue
            if on_type == ResourceType.DATABASE.value:
                database_future_grants.append(
                    {
                        "database": on,
                        "items_type": items_type,
                        "priv": str(data.get("priv") or ""),
                        # `to` is a bare role name or a labelled FQN (database_role/DB.ROLE).
                        "to": str(data.get("to") or "").split("/", 1)[-1],
                    }
                )
            elif on_type == ResourceType.SCHEMA.value:
                schema_future_grants.add((on, items_type))

    return managed_access_schemas, database_future_grants, schema_future_grants


def _format_schema_list(schemas: Sequence[str]) -> str:
    shown = list(schemas[:3])
    remainder = len(schemas) - len(shown)
    if remainder > 0:
        return f"{', '.join(shown)} (and {remainder} more)"
    return ", ".join(shown)


def future_grant_precedence_warnings(entries) -> list[str]:
    """
    Warn about database-level future grants that Snowflake will silently ignore.

    Snowflake gives schema-level future grants precedence over database-level future
    grants on the same object type: when both exist, the database-level grant is ignored
    for that schema, and objects created there never receive the privilege.

    Managed access schemas are where this bites hardest. They centralize privilege
    management on the schema owner, so the schema-level future grants that shadow a
    database-level grant are typically added by a different config (or a different team)
    than the one that declared the database-level grant, and nothing surfaces the
    conflict until someone reports missing access on a newly created table.

    Note that this precedence rule is not specific to managed access schemas, and that
    managed access does not by itself disable database-level future grants -- Snowflake
    documents that they apply to regular and managed access schemas alike. The one
    managed-access-specific exception is future grants of OWNERSHIP, which Snowcap does
    not support.
    """
    managed_access_schemas, database_future_grants, schema_future_grants = _future_grant_scopes(entries)

    if not database_future_grants:
        return []

    warnings = []

    # Case 1: a schema-level future grant on the same object type already shadows the
    # database-level grant. This is a live misconfiguration, not just a risk.
    shadowed: dict[tuple[str, str, str, str], list[str]] = defaultdict(list)
    for grant in database_future_grants:
        prefix = grant["database"] + "."
        for schema_fqn, schema_items_type in schema_future_grants:
            if schema_items_type == grant["items_type"] and schema_fqn.startswith(prefix):
                key = (grant["database"], grant["items_type"], grant["priv"], grant["to"])
                shadowed[key].append(schema_fqn)

    for (database, items_type, priv, to), schemas in sorted(shadowed.items()):
        schemas = sorted(set(schemas))
        managed = [schema for schema in schemas if schema in managed_access_schemas]
        managed_note = (
            f" {_format_schema_list(managed)} {'is a' if len(managed) == 1 else 'are'} managed access "
            f"{'schema' if len(managed) == 1 else 'schemas'}."
            if managed
            else ""
        )
        warnings.append(
            f"{priv} ON FUTURE {pluralize(items_type).upper()} IN DATABASE {database} to {to} is ignored for "
            f"{_format_schema_list(schemas)}, which define their own future grants on {items_type}. "
            f"Snowflake gives schema-level future grants precedence over database-level future grants on the "
            f"same object type, so objects created in those schemas will not receive this privilege."
            f"{managed_note} Declare the privilege as a schema-level future grant on those schemas. "
            f"See {FUTURE_GRANT_PRECEDENCE_DOCS}"
        )

    # Case 2: managed access schemas covered only by database-level future grants. Not
    # broken today, but a single schema-level future grant on the same object type --
    # added anywhere, by anyone -- silently switches the privilege off for that schema.
    databases_with_managed_access: dict[str, list[str]] = defaultdict(list)
    for schema_fqn in managed_access_schemas:
        databases_with_managed_access[schema_fqn.split(".", 1)[0]].append(schema_fqn)

    at_risk: dict[str, set[str]] = defaultdict(set)
    for grant in database_future_grants:
        database = grant["database"]
        if database not in databases_with_managed_access:
            continue
        if (database, grant["items_type"], grant["priv"], grant["to"]) in shadowed:
            continue
        at_risk[database].add(grant["items_type"])

    for database, items_types in sorted(at_risk.items()):
        schemas = sorted(databases_with_managed_access[database])
        types = ", ".join(pluralize(items_type).upper() for items_type in sorted(items_types))
        warnings.append(
            f"Database {database} grants access to {types} with database-level future grants and contains "
            f"managed access {'schema' if len(schemas) == 1 else 'schemas'} {_format_schema_list(schemas)}. "
            f"Adding a schema-level future grant on the same object type to any of those schemas silently "
            f"disables the database-level grant for that schema. Declaring future grants at the schema level "
            f"alongside managed_access is the durable pattern. See {FUTURE_GRANT_PRECEDENCE_DOCS}"
        )

    return warnings


def _container_covers(container_type: str, container: str, object_name: str) -> bool:
    """Does a container hold the named object, by identifier alone?"""
    if container_type == ResourceType.ACCOUNT.value:
        return True
    # Quote-aware split: a quoted identifier can contain a literal dot (e.g. "a.b"), which a
    # plain str.split would miscount and mis-classify.
    parts = smart_split(object_name, ".")
    if container_type == ResourceType.SCHEMA.value:
        return len(parts) == 3 and ResourceName(".".join(parts[:2])) == ResourceName(container)
    if container_type == ResourceType.DATABASE.value:
        return len(parts) >= 2 and ResourceName(parts[0]) == ResourceName(container)
    return False


def _covered_by_collection_grant(collection_grants: list["ManifestResource"], remote_res: dict) -> bool:
    """
    Is a remote per-object grant already provided by a declared ALL or INHERITED grant?

    Snowflake materializes `GRANT ... ON ALL` into one grant per object, and those per-object
    grants show up in remote state with nothing in the manifest to match them. Dropping them
    would undo the collection grant on every apply. Inherited grants are matched the same
    way so that migrating a config from per-object grants to an inherited grant does not
    revoke access in the same run that establishes it.
    """
    if remote_res.get("grant_type") != GrantType.OBJECT.value:
        return False
    for grant in collection_grants:
        data = grant.data
        if data["to"] != remote_res["to"]:
            continue
        # A declared `GRANT ALL` fans out into a concrete-privilege row per object (SELECT,
        # INSERT, ...); matching the privilege exactly would miss those and drop them on every
        # sync. ALL covers whatever privilege the row carries. Other collection grants still
        # match their single privilege.
        if data["priv"] != "ALL" and data["priv"] != remote_res["priv"]:
            continue
        if data["items_type"] != remote_res["on_type"]:
            continue
        if _container_covers(data["on_type"], data["on"], remote_res["on"]):
            return True
    return False


def _covered_by_imported_privileges(imported_privilege_grants: list["ManifestResource"], remote_res: dict) -> bool:
    """
    Is a remote per-object grant already provided by a declared IMPORTED PRIVILEGES grant?

    `GRANT IMPORTED PRIVILEGES ON DATABASE <shared_db> TO ROLE <r>` is one statement, but
    Snowflake fans it out in SHOW GRANTS into a row per object the share exposes -- every
    view, function, procedure, schema, database role, class, tag and image repository in
    the database, plus a USAGE row on the database itself. On the SNOWFLAKE shared database
    that is several hundred rows.

    None of those rows can be in the manifest: config declares the single IMPORTED
    PRIVILEGES grant, not the fan-out. Without this check they read as undeclared grants and
    get revoked on every sync, undoing the access the declared grant just handed out.

    Unlike `_covered_by_collection_grant` this deliberately ignores the privilege. The
    fan-out rows carry whatever privilege each object type takes -- SELECT on views, USAGE
    on functions, READ on image repositories, APPLY on tags -- none of which is
    "IMPORTED PRIVILEGES". Grantee and containment are what identify them.

    Matching on containment alone is safe because IMPORTED PRIVILEGES is only grantable on a
    shared database, and objects in a shared database cannot be granted independently: the
    share is the only source of privileges on them.
    """
    if remote_res.get("grant_type") != GrantType.OBJECT.value:
        return False
    for grant in imported_privilege_grants:
        data = grant.data
        if data["to"] != remote_res["to"]:
            continue
        database = data["on"]
        # The USAGE (and REFERENCE_USAGE) row Snowflake reports on the shared database itself
        if remote_res["on_type"] == ResourceType.DATABASE.value and ResourceName(remote_res["on"]) == ResourceName(
            database
        ):
            return True
        if _container_covers(ResourceType.DATABASE.value, database, remote_res["on"]):
            return True
    return False


def manifest_inherited_grants(manifest: "Manifest") -> list["ManifestResource"]:
    """Inherited grants declared in the manifest."""
    inherited = []
    for urn in manifest.urns:
        if urn.resource_type != ResourceType.GRANT:
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource) and item.data.get("grant_type") == GrantType.INHERITED.value:
            inherited.append(item)
    return inherited


def manifest_enables_inherited_grants(manifest: "Manifest") -> bool:
    """Does the config itself turn the inherited grants preview on?"""
    for urn in manifest.urns:
        if urn.resource_type != ResourceType.ACCOUNT_PARAMETER:
            continue
        if ResourceName(urn.fqn.name) != ResourceName(INHERITED_GRANTS_FEATURE_FLAG):
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            return str(item.data.get("value", "")).strip().upper() == "ENABLED"
    return False


def raise_if_inherited_grants_unavailable(session, manifest: "Manifest") -> None:
    """
    Fail before apply when config declares inherited grants an account cannot accept.

    Inherited grants are a preview feature. Without FEATURE_RBAC_INHERITED_GRANTS every
    GRANT INHERITED statement fails as a syntax error partway through an apply, which is a
    confusing way to learn the account is not opted in.

    A config that enables the parameter itself is left alone: the flag is off at plan time
    by definition, and the apply turns it on before the grants run.
    """
    declared = manifest_inherited_grants(manifest)
    if not declared:
        return

    if manifest_enables_inherited_grants(manifest):
        return

    if data_provider.fetch_inherited_grants_enabled(session) is not False:
        return

    example = grant_on_clause(_Grant(**declared[0].data))
    message = (
        f"This config declares {len(declared)} inherited grant(s), for example '{example}', but "
        f"{INHERITED_GRANTS_FEATURE_FLAG} is not enabled on this account.\n"
    )

    # Preview access gates every preview feature at once and is normally on. When it is
    # off, setting the parameter alone will not help, so say that rather than sending the
    # operator down the wrong path.
    if data_provider.fetch_preview_access_enabled(session) is False:
        message += (
            "  Preview features are disabled account-wide. Snowcap cannot change that -- it is a\n"
            "  system function, not a resource -- so an account admin needs to run:\n"
            "    SELECT SYSTEM$ENABLE_PREVIEW_ACCESS();\n"
            "  after which Snowcap can manage the parameter itself:\n"
        )
    else:
        message += "  Let Snowcap manage it:\n"

    message += (
        "    account_parameters:\n"
        f"      - name: {INHERITED_GRANTS_FEATURE_FLAG}\n"
        "        value: ENABLED\n"
        "  Or set it directly:\n"
        f"    ALTER ACCOUNT SET {INHERITED_GRANTS_FEATURE_FLAG} = 'ENABLED';\n"
        f"  See {INHERITED_GRANT_DOCS}"
    )
    raise MissingPrivilegeException(message)


def manifest_state_entries(manifest: "Manifest", remote_state: Optional["State"] = None):
    """Yield (urn, resource_type, data) for every concrete resource in the manifest, plus
    anything remote state knows about that the manifest doesn't declare."""
    seen = set()
    for urn, item in manifest.items():
        if isinstance(item, ManifestResource):
            seen.add(urn)
            yield urn, urn.resource_type, item.data
    for urn, data in (remote_state or {}).items():
        if urn not in seen and isinstance(data, dict):
            yield urn, urn.resource_type, data


def plan_entries(plan: "Plan"):
    """Yield (urn, resource_type, data) for the resources a plan creates or updates.

    A plan only carries what is changing, so this sees less than the manifest does. It is
    the fallback for `snowcap apply --plan plan.json`, where the manifest isn't rebuilt.
    """
    for change in plan:
        if isinstance(change, (CreateResource, UpdateResource)):
            yield change.urn, change.urn.resource_type, change.after


def manifest_future_grant_database_roles(manifest: "Manifest") -> set:
    """
    Return the set of database role names that have future grants in the manifest.

    This is used to optimize SHOW FUTURE GRANTS TO DATABASE ROLE by only querying
    database roles that actually have future grants defined in the manifest.

    Returns:
        Set of fully qualified database role names (e.g., "DB.ROLE") in uppercase.
    """
    database_roles = set()
    for urn in manifest.urns:
        if urn.resource_type != ResourceType.GRANT:
            continue
        item = manifest[urn]
        if isinstance(item, ManifestResource):
            if item.data.get("grant_type") == "FUTURE":
                # The "to" field contains the role name (FQN string)
                to = item.data.get("to", "")
                if to:
                    # Handle format: "database_role/DB.ROLE"
                    if "/" in to:
                        prefix, role_name = to.split("/", 1)
                        if prefix.lower() == "database_role":
                            database_roles.add(role_name.upper())
    return database_roles


@dataclass
class ResourceChange(ABC):
    urn: URN

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        pass


ResourceOwner = ResourceName
ContainerDescriptor = tuple[URN, ResourceOwner]


@dataclass
class CreateResource(ResourceChange):
    resource_cls: type[Resource]
    container: Optional[ContainerDescriptor]
    after: dict[str, str]

    def to_dict(self) -> dict[str, Union[str, dict[str, str], None]]:
        container_dict = None
        if self.container is not None:
            container_urn, container_owner = self.container
            container_dict = {str(container_urn): str(container_owner)}
        return {
            "action": "CREATE",
            "urn": str(self.urn),
            "resource_cls": self.resource_cls.__name__,
            "container": container_dict,
            "after": self.after,
        }


@dataclass
class DropResource(ResourceChange):
    before: dict[str, str]

    def to_dict(self) -> dict[str, Union[str, dict[str, str]]]:
        return {
            "action": "DROP",
            "urn": str(self.urn),
            "before": self.before,
        }


@dataclass
class UpdateResource(ResourceChange):
    resource_cls: type[Resource]
    before: dict[str, str]
    after: dict[str, str]
    delta: dict[str, str]

    def to_dict(self) -> dict[str, Union[str, dict[str, str]]]:
        return {
            "action": "UPDATE",
            "urn": str(self.urn),
            "resource_cls": self.resource_cls.__name__,
            "before": self.before,
            "after": self.after,
            "delta": self.delta,
        }


@dataclass
class TransferOwnership(ResourceChange):
    resource_cls: type[Resource]
    from_owner: str
    to_owner: str

    def to_dict(self) -> dict[str, str]:
        return {
            "action": "TRANSFER",
            "urn": str(self.urn),
            "resource_cls": self.resource_cls.__name__,
            "from_owner": self.from_owner,
            "to_owner": self.to_owner,
        }


State = dict[URN, dict]
Plan = list[ResourceChange]


def plan_from_dict(plan_dict) -> Plan:
    # A plan file is either a bare list of changes (older format) or {"changes": [...],
    # "levels": {...}} once dependency levels are persisted alongside it.
    changes_data = plan_dict.get("changes", []) if isinstance(plan_dict, dict) else plan_dict
    changes: list[ResourceChange] = []
    for change in changes_data:
        action = change["action"]
        if action == "CREATE":
            container_descriptor: Optional[ContainerDescriptor] = None
            if change.get("container"):
                for urn, owner in change["container"].items():
                    container_descriptor = (parse_URN(urn), ResourceName(owner))
            changes.append(
                CreateResource(
                    urn=parse_URN(change["urn"]),
                    resource_cls=Resource.__classes__[change["resource_cls"]],
                    container=container_descriptor,
                    after=change["after"],
                )
            )
        elif action == "DROP":
            changes.append(
                DropResource(
                    urn=parse_URN(change["urn"]),
                    before=change["before"],
                )
            )
        elif action == "UPDATE":
            changes.append(
                UpdateResource(
                    urn=parse_URN(change["urn"]),
                    resource_cls=Resource.__classes__[change["resource_cls"]],
                    before=change["before"],
                    after=change["after"],
                    delta=change["delta"],
                )
            )
        elif action == "TRANSFER":
            changes.append(
                TransferOwnership(
                    urn=parse_URN(change["urn"]),
                    resource_cls=Resource.__classes__[change["resource_cls"]],
                    from_owner=change["from_owner"],
                    to_owner=change["to_owner"],
                )
            )
        else:
            raise Exception(f"Unsupported action {action}")
    return changes


@dataclass
class ManifestResource:
    urn: URN
    resource_cls: type[Resource]
    data: dict[str, Any]
    implicit: bool
    lifecycle: ResourceLifecycleConfig


class Manifest:
    def __init__(self, account_locator: str = ""):
        self._account_locator = account_locator
        self._resources: dict[URN, Union[ManifestResource, ResourcePointer]] = {}
        self._refs: list[tuple[URN, URN]] = []

    def __getitem__(self, key: URN):
        if isinstance(key, URN):
            return self._resources[key]
        else:
            raise Exception("Manifest keys must be URNs")

    def __contains__(self, key: URN):
        if isinstance(key, URN):
            return key in self._resources
        else:
            raise Exception("Manifest keys must be URNs")

    def add(self, resource: Resource, account_edition: AccountEdition):

        urn = URN.from_resource(
            account_locator=self._account_locator,
            resource=resource,
        )

        if urn in self._resources:
            if not isinstance(resource, ResourcePointer):
                logger.warning(f"Duplicate resource {urn} with conflicting data, discarding {resource}")
            return
        if isinstance(resource, ResourcePointer):
            self._resources[urn] = resource
        else:
            self._resources[urn] = ManifestResource(
                urn,
                resource.__class__,
                resource.to_dict(account_edition),
                resource.implicit,
                resource.lifecycle,
            )
        for ref in resource.refs:
            ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
            self._refs.append((urn, ref_urn))

    def get(self, key: URN, default=None):
        if isinstance(key, URN):
            return self._resources.get(key, default)
        else:
            raise Exception("Manifest keys must be URNs")

    def items(self):
        return self._resources.items()

    def __repr__(self):
        contents = ""
        for urn, resource in self._resources.items():
            contents += f"[{urn}] =>\n"
            contents += f"  {resource}\n"
        return f"Manifest({len(self._resources)} resources)\n{contents}"

    @property
    def urns(self) -> list[URN]:
        return list(self._resources.keys())

    @property
    def refs(self):
        return self._refs

    @property
    def resources(self):
        return list(self._resources.values())


def dump_plan(plan: Plan, format: str = "json", levels: Optional[dict[URN, int]] = None):
    if format == "json":
        changes = [change.to_dict() for change in plan]
        if levels is None:
            return json.dumps(changes, indent=2)
        # Persist each change's dependency level so `apply --plan` preserves ordering (ownership
        # transfers before creates inside them, the inherited-grants feature flag before the
        # grants that need it). Without it the apply-plan path has no levels and runs everything
        # at level 0. Older plan files (a bare change list) fall back to that flat behaviour.
        payload = {
            "changes": changes,
            "levels": {str(change.urn): levels.get(change.urn, 0) for change in plan},
        }
        return json.dumps(payload, indent=2)
    elif format == "text":
        return _dump_plan_text(plan)
    else:
        raise Exception(f"Unsupported format {format}")


def levels_from_plan_dict(plan_dict) -> dict[URN, int]:
    """Dependency levels persisted alongside a plan by dump_plan. Empty for older plan files
    (a bare change list), so apply falls back to running everything at level 0."""
    if not isinstance(plan_dict, dict):
        return {}
    return {parse_URN(urn): level for urn, level in plan_dict.get("levels", {}).items()}


def _render_value(value):
    """Render a value for display in plan output."""
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def _summarize_plan_value(value, max_len: int = 60) -> str:
    """Render a delta value for the plan table on a single, bounded line.

    A multiline or long field (an alert THEN block, a task body, a view definition)
    dumped verbatim carries newlines the box table can't lay out and stretches the
    columns off-screen. Short scalars (state, schedule, comment) print as-is; a body
    is shown as a compact shape instead of its SQL, so the plan stays readable.
    """
    if value is None:
        return ""
    text = str(value)
    if "\n" not in text and len(text) <= max_len:
        return text
    lines = text.count("\n") + 1
    unit = "line" if lines == 1 else "lines"
    return f"<{lines} {unit}, {len(text)} chars>"


def _render_table(rows: list[list[str]], headers: list[str]) -> str:
    """
    Render a table with box-drawing characters.

    Args:
        rows: List of rows, each row is a list of cell values
        headers: List of column headers

    Returns:
        Formatted table string with box drawing characters
    """
    if not rows:
        return ""

    # Calculate column widths
    num_cols = len(headers)
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    # Build the table
    lines = []

    # Top border
    top_border = "┌" + "┬".join("─" * (w + 2) for w in col_widths) + "┐"
    lines.append(top_border)

    # Header row
    header_cells = [f" {headers[i]:<{col_widths[i]}} " for i in range(num_cols)]
    lines.append("│" + "│".join(header_cells) + "│")

    # Header separator
    header_sep = "├" + "┼".join("─" * (w + 2) for w in col_widths) + "┤"
    lines.append(header_sep)

    # Data rows
    for row in rows:
        cells = [f" {str(row[i]):<{col_widths[i]}} " for i in range(num_cols)]
        lines.append("│" + "│".join(cells) + "│")

    # Bottom border
    bottom_border = "└" + "┴".join("─" * (w + 2) for w in col_widths) + "┘"
    lines.append(bottom_border)

    return "\n".join(lines)


def _get_resource_type_order(resource_type: ResourceType) -> tuple[int, str]:
    """
    Return a sort key for resource types.
    Account-level resources come first, then database, then schema-level.
    """
    # Define ordering groups
    account_level = {
        ResourceType.ACCOUNT,
        ResourceType.ACCOUNT_PARAMETER,
        ResourceType.ROLE,
        ResourceType.USER,
        ResourceType.WAREHOUSE,
        ResourceType.RESOURCE_MONITOR,
        ResourceType.NETWORK_POLICY,
        ResourceType.SHARE,
        ResourceType.STORAGE_INTEGRATION,
        ResourceType.API_INTEGRATION,
        ResourceType.NOTIFICATION_INTEGRATION,
        ResourceType.SECURITY_INTEGRATION,
        ResourceType.EXTERNAL_ACCESS_INTEGRATION,
        ResourceType.EXTERNAL_VOLUME,
        ResourceType.COMPUTE_POOL,
        ResourceType.FAILOVER_GROUP,
        ResourceType.REPLICATION_GROUP,
        ResourceType.CATALOG_INTEGRATION,
        ResourceType.AUTHENTICATION_POLICY,
        ResourceType.PASSWORD_POLICY,
        ResourceType.PACKAGES_POLICY,
    }
    database_level = {
        ResourceType.DATABASE,
        ResourceType.DATABASE_ROLE,
        ResourceType.SCHEMA,
    }
    grant_types = {
        ResourceType.GRANT,
        ResourceType.ROLE_GRANT,
        ResourceType.DATABASE_ROLE_GRANT,
    }

    # Return sort key: (group_order, resource_type_name)
    if resource_type in account_level:
        return (0, str(resource_type))
    elif resource_type in database_level:
        return (1, str(resource_type))
    elif resource_type in grant_types:
        return (3, str(resource_type))  # Grants come last
    else:
        return (2, str(resource_type))  # Schema-level resources


def _format_grant_name(urn: URN, change: "ResourceChange") -> str:
    """
    Format a grant URN into a readable format.
    Example: USAGE on WAREHOUSE.REPORTING → ROLE.ANALYST
    Example (future grant): SELECT on FUTURE TABLES in DATABASE.MYDB → ROLE.ANALYST
    Example (role grant): ROLE.ANALYST → ROLE.SYSADMIN
    """
    # Get grant details from the change
    if isinstance(change, CreateResource):
        data = change.after
    elif isinstance(change, DropResource):
        data = change.before
    else:
        data = getattr(change, "after", {}) or getattr(change, "before", {})

    resource_type = urn.resource_type

    # Handle role grants (ROLE_GRANT, DATABASE_ROLE_GRANT)
    if resource_type == ResourceType.ROLE_GRANT:
        role = data.get("role", "")
        to_role = data.get("to_role", "")
        to_user = data.get("to_user", "")
        if role:
            if to_role:
                return f"ROLE.{role} → ROLE.{to_role}"
            elif to_user:
                return f"ROLE.{role} → USER.{to_user}"
        return str(urn.fqn.name)

    if resource_type == ResourceType.DATABASE_ROLE_GRANT:
        role = data.get("role", "")
        to_role = data.get("to_role", "")
        to_database_role = data.get("to_database_role", "")
        if role:
            if to_role:
                return f"DATABASE_ROLE.{role} → ROLE.{to_role}"
            elif to_database_role:
                return f"DATABASE_ROLE.{role} → DATABASE_ROLE.{to_database_role}"
        return str(urn.fqn.name)

    # Handle regular grants
    priv = data.get("priv", "")
    on_type = data.get("on_type", "")
    on = data.get("on", "")
    to_type = data.get("to_type", "")
    to = data.get("to", "")
    grant_type = data.get("grant_type", "")
    items_type = data.get("items_type", "")

    if priv and to:
        to_type_str = str(to_type).replace("ResourceType.", "").upper() if to_type else "ROLE"

        # Handle FUTURE and ALL grants
        grant_type_str = str(grant_type).replace("GrantType.", "").upper() if grant_type else "OBJECT"

        if grant_type_str in ("FUTURE", "ALL", "INHERITED") and items_type:
            # Format: SELECT on FUTURE TABLES in DATABASE.MYDB → ROLE.X
            items_type_str = str(items_type).replace("ResourceType.", "").upper()
            # Pluralize the items type
            items_plural = items_type_str + "S" if not items_type_str.endswith("S") else items_type_str
            on_type_str = str(on_type).replace("ResourceType.", "").upper() if on_type else ""
            # The account container has no name of its own
            container = on_type_str if on_type_str == "ACCOUNT" else f"{on_type_str}.{on}"
            return f"{priv} on {grant_type_str} {items_plural} in {container} → {to_type_str}.{to}"

        elif on_type:
            # Regular object grant: SELECT on TABLE.MYTABLE → ROLE.X
            on_type_str = str(on_type).replace("ResourceType.", "").upper()
            return f"{priv} on {on_type_str}.{on} → {to_type_str}.{to}"

    # Fallback to FQN name
    return str(urn.fqn.name)


def _format_resource_name(urn: URN, change: "ResourceChange") -> str:
    """
    Extract a clean resource name from a URN and change.
    For grants, returns a readable format.
    For other resources, returns the resource name with key properties.
    """
    resource_type = urn.resource_type

    # Handle grants specially
    if resource_type in (ResourceType.GRANT, ResourceType.ROLE_GRANT, ResourceType.DATABASE_ROLE_GRANT):
        return _format_grant_name(urn, change)

    # For other resources, use the FQN
    fqn = urn.fqn
    if fqn.database and fqn.schema:
        name = f"{fqn.database}.{fqn.schema}.{fqn.name}"
    elif fqn.database:
        name = f"{fqn.database}.{fqn.name}"
    else:
        name = str(fqn.name)

    # Include params for resources that use them (like tag_masking_policy_reference)
    if fqn.params:
        params_str = ", ".join(f"{k}={v}" for k, v in fqn.params.items())
        name += f" ({params_str})"

    return name


def _get_key_properties(change: "ResourceChange", resource_type: ResourceType) -> str:
    """Get key properties to display inline for CREATE actions."""
    if not isinstance(change, CreateResource):
        return ""

    data = change.after
    props = []

    # Skip owner for grants since the relationship is shown in the name
    is_grant = resource_type in (
        ResourceType.GRANT,
        ResourceType.ROLE_GRANT,
        ResourceType.DATABASE_ROLE_GRANT,
    )

    # Show owner if present (but not for grants)
    if not is_grant and "owner" in data and data["owner"]:
        props.append(f"owner: {data['owner']}")

    # Show size for warehouses (adaptive warehouses have no size, so warehouse_size is None)
    if data.get("warehouse_size") is not None:
        props.append(f"size: {data['warehouse_size']}")

    if props:
        return f" ({', '.join(props)})"
    return ""


def _dump_plan_text(plan: Plan) -> str:
    """
    Generate improved text output for a plan.

    Groups changes by resource type with:
    - Section headers for each resource type
    - Compact single-line format for creates/drops
    - Before/After tables for updates
    """
    # Datacoves brand colors (blue/cyan)
    blue = "\033[94m"
    cyan = "\033[96m"
    green = "\033[92m"
    red = "\033[91m"
    yellow = "\033[93m"
    dim = "\033[2m"
    reset = "\033[0m"

    # Count changes by type
    create_count = len([c for c in plan if isinstance(c, CreateResource)])
    update_count = len([c for c in plan if isinstance(c, UpdateResource)])
    transfer_count = len([c for c in plan if isinstance(c, TransferOwnership)])
    drop_count = len([c for c in plan if isinstance(c, DropResource)])

    output = f"\n{cyan}»{reset} {blue}snowcap{reset}\n"
    output += f"{cyan}»{reset} Plan: {create_count} to create, {update_count} to update, {transfer_count} to transfer, {drop_count} to drop.\n"

    if not plan:
        return output + "\n"

    # Group changes by resource type
    changes_by_type: dict[ResourceType, list[ResourceChange]] = defaultdict(list)
    for change in plan:
        changes_by_type[change.urn.resource_type].append(change)

    # Sort resource types
    sorted_types = sorted(changes_by_type.keys(), key=_get_resource_type_order)

    for resource_type in sorted_types:
        changes = changes_by_type[resource_type]

        # Section header
        type_label = str(resource_type).upper().replace(" ", "_") + "S"
        header_line = f"━━━ {type_label} "
        header_line += "━" * (70 - len(header_line))
        output += f"\n{header_line}\n"

        # Track if we have ALL grants in this section
        has_all_grants = False

        for change in changes:
            name = _format_resource_name(change.urn, change)

            # Check for ALL grants
            if resource_type == ResourceType.GRANT and isinstance(change, CreateResource):
                grant_type = change.after.get("grant_type", "")
                grant_type_str = str(grant_type).replace("GrantType.", "").upper()
                if grant_type_str == "ALL":
                    has_all_grants = True

            if isinstance(change, CreateResource):
                props = _get_key_properties(change, resource_type)
                output += f"{green}+ CREATE:{reset} {name}{props}\n"

            elif isinstance(change, DropResource):
                output += f"{red}- DROP:{reset}   {name}\n"

            elif isinstance(change, UpdateResource):
                output += f"{yellow}~ UPDATE:{reset} {name}\n"
                # Build table for changed properties
                rows = []
                for key, new_value in change.delta.items():
                    if key.startswith("_"):
                        continue
                    before = change.before.get(key, "")
                    rows.append(
                        [
                            key,
                            _summarize_plan_value(before),
                            _summarize_plan_value(new_value),
                        ]
                    )

                if rows:
                    table = _render_table(rows, ["Property", "Before", "After"])
                    # Indent table
                    indented_table = "\n".join("  " + line for line in table.split("\n"))
                    output += indented_table + "\n"

            elif isinstance(change, TransferOwnership):
                output += f"{yellow}~ TRANSFER:{reset} {name}\n"
                # Build table for owner change
                rows = [
                    [
                        "owner",
                        str(change.from_owner) if change.from_owner else "",
                        str(change.to_owner) if change.to_owner else "",
                    ]
                ]
                table = _render_table(rows, ["Property", "Before", "After"])
                indented_table = "\n".join("  " + line for line in table.split("\n"))
                output += indented_table + "\n"

        # Add note about ALL grants if present
        if has_all_grants:
            output += f'\n{dim}Note: "ALL" grants always appear in the plan because Snowflake converts them{reset}\n'
            output += f"{dim}to individual object grants. They are idempotent and safe to apply.{reset}\n"

    output += "\n"
    return output


def print_plan(plan: Plan):
    print(dump_plan(plan, format="text"))


def print_surviving_drops(survivors: list["ResourceChange"]):
    """
    Report drops Snowflake accepted without carrying out.

    Printed rather than logged: a logger warning scrolls past in the middle of an apply,
    and the whole problem with these is that nothing tells you they happened. Formatted the
    way the plan formats the same change, so the line reads as the one the user just saw
    under DROP rather than as a raw URN.
    """
    if not survivors:
        return

    yellow = "\033[93m"
    reset = "\033[0m"
    print(f"\n{yellow}!{reset} {len(survivors)} drop(s) reported success but the resource is still there:\n")
    for change in survivors:
        print(f"    {_format_resource_name(change.urn, change)}")
    print(
        "\n  Snowflake accepted these statements without carrying them out. A REVOKE does that\n"
        "  when the executing role does not own the privilege or cannot resolve the grantee.\n"
        "  They will appear in the next plan as well. Check what granted the privilege\n"
        "  (SHOW GRANTS ... , granted_by) and whether that role is available to this session.\n"
    )
    # Database-role grantees are the common cause: no held role could resolve one without
    # USAGE on its database, so the revoke ran as SECURITYADMIN and silently did nothing.
    # Name the role that would work.
    db_role_databases = sorted({d for d in (_database_of_database_role_grantee(c) for c in survivors) if d})
    if db_role_databases:
        print(
            "  Some are held by database roles, revocable only by a role that owns their\n"
            "  database. Grant your user the owner of: " + ", ".join(db_role_databases) + "\n"
        )


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def _split_by_scope(
    resources: list[Resource],
) -> tuple[list[Resource], list[Resource], list[Resource], list[Resource]]:
    org_scoped: list[Resource] = []
    acct_scoped: list[Resource] = []
    db_scoped: list[Resource] = []
    schema_scoped: list[Resource] = []

    seen = set()

    def route(resource: Resource):
        """The sorting hat"""

        if id(resource) not in seen:
            if isinstance(resource.scope, OrganizationScope):
                org_scoped.append(resource)
            elif isinstance(resource.scope, AccountScope):
                acct_scoped.append(resource)
            elif isinstance(resource.scope, DatabaseScope):
                db_scoped.append(resource)
            elif isinstance(resource.scope, SchemaScope):
                schema_scoped.append(resource)
            else:
                raise Exception(f"Unsupported resource type {type(resource)}")

        seen.add(id(resource))
        if isinstance(resource, ResourceContainer):
            for item in resource.items():
                route(item)

    for resource in resources:
        root = resource
        while getattr(root, "container", None) is not None:
            root = getattr(root, "container")
        route(root)
    return org_scoped, acct_scoped, db_scoped, schema_scoped


def _walk(resource: Resource) -> Generator[Resource, None, None]:
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _raise_if_plan_would_drop_session_user(session_ctx: SessionContext, plan: Plan):
    for change in plan:
        if change.urn.resource_type == ResourceType.USER and isinstance(change, DropResource):
            if ResourceName(session_ctx["user"]) == ResourceName(change.urn.fqn.name):
                raise Exception("Plan would drop the current session user, which is not allowed")


def _require_container(container: Resource, needed_for: Resource) -> ResourceContainer:
    """
    SharedDatabase is a DATABASE-typed resource that is deliberately not a ResourceContainer
    (shared databases are read-only: no schemas, no db-scoped resources). Every place that
    attaches `needed_for` to `container` via .add()/.find() funnels through here, so that case
    raises a guided OrphanResourceException instead of a bare AttributeError.
    """
    if not isinstance(container, ResourceContainer):
        raise OrphanResourceException(
            f"Cannot add {needed_for.resource_type.value} '{needed_for.name}' to "
            f"{type(container).__name__} '{container.name}': it is read-only and cannot contain "
            f"schemas or other resources.\n"
            f"  Add a regular Database to your config, or set an explicit database:/schema: on the resource."
        )
    return container


def _merge_pointers(resources: Sequence[Resource]) -> list[Resource]:
    """
    It is expected in yaml-defined blueprints that all resources are defined with static strings, instead
    of using object references.

    """

    namespace: dict[ResourceRef, Resource] = {}
    # Push pointers to the end
    resources = sorted(resources, key=lambda resource: isinstance(resource, ResourcePointer))

    def _merge(resource: Resource, pointer: ResourcePointer):
        if pointer.container is not None:
            # # The pointer has a container but the resource does not, merge fails
            # if getattr(resource, "container", None) is None:
            #     raise Exception(f"Cannot merge pointer {pointer} into resource {resource}")
            pointer.container.remove(pointer)

        # Migrate items from pointer to resource
        for item in pointer.items():
            pointer.remove(item)
            _require_container(resource, needed_for=item).add(item)

    for resource_or_pointer in resources:
        # Create a unique identifier for the resource
        resource_id: ResourceRef
        if isinstance(resource_or_pointer, NamedResource):
            resource_id = (
                resource_or_pointer.resource_type,
                str(resource_or_pointer.name),
                # Some resources are named within a parent rather than the account: two
                # users can each have a key pair named MY_KEY. Those report the parent as
                # an fqn param, and without it here the second one reads as a duplicate of
                # the first. Every other resource has no params, so this leaves their
                # identity unchanged.
                tuple(sorted((k, str(v)) for k, v in resource_or_pointer.fqn_params.items())),
            )
        else:
            resource_id = str(resource_or_pointer.urn)

        # If the resource is a pointer, attempt to merge it to an existing resource
        if isinstance(resource_or_pointer, ResourcePointer):
            pointer = resource_or_pointer
            if resource_id in namespace:
                _merge(namespace[resource_id], pointer)
            else:
                namespace[resource_id] = pointer
        else:
            resource = resource_or_pointer
            # We found a potentially conflicting resource
            if resource_id in namespace:
                # Throw away duplicate resources when the object id is the same
                if namespace[resource_id] is resource:
                    continue
                else:
                    resource_name = getattr(resource, "name", str(resource.fqn))
                    raise DuplicateResourceException(
                        f"Duplicate {resource.resource_type.value} found: '{resource_name}'\n"
                        f"  Each resource must be defined only once.\n"
                        f"  Check your config files for duplicate definitions."
                    )
            else:
                namespace[resource_id] = resource

    return list(namespace.values())


def _get_databases(
    resource: ResourceContainer,
) -> list[Union[Database, ResourcePointer]]:
    return cast(
        list[Union[Database, ResourcePointer]],
        resource.items(resource_type=ResourceType.DATABASE),
    )


def _get_schemas(resource: ResourceContainer) -> list[Union[Schema, ResourcePointer]]:
    return cast(
        list[Union[Schema, ResourcePointer]],
        resource.items(resource_type=ResourceType.SCHEMA),
    )


def _get_schema_by_name(resource: ResourceContainer, name: Union[ResourceName, str]) -> Union[Schema, ResourcePointer]:
    return cast(
        Union[Schema, ResourcePointer],
        resource.find(name=name, resource_type=ResourceType.SCHEMA),
    )


def _get_public_schema(resource: ResourceContainer) -> Union[Schema, ResourcePointer]:
    return _get_schema_by_name(resource, "PUBLIC")


def _get_role_grants(resource: ResourceContainer) -> list[RoleGrant]:
    return cast(list[RoleGrant], resource.items(resource_type=ResourceType.ROLE_GRANT))


def _kind_mismatch_message(urn: URN, declared_cls: Type[Resource], fetched_cls: Type[Resource]) -> str:
    hint = "Update your config to match, or drop the resource so it's no longer managed by snowcap."
    if {declared_cls, fetched_cls} == {Database, SharedDatabase}:
        hint = (
            "declare it with from_share: <provider_account>.<share_name>"
            if fetched_cls is SharedDatabase
            else "remove from_share: from its config so it's declared as a regular database"
        )
        hint = f"To fix, {hint}."
    return (
        f"{urn.fqn.name} is declared as {declared_cls.__name__} but Snowflake reports it as "
        f"{fetched_cls.__name__}. {hint}"
    )


def _resource_scope_is_outside_blueprint_scope(resource_type: ResourceType, blueprint_scope: BlueprintScope) -> bool:
    resource_scope = RESOURCE_SCOPES[resource_type]
    if blueprint_scope == BlueprintScope.SCHEMA and (
        resource_type == ResourceType.SCHEMA or isinstance(resource_scope, (SchemaScope, TableScope))
    ):
        return False
    elif blueprint_scope == BlueprintScope.DATABASE and (
        resource_type == ResourceType.DATABASE or isinstance(resource_scope, (DatabaseScope, SchemaScope, TableScope))
    ):
        return False
    elif blueprint_scope == BlueprintScope.ACCOUNT:
        return False
    return True


class Blueprint:
    def __init__(
        self,
        name: Optional[str] = None,
        resources: Optional[list[Resource]] = None,
        dry_run: bool = False,
        sync_resources: Optional[list[ResourceType]] = None,
        exclude_resources: Optional[list[ResourceType]] = None,
        vars: Optional[dict] = None,
        vars_spec: Optional[list[dict]] = None,
        scope: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        threads: int = 8,
        use_account_usage: bool = False,
    ) -> None:
        self._config = BlueprintConfig(
            name=name,
            resources=resources,
            dry_run=dry_run,
            sync_resources=[ResourceType(item) for item in sync_resources] if sync_resources else None,
            exclude_resources=[ResourceType(item) for item in exclude_resources] if exclude_resources else None,
            vars=vars or {},
            vars_spec=vars_spec or [],
            scope=BlueprintScope(scope) if scope else None,
            database=ResourceName(database) if database else None,
            schema=ResourceName(schema) if schema else None,
            threads=max(1, threads),  # Ensure at least 1 thread
            use_account_usage=use_account_usage,
        )
        self._finalized = False
        self._staged: list[Resource] = []
        self._root = ResourcePointer(name="ACCOUNT", resource_type=ResourceType.ACCOUNT)
        self._levels: dict[URN, int] = {}  # Store dependency levels
        self.add(resources or [])

    @classmethod
    def from_config(cls, config: BlueprintConfig):
        blueprint = cls.__new__(cls)
        blueprint._config = config
        blueprint._staged = []
        blueprint._root = ResourcePointer(name="ACCOUNT", resource_type=ResourceType.ACCOUNT)
        blueprint._finalized = False
        blueprint._levels = {}  # Initialize dependency levels
        blueprint.add(config.resources or [])
        return blueprint

    def _raise_for_nonconforming_plan(self, session_ctx: SessionContext, plan: Plan):
        exceptions = []
        enterprise_resources: dict[str, list[str]] = {}

        for change in plan:
            if isinstance(change, UpdateResource):
                if "name" in change.delta:
                    exceptions.append(f"Renaming resources is not allowed (ref: {change.urn})")
                if change.resource_cls.resource_type == ResourceType.GRANT:
                    exceptions.append(f"Grants cannot be updated (ref: {change.urn})")

            # Edition exceptions - collect by resource type for better display
            if session_ctx["account_edition"] == AccountEdition.STANDARD:
                if isinstance(change, CreateResource) and AccountEdition.STANDARD not in change.resource_cls.edition:
                    label = change.urn.resource_label
                    if label not in enterprise_resources:
                        enterprise_resources[label] = []
                    enterprise_resources[label].append(str(change.urn.fqn))

            # Scope exceptions
            if self._config.scope:
                if _resource_scope_is_outside_blueprint_scope(change.urn.resource_type, self._config.scope):
                    exceptions.append(
                        f"Resource {change.urn} is out of scope ({self._config.scope}) for this blueprint"
                    )

        # Format enterprise edition errors
        if enterprise_resources:
            lines = ["These resources require Enterprise edition (current account is Standard):"]
            exclude_types = []
            for label, resources in enterprise_resources.items():
                exclude_types.append(label)
                lines.append(f"  {label}:")
                for resource in resources[:3]:
                    lines.append(f"    - {resource}")
                if len(resources) > 3:
                    lines.append(f"    ... and {len(resources) - 3} more")
            lines.append("")
            lines.append(f"Use --exclude to skip: --exclude {','.join(sorted(set(exclude_types)))}")
            exceptions.insert(0, "\n".join(lines))

        if exceptions:
            if len(exceptions) > 5:
                exception_block = "\n".join(exceptions[0:5]) + f"\n... and {len(exceptions) - 5} more"
            else:
                exception_block = "\n".join(exceptions)
            raise NonConformingPlanException("Non-conforming actions found in plan:\n" + exception_block)

    def _warning_for_nonconforming_plan(
        self,
        session_ctx: SessionContext,
        plan: Plan,
        manifest: Optional[Manifest] = None,
        remote_state: Optional[State] = None,
    ):
        warnings = []

        # Future grant precedence is a property of the whole config, not of the changes in
        # the plan, so use the manifest when we have it and fall back to the plan when we
        # were handed a pre-built one.
        if manifest is not None:
            warnings.extend(future_grant_precedence_warnings(manifest_state_entries(manifest, remote_state)))
        else:
            warnings.extend(future_grant_precedence_warnings(plan_entries(plan)))

        grant_to_system = False
        role_grant_to_system = False
        grant_on_all = False
        for change in plan:
            # System role exceptions
            if isinstance(change, CreateResource) and change.resource_cls.resource_type == ResourceType.GRANT:
                if change.after["to"] in SYSTEM_ROLES:
                    grant_to_system = True
                if change.after["grant_type"] == GrantType.ALL.value:
                    grant_on_all = True

            if isinstance(change, CreateResource) and change.resource_cls.resource_type == ResourceType.ROLE_GRANT:
                if change.after["role"] in SYSTEM_ROLES:
                    role_grant_to_system = True

            # A key pair rotation leaves the prior key valid for a grace period, under a
            # name Snowflake generates. Snowcap doesn't manage that key -- it expires on
            # its own -- so the plan has to say it will still be there.
            if (
                isinstance(change, UpdateResource)
                and change.urn.resource_type == ResourceType.USER_KEY_PAIR
                and "fingerprint" in change.delta
            ):
                expire_after_hours = change.after.get("expire_rotated_key_pair_after_hours")
                if expire_after_hours == 0:
                    grace_period = "revoked immediately"
                elif expire_after_hours is None:
                    grace_period = "valid for 24 hours (Snowflake's default)"
                else:
                    grace_period = f"valid for {expire_after_hours} hours"
                warnings.append(
                    f"Key pair {change.urn} will be rotated. The prior key is kept as "
                    f"<name>_ROTATED_<epoch_ms> and stays {grace_period}; set "
                    "expire_rotated_key_pair_after_hours to change that."
                )

            # MCP server specification changes are applied via CREATE OR REPLACE, which
            # drops all grants on the server (see lifecycle.update_mcp_server).
            if (
                isinstance(change, UpdateResource)
                and change.urn.resource_type == ResourceType.MCP_SERVER
                and "specification" in change.delta
            ):
                warnings.append(
                    f"MCP server {change.urn} specification change will be applied via CREATE OR REPLACE, which drops "
                    "all existing grants on the MCP server; snowcap-managed grants are re-created in the same apply, "
                    "externally-managed grants must be re-applied manually."
                )

        if grant_to_system:
            warnings.append(
                "Grants to system role found. They will be always recreated since system roles are not managed by Snowcap"
            )
        if role_grant_to_system:
            warnings.append(
                "Role grants to system role found. They will be always recreated since system roles are not managed by Snowcap"
            )
        if grant_on_all:
            warnings.append(
                "Grants of type ALL found. They will be always recreated since Snowcap does not compare the affected objects."
            )

        if warnings:
            logger.warning("\nActions found in plan that should be reviewed:")
            for warning in warnings:
                logger.warning(" - " + warning)

    def fetch_remote_state(self, session, manifest: Manifest) -> State:
        """Fetch remote state with parallel resource retrieval."""
        state = {}
        logger = logging.getLogger(__name__)
        session_ctx = data_provider.fetch_session(session)

        data_provider.use_secondary_roles(session, all=True)

        # Pre-populate ACCOUNT_USAGE caches if enabled
        # This avoids many individual SHOW GRANTS commands later
        if self._config.use_account_usage:
            data_provider.populate_account_usage_caches(session)

        if self._config.sync_resources:
            urns = [item for item in manifest.urns if item.resource_type not in self._config.sync_resources]
            for resource_type in self._config.sync_resources:
                # Future grants are read in full whenever grants are synced, and neither
                # the query nor the set of roles queried is narrowed to what the manifest
                # declares. Narrowing would be sound for a plan that only creates, but
                # syncing a resource type means removing what is not declared, and a future
                # grant absent from config is precisely what has to be found.
                #
                # Skipping the query when the manifest declared no future grants kept the
                # ones already in Snowflake out of remote state, so sync could not propose
                # dropping them -- unseen rather than deliberately kept, with nothing in
                # the plan to say so. Migrating a config from ALL plus FUTURE pairs to
                # inherited grants removes the last future grant and hit exactly that: 26
                # orphaned future grants, zero drops, no warning.
                list_kwargs: dict[str, Any] = {}
                if resource_type == ResourceType.GRANT:
                    list_kwargs["include_future_grants"] = True
                for fqn in data_provider.list_resource(session, resource_label_for_type(resource_type), **list_kwargs):
                    if self._config.scope == BlueprintScope.DATABASE and fqn.database != self._config.database:
                        continue
                    if self._config.scope == BlueprintScope.SCHEMA and fqn.schema != self._config.schema:
                        continue

                    urns.append(
                        URN(
                            resource_type=resource_type,
                            fqn=fqn,
                            account_locator=session_ctx["account_locator"],
                        )
                    )
        else:
            urns = list(manifest.urns)

        # Filter out excluded resource types
        if self._config.exclude_resources:
            urns = [urn for urn in urns if urn.resource_type not in self._config.exclude_resources]

        urns = list(set(urns))  # Deduplicate urns

        # Pre-compute which databases have param fields (for schema inheritance check)
        db_with_params = databases_with_param_fields(manifest)

        def _needs_params(urn: URN) -> bool:
            """Check if this resource needs parameter fields fetched."""
            resource_type = urn.resource_type

            # For resources not in manifest, skip params entirely
            # Params are only needed for comparing against manifest values
            # This applies to both sync_resources types (remote-only, will be deleted)
            # and non-sync types (shouldn't happen, we only fetch manifest URNs for those)
            if urn not in manifest.urns:
                return False

            # For schemas, use per-URN check (only PUBLIC schemas with db params, or schemas with own params)
            if resource_type == ResourceType.SCHEMA:
                return schema_urn_needs_params(urn, manifest, db_with_params)

            # For other resource types with param fields, check per-URN
            # This avoids fetching params for resources that don't specify param values
            # Works for both sync_resources and non-sync manifest resources
            return resource_urn_needs_params(urn, manifest)

        with ThreadPoolExecutor(max_workers=self._config.threads) as executor:
            future_to_urn = {
                executor.submit(data_provider.fetch_resource, session, urn, include_params=_needs_params(urn)): urn
                for urn in urns
            }
            for future in as_completed(future_to_urn):
                urn = future_to_urn[future]
                try:
                    data = future.result()
                    if data:
                        if self._config.sync_resources and urn.resource_type in self._config.sync_resources:
                            resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
                        else:
                            item = manifest[urn]
                            if isinstance(item, ManifestResource):
                                resource_cls = item.resource_cls
                                # For polymorphic resource types, the class Snowflake's data actually
                                # matches can disagree with the class the manifest declared (e.g. a
                                # database declared as Database that Snowflake reports as an imported
                                # database). resource_cls.spec(**data) would then raise a raw TypeError
                                # from an unexpected/missing keyword, so check agreement up front.
                                fetched_cls = Resource.resolve_resource_cls(urn.resource_type, data)
                                if fetched_cls is not resource_cls:
                                    raise InvalidResourceException(
                                        _kind_mismatch_message(urn, resource_cls, fetched_cls)
                                    )
                            else:
                                resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
                        state[urn] = resource_cls.spec(**data).to_dict(session_ctx["account_edition"])
                    # If data is None, resource doesn't exist in Snowflake
                    # Don't add to state - reconciliation will create it
                except Exception as e:
                    logger.error(f"Failed to fetch resource {urn}: {e}")
                    raise  # Stop processing if any fetch fails

        # Check for references that are not in the state
        # Skip params and detailed queries for references - we just need to verify they exist
        checked_refs = []
        for parent, reference in manifest.refs:
            if reference in manifest.urns or reference in state or reference in checked_refs:
                continue
            is_public_schema = reference.resource_type == ResourceType.SCHEMA and reference.fqn.name == ResourceName(
                "PUBLIC"
            )
            try:
                data = data_provider.fetch_resource(session, reference, include_params=False, existence_only=True)
                if data is None and not is_public_schema:
                    available_names = [
                        str(u.fqn.name) for u in manifest.urns if u.resource_type == reference.resource_type
                    ]
                    raise MissingResourceException(
                        format_missing_resource_error(reference, parent, available_names),
                        missing_urn=reference,
                        required_by=parent,
                        suggestions=available_names,
                    )
                else:
                    checked_refs.append(reference)
            except Exception as e:
                if not is_public_schema:
                    logger.error(f"Error fetching reference {reference}: {e}")
                    raise
        return state

    def _resolve_vars(self):
        # Get all resources from the graph (after _build_resource_graph has run)
        all_resources = [r for r in _walk(self._root) if isinstance(r, Resource)]
        for resource in all_resources:
            resource._resolve_vars(self._config.vars, all_resources)

    def _resolve_role_refs(self):
        for resource in _walk(self._root):
            if isinstance(resource, ResourcePointer):
                continue
            resource._resolve_role_refs()

    def _build_resource_graph(self, session_ctx: SessionContext) -> None:
        """
        Convert the staged resources into a directed graph of resources
        """
        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)
        self._staged = []

        # Create root node of the resource graph
        if len(org_scoped) > 0:
            raise Exception("Blueprint cannot contain an Account resource")

        # Merge account scoped pointers into their proper resource
        acct_scoped = _merge_pointers(acct_scoped)

        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)

        if self._config.scope != BlueprintScope.ACCOUNT and self._config.database is not None:
            if len(acct_scoped) > 1:
                raise RuntimeError
            # The user has specified a database and added a resource to the config
            elif len(acct_scoped) == 1:
                scoped_database = acct_scoped[0]
                if scoped_database.resource_type != ResourceType.DATABASE:
                    raise RuntimeError(f"Expected a database, got {scoped_database.resource_type}")
                if scoped_database.name != self._config.database:
                    raise RuntimeError
            # The user has specified a database by name only
            else:
                scoped_database = ResourcePointer(name=self._config.database, resource_type=ResourceType.DATABASE)
                self._root.add(scoped_database)
                if self._config.schema is not None:
                    scoped_database.add(ResourcePointer(name=self._config.schema, resource_type=ResourceType.SCHEMA))

        # List all databases connected to root
        databases = _get_databases(self._root)

        # If the user didn't stage a database, create one from session context
        if len(databases) == 0 and (len(db_scoped) + len(schema_scoped) > 0):
            if session_ctx.get("database") is None:
                raise OrphanResourceException(
                    "Your config includes resources that require a database (schemas, tables, views, etc.) "
                    "but no database is defined.\n"
                    "  Add a database to your config:\n"
                    "    databases:\n"
                    "      - name: MY_DATABASE"
                )
            logger.warning(f"No database found in config, using database {session_ctx['database']} from session")
            self._root.add(ResourcePointer(name=session_ctx["database"], resource_type=ResourceType.DATABASE))
            databases = _get_databases(self._root)

        # Attach parentless schemas to the default database, if there is one
        for resource in db_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    _require_container(databases[0], needed_for=resource).add(resource)
                else:
                    raise OrphanResourceException(
                        f"Resource {resource.resource_type.value} '{resource.name}' has no database.\n"
                        f"  Your config has multiple databases. Specify which database this resource belongs to:\n"
                        f"    - name: {resource.name}\n"
                        f"      database: DATABASE_NAME"
                    )

        available_scopes = {}
        for database in databases:
            # SharedDatabase is a DATABASE-typed leaf, not a ResourceContainer: shared
            # databases are read-only, so they never have schemas to merge or scope.
            if not isinstance(database, ResourceContainer):
                continue
            database_resources = list(database.items())
            _merge_pointers(database_resources)
            for schema in _get_schemas(database):
                available_scopes[f"{database.name}.{schema.name}"] = schema

        for resource in schema_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    # When the blueprint is scoped all dangling resources should be assigned to the configured scope
                    if self._config.scope == BlueprintScope.SCHEMA and self._config.schema is not None:
                        scoped_schema = _get_schema_by_name(
                            _require_container(databases[0], needed_for=resource), self._config.schema
                        )
                        scoped_schema.add(resource)
                        # TODO: figure out how to handle the case where the schema is already in the blueprint
                    else:
                        logger.warning(f"Resource {resource} has no schema, using {databases[0].name}.PUBLIC")
                        _get_public_schema(_require_container(databases[0], needed_for=resource)).add(resource)
                else:
                    raise OrphanResourceException(
                        f"Resource {resource.resource_type.value} '{resource.name}' has no schema.\n"
                        f"  Your config has multiple databases. Specify which schema this resource belongs to:\n"
                        f"    - name: {resource.name}\n"
                        f"      database: DATABASE_NAME\n"
                        f"      schema: SCHEMA_NAME"
                    )
            elif isinstance(resource.container, ResourcePointer):
                schema_pointer = resource.container

                # We have a schema-scoped resource (eg a view) that has a resource pointer for the schema. The job is to connect
                # that resource into the tree
                #
                # If the schema pointer has no database, assume it lives in the only database we have
                if schema_pointer.container is None:
                    if len(databases) == 1:
                        _require_container(databases[0], needed_for=schema_pointer).add(schema_pointer)
                    else:
                        raise OrphanResourceException(
                            f"Resource {resource.resource_type.value} '{resource.name}' references schema "
                            f"'{resource.container.name}' but no database is specified.\n"
                            f"  Your config has multiple databases. Specify which database the schema belongs to:\n"
                            f"    - name: {resource.name}\n"
                            f"      database: DATABASE_NAME\n"
                            f"      schema: {resource.container.name}"
                        )
                elif isinstance(schema_pointer.container, ResourcePointer):
                    expected_scope = f"{schema_pointer.container.name}.{schema_pointer.name}"
                    if expected_scope in available_scopes:
                        schema = available_scopes[expected_scope]
                        schema.add(resource)
                    else:
                        self._root.add(schema_pointer.container)

            for ref in resource.refs:
                resource_and_ref_share_scope = isinstance(ref.scope, resource.scope.__class__)
                if ref.container is None and resource.container is not None and resource_and_ref_share_scope:
                    if isinstance(ref, ResourcePointer):
                        # For ResourcePointers, set the container directly (for URN matching)
                        # but don't add them to the container's items (they're just dependency refs)
                        ref._container = resource.container
                    else:
                        # For actual Resource objects, add them to the container
                        resource.container.add(ref)

    def _create_tag_references(self) -> None:
        """
        Tag name resolution in Snowflake is special. Tags can be referenced
        by name only. If that tag name is unique in the account, the tag will be applied.
        If the tag name is not unique, the error "does not exist or not authorized" will be raised.

        To emulate this behavior, Blueprint will attempt to look up any referenced tags by name
        """
        taggables: list[TaggableResource] = []
        tags: list[Tag] = []
        for resource in _walk(self._root):
            if isinstance(resource, TaggableResource):
                taggables.append(resource)
            elif isinstance(resource, Tag):
                tags.append(resource)

        for resource in taggables:
            new_tags = {}
            if resource._tags is None:
                continue
            for tag_name, tag_value in resource._tags.items():
                identifier = parse_identifier(tag_name)
                if "database" in identifier or "schema" in identifier:
                    new_tags[tag_name] = tag_value
                else:
                    for tag in tags:
                        if tag.name == tag_name:
                            new_tags[str(tag.fqn)] = tag_value
                            break
                    else:
                        # We couldn't resolve the tag, so just use the tag name as is
                        new_tags[tag_name] = tag_value
            resource._tags = ResourceTags(new_tags)
            tag_ref = resource.create_tag_reference()
            if tag_ref:
                self._root.add(tag_ref)

    def _create_ownership_refs(self, session_ctx: SessionContext) -> None:
        role_grants: list[RoleGrant] = _get_role_grants(self._root)

        for resource in _walk(self._root):
            if isinstance(resource, ResourcePointer):
                continue
            elif isinstance(resource, RoleGrant):
                # Support ordering for role grants in a role tree
                for role_grant in role_grants:
                    if isinstance(resource.to, Role) and resource.to.name == role_grant.role.name:
                        resource.requires(role_grant)
            elif hasattr(resource._data, "owner"):
                owner = getattr(resource._data, "owner")

                # Misconfigured resource, owner should always be a Role
                if isinstance(owner, str):
                    raise RuntimeError(f"Owner of {resource} is a string, {owner}")

                owner = cast(ResourcePointer, owner)

                # Skip Snowflake-owned system resources (like INFORMATION_SCHEMA) that are owned by blank
                if owner.name == "":
                    continue

                # Require that a resource's owner role exists in remote state or has been added to the blueprint
                resource.requires(owner)

                # If the owner role isn't available in the session, try to find a role grant that can be used to
                # satisfy the requirement.
                if owner.name not in session_ctx["available_roles"]:
                    for role_grant in role_grants:
                        # Only look for role grants that match the owner role
                        if role_grant.role.name != owner.name:
                            continue

                        # Only look for role-to-role grants
                        if role_grant._data.to_role is None:
                            continue
                        resource.requires(role_grant)

                    # It's non-trivial to determine if an owner role is available in the current session because
                    # database roles aren't explicitly available in the session context
                    # else:
                    #     raise InvalidOwnerException(
                    #         f"Blueprint resource {resource} owner {resource._data.owner} must be granted to the current session"
                    #     )

    def _create_grandparent_refs(self) -> None:
        for resource in _walk(self._root):
            if isinstance(resource.scope, SchemaScope):
                resource.requires(resource.container.container)

    def _create_stage_privilege_refs(self) -> None:
        stage_grants: dict[tuple, list[Grant]] = {}

        for resource in _walk(self._root):
            if isinstance(resource, Grant):
                # Snowflake requires READ before/with WRITE on a stage. Catch
                # both direct grants (on_type == STAGE) and bulk ALL/FUTURE
                # grants (items_type == STAGE), keyed by exact scope so the
                # WRITE -> READ dependency below covers each case.
                d = resource._data
                if d.on_type == ResourceType.STAGE or d.items_type == ResourceType.STAGE:
                    key = (d.on_type, d.on, d.grant_type, d.items_type)
                    if key not in stage_grants:
                        stage_grants[key] = []
                    stage_grants[key].append(resource)

        def _apply_refs(stage_grants):
            for stage in stage_grants.keys():
                read_grants = []
                write_grants = []
                for grant in stage_grants[stage]:
                    if grant._data.priv == "READ":
                        read_grants.append(grant)
                    elif grant._data.priv == "WRITE":
                        write_grants.append(grant)

                for w_grant in write_grants:
                    for r_grant in read_grants:
                        w_grant.requires(r_grant)

        _apply_refs(stage_grants)

    def _finalize_resources(self) -> None:
        for resource in _walk(self._root):
            resource._finalized = True

    def _finalize(self, session_ctx: SessionContext) -> None:
        if self._finalized:
            raise RuntimeError("Blueprint already finalized")
        self._finalized = True
        self._build_resource_graph(session_ctx)
        self._resolve_vars()
        self._resolve_role_refs()
        self._create_tag_references()
        self._create_ownership_refs(session_ctx)
        self._create_grandparent_refs()
        self._create_stage_privilege_refs()
        # Must run after _build_resource_graph populates self._root and before
        # _finalize_resources locks resources, like the other ref-creators above —
        # otherwise it walks an empty graph and the grant->flag edge is never added.
        self._link_inherited_grants_to_feature_flag()
        self._finalize_resources()

    def _link_inherited_grants_to_feature_flag(self) -> None:
        """
        Make inherited grants depend on the account parameter that enables them.

        A config can turn the preview on itself:

            account_parameters:
              - name: FEATURE_RBAC_INHERITED_GRANTS
                value: ENABLED

        Without a dependency between the two, both land at the same level of the plan and
        run concurrently, so the grants can reach Snowflake before the parameter does. The
        reference is only added when the parameter is declared, since a reference to a
        resource that is not in the manifest is an error in its own right.
        """
        resources = [r for r in _walk(self._root) if isinstance(r, Resource)]
        feature_flag = next(
            (
                r
                for r in resources
                if r.resource_type == ResourceType.ACCOUNT_PARAMETER
                and isinstance(r, NamedResource)
                and ResourceName(r.name) == ResourceName(INHERITED_GRANTS_FEATURE_FLAG)
            ),
            None,
        )
        if feature_flag is None:
            return

        for resource in resources:
            if (
                resource.resource_type == ResourceType.GRANT
                and getattr(resource, "grant_type", None) == GrantType.INHERITED
                and not resource._finalized
            ):
                resource.requires(feature_flag)

    def generate_manifest(self, session_ctx: SessionContext) -> Manifest:
        manifest = Manifest(account_locator=session_ctx["account_locator"])
        self._finalize(session_ctx)
        for resource in _walk(self._root):
            if isinstance(resource, Resource):
                # Skip resources that are in the exclude list
                if self._config.exclude_resources and resource.resource_type in self._config.exclude_resources:
                    continue
                # Skip grants that reference excluded resource types
                if (
                    self._config.exclude_resources
                    and resource.resource_type == ResourceType.GRANT
                    and hasattr(resource, "on_type")
                    and resource.on_type in self._config.exclude_resources
                ):
                    continue
                manifest.add(resource, session_ctx["account_edition"])
            else:
                raise RuntimeError(f"Unexpected object found in blueprint: {resource}")

        return manifest

    def _execute_change(self, session, commands: list[str]) -> None:
        """Execute a list of SQL commands for a single change."""
        logger = logging.getLogger(__name__)
        for sql in commands:
            if not self._config.dry_run:
                try:
                    execute(session, sql)
                except snowflake.connector.errors.ProgrammingError as err:
                    if err.errno == ALREADY_EXISTS_ERR:
                        logger.warning(f"Resource already exists: {sql}, skipping...")
                    elif err.errno == INVALID_GRANT_ERR:
                        logger.warning(f"Invalid grant: {sql}, skipping...")
                    elif err.errno == DOES_NOT_EXIST_ERR and sql.startswith(("REVOKE", "DROP")):
                        logger.warning(f"Resource does not exist: {sql}, skipping...")
                    else:
                        raise

    def plan(self, session) -> Plan:
        """Generate and store the plan, computing dependency levels."""
        logger = logging.getLogger(__name__)
        reset_cache()
        logger.debug("Using blueprint vars:")
        for key in self._config.vars.keys():
            logger.debug(f"  {key}")
        session_ctx = data_provider.fetch_session(session)
        manifest = self.generate_manifest(session_ctx)
        raise_if_inherited_grants_unavailable(session, manifest)
        remote_state = self.fetch_remote_state(session, manifest)
        try:
            finished_plan = diff(remote_state, manifest)
            # Filter plan based on sync_resources:
            # - For sync_resources types: keep ALL changes (full sync, YML is source of truth)
            # - For non-sync_resources types: keep CREATE/UPDATE/TRANSFER but NOT DROP
            #   (only sync resources that are defined in YML, don't delete remote-only resources)
            if self._config.sync_resources:
                finished_plan = [
                    change
                    for change in finished_plan
                    if (
                        # Keep all changes for sync_resources types
                        change.urn.resource_type in self._config.sync_resources
                        # For non-sync types, keep everything except DROP
                        or not isinstance(change, DropResource)
                    )
                ]
            # Compute dependency levels
            resource_set = set(manifest.urns + list(remote_state.keys()))
            for ref in manifest.refs:
                resource_set.add(ref[0])
                resource_set.add(ref[1])
            self._levels = compute_levels(resource_set, set(manifest.refs))
        except Exception:
            logger.error("~" * 80 + "REMOTE STATE")
            logger.error(remote_state)
            logger.error("~" * 80 + "MANIFEST")
            logger.error(manifest)
            raise
        self._raise_for_nonconforming_plan(session_ctx, finished_plan)
        self._warning_for_nonconforming_plan(session_ctx, finished_plan, manifest, remote_state)
        return finished_plan

    def apply(self, session, plan: Optional[Plan] = None) -> None:
        """Apply the plan with parallel execution of independent additive changes.

        At this point, we have a list of actions as a part of the plan. Each action is one of:
             1. ADD action (CREATE command)
             2. CHANGE action (one or many ALTER or SET PARAMETER commands)
             3. REMOVE action (DROP command, REVOKE command, or a rename operation)
             4. TRANSFER action (GRANT OWNERSHIP command)

         Each action requires:
             • a set of privileges necessary to run commands
             • the appropriate role to execute commands

         Once we've determined those things, we can compare the list of required roles and privileges
         against what we have access to in the session and the role tree."""

        def print_apply_summary(plan: Plan, phase: str = "start"):
            """Print a summary of what will be or was applied."""
            # Colors
            cyan = "\033[96m"
            blue = "\033[94m"
            green = "\033[92m"
            reset = "\033[0m"

            create_count = len([c for c in plan if isinstance(c, CreateResource)])
            update_count = len([c for c in plan if isinstance(c, UpdateResource)])
            transfer_count = len([c for c in plan if isinstance(c, TransferOwnership)])
            drop_count = len([c for c in plan if isinstance(c, DropResource)])

            if phase == "start":
                print(f"\n{cyan}»{reset} {blue}snowcap apply{reset}")
                print(
                    f"{cyan}»{reset} Applying: {green}{create_count}{reset} to create, {update_count} to update, {transfer_count} to transfer, {drop_count} to drop.\n"
                )
            else:
                print(
                    f"\n{cyan}»{reset} {green}Applied:{reset} {create_count} created, {update_count} updated, {transfer_count} transferred, {drop_count} dropped.\n"
                )

        def execute_commands_in_parallel(commands):
            """Execute a list of SQL commands in parallel using a thread pool."""
            with ThreadPoolExecutor(max_workers=self._config.threads) as executor:
                future_to_change = {
                    executor.submit(
                        self._execute_change,
                        session,
                        c["commands"],
                    ): c["change"]
                    for c in commands
                }
                for future in as_completed(future_to_change):
                    change = future_to_change[future]
                    try:
                        future.result()
                    except Exception as e:
                        verb = {
                            CreateResource: "create",
                            UpdateResource: "update",
                            DropResource: "drop",
                            TransferOwnership: "transfer ownership of",
                        }.get(type(change), "apply")
                        # Name the resource and the error, not the full change repr (which
                        # dumps the entire rendered SQL and buries what actually went wrong).
                        logger.error(f"Failed to {verb} {change.urn.resource_label} {change.urn.fqn}: {e}")
                        raise

        def process_commands(commands, roles, available_roles):
            # Check for missing roles upfront (filter out empty/invalid roles)
            missing_roles = {r for r in roles if str(r)} - set(available_roles)
            if missing_roles:
                # Build a mapping of missing role -> changes that require it
                role_to_changes: dict[str, list[str]] = {}
                for cmd in commands:
                    role = cmd["role"]
                    if role in missing_roles:
                        role_str = str(role)
                        if role_str not in role_to_changes:
                            role_to_changes[role_str] = []
                        change = cmd["change"]
                        role_to_changes[role_str].append(f"{change.urn.fqn}")

                # Build detailed error message
                details = []
                for role in sorted(role_to_changes.keys()):
                    changes = role_to_changes[role]
                    if len(changes) == 1:
                        details.append(f"  - {role}: required for {changes[0]}")
                    else:
                        details.append(f"  - {role}: required for {len(changes)} changes including {changes[0]}")

                raise MissingPrivilegeException(
                    "The following roles are required but not available to your user:\n"
                    + "\n".join(details)
                    + "\n\n  Grant the missing roles to your user:\n"
                    + "\n".join(f"    GRANT ROLE {role} TO USER your_user;" for role in sorted(role_to_changes.keys()))
                )

            # Map changes to their levels (default to 0 if not in self._levels)
            levels = {c["change"].urn: self._levels.get(c["change"].urn, 0) for c in commands}
            max_level = max(levels.values()) if levels else 0

            # Execute changes by level
            for level in range(max_level + 1):
                commands_at_level = [c for c in commands if levels.get(c["change"].urn, 0) == level]
                for role in roles:
                    # Execute changes in current level by role
                    commands_at_role_level = [c for c in commands_at_level if c["role"] == role]
                    if commands_at_role_level:
                        logger.debug(f"Executing level {level} role {role} with {len(commands_at_role_level)} changes")
                        execute(session, f"USE ROLE {role}")
                        execute_commands_in_parallel(commands_at_role_level)

        # TODO: cursor setup, including query tag

        logger = logging.getLogger(__name__)
        session_ctx = data_provider.fetch_session(session)
        if plan is None:
            # self.plan() already emits the nonconforming-plan warning.
            plan = self.plan(session)
        else:
            self._warning_for_nonconforming_plan(session_ctx, plan)

        # Print plan details (includes summary counts)
        print_plan(plan)

        if not plan:
            return

        _raise_if_plan_would_drop_session_user(session_ctx, plan)

        # Databases mounted from a share, resolved only when the plan actually revokes a
        # grant: privileges on those cannot be revoked one at a time. See
        # lifecycle.drop_shared_database_grant.
        shared_databases: Optional[set[str]] = None
        database_owners: Optional[dict[str, str]] = None
        if any(c.urn.resource_type == ResourceType.GRANT for c in plan):
            # Both read the same cached SHOW DATABASES response, so this is one query.
            shared_databases = data_provider.list_shared_database_names(session)
            database_owners = data_provider.list_database_owners(session)

        sql_commands_per_change, available_roles = compile_plan_to_sql(
            session_ctx, plan, shared_databases, database_owners
        )
        roles_list: list[Any] = []
        additive_commands = []
        destructive_commands = []
        for command in sql_commands_per_change:
            roles_list.append(command["role"])
            if isinstance(command["change"], (CreateResource, UpdateResource, TransferOwnership)):
                additive_commands.append(command)
            elif isinstance(command["change"], DropResource):
                destructive_commands.append(command)
        roles_set = set(roles_list)

        # Suppress SQL execution logs during apply (plan details already shown above)
        logging.getLogger("snowcap").setLevel(logging.WARNING)

        # Process additive changes (use available_roles which includes roles being created)
        process_commands(additive_commands, roles_set, available_roles)

        # Process destructive changes
        process_commands(destructive_commands, roles_set, available_roles)

        # Restore logging level
        logging.getLogger("snowcap").setLevel(logging.INFO)

        # Print completion summary
        print_apply_summary(plan, "end")

        if not self._config.dry_run:
            print_surviving_drops(surviving_drops(session, [c["change"] for c in destructive_commands]))

    def _add(self, resource: Resource):
        if self._finalized:
            raise Exception("Cannot add resources to a finalized blueprint")
        if not isinstance(resource, Resource):
            raise Exception(f"Expected a Resource, got {type(resource)} -> {resource}")
        if resource._finalized:
            raise Exception("Cannot add a finalized resource to a blueprint")
        self._staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


def owner_for_change(change: ResourceChange) -> Optional[ResourceName]:
    if isinstance(change, CreateResource) and "owner" in change.after:
        return ResourceName(change.after["owner"])
    elif isinstance(change, UpdateResource) and "owner" in change.after:
        # TRANSFER actions occur strictly after CHANGE actions, so we use the before owner
        # as the role for the change
        return ResourceName(change.before["owner"])
    elif isinstance(change, DropResource) and "owner" in change.before:
        return ResourceName(change.before["owner"])
    elif isinstance(change, TransferOwnership):
        return ResourceName(change.from_owner)
    else:
        return None


def _inherited_grant_execution_role(change: ResourceChange) -> Optional[str]:
    """
    The role an inherited grant declares itself to be managed by, if any.

    Grants get a default owner (SYSADMIN, or ACCOUNTADMIN for integrations and Snowflake
    schemas) when config does not name one, so those defaults are not treated as a
    delegation and fall through to the usual SECURITYADMIN strategy.
    """
    if change.urn.resource_type != ResourceType.GRANT:
        return None
    if isinstance(change, (CreateResource, UpdateResource)):
        data = change.after
    elif isinstance(change, DropResource):
        data = change.before
    else:
        return None
    if not isinstance(data, dict) or data.get("grant_type") != GrantType.INHERITED.value:
        return None
    owner = data.get("owner")
    if not owner or owner in ("SYSADMIN", "ACCOUNTADMIN"):
        return None
    return str(owner)


def _shared_database_for_grant(change: ResourceChange, shared_databases: Optional[set[str]]) -> Optional[str]:
    """
    The shared database a grant being dropped belongs to, if any.

    A grant on a shared database, or on anything inside one, is part of the fan-out of an
    IMPORTED PRIVILEGES grant and cannot be revoked on its own. Returns the database so the
    caller can revoke the share instead; None for ordinary grants.
    """
    if not shared_databases or not isinstance(change, DropResource):
        return None
    if change.urn.resource_type != ResourceType.GRANT:
        return None
    on = change.before.get("on")
    if not on:
        return None
    # A share fan-out only touches the database itself or objects that live inside one. An
    # account-level object (warehouse, integration, role, ...) that merely shares a name with
    # an imported database must revoke its own privilege, not the share.
    on_type = change.before.get("on_type")
    if on_type is not None:
        try:
            rt: Optional[ResourceType] = ResourceType(str(on_type))
        except ValueError:
            rt = None
        if rt is not None and rt != ResourceType.DATABASE and isinstance(RESOURCE_SCOPES.get(rt), AccountScope):
            return None
    # Quote-aware split (like _container_covers): a database quoted with a literal dot would
    # otherwise mis-split and miss the shared-databases set.
    database = smart_split(str(on), ".")[0].strip('"').upper()
    return database if database in shared_databases else None


def surviving_drops(session, changes: list[ResourceChange]) -> list[ResourceChange]:
    """
    Which of the resources an apply just dropped are still there.

    Snowflake does not always fail a statement it could not carry out. REVOKE is the
    conspicuous case: it reports success when the executing role does not own the privilege
    or cannot resolve the grantee, rather than raising. The apply sees no exception, counts
    the drop as applied, and the grant survives -- so the same drop comes back in the next
    plan, and the one after that, with nothing in any output saying why.

    Treating "no exception" as "applied" is what makes that invisible. Reading the dropped
    resources back is the only thing that distinguishes a real drop from one Snowflake
    quietly declined.

    Costs one existence check per dropped resource, and runs only when a plan dropped
    something. A resource type that cannot be read back is skipped rather than reported: not
    being able to confirm a drop is not evidence that it failed.
    """
    dropped = [change for change in changes if isinstance(change, DropResource)]
    if not dropped:
        return []

    # The apply just changed the very state these checks read. reset_cache() alone
    # leaves the ACCOUNT_USAGE grant snapshot in place, and _show_grants_to_role serves
    # it for account-role grants — so a revoked grant would re-appear as a false survivor
    # on use_account_usage runs. Clear both.
    reset_cache()
    data_provider.reset_account_usage_caches()

    survivors: list[ResourceChange] = []
    for change in dropped:
        try:
            if data_provider.fetch_resource(session, change.urn, existence_only=True) is not None:
                survivors.append(change)
        except Exception:  # pragma: no cover - defensive, see docstring
            logger.debug(f"Could not verify drop of {change.urn}", exc_info=True)
    return survivors


def _database_of_database_role_grantee(change: ResourceChange) -> Optional[str]:
    """
    The database a grant's grantee belongs to, when that grantee is a database role.

    A database role is named <database>.<role> and lives inside its database. Managing a
    grant held by one needs a role that can see that database: account-level MANAGE GRANTS
    lets SECURITYADMIN administer grants, but without USAGE on the database it cannot
    resolve the grantee, and REVOKE reports success rather than failing on a grantee it
    cannot resolve. The grant survives, and the same drop comes back in every later plan.

    Returns None for grants to account roles, which account-level authority does reach.
    """
    if change.urn.resource_type != ResourceType.GRANT:
        return None
    if isinstance(change, CreateResource):
        data = change.after
    elif isinstance(change, DropResource):
        data = change.before
    else:
        return None
    if str(data.get("to_type", "")).replace("_", " ").upper() != "DATABASE ROLE":
        return None
    grantee = str(data.get("to", ""))
    if "." not in grantee:
        return None
    return grantee.split(".")[0].strip('"').upper()


def execution_strategy_for_change(
    change: ResourceChange,
    available_roles: list[ResourceName],
    default_role: ResourceName,
    transferred_owners: Optional[dict[URN, ResourceName]] = None,
    database_owners: Optional[dict[str, str]] = None,
) -> tuple[ResourceName, bool]:

    change_owner = owner_for_change(change)

    if resource_type_is_grant(change.urn.resource_type):
        # Inherited grants require MANAGE GRANTS on the container, which is how Snowflake
        # lets a database or schema admin manage access without account-wide authority. An
        # explicit `owner` on the grant names that delegated role, so it is used in
        # preference to SECURITYADMIN when the session has it.
        # https://docs.snowflake.com/en/user-guide/container-manage-grants-intro
        inherited_grant_owner = _inherited_grant_execution_role(change)
        if inherited_grant_owner and inherited_grant_owner in available_roles:
            return ResourceName(inherited_grant_owner), False

        # 2024-10-22: maybe the better thing to do is check role privs selectively
        #
        # Revokes use the same role as grants. An account-level privilege belongs to the
        # system role that owns it, and Snowflake will not take one back from a role that
        # does not -- but it reports success rather than failing, so a revoke run as
        # SECURITYADMIN silently leaves the privilege in place and the same drop reappears
        # in every later plan.
        if change.urn.resource_type == ResourceType.GRANT and isinstance(change, (CreateResource, DropResource)):
            grant_data = change.after if isinstance(change, CreateResource) else change.before
            execution_role = system_role_for_priv(grant_data["priv"])
            if execution_role and execution_role in available_roles:
                return ResourceName(execution_role), False

        # Grants held by a database role are managed from inside that database, by the role
        # that owns it. SECURITYADMIN can hold MANAGE GRANTS and still be unable to resolve
        # the grantee without USAGE on the database.
        grantee_database = _database_of_database_role_grantee(change)
        if grantee_database and database_owners:
            database_owner = database_owners.get(grantee_database)
            if database_owner and ResourceName(database_owner) in available_roles:
                return ResourceName(database_owner), False

        if "SECURITYADMIN" in available_roles:
            return ResourceName("SECURITYADMIN"), False

        return default_role, False

    elif change.urn.resource_type == ResourceType.TAG_REFERENCE:
        # There are two ways you can create a tag reference:
        # 1. You have the global APPLY TAGS priv on the account (given to ACCOUNTADMIN by default)
        # 2. You have APPLY privilege on the TAG object AND you have ownership of the tagged object
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False

        return default_role, False

    elif change.urn.resource_type == ResourceType.TAG_MASKING_POLICY_REFERENCE:
        # Tag-based masking policy references require the APPLY MASKING POLICY privilege
        # which is granted to ACCOUNTADMIN by default
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False

        return default_role, False

    elif change.urn.resource_type == ResourceType.RESOURCE_MONITOR:
        # For some reason Snowflake chose to not have a priv type for resource monitors.
        # Only ACCOUNTADMIN can create them.
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False
        raise MissingPrivilegeException(
            "ACCOUNTADMIN role is required to manage resource monitors.\n"
            "  Grant ACCOUNTADMIN to your user or use a different connection."
        )

    elif change.urn.resource_type == ResourceType.ACCOUNT_PARAMETER:
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False
        raise MissingPrivilegeException(
            "ACCOUNTADMIN role is required to manage account parameters.\n"
            "  Grant ACCOUNTADMIN to your user or use a different connection."
        )

    elif change.urn.resource_type == ResourceType.USER_KEY_PAIR:
        # Every key pair operation is an ALTER USER, which needs OWNERSHIP of the user or
        # MODIFY PROGRAMMATIC AUTHENTICATION METHODS on it. Key pairs have no owner of
        # their own in Snowflake, so `owner` names the role that manages the user
        # (USERADMIN by default, matching the User resource) and is never transferred.
        #
        # That also means `owner` is not fetchable, so remote state carries the default
        # rather than what config declared. change_owner reads the before-owner on an
        # update, which would quietly downgrade a declared owner to USERADMIN on every
        # change after the first -- read the declared owner directly instead.
        declared_owner = None
        if isinstance(change, (CreateResource, UpdateResource)):
            declared_owner = change.after.get("owner")
        elif isinstance(change, DropResource):
            declared_owner = change.before.get("owner")
        if declared_owner and ResourceName(declared_owner) in available_roles:
            return ResourceName(declared_owner), False
        if "USERADMIN" in available_roles:
            return ResourceName("USERADMIN"), False
        return default_role, False

    elif change.urn.resource_type == ResourceType.SCANNER_PACKAGE:
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False
        raise MissingPrivilegeException(
            "ACCOUNTADMIN role is required to manage scanner packages.\n"
            "  Grant ACCOUNTADMIN to your user or use a different connection."
        )

    elif isinstance(change, (UpdateResource, DropResource, TransferOwnership)):
        if (
            isinstance(change, TransferOwnership)
            and change.urn.resource_type in OWNER_EXECUTED_RESOURCE_TYPES
            and "SECURITYADMIN" in available_roles
        ):
            # Owner-executed objects run their body or schedule with the privileges of
            # their owner. Snowflake tightened authorization for transferring them:
            # GRANT OWNERSHIP fails unless the receiving role is in the caller's active
            # role hierarchy or the caller holds account-level MANAGE GRANTS. Running the
            # transfer as the outgoing owner, which is what Snowcap does for every other
            # resource, satisfies neither condition in the common case.
            # https://docs.snowflake.com/en/user-guide/inherited-grants-intro
            return ResourceName("SECURITYADMIN"), False
        if change_owner:
            return change_owner, False
        else:
            raise MissingPrivilegeException(
                f"Insufficient privileges to modify {change.urn.resource_label} '{change.urn.fqn}'.\n"
                f"  You need ownership or appropriate grants on this resource."
            )
    elif isinstance(change, CreateResource):
        if isinstance(change.resource_cls.scope, AccountScope):
            # CREATE DATABASE ... FROM SHARE requires IMPORT SHARE, not CREATE DATABASE
            create_priv = (
                AccountPriv.IMPORT_SHARE
                if change.resource_cls is SharedDatabase
                else CREATE_PRIV_FOR_RESOURCE_TYPE[change.urn.resource_type]
            )

            # SHARE ownership cannot be changed
            if change.urn.resource_type == ResourceType.SHARE:
                if change_owner is None:
                    raise RuntimeError
                return change_owner, False

            system_role = system_role_for_priv(create_priv)
            if system_role and system_role in available_roles:
                transfer_ownership = system_role != change_owner
                return ResourceName(system_role), transfer_ownership
            raise MissingPrivilegeException(
                f"Role {system_role} is required to create {change.urn.resource_label} resources.\n"
                f"  Grant {system_role} to your user:\n"
                f"    GRANT ROLE {system_role} TO USER your_user;"
            )
        elif isinstance(change.resource_cls.scope, (DatabaseScope, SchemaScope)) and change.container:
            container_urn, container_owner = change.container
            container_owner = ResourceName(container_owner)
            # The container's owner is recorded when the plan is built. When the same plan
            # also transfers that container, the transfer has already run by the time this
            # CREATE executes -- a container sits at a lower dependency level than the
            # resources inside it -- so the role recorded here no longer owns the container
            # and cannot create anything in it. Use the owner the container ends up with.
            if transferred_owners and container_urn in transferred_owners:
                container_owner = transferred_owners[container_urn]
            transfer_ownership = container_owner != change_owner
            if transfer_ownership and change.urn.resource_type == ResourceType.NOTEBOOK:
                raise Exception("Notebook ownership cannot be transferred")
            return container_owner, transfer_ownership

    raise RuntimeError(f"Unhandled change type: {change}")


def _as_command_list(cmd: Union[None, str, list[str]]) -> list[str]:
    """
    Lifecycle functions return a single statement for most changes, and a list when the
    change genuinely needs more than one (a key pair rotation that also sets a comment,
    for example -- Snowflake has no syntax that combines them).
    """
    if cmd is None:
        return []
    if isinstance(cmd, list):
        return cmd
    return [cmd]


def sql_commands_for_change(
    change: ResourceChange,
    available_roles: list[ResourceName],
    default_role: ResourceName,
    transferred_owners: Optional[dict[URN, ResourceName]] = None,
    shared_databases: Optional[set[str]] = None,
    database_owners: Optional[dict[str, str]] = None,
) -> tuple[ResourceName, list[str]]:
    """
    In Snowflake's RBAC model, a session has an active role, and zero or more secondary roles.

    The active role of a session is set as follows:
    - When a session is started:
        - If the session is configured with a role, that is the active role
        - Otherwise, if the user of the session has a default_role set, and that role exists, that is the active role
        - Otherwise, the PUBLIC role is activated (PUBLIC cannot be revoked)
    - Any time the USE ROLE command is run, the active role is switched

    A session may run any command that's allowed by the active role or any role downstream from it in the role hierarchy.
    When secondary roles are active (by running the command USE SECONDARY ROLES ALL), then the session may also run any
    command that any secondary role or a role downstream from it is allowed to run.

    However, when a CREATE command is run, only the active role is considered. This is because the role that
    creates a new resource owns that resource by default. There are some exceptions with GRANTS.

    For those reasons, we generally don't have to worry about the current role as long as we have activated secondary roles.
    The exception is when creating new resources
    """

    before_change_cmd: list[str] = []
    change_cmd: Union[None, str, list[str]] = None
    after_change_cmd: list[str] = []

    execution_role, transfer_owner = execution_strategy_for_change(
        change,
        available_roles,
        default_role,
        transferred_owners,
        database_owners,
    )

    if isinstance(change, CreateResource):
        change_cmd = lifecycle.create_resource(change.urn, change.after, change.resource_cls.props)
        if transfer_owner:
            after_change_cmd.append(
                lifecycle.transfer_resource(
                    change.urn,
                    owner=change.after["owner"],
                    owner_resource_type=infer_role_type_from_name(change.after["owner"]),
                    copy_current_grants=True,
                )
            )
            # SPECIAL CASE: when creating a database with a custom owner that we will transfer ownership to,
            # we also need to transfer ownership of the public schema to that role. This replicates the behavior
            # if we were to create the database with a custom owner directly. Shared databases have no owned
            # PUBLIC schema (Snowflake replicates the provider's schemas read-only), so they're excluded.
            if change.urn.resource_type == ResourceType.DATABASE and change.resource_cls is not SharedDatabase:
                after_change_cmd.append(
                    lifecycle.transfer_resource(
                        public_schema_urn(change.urn),
                        owner=change.after["owner"],
                        owner_resource_type=infer_role_type_from_name(change.after["owner"]),
                        copy_current_grants=True,
                    )
                )

            if change.urn.resource_type == ResourceType.SCANNER_PACKAGE:
                after_change_cmd.extend(
                    _as_command_list(lifecycle.update_resource(change.urn, {}, change.resource_cls.props))
                )
        # ALTER USER ... ADD KEY PAIR has no DISABLED option, so a key pair declared
        # disabled is registered and then disabled. Without this the key would be live
        # until the next apply, and the plan right after would show drift.
        if change.urn.resource_type == ResourceType.USER_KEY_PAIR and change.after.get("disabled"):
            after_change_cmd.extend(
                _as_command_list(
                    lifecycle.update_resource(
                        change.urn,
                        {"disabled": True},
                        change.resource_cls.props,
                        after=change.after,
                    )
                )
            )
    elif isinstance(change, UpdateResource):
        props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
        change_cmd = lifecycle.update_resource(change.urn, change.delta, props, after=change.after)
        if change.urn.resource_type == ResourceType.TAG_MASKING_POLICY_REFERENCE:
            after_change_cmd.append(lifecycle.create_tag_masking_policy_reference(change.urn, change.after, props))
    elif isinstance(change, DropResource):
        if transfer_owner:
            before_change_cmd.append(
                lifecycle.transfer_resource(
                    change.urn,
                    owner=str(execution_role),
                    owner_resource_type=infer_role_type_from_name(str(execution_role)),
                    copy_current_grants=True,
                )
            )
        shared_database = _shared_database_for_grant(change, shared_databases)
        if shared_database:
            change_cmd = lifecycle.drop_shared_database_grant(change.before, shared_database)
        else:
            change_cmd = lifecycle.drop_resource(
                change.urn,
                change.before,
                if_exists=True,
            )
    elif isinstance(change, TransferOwnership):
        change_cmd = lifecycle.transfer_resource(
            change.urn,
            owner=change.to_owner,
            owner_resource_type=infer_role_type_from_name(change.to_owner),
            copy_current_grants=True,
        )

    all_cmds = before_change_cmd + _as_command_list(change_cmd) + after_change_cmd
    return execution_role, [cmd for cmd in all_cmds if cmd is not None]


def compile_plan_to_sql(
    session_ctx: SessionContext,
    plan: Plan,
    shared_databases: Optional[set[str]] = None,
    database_owners: Optional[dict[str, str]] = None,
) -> tuple[list[dict], list[ResourceName]]:
    """Compile the plan into a list of SQL command lists, one per change.

    Returns:
        A tuple of (sql_commands_per_change, available_roles) where available_roles
        includes any roles being created in this plan.
    """
    sql_commands_per_change = []
    available_roles = session_ctx["available_roles"].copy()
    default_role = session_ctx["role"]
    current_user = ResourceName(session_ctx.get("user", "")) if session_ctx.get("user") else None
    # Containers this plan hands to a new owner. Resources created inside one of them
    # have to be created by the owner it ends up with, not the one it had when the plan
    # was built, because the transfer runs first.
    transferred_owners: dict[URN, ResourceName] = {
        change.urn: ResourceName(change.to_owner) for change in plan if isinstance(change, TransferOwnership)
    }
    for change in plan:
        if isinstance(change, CreateResource):
            if change.urn.resource_type == ResourceType.ROLE:
                available_roles.append(ResourceName(change.after["name"]))
            elif change.urn.resource_type == ResourceType.ROLE_GRANT:
                # Handle role grants to another role that we already have
                if change.after.get("to_role") and change.after["to_role"] in available_roles:
                    available_roles.append(ResourceName(change.after["role"]))
                # Handle role grants to the current user
                elif (
                    current_user
                    and change.after.get("to_user")
                    and ResourceName(change.after["to_user"]) == current_user
                ):
                    available_roles.append(ResourceName(change.after["role"]))
    for change in plan:
        role, commands = sql_commands_for_change(
            change, available_roles, default_role, transferred_owners, shared_databases, database_owners
        )
        sql_commands_per_change.append({"role": role, "commands": commands, "change": change})
    return sql_commands_per_change, available_roles


def compute_levels(resource_set: Set[URN], references: Set[tuple[URN, URN]]) -> dict[URN, int]:
    """
    Compute the dependency level for each URN based on references.

    In this context, a reference (parent, ref) means that parent depends on ref.
    For example, if we have (A, B), it means A depends on B, so B must be created before A.

    The level of a resource indicates its position in the dependency hierarchy:
    - Level 0: Resources with no dependencies
    - Level 1: Resources that depend only on level 0 resources
    - Level 2: Resources that depend on level 0 or level 1 resources
    - And so on...

    This function uses Kahn's algorithm for topological sorting to assign levels.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Computing levels for {len(resource_set)} resources with {len(references)} references")

    # Make a copy of the resource set to avoid modifying the original
    resources = set(resource_set)

    # Initialize in-degrees dictionary
    in_degrees = {urn: 0 for urn in resources}

    # Build adjacency list for faster processing
    adjacency_list: dict[URN, list[URN]] = {urn: [] for urn in resources}

    # Compute in-degrees and build adjacency list
    # Note: (parent, ref) means parent depends on ref
    for parent, ref in references:
        in_degrees[parent] += 1  # Parent depends on ref, so increment parent's in-degree
        adjacency_list[ref].append(parent)  # ref -> parent (ref is required by parent)
        logger.debug(f"Dependency: {parent} depends on {ref}")

    levels = {}
    # Start with nodes that have no dependencies (in-degree = 0)
    queue = [urn for urn in resources if in_degrees[urn] == 0]
    logger.debug(f"Initial queue with {len(queue)} resources: {queue}")

    if not queue:
        # If there are no nodes with in-degree 0, there must be a cycle
        logger.error("No resources with in-degree 0 found, graph contains cycles")
        raise NotADAGException("Dependency graph contains cycles")

    current_level = 0
    processed_count = 0

    while queue:
        logger.debug(f"Processing level {current_level} with {len(queue)} resources")
        next_queue = []

        # All nodes in the current queue are at the current level
        for urn in queue:
            levels[urn] = current_level
            processed_count += 1
            logger.debug(f"Assigned level {current_level} to {urn}")

            # Process all resources that depend on this one
            for dependent in adjacency_list[urn]:
                in_degrees[dependent] -= 1
                logger.debug(f"Decremented in_degree for {dependent} to {in_degrees[dependent]}")

                if in_degrees[dependent] == 0:
                    logger.debug(f"Adding {dependent} to next_queue for level {current_level + 1}")
                    next_queue.append(dependent)

        queue = next_queue
        current_level += 1

        # Safety check to prevent infinite loops
        if not queue and processed_count < len(resources):
            remaining = [urn for urn in resources if urn not in levels]
            logger.error(f"Queue empty but {len(remaining)} resources not processed: {remaining}")
            logger.error(f"Remaining in_degrees: {[(urn, deg) for urn, deg in in_degrees.items() if urn in remaining]}")

            # Find cycles in the remaining nodes
            cycle_candidates = [urn for urn, deg in in_degrees.items() if deg > 0 and urn not in levels]
            if cycle_candidates:
                logger.error(f"Potential cycle involving: {cycle_candidates}")

            raise NotADAGException("Dependency graph contains cycles")

    logger.debug(f"Processed {processed_count}/{len(resources)} resources")
    logger.debug(f"Final levels: {levels}")

    # This check should never fail if the algorithm is implemented correctly
    if len(levels) != len(resources):
        unprocessed = resources - set(levels.keys())
        logger.error(f"Not all resources assigned levels. Unprocessed: {unprocessed}")
        raise NotADAGException("Dependency graph contains cycles")

    return levels


def diff(remote_state: State, manifest: Manifest) -> list:
    """Compute the differences between remote state and manifest"""

    def _container_descriptor(urn: URN) -> Optional[ContainerDescriptor]:
        """
        Given the URN of a resource, return a descriptor of the container that owns it.
        """
        if isinstance(RESOURCE_SCOPES[urn.resource_type], AccountScope):
            return None

        container_urn = _container_urn(urn)
        if container_urn in remote_state:
            if "owner" in remote_state[container_urn]:
                container_owner = remote_state[container_urn]["owner"]
            else:
                raise Exception(f"Remote state for {container_urn} is missing owner -> {remote_state[container_urn]}")
        else:
            manifest_item = manifest[container_urn]
            if isinstance(manifest_item, ManifestResource):
                container_owner = manifest_item.data["owner"]
            else:
                raise MissingResourceException(
                    format_missing_container_error(container_urn),
                    missing_urn=container_urn,
                )

        return (container_urn, container_owner)

    def _diff_resource_data(lhs: dict, rhs: dict) -> dict:
        delta = {}
        for field_name in lhs.keys():
            lhs_value = lhs[field_name]
            rhs_value = rhs[field_name]
            # Skip fields where manifest value is None or empty string - means "use Snowflake default/inherit"
            if rhs_value is None or rhs_value == "":
                continue
            # Normalize empty strings to None for comparison (Snowflake returns None for unset fields)
            if lhs_value == "":
                lhs_value = None
            if lhs_value != rhs_value:
                delta[field_name] = rhs_value
        return delta

    changes: list[ResourceChange] = []
    state_urns = set(remote_state.keys())
    manifest_urns = set(manifest.urns)

    # Debug logging for tag masking policy references
    tmpr_state_urns = [u for u in state_urns if u.resource_type == ResourceType.TAG_MASKING_POLICY_REFERENCE]
    tmpr_manifest_urns = [u for u in manifest_urns if u.resource_type == ResourceType.TAG_MASKING_POLICY_REFERENCE]
    if tmpr_state_urns or tmpr_manifest_urns:
        logger.debug("TAG_MASKING_POLICY_REFERENCE comparison:")
        logger.debug(f"  State URNs ({len(tmpr_state_urns)}):")
        for urn in tmpr_state_urns:
            logger.debug(f"    {urn} (hash={hash(urn)})")
        logger.debug(f"  Manifest URNs ({len(tmpr_manifest_urns)}):")
        for urn in tmpr_manifest_urns:
            logger.debug(f"    {urn} (hash={hash(urn)})")
            # Check if this URN matches any state URN
            for state_urn in tmpr_state_urns:
                if urn == state_urn:
                    logger.debug("      MATCHES state URN!")
                else:
                    logger.debug(f"      != {state_urn}")
                    logger.debug(f"        fqn match: {urn.fqn == state_urn.fqn}")
                    logger.debug(f"        resource_type match: {urn.resource_type == state_urn.resource_type}")
                    logger.debug(f"        account_locator match: {urn.account_locator == state_urn.account_locator}")

    collection_grants = [
        r
        for r in manifest.resources
        if not isinstance(r, ResourcePointer)
        and r.resource_cls == Grant
        and r.data["grant_type"] in (GrantType.ALL.value, GrantType.INHERITED.value)
    ]

    imported_privilege_grants = [
        r
        for r in manifest.resources
        if not isinstance(r, ResourcePointer)
        and r.resource_cls == Grant
        and r.data["priv"] == "IMPORTED PRIVILEGES"
        and r.data["on_type"] == ResourceType.DATABASE.value
    ]

    # Resources in remote state but not in the manifest should be removed
    for urn in state_urns - manifest_urns:
        remote_res = remote_state[urn]
        # A grant on a collection of objects covers the per-object grants it produced, which
        # are in remote state but never in the manifest. Dropping those would revoke the
        # access the collection grant just handed out.
        if _covered_by_collection_grant(collection_grants, remote_res):
            continue
        # Same reasoning for the fan-out of an IMPORTED PRIVILEGES grant on a shared database.
        if _covered_by_imported_privileges(imported_privilege_grants, remote_res):
            continue
        changes.append(DropResource(urn, remote_state[urn]))

    # Resources in the manifest but not in remote state should be added
    for urn in manifest_urns - state_urns:
        manifest_item = manifest[urn]
        if isinstance(manifest_item, ResourcePointer):
            available_names = [str(u.fqn.name) for u in manifest_urns if u.resource_type == urn.resource_type]
            raise MissingResourceException(
                format_missing_pointer_error(urn, available_names),
                missing_urn=urn,
                suggestions=available_names,
            )
        elif isinstance(manifest_item, ManifestResource) and not manifest_item.implicit:
            changes.append(
                CreateResource(
                    urn,
                    manifest_item.resource_cls,
                    _container_descriptor(urn),
                    manifest_item.data,
                )
            )

    # Resources in both should be compared
    for urn in state_urns & manifest_urns:
        manifest_item = manifest[urn]
        if isinstance(manifest_item, ResourcePointer):
            continue
        delta = _diff_resource_data(remote_state[urn], manifest_item.data)
        owner_attr = delta.pop("owner", None)

        replacement_attr = None
        replacement_message = None
        create_resource = False
        ignore_fields = set()

        for attr in delta.keys():
            attr_metadata = manifest_item.resource_cls.spec.get_metadata(attr)
            change_requires_replacement = attr_metadata.triggers_replacement
            change_triggers_create = attr_metadata.triggers_create
            change_is_fetchable = attr_metadata.fetchable
            change_is_known_after_apply = attr_metadata.known_after_apply
            change_should_be_ignored = attr in manifest_item.lifecycle.ignore_changes or attr_metadata.ignore_changes
            if change_requires_replacement:
                replacement_attr = attr
                replacement_message = attr_metadata.replacement_message
                break
            elif change_triggers_create:
                create_resource = True
                break
            elif not change_is_fetchable:
                ignore_fields.add(attr)
            elif change_is_known_after_apply:
                ignore_fields.add(attr)
            elif change_should_be_ignored:
                ignore_fields.add(attr)

        if replacement_attr:
            message = (
                f"Cannot update {urn}: changing '{replacement_attr}' requires replacing the resource, "
                "which snowcap does not do."
            )
            if replacement_message:
                message = f"{message} {replacement_message}"
            raise MarkedForReplacementException(message)

        if create_resource:
            changes.append(
                CreateResource(
                    urn,
                    manifest_item.resource_cls,
                    _container_descriptor(urn),
                    manifest_item.data,
                )
            )
            continue

        delta = {k: v for k, v in delta.items() if k not in ignore_fields}
        if delta:
            # Snowflake doesn't support converting to or from X5LARGE/X6LARGE warehouses
            # (https://docs.snowflake.com/en/user-guide/warehouses-adaptive), so fail at
            # plan time instead of erroring mid-apply.
            if urn.resource_type == ResourceType.WAREHOUSE and "warehouse_type" in delta:
                converting_adaptive = "ADAPTIVE" in (delta["warehouse_type"], remote_state[urn].get("warehouse_type"))
                sizes = {remote_state[urn].get("warehouse_size"), manifest_item.data.get("warehouse_size")}
                if converting_adaptive and sizes & {"X5LARGE", "X6LARGE"}:
                    raise InvalidResourceException(
                        f"{urn}: Snowflake does not support converting an X5LARGE or X6LARGE warehouse "
                        "to ADAPTIVE, or converting an ADAPTIVE warehouse to those sizes"
                    )
            changes.append(
                UpdateResource(
                    urn,
                    manifest_item.resource_cls,
                    remote_state[urn],
                    manifest_item.data,
                    delta,
                )
            )

        # Force transfers to occur after all other attribute changes
        if owner_attr:
            owner_metadata = manifest_item.resource_cls.spec.get_metadata("owner")
            owner_is_fetchable = owner_metadata.fetchable
            owner_changes_should_be_ignored = (
                "owner" in manifest_item.lifecycle.ignore_changes or owner_metadata.ignore_changes
            )

            if not owner_is_fetchable or owner_changes_should_be_ignored:
                continue

            changes.append(
                TransferOwnership(
                    urn,
                    manifest_item.resource_cls,
                    remote_state[urn]["owner"],
                    manifest_item.data["owner"],
                )
            )

    # An MCP server specification change is applied via CREATE OR REPLACE (Snowflake has no
    # ALTER MCP SERVER and COPY GRANTS is not supported), which drops all grants on the server.
    # Re-create manifest grants targeting the replaced server in the same plan; grants execute
    # after their target, so they are restored in the same apply.
    replaced_mcp_servers = {
        str(change.urn.fqn)
        for change in changes
        if isinstance(change, UpdateResource)
        and change.urn.resource_type == ResourceType.MCP_SERVER
        and "specification" in change.delta
    }
    if replaced_mcp_servers:
        changed_urns = {change.urn for change in changes}
        for urn in manifest_urns:
            manifest_item = manifest[urn]
            if (
                urn.resource_type == ResourceType.GRANT
                and not isinstance(manifest_item, ResourcePointer)
                and manifest_item.data["grant_type"] == GrantType.OBJECT.value
                and manifest_item.data["on_type"] == ResourceType.MCP_SERVER.value
                and manifest_item.data["on"] in replaced_mcp_servers
                and urn not in changed_urns
            ):
                changes.append(
                    CreateResource(
                        urn,
                        manifest_item.resource_cls,
                        _container_descriptor(urn),
                        manifest_item.data,
                    )
                )

    return changes


def _container_urn(resource_urn: URN) -> URN:
    scope = RESOURCE_SCOPES[resource_urn.resource_type]
    container_urn: URN

    if isinstance(scope, AccountScope):
        container_urn = resource_urn.account()
    elif isinstance(scope, DatabaseScope):
        container_urn = resource_urn.database()
    elif isinstance(scope, SchemaScope):
        container_urn = resource_urn.schema()
    else:
        raise NotImplementedError(f"Unsupported resource scope: {scope}")
    return container_urn
