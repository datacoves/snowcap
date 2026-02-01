import logging
import os
from typing import Any, Optional

import yaml
from inflection import pluralize
from pathspec import PathSpec
from pathspec.patterns.gitwildmatch import GitWildMatchPattern

from .blueprint_config import BlueprintConfig, set_vars_defaults
from .enums import BlueprintScope, ResourceType
from .identifiers import resource_label_for_type, resource_type_for_label
from .resources import Database, DatabaseRoleGrant, Resource, RoleGrant, Schema, User
from .resources.resource import ResourcePointer
from .var import process_for_each, string_contains_var

logger = logging.getLogger("snowcap")

ALIASES = {
    "account_parameters": ResourceType.ACCOUNT_PARAMETER,
}


def construct_string_on_off(loader, node):
    """Custom constructor for YAML bool values to handle 'on' and 'off' as strings."""
    value = loader.construct_scalar(node)
    if value in ("on", "off"):
        return value  # treat as string
    # fallback to default bool constructor for other values
    return yaml.constructor.SafeConstructor.construct_yaml_bool(loader, node)


yaml.add_constructor("tag:yaml.org,2002:bool", construct_string_on_off, yaml.SafeLoader)


def _resources_from_role_grants_config(role_grants_config: list) -> list:
    if len(role_grants_config) == 0:
        return []
    resources = []
    for role_grant in role_grants_config:
        # When only one role is being assigned
        if "role" in role_grant:
            # To one role
            if "to_role" in role_grant:
                resources.append(
                    RoleGrant(
                        role=role_grant["role"],
                        to_role=role_grant["to_role"],
                    )
                )
            # To one user
            elif "to_user" in role_grant:
                resources.append(
                    RoleGrant(
                        role=role_grant["role"],
                        to_user=role_grant["to_user"],
                    )
                )
            else:
                # To multiple users
                for user in role_grant.get("to_users", []):
                    resources.append(
                        RoleGrant(
                            role=role_grant["role"],
                            to_user=user,
                        )
                    )
                # To multiple roles
                for to_role in role_grant.get("to_roles", []):
                    resources.append(
                        RoleGrant(
                            role=role_grant["role"],
                            to_role=to_role,
                        )
                    )
        # When multiple roles are being assigned, to a single user
        elif "to_user" in role_grant:
            for role in role_grant.get("roles", []):
                resources.append(
                    RoleGrant(
                        role=role,
                        to_user=role_grant["to_user"],
                    )
                )
        # When multiple roles are being assigned, to a single role
        elif "to_role" in role_grant:
            for role in role_grant.get("roles", []):
                resources.append(
                    RoleGrant(
                        role=role,
                        to_role=role_grant["to_role"],
                    )
                )
    if len(resources) == 0:
        raise ValueError(f"No role grants found in config: `{role_grants_config}`")
    return resources


def _resources_from_database_role_grants_config(database_role_grants_config: list) -> list:
    if len(database_role_grants_config) == 0:
        return []
    resources = []
    for database_role_grant in database_role_grants_config:
        if "to_role" in database_role_grant:
            resources.append(
                DatabaseRoleGrant(
                    database_role=database_role_grant["database_role"],
                    to_role=database_role_grant["to_role"],
                )
            )
        else:
            for role in database_role_grant.get("roles", []):
                resources.append(
                    DatabaseRoleGrant(
                        database_role=database_role_grant["database_role"],
                        to_role=role,
                    )
                )
    return resources


def process_requires(resource: Resource, requires: list):
    for req in requires:
        resource.requires(ResourcePointer(name=req["name"], resource_type=ResourceType(req["resource_type"])))


def _resources_for_config(config: dict, vars: dict):
    # Special cases
    role_grants = config.pop("role_grants", [])
    database_role_grants = config.pop("database_role_grants", [])

    resources = []
    config_blocks = []

    for resource_type in Resource.__types__.keys():
        resource_label = pluralize(resource_label_for_type(resource_type))
        block = config.pop(resource_label, [])
        if block:
            config_blocks.append((resource_type, block))

    for alias, resource_type in ALIASES.items():
        if alias in config:
            config_blocks.append((resource_type, config.pop(alias)))

    for resource_type, block in config_blocks:
        for resource_data in block:

            if isinstance(resource_data, dict):
                if "for_each" in resource_data:
                    resource_cls = Resource.resolve_resource_cls(resource_type, resource_data)
                    resource_instance = resource_data.copy()
                    for_each = resource_instance.pop("for_each")

                    if isinstance(for_each, str) and for_each.startswith("var."):
                        var_name = for_each.split(".")[1]
                        if var_name not in vars:
                            raise ValueError(f"Var `{var_name}` not found in for_each")
                        for_each_input = vars[var_name]
                    else:
                        raise ValueError(f"for_each must be a var reference. Got: `{for_each}`")

                    for each_value in for_each_input:
                        for key, value in resource_data.items():
                            if isinstance(value, str) and string_contains_var(value):
                                key_type = getattr(resource_cls.spec, key, None)
                                resource_instance[key] = process_for_each(value, each_value)
                                if key_type and type(key_type) is int:
                                    resource_instance[key] = int(resource_instance[key])
                            elif isinstance(value, list):
                                new_value = []
                                for v in value:
                                    if isinstance(v, str) and string_contains_var(v):
                                        new_value.append(process_for_each(v, each_value))
                                    else:
                                        new_value.append(v)
                                resource_instance[key] = new_value

                        resource = resource_cls(**resource_instance)
                        resources.append(resource)
                        resources += resource.process_shortcuts()
                else:
                    requires = resource_data.pop("requires", [])
                    resource_cls = Resource.resolve_resource_cls(resource_type, resource_data)
                    resource = resource_cls(**resource_data)
                    process_requires(resource, requires)
                    resources.append(resource)
                    resources += resource.process_shortcuts()
            elif isinstance(resource_data, str):
                resource_cls = Resource.resolve_resource_cls(resource_type, {})
                resource = resource_cls.from_sql(resource_data)
                resources.append(resource)
            else:
                raise Exception(f"Unknown resource data type: {resource_data}")

    resources.extend(_resources_from_role_grants_config(role_grants))
    resources.extend(_resources_from_database_role_grants_config(database_role_grants))

    # This code helps resolve grant references to the fully qualified name of the resource.
    # This probably belongs in blueprint as a finalization step.
    # resource_cache = {}
    # for resource in resources:
    #     if hasattr(resource._data, "name"):
    #         resource_cache[(resource.resource_type, ResourceName(resource._data.name))] = resource

    # for resource in resources:
    #     if resource.resource_type == ResourceType.GRANT:
    #         cache_pointer = (resource.on_type, ResourceName(resource.on))
    #         if cache_pointer in resource_cache:
    #             resource._data.on = ResourceName(str(resource_cache[cache_pointer].fqn))

    return resources


def collect_blueprint_config(yaml_config: dict, cli_config: Optional[dict[str, Any]] = None) -> BlueprintConfig:
    yaml_config_ = yaml_config.copy()
    cli_config_ = cli_config.copy() if cli_config else {}
    blueprint_args: dict[str, Any] = {}

    for key in ["sync_resources", "dry_run", "name"]:
        if key in yaml_config_ and key in cli_config_:
            raise ValueError(f"Cannot specify `{key}` in both yaml config and cli")

    sync_resources = yaml_config_.pop("sync_resources", None) or cli_config_.pop("sync_resources", None)
    database = yaml_config_.pop("database", None) or cli_config_.pop("database", None)
    dry_run = yaml_config_.pop("dry_run", None) or cli_config_.pop("dry_run", None)
    name = yaml_config_.pop("name", None) or cli_config_.pop("name", None)
    scope = yaml_config_.pop("scope", None) or cli_config_.pop("scope", None)
    schema = yaml_config_.pop("schema", None) or cli_config_.pop("schema", None)
    input_vars = cli_config_.pop("vars", {}) or {}
    vars_spec = yaml_config_.pop("vars", [])

    if sync_resources:
        blueprint_args["sync_resources"] = [ResourceType(resource_type) for resource_type in sync_resources]

    if database:
        blueprint_args["database"] = database

    if dry_run:
        blueprint_args["dry_run"] = dry_run

    if name:
        blueprint_args["name"] = name

    if scope:
        blueprint_args["scope"] = BlueprintScope(scope)

    if schema:
        blueprint_args["schema"] = schema

    blueprint_args["vars"] = input_vars

    if vars_spec:
        if not isinstance(vars_spec, list):
            raise ValueError("vars config entry must be a list of dicts")
        blueprint_args["vars_spec"] = vars_spec
        blueprint_args["vars"] = set_vars_defaults(vars_spec, blueprint_args["vars"])

    resources = _resources_for_config(yaml_config_, blueprint_args["vars"])

    if len(resources) == 0:
        raise ValueError("No resources found in config")

    blueprint_args["resources"] = resources

    if yaml_config_:
        raise ValueError(f"Unknown keys in config: `{yaml_config_.keys()}`")

    return BlueprintConfig(**blueprint_args)


def crawl(path: str):
    # Load .snowcapignore patterns if the file exists
    gitignore_path = os.path.join(path, ".snowcapignore")
    if os.path.exists(gitignore_path):
        with open(gitignore_path) as f:
            spec = PathSpec.from_lines(GitWildMatchPattern, f.readlines())
    else:
        spec = PathSpec([])

    if os.path.isfile(path):
        yield path
        return

    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith(".yaml") or file.endswith(".yml"):
                full_path = os.path.join(root, file)
                # Get path relative to the base path for snowcapignore matching
                rel_path = os.path.relpath(full_path, path)
                if not spec.match_file(rel_path):
                    yield full_path


def read_config(config_path) -> dict:
    with open(config_path, "r") as f:
        try:
            config = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: `{config_path}`") from e
    return config


def merge_configs(config1: dict, config2: dict) -> dict:
    merged = config1.copy()
    for key, value in config2.items():
        if key in merged:
            if isinstance(merged[key], list):
                merged[key] = merged[key] + value
            elif merged[key] is None:
                merged[key] = value
            else:
                raise ValueError(f"Found a conflict for key `{key}` with {value} and {merged[key]}")
        else:
            merged[key] = value
    return merged


def collect_configs_from_path(path: str) -> list[tuple[str, dict]]:
    configs = []

    if not os.path.exists(path):
        raise ValueError(f"Invalid path: `{path}`. Must be a file or directory.")

    for file in crawl(path):
        config = read_config(file)
        configs.append((file, config))

    if len(configs) == 0:
        raise ValueError(f"No valid YAML files were read from the given path: {path}")

    return configs


def parse_resources(resource_labels_str: Optional[str]) -> Optional[list[ResourceType]]:
    if resource_labels_str is None or resource_labels_str == "all":
        return None
    return [resource_type_for_label(resource_label) for resource_label in resource_labels_str.split(",")]


def collect_vars_from_environment() -> dict:
    vars = {}
    for key, value in os.environ.items():
        if key.startswith("SNOWCAP_VAR_"):
            vars[key[12:].lower()] = value
    return vars


def merge_vars(vars: dict, other_vars: dict) -> dict:
    for key in other_vars.keys():
        if key in vars:
            raise ValueError(f"Conflicting var found: '{key}'")
    merged = vars.copy()
    merged.update(other_vars)
    return merged
