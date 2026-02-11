from dataclasses import dataclass, field
from typing import Optional

from .enums import BlueprintScope, ResourceType
from .exceptions import InvalidResourceException, MissingVarException
from .resource_name import ResourceName
from .resources.resource import Resource

_VAR_TYPE_MAP = {
    "bool": bool,
    "boolean": bool,
    "float": float,
    "int": int,
    "integer": int,
    "str": str,
    "string": str,
    "list": list,
}


@dataclass  # (frozen=True)
class BlueprintConfig:
    name: Optional[str] = None
    resources: Optional[list[Resource]] = None
    dry_run: bool = False
    sync_resources: Optional[list[ResourceType]] = None
    vars: dict = field(default_factory=dict)
    vars_spec: list[dict] = field(default_factory=list)
    scope: Optional[BlueprintScope] = None
    database: Optional[ResourceName] = None
    schema: Optional[ResourceName] = None
    threads: int = 8

    def __post_init__(self):

        if self.dry_run is None:
            raise ValueError("dry_run must be provided")
        if self.vars is None:
            raise ValueError("vars must be provided")
        if self.vars_spec is None:
            raise ValueError("vars_spec must be provided")

        if not isinstance(self.vars, dict):
            raise ValueError(f"vars must be a dictionary, got: {self.vars=}")

        if self.scope is not None and not isinstance(self.scope, BlueprintScope):
            raise ValueError(f"Invalid scope: {self.scope}")

        if self.sync_resources is not None:
            if len(self.sync_resources) == 0:
                raise ValueError("Sync Resources must have at least one resource type")

        if self.vars_spec:
            for var in self.vars_spec:
                if var.get("name") is None:
                    raise ValueError("All vars_spec entries must specify a name")
                if var.get("type") is None:
                    raise ValueError("All vars_spec entries must specify a type")
                elif var["type"] not in _VAR_TYPE_MAP:
                    raise ValueError(f"Vars must specify a valid type. Got: {var['type']}")

            # Create a set of all var names in vars_spec for efficient lookup
            spec_names = {var["name"] for var in self.vars_spec}

            # Check each var against its spec
            for var_name, var_value in self.vars.items():
                if var_name not in spec_names:
                    raise ValueError(f"Var '{var_name}' was provided without config")

                spec = next(s for s in self.vars_spec if s["name"] == var_name)
                if not isinstance(var_value, _VAR_TYPE_MAP[spec["type"]]):
                    raise TypeError(f"Var '{var_name}' should be of type {spec['type']}")

            # Check for missing vars and use defaults if available
            # TODO: this causes us to violate frozen=true
            self.vars = set_vars_defaults(self.vars_spec, self.vars)

        if self.scope == BlueprintScope.DATABASE and self.schema is not None:
            raise ValueError("Cannot specify a schema when using DATABASE scope")
        elif self.scope == BlueprintScope.ACCOUNT and (self.database is not None or self.schema is not None):
            raise ValueError(
                f"Cannot specify a database or schema when using ACCOUNT scope (database={repr(self.database)}, schema={repr(self.schema)})"
            )
        if not isinstance(self.threads, int):
            raise ValueError(f"Threads must be an integer, got: {self.threads}")


def set_vars_defaults(vars_spec: list[dict], vars: dict) -> dict:
    new_vars = vars.copy()
    for spec in vars_spec:
        if spec["name"] not in new_vars:
            if "default" in spec:
                new_vars[spec["name"]] = spec["default"]
            else:
                raise MissingVarException(f"Required var '{spec['name']}' is missing and has no default value")
    return new_vars


def print_blueprint_config(config: BlueprintConfig):
    print(f"{config.name=}")
    print(f"config.resources={len(config.resources or [])}")
    print(f"{config.dry_run=}")
    print(f"{config.sync_resources=}")
    print(f"config.vars={list(config.vars.keys())}")
