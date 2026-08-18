from typing import Any

from snowcap.blueprint import Blueprint
from snowcap.blueprint import plan_from_dict, levels_from_plan_dict
from snowcap.blueprint_config import BlueprintConfig

from snowcap.gitops import collect_blueprint_config
from snowcap.operations.connector import connect


def blueprint_plan(yaml_config: dict, cli_config: dict[str, Any]):
    blueprint_config = collect_blueprint_config(yaml_config, cli_config)
    blueprint = Blueprint.from_config(blueprint_config)
    session = connect()
    plan_obj = blueprint.plan(session)
    return plan_obj, blueprint._levels


def blueprint_apply(yaml_config: dict, cli_config: dict):
    blueprint_config = collect_blueprint_config(yaml_config, cli_config)
    blueprint = Blueprint.from_config(blueprint_config)
    session = connect()
    blueprint.apply(session)


def blueprint_apply_plan(plan_dict: dict, cli_config: dict):
    blueprint_config = BlueprintConfig(**cli_config)
    blueprint = Blueprint.from_config(blueprint_config)
    plan = plan_from_dict(plan_dict)
    # Restore the dependency levels the plan was saved with so ordering is preserved; without
    # this the apply-plan path runs every change at level 0.
    blueprint._levels = levels_from_plan_dict(plan_dict)
    session = connect()
    blueprint.apply(session, plan)
