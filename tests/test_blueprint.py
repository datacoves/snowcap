import json
import logging
import re
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from snowcap import resources as res


def strip_ansi(text):
    """Remove ANSI color codes from text."""
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def find_change_by_urn(plan, urn):
    """Find a change in the plan by its URN."""
    for change in plan:
        if change.urn == urn:
            return change
    return None


def flatten_sql_commands(sql_commands_result) -> list[str]:
    """Flatten compile_plan_to_sql output to a list of SQL strings for testing.

    Args:
        sql_commands_result: Either a list[dict] (legacy) or a tuple of (list[dict], available_roles)
    """
    # Handle both old and new return formats from compile_plan_to_sql
    if isinstance(sql_commands_result, tuple):
        sql_commands = sql_commands_result[0]
    else:
        sql_commands = sql_commands_result

    result = ["USE SECONDARY ROLES ALL"]
    last_role = None
    for cmd in sql_commands:
        if cmd["role"] != last_role:
            result.append(f"USE ROLE {cmd['role']}")
            last_role = cmd["role"]
        result.extend(cmd["commands"])
    return result


from snowcap import data_provider, var
from snowcap.blueprint import (
    Blueprint,
    CreateResource,
    DropResource,
    TransferOwnership,
    UpdateResource,
    compute_levels,
    _merge_pointers,
    _summarize_plan_value,
    compile_plan_to_sql,
    diff,
    dump_plan,
    execution_strategy_for_change,
    future_grant_precedence_warnings,
    manifest_state_entries,
    plan_entries,
    raise_if_inherited_grants_unavailable,
)
from snowcap.blueprint_config import BlueprintConfig
from snowcap.data_provider import fetch_warehouse
from snowcap.enums import AccountEdition, BlueprintScope, ResourceType
from snowcap.exceptions import (
    DuplicateResourceException,
    MissingPrivilegeException,
    InvalidResourceException,
    MarkedForReplacementException,
    MissingVarException,
    NonConformingPlanException,
    OrphanResourceException,
    WrongEditionException,
)
from snowcap.identifiers import FQN, URN, parse_URN
from snowcap.resource_name import ResourceName
from snowcap.resources.resource import ResourcePointer
from snowcap.var import VarString


@pytest.fixture
def session_ctx() -> dict:
    return {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "role": "SYSADMIN",
        "available_roles": [
            "SYSADMIN",
            "USERADMIN",
            "ACCOUNTADMIN",
            "SECURITYADMIN",
            "PUBLIC",
        ],
    }


@pytest.fixture
def remote_state() -> dict:
    return {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
    }


@pytest.fixture
def resource_manifest():
    session_ctx = {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "current_role": "SYSADMIN",
        "available_roles": ["SYSADMIN", "USERADMIN"],
    }
    db = res.Database(name="DB")
    schema = res.Schema(name="SCHEMA", database=db)
    table = res.Table(name="TABLE", columns=[{"name": "ID", "data_type": "INT"}])
    schema.add(table)
    view = res.View(name="VIEW", schema=schema, as_="SELECT 1")
    udf = res.PythonUDF(
        name="SOMEUDF",
        returns="VARCHAR",
        args=[],
        runtime_version="3.9",
        handler="main",
        comment="This is a UDF comment",
    )
    schema.add(udf)
    blueprint = Blueprint(name="blueprint", resources=[db, table, schema, view, udf])
    manifest = blueprint.generate_manifest(session_ctx)
    return manifest


def test_blueprint_with_database(resource_manifest):

    db_urn = parse_URN("urn::ABCD123:database/DB")
    assert db_urn in resource_manifest
    assert resource_manifest[db_urn].data == {
        "name": "DB",
        "owner": "SYSADMIN",
        "comment": None,
        "catalog": None,
        "external_volume": None,
        "data_retention_time_in_days": None,  # Uses Snowflake default
        "default_ddl_collation": None,  # Uses Snowflake default
        "max_data_extension_time_in_days": None,  # Uses Snowflake default
        "transient": False,
    }


def test_blueprint_with_schema(resource_manifest):
    schema_urn = parse_URN("urn::ABCD123:schema/DB.SCHEMA")
    assert schema_urn in resource_manifest
    assert resource_manifest[schema_urn].data == {
        "comment": None,
        "data_retention_time_in_days": None,  # Inherits from database
        "default_ddl_collation": None,  # Inherits from database
        "managed_access": False,
        "max_data_extension_time_in_days": None,  # Inherits from database
        "name": "SCHEMA",
        "owner": "SYSADMIN",
        "transient": False,
    }


def test_blueprint_with_view(resource_manifest):
    view_urn = parse_URN("urn::ABCD123:view/DB.SCHEMA.VIEW")
    assert view_urn in resource_manifest
    assert resource_manifest[view_urn].data == {
        "as_": "SELECT 1",
        "change_tracking": False,
        "columns": None,
        "comment": None,
        "copy_grants": False,
        "name": "VIEW",
        "owner": "SYSADMIN",
        "recursive": None,
        "secure": False,
        "volatile": None,
    }


def test_blueprint_with_table(resource_manifest):
    table_urn = parse_URN("urn::ABCD123:table/DB.SCHEMA.TABLE")
    assert table_urn in resource_manifest
    assert resource_manifest[table_urn].data == {
        "name": "TABLE",
        "owner": "SYSADMIN",
        "columns": [
            {
                "name": "ID",
                "data_type": "NUMBER(38,0)",
                "collate": None,
                "comment": None,
                "constraint": None,
                "not_null": False,
                "default": None,
                "tags": None,
            }
        ],
        "constraints": None,
        "transient": False,
        "cluster_by": None,
        "enable_schema_evolution": False,
        "data_retention_time_in_days": None,
        "max_data_extension_time_in_days": None,
        "change_tracking": False,
        "default_ddl_collation": None,
        "copy_grants": None,
        "row_access_policy": None,
        "comment": None,
    }


def test_blueprint_with_udf(resource_manifest):
    # parse URN is incorrectly stripping the parens. Not sure what the correct behavior should be
    # udf_urn = parse_URN("urn::ABCD123:function/DB.PUBLIC.SOMEUDF()")
    udf_urn = URN(
        resource_type=ResourceType.FUNCTION,
        fqn=FQN(
            database=ResourceName("DB"),
            schema=ResourceName("SCHEMA"),
            name=ResourceName("SOMEUDF"),
            arg_types=[],
        ),
        account_locator="ABCD123",
    )
    assert udf_urn in resource_manifest
    assert resource_manifest[udf_urn].data == {
        "name": "SOMEUDF",
        "owner": "SYSADMIN",
        "returns": "VARCHAR",
        "handler": "main",
        "runtime_version": "3.9",
        "comment": "This is a UDF comment",
        "args": [],
        "as_": None,
        "copy_grants": False,
        "language": "PYTHON",
        "external_access_integrations": None,
        "imports": None,
        "null_handling": None,
        "packages": None,
        "secrets": None,
        "secure": None,
        "volatility": None,
    }


def test_blueprint_resource_owned_by_plan_role(session_ctx, remote_state):
    role = res.Role("SOME_ROLE")
    wh = res.Warehouse("WH", owner=role)
    grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    blueprint = Blueprint(name="blueprint", resources=[wh, role, grant])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)

    # Check all expected URNs are present (order not guaranteed)
    plan_urns = set(change.urn for change in plan)
    assert parse_URN("urn::ABCD123:role/SOME_ROLE") in plan_urns
    assert parse_URN("urn::ABCD123:role_grant/SOME_ROLE?role=SYSADMIN") in plan_urns
    assert parse_URN("urn::ABCD123:warehouse/WH") in plan_urns
    assert len(plan_urns) == 3

    changes = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    # Check expected commands are present (order may vary)
    assert "USE SECONDARY ROLES ALL" in changes
    assert "CREATE ROLE SOME_ROLE" in changes
    assert "GRANT ROLE SOME_ROLE TO ROLE SYSADMIN" in changes
    assert any(c.startswith("CREATE WAREHOUSE WH") for c in changes)
    assert "GRANT OWNERSHIP ON WAREHOUSE WH TO ROLE SOME_ROLE COPY CURRENT GRANTS" in changes


def test_blueprint_deduplicate_resources(session_ctx, remote_state):
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Database("DB"),
            ResourcePointer(name="DB", resource_type=ResourceType.DATABASE),
        ],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")
    assert plan[0].resource_cls == res.Database

    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Database("DB"),
            res.Database("DB", comment="This is a comment"),
        ],
    )
    with pytest.raises(DuplicateResourceException):
        blueprint.generate_manifest(session_ctx)

    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Grant(priv="USAGE", on_database="DB", to="SOME_ROLE"),
            res.Grant(priv="USAGE", on_database="DB", to="SOME_ROLE"),
        ],
    )
    with pytest.raises(DuplicateResourceException):
        blueprint.generate_manifest(session_ctx)


def test_blueprint_dont_add_public_schema(session_ctx, remote_state):
    db = res.Database("DB")
    public = ResourcePointer(name="PUBLIC", resource_type=ResourceType.SCHEMA)
    blueprint = Blueprint(
        name="blueprint",
        resources=[db, public],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")
    assert plan[0].resource_cls == res.Database


def test_blueprint_implied_container_tree(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:database/STATIC_DB")] = {"owner": "SYSADMIN"}
    remote_state[parse_URN("urn::ABCD123:schema/STATIC_DB.PUBLIC")] = {"owner": "SYSADMIN"}
    func = res.JavascriptUDF(
        name="func", args=[], returns="INT", as_="return 1;", database="STATIC_DB", schema="public"
    )
    blueprint = Blueprint(name="blueprint", resources=[func])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn.fqn.name == "FUNC"
    assert plan[0].resource_cls == res.JavascriptUDF


def test_blueprint_chained_ownership(session_ctx, remote_state):
    role = res.Role("SOME_ROLE")
    role_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    db = res.Database("DB", owner=role)
    schema = res.Schema("SCHEMA", database=db, owner=role)
    blueprint = Blueprint(name="blueprint", resources=[db, schema, role_grant, role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 4
    # Find changes by URN instead of relying on order
    role_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:role/SOME_ROLE"))
    grant_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:role_grant/SOME_ROLE?role=SYSADMIN"))
    db_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:database/DB"))
    schema_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:schema/DB.SCHEMA"))
    assert isinstance(role_change, CreateResource)
    assert role_change.resource_cls == res.Role
    assert isinstance(grant_change, CreateResource)
    assert grant_change.resource_cls == res.RoleGrant
    assert isinstance(db_change, CreateResource)
    assert db_change.resource_cls == res.Database
    assert isinstance(schema_change, CreateResource)
    assert schema_change.resource_cls == res.Schema


def test_blueprint_polymorphic_resource_resolution(session_ctx, remote_state):

    role = res.Role(name="DEMO_ROLE")
    sysad_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    test_db = res.Database(name="TEST_SNOWCAP", transient=False, data_retention_time_in_days=1, comment="Test Snowcap")
    schema = res.Schema(name="TEST_SCHEMA", database=test_db, transient=False, comment="Test Snowcap Schema")
    warehouse = res.Warehouse(name="FAKER_LOADER", auto_suspend=60)

    future_schema_grant = res.Grant(priv="usage", on=["FUTURE", "SCHEMAS", test_db], to=role)
    post_grant = [future_schema_grant]

    grants = [
        res.Grant(priv="usage", to=role, on=warehouse),
        res.Grant(priv="operate", to=role, on=warehouse),
        res.Grant(priv="usage", to=role, on=test_db),
        # future_schema_grant,
        # x
        # Grant(priv="usage", to=role, on=schema)
    ]

    sales_table = res.Table(
        name="faker_data",
        schema=schema,
        columns=[
            res.Column(name="NAME", data_type="VARCHAR(16777216)"),
            res.Column(name="EMAIL", data_type="VARCHAR(16777216)"),
            res.Column(name="ADDRESS", data_type="VARCHAR(16777216)"),
            res.Column(name="ORDERED_AT_UTC", data_type="NUMBER(38,0)"),
            res.Column(name="EXTRACTED_AT_UTC", data_type="NUMBER(38,0)"),
            res.Column(name="SALES_ORDER_ID", data_type="VARCHAR(16777216)"),
        ],
        comment="Test Table",
    )
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            role,
            sysad_grant,
            # user_grant,
            test_db,
            # *pre_grant,
            schema,
            sales_table,
            # pipe,
            warehouse,
            *grants,
        ],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 9


def test_blueprint_scope_sorting(session_ctx, remote_state):
    db = res.Database(name="DB")
    schema = res.Schema(name="SCHEMA", database=db)
    view = res.View(name="SOME_VIEW", schema=schema, as_="SELECT 1")
    blueprint = Blueprint(name="blueprint", resources=[view, schema, db])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 3
    # Find changes by URN instead of relying on order
    db_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:database/DB"))
    schema_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:schema/DB.SCHEMA"))
    view_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:view/DB.SCHEMA.SOME_VIEW"))
    assert isinstance(db_change, CreateResource)
    assert db_change.resource_cls == res.Database
    assert isinstance(schema_change, CreateResource)
    assert schema_change.resource_cls == res.Schema
    assert isinstance(view_change, CreateResource)
    assert view_change.resource_cls == res.View


def test_blueprint_reference_sorting(session_ctx, remote_state):
    db1 = res.Database(name="DB1")
    db2 = res.Database(name="DB2")
    db2.requires(db1)
    db3 = res.Database(name="DB3")
    db3.requires(db2)
    blueprint = Blueprint(resources=[db3, db1, db2])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 3
    # Find changes by URN instead of relying on order
    db1_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:database/DB1"))
    db2_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:database/DB2"))
    db3_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:database/DB3"))
    assert isinstance(db1_change, CreateResource)
    assert db1_change.resource_cls == res.Database
    assert isinstance(db2_change, CreateResource)
    assert db2_change.resource_cls == res.Database
    assert isinstance(db3_change, CreateResource)
    assert db3_change.resource_cls == res.Database


def test_blueprint_bulk_stage_read_write_ordering(session_ctx):
    """
    Bulk (ALL/FUTURE) stage grants must order READ before WRITE, just like
    grants on a single named stage. _create_stage_privilege_refs should make
    each WRITE grant depend on the matching READ grant over the same scope,
    for every ALL/FUTURE x DATABASE/SCHEMA combination.
    """
    role = res.Role(name="R_RW")

    # Every ALL/FUTURE x DATABASE/SCHEMA combination must order READ before
    # WRITE, since _create_stage_privilege_refs keys on items_type == STAGE
    # regardless of container type or grant type.
    all_db_read = res.Grant(priv="READ", on="all stages in database DB", to=role)
    all_db_write = res.Grant(priv="WRITE", on="all stages in database DB", to=role)
    all_sc_read = res.Grant(priv="READ", on="all stages in schema DB.SC", to=role)
    all_sc_write = res.Grant(priv="WRITE", on="all stages in schema DB.SC", to=role)
    future_db_read = res.Grant(priv="READ", on="future stages in database DB", to=role)
    future_db_write = res.Grant(priv="WRITE", on="future stages in database DB", to=role)
    future_sc_read = res.Grant(priv="READ", on="future stages in schema DB.SC", to=role)
    future_sc_write = res.Grant(priv="WRITE", on="future stages in schema DB.SC", to=role)

    blueprint = Blueprint(
        resources=[
            role,
            all_db_read,
            all_db_write,
            all_sc_read,
            all_sc_write,
            future_db_read,
            future_db_write,
            future_sc_read,
            future_sc_write,
        ]
    )
    blueprint._finalize(session_ctx)

    # WRITE depends on READ -> READ is applied first, for each scope
    assert all_db_read in all_db_write.refs
    assert all_sc_read in all_sc_write.refs
    assert future_db_read in future_db_write.refs
    assert future_sc_read in future_sc_write.refs

    # Scopes must not cross-link: a WRITE only depends on the READ over its
    # exact same scope, not on READs from other container/grant-type scopes.
    assert all_sc_read not in all_db_write.refs
    assert future_db_read not in all_db_write.refs
    assert future_sc_read not in future_db_write.refs
    assert all_db_read not in future_sc_write.refs


def test_blueprint_ownership_sorting(session_ctx, remote_state):

    role = res.Role(name="SOME_ROLE")
    role_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    wh = res.Warehouse(name="WH", owner=role)

    blueprint = Blueprint(resources=[wh, role_grant, role])
    manifest = blueprint.generate_manifest(session_ctx)

    plan = diff(remote_state, manifest)
    assert len(plan) == 3
    # Find changes by URN instead of relying on order
    role_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:role/SOME_ROLE"))
    grant_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:role_grant/SOME_ROLE?role=SYSADMIN"))
    wh_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:warehouse/WH"))
    assert isinstance(role_change, CreateResource)
    assert role_change.resource_cls == res.Role
    assert isinstance(grant_change, CreateResource)
    assert grant_change.resource_cls == res.RoleGrant
    assert isinstance(wh_change, CreateResource)
    assert wh_change.resource_cls == res.Warehouse

    sql = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    # Check expected commands are present (order may vary)
    assert "USE SECONDARY ROLES ALL" in sql
    assert "CREATE ROLE SOME_ROLE" in sql
    assert "GRANT ROLE SOME_ROLE TO ROLE SYSADMIN" in sql
    assert any(s.startswith("CREATE WAREHOUSE WH") for s in sql)
    assert "GRANT OWNERSHIP ON WAREHOUSE WH TO ROLE SOME_ROLE COPY CURRENT GRANTS" in sql


def test_blueprint_dump_plan_create(session_ctx, remote_state):
    blueprint = Blueprint(resources=[res.Role("role1")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "CREATE",
            "urn": "urn::ABCD123:role/ROLE1",
            "resource_cls": "Role",
            "container": None,
            "after": {"name": "ROLE1", "owner": "USERADMIN", "comment": None},
        }
    ]
    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert plan_str == """
» snowcap
» Plan: 1 to create, 0 to update, 0 to transfer, 0 to drop.

━━━ ROLES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
+ CREATE: ROLE1 (owner: USERADMIN)

"""


def test_blueprint_dump_plan_update(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:role/ROLE1"): {
            "name": "ROLE1",
            "owner": "USERADMIN",
            "comment": "old",
        },
    }
    blueprint = Blueprint(resources=[res.Role("role1", comment="new")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "UPDATE",
            "resource_cls": "Role",
            "urn": "urn::ABCD123:role/ROLE1",
            "before": {"name": "ROLE1", "owner": "USERADMIN", "comment": "old"},
            "after": {"name": "ROLE1", "owner": "USERADMIN", "comment": "new"},
            "delta": {"comment": "new"},
        }
    ]
    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert plan_str == """
» snowcap
» Plan: 0 to create, 1 to update, 0 to transfer, 0 to drop.

━━━ ROLES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
~ UPDATE: ROLE1
  ┌──────────┬────────┬───────┐
  │ Property │ Before │ After │
  ├──────────┼────────┼───────┤
  │ comment  │ old    │ new   │
  └──────────┴────────┴───────┘

"""


def test_blueprint_dump_plan_transfer(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:role/ROLE1"): {
            "name": "ROLE1",
            "owner": "ACCOUNTADMIN",
            "comment": None,
        },
    }
    blueprint = Blueprint(resources=[res.Role("role1", owner="USERADMIN")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "TRANSFER",
            "resource_cls": "Role",
            "urn": "urn::ABCD123:role/ROLE1",
            "from_owner": "ACCOUNTADMIN",
            "to_owner": "USERADMIN",
        }
    ]
    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert plan_str == """
» snowcap
» Plan: 0 to create, 0 to update, 1 to transfer, 0 to drop.

━━━ ROLES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
~ TRANSFER: ROLE1
  ┌──────────┬──────────────┬───────────┐
  │ Property │ Before       │ After     │
  ├──────────┼──────────────┼───────────┤
  │ owner    │ ACCOUNTADMIN │ USERADMIN │
  └──────────┴──────────────┴───────────┘

"""


def test_blueprint_dump_plan_drop(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:role/ROLE1"): {
            "name": "ROLE1",
            "owner": "ACCOUNTADMIN",
            "comment": None,
        },
    }
    blueprint = Blueprint(resources=[], sync_resources=[ResourceType.ROLE])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    plan_dict = json.loads(plan_json_str)
    assert len(plan_dict) == 1
    assert plan_dict[0] == {
        "action": "DROP",
        "urn": "urn::ABCD123:role/ROLE1",
        "before": {"name": "ROLE1", "owner": "ACCOUNTADMIN", "comment": None},
    }

    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert plan_str == """
» snowcap
» Plan: 0 to create, 0 to update, 0 to transfer, 1 to drop.

━━━ ROLES ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- DROP:   ROLE1

"""


def test_dump_plan_round_trips_dependency_levels(session_ctx, remote_state):
    """apply --plan must preserve ordering, so dump_plan persists each change's level and the
    loaders read it back. A bare-list plan (older format) restores to no levels."""
    from snowcap.blueprint import plan_from_dict, levels_from_plan_dict

    blueprint = Blueprint(resources=[res.Role("role1")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    urn = plan[0].urn

    dumped = json.loads(dump_plan(plan, format="json", levels={urn: 3}))
    assert dumped["levels"] == {str(urn): 3}
    assert [c.urn for c in plan_from_dict(dumped)] == [urn]
    assert levels_from_plan_dict(dumped) == {urn: 3}

    # Backward compatibility: a bare-list plan still parses, with no levels restored.
    bare = json.loads(dump_plan(plan, format="json"))
    assert isinstance(bare, list)
    assert [c.urn for c in plan_from_dict(bare)] == [urn]
    assert levels_from_plan_dict(bare) == {}


def test_blueprint_vars(session_ctx):
    blueprint = Blueprint(
        resources=[res.Role(name="role", comment=var.role_comment)],
        vars={"role_comment": "var role comment"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].data["comment"] == "var role comment"

    role = res.Role(name="role", comment="some comment {{ var.suffix }}")
    assert isinstance(role._data.comment, VarString)
    blueprint = Blueprint(
        resources=[role],
        vars={"suffix": "1234"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].data["comment"] == "some comment 1234"

    role = res.Role(name=var.role_name)
    assert isinstance(role.name, VarString)
    blueprint = Blueprint(
        resources=[role],
        vars={"role_name": "role123"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].data["name"] == "role123"

    role = res.Role(name="role_{{ var.suffix }}")
    assert isinstance(role.name, VarString)
    blueprint = Blueprint(
        resources=[role],
        vars={"suffix": "5678"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].data["name"] == "role_5678"


def test_blueprint_vars_spec(session_ctx):
    blueprint = Blueprint(
        resources=[res.Role(name="role", comment=var.role_comment)],
        vars_spec=[
            {
                "name": "role_comment",
                "type": "string",
                "default": "var role comment",
            }
        ],
    )
    assert blueprint._config.vars == {"role_comment": "var role comment"}
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].data["comment"] == "var role comment"

    with pytest.raises(MissingVarException):
        blueprint = Blueprint(
            resources=[res.Role(name="role", comment=var.role_comment)],
            vars_spec=[{"name": "role_comment", "type": "string"}],
        )

    blueprint = Blueprint(resources=[res.Role(name="role", comment=var.role_comment)])
    with pytest.raises(MissingVarException):
        blueprint.generate_manifest(session_ctx)


def test_blueprint_vars_in_owner(session_ctx):
    blueprint = Blueprint(
        resources=[res.Schema(name="schema", owner="role_{{ var.role_name }}", database="STATIC_DATABASE")],
        vars={"role_name": "role123"},
    )
    assert blueprint.generate_manifest(session_ctx)


def test_blueprint_sync_resources(session_ctx, remote_state):
    blueprint = Blueprint(
        resources=[res.Role(name="role1")],
        sync_resources=[ResourceType.ROLE],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1

    blueprint = Blueprint(sync_resources=["ROLE"])
    assert blueprint._config.sync_resources == [ResourceType.ROLE]
    # Note: sync_resources only affects remote state syncing, not resource validation during add
    # The following validations were expected but are not implemented:
    # - Adding a Database when sync_resources=[ROLE] should raise InvalidResourceException
    # - Creating a Blueprint with Role when sync_resources=[DATABASE] should raise InvalidResourceException


def test_merge_account_scoped_resources():
    resources = [
        res.Database(name="DB1"),
        ResourcePointer(name="DB1", resource_type=ResourceType.DATABASE),
    ]
    merged = _merge_pointers(resources)
    assert len(merged) == 1
    assert isinstance(merged[0], res.Database)
    assert merged[0].name == "DB1"

    resources = [
        res.Database(name="DB1"),
        res.Database(name="DB2"),
    ]
    merged = _merge_pointers(resources)
    assert len(merged) == 2


def test_merge_account_scoped_resources_fail():
    resources = [
        res.Database(name="DB1"),
        res.Database(name="DB1", comment="namespace conflict"),
    ]
    with pytest.raises(DuplicateResourceException):
        _merge_pointers(resources)


def test_blueprint_edition_checks(session_ctx, remote_state):
    session_ctx = deepcopy(session_ctx)
    session_ctx["account_edition"] = AccountEdition.STANDARD

    blueprint = Blueprint(resources=[res.Database(name="DB1"), res.Tag(name="TAG1")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    with pytest.raises(NonConformingPlanException):
        blueprint._raise_for_nonconforming_plan(session_ctx, plan)

    blueprint = Blueprint(resources=[res.Warehouse(name="WH", min_cluster_count=2)])
    with pytest.raises(WrongEditionException):
        blueprint.generate_manifest(session_ctx)

    blueprint = Blueprint(resources=[res.Warehouse(name="WH", min_cluster_count=1)])
    assert blueprint.generate_manifest(session_ctx)

    blueprint = Blueprint(resources=[res.Warehouse(name="WH")])
    assert blueprint.generate_manifest(session_ctx)


def test_blueprint_warehouse_scaling_policy_doesnt_render_in_standard_edition(session_ctx, remote_state):
    session_ctx = deepcopy(session_ctx)
    session_ctx["account_edition"] = AccountEdition.STANDARD
    wh = res.Warehouse(name="WH", warehouse_size="XSMALL")
    blueprint = Blueprint(resources=[wh])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    sql = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert len(sql) == 3
    assert sql[0] == "USE SECONDARY ROLES ALL"
    assert sql[1] == "USE ROLE SYSADMIN"
    assert sql[2].startswith("CREATE WAREHOUSE WH")
    assert "scaling_policy" not in sql[2]


def test_blueprint_warehouse_generation_and_resource_constraint_update(session_ctx):
    wh_urn = parse_URN("urn::ABCD123:warehouse/WH")
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        wh_urn: res.Warehouse(
            name="WH",
            generation="1",
            resource_constraint="STANDARD_GEN_1",
        ).to_dict(),
    }
    blueprint = Blueprint(
        resources=[
            res.Warehouse(
                name="WH",
                generation="2",
                resource_constraint="STANDARD_GEN_2",
            )
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)

    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    wh_change = plan[0]
    assert wh_change.delta == {
        "generation": "2",
        "resource_constraint": "STANDARD_GEN_2",
    }

    sql = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert "ALTER WAREHOUSE WH SET GENERATION = '2' RESOURCE_CONSTRAINT = STANDARD_GEN_2" in sql


def _warehouse_remote_state(**show_row_overrides):
    """Build remote_state for warehouse "WH" through the real fetch_warehouse path, mocking
    only the underlying SHOW WAREHOUSES call -- same pattern as TestFetchWarehouse in
    tests/test_data_provider.py -- so the dict shape matches what production fetch actually
    returns (e.g. it omits keys like initially_suspended that Warehouse.to_dict() includes).
    """
    show_row = {
        "name": "WH",
        "owner": "SYSADMIN",
        "owner_role_type": "ROLE",
        "type": "STANDARD",
        "size": "X-SMALL",
        "auto_suspend": 600,
        "auto_resume": "true",
        "comment": "",
        "resource_monitor": "null",
    }
    show_row.update(show_row_overrides)
    with patch("snowcap.data_provider._show_resources", return_value=[show_row]):
        return fetch_warehouse(MagicMock(), FQN(name=ResourceName("WH")), include_params=False)


def test_blueprint_standard_to_adaptive_warehouse_conversion(session_ctx):
    # Snowflake documents ALTER WAREHOUSE ... SET WAREHOUSE_TYPE = 'ADAPTIVE' as an online
    # conversion that auto-computes adaptive settings server-side, so converting a STANDARD
    # warehouse to ADAPTIVE must produce a delta of only warehouse_type (blueprint skips
    # manifest-None fields) and no UNSETs for the fields that no longer apply.
    wh_urn = parse_URN("urn::ABCD123:warehouse/WH")
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        wh_urn: _warehouse_remote_state(),
    }
    blueprint = Blueprint(resources=[res.Warehouse(name="WH", warehouse_type="ADAPTIVE")])
    manifest = blueprint.generate_manifest(session_ctx)

    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    wh_change = plan[0]
    assert wh_change.delta == {"warehouse_type": "ADAPTIVE"}

    sql = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    # warehouse_type's EnumProp label renders verbatim (lowercase), unlike GENERATION/
    # RESOURCE_CONSTRAINT above which were declared with uppercase labels.
    assert "ALTER WAREHOUSE WH SET warehouse_type = 'ADAPTIVE'" in sql


def test_blueprint_adaptive_query_throughput_multiplier_no_drift_and_update(session_ctx):
    # A fetched ADAPTIVE warehouse reports query_throughput_multiplier (default 2). A manifest
    # declaring the same value must produce no plan; a different value must produce a
    # single-field delta and the matching ALTER statement.
    wh_urn = parse_URN("urn::ABCD123:warehouse/WH")
    remote = _warehouse_remote_state(
        type="ADAPTIVE",
        size="",
        max_query_performance_level="LARGE",
        query_throughput_multiplier=2,
    )
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        wh_urn: remote,
    }

    declared = dict(
        name="WH",
        warehouse_type="ADAPTIVE",
        max_query_performance_level="LARGE",
    )
    blueprint = Blueprint(resources=[res.Warehouse(**declared, query_throughput_multiplier=2)])
    manifest = blueprint.generate_manifest(session_ctx)
    assert diff(remote_state, manifest) == []

    blueprint = Blueprint(resources=[res.Warehouse(**declared, query_throughput_multiplier=4)])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].delta == {"query_throughput_multiplier": 4}

    sql = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert "ALTER WAREHOUSE WH SET QUERY_THROUGHPUT_MULTIPLIER = 4" in sql


def test_blueprint_adaptive_to_standard_warehouse_conversion(session_ctx):
    # Symmetric reverse direction: remote is a fetched ADAPTIVE warehouse (fetch_warehouse nulls
    # ADAPTIVE_UNSUPPORTED_FIELDS -- size/cluster/suspend-resume/scaling -- to None), and the
    # manifest declares a STANDARD warehouse whose dataclass defaults for those same fields are
    # non-None. _diff_resource_data only skips manifest-None fields, so the delta -- and the
    # emitted SQL -- combines warehouse_type with every STANDARD default that differs from the
    # nulled remote value, all in one ALTER ... SET statement. This assumes Snowflake's ALTER
    # WAREHOUSE SET grammar accepts multiple properties (including WAREHOUSE_TYPE) in a single
    # statement; live verification of this combined-statement form is a post-merge follow-up.
    wh_urn = parse_URN("urn::ABCD123:warehouse/WH")
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        wh_urn: _warehouse_remote_state(type="ADAPTIVE", size="", max_query_performance_level="LARGE"),
    }
    blueprint = Blueprint(resources=[res.Warehouse(name="WH")])
    manifest = blueprint.generate_manifest(session_ctx)

    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    wh_change = plan[0]
    assert wh_change.delta == {
        "warehouse_type": "STANDARD",
        "warehouse_size": "XSMALL",
        "auto_suspend": 600,
        "auto_resume": True,
        "max_cluster_count": 1,
        "min_cluster_count": 1,
        "scaling_policy": "STANDARD",
    }

    sql = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert (
        "ALTER WAREHOUSE WH SET warehouse_type = 'STANDARD' warehouse_size = XSMALL "
        "MAX_CLUSTER_COUNT = 1 MIN_CLUSTER_COUNT = 1 scaling_policy = STANDARD "
        "AUTO_SUSPEND = 600 AUTO_RESUME = TRUE"
    ) in sql


def test_blueprint_x5large_to_adaptive_warehouse_conversion_raises(session_ctx):
    # Snowflake does not support converting to or from an X5LARGE/X6LARGE warehouse
    # (https://docs.snowflake.com/en/user-guide/warehouses-adaptive), so the plan must
    # fail instead of emitting an ALTER that errors mid-apply.
    wh_urn = parse_URN("urn::ABCD123:warehouse/WH")
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        wh_urn: _warehouse_remote_state(size="5X-LARGE"),
    }
    blueprint = Blueprint(resources=[res.Warehouse(name="WH", warehouse_type="ADAPTIVE")])
    manifest = blueprint.generate_manifest(session_ctx)

    with pytest.raises(InvalidResourceException, match="X5LARGE or X6LARGE"):
        diff(remote_state, manifest)


def test_blueprint_adaptive_to_x6large_warehouse_conversion_raises(session_ctx):
    # Reverse direction of the same Snowflake restriction: an ADAPTIVE warehouse can't be
    # converted to an X5LARGE/X6LARGE standard warehouse.
    wh_urn = parse_URN("urn::ABCD123:warehouse/WH")
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        wh_urn: _warehouse_remote_state(type="ADAPTIVE", size="", max_query_performance_level="LARGE"),
    }
    blueprint = Blueprint(resources=[res.Warehouse(name="WH", warehouse_size="X6LARGE")])
    manifest = blueprint.generate_manifest(session_ctx)

    with pytest.raises(InvalidResourceException, match="X5LARGE or X6LARGE"):
        diff(remote_state, manifest)


def test_blueprint_key_properties_adaptive_warehouse_omits_size(session_ctx, remote_state):
    # Adaptive warehouses fetch/plan with warehouse_size=None; the CREATE preview must not
    # render "size: None" for them, while standard warehouses keep showing their size.
    blueprint = Blueprint(
        resources=[res.Warehouse(name="WH", warehouse_type="ADAPTIVE", max_query_performance_level="LARGE")]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert "size:" not in plan_str

    blueprint = Blueprint(resources=[res.Warehouse(name="WH", warehouse_size="LARGE")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert "size: LARGE" in plan_str


def test_blueprint_scope_config():

    bc = BlueprintConfig(
        scope=BlueprintScope.DATABASE,
        database=ResourceName("foo"),
    )
    assert bc

    with pytest.raises(ValueError):
        BlueprintConfig(
            scope=BlueprintScope.DATABASE,
            schema=ResourceName("bar"),
        )

    with pytest.raises(ValueError):
        BlueprintConfig(
            scope=BlueprintScope.ACCOUNT,
            database=ResourceName("foo"),
        )

    with pytest.raises(ValueError):
        BlueprintConfig(
            scope=BlueprintScope.ACCOUNT,
            schema=ResourceName("bar"),
        )

    with pytest.raises(ValueError):
        BlueprintConfig(
            scope=BlueprintScope.ACCOUNT,
            database=ResourceName("foo"),
            schema=ResourceName("bar"),
        )


def test_blueprint_scope(session_ctx, remote_state):

    blueprint = Blueprint(resources=[res.Database(name="DB1")], scope=BlueprintScope.DATABASE)
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1

    blueprint = Blueprint(resources=[res.Role(name="ROLE1")], scope=BlueprintScope.DATABASE)
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    with pytest.raises(NonConformingPlanException):
        blueprint._raise_for_nonconforming_plan(session_ctx, plan)

    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:database/DB1"): {"owner": "SYSADMIN"},
        parse_URN("urn::ABCD123:schema/DB1.PUBLIC"): {"owner": "SYSADMIN"},
    }

    blueprint = Blueprint(
        resources=[
            res.Schema(name="SCHEMA1"),
            res.Task(name="TASK1"),
        ],
        scope=BlueprintScope.DATABASE,
        database="DB1",
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 2

    blueprint = Blueprint(resources=[res.Database(name="DB2")], scope=BlueprintScope.SCHEMA)
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    with pytest.raises(NonConformingPlanException):
        blueprint._raise_for_nonconforming_plan(session_ctx, plan)


def test_blueprint_plan_scope_stubbing(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:database/DB1"): {"owner": "SYSADMIN"},
        parse_URN("urn::ABCD123:schema/DB1.PUBLIC"): {"owner": "SYSADMIN"},
    }

    blueprint = Blueprint(
        resources=[res.Task(name="TASK1")],
        scope=BlueprintScope.SCHEMA,
        database="DB1",
        schema="PUBLIC",
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1

    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:database/DB1"): {"owner": "SYSADMIN"},
        parse_URN("urn::ABCD123:schema/DB1.PUBLIC"): {"owner": "SYSADMIN"},
        parse_URN("urn::ABCD123:schema/DB1.ANOTHER_SCHEMA"): {"owner": "SYSADMIN"},
    }

    blueprint = Blueprint(
        resources=[res.Task(name="TASK1")],
        scope=BlueprintScope.SCHEMA,
        database="DB1",
        schema="ANOTHER_SCHEMA",
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1

    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:database/DB1"): {"owner": "SYSADMIN"},
        parse_URN("urn::ABCD123:schema/DB1.PUBLIC"): {"owner": "SYSADMIN"},
    }

    blueprint = Blueprint(
        resources=[res.Schema(name="A_THIRD_SCHEMA"), res.Task(name="TASK1")],
        scope=BlueprintScope.SCHEMA,
        database="DB1",
        schema="A_THIRD_SCHEMA",
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 2


def test_resource_type_needs_params(session_ctx):
    """Test that resource_type_needs_params correctly identifies when param fetching is needed.

    Parameter fields (like max_data_extension_time_in_days) now default to None,
    meaning "inherit from parent". The optimization skips SHOW PARAMETERS when
    no resource explicitly sets these fields.
    """
    from snowcap.blueprint import (
        Blueprint,
        resource_type_needs_params,
        schema_urn_needs_params,
        databases_with_param_fields,
    )
    from snowcap.identifiers import FQN, ResourceName

    # Schema type-level check always returns True (delegates to per-URN check)
    blueprint = Blueprint(
        resources=[
            res.Schema(name="MY_SCHEMA", database="MY_DB", owner="SYSADMIN"),
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert resource_type_needs_params(ResourceType.SCHEMA, manifest) is True  # Delegates to per-URN

    # Test per-URN schema check: schema without explicit param fields - should NOT need params
    db_with_params = databases_with_param_fields(manifest)
    schema_urn = URN(
        resource_type=ResourceType.SCHEMA,
        fqn=FQN(ResourceName("MY_SCHEMA"), database=ResourceName("MY_DB")),
        account_locator=session_ctx["account_locator"],
    )
    assert schema_urn_needs_params(schema_urn, manifest, db_with_params) is False

    # Schema with explicit default_ddl_collation - per-URN check SHOULD need params
    blueprint = Blueprint(
        resources=[
            res.Schema(
                name="MY_SCHEMA",
                database="MY_DB",
                owner="SYSADMIN",
                default_ddl_collation="en_US",
            ),
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    db_with_params = databases_with_param_fields(manifest)
    assert schema_urn_needs_params(schema_urn, manifest, db_with_params) is True

    # Schema with explicit max_data_extension_time_in_days - per-URN check SHOULD need params
    blueprint = Blueprint(
        resources=[
            res.Schema(
                name="MY_SCHEMA",
                database="MY_DB",
                owner="SYSADMIN",
                max_data_extension_time_in_days=28,
            ),
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    db_with_params = databases_with_param_fields(manifest)
    assert schema_urn_needs_params(schema_urn, manifest, db_with_params) is True

    # Database without explicit parameter fields - should NOT need params
    blueprint = Blueprint(
        resources=[
            res.Database(name="MY_DB", owner="SYSADMIN"),
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert resource_type_needs_params(ResourceType.DATABASE, manifest) is False

    # Database with explicit max_data_extension_time_in_days - SHOULD need params
    blueprint = Blueprint(
        resources=[
            res.Database(name="MY_DB", owner="SYSADMIN", max_data_extension_time_in_days=7),
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert resource_type_needs_params(ResourceType.DATABASE, manifest) is True

    # Database with params + schema without params - SCHEMA type-level check returns True
    # (delegates to per-URN check via schema_urn_needs_params)
    blueprint = Blueprint(
        resources=[
            res.Database(name="MY_DB", owner="SYSADMIN", max_data_extension_time_in_days=7),
            res.Schema(name="MY_SCHEMA", database="MY_DB", owner="SYSADMIN"),
        ]
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert resource_type_needs_params(ResourceType.DATABASE, manifest) is True
    assert resource_type_needs_params(ResourceType.SCHEMA, manifest) is True  # Delegates to per-URN

    # Per-URN check: PUBLIC schema needs params when database has params (inheritance)
    db_with_params = databases_with_param_fields(manifest)
    assert "MY_DB" in db_with_params
    public_schema_urn = URN(
        resource_type=ResourceType.SCHEMA,
        fqn=FQN(ResourceName("PUBLIC"), database=ResourceName("MY_DB")),
        account_locator=session_ctx["account_locator"],
    )
    assert schema_urn_needs_params(public_schema_urn, manifest, db_with_params) is True

    # Per-URN check: Non-PUBLIC schema without params does NOT need params
    other_schema_urn = URN(
        resource_type=ResourceType.SCHEMA,
        fqn=FQN(ResourceName("MY_SCHEMA"), database=ResourceName("MY_DB")),
        account_locator=session_ctx["account_locator"],
    )
    assert schema_urn_needs_params(other_schema_urn, manifest, db_with_params) is False

    # Empty manifest - SCHEMA type-level check still returns True (delegates to per-URN)
    blueprint = Blueprint(resources=[])
    manifest = blueprint.generate_manifest(session_ctx)
    assert resource_type_needs_params(ResourceType.SCHEMA, manifest) is True  # Delegates to per-URN

    # Roles have no PARAMETER_FIELDS entry - should always return True (no optimization)
    blueprint = Blueprint(
        resources=[res.Role(name="MY_ROLE")],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert resource_type_needs_params(ResourceType.ROLE, manifest) is True


class TestWarningForNonconformingPlanMCPServer:
    """
    Tests for the MCP server grant-drop warning surfaced by
    Blueprint._warning_for_nonconforming_plan.

    Snowflake has no ALTER MCP SERVER command, so a specification change is applied
    via CREATE OR REPLACE, which drops all grants on the server. This warning tells
    the operator that up front, at plan time, rather than leaving them to discover
    the dropped grants after apply.
    """

    def _mcp_server_urn(self, session_ctx):
        return URN(
            resource_type=ResourceType.MCP_SERVER,
            fqn=FQN(ResourceName("MY_SERVER"), database=ResourceName("MY_DB"), schema=ResourceName("MY_SCHEMA")),
            account_locator=session_ctx["account_locator"],
        )

    def test_specification_update_warns_about_dropped_grants(self, session_ctx, caplog):
        urn = self._mcp_server_urn(session_ctx)
        change = UpdateResource(
            urn=urn,
            resource_cls=res.MCPServer,
            before={"specification": "old"},
            after={"specification": "new"},
            delta={"specification": "new"},
        )
        blueprint = Blueprint(resources=[])

        with caplog.at_level(logging.WARNING, logger="snowcap"):
            blueprint._warning_for_nonconforming_plan(session_ctx, [change])

        assert str(urn) in caplog.text
        assert "grant" in caplog.text.lower()

    def test_update_without_specification_in_delta_produces_no_warning(self, session_ctx, caplog):
        urn = self._mcp_server_urn(session_ctx)
        change = UpdateResource(
            urn=urn,
            resource_cls=res.MCPServer,
            before={"owner": "SYSADMIN"},
            after={"owner": "OTHER_ROLE"},
            delta={"owner": "OTHER_ROLE"},
        )
        blueprint = Blueprint(resources=[])

        with caplog.at_level(logging.WARNING, logger="snowcap"):
            blueprint._warning_for_nonconforming_plan(session_ctx, [change])

        assert caplog.text == ""

    def test_create_produces_no_warning(self, session_ctx, caplog):
        urn = self._mcp_server_urn(session_ctx)
        change = CreateResource(
            urn=urn,
            resource_cls=res.MCPServer,
            container=None,
            after={"specification": "new"},
        )
        blueprint = Blueprint(resources=[])

        with caplog.at_level(logging.WARNING, logger="snowcap"):
            blueprint._warning_for_nonconforming_plan(session_ctx, [change])

        assert caplog.text == ""

    def test_apply_with_prebuilt_plan_warns_about_dropped_grants(self, monkeypatch, session_ctx, caplog):
        """The two-step CLI workflow (`snowcap plan -o plan.json` then `snowcap apply
        --plan plan.json`) hands apply() an already-built plan, so plan() never re-runs.
        apply() must surface the warning itself in that case."""
        urn = self._mcp_server_urn(session_ctx)
        change = UpdateResource(
            urn=urn,
            resource_cls=res.MCPServer,
            before={"specification": "old"},
            after={"specification": "new"},
            delta={"specification": "new"},
        )
        blueprint = Blueprint(resources=[])

        monkeypatch.setattr("snowcap.blueprint.data_provider.fetch_session", lambda session: session_ctx)
        monkeypatch.setattr(
            "snowcap.blueprint.compile_plan_to_sql",
            lambda session_ctx, plan, shared_databases=None, database_owners=None: ([], []),
        )

        with caplog.at_level(logging.WARNING, logger="snowcap"):
            blueprint.apply(session=None, plan=[change])

        assert str(urn) in caplog.text
        assert "grant" in caplog.text.lower()


class TestMCPServerSpecChangeRegrantsManagedGrants:
    """
    CREATE OR REPLACE MCP SERVER drops all grants on the server (Snowflake has no
    ALTER MCP SERVER, and COPY GRANTS is not supported for this object type). To keep
    snowcap-managed grants from silently disappearing until a later run, diff() must
    re-create every manifest grant targeting a spec-changed MCP server in the same
    plan — grants execute after their target, so they are restored in the same apply.
    """

    @staticmethod
    def _diff_for(session_ctx, remote_spec: str, grant_in_remote: bool = True):
        db = res.Database(name="DB")
        schema = res.Schema(name="SCHEMA", database=db)
        server = res.MCPServer(
            name="SERVER",
            specification="tools:\n- name: query-data\n  type: SYSTEM_EXECUTE_SQL\n",
            schema=schema,
        )
        role = res.Role(name="SOME_ROLE")
        grant = res.Grant(priv="USAGE", on_mcp_server="DB.SCHEMA.SERVER", to=role)
        blueprint = Blueprint(name="blueprint", resources=[db, schema, server, role, grant])
        manifest = blueprint.generate_manifest(session_ctx)

        server_urn = next(u for u in manifest.urns if u.resource_type == ResourceType.MCP_SERVER)
        grant_urn = next(u for u in manifest.urns if u.resource_type == ResourceType.GRANT)

        remote_state = {parse_URN("urn::ABCD123:account/ACCOUNT"): {}}
        for urn in manifest.urns:
            item = manifest[urn]
            if isinstance(item, ResourcePointer):
                continue
            remote_state[urn] = dict(item.data)
        remote_state[server_urn] = {**remote_state[server_urn], "specification": remote_spec}
        if not grant_in_remote:
            del remote_state[grant_urn]

        return diff(remote_state, manifest), server_urn, grant_urn

    def test_spec_change_recreates_managed_grants(self, session_ctx):
        from snowcap.resources.mcp_server import normalize_mcp_specification

        plan, server_urn, grant_urn = self._diff_for(session_ctx, remote_spec=normalize_mcp_specification("tools: []"))

        server_change = find_change_by_urn(plan, server_urn)
        assert isinstance(server_change, UpdateResource)
        assert "specification" in server_change.delta

        grant_change = find_change_by_urn(plan, grant_urn)
        assert isinstance(grant_change, CreateResource)
        assert len(plan) == 2

    def test_unchanged_spec_recreates_nothing(self, session_ctx):
        from snowcap.resources.mcp_server import normalize_mcp_specification

        plan, _, _ = self._diff_for(
            session_ctx,
            remote_spec=normalize_mcp_specification("tools:\n- name: query-data\n  type: SYSTEM_EXECUTE_SQL\n"),
        )
        assert plan == []

    def test_grant_already_in_plan_is_not_duplicated(self, session_ctx):
        from snowcap.resources.mcp_server import normalize_mcp_specification

        plan, _, grant_urn = self._diff_for(
            session_ctx,
            remote_spec=normalize_mcp_specification("tools: []"),
            grant_in_remote=False,
        )
        grant_changes = [change for change in plan if change.urn == grant_urn]
        assert len(grant_changes) == 1


def test_blueprint_shared_database_create_default_owner(session_ctx, remote_state):
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    blueprint = Blueprint(name="blueprint", resources=[shared_db])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].resource_cls == res.SharedDatabase

    commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert "USE ROLE ACCOUNTADMIN" in commands
    assert any(c.startswith("CREATE DATABASE") and "FROM SHARE" in c for c in commands)
    assert not any(c.startswith("GRANT OWNERSHIP") for c in commands)


def test_blueprint_shared_database_custom_owner_rejected_at_validation_time(session_ctx, remote_state):
    # Imported (FROM SHARE) databases are consumer-read-only and Snowflake prevents
    # GRANT OWNERSHIP on them, so a custom owner must fail at validation time with a
    # clear message -- never reach apply as a GRANT OWNERSHIP statement Snowflake rejects.
    with pytest.raises(ValueError, match="does not support a custom owner"):
        res.SharedDatabase(name="GONG", from_share="provider_account.share_name", owner="SYSADMIN")


def test_blueprint_shared_database_remote_owner_never_drifts(session_ctx, remote_state):
    # owner is non-fetchable on SharedDatabase: even if remote state carried a different
    # owner, the plan must not emit a TransferOwnership (GRANT OWNERSHIP fails on an
    # imported database).
    remote_state[parse_URN("urn::ABCD123:database/GONG")] = {
        "name": "GONG",
        "from_share": "PROVIDER_ACCOUNT.SHARE_NAME",
        "owner": "SOME_OTHER_ROLE",
    }
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    blueprint = Blueprint(name="blueprint", resources=[shared_db])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)

    assert plan == []


def test_blueprint_database_create_custom_owner_transfers_public_schema(session_ctx, remote_state):
    # Regression: a regular Database with a non-default owner still transfers the PUBLIC schema.
    db = res.Database(name="DB", owner="USERADMIN")
    blueprint = Blueprint(name="blueprint", resources=[db])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)

    commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert "GRANT OWNERSHIP ON DATABASE DB TO ROLE USERADMIN COPY CURRENT GRANTS" in commands
    assert "GRANT OWNERSHIP ON SCHEMA DB.PUBLIC TO ROLE USERADMIN COPY CURRENT GRANTS" in commands


def test_blueprint_shared_database_from_share_change_raises_not_implemented(session_ctx, remote_state):
    # Documents today's limitation: from_share triggers_replacement, and snowcap never
    # replaces resources, so a from_share drift is a plan-time error rather than a silent
    # (and nonsensical) ALTER at apply time.
    remote_state[parse_URN("urn::ABCD123:database/GONG")] = {
        "name": "GONG",
        "from_share": "provider_account.old_share",
        "owner": "ACCOUNTADMIN",
    }
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.new_share")
    blueprint = Blueprint(name="blueprint", resources=[shared_db])
    manifest = blueprint.generate_manifest(session_ctx)

    with pytest.raises(MarkedForReplacementException, match="from_share"):
        diff(remote_state, manifest)


def test_blueprint_shared_database_idempotent_round_trip(session_ctx, remote_state):
    # Remote state mirrors what fetch_shared_database returns: Snowflake normalizes
    # unquoted identifiers to uppercase, matching the manifest's own normalization.
    remote_state[parse_URN("urn::ABCD123:database/GONG")] = {
        "name": "GONG",
        "from_share": "PROVIDER_ACCOUNT.SHARE_NAME",
        "owner": "ACCOUNTADMIN",
    }
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    blueprint = Blueprint(name="blueprint", resources=[shared_db])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)

    assert plan == []


def _fetch_remote_state_with_mocked_fetch(blueprint, manifest, session_ctx, monkeypatch, fetched_data):
    """Drive Blueprint.fetch_remote_state with data_provider entirely mocked out.

    DATABASE-typed URNs get `fetched_data`; ROLE references (e.g. the database's owner) are
    reported as existing so the post-fetch reference check passes; anything else (e.g. the
    implicit ACCOUNT resource) is reported as not-found and simply skipped.
    """
    monkeypatch.setattr(data_provider, "fetch_session", lambda session: session_ctx)
    monkeypatch.setattr(data_provider, "use_secondary_roles", lambda session, all=False: None)

    def _fetch_resource(session, urn, include_params=True, existence_only=False):
        if urn.resource_type == ResourceType.DATABASE:
            return fetched_data
        if urn.resource_type == ResourceType.ROLE:
            return {"name": str(urn.fqn.name)}
        return None

    monkeypatch.setattr(data_provider, "fetch_resource", _fetch_resource)
    return blueprint.fetch_remote_state(session=None, manifest=manifest)


def test_fetch_remote_state_declared_database_but_remote_is_shared_raises_clear_error(session_ctx, monkeypatch):
    # Regression for FINDING 1: a pre-existing config declares a plain Database, but Snowflake
    # reports the same URN as an imported (shared) database. This must raise a guided domain
    # error, not a raw TypeError from Database(**data) choking on the unexpected 'from_share' key.
    db = res.Database(name="GONG", owner="SYSADMIN")
    blueprint = Blueprint(name="blueprint", resources=[db])
    manifest = blueprint.generate_manifest(session_ctx)

    fetched_data = {"name": "GONG", "from_share": "provider_account.share_name", "owner": "ACCOUNTADMIN"}
    with pytest.raises(
        InvalidResourceException, match="declared as Database but Snowflake reports it as SharedDatabase"
    ):
        _fetch_remote_state_with_mocked_fetch(blueprint, manifest, session_ctx, monkeypatch, fetched_data)


def test_fetch_remote_state_declared_shared_database_but_remote_is_standard_raises_clear_error(
    session_ctx, monkeypatch
):
    # Reverse of the above: declared as SharedDatabase, but Snowflake reports a STANDARD database.
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    blueprint = Blueprint(name="blueprint", resources=[shared_db])
    manifest = blueprint.generate_manifest(session_ctx)

    fetched_data = {"name": "GONG", "owner": "SYSADMIN"}
    with pytest.raises(
        InvalidResourceException, match="declared as SharedDatabase but Snowflake reports it as Database"
    ):
        _fetch_remote_state_with_mocked_fetch(blueprint, manifest, session_ctx, monkeypatch, fetched_data)


def test_fetch_remote_state_matching_declared_and_fetched_kind_does_not_raise(session_ctx, monkeypatch):
    # Regression: when the declared and fetched classes agree, fetch_remote_state proceeds normally.
    db = res.Database(name="GONG", owner="SYSADMIN")
    blueprint = Blueprint(name="blueprint", resources=[db])
    manifest = blueprint.generate_manifest(session_ctx)

    fetched_data = {"name": "GONG", "owner": "SYSADMIN"}
    state = _fetch_remote_state_with_mocked_fetch(blueprint, manifest, session_ctx, monkeypatch, fetched_data)
    assert len(state) == 1


def test_shared_database_sole_db_scoped_resource_raises_clear_error(session_ctx):
    # Regression for FINDING 2 (site: databases[0].add(resource) for parentless db-scoped
    # resources): a SharedDatabase is not a ResourceContainer, so a parentless Schema (no
    # database: set) must raise a guided OrphanResourceException instead of AttributeError.
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    schema = res.Schema(name="MY_SCHEMA")
    blueprint = Blueprint(name="blueprint", resources=[shared_db, schema])

    with pytest.raises(OrphanResourceException, match="Cannot add SCHEMA 'MY_SCHEMA' to SharedDatabase 'GONG'"):
        blueprint.generate_manifest(session_ctx)


def test_shared_database_sole_public_schema_target_raises_clear_error(session_ctx):
    # Regression for FINDING 2 (site: _get_public_schema(databases[0]) for parentless
    # schema-scoped resources): a Table with no schema:/database: set falls back to
    # "<sole database>.PUBLIC", which doesn't exist on a read-only SharedDatabase.
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    table = res.Table(name="MY_TABLE", columns=[{"name": "ID", "data_type": "INT"}])
    blueprint = Blueprint(name="blueprint", resources=[shared_db, table])

    with pytest.raises(OrphanResourceException, match="Cannot add TABLE 'MY_TABLE' to SharedDatabase 'GONG'"):
        blueprint.generate_manifest(session_ctx)


def test_shared_database_schema_pointer_without_database_raises_clear_error(session_ctx):
    # Regression for FINDING 2 (site: databases[0].add(schema_pointer) for a schema-scoped
    # resource that names its schema but not its database): the implied schema pointer would
    # be attached to the sole database, which cannot hold it if that database is shared.
    shared_db = res.SharedDatabase(name="GONG", from_share="provider_account.share_name")
    table = res.Table(name="MY_TABLE", schema="SOME_SCHEMA", columns=[{"name": "ID", "data_type": "INT"}])
    blueprint = Blueprint(name="blueprint", resources=[shared_db, table])

    with pytest.raises(OrphanResourceException, match="Cannot add SCHEMA 'SOME_SCHEMA' to SharedDatabase 'GONG'"):
        blueprint.generate_manifest(session_ctx)


def test_schema_under_shared_database_raises_clear_error(session_ctx):
    # Regression for FINDING 3: Schema(database='gong') registers a ResourcePointer that
    # _merge_pointers later merges into the resolved SharedDatabase resource. Since
    # SharedDatabase is not a ResourceContainer, this is a different code path from FINDING 2
    # (explicitly-parented vs. parentless) and must independently raise a guided error instead
    # of a bare AttributeError deep in merge internals.
    shared_db = res.SharedDatabase(name="gong", from_share="provider_account.share_name")
    schema = res.Schema(name="foo", database="gong")
    blueprint = Blueprint(name="blueprint", resources=[shared_db, schema])

    with pytest.raises(OrphanResourceException, match="Cannot add SCHEMA '.*' to SharedDatabase 'GONG'"):
        blueprint.generate_manifest(session_ctx)


class TestFutureGrantPrecedenceWarnings:
    """
    Tests for the database-level future grant warning surfaced by
    Blueprint._warning_for_nonconforming_plan.

    Snowflake gives schema-level future grants precedence over database-level future
    grants on the same object type, and silently ignores the database-level grant for
    that schema. Managed access schemas make the conflict easy to introduce from a
    separate config, so the check calls both situations out at plan time.
    """

    def _database_future_grants(self, database="MY_DB", to="READER", priv="SELECT"):
        return [
            res.Grant(priv=priv, on=f"future tables in database {database}", to=to),
            res.Grant(priv=priv, on=f"future views in database {database}", to=to),
        ]

    def _warnings_for(self, session_ctx, resources):
        blueprint = Blueprint(resources=resources)
        manifest = blueprint.generate_manifest(session_ctx)
        return future_grant_precedence_warnings(manifest_state_entries(manifest))

    def test_managed_access_schema_with_database_future_grants_warns(self, session_ctx):
        resources = [
            res.Database(name="MY_DB"),
            res.Schema(name="MY_SCHEMA", database="MY_DB", managed_access=True),
            res.Role(name="READER"),
            *self._database_future_grants(),
        ]

        warnings = self._warnings_for(session_ctx, resources)

        assert len(warnings) == 1
        assert "MY_DB.MY_SCHEMA" in warnings[0]
        assert "managed access" in warnings[0]
        assert "TABLES" in warnings[0] and "VIEWS" in warnings[0]

    def test_schema_without_managed_access_produces_no_warning(self, session_ctx):
        resources = [
            res.Database(name="MY_DB"),
            res.Schema(name="MY_SCHEMA", database="MY_DB"),
            res.Role(name="READER"),
            *self._database_future_grants(),
        ]

        assert self._warnings_for(session_ctx, resources) == []

    def test_shadowing_schema_future_grant_warns_that_database_grant_is_ignored(self, session_ctx):
        resources = [
            res.Database(name="MY_DB"),
            res.Schema(name="MY_SCHEMA", database="MY_DB", managed_access=True),
            res.Role(name="READER"),
            res.Role(name="WRITER"),
            res.Grant(priv="SELECT", on="future tables in database MY_DB", to="READER"),
            # A future grant on the same object type at the schema level, even to a
            # different role, makes Snowflake ignore the database-level grant.
            res.Grant(priv="INSERT", on="future tables in schema MY_DB.MY_SCHEMA", to="WRITER"),
        ]

        warnings = self._warnings_for(session_ctx, resources)

        assert len(warnings) == 1
        assert "is ignored for MY_DB.MY_SCHEMA" in warnings[0]
        assert "SELECT ON FUTURE TABLES IN DATABASE MY_DB to READER" in warnings[0]
        assert "managed access" in warnings[0]

    def test_shadowing_is_scoped_to_the_same_object_type(self, session_ctx):
        resources = [
            res.Database(name="MY_DB"),
            res.Schema(name="MY_SCHEMA", database="MY_DB"),
            res.Role(name="READER"),
            res.Grant(priv="SELECT", on="future tables in database MY_DB", to="READER"),
            res.Grant(priv="SELECT", on="future views in schema MY_DB.MY_SCHEMA", to="READER"),
        ]

        assert self._warnings_for(session_ctx, resources) == []

    def test_shadowing_is_scoped_to_the_same_database(self, session_ctx):
        resources = [
            res.Database(name="MY_DB"),
            res.Database(name="OTHER_DB"),
            res.Schema(name="MY_SCHEMA", database="OTHER_DB"),
            res.Role(name="READER"),
            res.Grant(priv="SELECT", on="future tables in database MY_DB", to="READER"),
            res.Grant(priv="SELECT", on="future tables in schema OTHER_DB.MY_SCHEMA", to="READER"),
        ]

        assert self._warnings_for(session_ctx, resources) == []

    def test_schema_level_future_grants_alone_produce_no_warning(self, session_ctx):
        """The fix for the database-level trap: declare the grants at the schema level."""
        resources = [
            res.Database(name="MY_DB"),
            res.Schema(name="MY_SCHEMA", database="MY_DB", managed_access=True),
            res.Role(name="READER"),
            res.Grant(priv="SELECT", on="future tables in schema MY_DB.MY_SCHEMA", to="READER"),
            res.Grant(priv="SELECT", on="all tables in schema MY_DB.MY_SCHEMA", to="READER"),
        ]

        assert self._warnings_for(session_ctx, resources) == []

    def test_managed_access_from_remote_state_is_detected(self, session_ctx):
        """The schema is already managed access in Snowflake and unchanged in this run, so
        only remote state knows about it."""
        blueprint = Blueprint(
            resources=[
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                *self._database_future_grants(),
            ]
        )
        manifest = blueprint.generate_manifest(session_ctx)
        schema_urn = URN(
            resource_type=ResourceType.SCHEMA,
            fqn=FQN(ResourceName("REMOTE_SCHEMA"), database=ResourceName("MY_DB")),
            account_locator=session_ctx["account_locator"],
        )
        remote_state = {schema_urn: {"name": "REMOTE_SCHEMA", "managed_access": True}}

        warnings = future_grant_precedence_warnings(manifest_state_entries(manifest, remote_state))

        assert len(warnings) == 1
        assert "MY_DB.REMOTE_SCHEMA" in warnings[0]

    def test_warning_is_surfaced_by_the_plan_warning_hook(self, session_ctx, caplog):
        blueprint = Blueprint(
            resources=[
                res.Database(name="MY_DB"),
                res.Schema(name="MY_SCHEMA", database="MY_DB", managed_access=True),
                res.Role(name="READER"),
                *self._database_future_grants(),
            ]
        )
        manifest = blueprint.generate_manifest(session_ctx)

        with caplog.at_level(logging.WARNING, logger="snowcap"):
            blueprint._warning_for_nonconforming_plan(session_ctx, [], manifest)

        assert "managed access" in caplog.text
        assert "MY_DB.MY_SCHEMA" in caplog.text

    def test_prebuilt_plan_falls_back_to_plan_contents(self, session_ctx):
        """`snowcap apply --plan plan.json` never rebuilds the manifest, so the check runs
        over the changes in the plan instead."""
        blueprint = Blueprint(
            resources=[
                res.Database(name="MY_DB"),
                res.Schema(name="MY_SCHEMA", database="MY_DB", managed_access=True),
                res.Role(name="READER"),
                *self._database_future_grants(),
            ]
        )
        manifest = blueprint.generate_manifest(session_ctx)
        plan = [
            CreateResource(urn, item.resource_cls, None, item.data)
            for urn, item in manifest.items()
            if hasattr(item, "data")
        ]

        warnings = future_grant_precedence_warnings(plan_entries(plan))

        assert len(warnings) == 1
        assert "MY_DB.MY_SCHEMA" in warnings[0]


class TestInheritedGrantPlanning:
    """
    Tests for planning inherited grants: the account-level feature gate, how a container
    grant covers the per-object grants it produced, and which role issues it.
    """

    def _object_grant_state(self, priv="SELECT", to="SOMEROLE", on="DB.SCH.TBL"):
        urn = parse_URN(f"urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv={priv}&on=table/{on}&to=role/{to}")
        return urn, {
            "priv": priv,
            "on": on,
            "on_type": "TABLE",
            "to": to,
            "items_type": None,
            "to_type": "ROLE",
            "grant_option": False,
            "grant_type": "OBJECT",
            "owner": "SYSADMIN",
            "_privs": [priv],
        }

    def _manifest(self, session_ctx, resources):
        return Blueprint(resources=resources).generate_manifest(session_ctx)

    def test_plan_fails_when_the_account_has_not_enabled_the_feature(self, session_ctx):
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER"),
            ],
        )

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled", return_value=False):
            with pytest.raises(MissingPrivilegeException, match="FEATURE_RBAC_INHERITED_GRANTS"):
                raise_if_inherited_grants_unavailable(MagicMock(), manifest)

    def test_plan_proceeds_when_the_feature_flag_cannot_be_read(self, session_ctx):
        """Reading account parameters needs privileges the session may not hold; that must
        not block an apply that would otherwise succeed."""
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER"),
            ],
        )

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled", return_value=None):
            raise_if_inherited_grants_unavailable(MagicMock(), manifest)

    def test_the_feature_flag_is_not_probed_without_inherited_grants(self, session_ctx):
        manifest = self._manifest(session_ctx, [res.Database(name="MY_DB")])

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled") as probe:
            raise_if_inherited_grants_unavailable(MagicMock(), manifest)

        probe.assert_not_called()

    def test_inherited_grant_covers_remote_per_object_grants(self, session_ctx, remote_state):
        """Migrating per-object grants to an inherited grant must not revoke the access the
        inherited grant provides."""
        remote_state = remote_state.copy()
        urn, data = self._object_grant_state()
        remote_state[urn] = data
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="DB"),
                res.Role(name="SOMEROLE"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE DB", to="SOMEROLE"),
            ],
        )

        plan = diff(remote_state, manifest)

        assert not [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_uncovered_object_grants_are_still_dropped(self, session_ctx, remote_state):
        """Coverage is per privilege, grantee, object type, and container -- a collection
        grant elsewhere in the config does not protect an unrelated grant."""
        remote_state = remote_state.copy()
        urn, data = self._object_grant_state(priv="INSERT")
        remote_state[urn] = data
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="DB"),
                res.Role(name="SOMEROLE"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE DB", to="SOMEROLE"),
            ],
        )

        plan = diff(remote_state, manifest)

        assert [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_grant_on_all_covers_objects_in_its_database(self, session_ctx, remote_state):
        remote_state = remote_state.copy()
        urn, data = self._object_grant_state()
        remote_state[urn] = data
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="DB"),
                res.Role(name="SOMEROLE"),
                res.Grant(priv="SELECT", on="ALL TABLES IN DATABASE DB", to="SOMEROLE"),
            ],
        )

        plan = diff(remote_state, manifest)

        assert not [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_grant_all_collection_covers_expanded_privilege_rows(self, session_ctx, remote_state):
        """`GRANT ALL ON ALL TABLES` fans out into concrete-privilege rows (SELECT, INSERT, ...).
        A declared ALL collection grant must cover them, or sync drops each one every run."""
        remote_state = remote_state.copy()
        urn, data = self._object_grant_state(priv="SELECT")
        remote_state[urn] = data
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="DB"),
                res.Role(name="SOMEROLE"),
                res.Grant(priv="ALL", on="ALL TABLES IN DATABASE DB", to="SOMEROLE"),
            ],
        )

        plan = diff(remote_state, manifest)

        assert not [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_container_covers_handles_quoted_identifiers_with_dots(self):
        """A quoted identifier can contain a literal dot; a plain split miscounts the parts and
        mis-classifies containment."""
        from snowcap.blueprint import _container_covers
        from snowcap.enums import ResourceType

        assert _container_covers(ResourceType.SCHEMA.value, 'DB."a.b"', 'DB."a.b".TBL')
        assert not _container_covers(ResourceType.SCHEMA.value, 'DB."a.b"', "DB.OTHER.TBL")
        assert _container_covers(ResourceType.DATABASE.value, "DB", 'DB."a.b".TBL')

    def test_a_collection_grant_in_another_database_does_not_protect_the_grant(self, session_ctx, remote_state):
        remote_state = remote_state.copy()
        urn, data = self._object_grant_state()
        remote_state[urn] = data
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="OTHER_DB"),
                res.Role(name="SOMEROLE"),
                res.Grant(priv="SELECT", on="ALL TABLES IN DATABASE OTHER_DB", to="SOMEROLE"),
            ],
        )

        plan = diff(remote_state, manifest)

        assert [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_inherited_grant_runs_as_its_declared_container_admin(self, session_ctx):
        """Container-level MANAGE GRANTS is how a database admin manages access without
        account-wide authority, so a declared owner is used in preference to SECURITYADMIN."""
        grant = res.Grant(
            priv="SELECT",
            on="INHERITED TABLES IN DATABASE SALES_DB",
            to="ANALYST",
            owner="SALES_DB_ADMIN",
        )
        change = CreateResource(
            urn=parse_URN(
                "urn::ABCD123:grant/GRANT?grant_type=INHERITED&priv=SELECT&on=database/SALES_DB.<TABLE>&to=role/ANALYST"
            ),
            resource_cls=res.Grant,
            container=None,
            after=grant.to_dict(),
        )

        role, _ = execution_strategy_for_change(
            change, ["SYSADMIN", "SECURITYADMIN", "SALES_DB_ADMIN"], ResourceName("SYSADMIN")
        )

        assert role == ResourceName("SALES_DB_ADMIN")

    def test_inherited_grant_falls_back_to_securityadmin(self, session_ctx):
        grant = res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE SALES_DB", to="ANALYST")
        change = CreateResource(
            urn=parse_URN(
                "urn::ABCD123:grant/GRANT?grant_type=INHERITED&priv=SELECT&on=database/SALES_DB.<TABLE>&to=role/ANALYST"
            ),
            resource_cls=res.Grant,
            container=None,
            after=grant.to_dict(),
        )

        role, _ = execution_strategy_for_change(change, ["SYSADMIN", "SECURITYADMIN"], ResourceName("SYSADMIN"))

        assert role == ResourceName("SECURITYADMIN")

    def test_object_grants_are_unaffected_by_the_delegation_path(self, session_ctx):
        """A declared owner on an ordinary grant keeps running as SECURITYADMIN, as before."""
        grant = res.Grant(priv="SELECT", on_table="DB.SCH.TBL", to="ANALYST", owner="SOME_ROLE")
        change = CreateResource(
            urn=parse_URN("urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv=SELECT&on=table/DB.SCH.TBL&to=role/ANALYST"),
            resource_cls=res.Grant,
            container=None,
            after=grant.to_dict(),
        )

        role, _ = execution_strategy_for_change(
            change, ["SYSADMIN", "SECURITYADMIN", "SOME_ROLE"], ResourceName("SYSADMIN")
        )

        assert role == ResourceName("SECURITYADMIN")

    def test_an_existing_inherited_grant_produces_no_changes(self, session_ctx, remote_state):
        """The point of inherited grants over ON ALL: Snowflake reports one durable record,
        so the plan is empty on a second run instead of reapplying the grant every time."""
        from snowcap import data_provider

        grant = res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE SALES_DB", to="ANALYST")
        manifest = self._manifest(session_ctx, [res.Database(name="SALES_DB"), res.Role(name="ANALYST"), grant])
        urn = URN.from_resource(account_locator=session_ctx["account_locator"], resource=grant)
        row = {
            "privilege": "SELECT",
            "granted_on": "TABLE",
            "name": "",
            "granted_to": "ROLE",
            "grantee_name": "ANALYST",
            "grant_option": "false",
            "granted_by": "SECURITYADMIN",
            "is_inherited": True,
            "inherited_from": "DATABASE",
            "inherited_from_database": "SALES_DB",
            "inherited_from_schema": "",
        }

        with patch("snowcap.data_provider.execute", return_value=[row]):
            fetched = data_provider.fetch_inherited_grant(MagicMock(), urn.fqn)

        remote_state = remote_state.copy()
        remote_state[urn] = fetched

        assert [change for change in diff(remote_state, manifest) if change.urn == urn] == []

    def test_config_can_enable_the_feature_itself(self, session_ctx):
        """Declaring the account parameter is the supported way to turn the preview on, so
        the plan gate must not block the very config that enables it."""
        manifest = self._manifest(
            session_ctx,
            [
                res.AccountParameter(name="FEATURE_RBAC_INHERITED_GRANTS", value="ENABLED"),
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER"),
            ],
        )

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled", return_value=False) as probe:
            raise_if_inherited_grants_unavailable(MagicMock(), manifest)

        assert not probe.called

    def test_disabling_the_parameter_does_not_count_as_enabling_it(self, session_ctx):
        manifest = self._manifest(
            session_ctx,
            [
                res.AccountParameter(name="FEATURE_RBAC_INHERITED_GRANTS", value="DISABLED"),
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER"),
            ],
        )

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled", return_value=False):
            with pytest.raises(MissingPrivilegeException):
                raise_if_inherited_grants_unavailable(MagicMock(), manifest)

    def test_inherited_grants_are_applied_after_the_feature_flag(self, session_ctx):
        """Both are account-scoped with nothing else linking them, so without an explicit
        dependency they would land in the same level and run concurrently."""
        flag = res.AccountParameter(name="FEATURE_RBAC_INHERITED_GRANTS", value="ENABLED")
        grant = res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER")
        manifest = self._manifest(session_ctx, [flag, res.Database(name="MY_DB"), res.Role(name="READER"), grant])

        locator = session_ctx["account_locator"]
        flag_urn = URN.from_resource(account_locator=locator, resource=flag)
        grant_urn = URN.from_resource(account_locator=locator, resource=grant)

        # The ordering must come from a real dependency edge, not incidental level
        # assignment: assert the grant->flag edge is present in the manifest.
        assert (grant_urn, flag_urn) in set(manifest.refs)

        resource_set = set(manifest.urns)
        for parent, ref in manifest.refs:
            resource_set.add(parent)
            resource_set.add(ref)
        levels = compute_levels(resource_set, set(manifest.refs))
        assert levels[grant_urn] > levels[flag_urn]

    def test_grants_on_all_are_not_linked_to_the_feature_flag(self, session_ctx):
        """Only inherited grants need the preview; an ON ALL grant must not be held back."""
        flag = res.AccountParameter(name="FEATURE_RBAC_INHERITED_GRANTS", value="ENABLED")
        grant = res.Grant(priv="SELECT", on="ALL TABLES IN DATABASE MY_DB", to="READER")
        manifest = self._manifest(session_ctx, [flag, res.Database(name="MY_DB"), res.Role(name="READER"), grant])

        locator = session_ctx["account_locator"]
        flag_urn = URN.from_resource(account_locator=locator, resource=flag)
        grant_urn = URN.from_resource(account_locator=locator, resource=grant)
        assert (grant_urn, flag_urn) not in manifest.refs

    def test_error_points_at_preview_access_when_it_is_disabled(self, session_ctx):
        """Setting the parameter will not help while preview features are off account-wide,
        and Snowcap cannot turn them on -- it is a system function, not a resource."""
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER"),
            ],
        )

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled", return_value=False):
            with patch("snowcap.data_provider.fetch_preview_access_enabled", return_value=False):
                with pytest.raises(MissingPrivilegeException, match=r"SYSTEM\$ENABLE_PREVIEW_ACCESS"):
                    raise_if_inherited_grants_unavailable(MagicMock(), manifest)

    def test_error_suggests_the_parameter_when_preview_access_is_fine(self, session_ctx):
        manifest = self._manifest(
            session_ctx,
            [
                res.Database(name="MY_DB"),
                res.Role(name="READER"),
                res.Grant(priv="SELECT", on="INHERITED TABLES IN DATABASE MY_DB", to="READER"),
            ],
        )

        with patch("snowcap.data_provider.fetch_inherited_grants_enabled", return_value=False):
            with patch("snowcap.data_provider.fetch_preview_access_enabled", return_value=True):
                with pytest.raises(MissingPrivilegeException) as excinfo:
                    raise_if_inherited_grants_unavailable(MagicMock(), manifest)

        assert "account_parameters" in str(excinfo.value)
        assert "SYSTEM$ENABLE_PREVIEW_ACCESS" not in str(excinfo.value)


class TestImportedPrivilegesPlanning:
    """
    A single IMPORTED PRIVILEGES grant on a shared database fans out in SHOW GRANTS into a
    row per object the share exposes. Those rows can never be in the manifest, so sync must
    recognise them as covered rather than revoking the access the declared grant provides.
    """

    def _shared_object_grant_state(self, priv, on, on_type, to="Z_DB__SNOWFLAKE"):
        urn = parse_URN(
            f"urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv={priv}&on={on_type.lower()}/{on}&to=role/{to}"
        )
        return urn, {
            "priv": priv,
            "on": on,
            "on_type": on_type,
            "to": to,
            "items_type": None,
            "to_type": "ROLE",
            "grant_option": False,
            "grant_type": "OBJECT",
            "owner": "SYSADMIN",
            "_privs": [priv],
        }

    def _manifest(self, session_ctx, resources):
        return Blueprint(resources=resources).generate_manifest(session_ctx)

    def _snowflake_share_manifest(self, session_ctx, to="Z_DB__SNOWFLAKE"):
        return self._manifest(
            session_ctx,
            [
                res.Role(name=to),
                res.Grant(priv="IMPORTED PRIVILEGES", on="database SNOWFLAKE", to=to),
            ],
        )

    @pytest.mark.parametrize(
        "priv,on,on_type",
        [
            # The fan-out carries whatever privilege each object type takes, never
            # "IMPORTED PRIVILEGES" itself.
            ("SELECT", "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", "VIEW"),
            ("USAGE", "SNOWFLAKE.CORE.DUPLICATE_COUNT(TABLE(DATE)", "FUNCTION"),
            ("USAGE", "SNOWFLAKE.CORTEX.CREATE_AI_FUNCTION(VARCHAR)", "PROCEDURE"),
            ("USAGE", "SNOWFLAKE.ACCOUNT_USAGE", "SCHEMA"),
            ("USAGE", "SNOWFLAKE.CORTEX_USER", "DATABASE_ROLE"),
            ("READ", "SNOWFLAKE.IMAGES.SNOWFLAKE_IMAGES", "IMAGE_REPOSITORY"),
            ("APPLY", "SNOWFLAKE.CORE.CERTIFICATION_STATUS", "TAG"),
            # Snowflake also reports the database itself
            ("USAGE", "SNOWFLAKE", "DATABASE"),
            ("REFERENCE_USAGE", "SNOWFLAKE", "DATABASE"),
        ],
    )
    def test_fan_out_of_an_imported_privileges_grant_is_not_dropped(self, session_ctx, remote_state, priv, on, on_type):
        remote_state = remote_state.copy()
        urn, data = self._shared_object_grant_state(priv, on, on_type)
        remote_state[urn] = data

        plan = diff(remote_state, self._snowflake_share_manifest(session_ctx))

        assert not [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_fan_out_to_a_different_grantee_is_still_dropped(self, session_ctx, remote_state):
        """Coverage is scoped to the role named by the declared grant."""
        remote_state = remote_state.copy()
        urn, data = self._shared_object_grant_state(
            "SELECT", "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", "VIEW", to="SOME_OTHER_ROLE"
        )
        remote_state[urn] = data

        plan = diff(remote_state, self._snowflake_share_manifest(session_ctx))

        assert [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_grants_outside_the_shared_database_are_still_dropped(self, session_ctx, remote_state):
        """An IMPORTED PRIVILEGES grant protects only objects inside its own database."""
        remote_state = remote_state.copy()
        urn, data = self._shared_object_grant_state("SELECT", "OTHER_DB.SCH.TBL", "TABLE")
        remote_state[urn] = data

        plan = diff(remote_state, self._snowflake_share_manifest(session_ctx))

        assert [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_a_database_named_like_the_share_is_not_covered(self, session_ctx, remote_state):
        """Containment is by identifier; a different database is a different container."""
        remote_state = remote_state.copy()
        urn, data = self._shared_object_grant_state("USAGE", "SNOWFLAKE_OTHER", "DATABASE")
        remote_state[urn] = data

        plan = diff(remote_state, self._snowflake_share_manifest(session_ctx))

        assert [change for change in plan if isinstance(change, DropResource) and change.urn == urn]

    def test_object_grants_are_dropped_when_no_imported_privileges_are_declared(self, session_ctx, remote_state):
        """Without a declared IMPORTED PRIVILEGES grant nothing changes: undeclared object
        grants are still reaped by sync."""
        remote_state = remote_state.copy()
        urn, data = self._shared_object_grant_state("SELECT", "SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY", "VIEW")
        remote_state[urn] = data
        manifest = self._manifest(session_ctx, [res.Role(name="Z_DB__SNOWFLAKE")])

        plan = diff(remote_state, manifest)

        assert [change for change in plan if isinstance(change, DropResource) and change.urn == urn]


class TestCreateInsideTransferredContainer:
    """A plan that adopts an existing database both transfers it and creates resources
    inside it. Containers sit at a lower dependency level than their contents, so the
    transfer runs first, and a CREATE planned against the container's old owner arrives
    to find that role no longer owns anything."""

    def _database_role_change(self, container_owner):
        database_role = res.DatabaseRole(name="DR_READER_ROLE", database="GREAT_BAY_DEV", owner="USERADMIN")
        return CreateResource(
            urn=parse_URN("urn::ABCD123:database_role/GREAT_BAY_DEV.DR_READER_ROLE"),
            resource_cls=res.DatabaseRole,
            container=(parse_URN("urn::ABCD123:database/GREAT_BAY_DEV"), ResourceName(container_owner)),
            after=database_role.to_dict(),
        )

    def test_create_runs_as_the_owner_the_container_ends_up_with(self):
        change = self._database_role_change("ANALYST")
        transferred = {parse_URN("urn::ABCD123:database/GREAT_BAY_DEV"): ResourceName("TRANSFORMER_DBT")}

        role, _ = execution_strategy_for_change(
            change,
            [ResourceName("ANALYST"), ResourceName("TRANSFORMER_DBT"), ResourceName("SECURITYADMIN")],
            ResourceName("SECURITYADMIN"),
            transferred,
        )

        assert role == ResourceName("TRANSFORMER_DBT")

    def test_create_runs_as_the_current_owner_when_the_container_is_not_transferred(self):
        change = self._database_role_change("ANALYST")

        role, _ = execution_strategy_for_change(
            change,
            [ResourceName("ANALYST"), ResourceName("TRANSFORMER_DBT"), ResourceName("SECURITYADMIN")],
            ResourceName("SECURITYADMIN"),
            {},
        )

        assert role == ResourceName("ANALYST")

    def test_a_transfer_of_a_different_container_is_ignored(self):
        change = self._database_role_change("ANALYST")
        transferred = {parse_URN("urn::ABCD123:database/BALBOA_DEV"): ResourceName("TRANSFORMER_DBT")}

        role, _ = execution_strategy_for_change(
            change,
            [ResourceName("ANALYST"), ResourceName("TRANSFORMER_DBT"), ResourceName("SECURITYADMIN")],
            ResourceName("SECURITYADMIN"),
            transferred,
        )

        assert role == ResourceName("ANALYST")

    def test_compile_plan_to_sql_picks_up_the_transfer_from_the_plan(self, session_ctx):
        """The end-to-end path: compile_plan_to_sql derives the mapping from the plan
        itself, so a caller does not have to know the transfer happened."""
        plan = [
            TransferOwnership(
                urn=parse_URN("urn::ABCD123:database/GREAT_BAY_DEV"),
                resource_cls=res.Database,
                from_owner="ANALYST",
                to_owner="TRANSFORMER_DBT",
            ),
            self._database_role_change("ANALYST"),
        ]

        commands, _ = compile_plan_to_sql(session_ctx, plan)

        create = [c for c in commands if isinstance(c["change"], CreateResource)][0]
        assert create["role"] == ResourceName("TRANSFORMER_DBT")


class TestDroppingGrantsOnSharedDatabases:
    """Privileges on a shared database arrive as the fan-out of one IMPORTED PRIVILEGES
    grant and cannot be revoked one at a time. Snowflake rejects the individual revoke with
    "Revoking individual privileges on imported database is not allowed", and because that
    is a SQL compilation error rather than a permissions one it aborts the apply."""

    def _drop(self, priv, on, on_type, to="ANALYST"):
        return DropResource(
            urn=parse_URN(
                f"urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv={priv}&on={on_type.lower()}/{on}&to=role/{to}"
            ),
            before={
                "priv": priv,
                "on": on,
                "on_type": on_type,
                "to": to,
                "to_type": "ROLE",
                "items_type": None,
                "grant_option": False,
                "grant_type": "OBJECT",
                "owner": "SYSADMIN",
                "_privs": [priv],
            },
        )

    @pytest.mark.parametrize(
        "priv,on,on_type",
        [
            ("USAGE", "WORLDWIDE_ADDRESS_DATA", "DATABASE"),
            ("USAGE", "WORLDWIDE_ADDRESS_DATA.ADDRESS", "SCHEMA"),
            ("SELECT", "WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS", "TABLE"),
        ],
    )
    def test_every_row_of_the_fan_out_revokes_the_share(self, session_ctx, priv, on, on_type):
        """Database, schema and object rows all map to the same statement -- the share is
        the only thing that can be given back."""
        commands, _ = compile_plan_to_sql(session_ctx, [self._drop(priv, on, on_type)], {"WORLDWIDE_ADDRESS_DATA"})

        sql = " ".join(commands[0]["commands"])
        assert "REVOKE IMPORTED PRIVILEGES ON DATABASE WORLDWIDE_ADDRESS_DATA FROM ROLE ANALYST" in sql
        assert "REVOKE USAGE ON DATABASE WORLDWIDE_ADDRESS_DATA" not in sql

    def test_ordinary_databases_still_revoke_the_individual_privilege(self, session_ctx):
        """The share form must not leak onto normal databases, where it is invalid."""
        commands, _ = compile_plan_to_sql(
            session_ctx, [self._drop("USAGE", "BALBOA", "DATABASE")], {"WORLDWIDE_ADDRESS_DATA"}
        )

        sql = " ".join(commands[0]["commands"])
        assert "REVOKE USAGE ON DATABASE BALBOA FROM ROLE ANALYST" in sql
        assert "IMPORTED PRIVILEGES" not in sql

    def test_account_level_object_named_like_a_shared_db_is_not_the_share(self, session_ctx):
        """A warehouse (or other account-level object) whose name collides with an imported
        database must revoke its own privilege, not IMPORTED PRIVILEGES on the share."""
        commands, _ = compile_plan_to_sql(
            session_ctx, [self._drop("USAGE", "WORLDWIDE_ADDRESS_DATA", "WAREHOUSE")], {"WORLDWIDE_ADDRESS_DATA"}
        )

        sql = " ".join(commands[0]["commands"])
        assert "REVOKE USAGE ON WAREHOUSE WORLDWIDE_ADDRESS_DATA FROM ROLE ANALYST" in sql
        assert "IMPORTED PRIVILEGES" not in sql

    def test_shared_database_match_is_quote_aware(self):
        """A database quoted with a literal dot must still match the shared-databases set; a
        naive split would produce the wrong name and miss it."""
        from snowcap.blueprint import _shared_database_for_grant

        change = DropResource(
            urn=parse_URN("urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv=USAGE&on=database/X&to=role/R"),
            before={"on": '"prod.mirror".ADDRESS', "on_type": "SCHEMA", "priv": "USAGE", "to": "R"},
        )

        assert _shared_database_for_grant(change, {"PROD.MIRROR"}) == "PROD.MIRROR"

    def test_no_shared_databases_known_leaves_behaviour_unchanged(self, session_ctx):
        commands, _ = compile_plan_to_sql(session_ctx, [self._drop("USAGE", "WORLDWIDE_ADDRESS_DATA", "DATABASE")])

        sql = " ".join(commands[0]["commands"])
        assert "REVOKE USAGE ON DATABASE WORLDWIDE_ADDRESS_DATA" in sql


class TestRevokingAccountLevelPrivileges:
    """An account-level privilege belongs to the system role that owns it. Snowflake will
    not take one back from a role that does not own it -- and rather than failing, the
    REVOKE reports success while leaving the privilege in place, so the same drop reappears
    in every later plan and nothing in the output says why."""

    def _account_grant_change(self, cls, priv="CREATE DATABASE", to="TRANSFORMER_DBT"):
        data = {
            "priv": priv,
            "on": "ACCOUNT",
            "on_type": "ACCOUNT",
            "to": to,
            "to_type": "ROLE",
            "items_type": None,
            "grant_option": False,
            "grant_type": "OBJECT",
            "owner": "SECURITYADMIN",
            "_privs": [priv],
        }
        urn = parse_URN(
            f"urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv={priv.replace(' ', '%20')}"
            f"&on=account/ACCOUNT&to=role/{to}"
        )
        if cls is DropResource:
            return DropResource(urn=urn, before=data)
        return CreateResource(urn=urn, resource_cls=res.Grant, container=None, after=data)

    ROLES = [ResourceName("SYSADMIN"), ResourceName("SECURITYADMIN"), ResourceName("ACCOUNTADMIN")]

    def test_revoke_runs_as_the_system_role_that_owns_the_privilege(self):
        change = self._account_grant_change(DropResource)

        role, _ = execution_strategy_for_change(change, self.ROLES, ResourceName("SECURITYADMIN"))

        assert role == ResourceName("SYSADMIN")

    def test_grant_and_revoke_agree_on_the_role(self):
        """The asymmetry was the bug: grants already used the system role."""
        grant_role, _ = execution_strategy_for_change(
            self._account_grant_change(CreateResource), self.ROLES, ResourceName("SECURITYADMIN")
        )
        revoke_role, _ = execution_strategy_for_change(
            self._account_grant_change(DropResource), self.ROLES, ResourceName("SECURITYADMIN")
        )

        assert grant_role == revoke_role == ResourceName("SYSADMIN")

    def test_openflow_data_plane_integration_is_a_known_account_privilege(self):
        """Snowcap did not know this privilege, so it fell through to SECURITYADMIN and the
        revoke silently did nothing."""
        from snowcap.privs import system_role_for_priv

        assert system_role_for_priv("CREATE OPENFLOW DATA PLANE INTEGRATION") == "ACCOUNTADMIN"

        change = self._account_grant_change(DropResource, priv="CREATE OPENFLOW DATA PLANE INTEGRATION", to="LOADER")
        role, _ = execution_strategy_for_change(change, self.ROLES, ResourceName("SECURITYADMIN"))

        assert role == ResourceName("ACCOUNTADMIN")

    def test_revoke_falls_back_to_securityadmin_without_the_system_role(self):
        change = self._account_grant_change(DropResource)

        role, _ = execution_strategy_for_change(change, [ResourceName("SECURITYADMIN")], ResourceName("SECURITYADMIN"))

        assert role == ResourceName("SECURITYADMIN")

    def test_object_grant_revokes_still_use_securityadmin(self):
        """Only account-level privileges have an owning system role; ordinary object grants
        must keep going through SECURITYADMIN."""
        data = {
            "priv": "USAGE",
            "on": "BALBOA",
            "on_type": "DATABASE",
            "to": "ANALYST",
            "to_type": "ROLE",
            "items_type": None,
            "grant_option": False,
            "grant_type": "OBJECT",
            "owner": "SECURITYADMIN",
            "_privs": ["USAGE"],
        }
        change = DropResource(
            urn=parse_URN("urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv=USAGE&on=database/BALBOA&to=role/ANALYST"),
            before=data,
        )

        role, _ = execution_strategy_for_change(change, self.ROLES, ResourceName("SECURITYADMIN"))

        assert role == ResourceName("SECURITYADMIN")


class TestGrantsHeldByDatabaseRoles:
    """A database role is named <database>.<role> and lives inside its database. Managing a
    grant it holds needs a role that can see that database. SECURITYADMIN can hold
    account-level MANAGE GRANTS and still lack USAGE on the database, and REVOKE reports
    success rather than failing on a grantee it cannot resolve -- so the grant survives and
    the same drop reappears in every later plan, with nothing in the output to say why."""

    OWNERS = {"GREAT_BAY": "TRANSFORMER_DBT"}

    def _change(self, cls, to="GREAT_BAY.DR_CREATE_ROLE", to_type="DATABASE ROLE"):
        data = {
            "priv": "USAGE",
            "on": "GREAT_BAY",
            "on_type": "DATABASE",
            "to": to,
            "to_type": to_type,
            "items_type": None,
            "grant_option": False,
            "grant_type": "OBJECT",
            "owner": "SECURITYADMIN",
            "_privs": ["USAGE"],
        }
        urn = parse_URN(
            "urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv=USAGE&on=database/GREAT_BAY"
            f"&to={to_type.lower().replace(' ', '_')}/{to}"
        )
        if cls is DropResource:
            return DropResource(urn=urn, before=data)
        return CreateResource(urn=urn, resource_cls=res.Grant, container=None, after=data)

    ROLES = [ResourceName("SECURITYADMIN"), ResourceName("TRANSFORMER_DBT")]

    def test_revoke_runs_as_the_database_owner(self):
        role, _ = execution_strategy_for_change(
            self._change(DropResource), self.ROLES, ResourceName("SECURITYADMIN"), None, self.OWNERS
        )

        assert role == ResourceName("TRANSFORMER_DBT")

    def test_grant_and_revoke_agree_on_the_role(self):
        grant_role, _ = execution_strategy_for_change(
            self._change(CreateResource), self.ROLES, ResourceName("SECURITYADMIN"), None, self.OWNERS
        )
        revoke_role, _ = execution_strategy_for_change(
            self._change(DropResource), self.ROLES, ResourceName("SECURITYADMIN"), None, self.OWNERS
        )

        assert grant_role == revoke_role == ResourceName("TRANSFORMER_DBT")

    def test_grants_to_account_roles_still_use_securityadmin(self):
        """Account-level authority does reach an account role, so nothing changes there."""
        role, _ = execution_strategy_for_change(
            self._change(DropResource, to="ANALYST", to_type="ROLE"),
            self.ROLES,
            ResourceName("SECURITYADMIN"),
            None,
            self.OWNERS,
        )

        assert role == ResourceName("SECURITYADMIN")

    def test_falls_back_when_the_database_owner_is_not_available(self):
        role, _ = execution_strategy_for_change(
            self._change(DropResource),
            [ResourceName("SECURITYADMIN")],
            ResourceName("SECURITYADMIN"),
            None,
            self.OWNERS,
        )

        assert role == ResourceName("SECURITYADMIN")

    def test_falls_back_when_the_database_is_unknown(self):
        role, _ = execution_strategy_for_change(
            self._change(DropResource), self.ROLES, ResourceName("SECURITYADMIN"), None, {"OTHER_DB": "SYSADMIN"}
        )

        assert role == ResourceName("SECURITYADMIN")

    def test_no_owner_map_leaves_behaviour_unchanged(self):
        role, _ = execution_strategy_for_change(self._change(DropResource), self.ROLES, ResourceName("SECURITYADMIN"))

        assert role == ResourceName("SECURITYADMIN")


class TestSurvivingDropsAreReported:
    """Snowflake does not always fail a statement it could not carry out -- REVOKE reports
    success when the executing role does not own the privilege or cannot resolve the
    grantee. The apply sees no exception and counts the drop as applied, so the grant
    survives and the same drop returns in every later plan with nothing explaining why."""

    def _drop(self, to="ANALYST"):
        return DropResource(
            urn=parse_URN(f"urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv=USAGE&on=database/GREAT_BAY&to=role/{to}"),
            before={
                "priv": "USAGE",
                "on": "GREAT_BAY",
                "on_type": "DATABASE",
                "to": to,
                "to_type": "ROLE",
                "items_type": None,
                "grant_option": False,
                "grant_type": "OBJECT",
                "owner": "SECURITYADMIN",
                "_privs": ["USAGE"],
            },
        )

    @patch("snowcap.blueprint.reset_cache")
    @patch("snowcap.blueprint.data_provider.fetch_resource")
    def test_a_drop_whose_resource_is_still_there_is_reported(self, mock_fetch, _mock_reset):
        from snowcap.blueprint import surviving_drops

        mock_fetch.return_value = {"priv": "USAGE"}  # still present after the revoke
        change = self._drop()

        assert surviving_drops(MagicMock(), [change]) == [change]

    @patch("snowcap.blueprint.reset_cache")
    @patch("snowcap.blueprint.data_provider.fetch_resource")
    def test_a_drop_that_took_effect_is_not_reported(self, mock_fetch, _mock_reset):
        from snowcap.blueprint import surviving_drops

        mock_fetch.return_value = None

        assert surviving_drops(MagicMock(), [self._drop()]) == []

    @patch("snowcap.blueprint.data_provider.reset_account_usage_caches")
    @patch("snowcap.blueprint.reset_cache")
    @patch("snowcap.blueprint.data_provider.fetch_resource")
    def test_state_is_re_read_rather_than_served_from_the_apply_s_cache(self, mock_fetch, mock_reset, mock_au_reset):
        """The apply just changed the state these checks read. Both the general cache and the
        ACCOUNT_USAGE grant snapshot must be cleared, or revoked account-role grants re-appear
        as false survivors on use_account_usage runs."""
        from snowcap.blueprint import surviving_drops

        mock_fetch.return_value = None
        surviving_drops(MagicMock(), [self._drop()])

        mock_reset.assert_called_once()
        mock_au_reset.assert_called_once()

    @patch("snowcap.blueprint.reset_cache")
    @patch("snowcap.blueprint.data_provider.fetch_resource")
    def test_a_resource_that_cannot_be_read_back_is_not_reported_as_surviving(self, mock_fetch, _mock_reset):
        """Not being able to confirm a drop is not evidence that it failed."""
        from snowcap.blueprint import surviving_drops

        mock_fetch.side_effect = Exception("no fetch function for this resource type")

        assert surviving_drops(MagicMock(), [self._drop()]) == []

    @patch("snowcap.blueprint.reset_cache")
    @patch("snowcap.blueprint.data_provider.fetch_resource")
    def test_nothing_is_read_back_when_the_plan_dropped_nothing(self, mock_fetch, mock_reset):
        """Applies that only create must not pay for this."""
        from snowcap.blueprint import surviving_drops

        change = CreateResource(
            urn=parse_URN("urn::ABCD123:role/SOME_ROLE"),
            resource_cls=res.Role,
            container=None,
            after={"name": "SOME_ROLE", "owner": "USERADMIN"},
        )

        assert surviving_drops(MagicMock(), [change]) == []
        mock_fetch.assert_not_called()
        mock_reset.assert_not_called()

    def test_report_names_each_survivor(self, capsys):
        from snowcap.blueprint import print_surviving_drops

        print_surviving_drops([self._drop()])
        out = capsys.readouterr().out

        assert "1 drop(s) reported success" in out
        assert "USAGE on DATABASE.GREAT_BAY \u2192 ROLE.ANALYST" in out

    def test_report_guides_on_database_role_grantees(self, capsys):
        """A survivor held by a database role gets the specific remedy: grant the role that
        owns its database, since SECURITYADMIN cannot resolve the grantee."""
        from snowcap.blueprint import print_surviving_drops

        survivor = DropResource(
            urn=parse_URN(
                "urn::ABCD123:grant/GRANT?grant_type=OBJECT&priv=USAGE"
                "&on=database/GREAT_BAY&to=database_role/GREAT_BAY.DR"
            ),
            before={
                "priv": "USAGE",
                "on": "GREAT_BAY",
                "on_type": "DATABASE",
                "to": "GREAT_BAY.DR",
                "to_type": "DATABASE ROLE",
                "items_type": None,
                "grant_option": False,
                "grant_type": "OBJECT",
                "owner": "SECURITYADMIN",
                "_privs": ["USAGE"],
            },
        )

        print_surviving_drops([survivor])
        out = capsys.readouterr().out

        assert "database roles" in out
        assert "GREAT_BAY" in out

    def test_report_is_silent_when_every_drop_took_effect(self, capsys):
        from snowcap.blueprint import print_surviving_drops

        print_surviving_drops([])

        assert capsys.readouterr().out == ""


class TestSyncReadsFutureGrantsRegardless:
    """Syncing a resource type means removing what config does not declare, so a future
    grant absent from config is exactly what has to be found. Skipping the SHOW FUTURE
    GRANTS query when the manifest declared none kept the ones already in Snowflake out of
    remote state, so sync could not propose dropping them -- unseen rather than kept, with
    nothing in the plan to say so.

    Migrating from ALL plus FUTURE pairs to inherited grants removes the last future grant
    from config, which is precisely when this bites."""

    def _grant_list_kwargs(self, resources):
        """How fetch_remote_state asks for grants, for a config with no future grants.

        Only the listing call matters here, and it happens before the rest of
        fetch_remote_state; the later failure is mock plumbing for reference resolution,
        not the behaviour under test.
        """
        from snowcap.blueprint_config import BlueprintConfig

        bp = Blueprint(resources=resources)
        bp._config = BlueprintConfig(sync_resources={ResourceType.GRANT})

        with (
            patch("snowcap.blueprint.data_provider.fetch_session") as mock_session,
            patch("snowcap.blueprint.data_provider.use_secondary_roles"),
            patch("snowcap.blueprint.data_provider.list_resource") as mock_list,
        ):
            mock_session.return_value = self.SESSION_CTX
            mock_list.return_value = []
            manifest = bp.generate_manifest(self.SESSION_CTX)
            try:
                bp.fetch_remote_state(MagicMock(), manifest)
            except Exception:
                pass
            grant_calls = [c for c in mock_list.call_args_list if c.args[1] == "grant"]

        assert grant_calls, "grants must be listed when grant is a sync_resource"
        return grant_calls[0].kwargs

    @pytest.fixture(autouse=True)
    def _ctx(self, session_ctx):
        type(self).SESSION_CTX = session_ctx

    def test_future_grants_are_listed_when_config_declares_none(self):
        kwargs = self._grant_list_kwargs([res.Role(name="SOME_ROLE")])

        assert kwargs["include_future_grants"] is True

    def test_the_query_is_not_narrowed_to_roles_named_in_config(self):
        """A role holding a future grant only in Snowflake was never queried, so its grant
        could not be dropped either."""
        kwargs = self._grant_list_kwargs([res.Role(name="SOME_ROLE")])

        assert "future_grant_roles" not in kwargs
        assert "future_grant_database_roles" not in kwargs


class TestSummarizePlanValue:
    """The plan table must not dump a multiline SQL body (alert THEN, task body)."""

    def test_short_scalar_prints_verbatim(self):
        assert _summarize_plan_value("STARTED") == "STARTED"
        assert _summarize_plan_value("1 MINUTE") == "1 MINUTE"

    def test_none_is_empty(self):
        assert _summarize_plan_value(None) == ""

    def test_multiline_body_becomes_a_shape_not_sql(self):
        body = "BEGIN\n  LET x;\n  CALL foo();\nEND"
        summary = _summarize_plan_value(body)
        assert summary == "<4 lines, 32 chars>"
        assert "\n" not in summary
        assert "CALL" not in summary

    def test_long_single_line_is_bounded(self):
        summary = _summarize_plan_value("x" * 90)
        assert summary == "<1 line, 90 chars>"
        assert len(summary) < 90
