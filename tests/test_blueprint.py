import json
import re
from copy import deepcopy

import pytest

from snowcap import resources as res


def strip_ansi(text):
    """Remove ANSI color codes from text."""
    return re.sub(r'\x1b\[[0-9;]*m', '', text)


def find_change_by_urn(plan, urn):
    """Find a change in the plan by its URN."""
    for change in plan:
        if change.urn == urn:
            return change
    return None


def flatten_sql_commands(sql_commands: list[dict]) -> list[str]:
    """Flatten compile_plan_to_sql output to a list of SQL strings for testing."""
    result = ["USE SECONDARY ROLES ALL"]
    last_role = None
    for cmd in sql_commands:
        if cmd["role"] != last_role:
            result.append(f"USE ROLE {cmd['role']}")
            last_role = cmd["role"]
        result.extend(cmd["commands"])
    return result


from snowcap import var
from snowcap.blueprint import (
    Blueprint,
    CreateResource,
    _merge_pointers,
    compile_plan_to_sql,
    diff,
    dump_plan,
)
from snowcap.blueprint_config import BlueprintConfig
from snowcap.enums import AccountEdition, BlueprintScope, ResourceType
from snowcap.exceptions import (
    DuplicateResourceException,
    InvalidResourceException,
    MissingVarException,
    NonConformingPlanException,
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
    test_db = res.Database(name="TEST_TITAN", transient=False, data_retention_time_in_days=1, comment="Test Titan")
    schema = res.Schema(name="TEST_SCHEMA", database=test_db, transient=False, comment="Test Titan Schema")
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
            "resource_cls": "Role",
            "urn": "urn::ABCD123:role/ROLE1",
            "after": {"name": "ROLE1", "owner": "USERADMIN", "comment": None},
        }
    ]
    plan_str = strip_ansi(dump_plan(plan, format="text"))
    assert (
        plan_str
        == """
» snowcap
» Plan: 1 to create, 0 to update, 0 to transfer, 0 to drop.

+ urn::ABCD123:role/ROLE1 {
  + name    = "ROLE1"
  + owner   = "USERADMIN"
  + comment = None
}

"""
    )


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
    assert (
        plan_str
        == """
» snowcap
» Plan: 0 to create, 1 to update, 0 to transfer, 0 to drop.

~ urn::ABCD123:role/ROLE1 {
  ~ comment = "old" -> "new"
}

"""
    )


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
    assert (
        plan_str
        == """
» snowcap
» Plan: 0 to create, 0 to update, 1 to transfer, 0 to drop.

~ urn::ABCD123:role/ROLE1 {
  ~ owner = "ACCOUNTADMIN" -> "USERADMIN"
}

"""
    )


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
    assert (
        plan_str
        == """
» snowcap
» Plan: 0 to create, 0 to update, 0 to transfer, 1 to drop.

- urn::ABCD123:role/ROLE1

"""
    )


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
