import pytest

from snowcap import resources as res
from snowcap.blueprint import (
    Blueprint,
    CreateResource,
    MissingPrivilegeException,
    TransferOwnership,
    UpdateResource,
    compile_plan_to_sql,
    diff,
)
from snowcap.enums import AccountEdition
from snowcap.identifiers import parse_URN
from snowcap.resource_name import ResourceName


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


def find_change_by_urn(plan, urn):
    """Find a change in the plan by its URN."""
    for change in plan:
        if change.urn == urn:
            return change
    return None


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


def test_default_owner(session_ctx, remote_state):
    warehouse = res.Warehouse(name="test_warehouse")
    assert warehouse._data.owner.name == "SYSADMIN"
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].after["owner"] == "SYSADMIN"


def test_non_default_owner(session_ctx, remote_state):
    warehouse = res.Warehouse(name="test_warehouse", owner="ACCOUNTADMIN")
    assert warehouse._data.owner.name == "ACCOUNTADMIN"
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].after["owner"] == "ACCOUNTADMIN"


def test_custom_role_owner(session_ctx, remote_state):
    role = res.Role(name="CUSTOMROLE")
    grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    warehouse = res.Warehouse(name="test_warehouse", owner=role)
    assert warehouse._data.owner.name == "CUSTOMROLE"
    blueprint = Blueprint(resources=[role, grant, warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 3
    # Find changes by URN instead of relying on order
    role_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:role/CUSTOMROLE"))
    grant_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:role_grant/CUSTOMROLE?role=SYSADMIN"))
    warehouse_change = find_change_by_urn(plan, parse_URN("urn::ABCD123:warehouse/test_warehouse"))
    assert isinstance(role_change, CreateResource)
    assert isinstance(grant_change, CreateResource)
    assert isinstance(warehouse_change, CreateResource)
    assert warehouse_change.after["owner"] == "CUSTOMROLE"


# def test_invalid_custom_role_owner(session_ctx):
#     role = res.Role(name="INVALIDROLE")
#     warehouse = res.Warehouse(name="test_warehouse", owner=role)
#     blueprint = Blueprint(resources=[role, warehouse])
#     with pytest.raises(InvalidOwnerException):
#         blueprint.generate_manifest(session_ctx)


def test_transfer_ownership(session_ctx, remote_state):
    remote_state = remote_state.copy()
    remote_state[parse_URN("urn::ABCD123:role/test_role")] = {
        "name": str(ResourceName("test_role")),
        "owner": "ACCOUNTADMIN",
        "comment": None,
    }

    role = res.Role(name="test_role", owner="USERADMIN")
    blueprint = Blueprint(resources=[role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], TransferOwnership)
    assert plan[0].from_owner == "ACCOUNTADMIN"
    assert plan[0].to_owner == "USERADMIN"
    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE ACCOUNTADMIN"
    assert sql_commands[2] == "GRANT OWNERSHIP ON ROLE TEST_ROLE TO ROLE USERADMIN COPY CURRENT GRANTS"


def test_transfer_ownership_with_changes(session_ctx, remote_state):
    remote_state = remote_state.copy()
    remote_state[parse_URN("urn::ABCD123:role/test_role")] = {
        "name": str(ResourceName("test_role")),
        "owner": "ACCOUNTADMIN",
        "comment": None,
    }

    role = res.Role(name="test_role", comment="This comment has been added", owner="USERADMIN")
    blueprint = Blueprint(resources=[role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 2
    # Find changes by type since order is not guaranteed
    update_change = next((c for c in plan if isinstance(c, UpdateResource)), None)
    transfer_change = next((c for c in plan if isinstance(c, TransferOwnership)), None)
    assert update_change is not None
    assert update_change.after["comment"] == "This comment has been added"
    assert transfer_change is not None
    assert transfer_change.from_owner == "ACCOUNTADMIN"
    assert transfer_change.to_owner == "USERADMIN"
    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    # Check that all expected commands are present (order may vary)
    assert "USE SECONDARY ROLES ALL" in sql_commands
    assert "USE ROLE ACCOUNTADMIN" in sql_commands
    assert "ALTER ROLE TEST_ROLE SET COMMENT = $$This comment has been added$$" in sql_commands
    assert "GRANT OWNERSHIP ON ROLE TEST_ROLE TO ROLE USERADMIN COPY CURRENT GRANTS" in sql_commands


def test_resource_is_transferred_to_custom_role_owner(session_ctx, remote_state):
    session_ctx = session_ctx.copy()
    session_ctx["available_roles"].append(ResourceName("test_role"))

    warehouse = res.Warehouse(name="test_warehouse", owner="test_role")
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE SYSADMIN"
    assert sql_commands[2].startswith("CREATE WAREHOUSE TEST_WAREHOUSE")
    assert sql_commands[3] == "GRANT OWNERSHIP ON WAREHOUSE TEST_WAREHOUSE TO ROLE TEST_ROLE COPY CURRENT GRANTS"


def test_resource_cant_be_created(remote_state):
    session_ctx = {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "role": "TEST_ROLE",
        "available_roles": [
            "TEST_ROLE",
        ],
    }
    warehouse = res.Warehouse(name="test_warehouse", owner="test_role")
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    with pytest.raises(MissingPrivilegeException):
        compile_plan_to_sql(session_ctx, plan)


def test_grant_with_grant_admin_custom_role(remote_state):
    session_ctx = {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "role": "GRANT_ADMIN",
        "available_roles": [
            "GRANT_ADMIN",
        ],
    }

    grant = res.RoleGrant(role="GRANT_ADMIN", to_role="SYSADMIN")
    blueprint = Blueprint(resources=[grant])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:role_grant/GRANT_ADMIN?role=SYSADMIN")
    compile_plan_to_sql(session_ctx, plan)


def test_tag_reference_with_tag_admin_custom_role():
    session_ctx = {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "role": "TAG_ADMIN",
        "available_roles": [
            "TAG_ADMIN",
        ],
        "tags": ["tags.tags.cost_center"],
    }

    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
    }

    tag_reference = res.TagReference(
        object_name="SOME_ROLE",
        object_domain="ROLE",
        tags={"tags.tags.cost_center": "finance"},
    )
    blueprint = Blueprint(resources=[tag_reference])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:tag_reference/SOME_ROLE?domain=ROLE")
    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    assert len(sql_commands) == 3
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE TAG_ADMIN"
    assert sql_commands[2] == "ALTER ROLE SOME_ROLE SET TAG tags.tags.cost_center='finance'"


def test_owner_is_database_role(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
        parse_URN("urn::ABCD123:database/SOME_DATABASE"): {"owner": "SYSADMIN"},
        parse_URN("urn::ABCD123:schema/SOME_DATABASE.PUBLIC"): {"owner": "SYSADMIN"},
    }

    def _plan_for_resources(resources):
        blueprint = Blueprint(resources=resources)
        manifest = blueprint.generate_manifest(session_ctx)
        return diff(remote_state, manifest)

    # Test case 1: Specify owner as a string
    database1 = res.Database(name="SOME_DATABASE")
    database_role1 = res.DatabaseRole(
        name="SOME_DATABASE_ROLE",
        database=database1,
    )
    schema1 = res.Schema(
        name="SOME_SCHEMA",
        database=database1,
        owner="SOME_DATABASE.SOME_DATABASE_ROLE",
    )
    plan = _plan_for_resources([database1, database_role1, schema1])
    # Only database_role and schema are new (database matches remote state)
    assert len(plan) == 2

    # Test case 2: Specify owner as a resource
    database2 = res.Database(name="SOME_DATABASE")
    database_role2 = res.DatabaseRole(
        name="SOME_DATABASE_ROLE",
        database=database2,
    )
    schema2 = res.Schema(
        name="SOME_SCHEMA",
        database=database2,
        owner=database_role2,
    )
    plan = _plan_for_resources([database2, database_role2, schema2])
    assert len(plan) == 2


def test_blueprint_create_resource_with_database_role_owner(session_ctx, remote_state):

    database = res.Database(name="SOME_DATABASE")
    database_role = res.DatabaseRole(
        name="SOME_DATABASE_ROLE",
        database=database,
        owner="SYSADMIN",
    )
    schema = res.Schema(name="test_schema", database=database, owner=database_role)
    blueprint = Blueprint(resources=[database, database_role, schema])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 3
    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    # Check expected commands are present (order may vary due to non-deterministic set iteration)
    assert "USE SECONDARY ROLES ALL" in sql_commands
    assert "USE ROLE SYSADMIN" in sql_commands
    assert any(cmd.startswith("CREATE DATABASE SOME_DATABASE") for cmd in sql_commands)
    assert "CREATE DATABASE ROLE SOME_DATABASE.SOME_DATABASE_ROLE" in sql_commands
    assert any(cmd.startswith("CREATE SCHEMA SOME_DATABASE.TEST_SCHEMA") for cmd in sql_commands)
    assert (
        "GRANT OWNERSHIP ON SCHEMA SOME_DATABASE.TEST_SCHEMA TO DATABASE ROLE SOME_DATABASE.SOME_DATABASE_ROLE COPY CURRENT GRANTS"
        in sql_commands
    )


def test_database_with_custom_owner_modifies_public_schema_owner(session_ctx, remote_state):
    role = res.Role(name="CUSTOM_ROLE")
    role_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    database = res.Database(name="SOME_DATABASE", owner=role)
    blueprint = Blueprint(resources=[database, role, role_grant])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 3
    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))
    # Check expected commands are present (order may vary)
    assert "USE SECONDARY ROLES ALL" in sql_commands
    assert "CREATE ROLE CUSTOM_ROLE" in sql_commands
    assert "GRANT ROLE CUSTOM_ROLE TO ROLE SYSADMIN" in sql_commands
    assert any(cmd.startswith("CREATE DATABASE SOME_DATABASE") for cmd in sql_commands)
    assert "GRANT OWNERSHIP ON DATABASE SOME_DATABASE TO ROLE CUSTOM_ROLE COPY CURRENT GRANTS" in sql_commands
    assert "GRANT OWNERSHIP ON SCHEMA SOME_DATABASE.PUBLIC TO ROLE CUSTOM_ROLE COPY CURRENT GRANTS" in sql_commands
