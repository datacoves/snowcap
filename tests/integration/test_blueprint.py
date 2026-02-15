import os

import pytest
import snowflake.connector

from tests.helpers import flatten_sql_commands, safe_fetch
from snowcap import data_provider
from snowcap import resources as res
from snowcap.blueprint import (
    Blueprint,
    CreateResource,
    DropResource,
    MissingResourceException,
    UpdateResource,
    compile_plan_to_sql,
)
from snowcap.client import reset_cache
from snowcap.enums import BlueprintScope, ResourceType
from snowcap.exceptions import NotADAGException
from snowcap.gitops import collect_blueprint_config
from snowcap.resources.database import public_schema_urn

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

pytestmark = pytest.mark.requires_snowflake


@pytest.fixture(autouse=True)
def clear_cache():
    reset_cache()
    yield


@pytest.fixture(scope="session")
def user(suffix, cursor, marked_for_cleanup):
    user = res.User(name=f"TEST_USER_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    return user


@pytest.fixture(scope="session")
def role(suffix, cursor, marked_for_cleanup):
    role = res.Role(name=f"TEST_ROLE_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(role.create_sql())
    marked_for_cleanup.append(role)
    return role


@pytest.fixture(scope="session")
def noprivs_role(cursor, test_db, marked_for_cleanup):
    role = res.Role(name="NOPRIVS")
    cursor.execute(role.create_sql(if_not_exists=True))
    cursor.execute(f"GRANT ROLE NOPRIVS TO USER {cursor.connection.user}")
    cursor.execute(f"GRANT USAGE ON DATABASE {test_db} TO ROLE NOPRIVS")
    cursor.execute(f"GRANT USAGE ON SCHEMA {test_db}.PUBLIC TO ROLE NOPRIVS")
    marked_for_cleanup.append(role)
    return role.name


def test_plan(cursor, user, role):
    session = cursor.connection
    blueprint = Blueprint(name="test")
    role_grant = res.RoleGrant(role=role, to_user=user)
    blueprint.add(role_grant)
    changes = blueprint.plan(session)
    assert len(changes) == 1
    blueprint.apply(session, changes)
    role_grant_remote = data_provider.fetch_role_grant(session, role_grant.fqn)
    assert role_grant_remote


def test_blueprint_plan_no_changes(cursor, user, role):
    session = cursor.connection

    def _blueprint():
        blueprint = Blueprint(name="test_no_changes")
        # Assuming role_grant already exists in the setup for this test
        role_grant = res.RoleGrant(role=role, to_user=user)
        blueprint.add(role_grant)
        return blueprint

    bp = _blueprint()
    # Apply the initial blueprint to ensure the state is as expected
    initial_changes = bp.plan(session)
    bp.apply(session, initial_changes)

    # Plan again to verify no changes are detected
    bp = _blueprint()
    subsequent_changes = bp.plan(session)
    assert len(subsequent_changes) == 0, "Expected no changes in the blueprint plan but found some."


def test_blueprint_zero_drift_after_apply(cursor, test_db, suffix, marked_for_cleanup):
    session = cursor.connection
    blueprint = Blueprint(name="test_zero_drift_after_apply")
    schema = res.Schema(name=f"zero_drift_schema_{suffix}", database=test_db, owner=TEST_ROLE)
    tbl = res.Table(
        name=f"zero_drift_table_{suffix}",
        database=test_db,
        schema=schema,
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
        owner=TEST_ROLE,
    )
    marked_for_cleanup.append(schema)
    blueprint.add(schema, tbl)
    initial_plan = blueprint.plan(session)
    assert len(initial_plan) == 2
    blueprint.apply(session, initial_plan)

    # Plan again to verify no changes are detected
    reset_cache()
    blueprint = Blueprint(name="test_zero_drift_after_apply")
    schema = res.Schema(name=f"zero_drift_schema_{suffix}", database=test_db, owner=TEST_ROLE)
    tbl = res.Table(
        name=f"zero_drift_table_{suffix}",
        database=test_db,
        schema=schema,
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
        owner=TEST_ROLE,
    )
    blueprint.add(schema, tbl)
    subsequent_changes = blueprint.plan(session)
    assert len(subsequent_changes) == 0, "Expected no changes in the blueprint plan but found some."


def test_blueprint_modify_resource(cursor, suffix, marked_for_cleanup):
    cursor.execute(f"CREATE WAREHOUSE modify_me_{suffix}")
    session = cursor.connection
    blueprint = Blueprint(name="test_remove_resource")
    warehouse = res.Warehouse(
        name=f"modify_me_{suffix}",
        auto_suspend=60,
        owner=TEST_ROLE,
    )
    marked_for_cleanup.append(warehouse)
    blueprint.add(warehouse)
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert isinstance(plan[0], UpdateResource)
    assert plan[0].urn.fqn.name == f"MODIFY_ME_{suffix}"
    assert plan[0].delta == {"auto_suspend": 60}

    blueprint.apply(session, plan)


def test_blueprint_name_equivalence_drift(cursor, suffix, marked_for_cleanup):
    """
    Test that login_name comparison is case-insensitive.

    login_name='TEST_USER' and login_name='test_user' should be equivalent and not cause drift.
    """
    # Create user
    user_name = f"TEST_USER_{suffix}_NAME_EQUIVALENCE".upper()
    user = res.User(name=user_name, login_name=user_name, owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql(if_not_exists=True))
    marked_for_cleanup.append(user)

    session = cursor.connection
    blueprint = Blueprint(name="test_name_equivalence_drift")
    # Use lowercase login_name - should be equivalent to uppercase
    # Must also specify default_secondary_roles and type to match what Snowflake returns
    blueprint.add(
        res.User(
            name=user_name,
            login_name=user_name.lower(),
            owner="ACCOUNTADMIN",
            default_secondary_roles=["ALL"],
            type="PERSON",
        )
    )
    plan = blueprint.plan(session)

    assert len(plan) == 0, f"Expected no changes in the blueprint plan but found: {plan}"


def test_blueprint_plan_sql(cursor, user):
    session = cursor.connection

    blueprint = Blueprint(name="test_add_database")
    somedb = res.Database(name="this_database_does_not_exist")
    blueprint.add(somedb)
    plan = blueprint.plan(session)

    session_ctx = data_provider.fetch_session(session)

    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))

    # The plan includes CREATE DATABASE followed by ownership grants
    assert "USE SECONDARY ROLES ALL" in sql_commands
    assert any("CREATE DATABASE THIS_DATABASE_DOES_NOT_EXIST" in cmd for cmd in sql_commands)

    blueprint = Blueprint(name="test_modify_user")
    modified_user = res.User(name=user.name, owner=user.owner, display_name="new_display_name")
    blueprint.add(modified_user)
    plan = blueprint.plan(session)

    sql_commands = flatten_sql_commands(compile_plan_to_sql(session_ctx, plan))

    assert "USE SECONDARY ROLES ALL" in sql_commands
    # There should be some user alter command (the exact format may vary)
    assert any(f"ALTER USER {user.name}" in cmd for cmd in sql_commands)


def test_blueprint_missing_resource_pointer(cursor):
    session = cursor.connection
    grant = res.Grant.from_sql("GRANT ALL ON WAREHOUSE missing_wh TO ROLE SOMEROLE")
    blueprint = Blueprint(name="blueprint", resources=[grant])
    with pytest.raises(MissingResourceException):
        blueprint.plan(session)


def test_blueprint_present_resource_pointer(cursor):
    session = cursor.connection
    grant = res.Grant.from_sql("GRANT AUDIT ON ACCOUNT TO ROLE THISROLEDOESNTEXIST")
    role = res.Role(name="THISROLEDOESNTEXIST")
    blueprint = Blueprint(name="blueprint", resources=[grant, role])
    plan = blueprint.plan(session)
    assert len(plan) == 2


def test_blueprint_missing_database_inferred_from_session_context(cursor):
    session = cursor.connection
    func = res.JavascriptUDF(name="func", args=[], returns="INT", as_="return 1;", schema="public")
    blueprint = Blueprint(name="blueprint", resources=[func])
    blueprint.plan(session)


def test_blueprint_all_grant_triggers_create(cursor, test_db, role):
    cursor.execute(f"GRANT USAGE ON DATABASE {test_db} TO ROLE {role.name}")
    session = cursor.connection
    all_grant = res.Grant(priv="ALL", on_database=test_db, to=role, owner=TEST_ROLE)
    blueprint = Blueprint(name="blueprint", resources=[all_grant])
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)


def test_blueprint_sync_dont_remove_system_schemas(cursor, suffix):
    session = cursor.connection
    db_name = f"BLUEPRINT_SYNC_DONT_REMOVE_SYSTEM_SCHEMAS_{suffix}"
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        blueprint = Blueprint(
            name="blueprint",
            resources=[],
            sync_resources=[ResourceType.SCHEMA],
            scope="DATABASE",
            database=db_name,
        )
        plan = blueprint.plan(session)
        assert len(plan) == 0
    finally:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")


def test_blueprint_sync_resource_missing_from_remote_state(cursor, suffix):
    """
    Test that sync_resources creates missing schemas.

    Note: This test was simplified to not include INFORMATION_SCHEMA, as that
    is a system schema that cannot be user-managed.
    """
    session = cursor.connection
    db_name = f"BLUEPRINT_SYNC_RESOURCE_MISSING_{suffix}"
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        blueprint = Blueprint(
            name="blueprint",
            resources=[
                res.Schema(name="ABSENT", database=db_name),
            ],
            sync_resources=[ResourceType.SCHEMA],
            scope="DATABASE",
            database=db_name,
        )
        plan = blueprint.plan(session)
        # Should only create ABSENT (PUBLIC and INFORMATION_SCHEMA are system schemas and ignored)
        assert len(plan) == 1
        assert isinstance(plan[0], CreateResource)
        assert plan[0].urn.fqn.name == "ABSENT"
    finally:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")


def test_blueprint_sync_plan_matches_remote_state(cursor, suffix):
    session = cursor.connection
    db_name = f"BLUEPRINT_SYNC_PLAN_MATCHES_REMOTE_STATE_{suffix}"
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        cursor.execute(f"CREATE SCHEMA {db_name}.PRESENT")
        blueprint = Blueprint(
            name="blueprint",
            resources=[
                res.Schema(name="PRESENT", owner=TEST_ROLE),
            ],
            sync_resources=[ResourceType.SCHEMA],
            scope="DATABASE",
            database=db_name,
        )
        plan = blueprint.plan(session)
        assert len(plan) == 0
    finally:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")


def test_blueprint_sync_remote_state_contains_extra_resource(cursor, suffix):
    """
    Test that sync_resources drops schemas not in the blueprint.

    Note: This test was simplified to not include INFORMATION_SCHEMA in the blueprint,
    as that is a system schema that cannot be user-managed. Instead, we test with
    a real user schema (KEEP) and verify the extra schema (EXTRA) is dropped.
    """
    session = cursor.connection
    db_name = f"BLUEPRINT_SYNC_REMOTE_STATE_CONTAINS_EXTRA_RESOURCE_{suffix}"
    try:
        cursor.execute(f"CREATE DATABASE {db_name}")
        cursor.execute(f"CREATE SCHEMA {db_name}.KEEP")
        cursor.execute(f"CREATE SCHEMA {db_name}.EXTRA")
        blueprint = Blueprint(
            name="blueprint",
            resources=[res.Schema(name="KEEP", database=db_name, owner=TEST_ROLE)],
            sync_resources=[ResourceType.SCHEMA],
            scope="DATABASE",
            database=db_name,
        )
        plan = blueprint.plan(session)
        # Should drop EXTRA (but not PUBLIC or INFORMATION_SCHEMA as they're system schemas)
        assert len(plan) == 1
        assert isinstance(plan[0], DropResource)
        assert plan[0].urn.fqn.name == "EXTRA"
    finally:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")


def test_blueprint_quoted_references(cursor):
    session = cursor.connection
    try:
        cursor.execute('CREATE USER IF NOT EXISTS "info@applytitan.com"')
        cursor.execute('GRANT ROLE STATIC_ROLE TO USER "info@applytitan.com"')

        blueprint = Blueprint(
            name="test_quoted_references",
            resources=[res.RoleGrant(role="STATIC_ROLE", to_user="info@applytitan.com")],
        )
        plan = blueprint.plan(session)
        assert len(plan) == 0
    finally:
        cursor.execute('DROP USER IF EXISTS "info@applytitan.com"')


def test_blueprint_grant_with_lowercase_priv_drift(cursor, suffix, marked_for_cleanup):
    """
    Test that grants with lowercase privilege names work correctly.

    This tests that 'usage' (lowercase) is handled the same as 'USAGE' (uppercase).
    """
    session = cursor.connection

    role = res.Role(name=f"TITAN_TEST_ROLE_{suffix}")
    warehouse = res.Warehouse(
        name=f"TITAN_TEST_WAREHOUSE_{suffix}",
        warehouse_size="xsmall",
        auto_suspend=60,
    )
    grant = res.Grant(priv="usage", to=role, on=warehouse)
    marked_for_cleanup.append(role)
    marked_for_cleanup.append(warehouse)

    bp = Blueprint()
    bp.add(role, warehouse, grant)
    plan = bp.plan(session)
    assert len(plan) == 3

    # Apply must be called on the same blueprint that generated the plan
    # because apply() relies on self._levels which is populated during plan()
    bp.apply(session, plan)

    # Verify by creating a new blueprint and checking for drift
    reset_cache()
    bp2 = Blueprint()
    bp2.add(
        res.Role(name=f"TITAN_TEST_ROLE_{suffix}"),
        res.Warehouse(name=f"TITAN_TEST_WAREHOUSE_{suffix}", warehouse_size="xsmall", auto_suspend=60),
        res.Grant(priv="usage", to=role, on=warehouse),
    )
    plan2 = bp2.plan(session)
    assert len(plan2) == 0, f"Expected no drift but got: {plan2}"


def test_blueprint_quoted_identifier_drift(cursor, test_db, suffix):
    session = cursor.connection

    cursor.execute(f'CREATE SCHEMA {test_db}."multiCaseString_{suffix}"')

    blueprint = Blueprint(
        resources=[res.Schema(name=f'"multiCaseString_{suffix}"', database=test_db, owner=TEST_ROLE)],
    )
    plan = blueprint.plan(session)
    cursor.execute(f'DROP SCHEMA {test_db}."multiCaseString_{suffix}"')

    assert len(plan) == 0


def test_blueprint_grant_role_to_public(cursor, suffix, marked_for_cleanup):
    session = cursor.connection

    role_name = f"role{suffix}_grant_role_to_public"
    role = res.Role(name=role_name)
    marked_for_cleanup.append(role)
    grant = res.RoleGrant(role=role, to_role="PUBLIC")
    blueprint = Blueprint(resources=[role, grant])
    blueprint.apply(session)
    role_data = safe_fetch(cursor, role.urn)
    assert role_data is not None
    assert role_data["name"] == role.name

    grant_data = safe_fetch(cursor, grant.urn)
    assert grant_data is not None
    assert grant_data["role"] == role_name
    assert grant_data["to_role"] == "PUBLIC"


def test_blueprint_account_grants(cursor, suffix, marked_for_cleanup):
    session = cursor.connection

    role_name = f"ROLE{suffix}_ACCOUNT_GRANTS"
    role = res.Role(name=role_name)
    marked_for_cleanup.append(role)
    grant = res.Grant(priv="CREATE DATABASE", on="ACCOUNT", to=role)
    blueprint = Blueprint(resources=[role, grant])
    blueprint.apply(session)
    role_data = safe_fetch(cursor, role.urn)
    assert role_data is not None
    assert role_data["name"] == role.name

    grant_data = safe_fetch(cursor, grant.urn)
    assert grant_data is not None
    assert grant_data["to"] == role_name
    assert grant_data["priv"] == "CREATE DATABASE"
    assert grant_data["on"] == "ACCOUNT"
    assert grant_data["on_type"] == "ACCOUNT"


def test_blueprint_create_resource_with_database_role_owner(cursor, suffix, test_db):
    session = cursor.connection

    database_role = res.DatabaseRole(
        name=f"TEST_BLUEPRINT_CREATE_RESOURCE_WITH_DATABASE_ROLE_OWNER_{suffix}",
        database=test_db,
        owner=TEST_ROLE,
    )
    schema = res.Schema(
        name="test_schema",
        database=test_db,
        owner=database_role,
    )
    blueprint = Blueprint(resources=[database_role, schema])
    plan = blueprint.plan(session)
    assert len(plan) == 2

    blueprint.apply(session, plan)

    schema_data = safe_fetch(cursor, schema.urn)
    assert schema_data is not None
    assert schema_data["name"] == schema.name
    assert schema_data["owner"] == str(database_role.fqn)


def test_blueprint_database_params_passed_to_public_schema(cursor, suffix):
    session = cursor.connection

    db_name = f"test_db_params_passed_to_public_schema_{suffix}"

    def _database():
        return res.Database(
            name=db_name,
            data_retention_time_in_days=1,
            max_data_extension_time_in_days=2,
            default_ddl_collation="en_US",
        )

    try:
        database = _database()
        blueprint = Blueprint(resources=[database])
        plan = blueprint.plan(session)
        assert len(plan) == 1
        blueprint.apply(session, plan)
        schema_data = safe_fetch(cursor, public_schema_urn(database.urn))
        assert schema_data is not None
        assert schema_data["data_retention_time_in_days"] == 1
        assert schema_data["max_data_extension_time_in_days"] == 2
        assert schema_data["default_ddl_collation"] == "en_US"
        database = _database()
        blueprint = Blueprint(resources=[database])
        plan = blueprint.plan(session)
        assert len(plan) == 0
    finally:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")


def test_blueprint_account_parameters_sync_drift(cursor):
    cursor.execute("ALTER ACCOUNT SET PREVENT_UNLOAD_TO_INLINE_URL = TRUE")
    session = cursor.connection
    try:
        blueprint = Blueprint(
            name="test_account_parameters_sync_drift",
            sync_resources=[ResourceType.ACCOUNT_PARAMETER],
        )
        plan = blueprint.plan(session)
        assert len(plan) > 0
        max_concurrency_level = next((r for r in plan if r.urn.fqn.name == "PREVENT_UNLOAD_TO_INLINE_URL"), None)
        assert max_concurrency_level is not None
        assert isinstance(max_concurrency_level, DropResource)
    finally:
        cursor.execute("ALTER ACCOUNT UNSET PREVENT_UNLOAD_TO_INLINE_URL")


def test_blueprint_scope_missing_resource(cursor):
    session = cursor.connection
    blueprint = Blueprint(scope="DATABASE", database="THIS_DATABASE_DOES_NOT_EXIST")
    with pytest.raises(MissingResourceException):
        blueprint.plan(session)


def test_blueprint_single_schema_example(cursor, suffix):
    session = cursor.connection
    cursor.execute(f"CREATE SCHEMA STATIC_DATABASE.DEV_{suffix}")
    yaml_config = {
        "scope": "SCHEMA",
        "database": "STATIC_DATABASE",
        "tables": [
            {
                "name": "my_table",
                "columns": [
                    {"name": "my_column", "data_type": "string"},
                ],
            },
        ],
        "views": [
            {
                "name": "my_view",
                "as_": "SELECT * FROM my_table",
                "requires": [
                    {"name": "my_table", "resource_type": "TABLE"},
                ],
            },
        ],
    }
    cli_config = {"schema": f"DEV_{suffix}"}
    try:
        bc = collect_blueprint_config(yaml_config, cli_config)
        blueprint = Blueprint.from_config(bc)
        assert blueprint._config.scope == BlueprintScope.SCHEMA
        plan = blueprint.plan(session)
        assert len(plan) == 2
        # Check both resources are CreateResource and have the expected names
        resource_names = {str(p.urn.fqn.name).upper() for p in plan}
        assert resource_names == {"MY_TABLE", "MY_VIEW"}
        assert all(isinstance(p, CreateResource) for p in plan)
    finally:
        cursor.execute(f"DROP SCHEMA STATIC_DATABASE.DEV_{suffix}")


# test_blueprint_split_role_user removed - requires MFA bypass (account policy cannot be changed in tests)
# See tests/SKIPPED_TESTS.md for details


def test_blueprint_share_custom_owner(cursor, suffix):
    """
    Test creating a share with a custom owner.

    Uses ACCOUNTADMIN as the owner since it exists in all accounts.
    """
    session = cursor.connection
    share_name = f"TEST_SHARE_CUSTOM_OWNER_{suffix}"
    share = res.Share(name=share_name, owner="ACCOUNTADMIN")

    try:
        blueprint = Blueprint(resources=[share])
        plan = blueprint.plan(session)
        assert len(plan) == 1
        assert isinstance(plan[0], CreateResource)
        assert plan[0].urn.fqn.name == share_name
        blueprint.apply(session, plan)
    finally:
        cursor.execute(f"DROP SHARE IF EXISTS {share_name}")


def test_stage_read_write_privilege_execution_order(cursor, suffix, marked_for_cleanup):
    """
    Test that cycle detection works for stage READ/WRITE grants.

    Stage grants require WRITE before READ (Snowflake requirement).
    If a user explicitly sets read_grant.requires(write_grant), this creates
    an invalid cycle and should raise NotADAGException.
    """
    session = cursor.connection

    role_name = f"STAGE_ACCESS_ROLE_{suffix}"

    blueprint = Blueprint()

    role = res.Role(name=role_name)
    read_grant = res.Grant(priv="READ", on_stage="STATIC_DATABASE.PUBLIC.STATIC_STAGE", to=role)
    write_grant = res.Grant(priv="WRITE", on_stage="STATIC_DATABASE.PUBLIC.STATIC_STAGE", to=role)

    # Incorrect order of execution - this creates a cycle
    read_grant.requires(write_grant)

    blueprint.add(role, read_grant, write_grant)

    marked_for_cleanup.append(role)

    with pytest.raises(NotADAGException):
        blueprint.plan(session)

    # Second test: Without explicit requires, grants should work
    blueprint = Blueprint()

    role = res.Role(name=role_name)
    read_grant = res.Grant(priv="READ", on_stage="STATIC_DATABASE.PUBLIC.STATIC_STAGE", to=role)
    write_grant = res.Grant(priv="WRITE", on_stage="STATIC_DATABASE.PUBLIC.STATIC_STAGE", to=role)

    blueprint.add(role, write_grant, read_grant)

    plan = blueprint.plan(session)
    assert len(plan) == 3
    blueprint.apply(session, plan)


def test_grant_database_role_to_database_role(cursor, suffix, marked_for_cleanup):
    session = cursor.connection
    bp = Blueprint()

    parent = res.DatabaseRole(name=f"DBR2DBR_PARENT_{suffix}", database="STATIC_DATABASE")
    child1 = res.DatabaseRole(name=f"DBR2DBR_CHILD_1_{suffix}", database="STATIC_DATABASE")
    child2 = res.DatabaseRole(name=f"DBR2DBR_CHILD_2_{suffix}", database="STATIC_DATABASE")
    drg1 = res.DatabaseRoleGrant(database_role=child1, to_database_role=parent)
    drg2 = res.DatabaseRoleGrant(database_role=child2, to_database_role=parent)

    marked_for_cleanup.append(parent)
    marked_for_cleanup.append(child1)
    marked_for_cleanup.append(child2)

    bp.add(parent, child1, child2, drg1, drg2)
    plan = bp.plan(session)
    assert len(plan) == 5
    bp.apply(session, plan)

    grant1 = safe_fetch(cursor, res.DatabaseRoleGrant(database_role=child1, to_database_role=parent).urn)
    assert grant1 is not None
    assert grant1["database_role"] == str(child1.fqn)
    assert grant1["to_database_role"] == str(parent.fqn)

    grant2 = safe_fetch(cursor, res.DatabaseRoleGrant(database_role=child2, to_database_role=parent).urn)
    assert grant2 is not None
    assert grant2["database_role"] == str(child2.fqn)
    assert grant2["to_database_role"] == str(parent.fqn)
