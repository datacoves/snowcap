import pytest

from snowcap import resources as res
from snowcap.enums import GrantType, ResourceType
from snowcap.privs import all_privs_for_resource_type
from snowcap.identifiers import URN
from snowcap.resource_name import ResourceName
from snowcap.resources.resource import ResourcePointer


def test_grant_global_priv():
    grant = res.Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")
    assert grant.priv == "CREATE WAREHOUSE"
    assert grant.on == "ACCOUNT"
    assert grant.to.name == "somerole"
    assert (
        str(URN.from_resource(grant)) == "urn:::grant/GRANT?grant_type=OBJECT&priv=CREATE WAREHOUSE&on=account/ACCOUNT&to=role/SOMEROLE"
    )
    assert grant.create_sql() == "GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE SOMEROLE"


def test_grant_account_obj_priv():
    """Test grant creation with resource object AND kwarg patterns."""
    expected_urn = "urn:::grant/GRANT?grant_type=OBJECT&priv=MODIFY&on=warehouse/SOMEWH&to=role/SOMEROLE"

    # Pattern 1: With resource object
    wh = res.Warehouse(name="somewh")
    grant_with_resource = res.Grant(priv="MODIFY", on=wh, to="somerole")
    assert grant_with_resource.priv == "MODIFY"
    assert grant_with_resource.on == "SOMEWH"
    assert grant_with_resource.on_type == ResourceType.WAREHOUSE
    assert grant_with_resource.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant_with_resource)) == expected_urn

    # Pattern 2: With kwarg
    grant_with_kwarg = res.Grant(priv="MODIFY", on_warehouse="somewh", to="somerole")
    assert grant_with_kwarg.priv == "MODIFY"
    assert grant_with_kwarg.on == "SOMEWH"
    assert grant_with_kwarg.on_type == ResourceType.WAREHOUSE
    assert grant_with_kwarg.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant_with_kwarg)) == expected_urn


def test_grant_schema_priv():
    """Test schema privilege grant creation with resource object AND kwarg patterns."""
    expected_urn = "urn:::grant/GRANT?grant_type=OBJECT&priv=CREATE VIEW&on=schema/SOMESCHEMA&to=role/SOMEROLE"

    # Pattern 1: With resource object
    sch = res.Schema(name="someschema")
    grant_with_resource = res.Grant(priv="CREATE VIEW", on=sch, to="somerole")
    assert grant_with_resource.priv == "CREATE VIEW"
    assert grant_with_resource.on == "SOMESCHEMA"
    assert grant_with_resource.on_type == ResourceType.SCHEMA
    assert grant_with_resource.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant_with_resource)) == expected_urn

    # Pattern 2: With kwarg
    grant_with_kwarg = res.Grant(priv="CREATE VIEW", on_schema="someschema", to="somerole")
    assert grant_with_kwarg.priv == "CREATE VIEW"
    assert grant_with_kwarg.on == "SOMESCHEMA"
    assert grant_with_kwarg.on_type == ResourceType.SCHEMA
    assert grant_with_kwarg.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant_with_kwarg)) == expected_urn


def test_grant_all():
    grant = res.Grant(priv="ALL", on_warehouse="somewh", to="somerole")
    assert grant.priv == "ALL"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert grant._data._privs == all_privs_for_resource_type(ResourceType.WAREHOUSE)
    assert str(URN.from_resource(grant)) == "urn:::grant/GRANT?grant_type=OBJECT&priv=ALL&on=warehouse/SOMEWH&to=role/SOMEROLE"


def test_role_grant_to_user():
    """Test role grant to user creation with kwarg AND resource object patterns."""
    expected_urn = "urn:::role_grant/SOMEROLE?user=SOMEUSER"

    # Pattern 1: With kwarg
    grant_with_kwarg = res.RoleGrant(role="somerole", to_user="someuser")
    assert grant_with_kwarg.role.name == "somerole"
    assert grant_with_kwarg._data.to_user is not None
    assert grant_with_kwarg.to.name == "someuser"
    assert str(URN.from_resource(grant_with_kwarg)) == expected_urn

    # Pattern 2: With resource object
    grant_with_resource = res.RoleGrant(role="somerole", to=res.User(name="someuser"))
    assert grant_with_resource.role.name == "somerole"
    assert grant_with_resource._data.to_user is not None
    assert grant_with_resource.to.name == "someuser"
    assert str(URN.from_resource(grant_with_resource)) == expected_urn


def test_role_grant_to_role():
    """Test role grant to role creation with kwarg AND resource object patterns."""
    expected_urn = "urn:::role_grant/SOMEROLE?role=SOMEOTHERROLE"

    # Pattern 1: With kwarg
    grant_with_kwarg = res.RoleGrant(role="somerole", to_role="someotherrole")
    assert grant_with_kwarg.role.name == "somerole"
    assert grant_with_kwarg._data.to_role is not None
    assert grant_with_kwarg.to.name == "someotherrole"
    assert str(URN.from_resource(grant_with_kwarg)) == expected_urn

    # Pattern 2: With resource object
    grant_with_resource = res.RoleGrant(role="somerole", to=res.Role(name="someotherrole"))
    assert grant_with_resource.role.name == "somerole"
    assert grant_with_resource._data.to_role is not None
    assert grant_with_resource.to.name == "someotherrole"
    assert str(URN.from_resource(grant_with_resource)) == expected_urn


def test_grant_redirect_to_all():
    with pytest.raises(ValueError):
        res.Grant(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")


def test_grant_priv_is_serialized_uppercase():
    grant = res.Grant(priv="usage", on_warehouse="somewh", to="somerole")
    assert grant.priv == "USAGE"


def test_grant_on_accepts_resource_name():
    wh = res.Warehouse(name="somewh")
    assert isinstance(wh.name, ResourceName)
    grant = res.Grant(priv="usage", on_warehouse=wh.name, to="somerole")
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE


def test_grant_on_dynamic_tables():
    grant = res.Grant(
        priv="SELECT",
        on_dynamic_table="somedb.someschema.sometable",
        to="somerole",
    )
    assert grant._data.on == "SOMEDB.SOMESCHEMA.SOMETABLE"
    assert grant._data.on_type == ResourceType.DYNAMIC_TABLE

    dynamic_table = ResourcePointer(name="sometable", resource_type=ResourceType.DYNAMIC_TABLE)
    grant = res.Grant(
        priv="SELECT",
        on=dynamic_table,
        to="somerole",
    )
    assert grant._data.on == "SOMETABLE"
    assert grant._data.on_type == ResourceType.DYNAMIC_TABLE


def test_grant_on_cortex_search_service():
    """USAGE/MONITOR on a CORTEX SEARCH SERVICE parses and renders correctly.

    Cortex Search is a schema-scoped service. Grants like
        GRANT USAGE ON CORTEX SEARCH SERVICE <db>.<schema>.<svc> TO ROLE r
    let consuming roles query the service via SNOWFLAKE.CORTEX.SEARCH_PREVIEW;
    MONITOR exposes get_ai_observability_events logs.
    """
    grant = res.Grant(
        priv="USAGE",
        on_cortex_search_service="somedb.someschema.someservice",
        to="somerole",
    )
    assert grant._data.on == "SOMEDB.SOMESCHEMA.SOMESERVICE"
    assert grant._data.on_type == ResourceType.CORTEX_SEARCH_SERVICE
    assert "USAGE ON CORTEX SEARCH SERVICE" in grant.create_sql()

    monitor_grant = res.Grant(
        priv="MONITOR",
        on_cortex_search_service="somedb.someschema.someservice",
        to="somerole",
    )
    assert monitor_grant._data.on_type == ResourceType.CORTEX_SEARCH_SERVICE
    assert "MONITOR ON CORTEX SEARCH SERVICE" in monitor_grant.create_sql()


def test_grant_on_adaptive_warehouse():
    """Grants on an ADAPTIVE warehouse render identically to grants on a STANDARD warehouse."""
    wh = res.Warehouse(name="somewh", warehouse_type="ADAPTIVE", max_query_performance_level="LARGE")

    usage_grant = res.Grant(priv="usage", on=wh, to="somerole")
    assert usage_grant.create_sql() == "GRANT USAGE ON WAREHOUSE SOMEWH TO ROLE SOMEROLE"

    operate_grant = res.Grant(priv="operate", on=wh, to="somerole")
    assert operate_grant.create_sql() == "GRANT OPERATE ON WAREHOUSE SOMEWH TO ROLE SOMEROLE"

    monitor_grant = res.Grant(priv="monitor", on=wh, to="somerole")
    assert monitor_grant.create_sql() == "GRANT MONITOR ON WAREHOUSE SOMEWH TO ROLE SOMEROLE"


def test_grant_database_role_to_database_role():
    database = res.Database(name="somedb")
    parent = res.DatabaseRole(name="parent", database=database)
    child = res.DatabaseRole(name="child", database=database)
    grant = res.DatabaseRoleGrant(database_role=child, to_database_role=parent)
    assert grant.database_role.name == "child"
    assert grant.to.name == "parent"


def test_grant_database_role_to_account_role():
    database = res.Database(name="somedb")
    parent = res.Role(name="parent")
    child = res.DatabaseRole(name="child", database=database)
    grant = res.DatabaseRoleGrant(database_role=child, to_role=parent)
    assert grant.database_role.name == "child"
    assert grant.to.name == "parent"


def test_grant_database_role_to_system_role():
    database = res.Database(name="somedb")
    child = res.DatabaseRole(name="child", database=database)
    grant = res.DatabaseRoleGrant(database_role=child, to_role="SYSADMIN")
    assert grant.database_role.name == "child"
    assert grant.to.name == "SYSADMIN"


# =============================================================================
# Grants TO database roles tests
# =============================================================================


def test_grant_to_database_role_string():
    """Test grant TO a database role using string notation."""
    grant = res.Grant(
        priv="SELECT",
        on_table="somedb.someschema.sometable",
        to="somedb.mydbrole"  # Database role inferred from dot notation
    )
    assert grant.to_type == ResourceType.DATABASE_ROLE
    assert grant.to.name == "MYDBROLE"
    assert "TO DATABASE ROLE SOMEDB.MYDBROLE" in grant.create_sql()


def test_grant_to_database_role_object():
    """Test grant TO a database role using DatabaseRole object."""
    db_role = res.DatabaseRole(name="mydbrole", database="somedb")
    grant = res.Grant(
        priv="SELECT",
        on_table="somedb.someschema.sometable",
        to=db_role
    )
    assert grant.to_type == ResourceType.DATABASE_ROLE
    assert grant.to.name == "MYDBROLE"
    assert "TO DATABASE ROLE SOMEDB.MYDBROLE" in grant.create_sql()


def test_future_grant_to_database_role():
    """Test future grant TO a database role."""
    grant = res.Grant(
        priv="SELECT",
        on="FUTURE TABLES IN SCHEMA somedb.someschema",
        to="somedb.mydbrole"
    )
    assert grant.to_type == ResourceType.DATABASE_ROLE
    assert grant.grant_type == GrantType.FUTURE
    sql = grant.create_sql()
    assert "GRANT SELECT ON FUTURE TABLES IN SCHEMA SOMEDB.SOMESCHEMA TO DATABASE ROLE SOMEDB.MYDBROLE" == sql


def test_future_grant_to_database_role_object():
    """Test future grant TO a database role using DatabaseRole object."""
    db_role = res.DatabaseRole(name="mydbrole", database="somedb")
    grant = res.Grant(
        priv="CREATE VIEW",
        on=["FUTURE", "SCHEMAS", res.Database(name="somedb")],
        to=db_role
    )
    assert grant.to_type == ResourceType.DATABASE_ROLE
    assert grant.grant_type == GrantType.FUTURE
    sql = grant.create_sql()
    assert "GRANT CREATE VIEW ON FUTURE SCHEMAS IN DATABASE SOMEDB TO DATABASE ROLE SOMEDB.MYDBROLE" == sql


# =============================================================================
# Multi-privilege grant tests (US-011)
# =============================================================================


class TestMultiPrivilegeGrants:
    """Tests for grants with multiple privileges (balboa pattern)."""

    def test_multi_priv_creates_separate_grants(self):
        """Test: priv: [USAGE, MONITOR] creates separate grant per privilege."""
        grant = res.Grant(priv=["USAGE", "MONITOR"], on_warehouse="somewh", to="somerole")
        # First privilege is the main grant
        assert grant.priv == "USAGE"
        # rest_of_privs contains the remaining privileges
        assert grant.rest_of_privs == ["MONITOR"]
        # process_shortcuts creates the additional grants
        additional_grants = grant.process_shortcuts()
        assert len(additional_grants) == 1
        assert additional_grants[0].priv == "MONITOR"
        assert additional_grants[0].on == "SOMEWH"
        assert additional_grants[0].to.name == "SOMEROLE"

    def test_multi_priv_on_stage_creates_grants(self):
        """Test: priv: [READ, WRITE] on stage creates 2 grants."""
        grant = res.Grant(priv=["READ", "WRITE"], on_stage="somestage", to="somerole")
        assert grant.priv == "READ"
        assert grant.rest_of_privs == ["WRITE"]
        additional_grants = grant.process_shortcuts()
        assert len(additional_grants) == 1
        assert additional_grants[0].priv == "WRITE"
        assert additional_grants[0].on_type == ResourceType.STAGE

    def test_each_grant_has_correct_privilege(self):
        """Test: Each grant has correct privilege after expansion."""
        grant = res.Grant(priv=["SELECT", "INSERT", "UPDATE"], on_table="sometable", to="somerole")
        assert grant.priv == "SELECT"
        additional_grants = grant.process_shortcuts()
        all_grants = [grant] + additional_grants
        privs = [g.priv for g in all_grants]
        assert privs == ["SELECT", "INSERT", "UPDATE"]
        # All grants target the same table
        for g in all_grants:
            assert g.on == "SOMETABLE"
            assert g.on_type == ResourceType.TABLE

    def test_grant_list_serialization(self):
        """Test: Grant list serialization works correctly."""
        grant = res.Grant(priv=["USAGE", "MONITOR", "OPERATE"], on_warehouse="somewh", to="somerole")
        additional_grants = grant.process_shortcuts()
        all_grants = [grant] + additional_grants
        assert len(all_grants) == 3
        # Verify all grants can generate SQL
        for g in all_grants:
            sql = g.create_sql()
            assert "GRANT" in sql
            assert "SOMEWH" in sql
            assert "SOMEROLE" in sql

    def test_multi_priv_preserves_grant_option(self):
        """Test: Grant option is preserved across all expanded grants."""
        grant = res.Grant(priv=["USAGE", "MONITOR"], on_warehouse="somewh", to="somerole", grant_option=True)
        additional_grants = grant.process_shortcuts()
        all_grants = [grant] + additional_grants
        for g in all_grants:
            assert g._data.grant_option is True

    def test_multi_priv_empty_list_raises_error(self):
        """Test: Empty privilege list raises error."""
        with pytest.raises(ValueError, match="at least one privilege"):
            res.Grant(priv=[], on_warehouse="somewh", to="somerole")

    def test_single_priv_in_list_works(self):
        """Test: Single privilege in list works correctly."""
        grant = res.Grant(priv=["USAGE"], on_warehouse="somewh", to="somerole")
        assert grant.priv == "USAGE"
        assert grant.rest_of_privs == []
        additional_grants = grant.process_shortcuts()
        assert len(additional_grants) == 0

    def test_multi_priv_with_resource_object(self):
        """Test: Multi-privilege grant works with resource object."""
        wh = res.Warehouse(name="somewh")
        grant = res.Grant(priv=["USAGE", "MONITOR"], on=wh, to="somerole")
        assert grant.priv == "USAGE"
        additional_grants = grant.process_shortcuts()
        assert len(additional_grants) == 1
        assert additional_grants[0].priv == "MONITOR"
        assert additional_grants[0].on_type == ResourceType.WAREHOUSE

    def test_multi_priv_on_schema(self):
        """Test: Multi-privilege grant on schema."""
        grant = res.Grant(priv=["CREATE TABLE", "CREATE VIEW"], on_schema="someschema", to="somerole")
        assert grant.priv == "CREATE TABLE"
        additional_grants = grant.process_shortcuts()
        assert len(additional_grants) == 1
        assert additional_grants[0].priv == "CREATE VIEW"
        assert additional_grants[0].on_type == ResourceType.SCHEMA

    def test_multi_priv_from_config_dict(self):
        """Test: Multi-privilege from config dict (as in YAML)."""
        # Simulates what would come from YAML parsing
        config = {
            "priv": ["USAGE", "MONITOR"],
            "on_warehouse": "somewh",
            "to": "somerole",
        }
        grant = res.Grant(**config)
        all_grants = [grant] + grant.process_shortcuts()
        assert len(all_grants) == 2
        assert all_grants[0].priv == "USAGE"
        assert all_grants[1].priv == "MONITOR"


# =============================================================================
# Role grants with roles list tests (US-012)
# =============================================================================


class TestRoleGrantsWithRolesList:
    """Tests for role_grants with roles list (balboa pattern).

    The role_grants section in YAML supports multiple patterns:
    - role: X + to_role: Y (single role to single role)
    - role: X + to_user: Y (single role to single user)
    - role: X + to_roles: [Y, Z] (single role to multiple roles)
    - role: X + to_users: [Y, Z] (single role to multiple users)
    - roles: [X, Y] + to_role: Z (multiple roles to single role)
    - roles: [X, Y] + to_user: Z (multiple roles to single user)
    """

    def test_role_grants_single_role_to_single_role(self):
        """Test: role_grants: with role: X, to_role: Y creates one grant."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {"role": "ANALYST", "to_role": "SYSADMIN"}
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 1
        grant = blueprint_config.resources[0]
        assert grant.role.name == "ANALYST"
        assert grant.to.name == "SYSADMIN"

    def test_role_grants_single_role_to_single_user(self):
        """Test: role_grants: with role: X, to_user: Y creates one grant."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {"role": "ANALYST", "to_user": "john_doe"}
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 1
        grant = blueprint_config.resources[0]
        assert grant.role.name == "ANALYST"
        assert grant.to.name == "john_doe"

    def test_role_grants_roles_list_to_single_role(self):
        """Test: role_grants: with roles: [X, Y, Z] to_role: creates multiple grants."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {
                    "roles": ["ANALYST", "ENGINEER", "DATA_SCIENTIST"],
                    "to_role": "SYSADMIN"
                }
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all grants are to the same target role
        role_names = [grant.role.name for grant in blueprint_config.resources]
        assert "ANALYST" in role_names
        assert "ENGINEER" in role_names
        assert "DATA_SCIENTIST" in role_names

        for grant in blueprint_config.resources:
            assert grant.to.name == "SYSADMIN"

    def test_role_grants_roles_list_to_single_user(self):
        """Test: role_grants: with roles: [X, Y, Z] to_user: creates multiple grants."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {
                    "roles": ["ANALYST", "ENGINEER"],
                    "to_user": "jane_doe"
                }
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        # Verify all grants are to the same target user
        role_names = [grant.role.name for grant in blueprint_config.resources]
        assert "ANALYST" in role_names
        assert "ENGINEER" in role_names

        for grant in blueprint_config.resources:
            assert grant.to.name == "jane_doe"

    def test_role_grants_single_role_to_roles_list(self):
        """Test: role_grants: with role: X, to_roles: [Y, Z] creates multiple grants."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {
                    "role": "ANALYST",
                    "to_roles": ["SYSADMIN", "ACCOUNTADMIN", "SECURITYADMIN"]
                }
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all grants are from the same source role
        for grant in blueprint_config.resources:
            assert grant.role.name == "ANALYST"

        # Verify different target roles
        to_role_names = [grant.to.name for grant in blueprint_config.resources]
        assert "SYSADMIN" in to_role_names
        assert "ACCOUNTADMIN" in to_role_names
        assert "SECURITYADMIN" in to_role_names

    def test_role_grants_single_role_to_users_list(self):
        """Test: role_grants: with role: X, to_users: [Y, Z] creates multiple grants."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {
                    "role": "ANALYST",
                    "to_users": ["john_doe", "jane_doe", "bob_smith"]
                }
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all grants are from the same source role
        for grant in blueprint_config.resources:
            assert grant.role.name == "ANALYST"

        # Verify different target users
        to_user_names = [grant.to.name for grant in blueprint_config.resources]
        assert "john_doe" in to_user_names
        assert "jane_doe" in to_user_names
        assert "bob_smith" in to_user_names

    def test_role_grants_nested_role_hierarchies(self):
        """Test: Nested role hierarchies resolve correctly with multiple role_grants entries."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                # Create hierarchy: DEV_ANALYST -> DEV_ADMIN -> SYSADMIN
                {"role": "DEV_ANALYST", "to_role": "DEV_ADMIN"},
                {"role": "DEV_ADMIN", "to_role": "SYSADMIN"},
                # Also grant DEV_ANALYST to users
                {
                    "role": "DEV_ANALYST",
                    "to_users": ["developer1", "developer2"]
                }
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        # 2 role-to-role grants + 2 role-to-user grants = 4 total
        assert len(blueprint_config.resources) == 4

        # Verify the hierarchy grants exist
        role_to_role_grants = [g for g in blueprint_config.resources if g._data.to_role is not None]
        role_to_user_grants = [g for g in blueprint_config.resources if g._data.to_user is not None]

        assert len(role_to_role_grants) == 2
        assert len(role_to_user_grants) == 2

    def test_role_grants_empty_roles_list_raises_error(self):
        """Test: Empty roles list raises error."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                {
                    "roles": [],
                    "to_role": "SYSADMIN"
                }
            ]
        }
        with pytest.raises(ValueError, match="No role grants found"):
            collect_blueprint_config(config)

    def test_role_grants_mixed_patterns_in_same_config(self):
        """Test: Multiple role_grants entries with different patterns work together."""
        from snowcap.gitops import collect_blueprint_config

        config = {
            "role_grants": [
                # Pattern 1: single role to single role
                {"role": "DEV", "to_role": "SYSADMIN"},
                # Pattern 2: multiple roles to single role
                {"roles": ["ANALYST", "ENGINEER"], "to_role": "ACCOUNTADMIN"},
                # Pattern 3: single role to multiple users
                {"role": "READER", "to_users": ["user1", "user2"]},
            ]
        }
        blueprint_config = collect_blueprint_config(config)
        # 1 + 2 + 2 = 5 grants total
        assert len(blueprint_config.resources) == 5


class TestGrantOnList:
    """Tests for `on:` list expansion (multiple grants from one entry)."""

    def test_on_list_of_plain_objects_expands_to_multiple_grants(self):
        """Test: on: [warehouse FOO, warehouse BAR] expands to 2 grants."""
        grant = res.Grant(
            priv="USAGE",
            on=["warehouse FOO", "warehouse BAR"],
            to="somerole",
        )
        assert grant.priv == "USAGE"
        assert grant.on == "FOO"
        assert grant.on_type == ResourceType.WAREHOUSE
        assert grant.rest_of_ons == ["warehouse BAR"]
        additional = grant.process_shortcuts()
        assert len(additional) == 1
        assert additional[0].on == "BAR"
        assert additional[0].on_type == ResourceType.WAREHOUSE

    def test_on_list_of_mixed_resource_types(self):
        """Test: on: [warehouse FOO, database BAR] expands to 2 grants of different types."""
        grant = res.Grant(
            priv="USAGE",
            on=["warehouse FOO", "database BAR"],
            to="somerole",
        )
        assert grant.on == "FOO"
        assert grant.on_type == ResourceType.WAREHOUSE
        additional = grant.process_shortcuts()
        assert len(additional) == 1
        assert additional[0].on == "BAR"
        assert additional[0].on_type == ResourceType.DATABASE

    def test_on_list_of_multiword_types(self):
        """Test: on: list of multi-word resource types (git repository) expands."""
        grant = res.Grant(
            priv="READ",
            on=["git repository D.S.A", "git repository D.S.B"],
            to="somerole",
        )
        assert grant.on_type == ResourceType.GIT_REPOSITORY
        additional = grant.process_shortcuts()
        assert len(additional) == 1
        assert additional[0].on_type == ResourceType.GIT_REPOSITORY
        assert additional[0].on == "D.S.B"

    def test_on_list_4_element_grant_type_form_unchanged(self):
        """Test: existing on: [FUTURE/ALL, items, object_type, object] form still
        parses as a single grant (not expanded into multiple)."""
        grant = res.Grant(
            priv="SELECT",
            on=["FUTURE", "TABLES", "SCHEMA", "D.S"],
            to="somerole",
        )
        assert grant.rest_of_ons == []
        assert grant.on == "D.S"
        assert grant.on_type == ResourceType.SCHEMA

    def test_on_list_of_all_future_grants_still_expands(self):
        """Test: on: [all schemas in DB X, future schemas in DB X] still expands
        (existing behavior preserved)."""
        grant = res.Grant(
            priv="USAGE",
            on=["all schemas in database raw_prd", "future schemas in database raw_prd"],
            to="somerole",
        )
        assert grant.rest_of_ons == ["future schemas in database raw_prd"]
        additional = grant.process_shortcuts()
        assert len(additional) == 1
