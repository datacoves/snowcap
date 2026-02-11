"""Integration tests for grant patterns against Snowflake.

US-013: Add grant integration test

These tests verify that multi-privilege grants and role grants work correctly
when applied to a real Snowflake instance.
"""

import os

import pytest

from tests.helpers import safe_fetch
from snowcap import resources as res
from snowcap.blueprint import Blueprint, CreateResource
from snowcap.client import reset_cache
from snowcap.gitops import collect_blueprint_config

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

pytestmark = pytest.mark.requires_snowflake


@pytest.fixture(autouse=True)
def clear_cache():
    reset_cache()
    yield


class TestMultiPrivilegeGrantsIntegration:
    """Test that multi-privilege grants create correctly in Snowflake."""

    def test_multi_priv_grant_on_database(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: priv: [USAGE, MONITOR] on database creates 2 grants."""
        session = cursor.connection

        # Create a role to receive grants
        role_name = f"MULTI_PRIV_DB_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(role)

        # Create multi-privilege grants using YAML-style config
        yaml_config = {
            "grants": [
                {
                    "priv": ["USAGE", "MONITOR"],
                    "on_database": test_db,
                    "to_role": role_name,
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        # Should have 2 grants (one per privilege)
        assert len(plan) == 2
        assert all(isinstance(p, CreateResource) for p in plan)

        # Apply the grants
        blueprint.apply(session, plan)

        # Verify USAGE grant exists
        usage_grant = res.Grant(priv="USAGE", on_database=test_db, to=role)
        usage_data = safe_fetch(cursor, usage_grant.urn)
        assert usage_data is not None, "USAGE grant was not created"
        assert usage_data["priv"] == "USAGE"

        # Verify MONITOR grant exists
        monitor_grant = res.Grant(priv="MONITOR", on_database=test_db, to=role)
        monitor_data = safe_fetch(cursor, monitor_grant.urn)
        assert monitor_data is not None, "MONITOR grant was not created"
        assert monitor_data["priv"] == "MONITOR"

    def test_multi_priv_grant_on_warehouse(self, cursor, suffix, marked_for_cleanup):
        """Test: priv: [USAGE, OPERATE, MONITOR] on warehouse creates 3 grants."""
        session = cursor.connection

        # Create a warehouse for testing using raw SQL to avoid enterprise feature issues
        wh_name = f"MULTI_PRIV_WH_{suffix}"
        cursor.execute(f"CREATE WAREHOUSE IF NOT EXISTS {wh_name} WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60")
        wh = res.Warehouse(name=wh_name)
        marked_for_cleanup.append(wh)

        # Create a role to receive grants
        role_name = f"MULTI_PRIV_WH_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(role)

        # Create multi-privilege grants
        yaml_config = {
            "grants": [
                {
                    "priv": ["USAGE", "OPERATE", "MONITOR"],
                    "on_warehouse": wh_name,
                    "to_role": role_name,
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 3

        blueprint.apply(session, plan)

        # Verify all grants exist
        for priv in ["USAGE", "OPERATE", "MONITOR"]:
            grant = res.Grant(priv=priv, on_warehouse=wh_name, to=role)
            grant_data = safe_fetch(cursor, grant.urn)
            assert grant_data is not None, f"{priv} grant was not created"
            assert grant_data["priv"] == priv

    def test_multi_priv_grant_on_schema(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: priv: [CREATE TABLE, CREATE VIEW] on schema creates 2 grants."""
        session = cursor.connection

        # Create a schema for testing
        schema_name = f"MULTI_PRIV_SCHEMA_{suffix}"
        schema = res.Schema(name=f"{test_db}.{schema_name}")
        cursor.execute(schema.create_sql(if_not_exists=True))
        marked_for_cleanup.append(schema)

        # Create a role to receive grants
        role_name = f"MULTI_PRIV_SCH_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(role)

        # Create multi-privilege grants
        yaml_config = {
            "grants": [
                {
                    "priv": ["CREATE TABLE", "CREATE VIEW"],
                    "on_schema": f"{test_db}.{schema_name}",
                    "to_role": role_name,
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 2

        blueprint.apply(session, plan)

        # Verify both grants exist
        for priv in ["CREATE TABLE", "CREATE VIEW"]:
            grant = res.Grant(priv=priv, on_schema=f"{test_db}.{schema_name}", to=role)
            grant_data = safe_fetch(cursor, grant.urn)
            assert grant_data is not None, f"{priv} grant was not created"
            assert grant_data["priv"] == priv


class TestGrantsFetchBackIntegration:
    """Test that grants can be fetched back and match expected state."""

    def test_grant_fetch_matches_expected_state(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: Grants can be fetched back with correct properties."""
        session = cursor.connection

        # Create a role
        role_name = f"FETCH_GRANT_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(role)

        # Create a grant using blueprint
        yaml_config = {
            "grants": [
                {
                    "priv": "USAGE",
                    "on_database": test_db,
                    "to_role": role_name,
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 1

        blueprint.apply(session, plan)

        # Fetch the grant back
        grant = res.Grant(priv="USAGE", on_database=test_db, to=role)
        fetched = safe_fetch(cursor, grant.urn)

        assert fetched is not None
        assert fetched["priv"] == "USAGE"
        assert test_db in fetched["on"]
        assert fetched["to"] == role_name

    def test_no_drift_after_multi_priv_grant_apply(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: After apply, re-planning shows no changes (no drift)."""
        session = cursor.connection

        # Create a role
        role_name = f"NO_DRIFT_GRANT_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(role)

        # Create multi-privilege grant
        yaml_config = {
            "grants": [
                {
                    "priv": ["USAGE", "MONITOR"],
                    "on_database": test_db,
                    "to_role": role_name,
                }
            ],
        }

        # First apply
        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 2
        blueprint.apply(session, plan)

        # Re-plan should show no changes
        reset_cache()
        bc2 = collect_blueprint_config(yaml_config)
        blueprint2 = Blueprint.from_config(bc2)
        plan2 = blueprint2.plan(session)

        assert len(plan2) == 0, f"Expected no drift but got: {plan2}"


class TestRoleGrantsIntegration:
    """Test that role grants work correctly in Snowflake."""

    def test_role_grant_single_role_to_role(self, cursor, suffix, marked_for_cleanup):
        """Test: Role grant from one role to another role."""
        session = cursor.connection

        # Create source and target roles
        source_role_name = f"SRC_ROLE_{suffix}"
        target_role_name = f"TGT_ROLE_{suffix}"

        source_role = res.Role(name=source_role_name)
        target_role = res.Role(name=target_role_name)

        cursor.execute(source_role.create_sql(if_not_exists=True))
        cursor.execute(target_role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(source_role)
        marked_for_cleanup.append(target_role)

        # Create role grant
        yaml_config = {
            "role_grants": [
                {"role": source_role_name, "to_role": target_role_name}
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 1

        blueprint.apply(session, plan)

        # Verify the role grant exists
        role_grant = res.RoleGrant(role=source_role_name, to_role=target_role_name)
        grant_data = safe_fetch(cursor, role_grant.urn)
        assert grant_data is not None, "Role grant was not created"

    def test_role_grant_multiple_roles_to_single_role(self, cursor, suffix, marked_for_cleanup):
        """Test: Multiple roles granted to a single role."""
        session = cursor.connection

        # Create multiple source roles and one target
        source_names = [f"MULTI_SRC_A_{suffix}", f"MULTI_SRC_B_{suffix}"]
        target_name = f"MULTI_TGT_{suffix}"

        for name in source_names:
            role = res.Role(name=name)
            cursor.execute(role.create_sql(if_not_exists=True))
            marked_for_cleanup.append(role)

        target_role = res.Role(name=target_name)
        cursor.execute(target_role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(target_role)

        # Create role grants using roles list
        yaml_config = {
            "role_grants": [
                {
                    "roles": source_names,
                    "to_role": target_name,
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 2

        blueprint.apply(session, plan)

        # Verify all role grants exist
        for source_name in source_names:
            role_grant = res.RoleGrant(role=source_name, to_role=target_name)
            grant_data = safe_fetch(cursor, role_grant.urn)
            assert grant_data is not None, f"Role grant {source_name} -> {target_name} was not created"


class TestGrantCleanupIntegration:
    """Test that grant cleanup works correctly."""

    def test_grant_cleanup_after_revoke(self, cursor, suffix, test_db):
        """Test: Grants are properly cleaned up when revoked."""
        session = cursor.connection

        # Create a role
        role_name = f"CLEANUP_GRANT_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))

        try:
            # Create a grant
            grant = res.Grant(priv="USAGE", on_database=test_db, to=role)
            cursor.execute(grant.create_sql())

            # Verify grant exists
            grant_data = safe_fetch(cursor, grant.urn)
            assert grant_data is not None, "Grant should exist"

            # Revoke the grant
            cursor.execute(f"REVOKE USAGE ON DATABASE {test_db} FROM ROLE {role_name}")

            # Verify grant is gone
            reset_cache()
            grant_data = safe_fetch(cursor, grant.urn)
            assert grant_data is None, "Grant should be revoked"

        finally:
            # Cleanup the role
            cursor.execute(f"DROP ROLE IF EXISTS {role_name}")

    def test_multi_priv_grant_cleanup(self, cursor, suffix, test_db):
        """Test: Multi-privilege grants are all cleaned up correctly."""
        session = cursor.connection

        # Create a role
        role_name = f"MULTI_CLEANUP_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))

        try:
            # Create multi-privilege grants via blueprint
            yaml_config = {
                "grants": [
                    {
                        "priv": ["USAGE", "MONITOR"],
                        "on_database": test_db,
                        "to_role": role_name,
                    }
                ],
            }

            bc = collect_blueprint_config(yaml_config)
            blueprint = Blueprint.from_config(bc)

            plan = blueprint.plan(session)
            blueprint.apply(session, plan)

            # Verify grants exist
            for priv in ["USAGE", "MONITOR"]:
                grant = res.Grant(priv=priv, on_database=test_db, to=role)
                grant_data = safe_fetch(cursor, grant.urn)
                assert grant_data is not None, f"{priv} grant should exist"

            # Revoke all grants
            for priv in ["USAGE", "MONITOR"]:
                cursor.execute(f"REVOKE {priv} ON DATABASE {test_db} FROM ROLE {role_name}")

            # Verify all grants are gone
            reset_cache()
            for priv in ["USAGE", "MONITOR"]:
                grant = res.Grant(priv=priv, on_database=test_db, to=role)
                grant_data = safe_fetch(cursor, grant.urn)
                assert grant_data is None, f"{priv} grant should be revoked"

        finally:
            # Cleanup the role
            cursor.execute(f"DROP ROLE IF EXISTS {role_name}")

    def test_role_grant_cleanup(self, cursor, suffix):
        """Test: Role grants are cleaned up when revoked."""
        session = cursor.connection

        # Create roles
        source_name = f"RG_CLEANUP_SRC_{suffix}"
        target_name = f"RG_CLEANUP_TGT_{suffix}"

        source_role = res.Role(name=source_name)
        target_role = res.Role(name=target_name)

        cursor.execute(source_role.create_sql(if_not_exists=True))
        cursor.execute(target_role.create_sql(if_not_exists=True))

        try:
            # Grant source role to target role
            cursor.execute(f"GRANT ROLE {source_name} TO ROLE {target_name}")

            # Verify role grant exists
            role_grant = res.RoleGrant(role=source_name, to_role=target_name)
            grant_data = safe_fetch(cursor, role_grant.urn)
            assert grant_data is not None, "Role grant should exist"

            # Revoke the role grant
            cursor.execute(f"REVOKE ROLE {source_name} FROM ROLE {target_name}")

            # Verify role grant is gone
            reset_cache()
            grant_data = safe_fetch(cursor, role_grant.urn)
            assert grant_data is None, "Role grant should be revoked"

        finally:
            # Cleanup roles
            cursor.execute(f"DROP ROLE IF EXISTS {source_name}")
            cursor.execute(f"DROP ROLE IF EXISTS {target_name}")
