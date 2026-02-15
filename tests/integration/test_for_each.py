"""Integration tests for for_each functionality against Snowflake.

US-010: Add for_each integration test

These tests verify that for_each expansion works correctly when applied
to a real Snowflake instance.
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


class TestForEachRolesIntegration:
    """Test that for_each with roles creates all roles in Snowflake."""

    def test_for_each_creates_multiple_roles(self, cursor, suffix, marked_for_cleanup):
        """Test: Blueprint with for_each roles creates all roles in Snowflake."""
        session = cursor.connection

        # Define config with for_each for roles
        yaml_config = {
            "vars": [
                {
                    "name": "role_names",
                    "type": "list",
                    "default": [f"FOR_EACH_ROLE_A_{suffix}", f"FOR_EACH_ROLE_B_{suffix}", f"FOR_EACH_ROLE_C_{suffix}"],
                }
            ],
            "roles": [
                {
                    "for_each": "var.role_names",
                    "name": "{{ each.value }}",
                    "comment": "Created by for_each test",
                }
            ],
        }

        # Collect config and create blueprint
        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        # Plan should show 3 create operations
        plan = blueprint.plan(session)
        assert len(plan) == 3
        assert all(isinstance(p, CreateResource) for p in plan)

        # Track resources for cleanup
        for resource in bc.resources:
            marked_for_cleanup.append(resource)

        # Apply the changes
        blueprint.apply(session, plan)

        # Verify all roles were created
        for role_name in [f"FOR_EACH_ROLE_A_{suffix}", f"FOR_EACH_ROLE_B_{suffix}", f"FOR_EACH_ROLE_C_{suffix}"]:
            role = res.Role(name=role_name)
            role_data = safe_fetch(cursor, role.urn)
            assert role_data is not None, f"Role {role_name} was not created"
            assert role_data["name"] == role_name
            assert role_data["comment"] == "Created by for_each test"

    def test_for_each_roles_with_object_list(self, cursor, suffix, marked_for_cleanup):
        """Test: for_each with object list creates roles with all properties."""
        session = cursor.connection

        yaml_config = {
            "vars": [
                {
                    "name": "role_configs",
                    "type": "list",
                    "default": [
                        {"name": f"FOR_EACH_OBJ_ROLE_A_{suffix}", "comment": "First role"},
                        {"name": f"FOR_EACH_OBJ_ROLE_B_{suffix}", "comment": "Second role"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.role_configs",
                    "name": "{{ each.value.name }}",
                    "comment": "{{ each.value.comment }}",
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 2

        for resource in bc.resources:
            marked_for_cleanup.append(resource)

        blueprint.apply(session, plan)

        # Verify roles with correct comments
        role_a = safe_fetch(cursor, res.Role(name=f"FOR_EACH_OBJ_ROLE_A_{suffix}").urn)
        assert role_a is not None
        assert role_a["comment"] == "First role"

        role_b = safe_fetch(cursor, res.Role(name=f"FOR_EACH_OBJ_ROLE_B_{suffix}").urn)
        assert role_b is not None
        assert role_b["comment"] == "Second role"


class TestForEachGrantsIntegration:
    """Test that for_each with grants creates all grants in Snowflake."""

    def test_for_each_creates_multiple_grants(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: Blueprint with for_each grants creates all grants."""
        session = cursor.connection

        # First create a role to grant to
        role_name = f"FOR_EACH_GRANT_ROLE_{suffix}"
        role = res.Role(name=role_name)
        cursor.execute(role.create_sql(if_not_exists=True))
        marked_for_cleanup.append(role)

        # Create grants using for_each
        yaml_config = {
            "vars": [
                {
                    "name": "privileges",
                    "type": "list",
                    "default": ["USAGE", "MONITOR"],
                }
            ],
            "grants": [
                {
                    "for_each": "var.privileges",
                    "priv": "{{ each.value }}",
                    "on_database": test_db,
                    "to_role": role_name,
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 2

        blueprint.apply(session, plan)

        # Verify grants were created
        for priv in ["USAGE", "MONITOR"]:
            grant = res.Grant(priv=priv, on_database=test_db, to=role)
            grant_data = safe_fetch(cursor, grant.urn)
            assert grant_data is not None, f"Grant {priv} was not created"
            assert grant_data["priv"] == priv

    def test_for_each_grants_to_multiple_roles(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: for_each can grant same privilege to multiple roles."""
        session = cursor.connection

        # Create roles first
        role_names = [f"FOR_EACH_MULTI_ROLE_A_{suffix}", f"FOR_EACH_MULTI_ROLE_B_{suffix}"]
        for role_name in role_names:
            role = res.Role(name=role_name)
            cursor.execute(role.create_sql(if_not_exists=True))
            marked_for_cleanup.append(role)

        yaml_config = {
            "vars": [
                {
                    "name": "roles",
                    "type": "list",
                    "default": role_names,
                }
            ],
            "grants": [
                {
                    "for_each": "var.roles",
                    "priv": "USAGE",
                    "on_database": test_db,
                    "to_role": "{{ each.value }}",
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        assert len(plan) == 2

        blueprint.apply(session, plan)

        # Verify grants to both roles
        for role_name in role_names:
            grant = res.Grant(priv="USAGE", on_database=test_db, to=res.Role(name=role_name))
            grant_data = safe_fetch(cursor, grant.urn)
            assert grant_data is not None, f"Grant to {role_name} was not created"


class TestForEachPlanOperations:
    """Test that for_each plan shows correct operations."""

    def test_for_each_plan_shows_correct_create_operations(self, cursor, suffix):
        """Test: Plan shows correct create operations for expanded resources."""
        session = cursor.connection

        yaml_config = {
            "vars": [
                {
                    "name": "schemas",
                    "type": "list",
                    "default": [
                        {"name": f"FOR_EACH_SCHEMA_A_{suffix}"},
                        {"name": f"FOR_EACH_SCHEMA_B_{suffix}"},
                        {"name": f"FOR_EACH_SCHEMA_C_{suffix}"},
                    ],
                }
            ],
            "schemas": [
                {
                    "for_each": "var.schemas",
                    "name": "STATIC_DATABASE.{{ each.value.name }}",
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)

        # Verify 3 create operations
        assert len(plan) == 3
        assert all(isinstance(p, CreateResource) for p in plan)

        # Verify correct schema names in plan
        schema_names = {p.urn.fqn.name for p in plan}
        expected_names = {
            f"FOR_EACH_SCHEMA_A_{suffix}",
            f"FOR_EACH_SCHEMA_B_{suffix}",
            f"FOR_EACH_SCHEMA_C_{suffix}",
        }
        assert schema_names == expected_names

    def test_for_each_no_drift_after_apply(self, cursor, suffix, marked_for_cleanup):
        """Test: After apply, re-planning shows no changes (no drift)."""
        session = cursor.connection

        yaml_config = {
            "vars": [
                {
                    "name": "role_names",
                    "type": "list",
                    "default": [f"FOR_EACH_NODRIFT_A_{suffix}", f"FOR_EACH_NODRIFT_B_{suffix}"],
                }
            ],
            "roles": [
                {
                    "for_each": "var.role_names",
                    "name": "{{ each.value }}",
                }
            ],
        }

        # First apply
        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        for resource in bc.resources:
            marked_for_cleanup.append(resource)

        plan = blueprint.plan(session)
        assert len(plan) == 2
        blueprint.apply(session, plan)

        # Re-plan should show no changes
        reset_cache()
        bc2 = collect_blueprint_config(yaml_config)
        blueprint2 = Blueprint.from_config(bc2)
        plan2 = blueprint2.plan(session)

        assert len(plan2) == 0, f"Expected no drift but got: {plan2}"


class TestForEachCleanup:
    """Test that resources created by for_each are properly cleaned up."""

    def test_cleanup_drops_for_each_resources(self, cursor, suffix):
        """Test: Cleanup drops all created resources."""
        session = cursor.connection

        role_names = [f"FOR_EACH_CLEANUP_A_{suffix}", f"FOR_EACH_CLEANUP_B_{suffix}"]

        yaml_config = {
            "vars": [
                {
                    "name": "role_names",
                    "type": "list",
                    "default": role_names,
                }
            ],
            "roles": [
                {
                    "for_each": "var.role_names",
                    "name": "{{ each.value }}",
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        plan = blueprint.plan(session)
        blueprint.apply(session, plan)

        # Verify roles exist
        for role_name in role_names:
            role_data = safe_fetch(cursor, res.Role(name=role_name).urn)
            assert role_data is not None

        # Cleanup by dropping roles
        for role_name in role_names:
            cursor.execute(f"DROP ROLE IF EXISTS {role_name}")

        # Verify roles are gone
        for role_name in role_names:
            role_data = safe_fetch(cursor, res.Role(name=role_name).urn)
            assert role_data is None, f"Role {role_name} should have been dropped"


class TestForEachComplexPatterns:
    """Test complex for_each patterns similar to real-world usage."""

    def test_for_each_with_jinja_filters(self, cursor, suffix, marked_for_cleanup):
        """Test: for_each with Jinja filters works in integration."""
        session = cursor.connection

        yaml_config = {
            "vars": [
                {
                    "name": "envs",
                    "type": "list",
                    "default": [
                        {"env": "dev", "comment": "development"},
                        {"env": "prod", "comment": "production"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.envs",
                    "name": "{{ each.value.env | upper }}_ROLE_{{ suffix }}".replace("{{ suffix }}", suffix),
                    "comment": "{{ each.value.comment }} environment role",
                }
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        for resource in bc.resources:
            marked_for_cleanup.append(resource)

        plan = blueprint.plan(session)
        assert len(plan) == 2

        blueprint.apply(session, plan)

        # Verify roles created with uppercase names
        dev_role = safe_fetch(cursor, res.Role(name=f"DEV_ROLE_{suffix}").urn)
        assert dev_role is not None
        assert dev_role["comment"] == "development environment role"

        prod_role = safe_fetch(cursor, res.Role(name=f"PROD_ROLE_{suffix}").urn)
        assert prod_role is not None
        assert prod_role["comment"] == "production environment role"

    def test_for_each_combined_with_static_resources(self, cursor, suffix, test_db, marked_for_cleanup):
        """Test: for_each resources work alongside static resources."""
        session = cursor.connection

        # Mix of static and for_each resources
        yaml_config = {
            "vars": [
                {
                    "name": "dynamic_roles",
                    "type": "list",
                    "default": [f"DYNAMIC_A_{suffix}", f"DYNAMIC_B_{suffix}"],
                }
            ],
            "roles": [
                # Static role
                {"name": f"STATIC_ROLE_{suffix}", "comment": "Static role"},
                # Dynamic roles via for_each
                {
                    "for_each": "var.dynamic_roles",
                    "name": "{{ each.value }}",
                    "comment": "Dynamic role",
                },
            ],
        }

        bc = collect_blueprint_config(yaml_config)
        blueprint = Blueprint.from_config(bc)

        for resource in bc.resources:
            marked_for_cleanup.append(resource)

        plan = blueprint.plan(session)
        assert len(plan) == 3  # 1 static + 2 dynamic

        blueprint.apply(session, plan)

        # Verify all roles
        static_role = safe_fetch(cursor, res.Role(name=f"STATIC_ROLE_{suffix}").urn)
        assert static_role is not None
        assert static_role["comment"] == "Static role"

        for role_name in [f"DYNAMIC_A_{suffix}", f"DYNAMIC_B_{suffix}"]:
            dynamic_role = safe_fetch(cursor, res.Role(name=role_name).urn)
            assert dynamic_role is not None
            assert dynamic_role["comment"] == "Dynamic role"
