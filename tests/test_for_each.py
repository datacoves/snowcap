"""Tests for for_each functionality in YAML configuration.

US-006: Add for_each with simple list values test
US-007: Add for_each with object list values test
US-008: Add for_each with Jinja filters test
US-009: Add for_each with multiple resource types test
"""

import pytest

from snowcap.gitops import collect_blueprint_config
from snowcap.enums import ResourceType


class TestForEachSimpleList:
    """US-006: Tests for for_each with simple list values."""

    def test_for_each_with_var_list(self):
        """Test: for_each: var.schemas with ["schema1", "schema2"] produces 2 resources."""
        config = {
            "vars": [{"name": "schemas", "default": ["schema1", "schema2"], "type": "list"}],
            "roles": [
                {
                    "for_each": "var.schemas",
                    "name": "role_{{ each.value }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert blueprint_config.resources is not None
        assert len(blueprint_config.resources) == 2

    def test_for_each_name_interpolation(self):
        """Test: name: "role_{{ each.value }}" interpolates correctly."""
        config = {
            "vars": [{"name": "items", "default": ["alpha", "beta", "gamma"], "type": "list"}],
            "roles": [
                {
                    "for_each": "var.items",
                    "name": "role_{{ each.value }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert names == ["role_alpha", "role_beta", "role_gamma"]

    def test_for_each_produces_correct_resource_names(self):
        """Test: Resources have correct names after expansion."""
        config = {
            "vars": [{"name": "envs", "default": ["dev", "staging", "prod"], "type": "list"}],
            "roles": [
                {
                    "for_each": "var.envs",
                    "name": "{{ each.value }}_admin",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "dev_admin" in names
        assert "staging_admin" in names
        assert "prod_admin" in names

    def test_for_each_with_single_item_list(self):
        """Test: for_each works with single item list."""
        config = {
            "vars": [{"name": "single", "default": ["only_one"], "type": "list"}],
            "roles": [
                {
                    "for_each": "var.single",
                    "name": "role_{{ each.value }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 1
        assert blueprint_config.resources[0].urn.fqn.name == "role_only_one"

    def test_for_each_with_empty_list_raises_error(self):
        """Test: for_each with empty list and no other resources raises error.

        This is expected behavior - a config with no resources is invalid.
        """
        config = {
            "vars": [{"name": "empty", "default": [], "type": "list"}],
            "roles": [
                {
                    "for_each": "var.empty",
                    "name": "role_{{ each.value }}",
                }
            ],
        }
        with pytest.raises(ValueError, match="No resources found in config"):
            collect_blueprint_config(config)

    def test_for_each_missing_var_raises_error(self):
        """Test: for_each with missing variable raises ValueError."""
        config = {
            "roles": [
                {
                    "for_each": "var.nonexistent",
                    "name": "role_{{ each.value }}",
                }
            ],
        }
        with pytest.raises(ValueError, match="Var `nonexistent` not found"):
            collect_blueprint_config(config)

    def test_for_each_invalid_reference_raises_error(self):
        """Test: for_each without var. prefix raises ValueError."""
        config = {
            "vars": [{"name": "schemas", "default": ["a", "b"], "type": "list"}],
            "roles": [
                {
                    "for_each": "schemas",  # Missing var. prefix
                    "name": "role_{{ each.value }}",
                }
            ],
        }
        with pytest.raises(ValueError, match="for_each must be a var reference"):
            collect_blueprint_config(config)

    def test_for_each_with_cli_vars(self):
        """Test: for_each works with CLI-provided variables."""
        yaml_config = {
            "vars": [{"name": "regions", "type": "list"}],
            "roles": [
                {
                    "for_each": "var.regions",
                    "name": "region_{{ each.value }}_role",
                }
            ],
        }
        cli_config = {"vars": {"regions": ["us-east", "us-west", "eu-central"]}}
        blueprint_config = collect_blueprint_config(yaml_config, cli_config)
        assert len(blueprint_config.resources) == 3
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "region_us-east_role" in names
        assert "region_us-west_role" in names
        assert "region_eu-central_role" in names

    def test_for_each_preserves_static_properties(self):
        """Test: Static properties not using each.value are preserved."""
        config = {
            "vars": [{"name": "teams", "default": ["engineering", "sales"], "type": "list"}],
            "roles": [
                {
                    "for_each": "var.teams",
                    "name": "{{ each.value }}_role",
                    "comment": "Team role",  # Static property
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2
        for resource in blueprint_config.resources:
            assert resource._data.comment == "Team role"


class TestForEachObjectList:
    """US-007: Tests for for_each with object list values (balboa pattern)."""

    def test_for_each_with_object_list(self):
        """Test: for_each: var.databases with list of objects produces correct resource count."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {"name": "db_dev", "owner": "SYSADMIN"},
                        {"name": "db_prod", "owner": "SYSADMIN"},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                    "owner": "{{ each.value.owner }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

    def test_for_each_object_name_property(self):
        """Test: name: "{{ each.value.name }}" accesses object properties."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {"name": "analytics", "owner": "SYSADMIN"},
                        {"name": "reporting", "owner": "SYSADMIN"},
                        {"name": "warehouse", "owner": "SYSADMIN"},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert names == ["analytics", "reporting", "warehouse"]

    def test_for_each_object_nested_property(self):
        """Test: owner: "{{ each.value.owner }}" accesses nested properties."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {"name": "db1", "owner": "SYSADMIN"},
                        {"name": "db2", "owner": "ACCOUNTADMIN"},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                    "owner": "{{ each.value.owner }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        # Owner is converted to Role object, access name via urn.fqn.name
        owner_names = [resource._data.owner.urn.fqn.name for resource in blueprint_config.resources]
        assert "SYSADMIN" in owner_names
        assert "ACCOUNTADMIN" in owner_names

    def test_for_each_multiple_object_properties(self):
        """Test: Multiple properties from same object work together."""
        config = {
            "vars": [
                {
                    "name": "roles",
                    "type": "list",
                    "default": [
                        {"name": "dev_role", "comment": "Development role"},
                        {"name": "prod_role", "comment": "Production role"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.roles",
                    "name": "{{ each.value.name }}",
                    "comment": "{{ each.value.comment }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        # Find each role and verify both properties
        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            comment = resource._data.comment
            if name == "dev_role":
                assert comment == "Development role"
            elif name == "prod_role":
                assert comment == "Production role"
            else:
                pytest.fail(f"Unexpected role name: {name}")

    def test_for_each_object_with_integer_property(self):
        """Test: Integer properties like max_data_extension_time_in_days are typed correctly."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {"name": "db1", "max_data_extension_time_in_days": 7},
                        {"name": "db2", "max_data_extension_time_in_days": 14},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                    "max_data_extension_time_in_days": "{{ each.value.max_data_extension_time_in_days }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            max_extension = resource._data.max_data_extension_time_in_days
            # The value should be an integer (type coercion happens in gitops.py)
            assert isinstance(max_extension, int), f"Expected int, got {type(max_extension)}"
            if name == "db1":
                assert max_extension == 7
            elif name == "db2":
                assert max_extension == 14

    def test_for_each_object_with_all_properties(self):
        """Test: Complete database object with name, owner, and integer properties."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {
                            "name": "analytics_db",
                            "owner": "ANALYTICS_ADMIN",
                            "data_retention_time_in_days": 30,
                            "comment": "Analytics database",
                        },
                        {
                            "name": "staging_db",
                            "owner": "SYSADMIN",
                            "data_retention_time_in_days": 1,
                            "comment": "Staging environment",
                        },
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                    "owner": "{{ each.value.owner }}",
                    "data_retention_time_in_days": "{{ each.value.data_retention_time_in_days }}",
                    "comment": "{{ each.value.comment }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            # Owner is converted to Role object, access name via urn.fqn.name
            owner = resource._data.owner.urn.fqn.name
            retention = resource._data.data_retention_time_in_days
            comment = resource._data.comment

            if name == "analytics_db":
                assert owner == "ANALYTICS_ADMIN"
                assert retention == 30
                assert comment == "Analytics database"
            elif name == "staging_db":
                assert owner == "SYSADMIN"
                assert retention == 1
                assert comment == "Staging environment"


class TestForEachJinjaFilters:
    """US-008: Tests for for_each with Jinja filters (balboa pattern)."""

    def test_for_each_split_first_part(self):
        """Test: {{ each.value.name.split('.')[0] }} extracts first part (database)."""
        config = {
            "vars": [
                {
                    "name": "schemas",
                    "type": "list",
                    "default": [
                        {"name": "analytics.raw"},
                        {"name": "reporting.curated"},
                        {"name": "staging.temp"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.schemas",
                    "name": "{{ each.value.name.split('.')[0] }}_admin",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert names == ["analytics_admin", "reporting_admin", "staging_admin"]

    def test_for_each_split_second_part(self):
        """Test: {{ each.value.name.split('.')[1] }} extracts second part (schema)."""
        config = {
            "vars": [
                {
                    "name": "schemas",
                    "type": "list",
                    "default": [
                        {"name": "analytics.raw"},
                        {"name": "reporting.curated"},
                        {"name": "staging.temp"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.schemas",
                    "name": "{{ each.value.name.split('.')[1] }}_role",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert names == ["raw_role", "curated_role", "temp_role"]

    def test_for_each_default_filter_missing_key(self):
        """Test: {{ each.value.timeout | default(3600) }} uses default when missing."""
        config = {
            "vars": [
                {
                    "name": "tasks",
                    "type": "list",
                    "default": [
                        {"name": "task1"},  # No timeout, should use default
                        {"name": "task2", "timeout": 7200},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.tasks",
                    "name": "{{ each.value.name }}",
                    "comment": "Timeout: {{ each.value.timeout | default(3600) }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            comment = resource._data.comment
            if name == "task1":
                assert comment == "Timeout: 3600"
            elif name == "task2":
                assert comment == "Timeout: 7200"

    def test_for_each_get_method_with_fallback(self):
        """Test: {{ each.value.get('owner', 'SYSADMIN') }} uses fallback value."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {"name": "db1"},  # No owner, should use fallback
                        {"name": "db2", "owner": "ACCOUNTADMIN"},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                    "owner": "{{ each.value.get('owner', 'SYSADMIN') }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            owner = resource._data.owner.urn.fqn.name
            if name == "db1":
                assert owner == "SYSADMIN"
            elif name == "db2":
                assert owner == "ACCOUNTADMIN"

    def test_for_each_filters_with_integer_coercion(self):
        """Test: Filters work with integer type coercion."""
        config = {
            "vars": [
                {
                    "name": "databases",
                    "type": "list",
                    "default": [
                        {"name": "db1"},  # No retention, should use default
                        {"name": "db2", "retention": 30},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.databases",
                    "name": "{{ each.value.name }}",
                    "data_retention_time_in_days": "{{ each.value.get('retention', 7) }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            retention = resource._data.data_retention_time_in_days
            # Check integer type coercion works
            assert isinstance(retention, int), f"Expected int, got {type(retention)}"
            if name == "db1":
                assert retention == 7
            elif name == "db2":
                assert retention == 30

    def test_for_each_combined_filters(self):
        """Test: Multiple Jinja operations can be combined in one expression."""
        config = {
            "vars": [
                {
                    "name": "sources",
                    "type": "list",
                    "default": [
                        {"fqn": "prod.analytics.customers"},
                        {"fqn": "dev.staging.orders"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.sources",
                    "name": "{{ each.value.fqn.split('.')[0] }}_{{ each.value.fqn.split('.')[1] }}_role",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert names == ["prod_analytics_role", "dev_staging_role"]

    def test_for_each_upper_lower_filters(self):
        """Test: Jinja upper/lower filters work in templates."""
        config = {
            "vars": [
                {
                    "name": "envs",
                    "type": "list",
                    "default": [
                        {"name": "Dev"},
                        {"name": "Prod"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.envs",
                    "name": "{{ each.value.name | upper }}_ROLE",
                    "comment": "{{ each.value.name | lower }} environment",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            comment = resource._data.comment
            if name == "DEV_ROLE":
                assert comment == "dev environment"
            elif name == "PROD_ROLE":
                assert comment == "prod environment"

    def test_for_each_replace_filter(self):
        """Test: Jinja replace filter works in templates."""
        config = {
            "vars": [
                {
                    "name": "names",
                    "type": "list",
                    "default": [
                        {"name": "my-role-name"},
                        {"name": "another-role"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.names",
                    # Replace dashes with underscores
                    "name": "{{ each.value.name | replace('-', '_') }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert names == ["my_role_name", "another_role"]


class TestForEachMultipleResourceTypes:
    """US-009: Tests for for_each with multiple resource types.

    Verifies that for_each works correctly for databases, schemas, roles,
    warehouses, and grants - all resource types commonly used in balboa patterns.
    """

    def test_for_each_databases_section(self):
        """Test: databases: section with for_each produces Database resources."""
        config = {
            "vars": [
                {
                    "name": "db_configs",
                    "type": "list",
                    "default": [
                        {"name": "raw_db", "comment": "Raw data"},
                        {"name": "staging_db", "comment": "Staging area"},
                        {"name": "analytics_db", "comment": "Analytics"},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.db_configs",
                    "name": "{{ each.value.name }}",
                    "comment": "{{ each.value.comment }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all resources are databases
        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.DATABASE

        # Verify names
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "raw_db" in names
        assert "staging_db" in names
        assert "analytics_db" in names

    def test_for_each_schemas_section(self):
        """Test: schemas: section with for_each produces Schema resources."""
        config = {
            "vars": [
                {
                    "name": "schema_configs",
                    "type": "list",
                    "default": [
                        {"name": "raw", "database": "analytics"},
                        {"name": "curated", "database": "analytics"},
                        {"name": "reporting", "database": "analytics"},
                    ],
                }
            ],
            "schemas": [
                {
                    "for_each": "var.schema_configs",
                    "name": "{{ each.value.database }}.{{ each.value.name }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all resources are schemas
        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.SCHEMA

        # Verify names include both database and schema
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "raw" in names
        assert "curated" in names
        assert "reporting" in names

    def test_for_each_roles_section(self):
        """Test: roles: section with for_each produces Role resources."""
        config = {
            "vars": [
                {
                    "name": "role_configs",
                    "type": "list",
                    "default": [
                        {"name": "analyst", "comment": "Data analyst role"},
                        {"name": "engineer", "comment": "Data engineer role"},
                        {"name": "admin", "comment": "Admin role"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.role_configs",
                    "name": "{{ each.value.name }}_role",
                    "comment": "{{ each.value.comment }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all resources are roles
        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.ROLE

        # Verify names
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "analyst_role" in names
        assert "engineer_role" in names
        assert "admin_role" in names

    def test_for_each_warehouses_section(self):
        """Test: warehouses: section with for_each produces Warehouse resources."""
        config = {
            "vars": [
                {
                    "name": "wh_configs",
                    "type": "list",
                    "default": [
                        {"name": "dev_wh", "size": "XSMALL"},
                        {"name": "prod_wh", "size": "MEDIUM"},
                    ],
                }
            ],
            "warehouses": [
                {
                    "for_each": "var.wh_configs",
                    "name": "{{ each.value.name }}",
                    "warehouse_size": "{{ each.value.size }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        # Verify all resources are warehouses
        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.WAREHOUSE

        # Verify names
        names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "dev_wh" in names
        assert "prod_wh" in names

    def test_for_each_grants_section(self):
        """Test: grants: section with for_each produces Grant resources."""
        config = {
            "vars": [
                {
                    "name": "grant_configs",
                    "type": "list",
                    "default": [
                        {"role": "analyst", "priv": "USAGE", "database": "analytics"},
                        {"role": "engineer", "priv": "ALL", "database": "staging"},
                    ],
                }
            ],
            "grants": [
                {
                    "for_each": "var.grant_configs",
                    "priv": "{{ each.value.priv }}",
                    "on_database": "{{ each.value.database }}",
                    "to_role": "{{ each.value.role }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        # Note: ALL priv expands to multiple grants, so check at least 2 resources
        assert len(blueprint_config.resources) >= 2

        # Verify all resources are grants
        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.GRANT

    def test_for_each_resources_have_correct_properties(self):
        """Test: All resource types have correct properties after expansion."""
        config = {
            "vars": [
                {
                    "name": "envs",
                    "type": "list",
                    "default": [
                        {"name": "dev", "retention": 1},
                        {"name": "prod", "retention": 30},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.envs",
                    "name": "{{ each.value.name }}_db",
                    "data_retention_time_in_days": "{{ each.value.retention }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            name = resource.urn.fqn.name
            retention = resource._data.data_retention_time_in_days

            if name == "dev_db":
                assert retention == 1
            elif name == "prod_db":
                assert retention == 30
            else:
                pytest.fail(f"Unexpected database name: {name}")

    def test_for_each_mixed_resource_types(self):
        """Test: Multiple resource types can be expanded in the same config."""
        config = {
            "vars": [
                {
                    "name": "projects",
                    "type": "list",
                    "default": [
                        {"name": "alpha"},
                        {"name": "beta"},
                    ],
                }
            ],
            "databases": [
                {
                    "for_each": "var.projects",
                    "name": "{{ each.value.name }}_db",
                }
            ],
            "roles": [
                {
                    "for_each": "var.projects",
                    "name": "{{ each.value.name }}_role",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        # 2 databases + 2 roles = 4 resources
        assert len(blueprint_config.resources) == 4

        # Verify we have both databases and roles
        db_count = sum(1 for r in blueprint_config.resources if r.resource_type == ResourceType.DATABASE)
        role_count = sum(1 for r in blueprint_config.resources if r.resource_type == ResourceType.ROLE)

        assert db_count == 2
        assert role_count == 2

    def test_for_each_schema_with_database_reference(self):
        """Test: Schema for_each can reference database from same variable."""
        config = {
            "vars": [
                {
                    "name": "schema_locations",
                    "type": "list",
                    "default": [
                        {"db": "raw", "schema": "landing"},
                        {"db": "raw", "schema": "staging"},
                        {"db": "analytics", "schema": "curated"},
                    ],
                }
            ],
            "schemas": [
                {
                    "for_each": "var.schema_locations",
                    "name": "{{ each.value.db }}.{{ each.value.schema }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 3

        # Verify all resources are schemas
        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.SCHEMA

        # Verify schema names
        schema_names = [resource.urn.fqn.name for resource in blueprint_config.resources]
        assert "landing" in schema_names
        assert "staging" in schema_names
        assert "curated" in schema_names

    def test_for_each_warehouse_with_all_properties(self):
        """Test: Warehouse for_each properly expands all warehouse properties."""
        config = {
            "vars": [
                {
                    "name": "warehouse_specs",
                    "type": "list",
                    "default": [
                        {
                            "name": "loading_wh",
                            "size": "MEDIUM",
                            "auto_suspend": 60,
                            "comment": "ETL loading warehouse",
                        },
                        {
                            "name": "reporting_wh",
                            "size": "SMALL",
                            "auto_suspend": 300,
                            "comment": "Reporting queries",
                        },
                    ],
                }
            ],
            "warehouses": [
                {
                    "for_each": "var.warehouse_specs",
                    "name": "{{ each.value.name }}",
                    "warehouse_size": "{{ each.value.size }}",
                    "auto_suspend": "{{ each.value.auto_suspend }}",
                    "comment": "{{ each.value.comment }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2

        for resource in blueprint_config.resources:
            assert resource.resource_type == ResourceType.WAREHOUSE
            name = resource.urn.fqn.name
            auto_suspend = resource._data.auto_suspend
            comment = resource._data.comment

            # Verify integer type coercion for auto_suspend
            assert isinstance(auto_suspend, int), f"Expected int, got {type(auto_suspend)}"

            if name == "loading_wh":
                assert auto_suspend == 60
                assert comment == "ETL loading warehouse"
            elif name == "reporting_wh":
                assert auto_suspend == 300
                assert comment == "Reporting queries"
