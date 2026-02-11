"""Tests for YAML configuration and variable interpolation.

US-014: Add YAML variable interpolation tests
US-015: Add YAML config merging tests
"""

import os

import pytest

from snowcap.gitops import collect_blueprint_config, merge_configs
from snowcap.blueprint_config import set_vars_defaults
from snowcap.exceptions import MissingVarException


class TestVariableInterpolation:
    """US-014: Tests for variable interpolation in YAML configuration."""

    def test_simple_variable_reference(self):
        """Test: Simple variable reference {{ var.name }} works."""
        config = {
            "vars": [{"name": "role_name", "type": "str", "default": "my_role"}],
            "roles": [{"name": "{{ var.role_name }}"}],
        }
        # Note: var references in resource fields are handled differently -
        # they are VarString objects resolved during blueprint execution
        # For collect_blueprint_config, we test that vars are set correctly
        blueprint_config = collect_blueprint_config(config)
        assert blueprint_config.vars["role_name"] == "my_role"
        assert len(blueprint_config.resources) == 1

    def test_nested_variable_reference(self):
        """Test: Nested variable reference {{ var.config.name }} works via for_each.

        Note: Deep nested var references like {{ var.config.name }} require for_each
        pattern since YAML vars themselves are flat (not nested objects).
        The nested pattern works through for_each's each.value.property syntax.
        """
        config = {
            "vars": [
                {
                    "name": "configs",
                    "type": "list",
                    "default": [
                        {"name": "alpha", "owner": "SYSADMIN"},
                        {"name": "beta", "owner": "ACCOUNTADMIN"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.configs",
                    "name": "{{ each.value.name }}_role",
                    "comment": "Owned by {{ each.value.owner }}",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2
        names = [r.urn.fqn.name for r in blueprint_config.resources]
        assert "alpha_role" in names
        assert "beta_role" in names

    @pytest.mark.parametrize(
        "var_name,var_type,default_value,expected_type",
        [
            ("comment", "str", "a string value", str),
            ("retention", "int", 30, int),
            ("enabled", "bool", True, bool),
            ("schemas", "list", ["raw", "staging", "curated"], list),
        ],
        ids=["string", "integer", "boolean", "list"],
    )
    def test_variable_type_coercion(self, var_name, var_type, default_value, expected_type):
        """Test: Variable type coercion works for str, int, bool, and list types."""
        config = {
            "vars": [{"name": var_name, "type": var_type, "default": default_value}],
            "roles": [{"name": "test_role"}],
        }
        blueprint_config = collect_blueprint_config(config)
        assert blueprint_config.vars[var_name] == default_value
        assert isinstance(blueprint_config.vars[var_name], expected_type)

    def test_missing_required_variable_raises_error(self):
        """Test: Missing required variable raises clear error."""
        config = {
            "vars": [{"name": "required_var", "type": "str"}],  # No default = required
            "roles": [{"name": "test_role"}],
        }
        with pytest.raises(MissingVarException, match="required_var"):
            collect_blueprint_config(config)

    def test_missing_required_variable_error_message(self):
        """Test: Missing required variable error message is clear and informative."""
        config = {
            "vars": [{"name": "database_name", "type": "str"}],
            "roles": [{"name": "test_role"}],
        }
        with pytest.raises(MissingVarException) as exc_info:
            collect_blueprint_config(config)

        error_message = str(exc_info.value)
        assert "database_name" in error_message
        assert "missing" in error_message.lower() or "no default" in error_message.lower()

    def test_variable_override_from_cli(self):
        """Test: Variable override from CLI works."""
        yaml_config = {
            "vars": [{"name": "environment", "type": "str", "default": "development"}],
            "roles": [{"name": "test_role"}],
        }
        cli_config = {"vars": {"environment": "production"}}

        blueprint_config = collect_blueprint_config(yaml_config, cli_config)
        # CLI value should override YAML default
        assert blueprint_config.vars["environment"] == "production"

    def test_variable_override_cli_without_default(self):
        """Test: CLI can provide value for variable without default."""
        yaml_config = {
            "vars": [{"name": "required_setting", "type": "str"}],
            "roles": [{"name": "test_role"}],
        }
        cli_config = {"vars": {"required_setting": "provided_value"}}

        blueprint_config = collect_blueprint_config(yaml_config, cli_config)
        assert blueprint_config.vars["required_setting"] == "provided_value"

    def test_multiple_variables_work_together(self):
        """Test: Multiple variables can be used together."""
        config = {
            "vars": [
                {"name": "env", "type": "str", "default": "dev"},
                {"name": "team", "type": "str", "default": "analytics"},
                {"name": "retention", "type": "int", "default": 7},
            ],
            "roles": [{"name": "test_role"}],
        }
        blueprint_config = collect_blueprint_config(config)
        assert blueprint_config.vars["env"] == "dev"
        assert blueprint_config.vars["team"] == "analytics"
        assert blueprint_config.vars["retention"] == 7

    def test_variable_type_mismatch_raises_error(self):
        """Test: Providing wrong type raises TypeError."""
        yaml_config = {
            "vars": [{"name": "count", "type": "int"}],
            "roles": [{"name": "test_role"}],
        }
        cli_config = {"vars": {"count": "not_an_int"}}

        with pytest.raises(TypeError, match="count"):
            collect_blueprint_config(yaml_config, cli_config)

    def test_variable_without_spec_raises_error(self):
        """Test: Providing variable not in vars_spec raises ValueError."""
        yaml_config = {
            "vars": [{"name": "defined_var", "type": "str", "default": "value"}],
            "roles": [{"name": "test_role"}],
        }
        cli_config = {"vars": {"undefined_var": "some_value"}}

        with pytest.raises(ValueError, match="undefined_var"):
            collect_blueprint_config(yaml_config, cli_config)

    def test_vars_spec_requires_name(self):
        """Test: vars_spec entries must have a name."""
        config = {
            "vars": [{"type": "str", "default": "value"}],  # Missing name
            "roles": [{"name": "test_role"}],
        }
        # Raises KeyError when accessing spec["name"] in set_vars_defaults
        with pytest.raises(KeyError):
            collect_blueprint_config(config)

    def test_vars_spec_requires_type(self):
        """Test: vars_spec entries must have a type."""
        config = {
            "vars": [{"name": "my_var", "default": "value"}],  # Missing type
            "roles": [{"name": "test_role"}],
        }
        with pytest.raises(ValueError, match="type"):
            collect_blueprint_config(config)

    def test_vars_spec_invalid_type_raises_error(self):
        """Test: Invalid type in vars_spec raises ValueError."""
        config = {
            "vars": [{"name": "my_var", "type": "invalid_type", "default": "value"}],
            "roles": [{"name": "test_role"}],
        }
        with pytest.raises(ValueError, match="valid type"):
            collect_blueprint_config(config)

    def test_vars_spec_accepts_type_aliases(self):
        """Test: Type aliases (integer, boolean, string) are accepted."""
        config = {
            "vars": [
                {"name": "int_var", "type": "integer", "default": 42},
                {"name": "bool_var", "type": "boolean", "default": False},
                {"name": "str_var", "type": "string", "default": "hello"},
            ],
            "roles": [{"name": "test_role"}],
        }
        blueprint_config = collect_blueprint_config(config)
        assert blueprint_config.vars["int_var"] == 42
        assert blueprint_config.vars["bool_var"] is False
        assert blueprint_config.vars["str_var"] == "hello"

    def test_vars_with_for_each_interpolation(self):
        """Test: Variables work correctly with for_each using object properties.

        Note: Mixing {{ var.X }} with {{ each.value }} in the same template
        is not supported. Instead, include the var value in the for_each objects.
        """
        config = {
            "vars": [
                {
                    "name": "role_configs",
                    "type": "list",
                    "default": [
                        {"env": "dev", "team": "data"},
                        {"env": "prod", "team": "data"},
                    ],
                }
            ],
            "roles": [
                {
                    "for_each": "var.role_configs",
                    "name": "{{ each.value.env }}_{{ each.value.team }}_role",
                }
            ],
        }
        blueprint_config = collect_blueprint_config(config)
        assert len(blueprint_config.resources) == 2
        names = [r.urn.fqn.name for r in blueprint_config.resources]
        assert "dev_data_role" in names
        assert "prod_data_role" in names


class TestSetVarsDefaults:
    """Tests for set_vars_defaults function."""

    def test_set_vars_defaults_uses_defaults(self):
        """Test: set_vars_defaults fills in default values."""
        vars_spec = [
            {"name": "env", "type": "str", "default": "development"},
            {"name": "team", "type": "str", "default": "engineering"},
        ]
        vars = {}

        result = set_vars_defaults(vars_spec, vars)
        assert result["env"] == "development"
        assert result["team"] == "engineering"

    def test_set_vars_defaults_preserves_provided(self):
        """Test: set_vars_defaults preserves provided values."""
        vars_spec = [
            {"name": "env", "type": "str", "default": "development"},
        ]
        vars = {"env": "production"}

        result = set_vars_defaults(vars_spec, vars)
        assert result["env"] == "production"

    def test_set_vars_defaults_raises_on_missing_required(self):
        """Test: set_vars_defaults raises for missing required vars."""
        vars_spec = [
            {"name": "required_var", "type": "str"},  # No default
        ]
        vars = {}

        with pytest.raises(MissingVarException, match="required_var"):
            set_vars_defaults(vars_spec, vars)

    def test_set_vars_defaults_partial_defaults(self):
        """Test: set_vars_defaults handles mix of defaults and provided."""
        vars_spec = [
            {"name": "env", "type": "str", "default": "development"},
            {"name": "count", "type": "int", "default": 10},
            {"name": "enabled", "type": "bool", "default": True},
        ]
        vars = {"count": 20}

        result = set_vars_defaults(vars_spec, vars)
        assert result["env"] == "development"  # default
        assert result["count"] == 20  # provided
        assert result["enabled"] is True  # default


class TestMergeConfigs:
    """Tests for config merging functionality."""

    def test_merge_configs_list_values(self):
        """Test: Two files with same resource type merge lists."""
        config1 = {"roles": [{"name": "role1"}]}
        config2 = {"roles": [{"name": "role2"}]}

        result = merge_configs(config1, config2)
        assert len(result["roles"]) == 2
        assert result["roles"][0]["name"] == "role1"
        assert result["roles"][1]["name"] == "role2"

    def test_merge_configs_different_keys(self):
        """Test: Different keys are combined."""
        config1 = {"databases": [{"name": "db1"}]}
        config2 = {"roles": [{"name": "role1"}]}

        result = merge_configs(config1, config2)
        assert "databases" in result
        assert "roles" in result
        assert len(result["databases"]) == 1
        assert len(result["roles"]) == 1

    def test_merge_configs_conflicting_scalars_raises_error(self):
        """Test: Conflicting scalar values raise error."""
        config1 = {"name": "blueprint1"}
        config2 = {"name": "blueprint2"}

        with pytest.raises(ValueError, match="conflict"):
            merge_configs(config1, config2)

    def test_merge_configs_none_value_replaced(self):
        """Test: None values can be replaced."""
        config1 = {"name": None}
        config2 = {"name": "blueprint"}

        result = merge_configs(config1, config2)
        assert result["name"] == "blueprint"

    def test_merge_configs_vars_spec_merge(self):
        """Test: vars_spec from multiple files merge correctly."""
        config1 = {"vars": [{"name": "var1", "type": "str", "default": "val1"}]}
        config2 = {"vars": [{"name": "var2", "type": "int", "default": 10}]}

        result = merge_configs(config1, config2)
        assert len(result["vars"]) == 2
        var_names = [v["name"] for v in result["vars"]]
        assert "var1" in var_names
        assert "var2" in var_names

    def test_merge_configs_empty_list_with_non_empty(self):
        """Test: Empty list merges with non-empty list."""
        config1 = {"roles": []}
        config2 = {"roles": [{"name": "role1"}]}

        result = merge_configs(config1, config2)
        assert len(result["roles"]) == 1


class TestSnowcapIgnore:
    """US-015: Tests for .snowcapignore pattern exclusion."""

    def test_snowcapignore_excludes_matching_files(self, tmp_path):
        """Test: .snowcapignore patterns exclude files correctly."""
        from snowcap.gitops import crawl

        # Create directory structure
        (tmp_path / "config.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "ignore_me.yaml").write_text("roles:\n  - name: role2\n")
        (tmp_path / ".snowcapignore").write_text("ignore_me.yaml\n")

        files = list(crawl(str(tmp_path)))
        file_names = [os.path.basename(f) for f in files]

        assert "config.yaml" in file_names
        assert "ignore_me.yaml" not in file_names

    def test_snowcapignore_wildcard_pattern(self, tmp_path):
        """Test: .snowcapignore supports wildcard patterns."""
        from snowcap.gitops import crawl

        # Create files
        (tmp_path / "keep.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "test_file.yaml").write_text("roles:\n  - name: role2\n")
        (tmp_path / "test_another.yaml").write_text("roles:\n  - name: role3\n")
        (tmp_path / ".snowcapignore").write_text("test_*.yaml\n")

        files = list(crawl(str(tmp_path)))
        file_names = [os.path.basename(f) for f in files]

        assert "keep.yaml" in file_names
        assert "test_file.yaml" not in file_names
        assert "test_another.yaml" not in file_names

    def test_snowcapignore_directory_pattern(self, tmp_path):
        """Test: .snowcapignore supports directory patterns."""
        from snowcap.gitops import crawl

        # Create directory structure
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "main.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "drafts").mkdir()
        (tmp_path / "drafts" / "draft.yaml").write_text("roles:\n  - name: role2\n")
        (tmp_path / ".snowcapignore").write_text("drafts/\n")

        files = list(crawl(str(tmp_path)))
        file_names = [os.path.basename(f) for f in files]

        assert "main.yaml" in file_names
        assert "draft.yaml" not in file_names

    def test_snowcapignore_no_file_collects_all(self, tmp_path):
        """Test: Without .snowcapignore, all YAML files are collected."""
        from snowcap.gitops import crawl

        # Create files without ignore file
        (tmp_path / "config1.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "config2.yaml").write_text("roles:\n  - name: role2\n")

        files = list(crawl(str(tmp_path)))
        file_names = [os.path.basename(f) for f in files]

        assert "config1.yaml" in file_names
        assert "config2.yaml" in file_names
        assert len(file_names) == 2

    def test_snowcapignore_comment_lines(self, tmp_path):
        """Test: .snowcapignore ignores comment lines starting with #."""
        from snowcap.gitops import crawl

        (tmp_path / "keep.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "ignore.yaml").write_text("roles:\n  - name: role2\n")
        (tmp_path / ".snowcapignore").write_text("# This is a comment\nignore.yaml\n# Another comment\n")

        files = list(crawl(str(tmp_path)))
        file_names = [os.path.basename(f) for f in files]

        assert "keep.yaml" in file_names
        assert "ignore.yaml" not in file_names


class TestNestedDirectoryCollection:
    """US-015: Tests for nested directory structure config collection."""

    def test_nested_directory_collects_all_configs(self, tmp_path):
        """Test: Nested directory structure collects all configs."""
        from snowcap.gitops import collect_configs_from_path

        # Create nested structure
        (tmp_path / "databases.yaml").write_text("databases:\n  - name: db1\n")
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "raw.yaml").write_text("schemas:\n  - name: raw\n")
        (tmp_path / "schemas" / "staging.yaml").write_text("schemas:\n  - name: staging\n")
        (tmp_path / "roles").mkdir()
        (tmp_path / "roles" / "analysts.yaml").write_text("roles:\n  - name: analyst\n")

        configs = collect_configs_from_path(str(tmp_path))

        assert len(configs) == 4
        # Extract just the file names for comparison
        file_names = [os.path.basename(path) for path, _ in configs]
        assert "databases.yaml" in file_names
        assert "raw.yaml" in file_names
        assert "staging.yaml" in file_names
        assert "analysts.yaml" in file_names

    def test_deeply_nested_directory(self, tmp_path):
        """Test: Deeply nested directory structure works."""
        from snowcap.gitops import collect_configs_from_path

        # Create deeply nested structure
        deep_path = tmp_path / "level1" / "level2" / "level3"
        deep_path.mkdir(parents=True)
        (deep_path / "deep_config.yaml").write_text("roles:\n  - name: deep_role\n")
        (tmp_path / "top.yaml").write_text("roles:\n  - name: top_role\n")

        configs = collect_configs_from_path(str(tmp_path))

        assert len(configs) == 2
        file_names = [os.path.basename(path) for path, _ in configs]
        assert "deep_config.yaml" in file_names
        assert "top.yaml" in file_names

    def test_yml_and_yaml_extensions(self, tmp_path):
        """Test: Both .yml and .yaml extensions are collected."""
        from snowcap.gitops import collect_configs_from_path

        (tmp_path / "config1.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "config2.yml").write_text("roles:\n  - name: role2\n")

        configs = collect_configs_from_path(str(tmp_path))

        assert len(configs) == 2
        file_names = [os.path.basename(path) for path, _ in configs]
        assert "config1.yaml" in file_names
        assert "config2.yml" in file_names

    def test_non_yaml_files_ignored(self, tmp_path):
        """Test: Non-YAML files are ignored."""
        from snowcap.gitops import collect_configs_from_path

        (tmp_path / "config.yaml").write_text("roles:\n  - name: role1\n")
        (tmp_path / "readme.md").write_text("# Documentation\n")
        (tmp_path / "script.py").write_text("print('hello')\n")
        (tmp_path / "data.json").write_text('{"key": "value"}\n')

        configs = collect_configs_from_path(str(tmp_path))

        assert len(configs) == 1
        assert os.path.basename(configs[0][0]) == "config.yaml"

    def test_empty_directory_raises_error(self, tmp_path):
        """Test: Directory with no YAML files raises error."""
        from snowcap.gitops import collect_configs_from_path

        (tmp_path / "readme.md").write_text("# No YAML here\n")

        with pytest.raises(ValueError, match="No valid YAML files"):
            collect_configs_from_path(str(tmp_path))

    def test_nested_with_snowcapignore(self, tmp_path):
        """Test: Nested directory respects .snowcapignore patterns."""
        from snowcap.gitops import collect_configs_from_path

        # Create structure with some files to ignore
        (tmp_path / "main.yaml").write_text("roles:\n  - name: main_role\n")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "keep.yaml").write_text("roles:\n  - name: keep_role\n")
        (tmp_path / "subdir" / "test_ignore.yaml").write_text("roles:\n  - name: ignore_role\n")
        (tmp_path / ".snowcapignore").write_text("**/test_*.yaml\n")

        configs = collect_configs_from_path(str(tmp_path))

        file_names = [os.path.basename(path) for path, _ in configs]
        assert "main.yaml" in file_names
        assert "keep.yaml" in file_names
        assert "test_ignore.yaml" not in file_names
        assert len(configs) == 2

    def test_single_file_path(self, tmp_path):
        """Test: Single file path works (not a directory)."""
        from snowcap.gitops import collect_configs_from_path

        config_file = tmp_path / "single.yaml"
        config_file.write_text("roles:\n  - name: single_role\n")

        configs = collect_configs_from_path(str(config_file))

        assert len(configs) == 1
        assert os.path.basename(configs[0][0]) == "single.yaml"

    def test_invalid_path_raises_error(self, tmp_path):
        """Test: Invalid path raises ValueError."""
        from snowcap.gitops import collect_configs_from_path

        with pytest.raises(ValueError, match="Invalid path"):
            collect_configs_from_path(str(tmp_path / "nonexistent"))


class TestBalboaYamlFixtures:
    """US-018: Tests for balboa-style YAML fixtures.

    These tests verify that the YAML fixtures representing real-world
    balboa patterns are valid and can be loaded by the config parser.
    """

    FIXTURES_DIR = os.path.join(
        os.path.dirname(__file__), "fixtures", "yaml"
    )

    def _load_fixture(self, filename):
        """Load a YAML fixture file and return the dict."""
        import yaml

        fixture_path = os.path.join(self.FIXTURES_DIR, filename)
        with open(fixture_path) as f:
            return yaml.safe_load(f)

    def test_balboa_databases_produces_resources(self):
        """Test: balboa_databases.yml produces Database resources."""
        config = self._load_fixture("balboa_databases.yml")
        blueprint_config = collect_blueprint_config(config)

        assert len(blueprint_config.resources) == 4
        names = [r.urn.fqn.name for r in blueprint_config.resources]
        assert "RAW_DB" in names
        assert "STAGING_DB" in names
        assert "ANALYTICS_DB" in names
        assert "DEV_DB" in names

    def test_balboa_schemas_uses_split_filter(self):
        """Test: balboa_schemas.yml uses split() to extract database and schema."""
        config = self._load_fixture("balboa_schemas.yml")
        blueprint_config = collect_blueprint_config(config)

        assert len(blueprint_config.resources) == 8
        # Check that schema names are correct (extracted via split)
        names = [r.urn.fqn.name for r in blueprint_config.resources]
        assert "SALESFORCE" in names
        assert "HUBSPOT" in names
        assert "CORE" in names
        assert "MARTS" in names

    def test_balboa_warehouses_uses_get_method(self):
        """Test: balboa_warehouses.yml uses .get() for default values."""
        config = self._load_fixture("balboa_warehouses.yml")
        blueprint_config = collect_blueprint_config(config)

        assert len(blueprint_config.resources) == 5
        names = [r.urn.fqn.name for r in blueprint_config.resources]
        assert "LOADING_WH" in names
        assert "TRANSFORMING_WH" in names
        assert "REPORTING_WH" in names

    def test_balboa_grants_multi_privilege_pattern(self):
        """Test: balboa_grants.yml uses multi-privilege pattern."""
        config = self._load_fixture("balboa_grants.yml")
        blueprint_config = collect_blueprint_config(config)

        # Grants should expand - each grant with privs: [A, B] becomes 2 grants
        assert len(blueprint_config.resources) > 0

    def test_balboa_roles_produces_hierarchy(self):
        """Test: balboa_roles.yml produces roles and role_grants."""
        config = self._load_fixture("balboa_roles.yml")
        blueprint_config = collect_blueprint_config(config)

        # Should have 9 roles (5 functional + 4 access)
        from snowcap.enums import ResourceType

        roles = [r for r in blueprint_config.resources if r.resource_type == ResourceType.ROLE]
        assert len(roles) == 9

        # Check that role_grants were created
        role_grants = [r for r in blueprint_config.resources if r.resource_type == ResourceType.ROLE_GRANT]
        assert len(role_grants) > 0

    def test_all_fixtures_can_be_loaded_by_parser(self):
        """Test: All balboa fixtures can be loaded by collect_blueprint_config."""
        import glob

        fixture_files = glob.glob(os.path.join(self.FIXTURES_DIR, "balboa_*.yml"))

        for fixture_path in fixture_files:
            config = self._load_fixture(os.path.basename(fixture_path))
            blueprint_config = collect_blueprint_config(config)
            assert len(blueprint_config.resources) > 0, f"{fixture_path} produced no resources"
