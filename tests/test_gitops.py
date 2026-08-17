import pytest
from inflection import pluralize

from tests.helpers import get_json_fixtures
from snowcap.gitops import collect_blueprint_config
from snowcap.identifiers import resource_label_for_type

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture
def database_config() -> dict:
    return {
        "databases": [
            {
                "name": "test_database",
                "comment": "test database",
                "schemas": [
                    {
                        "name": "test_schema",
                        "comment": "test schema",
                    }
                ],
            }
        ]
    }


@pytest.fixture
def resource_config() -> dict:
    config = {}
    # Exclude COLUMN types - they are pseudo-resources embedded in tables, not collected via config
    for resource_cls, resource_config in JSON_FIXTURES:
        if resource_cls.resource_type.name == "COLUMN":
            continue
        key = pluralize(resource_label_for_type(resource_cls.resource_type))
        if key not in config:
            config[key] = []
        config[key].append(resource_config)

    return config


def test_database_config(database_config):
    blueprint_config = collect_blueprint_config(database_config)
    assert len(blueprint_config.resources) == 2


def test_resource_config(resource_config):
    bp_config = collect_blueprint_config(resource_config)
    resource_types = set([resource.resource_type for resource in bp_config.resources])
    # Exclude COLUMN types - they are pseudo-resources embedded in tables, not collected via config
    expected_resource_types = set(
        [resource_cls.resource_type for resource_cls, _ in JSON_FIXTURES if resource_cls.resource_type.name != "COLUMN"]
    )
    assert resource_types == expected_resource_types


def test_vars_type_validation(database_config):
    yaml_config = {
        "vars": [{"name": "foo", "type": "string"}],
        **database_config,
    }
    cli_config = {
        "vars": {"foo": 42},
    }
    with pytest.raises(TypeError):
        collect_blueprint_config(yaml_config, cli_config)

    yaml_config = {
        "vars": [{"name": "foo", "type": "int", "default": 0}],
        **database_config,
    }
    cli_config = {
        "vars": {"foo": "bar"},
    }
    with pytest.raises(TypeError):
        collect_blueprint_config(yaml_config, cli_config)


def test_vars_defaults(database_config):
    config = {
        "vars": [{"name": "foo", "default": "bar", "type": "string"}],
        **database_config,
    }
    blueprint_config = collect_blueprint_config(config)
    assert blueprint_config.vars["foo"] == "bar"


def test_for_each():
    config = {
        "vars": [{"name": "some_list_var", "default": ["bar", "baz"], "type": "list"}],
        "roles": [
            {
                "for_each": "var.some_list_var",
                "name": "role_{{ each.value}}",
            }
        ],
    }
    blueprint_config = collect_blueprint_config(config)
    assert blueprint_config.resources is not None
    assert len(blueprint_config.resources) == 2
    assert [resource.urn.fqn.name for resource in blueprint_config.resources] == ["role_bar", "role_baz"]


def test_for_each_where_filters_items():
    """`where` narrows a for_each to a subset without a second var.

    Lets one list drive several blocks that each cover part of it -- e.g. the
    same schema list granting on a source database and on a clone of it.
    """
    config = {
        "vars": [{"name": "schemas", "default": ["SRC.ONE", "SRC.TWO", "OTHER.THREE"], "type": "list"}],
        "roles": [
            {
                "for_each": "var.schemas",
                "where": "each.value.split('.')[0] == 'SRC'",
                "name": "role_{{ each.value.split('.')[1] }}",
            }
        ],
    }
    blueprint_config = collect_blueprint_config(config)
    assert blueprint_config.resources is not None
    assert [resource.urn.fqn.name for resource in blueprint_config.resources] == ["role_ONE", "role_TWO"]


def test_for_each_without_where_is_unfiltered():
    config = {
        "vars": [{"name": "schemas", "default": ["SRC.ONE", "OTHER.TWO"], "type": "list"}],
        "roles": [{"for_each": "var.schemas", "name": "role_{{ each.value.split('.')[1] }}"}],
    }
    blueprint_config = collect_blueprint_config(config)
    assert [resource.urn.fqn.name for resource in blueprint_config.resources] == ["role_ONE", "role_TWO"]


def test_for_each_where_rejects_var_reference():
    """var.* inside `where` used to resolve to a literal string and silently filter out every
    item (an empty block, which becomes DROPs in sync mode). It must raise instead."""
    from snowcap.var import evaluate_for_each_where
    from snowcap.exceptions import MissingVarException

    with pytest.raises(MissingVarException, match="each.value"):
        evaluate_for_each_where("each.value == var.target_region", "SRC.ONE")


def test_for_each_where_var_reference_does_not_silently_empty_the_block():
    """End to end: a var.* reference in `where` surfaces an error rather than declaring zero
    resources (which would drop the grants the block owns)."""
    config = {
        "vars": [{"name": "schemas", "default": ["SRC.ONE"], "type": "list"}],
        "roles": [{"for_each": "var.schemas", "where": "each.value == var.target", "name": "r_{{ each.value }}"}],
    }
    with pytest.raises(Exception):
        collect_blueprint_config(config)


class TestDatabaseRoleGrantsFromYaml:
    """A database role can be granted to an account role or to another database role.
    DatabaseRoleGrant and the SQL either side of it have always handled both; only this
    loader did not, so nesting was expressible in Python and not in config -- and an entry
    asking for it produced no resource rather than an error, so the grant never appeared in
    the plan at all."""

    def _build(self, config):
        from snowcap.gitops import _resources_from_database_role_grants_config

        return [r.create_sql() for r in _resources_from_database_role_grants_config(config)]

    def test_grant_to_an_account_role(self):
        assert self._build([{"database_role": "db.child", "to_role": "analyst"}]) == [
            "GRANT DATABASE ROLE DB.CHILD TO ROLE ANALYST"
        ]

    def test_grant_to_several_account_roles(self):
        """`roles` is the long-standing plural here and has to keep working."""
        assert self._build([{"database_role": "db.child", "roles": ["analyst", "loader"]}]) == [
            "GRANT DATABASE ROLE DB.CHILD TO ROLE ANALYST",
            "GRANT DATABASE ROLE DB.CHILD TO ROLE LOADER",
        ]

    def test_grant_to_another_database_role(self):
        assert self._build([{"database_role": "db.child", "to_database_role": "db.parent"}]) == [
            "GRANT DATABASE ROLE DB.CHILD TO DATABASE ROLE DB.PARENT"
        ]

    def test_grant_to_several_database_roles(self):
        assert self._build([{"database_role": "db.child", "database_roles": ["db.p1", "db.p2"]}]) == [
            "GRANT DATABASE ROLE DB.CHILD TO DATABASE ROLE DB.P1",
            "GRANT DATABASE ROLE DB.CHILD TO DATABASE ROLE DB.P2",
        ]

    def test_both_kinds_of_target_in_one_entry(self):
        assert self._build([{"database_role": "db.child", "roles": ["analyst"], "database_roles": ["db.parent"]}]) == [
            "GRANT DATABASE ROLE DB.CHILD TO ROLE ANALYST",
            "GRANT DATABASE ROLE DB.CHILD TO DATABASE ROLE DB.PARENT",
        ]

    def test_an_entry_that_grants_to_nothing_is_an_error(self):
        """This is what made the gap invisible: it used to yield no resource and no
        complaint, so the grant was simply missing from the plan."""
        with pytest.raises(ValueError, match="grants it to nothing"):
            self._build([{"database_role": "db.child"}])

    def test_a_misspelled_key_is_an_error(self):
        with pytest.raises(ValueError, match="to_rolez"):
            self._build([{"database_role": "db.child", "to_rolez": "analyst"}])

    def test_an_entry_without_a_database_role_is_an_error(self):
        with pytest.raises(ValueError, match="must specify"):
            self._build([{"to_role": "analyst"}])

    def test_empty_config_builds_nothing(self):
        assert self._build([]) == []

    @pytest.mark.parametrize(
        "config",
        [
            {"database_role": "db.child", "to_role": "analyst", "to_database_role": None},
            {"database_role": "db.child", "to_role": "analyst", "database_roles": None},
            {"database_role": "db.child", "to_role": "analyst", "roles": None},
        ],
    )
    def test_a_key_present_but_null_counts_as_absent(self, config):
        """YAML spells "not specified" as a key with nothing after it, and serialized
        configs round-trip unset fields as explicit nulls."""
        assert self._build([config]) == ["GRANT DATABASE ROLE DB.CHILD TO ROLE ANALYST"]
