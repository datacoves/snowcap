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
    expected_resource_types = set([resource_cls.resource_type for resource_cls, _ in JSON_FIXTURES if resource_cls.resource_type.name != "COLUMN"])
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
