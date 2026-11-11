from unittest.mock import patch

import pytest

import snowcap.resources as res

from snowcap.enums import ResourceType
from snowcap.identifiers import parse_identifier
from snowcap.resources.resource import ResourcePointer


def test_fqn_construction():
    ptr = ResourcePointer(name="my_db.my_schema.my_table", resource_type=ResourceType.TABLE)
    assert ptr.name == "my_table"
    assert ptr.container.name == "my_schema"
    assert ptr.container.container.name == "my_db"


def test_resource_pointer_type_checking():
    invalid_pointer = ResourcePointer(name="my_network_rule", resource_type=ResourceType.DATABASE)
    with pytest.raises(TypeError):
        res.NetworkPolicy(name="my_network_policy", allowed_network_rule_list=[invalid_pointer])

    with pytest.raises(TypeError):
        res.NetworkPolicy(name="my_network_policy", allowed_network_rule_list=[111])


def test_quoted_name_with_parenthesis_is_not_a_signature():
    """Only a function or procedure name carries argument types. A quoted identifier of
    any other type can hold a literal `(`."""
    ptr = ResourcePointer(name='"MY(POOL)"', resource_type=ResourceType.WAREHOUSE)
    assert ptr.fqn.arg_types is None
    assert str(ptr.fqn) == '"MY(POOL)"'
    assert ptr == ResourcePointer(name='"MY(POOL)"', resource_type=ResourceType.WAREHOUSE)


@pytest.mark.parametrize(
    "resource_type",
    [ResourceType.FUNCTION, ResourceType.PROCEDURE, ResourceType.EXTERNAL_FUNCTION],
)
def test_function_signature_is_part_of_identity(resource_type):
    ptr = ResourcePointer(name="DB.SCH.FN(NUMBER, VARCHAR)", resource_type=resource_type)
    assert ptr.fqn.arg_types == ["NUMBER", "VARCHAR"]
    assert str(ptr.fqn) == "DB.SCH.FN(NUMBER, VARCHAR)"
    assert ptr != ResourcePointer(name="DB.SCH.FN(NUMBER)", resource_type=resource_type)


def test_function_pointer_parses_its_name_once():
    name = "DB.SCH.FN(NUMBER)"
    with patch("snowcap.resources.resource.parse_identifier", wraps=parse_identifier) as spy:
        ResourcePointer(name=name, resource_type=ResourceType.FUNCTION)
    assert [call.args[0] for call in spy.call_args_list].count(name) == 1


def test_function_pointer_with_var_name():
    """Vars resolve after the pointer exists, so the pointer must not parse the raw var text."""
    ResourcePointer(name="{{ var.db }}.SCH.FN(VARCHAR)", resource_type=ResourceType.FUNCTION)
