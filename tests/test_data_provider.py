"""
Unit tests for snowcap/data_provider.py

Tests the data provider functions without requiring a real Snowflake connection.
All functions are tested with mocked execute() results.
"""

import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from snowcap.data_provider import (
    # Helper functions
    _quote_snowflake_identifier,
    _get_owner_identifier,
    _desc_result_to_dict,
    _desc_type2_result_to_dict,
    _desc_type3_result_to_dict,
    _desc_type4_result_to_dict,
    _fail_if_not_granted,
    _filter_result,
    _convert_to_gmt,
    _parse_cluster_keys,
    _parse_function_arguments,
    _parse_function_arguments_2023_compat,
    _parse_list_property,
    _parse_signature,
    _parse_comma_separated_values,
    _parse_packages,
    _parse_storage_location,
    _cast_param_value,
    params_result_to_dict,
    options_result_to_list,
    remove_none_values,
    # Dispatcher functions
    fetch_resource,
    list_resource,
    list_account_scoped_resource,
    list_schema_scoped_resource,
    # Session functions
    fetch_account_locator,
    fetch_region,
)
from snowcap.identifiers import FQN, URN
from snowcap.resource_name import ResourceName
from snowcap.enums import ResourceType

import datetime
import pytz


class TestQuoteSnowflakeIdentifier:
    """Tests for _quote_snowflake_identifier helper function."""

    def test_uppercase_identifier(self):
        # Uppercase identifiers pass through without quotes
        result = _quote_snowflake_identifier("MY_TABLE")
        assert str(result) == "MY_TABLE"

    def test_lowercase_identifier_gets_quoted(self):
        # Lowercase identifiers from Snowflake metadata get quoted
        result = _quote_snowflake_identifier("my_table")
        assert str(result) == '"my_table"'

    def test_identifier_with_special_chars(self):
        result = _quote_snowflake_identifier("my-table")
        assert str(result) == '"my-table"'

    def test_already_quoted_identifier(self):
        result = _quote_snowflake_identifier('"MyTable"')
        assert str(result) == '"MyTable"'

    def test_string_input(self):
        # resource_name_from_snowflake_metadata requires string input
        result = _quote_snowflake_identifier("MY_SCHEMA")
        assert str(result) == "MY_SCHEMA"


class TestGetOwnerIdentifier:
    """Tests for _get_owner_identifier helper function."""

    def test_simple_owner(self):
        data = {"owner": "ACCOUNTADMIN"}
        result = _get_owner_identifier(data)
        assert result == "ACCOUNTADMIN"

    def test_database_role_owner(self):
        # Lowercase names from Snowflake metadata get quoted
        data = {
            "owner": "my_role",
            "owner_role_type": "DATABASE_ROLE",
            "database_name": "my_db"
        }
        result = _get_owner_identifier(data)
        assert result == '"my_db"."my_role"'

    def test_database_role_owner_uppercase(self):
        # Uppercase names pass through without quotes
        data = {
            "owner": "MY_ROLE",
            "owner_role_type": "DATABASE_ROLE",
            "database_name": "MY_DB"
        }
        result = _get_owner_identifier(data)
        assert result == "MY_DB.MY_ROLE"

    def test_role_type_owner(self):
        data = {
            "owner": "SYSADMIN",
            "owner_role_type": "ROLE"
        }
        result = _get_owner_identifier(data)
        assert result == "SYSADMIN"

    def test_empty_owner_with_role_type(self):
        data = {
            "owner": "",
            "owner_role_type": "ROLE"
        }
        result = _get_owner_identifier(data)
        assert result == ""

    def test_unsupported_owner_role_type_raises(self):
        data = {
            "owner": "my_role",
            "owner_role_type": "UNKNOWN_TYPE",
            "database_name": "my_db"
        }
        with pytest.raises(Exception, match="Unsupported owner role type"):
            _get_owner_identifier(data)


class TestDescResultToDict:
    """Tests for _desc_result_to_dict helper function."""

    def test_basic_desc_result(self):
        desc_result = [
            {"property": "NAME", "value": "test_table"},
            {"property": "TYPE", "value": "TABLE"},
        ]
        result = _desc_result_to_dict(desc_result)
        assert result == {"NAME": "test_table", "TYPE": "TABLE"}

    def test_lower_properties(self):
        desc_result = [
            {"property": "NAME", "value": "test_table"},
            {"property": "TYPE", "value": "TABLE"},
        ]
        result = _desc_result_to_dict(desc_result, lower_properties=True)
        assert result == {"name": "test_table", "type": "TABLE"}

    def test_empty_result(self):
        result = _desc_result_to_dict([])
        assert result == {}


class TestDescType2ResultToDict:
    """Tests for _desc_type2_result_to_dict helper function."""

    def test_boolean_property(self):
        desc_result = [
            {"property": "ENABLED", "property_value": "true", "property_type": "Boolean"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["ENABLED"] is True

    def test_boolean_false(self):
        desc_result = [
            {"property": "ENABLED", "property_value": "false", "property_type": "Boolean"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["ENABLED"] is False

    def test_long_property(self):
        desc_result = [
            {"property": "SIZE", "property_value": "1024", "property_type": "Long"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["SIZE"] == "1024"

    def test_long_empty_value(self):
        desc_result = [
            {"property": "SIZE", "property_value": "", "property_type": "Long"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["SIZE"] is None

    def test_integer_property(self):
        desc_result = [
            {"property": "COUNT", "property_value": "42", "property_type": "Integer"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["COUNT"] == 42

    def test_string_property(self):
        desc_result = [
            {"property": "NAME", "property_value": "my_name", "property_type": "String"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["NAME"] == "my_name"

    def test_string_empty_value(self):
        desc_result = [
            {"property": "NAME", "property_value": "", "property_type": "String"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["NAME"] is None

    def test_list_property(self):
        desc_result = [
            {"property": "ROLES", "property_value": "[role1, role2]", "property_type": "List"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["ROLES"] == ["role1", "role2"]

    def test_object_property(self):
        desc_result = [
            {"property": "CONFIG", "property_value": "[a, b, c]", "property_type": "Object"}
        ]
        result = _desc_type2_result_to_dict(desc_result)
        assert result["CONFIG"] == ["a", "b", "c"]


class TestDescType3ResultToDict:
    """Tests for _desc_type3_result_to_dict helper function."""

    def test_flat_property(self):
        desc_result = [
            {"parent_property": "", "property": "NAME", "property_value": "test", "property_type": "String"}
        ]
        result = _desc_type3_result_to_dict(desc_result)
        assert result["NAME"] == "test"

    def test_nested_property(self):
        desc_result = [
            {"parent_property": "CONFIG", "property": "SIZE", "property_value": "10", "property_type": "Integer"}
        ]
        result = _desc_type3_result_to_dict(desc_result)
        assert result["CONFIG"]["SIZE"] == 10

    def test_multiple_nested_properties(self):
        desc_result = [
            {"parent_property": "CONFIG", "property": "SIZE", "property_value": "10", "property_type": "Integer"},
            {"parent_property": "CONFIG", "property": "ENABLED", "property_value": "true", "property_type": "Boolean"},
        ]
        result = _desc_type3_result_to_dict(desc_result)
        assert result["CONFIG"]["SIZE"] == 10
        assert result["CONFIG"]["ENABLED"] is True


class TestDescType4ResultToDict:
    """Tests for _desc_type4_result_to_dict helper function."""

    def test_basic_result(self):
        desc_result = [
            {"name": "param1", "value": "value1"},
            {"name": "param2", "value": "value2"},
        ]
        result = _desc_type4_result_to_dict(desc_result)
        assert result == {"param1": "value1", "param2": "value2"}

    def test_lower_properties(self):
        desc_result = [
            {"name": "PARAM1", "value": "value1"},
        ]
        result = _desc_type4_result_to_dict(desc_result, lower_properties=True)
        assert result == {"param1": "value1"}


class TestFailIfNotGranted:
    """Tests for _fail_if_not_granted helper function."""

    def test_empty_result_raises(self):
        with pytest.raises(Exception, match="Failed to create grant"):
            _fail_if_not_granted([])

    def test_insufficient_privileges_raises(self):
        result = [{"status": "Grant not executed: Insufficient privileges."}]
        with pytest.raises(Exception, match="Insufficient privileges"):
            _fail_if_not_granted(result)

    def test_success_does_not_raise(self):
        result = [{"status": "Grant succeeded."}]
        _fail_if_not_granted(result)  # Should not raise


class TestFilterResult:
    """Tests for _filter_result helper function."""

    def test_filter_by_single_key(self):
        result = [
            {"name": "TABLE1", "type": "TABLE"},
            {"name": "TABLE2", "type": "VIEW"},
        ]
        filtered = _filter_result(result, type="TABLE")
        assert len(filtered) == 1
        assert filtered[0]["name"] == "TABLE1"

    def test_filter_by_name_resource_name(self):
        result = [
            {"name": "MY_TABLE", "type": "TABLE"},
            {"name": "OTHER_TABLE", "type": "TABLE"},
        ]
        filtered = _filter_result(result, name="my_table")
        assert len(filtered) == 1
        assert filtered[0]["name"] == "MY_TABLE"

    def test_filter_by_multiple_keys(self):
        result = [
            {"name": "TABLE1", "database_name": "DB1", "type": "TABLE"},
            {"name": "TABLE1", "database_name": "DB2", "type": "TABLE"},
            {"name": "TABLE2", "database_name": "DB1", "type": "VIEW"},
        ]
        filtered = _filter_result(result, name="TABLE1", database_name="DB1")
        assert len(filtered) == 1
        assert filtered[0]["database_name"] == "DB1"

    def test_filter_ignores_none_values(self):
        result = [
            {"name": "TABLE1", "type": "TABLE"},
            {"name": "TABLE2", "type": "VIEW"},
        ]
        filtered = _filter_result(result, name=None, type="TABLE")
        assert len(filtered) == 1


class TestConvertToGmt:
    """Tests for _convert_to_gmt helper function."""

    def test_convert_pst_to_gmt(self):
        pst = pytz.timezone("America/Los_Angeles")
        dt = pst.localize(datetime.datetime(2024, 1, 15, 12, 0, 0))
        result = _convert_to_gmt(dt)
        assert result == "2024-01-15 20:00:00"

    def test_none_input_returns_none(self):
        result = _convert_to_gmt(None)
        assert result is None

    def test_custom_format(self):
        pst = pytz.timezone("America/Los_Angeles")
        dt = pst.localize(datetime.datetime(2024, 1, 15, 12, 30, 45))
        result = _convert_to_gmt(dt, fmt_str="%Y-%m-%d %H:%M")
        assert result == "2024-01-15 20:30"


class TestParseClusterKeys:
    """Tests for _parse_cluster_keys helper function."""

    def test_simple_cluster_keys(self):
        result = _parse_cluster_keys("LINEAR(C1, C3)")
        assert result == ["C1", "C3"]

    def test_expression_cluster_keys(self):
        # Note: The function uses simple comma split, so nested expressions get split
        result = _parse_cluster_keys("LINEAR(SUBSTRING(C2, 5, 15), CAST(C1 AS DATE))")
        # This shows the current behavior - nested commas are also split
        assert len(result) > 0
        assert "SUBSTRING(C2" in result[0]

    def test_none_returns_none(self):
        result = _parse_cluster_keys(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = _parse_cluster_keys("")
        assert result is None


class TestParseFunctionArguments:
    """Tests for _parse_function_arguments helper function."""

    def test_simple_function(self):
        identifier, returns = _parse_function_arguments("FETCH_DATABASE(VARCHAR) RETURN OBJECT")
        # The FQN name field holds just the function name, sig is in the FQN
        assert str(identifier.name) == "FETCH_DATABASE"
        assert returns == "OBJECT"

    def test_multiple_arguments(self):
        identifier, returns = _parse_function_arguments("MY_FUNC(VARCHAR, NUMBER) RETURN TABLE")
        assert str(identifier.name) == "MY_FUNC"
        assert returns == "TABLE"

    def test_returns_fqn_with_arg_types(self):
        identifier, returns = _parse_function_arguments("FETCH_DATABASE(VARCHAR) RETURN OBJECT")
        # The FQN should contain the argument types
        assert identifier.arg_types == ["VARCHAR"]


class TestParseFunctionArguments2023Compat:
    """Tests for _parse_function_arguments_2023_compat helper function."""

    def test_optional_arguments(self):
        identifier, returns = _parse_function_arguments_2023_compat("FETCH_DATABASE(OBJECT [, BOOLEAN]) RETURN OBJECT")
        # Optional brackets are removed
        assert str(identifier.name) == "FETCH_DATABASE"
        assert identifier.arg_types == ["OBJECT", "BOOLEAN"]
        assert returns == "OBJECT"


class TestParseListProperty:
    """Tests for _parse_list_property helper function."""

    def test_simple_list(self):
        result = _parse_list_property("[a, b, c]")
        assert result == ["a", "b", "c"]

    def test_empty_list(self):
        result = _parse_list_property("[]")
        assert result == []

    def test_none_returns_none(self):
        result = _parse_list_property(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = _parse_list_property("")
        assert result is None

    def test_trims_whitespace(self):
        result = _parse_list_property("[ item1 ,  item2 ]")
        assert result == ["item1", "item2"]


class TestParseSignature:
    """Tests for _parse_signature helper function."""

    def test_simple_signature(self):
        result = _parse_signature("(col1 VARCHAR, col2 NUMBER)")
        assert len(result) == 2

    def test_empty_signature(self):
        result = _parse_signature("()")
        assert result == []


class TestParseCommaSeparatedValues:
    """Tests for _parse_comma_separated_values helper function."""

    def test_simple_values(self):
        result = _parse_comma_separated_values("a, b, c")
        assert result == ["a", "b", "c"]

    def test_none_returns_none(self):
        result = _parse_comma_separated_values(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = _parse_comma_separated_values("")
        assert result is None


class TestParsePackages:
    """Tests for _parse_packages helper function."""

    def test_simple_packages(self):
        result = _parse_packages("['numpy', 'pandas']")
        assert result == ["numpy", "pandas"]

    def test_none_returns_none(self):
        result = _parse_packages(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = _parse_packages("")
        assert result is None


class TestParseStorageLocation:
    """Tests for _parse_storage_location helper function."""

    def test_s3_storage_location(self):
        storage_str = '{"name": "loc1", "storage_provider": "S3", "storage_base_url": "s3://bucket/path", "storage_aws_role_arn": "arn:aws:iam::123:role/test"}'
        result = _parse_storage_location(storage_str)
        assert result["name"] == "loc1"
        assert result["storage_provider"] == "S3"
        assert result["storage_base_url"] == "s3://bucket/path"

    def test_with_encryption(self):
        storage_str = '{"name": "loc1", "storage_provider": "S3", "encryption_type": "SSE_S3"}'
        result = _parse_storage_location(storage_str)
        assert result["encryption"]["type"] == "SSE_S3"

    def test_none_returns_none(self):
        result = _parse_storage_location(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = _parse_storage_location("")
        assert result is None


class TestCastParamValue:
    """Tests for _cast_param_value helper function."""

    def test_boolean_true(self):
        result = _cast_param_value("true", "BOOLEAN")
        assert result is True

    def test_boolean_false(self):
        result = _cast_param_value("false", "BOOLEAN")
        assert result is False

    def test_number_integer(self):
        result = _cast_param_value("42", "NUMBER")
        assert result == 42
        assert isinstance(result, int)

    def test_number_float(self):
        result = _cast_param_value("3.14", "NUMBER")
        assert result == 3.14
        assert isinstance(result, float)

    def test_string(self):
        result = _cast_param_value("hello", "STRING")
        assert result == "hello"

    def test_string_empty(self):
        result = _cast_param_value("", "STRING")
        assert result is None

    def test_unknown_type_returns_raw(self):
        result = _cast_param_value("value", "UNKNOWN")
        assert result == "value"

    def test_invalid_number_raises(self):
        with pytest.raises(Exception, match="Unsupported number type"):
            _cast_param_value("not_a_number", "NUMBER")


class TestParamsResultToDict:
    """Tests for params_result_to_dict helper function."""

    def test_basic_params(self):
        params_result = [
            {"key": "PARAM1", "value": "true", "type": "BOOLEAN"},
            {"key": "PARAM2", "value": "42", "type": "NUMBER"},
            {"key": "PARAM3", "value": "hello", "type": "STRING"},
        ]
        result = params_result_to_dict(params_result)
        assert result["param1"] is True
        assert result["param2"] == 42
        assert result["param3"] == "hello"


class TestOptionsResultToList:
    """Tests for options_result_to_list helper function."""

    def test_simple_options(self):
        result = options_result_to_list("option1, option2, option3")
        assert result == ["option1", "option2", "option3"]


class TestRemoveNoneValues:
    """Tests for remove_none_values helper function."""

    def test_removes_none_at_top_level(self):
        d = {"a": 1, "b": None, "c": "hello"}
        result = remove_none_values(d)
        assert result == {"a": 1, "c": "hello"}

    def test_removes_none_in_nested_dict(self):
        d = {"a": {"b": 1, "c": None}}
        result = remove_none_values(d)
        assert result == {"a": {"b": 1}}

    def test_removes_none_in_list_of_dicts(self):
        d = {"items": [{"a": 1, "b": None}, {"c": 2}]}
        result = remove_none_values(d)
        assert result["items"][0] == {"a": 1}
        assert result["items"][1] == {"c": 2}

    def test_keeps_non_none_values(self):
        d = {"a": 0, "b": False, "c": ""}
        result = remove_none_values(d)
        assert result == {"a": 0, "b": False, "c": ""}


class TestFetchResource:
    """Tests for fetch_resource dispatcher function."""

    @patch("snowcap.data_provider.fetch_database")
    def test_dispatches_to_correct_fetch_function(self, mock_fetch_database):
        mock_fetch_database.return_value = {"name": "MY_DB"}
        mock_session = MagicMock()

        urn = URN(
            resource_type=ResourceType.DATABASE,
            account_locator="ABC123",
            fqn=FQN(name=ResourceName("MY_DB"))
        )

        result = fetch_resource(mock_session, urn)

        mock_fetch_database.assert_called_once_with(mock_session, urn.fqn)
        assert result == {"name": "MY_DB"}

    @patch("snowcap.data_provider.fetch_schema")
    def test_dispatches_to_schema_fetch(self, mock_fetch_schema):
        mock_fetch_schema.return_value = {"name": "MY_SCHEMA"}
        mock_session = MagicMock()

        urn = URN(
            resource_type=ResourceType.SCHEMA,
            account_locator="ABC123",
            fqn=FQN(database=ResourceName("MY_DB"), name=ResourceName("MY_SCHEMA"))
        )

        result = fetch_resource(mock_session, urn)

        mock_fetch_schema.assert_called_once()
        assert result == {"name": "MY_SCHEMA"}

    @patch("snowcap.data_provider.fetch_role")
    def test_returns_none_on_does_not_exist_error(self, mock_fetch_role):
        from snowflake.connector.errors import ProgrammingError
        mock_fetch_role.side_effect = ProgrammingError(errno=2003)
        mock_session = MagicMock()

        urn = URN(
            resource_type=ResourceType.ROLE,
            account_locator="ABC123",
            fqn=FQN(name=ResourceName("MISSING_ROLE"))
        )

        result = fetch_resource(mock_session, urn)
        assert result is None

    @patch("snowcap.data_provider.fetch_role")
    def test_raises_other_programming_errors(self, mock_fetch_role):
        from snowflake.connector.errors import ProgrammingError
        mock_fetch_role.side_effect = ProgrammingError(errno=1234)
        mock_session = MagicMock()

        urn = URN(
            resource_type=ResourceType.ROLE,
            account_locator="ABC123",
            fqn=FQN(name=ResourceName("MY_ROLE"))
        )

        with pytest.raises(ProgrammingError):
            fetch_resource(mock_session, urn)


class TestListResource:
    """Tests for list_resource dispatcher function."""

    @patch("snowcap.data_provider.list_databases")
    def test_dispatches_to_correct_list_function(self, mock_list_databases):
        mock_list_databases.return_value = [FQN(name=ResourceName("DB1")), FQN(name=ResourceName("DB2"))]
        mock_session = MagicMock()

        result = list_resource(mock_session, "database")

        mock_list_databases.assert_called_once_with(mock_session)
        assert len(result) == 2

    @patch("snowcap.data_provider.list_schemas")
    def test_dispatches_to_schemas_list(self, mock_list_schemas):
        mock_list_schemas.return_value = [FQN(database=ResourceName("DB1"), name=ResourceName("S1"))]
        mock_session = MagicMock()

        result = list_resource(mock_session, "schema")

        mock_list_schemas.assert_called_once_with(mock_session)
        assert len(result) == 1

    @patch("snowcap.data_provider.list_tables")
    def test_pluralizes_resource_label(self, mock_list_tables):
        mock_list_tables.return_value = []
        mock_session = MagicMock()

        list_resource(mock_session, "table")

        mock_list_tables.assert_called_once()


class TestListAccountScopedResource:
    """Tests for list_account_scoped_resource helper function."""

    @patch("snowcap.data_provider.execute")
    def test_lists_account_scoped_resources(self, mock_execute):
        mock_execute.return_value = [
            {"name": "RESOURCE1"},
            {"name": "RESOURCE2"},
        ]
        mock_session = MagicMock()

        result = list_account_scoped_resource(mock_session, "WAREHOUSES")

        mock_execute.assert_called_once_with(mock_session, "SHOW WAREHOUSES", cacheable=True)
        assert len(result) == 2
        assert str(result[0].name) == "RESOURCE1"
        assert str(result[1].name) == "RESOURCE2"


class TestListSchemaScopedResource:
    """Tests for list_schema_scoped_resource helper function."""

    @patch("snowcap.data_provider.execute")
    def test_lists_schema_scoped_resources(self, mock_execute):
        mock_execute.return_value = [
            {"name": "TABLE1", "database_name": "DB1", "schema_name": "SCHEMA1"},
            {"name": "TABLE2", "database_name": "DB1", "schema_name": "SCHEMA1"},
        ]
        mock_session = MagicMock()

        result = list_schema_scoped_resource(mock_session, "TABLES")

        mock_execute.assert_called_once_with(mock_session, "SHOW TABLES IN ACCOUNT", cacheable=True)
        assert len(result) == 2

    @patch("snowcap.data_provider.execute")
    def test_filters_system_databases(self, mock_execute):
        mock_execute.return_value = [
            {"name": "TABLE1", "database_name": "MY_DB", "schema_name": "SCHEMA1"},
            {"name": "SYS_TABLE", "database_name": "SNOWFLAKE", "schema_name": "ACCOUNT_USAGE"},
        ]
        mock_session = MagicMock()

        result = list_schema_scoped_resource(mock_session, "TABLES")

        assert len(result) == 1
        assert str(result[0].database) == "MY_DB"


class TestFetchAccountLocator:
    """Tests for fetch_account_locator function."""

    @patch("snowcap.data_provider.execute")
    def test_returns_account_locator(self, mock_execute):
        mock_execute.return_value = [{"ACCOUNT_LOCATOR": "ABC123"}]
        mock_session = MagicMock()

        result = fetch_account_locator(mock_session)

        assert result == "ABC123"


class TestFetchRegion:
    """Tests for fetch_region function."""

    @patch("snowcap.data_provider.execute")
    def test_returns_region(self, mock_execute):
        mock_execute.return_value = [{"CURRENT_REGION()": "AWS_US_WEST_2"}]
        mock_session = MagicMock()

        result = fetch_region(mock_session)

        assert result == {"CURRENT_REGION()": "AWS_US_WEST_2"}
