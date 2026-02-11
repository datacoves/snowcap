"""
Tests for snowcap/client.py - SQL execution layer

These tests cover:
- execute() function with various connection types
- execute() caching behavior
- execute() error handling for all error codes
- execute_in_parallel() concurrent execution
- reset_cache() cache management
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from concurrent.futures import ThreadPoolExecutor

from snowflake.connector.errors import ProgrammingError
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

from snowcap.client import (
    execute,
    execute_in_parallel,
    reset_cache,
    _EXECUTION_CACHE,
    UNSUPPORTED_FEATURE,
    SYNTAX_ERROR,
    OBJECT_ALREADY_EXISTS_ERR,
    DOES_NOT_EXIST_ERR,
    INVALID_IDENTIFIER,
    OBJECT_DOES_NOT_EXIST_ERR,
    ACCESS_CONTROL_ERR,
    ALREADY_EXISTS_ERR,
    INVALID_GRANT_ERR,
    FEATURE_NOT_ENABLED_ERR,
)


class TestResetCache:
    """Tests for reset_cache() function"""

    def test_reset_cache_clears_execution_cache(self):
        """Test that reset_cache clears the global execution cache"""
        import snowcap.client

        # Add some data to the cache
        snowcap.client._EXECUTION_CACHE["TESTROLE"] = {"SELECT 1": [{"1": 1}]}

        # Reset the cache
        reset_cache()

        # Verify cache is empty
        assert snowcap.client._EXECUTION_CACHE == {}

    def test_reset_cache_clears_multiple_roles(self):
        """Test that reset_cache clears cache for all roles"""
        import snowcap.client

        # Add data for multiple roles
        snowcap.client._EXECUTION_CACHE["ROLE1"] = {"SELECT 1": [{"1": 1}]}
        snowcap.client._EXECUTION_CACHE["ROLE2"] = {"SELECT 2": [{"2": 2}]}

        reset_cache()

        assert snowcap.client._EXECUTION_CACHE == {}

    def test_reset_cache_on_empty_cache(self):
        """Test that reset_cache works when cache is already empty"""
        import snowcap.client

        snowcap.client._EXECUTION_CACHE = {}
        reset_cache()

        assert snowcap.client._EXECUTION_CACHE == {}


class TestExecuteBasic:
    """Tests for execute() basic functionality"""

    def setup_method(self):
        """Reset cache before each test"""
        reset_cache()

    def test_execute_with_snowflake_connection(self):
        """Test execute() with SnowflakeConnection creates DictCursor"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"col1": "value1"}]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        result = execute(mock_connection, "SELECT 1")

        # Verify DictCursor was requested
        mock_connection.cursor.assert_called()
        assert result == [{"col1": "value1"}]

    def test_execute_with_dict_cursor(self):
        """Test execute() with DictCursor uses it directly"""
        mock_connection = Mock()
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        mock_cursor = Mock(spec=DictCursor)
        mock_cursor.connection = mock_connection
        mock_cursor.fetchall.return_value = [{"result": 42}]

        result = execute(mock_cursor, "SELECT 42")

        assert result == [{"result": 42}]
        mock_cursor.execute.assert_called_once_with("SELECT 42")

    def test_execute_with_snowflake_cursor(self):
        """Test execute() with SnowflakeCursor sets _use_dict_result"""
        mock_connection = Mock()
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        mock_cursor = Mock(spec=SnowflakeCursor)
        mock_cursor.connection = mock_connection
        mock_cursor.fetchall.return_value = [{"data": "test"}]

        result = execute(mock_cursor, "SELECT 'test'")

        assert mock_cursor._use_dict_result == True
        assert result == [{"data": "test"}]

    def test_execute_with_cursor_like_object(self):
        """Test execute() with cursor-like object (has connection and execute)"""
        mock_connection = Mock()
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        mock_cursor = Mock()
        mock_cursor.connection = mock_connection
        mock_cursor.execute = Mock()
        mock_cursor.fetchall.return_value = [{"x": 1}]

        result = execute(mock_cursor, "SELECT x FROM t")

        assert mock_cursor._use_dict_result == True
        assert result == [{"x": 1}]

    def test_execute_returns_results(self):
        """Test execute() returns fetchall results"""
        mock_cursor = Mock()
        expected_results = [{"id": 1}, {"id": 2}, {"id": 3}]
        mock_cursor.fetchall.return_value = expected_results

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        result = execute(mock_connection, "SELECT id FROM table")

        assert result == expected_results

    def test_execute_invalid_sql_type_raises_exception(self):
        """Test execute() raises exception for non-string SQL"""
        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(Exception) as excinfo:
            execute(mock_connection, 12345)

        assert "Unknown sql type" in str(excinfo.value)


class TestExecuteUseRoleOptimization:
    """Tests for USE ROLE optimization in execute()"""

    def setup_method(self):
        """Reset cache before each test"""
        reset_cache()

    def test_execute_use_role_same_role_returns_early(self):
        """Test USE ROLE returns early if role matches current session role"""
        mock_cursor = Mock()
        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "SYSADMIN"

        result = execute(mock_connection, "USE ROLE SYSADMIN")

        # Should return early without executing
        mock_cursor.execute.assert_not_called()
        assert result == [[]]

    def test_execute_use_role_different_role_executes(self):
        """Test USE ROLE executes when role is different"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "SYSADMIN"

        result = execute(mock_connection, "USE ROLE ACCOUNTADMIN")

        mock_cursor.execute.assert_called_once()


class TestExecuteCaching:
    """Tests for execute() caching behavior"""

    def setup_method(self):
        """Reset cache before each test"""
        reset_cache()

    def test_execute_cacheable_stores_result(self):
        """Test execute() with cacheable=True stores result in cache"""
        import snowcap.client

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"cached": "value"}]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "TESTROLE"

        execute(mock_connection, "SELECT 'value'", cacheable=True)

        assert "TESTROLE" in snowcap.client._EXECUTION_CACHE
        assert "SELECT 'value'" in snowcap.client._EXECUTION_CACHE["TESTROLE"]
        assert snowcap.client._EXECUTION_CACHE["TESTROLE"]["SELECT 'value'"] == [{"cached": "value"}]

    def test_execute_cacheable_returns_cached_result(self):
        """Test execute() returns cached result without re-executing"""
        import snowcap.client

        # Pre-populate cache
        snowcap.client._EXECUTION_CACHE["TESTROLE"] = {"SELECT 'cached'": [{"result": "from_cache"}]}

        mock_cursor = Mock()
        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "TESTROLE"

        result = execute(mock_connection, "SELECT 'cached'", cacheable=True)

        # Should not execute, should return cached
        mock_cursor.execute.assert_not_called()
        assert result == [{"result": "from_cache"}]

    def test_execute_non_cacheable_does_not_cache(self):
        """Test execute() with cacheable=False (default) does not cache"""
        import snowcap.client

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"data": 1}]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "TESTROLE"

        execute(mock_connection, "SELECT 1")

        assert "TESTROLE" not in snowcap.client._EXECUTION_CACHE

    def test_execute_cache_per_role(self):
        """Test cache is maintained per role"""
        import snowcap.client

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"x": 1}]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"

        # Execute as ROLE1
        mock_connection.role = "ROLE1"
        execute(mock_connection, "SELECT x", cacheable=True)

        # Execute as ROLE2
        mock_cursor.fetchall.return_value = [{"x": 2}]
        mock_connection.role = "ROLE2"
        execute(mock_connection, "SELECT x", cacheable=True)

        assert "ROLE1" in snowcap.client._EXECUTION_CACHE
        assert "ROLE2" in snowcap.client._EXECUTION_CACHE
        assert snowcap.client._EXECUTION_CACHE["ROLE1"]["SELECT x"] == [{"x": 1}]
        assert snowcap.client._EXECUTION_CACHE["ROLE2"]["SELECT x"] == [{"x": 2}]


class TestExecuteErrorHandling:
    """Tests for execute() error handling"""

    def setup_method(self):
        """Reset cache before each test"""
        reset_cache()

    def test_execute_programming_error_raised(self):
        """Test execute() re-raises ProgrammingError with SQL context"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Object not found", errno=DOES_NOT_EXIST_ERR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(ProgrammingError) as excinfo:
            execute(mock_connection, "SELECT * FROM nonexistent")

        assert excinfo.value.errno == DOES_NOT_EXIST_ERR
        assert "SELECT * FROM nonexistent" in str(excinfo.value)

    def test_execute_empty_response_code_returns_empty_list(self):
        """Test execute() returns empty list for error codes in empty_response_codes"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Not found", errno=DOES_NOT_EXIST_ERR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        result = execute(
            mock_connection,
            "SELECT * FROM missing",
            empty_response_codes=[DOES_NOT_EXIST_ERR]
        )

        assert result == []

    def test_execute_empty_response_code_caches_empty_result(self):
        """Test execute() caches empty result for empty_response_codes errors when cacheable"""
        import snowcap.client

        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Not found", errno=DOES_NOT_EXIST_ERR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "TESTROLE"

        execute(
            mock_connection,
            "SELECT * FROM missing",
            cacheable=True,
            empty_response_codes=[DOES_NOT_EXIST_ERR]
        )

        assert snowcap.client._EXECUTION_CACHE["TESTROLE"]["SELECT * FROM missing"] == []

    def test_execute_error_unsupported_feature(self):
        """Test execute() handles UNSUPPORTED_FEATURE error"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Feature not supported", errno=UNSUPPORTED_FEATURE)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(ProgrammingError) as excinfo:
            execute(mock_connection, "CREATE MASKING POLICY ...")

        assert excinfo.value.errno == UNSUPPORTED_FEATURE

    def test_execute_error_syntax_error(self):
        """Test execute() handles SYNTAX_ERROR"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Syntax error", errno=SYNTAX_ERROR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(ProgrammingError) as excinfo:
            execute(mock_connection, "SELCT * FORM table")

        assert excinfo.value.errno == SYNTAX_ERROR

    def test_execute_error_object_already_exists(self):
        """Test execute() handles OBJECT_ALREADY_EXISTS_ERR"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Already exists", errno=OBJECT_ALREADY_EXISTS_ERR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(ProgrammingError) as excinfo:
            execute(mock_connection, "CREATE DATABASE existing_db")

        assert excinfo.value.errno == OBJECT_ALREADY_EXISTS_ERR

    def test_execute_error_access_control(self):
        """Test execute() handles ACCESS_CONTROL_ERR"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Insufficient privileges", errno=ACCESS_CONTROL_ERR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(ProgrammingError) as excinfo:
            execute(mock_connection, "DROP USER admin")

        assert excinfo.value.errno == ACCESS_CONTROL_ERR

    def test_execute_error_feature_not_enabled(self):
        """Test execute() handles FEATURE_NOT_ENABLED_ERR"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Feature not enabled", errno=FEATURE_NOT_ENABLED_ERR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        with pytest.raises(ProgrammingError) as excinfo:
            execute(mock_connection, "CREATE REPLICATION GROUP ...")

        assert excinfo.value.errno == FEATURE_NOT_ENABLED_ERR


class TestExecuteInParallel:
    """Tests for execute_in_parallel() function"""

    def setup_method(self):
        """Reset cache before each test"""
        reset_cache()

    def test_execute_in_parallel_returns_results(self):
        """Test execute_in_parallel yields results for each SQL"""
        mock_cursor = Mock()
        mock_cursor.fetchall.side_effect = [
            [{"count": 10}],
            [{"count": 20}],
            [{"count": 30}],
        ]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        sqls = [
            ("SELECT COUNT(*) FROM t1", "item1"),
            ("SELECT COUNT(*) FROM t2", "item2"),
            ("SELECT COUNT(*) FROM t3", "item3"),
        ]

        results = list(execute_in_parallel(mock_connection, sqls))

        assert len(results) == 3
        items = [item for item, result in results]
        assert set(items) == {"item1", "item2", "item3"}

    def test_execute_in_parallel_error_handler_called(self):
        """Test execute_in_parallel calls error_handler on exception"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Error", errno=SYNTAX_ERROR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        errors = []
        def error_handler(err, sql):
            errors.append((err, sql))

        sqls = [("BAD SQL", "item1")]

        list(execute_in_parallel(mock_connection, sqls, error_handler=error_handler))

        assert len(errors) == 1
        assert "BAD SQL" in errors[0][1]

    def test_execute_in_parallel_raises_without_error_handler(self):
        """Test execute_in_parallel raises exception when no error_handler"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = ProgrammingError("Error", errno=SYNTAX_ERROR)

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        sqls = [("BAD SQL", "item1")]

        with pytest.raises(ProgrammingError):
            list(execute_in_parallel(mock_connection, sqls))

    def test_execute_in_parallel_max_workers(self):
        """Test execute_in_parallel uses ThreadPoolExecutor with max_workers"""
        # This test verifies that execute_in_parallel uses ThreadPoolExecutor
        # We can't easily test max_workers without complex mocking, but we can
        # verify basic threading behavior works
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"x": 1}]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "testrole"

        # Simple test with max_workers=2
        sqls = [
            ("SELECT 1", "item1"),
            ("SELECT 2", "item2"),
        ]

        results = list(execute_in_parallel(mock_connection, sqls, max_workers=2))

        # Should get 2 results
        assert len(results) == 2

    def test_execute_in_parallel_cacheable_parameter(self):
        """Test execute_in_parallel passes cacheable parameter to execute"""
        import snowcap.client

        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [{"result": 1}]

        mock_connection = Mock(spec=SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_connection.user = "testuser"
        mock_connection.role = "TESTROLE"

        sqls = [("SELECT 1", "item1")]

        list(execute_in_parallel(mock_connection, sqls, cacheable=True))

        # Verify result was cached (execute was called with cacheable=True)
        assert "TESTROLE" in snowcap.client._EXECUTION_CACHE


class TestErrorCodeConstants:
    """Tests to verify error code constants are defined correctly"""

    def test_error_code_unsupported_feature(self):
        assert UNSUPPORTED_FEATURE == 2

    def test_error_code_syntax_error(self):
        assert SYNTAX_ERROR == 1003

    def test_error_code_object_already_exists(self):
        assert OBJECT_ALREADY_EXISTS_ERR == 2002

    def test_error_code_does_not_exist(self):
        assert DOES_NOT_EXIST_ERR == 2003

    def test_error_code_invalid_identifier(self):
        assert INVALID_IDENTIFIER == 2004

    def test_error_code_object_does_not_exist(self):
        assert OBJECT_DOES_NOT_EXIST_ERR == 2043

    def test_error_code_access_control(self):
        assert ACCESS_CONTROL_ERR == 3001

    def test_error_code_already_exists(self):
        assert ALREADY_EXISTS_ERR == 3041

    def test_error_code_invalid_grant(self):
        assert INVALID_GRANT_ERR == 3042

    def test_error_code_feature_not_enabled(self):
        assert FEATURE_NOT_ENABLED_ERR == 3078
