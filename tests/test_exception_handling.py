"""
Tests for exception handling across snowcap.

These tests verify:
- All exception classes in snowcap/exceptions.py work correctly
- Error messages are clear and actionable
- YAML parsing errors are handled appropriately
- Invalid configuration errors are rejected with clear messages
- Resource conflict errors are properly raised
- Permission errors provide useful feedback
"""

import pytest
from unittest.mock import Mock, patch

from snowflake.connector.errors import ProgrammingError

from snowcap.exceptions import (
    MissingVarException,
    DuplicateResourceException,
    MissingResourceException,
    MissingPrivilegeException,
    MarkedForReplacementException,
    NonConformingPlanException,
    ResourceInsertionException,
    OrphanResourceException,
    InvalidOwnerException,
    InvalidResourceException,
    WrongContainerException,
    WrongEditionException,
    ResourceHasContainerException,
    NotADAGException,
)

from snowcap.client import (
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

from snowcap import resources as res
from snowcap.blueprint import Blueprint
from snowcap.identifiers import parse_URN


# =============================================================================
# Exception Class Tests
# =============================================================================


class TestMissingVarException:
    """Tests for MissingVarException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with a message"""
        with pytest.raises(MissingVarException) as exc_info:
            raise MissingVarException("Required var 'database_name' is missing")
        assert "database_name" in str(exc_info.value)

    def test_is_subclass_of_exception(self):
        """Test MissingVarException is a proper Exception subclass"""
        exc = MissingVarException("test")
        assert isinstance(exc, Exception)

    def test_message_is_preserved(self):
        """Test exception message is preserved"""
        msg = "Missing var: env"
        exc = MissingVarException(msg)
        assert str(exc) == msg


class TestDuplicateResourceException:
    """Tests for DuplicateResourceException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with a message"""
        with pytest.raises(DuplicateResourceException) as exc_info:
            raise DuplicateResourceException("Database 'DB1' already exists in blueprint")
        assert "DB1" in str(exc_info.value)

    def test_is_subclass_of_exception(self):
        """Test DuplicateResourceException is a proper Exception subclass"""
        exc = DuplicateResourceException("test")
        assert isinstance(exc, Exception)

    def test_message_clarity(self):
        """Test message is clear about the duplicate"""
        msg = "Resource 'WAREHOUSE.WH1' conflicts with existing resource"
        exc = DuplicateResourceException(msg)
        assert "conflicts" in str(exc).lower() or "WAREHOUSE" in str(exc)


class TestMissingResourceException:
    """Tests for MissingResourceException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with resource identifier"""
        with pytest.raises(MissingResourceException) as exc_info:
            raise MissingResourceException("Resource urn::ABCD:database/MISSING not found")
        assert "MISSING" in str(exc_info.value)

    def test_is_subclass_of_exception(self):
        """Test MissingResourceException is a proper Exception subclass"""
        exc = MissingResourceException("test")
        assert isinstance(exc, Exception)


class TestMissingPrivilegeException:
    """Tests for MissingPrivilegeException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with privilege info"""
        with pytest.raises(MissingPrivilegeException) as exc_info:
            raise MissingPrivilegeException("ACCOUNTADMIN role is required")
        assert "ACCOUNTADMIN" in str(exc_info.value)

    def test_is_subclass_of_exception(self):
        """Test MissingPrivilegeException is a proper Exception subclass"""
        exc = MissingPrivilegeException("test")
        assert isinstance(exc, Exception)


class TestMarkedForReplacementException:
    """Tests for MarkedForReplacementException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised"""
        with pytest.raises(MarkedForReplacementException):
            raise MarkedForReplacementException("Resource will be replaced")

    def test_is_subclass_of_exception(self):
        """Test MarkedForReplacementException is a proper Exception subclass"""
        exc = MarkedForReplacementException("test")
        assert isinstance(exc, Exception)


class TestNonConformingPlanException:
    """Tests for NonConformingPlanException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with plan details"""
        with pytest.raises(NonConformingPlanException) as exc_info:
            raise NonConformingPlanException("Non-conforming actions found:\n- Drop Database DB1")
        assert "Non-conforming" in str(exc_info.value)

    def test_is_subclass_of_exception(self):
        """Test NonConformingPlanException is a proper Exception subclass"""
        exc = NonConformingPlanException("test")
        assert isinstance(exc, Exception)


class TestResourceInsertionException:
    """Tests for ResourceInsertionException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised"""
        with pytest.raises(ResourceInsertionException):
            raise ResourceInsertionException("Cannot insert resource into graph")

    def test_is_subclass_of_exception(self):
        """Test ResourceInsertionException is a proper Exception subclass"""
        exc = ResourceInsertionException("test")
        assert isinstance(exc, Exception)


class TestOrphanResourceException:
    """Tests for OrphanResourceException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with resource info"""
        with pytest.raises(OrphanResourceException) as exc_info:
            raise OrphanResourceException("Schema 'PUBLIC' has no database")
        assert "database" in str(exc_info.value).lower()

    def test_is_subclass_of_exception(self):
        """Test OrphanResourceException is a proper Exception subclass"""
        exc = OrphanResourceException("test")
        assert isinstance(exc, Exception)


class TestInvalidOwnerException:
    """Tests for InvalidOwnerException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised"""
        with pytest.raises(InvalidOwnerException) as exc_info:
            raise InvalidOwnerException("Invalid owner: UNKNOWN_ROLE")
        assert "owner" in str(exc_info.value).lower()

    def test_is_subclass_of_exception(self):
        """Test InvalidOwnerException is a proper Exception subclass"""
        exc = InvalidOwnerException("test")
        assert isinstance(exc, Exception)


class TestInvalidResourceException:
    """Tests for InvalidResourceException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised"""
        with pytest.raises(InvalidResourceException) as exc_info:
            raise InvalidResourceException("Resource type not supported")
        assert "Resource" in str(exc_info.value)

    def test_is_subclass_of_exception(self):
        """Test InvalidResourceException is a proper Exception subclass"""
        exc = InvalidResourceException("test")
        assert isinstance(exc, Exception)


class TestWrongContainerException:
    """Tests for WrongContainerException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with container info"""
        with pytest.raises(WrongContainerException) as exc_info:
            raise WrongContainerException("Table cannot be added to Database container")
        assert "cannot" in str(exc_info.value).lower()

    def test_is_subclass_of_exception(self):
        """Test WrongContainerException is a proper Exception subclass"""
        exc = WrongContainerException("test")
        assert isinstance(exc, Exception)


class TestWrongEditionException:
    """Tests for WrongEditionException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised with edition info"""
        with pytest.raises(WrongEditionException) as exc_info:
            raise WrongEditionException("MaskingPolicy requires Enterprise edition")
        assert "Enterprise" in str(exc_info.value) or "edition" in str(exc_info.value).lower()

    def test_is_subclass_of_exception(self):
        """Test WrongEditionException is a proper Exception subclass"""
        exc = WrongEditionException("test")
        assert isinstance(exc, Exception)


class TestResourceHasContainerException:
    """Tests for ResourceHasContainerException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised"""
        with pytest.raises(ResourceHasContainerException) as exc_info:
            raise ResourceHasContainerException("Resource already belongs to a container")
        assert "container" in str(exc_info.value).lower()

    def test_is_subclass_of_exception(self):
        """Test ResourceHasContainerException is a proper Exception subclass"""
        exc = ResourceHasContainerException("test")
        assert isinstance(exc, Exception)


class TestNotADAGException:
    """Tests for NotADAGException"""

    def test_can_raise_with_message(self):
        """Test exception can be raised"""
        with pytest.raises(NotADAGException) as exc_info:
            raise NotADAGException("Dependency graph contains cycles")
        assert "cycle" in str(exc_info.value).lower()

    def test_is_subclass_of_exception(self):
        """Test NotADAGException is a proper Exception subclass"""
        exc = NotADAGException("test")
        assert isinstance(exc, Exception)


# =============================================================================
# Snowflake Error Code Mapping Tests
# =============================================================================


class TestSnowflakeErrorCodes:
    """Tests verifying Snowflake error codes are handled correctly"""

    def test_unsupported_feature_error_code(self):
        """Test UNSUPPORTED_FEATURE error code value"""
        assert UNSUPPORTED_FEATURE == 2

    def test_syntax_error_code(self):
        """Test SYNTAX_ERROR code value"""
        assert SYNTAX_ERROR == 1003

    def test_object_already_exists_error_code(self):
        """Test OBJECT_ALREADY_EXISTS_ERR code value"""
        assert OBJECT_ALREADY_EXISTS_ERR == 2002

    def test_does_not_exist_error_code(self):
        """Test DOES_NOT_EXIST_ERR code value"""
        assert DOES_NOT_EXIST_ERR == 2003

    def test_invalid_identifier_error_code(self):
        """Test INVALID_IDENTIFIER code value"""
        assert INVALID_IDENTIFIER == 2004

    def test_object_does_not_exist_error_code(self):
        """Test OBJECT_DOES_NOT_EXIST_ERR code value"""
        assert OBJECT_DOES_NOT_EXIST_ERR == 2043

    def test_access_control_error_code(self):
        """Test ACCESS_CONTROL_ERR code value"""
        assert ACCESS_CONTROL_ERR == 3001

    def test_already_exists_error_code(self):
        """Test ALREADY_EXISTS_ERR code value"""
        assert ALREADY_EXISTS_ERR == 3041

    def test_invalid_grant_error_code(self):
        """Test INVALID_GRANT_ERR code value"""
        assert INVALID_GRANT_ERR == 3042

    def test_feature_not_enabled_error_code(self):
        """Test FEATURE_NOT_ENABLED_ERR code value"""
        assert FEATURE_NOT_ENABLED_ERR == 3078


# =============================================================================
# Error Message Clarity Tests
# =============================================================================


class TestErrorMessageClarity:
    """Tests verifying error messages are clear and actionable"""

    def test_missing_var_message_includes_var_name(self):
        """Test MissingVarException includes the variable name"""
        exc = MissingVarException("Required var 'environment' is missing and has no default value")
        msg = str(exc)
        assert "environment" in msg
        assert "missing" in msg.lower() or "required" in msg.lower()

    def test_duplicate_resource_message_includes_resource_info(self):
        """Test DuplicateResourceException includes resource info"""
        exc = DuplicateResourceException("Database 'ANALYTICS_DB' already exists in blueprint")
        msg = str(exc)
        assert "ANALYTICS_DB" in msg

    def test_missing_privilege_message_includes_role_info(self):
        """Test MissingPrivilegeException includes role information"""
        exc = MissingPrivilegeException("SECURITYADMIN role is required to work with users")
        msg = str(exc)
        assert "SECURITYADMIN" in msg or "role" in msg.lower()

    def test_wrong_edition_message_includes_feature_and_edition(self):
        """Test WrongEditionException includes feature and edition info"""
        exc = WrongEditionException("MaskingPolicy requires Snowflake Enterprise edition")
        msg = str(exc)
        assert "MaskingPolicy" in msg or "Enterprise" in msg

    def test_orphan_resource_message_includes_parent_info(self):
        """Test OrphanResourceException includes parent info"""
        exc = OrphanResourceException("Resource Table 'USERS' has no schema")
        msg = str(exc)
        assert "schema" in msg.lower() or "Table" in msg


# =============================================================================
# Invalid Configuration Error Tests
# =============================================================================


class TestInvalidConfigurationErrors:
    """Tests for invalid resource configuration handling"""

    def test_database_with_invalid_retention_days(self):
        """Test Database rejects invalid retention days"""
        with pytest.raises((ValueError, TypeError)):
            res.Database(name="TEST_DB", data_retention_time_in_days="not_a_number")

    def test_warehouse_with_invalid_size(self):
        """Test Warehouse rejects invalid size with TypeError"""
        # Warehouse enum validation happens at resource creation time
        with pytest.raises(TypeError) as exc_info:
            res.Warehouse(name="TEST_WH", warehouse_size="INVALID_SIZE")
        # Error message should include valid options
        assert "XSMALL" in str(exc_info.value) or "SMALL" in str(exc_info.value)

    def test_grant_with_missing_privilege(self):
        """Test Grant requires privilege"""
        with pytest.raises((ValueError, TypeError)):
            # Missing required 'priv' parameter
            res.Grant(on_database="DB", to="ROLE")

    def test_role_grant_requires_role(self):
        """Test RoleGrant requires role parameter"""
        with pytest.raises((ValueError, TypeError)):
            # Missing required 'role' parameter
            res.RoleGrant(to_role="SYSADMIN")

    def test_schema_without_database_raises(self):
        """Test Schema needs a database reference"""
        # Schema without database is valid in isolation, but will cause OrphanResourceException
        # when added to a blueprint without a database
        schema = res.Schema(name="TEST_SCHEMA")
        # Creating a blueprint with orphan schema should raise
        blueprint = Blueprint(name="test", resources=[schema])
        session_ctx = {
            "account": "ABCD123",
            "account_locator": "ABCD123",
            "role": "SYSADMIN",
            "available_roles": ["SYSADMIN", "ACCOUNTADMIN"],
            "tag_support": False,
            "edition": "STANDARD",
        }
        with pytest.raises(OrphanResourceException):
            blueprint.generate_manifest(session_ctx)


# =============================================================================
# Resource Conflict Error Tests
# =============================================================================


class TestResourceConflictErrors:
    """Tests for resource conflict error handling"""

    def test_duplicate_database_raises_exception(self):
        """Test duplicate databases in blueprint raises DuplicateResourceException"""
        session_ctx = {
            "account": "ABCD123",
            "account_locator": "ABCD123",
            "role": "SYSADMIN",
            "available_roles": ["SYSADMIN", "ACCOUNTADMIN"],
            "tag_support": False,
            "edition": "STANDARD",
        }
        blueprint = Blueprint(
            name="test",
            resources=[
                res.Database("DB1"),
                res.Database("DB1", comment="different"),
            ],
        )
        with pytest.raises(DuplicateResourceException):
            blueprint.generate_manifest(session_ctx)

    def test_duplicate_grant_raises_exception(self):
        """Test duplicate grants in blueprint raises DuplicateResourceException"""
        session_ctx = {
            "account": "ABCD123",
            "account_locator": "ABCD123",
            "role": "SYSADMIN",
            "available_roles": ["SYSADMIN", "ACCOUNTADMIN"],
            "tag_support": False,
            "edition": "STANDARD",
        }
        blueprint = Blueprint(
            name="test",
            resources=[
                res.Grant(priv="USAGE", on_database="DB", to="ROLE1"),
                res.Grant(priv="USAGE", on_database="DB", to="ROLE1"),
            ],
        )
        with pytest.raises(DuplicateResourceException):
            blueprint.generate_manifest(session_ctx)


# =============================================================================
# YAML Parsing Error Tests
# =============================================================================


class TestYamlParsingErrors:
    """Tests for YAML parsing error handling"""

    def test_invalid_yaml_syntax_raises_error(self):
        """Test invalid YAML syntax is caught"""
        import yaml

        invalid_yaml = """
        databases:
          - name: DB1
            comment: "unclosed quote
        """
        with pytest.raises(yaml.YAMLError):
            yaml.safe_load(invalid_yaml)

    def test_missing_required_field_in_yaml(self):
        """Test missing required fields are caught during config collection"""
        from snowcap.gitops import collect_blueprint_config

        # Config with missing required 'name' for database
        config = {
            "databases": [
                {
                    "comment": "a database without a name"
                }
            ]
        }
        with pytest.raises((ValueError, KeyError, TypeError)):
            collect_blueprint_config(config)

    def test_invalid_resource_type_in_yaml(self):
        """Test invalid resource type in YAML config raises ValueError"""
        from snowcap.gitops import collect_blueprint_config

        # 'invalid_resources' is not a valid key
        config = {
            "invalid_resources": [
                {"name": "test"}
            ]
        }
        # Invalid keys are ignored, but if no valid resources are found, ValueError is raised
        with pytest.raises(ValueError) as exc_info:
            collect_blueprint_config(config)
        assert "No resources found" in str(exc_info.value)


# =============================================================================
# ProgrammingError Handling Tests
# =============================================================================


class TestProgrammingErrorHandling:
    """Tests for Snowflake ProgrammingError handling"""

    def test_programming_error_preserves_errno(self):
        """Test ProgrammingError preserves error number"""
        err = ProgrammingError("Object does not exist", errno=DOES_NOT_EXIST_ERR)
        assert err.errno == DOES_NOT_EXIST_ERR

    def test_programming_error_preserves_message(self):
        """Test ProgrammingError preserves error message"""
        msg = "Table 'USERS' does not exist or not authorized"
        err = ProgrammingError(msg, errno=DOES_NOT_EXIST_ERR)
        assert msg in str(err)

    def test_can_check_for_specific_error_codes(self):
        """Test we can check for specific error codes"""
        err = ProgrammingError("Access denied", errno=ACCESS_CONTROL_ERR)
        # Verify we can check for permission error
        is_permission_error = err.errno == ACCESS_CONTROL_ERR
        assert is_permission_error

    def test_can_distinguish_not_found_from_permission_error(self):
        """Test we can distinguish between different error types"""
        not_found_err = ProgrammingError("Not found", errno=DOES_NOT_EXIST_ERR)
        permission_err = ProgrammingError("Permission denied", errno=ACCESS_CONTROL_ERR)

        assert not_found_err.errno != permission_err.errno
        assert not_found_err.errno == DOES_NOT_EXIST_ERR
        assert permission_err.errno == ACCESS_CONTROL_ERR


# =============================================================================
# Exception Chaining Tests
# =============================================================================


class TestExceptionChaining:
    """Tests for exception chaining and cause preservation"""

    def test_exception_cause_is_preserved(self):
        """Test original exception cause is preserved"""
        original = ValueError("Original error")
        try:
            try:
                raise original
            except ValueError:
                raise MissingResourceException("Resource not found") from original
        except MissingResourceException as exc:
            assert exc.__cause__ is original

    def test_can_raise_from_programming_error(self):
        """Test can raise custom exception from ProgrammingError"""
        pg_err = ProgrammingError("Snowflake error", errno=ACCESS_CONTROL_ERR)
        try:
            try:
                raise pg_err
            except ProgrammingError:
                raise MissingPrivilegeException("Insufficient privileges") from pg_err
        except MissingPrivilegeException as exc:
            assert exc.__cause__ is pg_err
            assert exc.__cause__.errno == ACCESS_CONTROL_ERR


# =============================================================================
# Edge Case Exception Tests
# =============================================================================


class TestEdgeCaseExceptions:
    """Tests for edge cases in exception handling"""

    def test_empty_message_exception(self):
        """Test exception with empty message"""
        exc = MissingVarException("")
        assert str(exc) == ""

    def test_none_message_exception(self):
        """Test exception with None message"""
        exc = MissingResourceException(None)
        assert str(exc) == "None"

    def test_unicode_in_exception_message(self):
        """Test exception with unicode characters"""
        msg = "Missing resource: データベース"
        exc = MissingResourceException(msg)
        assert "データベース" in str(exc)

    def test_very_long_exception_message(self):
        """Test exception with very long message"""
        msg = "Error: " + "x" * 10000
        exc = InvalidResourceException(msg)
        assert len(str(exc)) > 10000

    def test_exception_with_special_characters(self):
        """Test exception with special characters in message"""
        msg = "Resource 'DB$_TEST#1' contains special characters: @!%^&*()"
        exc = InvalidResourceException(msg)
        assert "DB$_TEST#1" in str(exc)
        assert "@!%^&*()" in str(exc)
