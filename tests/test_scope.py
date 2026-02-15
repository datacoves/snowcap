"""
Unit tests for snowcap/scope.py

Tests for resource scope classes and containment logic.
"""

import pytest

from snowcap.scope import (
    ResourceScope,
    OrganizationScope,
    AccountScope,
    DatabaseScope,
    SchemaScope,
    TableScope,
    AnonymousScope,
    resource_can_be_contained_in,
)
from snowcap.identifiers import FQN
from snowcap.resource_name import ResourceName


class TestResourceScopeAbstract:
    """Tests for the abstract ResourceScope base class."""

    def test_resource_scope_fully_qualified_name_raises_not_implemented(self):
        """ResourceScope.fully_qualified_name() should raise NotImplementedError."""
        scope = ResourceScope()
        resource_name = ResourceName("test_resource")
        with pytest.raises(NotImplementedError):
            scope.fully_qualified_name(None, resource_name)


class TestOrganizationScope:
    """Tests for OrganizationScope class."""

    def test_organization_scope_instance(self):
        """OrganizationScope should be instantiable."""
        scope = OrganizationScope()
        assert isinstance(scope, ResourceScope)

    def test_fully_qualified_name_returns_fqn_with_name_only(self):
        """OrganizationScope.fully_qualified_name() returns FQN with only name."""
        scope = OrganizationScope()
        resource_name = ResourceName("my_org_resource")
        fqn = scope.fully_qualified_name(None, resource_name)
        assert isinstance(fqn, FQN)
        assert fqn.name == resource_name
        assert fqn.database is None
        assert fqn.schema is None

    def test_fully_qualified_name_ignores_container(self):
        """OrganizationScope ignores the container parameter."""
        scope = OrganizationScope()
        resource_name = ResourceName("test")
        # Pass any arbitrary container - it should be ignored
        fqn = scope.fully_qualified_name("ignored_container", resource_name)
        assert fqn.name == resource_name
        assert fqn.database is None


class TestAccountScope:
    """Tests for AccountScope class."""

    def test_account_scope_instance(self):
        """AccountScope should be instantiable."""
        scope = AccountScope()
        assert isinstance(scope, ResourceScope)

    def test_fully_qualified_name_returns_fqn_with_name_only(self):
        """AccountScope.fully_qualified_name() returns FQN with only name."""
        scope = AccountScope()
        resource_name = ResourceName("MY_WAREHOUSE")
        fqn = scope.fully_qualified_name(None, resource_name)
        assert isinstance(fqn, FQN)
        assert fqn.name == resource_name
        assert fqn.database is None
        assert fqn.schema is None

    def test_fully_qualified_name_ignores_container(self):
        """AccountScope ignores the container parameter."""
        scope = AccountScope()
        resource_name = ResourceName("MY_ROLE")
        fqn = scope.fully_qualified_name("ignored", resource_name)
        assert fqn.name == resource_name

    def test_account_scope_with_quoted_name(self):
        """AccountScope works with quoted resource names."""
        scope = AccountScope()
        resource_name = ResourceName('"My Mixed Case Name"')
        fqn = scope.fully_qualified_name(None, resource_name)
        assert fqn.name == resource_name


class TestDatabaseScope:
    """Tests for DatabaseScope class."""

    def test_database_scope_instance(self):
        """DatabaseScope should be instantiable."""
        scope = DatabaseScope()
        assert isinstance(scope, ResourceScope)

    def test_fully_qualified_name_with_database_container(self):
        """DatabaseScope.fully_qualified_name() includes database from container."""
        scope = DatabaseScope()
        resource_name = ResourceName("MY_SCHEMA")

        # Mock a database container
        class MockDatabase:
            name = ResourceName("MY_DATABASE")

        database = MockDatabase()
        fqn = scope.fully_qualified_name(database, resource_name)

        assert isinstance(fqn, FQN)
        assert fqn.name == resource_name
        assert fqn.database == ResourceName("MY_DATABASE")
        assert fqn.schema is None

    def test_fully_qualified_name_without_container(self):
        """DatabaseScope with None container returns FQN with None database."""
        scope = DatabaseScope()
        resource_name = ResourceName("MY_SCHEMA")
        fqn = scope.fully_qualified_name(None, resource_name)

        assert fqn.name == resource_name
        assert fqn.database is None
        assert fqn.schema is None

    def test_fully_qualified_name_str_representation(self):
        """DatabaseScope FQN should have correct string representation."""
        scope = DatabaseScope()
        resource_name = ResourceName("SCHEMA_NAME")

        class MockDatabase:
            name = ResourceName("DB_NAME")

        fqn = scope.fully_qualified_name(MockDatabase(), resource_name)
        # FQN string should be "DB_NAME.SCHEMA_NAME"
        assert str(fqn) == "DB_NAME.SCHEMA_NAME"


class TestSchemaScope:
    """Tests for SchemaScope class."""

    def test_schema_scope_instance(self):
        """SchemaScope should be instantiable."""
        scope = SchemaScope()
        assert isinstance(scope, ResourceScope)

    def test_fully_qualified_name_with_schema_container(self):
        """SchemaScope.fully_qualified_name() includes database and schema from container."""
        scope = SchemaScope()
        resource_name = ResourceName("MY_TABLE")

        # Mock a schema container with a database container
        class MockDatabase:
            name = ResourceName("MY_DATABASE")

        class MockSchema:
            name = ResourceName("MY_SCHEMA")
            container = MockDatabase()

        schema = MockSchema()
        fqn = scope.fully_qualified_name(schema, resource_name)

        assert isinstance(fqn, FQN)
        assert fqn.name == resource_name
        assert fqn.database == ResourceName("MY_DATABASE")
        assert fqn.schema == ResourceName("MY_SCHEMA")

    def test_fully_qualified_name_without_container(self):
        """SchemaScope with None container returns FQN with None database and schema."""
        scope = SchemaScope()
        resource_name = ResourceName("MY_VIEW")
        fqn = scope.fully_qualified_name(None, resource_name)

        assert fqn.name == resource_name
        assert fqn.database is None
        assert fqn.schema is None

    def test_fully_qualified_name_schema_without_database(self):
        """SchemaScope with schema but no database container."""
        scope = SchemaScope()
        resource_name = ResourceName("MY_PROC")

        class MockSchema:
            name = ResourceName("MY_SCHEMA")
            container = None

        fqn = scope.fully_qualified_name(MockSchema(), resource_name)

        assert fqn.name == resource_name
        assert fqn.database is None
        assert fqn.schema == ResourceName("MY_SCHEMA")

    def test_fully_qualified_name_str_representation(self):
        """SchemaScope FQN should have correct string representation."""
        scope = SchemaScope()
        resource_name = ResourceName("TABLE_NAME")

        class MockDatabase:
            name = ResourceName("DB")

        class MockSchema:
            name = ResourceName("SCH")
            container = MockDatabase()

        fqn = scope.fully_qualified_name(MockSchema(), resource_name)
        # FQN string should be "DB.SCH.TABLE_NAME"
        assert str(fqn) == "DB.SCH.TABLE_NAME"


class TestTableScope:
    """Tests for TableScope class."""

    def test_table_scope_instance(self):
        """TableScope should be instantiable."""
        scope = TableScope()
        assert isinstance(scope, ResourceScope)

    def test_fully_qualified_name_raises_not_implemented(self):
        """TableScope.fully_qualified_name() raises NotImplementedError (not yet implemented)."""
        scope = TableScope()
        resource_name = ResourceName("MY_COLUMN")
        with pytest.raises(NotImplementedError):
            scope.fully_qualified_name(None, resource_name)


class TestAnonymousScope:
    """Tests for AnonymousScope class."""

    def test_anonymous_scope_instance(self):
        """AnonymousScope should be instantiable."""
        scope = AnonymousScope()
        assert isinstance(scope, ResourceScope)

    def test_fully_qualified_name_returns_fqn_with_name_only(self):
        """AnonymousScope.fully_qualified_name() returns FQN with only name."""
        scope = AnonymousScope()
        resource_name = ResourceName("ANON_RESOURCE")
        fqn = scope.fully_qualified_name(None, resource_name)

        assert isinstance(fqn, FQN)
        assert fqn.name == resource_name
        assert fqn.database is None
        assert fqn.schema is None

    def test_anonymous_scope_ignores_container(self):
        """AnonymousScope ignores the container parameter."""
        scope = AnonymousScope()
        resource_name = ResourceName("test")
        fqn = scope.fully_qualified_name("ignored", resource_name)
        assert fqn.name == resource_name


class TestResourceCanBeContainedIn:
    """Tests for resource_can_be_contained_in() function."""

    def test_account_scope_in_account_container(self):
        """Account-scoped resource can be contained in Account container."""
        class MockResource:
            scope = AccountScope()

        class Account:
            pass

        result = resource_can_be_contained_in(MockResource(), Account())
        assert result is True

    def test_database_scope_in_database_container(self):
        """Database-scoped resource can be contained in Database container."""
        class MockResource:
            scope = DatabaseScope()

        class Database:
            pass

        result = resource_can_be_contained_in(MockResource(), Database())
        assert result is True

    def test_schema_scope_in_schema_container(self):
        """Schema-scoped resource can be contained in Schema container."""
        class MockResource:
            scope = SchemaScope()

        class Schema:
            pass

        result = resource_can_be_contained_in(MockResource(), Schema())
        assert result is True

    def test_account_scope_not_in_database_container(self):
        """Account-scoped resource cannot be contained in Database container."""
        class MockResource:
            scope = AccountScope()

        class Database:
            pass

        result = resource_can_be_contained_in(MockResource(), Database())
        assert result is False

    def test_database_scope_not_in_account_container(self):
        """Database-scoped resource cannot be contained in Account container."""
        class MockResource:
            scope = DatabaseScope()

        class Account:
            pass

        result = resource_can_be_contained_in(MockResource(), Account())
        assert result is False

    def test_schema_scope_not_in_database_container(self):
        """Schema-scoped resource cannot be contained in Database container."""
        class MockResource:
            scope = SchemaScope()

        class Database:
            pass

        result = resource_can_be_contained_in(MockResource(), Database())
        assert result is False

    def test_organization_scope_not_in_any_container(self):
        """Organization-scoped resource is not contained in standard containers."""
        class MockResource:
            scope = OrganizationScope()

        class Account:
            pass

        result = resource_can_be_contained_in(MockResource(), Account())
        assert result is False

    def test_anonymous_scope_not_in_any_container(self):
        """Anonymous-scoped resource is not contained in standard containers."""
        class MockResource:
            scope = AnonymousScope()

        class Database:
            pass

        result = resource_can_be_contained_in(MockResource(), Database())
        assert result is False

    def test_resource_pointer_container_account(self):
        """Resource pointer container with Account type works correctly."""
        from snowcap.enums import ResourceType

        class MockResource:
            scope = AccountScope()

        class ResourcePointer:
            resource_type = ResourceType.ACCOUNT

        result = resource_can_be_contained_in(MockResource(), ResourcePointer())
        assert result is True

    def test_resource_pointer_container_database(self):
        """Resource pointer container with Database type works correctly."""
        from snowcap.enums import ResourceType

        class MockResource:
            scope = DatabaseScope()

        class ResourcePointer:
            resource_type = ResourceType.DATABASE

        result = resource_can_be_contained_in(MockResource(), ResourcePointer())
        assert result is True

    def test_resource_pointer_container_schema(self):
        """Resource pointer container with Schema type works correctly."""
        from snowcap.enums import ResourceType

        class MockResource:
            scope = SchemaScope()

        class ResourcePointer:
            resource_type = ResourceType.SCHEMA

        result = resource_can_be_contained_in(MockResource(), ResourcePointer())
        assert result is True

    def test_resource_pointer_wrong_scope(self):
        """Resource pointer with wrong scope returns False."""
        from snowcap.enums import ResourceType

        class MockResource:
            scope = AccountScope()

        class ResourcePointer:
            resource_type = ResourceType.SCHEMA

        result = resource_can_be_contained_in(MockResource(), ResourcePointer())
        assert result is False


class TestScopeInheritance:
    """Tests for scope class inheritance."""

    def test_all_scopes_inherit_from_resource_scope(self):
        """All scope classes should inherit from ResourceScope."""
        assert issubclass(OrganizationScope, ResourceScope)
        assert issubclass(AccountScope, ResourceScope)
        assert issubclass(DatabaseScope, ResourceScope)
        assert issubclass(SchemaScope, ResourceScope)
        assert issubclass(TableScope, ResourceScope)
        assert issubclass(AnonymousScope, ResourceScope)

    def test_scope_instances_are_resource_scope_instances(self):
        """All scope instances should be instances of ResourceScope."""
        assert isinstance(OrganizationScope(), ResourceScope)
        assert isinstance(AccountScope(), ResourceScope)
        assert isinstance(DatabaseScope(), ResourceScope)
        assert isinstance(SchemaScope(), ResourceScope)
        assert isinstance(TableScope(), ResourceScope)
        assert isinstance(AnonymousScope(), ResourceScope)


class TestScopeEquality:
    """Tests for scope instance comparison."""

    def test_scope_instances_are_independent(self):
        """Each scope instantiation creates an independent instance."""
        scope1 = AccountScope()
        scope2 = AccountScope()
        # They are different instances
        assert scope1 is not scope2

    def test_different_scope_types_are_not_equal(self):
        """Different scope types are not equal to each other."""
        assert AccountScope() != DatabaseScope()
        assert DatabaseScope() != SchemaScope()
        assert OrganizationScope() != AnonymousScope()


class TestScopeWithRealResources:
    """Tests using real resource patterns (not mocks)."""

    def test_account_scope_with_warehouse_pattern(self):
        """Test AccountScope with a warehouse-like resource name."""
        scope = AccountScope()
        resource_name = ResourceName("COMPUTE_WH")
        fqn = scope.fully_qualified_name(None, resource_name)
        assert str(fqn) == "COMPUTE_WH"

    def test_database_scope_with_schema_pattern(self):
        """Test DatabaseScope with a schema-like resource name pattern."""
        scope = DatabaseScope()
        resource_name = ResourceName("RAW")

        class MockDatabase:
            name = ResourceName("ANALYTICS")

        fqn = scope.fully_qualified_name(MockDatabase(), resource_name)
        assert str(fqn) == "ANALYTICS.RAW"

    def test_schema_scope_with_table_pattern(self):
        """Test SchemaScope with a table-like resource name pattern."""
        scope = SchemaScope()
        resource_name = ResourceName("CUSTOMERS")

        class MockDatabase:
            name = ResourceName("PROD")

        class MockSchema:
            name = ResourceName("PUBLIC")
            container = MockDatabase()

        fqn = scope.fully_qualified_name(MockSchema(), resource_name)
        assert str(fqn) == "PROD.PUBLIC.CUSTOMERS"

    def test_schema_scope_with_function_pattern(self):
        """Test SchemaScope with a function-like resource."""
        scope = SchemaScope()
        resource_name = ResourceName("GET_DATA")

        class MockDatabase:
            name = ResourceName("UTILS")

        class MockSchema:
            name = ResourceName("FUNCS")
            container = MockDatabase()

        fqn = scope.fully_qualified_name(MockSchema(), resource_name)
        assert str(fqn) == "UTILS.FUNCS.GET_DATA"
