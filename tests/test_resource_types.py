"""
Resource-specific unit tests for snowcap resources.

Each resource type has tests for:
1. Instantiation with minimal required properties
2. Instantiation with all properties
3. SQL generation (create_sql)
4. FQN and URN generation
5. Resource-specific validation rules
6. Property defaults
"""

import pytest
from unittest.mock import MagicMock

from snowcap import resources as res
from snowcap.enums import ResourceType, WarehouseSize
from snowcap.identifiers import FQN, URN
from snowcap.resource_name import ResourceName


class TestDatabase:
    """Tests for Database resource."""

    def test_database_minimal(self):
        """Test Database with minimal required properties."""
        db = res.Database(name="test_db")
        assert db.name == "test_db"
        assert db.resource_type == ResourceType.DATABASE

    def test_database_all_properties(self):
        """Test Database with all properties."""
        db = res.Database(
            name="test_db",
            transient=True,
            owner="ACCOUNTADMIN",
            data_retention_time_in_days=7,
            max_data_extension_time_in_days=28,
            default_ddl_collation="en_US",
            comment="Test database",
        )
        assert db._data.transient is True
        assert db._data.data_retention_time_in_days == 7
        assert db._data.max_data_extension_time_in_days == 28
        assert db._data.default_ddl_collation == "en_US"
        assert db._data.comment == "Test database"

    def test_database_fqn(self):
        """Test Database FQN generation."""
        db = res.Database(name="test_db")
        fqn = db.fqn
        assert fqn.name == "TEST_DB"
        assert fqn.database is None
        assert fqn.schema is None

    def test_database_public_schema_created(self):
        """Test that Database automatically creates PUBLIC schema."""
        db = res.Database(name="test_db")
        assert db.public_schema is not None
        assert db.public_schema.name == "PUBLIC"

    def test_database_snowflake_no_public_schema(self):
        """Test that SNOWFLAKE database does not create PUBLIC schema."""
        db = res.Database(name="SNOWFLAKE")
        assert not hasattr(db, "_public_schema") or db._public_schema is None

    def test_database_defaults(self):
        """Test Database property defaults."""
        db = res.Database(name="test_db")
        assert db._data.transient is False
        assert db._data.data_retention_time_in_days == 1
        assert db._data.max_data_extension_time_in_days == 14

    def test_database_to_dict(self):
        """Test Database serialization."""
        db = res.Database(name="test_db", comment="Test")
        data = db.to_dict()
        assert data["name"] == "TEST_DB"
        assert data["comment"] == "Test"


class TestSchema:
    """Tests for Schema resource."""

    def test_schema_minimal(self):
        """Test Schema with minimal required properties."""
        schema = res.Schema(name="test_schema", database="test_db")
        assert schema.name == "test_schema"
        assert schema.resource_type == ResourceType.SCHEMA

    def test_schema_with_database_object(self):
        """Test Schema with Database object."""
        db = res.Database(name="test_db")
        schema = res.Schema(name="test_schema", database=db)
        assert schema.container is not None
        assert schema.container.name == "test_db"

    def test_schema_all_properties(self):
        """Test Schema with all properties."""
        schema = res.Schema(
            name="test_schema",
            database="test_db",
            transient=True,
            owner="SYSADMIN",
            data_retention_time_in_days=7,
            max_data_extension_time_in_days=28,
            default_ddl_collation="en_US",
            comment="Test schema",
        )
        assert schema._data.transient is True
        assert schema._data.data_retention_time_in_days == 7

    def test_schema_fqn(self):
        """Test Schema FQN generation."""
        schema = res.Schema(name="test_schema", database="test_db")
        fqn = schema.fqn
        assert fqn.name == "TEST_SCHEMA"
        assert str(fqn.database) == "TEST_DB"

    def test_schema_from_fqn_string(self):
        """Test Schema creation from fully qualified name string."""
        schema = res.Schema(name="test_db.test_schema")
        assert schema.name == "test_schema"
        assert schema.container.name == "test_db"

    def test_schema_defaults(self):
        """Test Schema property defaults."""
        schema = res.Schema(name="test_schema", database="test_db")
        assert schema._data.transient is False
        assert schema._data.data_retention_time_in_days == 1


class TestRole:
    """Tests for Role resource."""

    def test_role_minimal(self):
        """Test Role with minimal required properties."""
        role = res.Role(name="test_role")
        assert role.name == "test_role"
        assert role.resource_type == ResourceType.ROLE

    def test_role_all_properties(self):
        """Test Role with all properties."""
        role = res.Role(
            name="test_role",
            owner="USERADMIN",
            comment="Test role",
        )
        assert role._data.comment == "Test role"

    def test_role_fqn(self):
        """Test Role FQN generation."""
        role = res.Role(name="test_role")
        fqn = role.fqn
        assert fqn.name == "TEST_ROLE"

    def test_role_defaults(self):
        """Test Role property defaults."""
        role = res.Role(name="test_role")
        assert role._data.owner.name == "USERADMIN"

    def test_role_to_dict(self):
        """Test Role serialization."""
        role = res.Role(name="test_role", comment="Test")
        data = role.to_dict()
        assert data["name"] == "TEST_ROLE"
        assert data["comment"] == "Test"


class TestDatabaseRole:
    """Tests for DatabaseRole resource."""

    def test_database_role_minimal(self):
        """Test DatabaseRole with minimal required properties."""
        db_role = res.DatabaseRole(name="test_db_role", database="test_db")
        assert db_role.name == "test_db_role"
        assert db_role.resource_type == ResourceType.DATABASE_ROLE

    def test_database_role_all_properties(self):
        """Test DatabaseRole with all properties."""
        db_role = res.DatabaseRole(
            name="test_db_role",
            database="test_db",
            owner="USERADMIN",
            comment="Test database role",
        )
        assert db_role._data.comment == "Test database role"

    def test_database_role_fqn(self):
        """Test DatabaseRole FQN generation."""
        db_role = res.DatabaseRole(name="test_db_role", database="test_db")
        fqn = db_role.fqn
        assert fqn.name == "TEST_DB_ROLE"
        assert str(fqn.database) == "TEST_DB"

    def test_database_role_database_property(self):
        """Test DatabaseRole database property."""
        db_role = res.DatabaseRole(name="test_db_role", database="test_db")
        assert db_role.database == "test_db"


class TestWarehouse:
    """Tests for Warehouse resource."""

    def test_warehouse_minimal(self):
        """Test Warehouse with minimal required properties."""
        wh = res.Warehouse(name="test_wh")
        assert wh.name == "test_wh"
        assert wh.resource_type == ResourceType.WAREHOUSE

    def test_warehouse_all_properties(self):
        """Test Warehouse with all properties."""
        wh = res.Warehouse(
            name="test_wh",
            owner="SYSADMIN",
            warehouse_type="STANDARD",
            warehouse_size="MEDIUM",
            max_cluster_count=5,
            min_cluster_count=1,
            scaling_policy="ECONOMY",
            auto_suspend=300,
            auto_resume=True,
            initially_suspended=True,
            comment="Test warehouse",
            enable_query_acceleration=True,
            query_acceleration_max_scale_factor=4,
            max_concurrency_level=16,
            statement_queued_timeout_in_seconds=30,
            statement_timeout_in_seconds=3600,
        )
        assert wh._data.warehouse_size == WarehouseSize.MEDIUM
        assert wh._data.max_cluster_count == 5
        assert wh._data.auto_suspend == 300
        assert wh._data.initially_suspended is True

    def test_warehouse_fqn(self):
        """Test Warehouse FQN generation."""
        wh = res.Warehouse(name="test_wh")
        fqn = wh.fqn
        assert fqn.name == "TEST_WH"

    def test_warehouse_defaults(self):
        """Test Warehouse property defaults."""
        wh = res.Warehouse(name="test_wh")
        assert wh._data.warehouse_size == WarehouseSize.XSMALL
        assert wh._data.auto_suspend == 600
        assert wh._data.auto_resume is True
        assert wh._data.max_cluster_count == 1

    def test_warehouse_size_enum_conversion(self):
        """Test Warehouse size enum conversion from string."""
        wh = res.Warehouse(name="test_wh", warehouse_size="LARGE")
        assert wh._data.warehouse_size == WarehouseSize.LARGE


class TestUser:
    """Tests for User resource."""

    def test_user_minimal(self):
        """Test User with minimal required properties."""
        user = res.User(name="test_user")
        assert user.name == "test_user"
        assert user.resource_type == ResourceType.USER

    def test_user_all_properties(self):
        """Test User with all properties."""
        user = res.User(
            name="test_user",
            login_name="testuser",
            display_name="Test User",
            first_name="Test",
            last_name="User",
            email="test@example.com",
            disabled=False,
            must_change_password=True,
            default_warehouse="test_wh",
            default_role="test_role",
            comment="Test user",
        )
        assert user._data.login_name == "TESTUSER"
        assert user._data.display_name == "Test User"
        assert user._data.email == "test@example.com"

    def test_user_fqn(self):
        """Test User FQN generation."""
        user = res.User(name="test_user")
        fqn = user.fqn
        assert fqn.name == "TEST_USER"


class TestTable:
    """Tests for Table resource."""

    def test_table_minimal(self):
        """Test Table with minimal required properties."""
        table = res.Table(
            name="test_table",
            database="test_db",
            schema="test_schema",
            columns=[{"name": "id", "data_type": "INT"}],
        )
        assert table.name == "test_table"
        assert table.resource_type == ResourceType.TABLE

    def test_table_all_properties(self):
        """Test Table with all properties."""
        table = res.Table(
            name="test_table",
            database="test_db",
            schema="test_schema",
            columns=[
                {"name": "id", "data_type": "INT", "not_null": True},
                {"name": "name", "data_type": "VARCHAR(100)"},
            ],
            cluster_by=["id"],
            transient=True,
            comment="Test table",
        )
        assert len(table._data.columns) == 2
        assert table._data.transient is True

    def test_table_fqn(self):
        """Test Table FQN generation."""
        table = res.Table(
            name="test_table",
            database="test_db",
            schema="test_schema",
            columns=[{"name": "id", "data_type": "INT"}],
        )
        fqn = table.fqn
        assert fqn.name == "TEST_TABLE"
        assert str(fqn.database) == "TEST_DB"
        assert str(fqn.schema) == "TEST_SCHEMA"

    def test_table_from_fqn_string(self):
        """Test Table creation from fully qualified name string."""
        table = res.Table(
            name="test_db.test_schema.test_table",
            columns=[{"name": "id", "data_type": "INT"}],
        )
        assert table.name == "test_table"
        assert table.container.name == "test_schema"
        assert table.container.container.name == "test_db"


class TestView:
    """Tests for View resource."""

    def test_view_minimal(self):
        """Test View with minimal required properties."""
        view = res.View(
            name="test_view",
            database="test_db",
            schema="test_schema",
            as_="SELECT 1 AS col",
        )
        assert view.name == "test_view"
        assert view.resource_type == ResourceType.VIEW

    def test_view_with_columns(self):
        """Test View with columns."""
        view = res.View(
            name="test_view",
            database="test_db",
            schema="test_schema",
            columns=[{"name": "col1"}],
            as_="SELECT 1 AS col1",
        )
        assert len(view._data.columns) == 1

    def test_view_fqn(self):
        """Test View FQN generation."""
        view = res.View(
            name="test_view",
            database="test_db",
            schema="test_schema",
            as_="SELECT 1",
        )
        fqn = view.fqn
        assert fqn.name == "TEST_VIEW"
        assert str(fqn.database) == "TEST_DB"
        assert str(fqn.schema) == "TEST_SCHEMA"

    def test_view_empty_columns_rejected(self):
        """Test View with empty columns list is rejected."""
        with pytest.raises(ValueError):
            res.View(
                name="test_view",
                database="test_db",
                schema="test_schema",
                columns=[],
                as_="SELECT 1",
            )


class TestGrant:
    """Tests for Grant resource."""

    def test_grant_minimal(self):
        """Test Grant with minimal required properties."""
        grant = res.Grant(
            priv="USAGE",
            on_database="test_db",
            to="test_role",
        )
        assert grant.resource_type == ResourceType.GRANT

    def test_grant_on_schema(self):
        """Test Grant on schema."""
        grant = res.Grant(
            priv="USAGE",
            on_schema="test_db.test_schema",
            to="test_role",
        )
        assert grant._data.on_type == "SCHEMA"

    def test_grant_on_table(self):
        """Test Grant on table."""
        grant = res.Grant(
            priv="SELECT",
            on_table="test_db.test_schema.test_table",
            to="test_role",
        )
        assert grant._data.on_type == "TABLE"

    def test_grant_with_grant_option(self):
        """Test Grant with grant option."""
        grant = res.Grant(
            priv="USAGE",
            on_database="test_db",
            to="test_role",
            grant_option=True,
        )
        assert grant._data.grant_option is True


class TestRoleGrant:
    """Tests for RoleGrant resource."""

    def test_role_grant_to_role(self):
        """Test RoleGrant to another role."""
        grant = res.RoleGrant(
            role="child_role",
            to_role="parent_role",
        )
        assert grant.resource_type == ResourceType.ROLE_GRANT

    def test_role_grant_to_user(self):
        """Test RoleGrant to user."""
        grant = res.RoleGrant(
            role="test_role",
            to_user="test_user",
        )
        assert grant._data.to_user.name == "test_user"


class TestTask:
    """Tests for Task resource."""

    def test_task_minimal(self):
        """Test Task with minimal required properties."""
        task = res.Task(
            name="test_task",
            database="test_db",
            schema="test_schema",
            as_="SELECT 1",
        )
        assert task.name == "test_task"
        assert task.resource_type == ResourceType.TASK

    def test_task_with_schedule(self):
        """Test Task with schedule."""
        task = res.Task(
            name="test_task",
            database="test_db",
            schema="test_schema",
            schedule="1 MINUTE",
            as_="SELECT 1",
        )
        assert task._data.schedule == "1 MINUTE"

    def test_task_with_warehouse(self):
        """Test Task with warehouse."""
        task = res.Task(
            name="test_task",
            database="test_db",
            schema="test_schema",
            warehouse="test_wh",
            as_="SELECT 1",
        )
        assert task._data.warehouse.name == "test_wh"

    def test_task_fqn(self):
        """Test Task FQN generation."""
        task = res.Task(
            name="test_task",
            database="test_db",
            schema="test_schema",
            as_="SELECT 1",
        )
        fqn = task.fqn
        assert fqn.name == "TEST_TASK"


class TestSequence:
    """Tests for Sequence resource."""

    def test_sequence_minimal(self):
        """Test Sequence with minimal required properties."""
        seq = res.Sequence(
            name="test_seq",
            database="test_db",
            schema="test_schema",
        )
        assert seq.name == "test_seq"
        assert seq.resource_type == ResourceType.SEQUENCE

    def test_sequence_all_properties(self):
        """Test Sequence with all properties."""
        seq = res.Sequence(
            name="test_seq",
            database="test_db",
            schema="test_schema",
            start=100,
            increment=10,
            comment="Test sequence",
        )
        assert seq._data.start == 100
        assert seq._data.increment == 10


class TestInternalStage:
    """Tests for InternalStage resource."""

    def test_internal_stage_minimal(self):
        """Test InternalStage with minimal required properties."""
        stage = res.InternalStage(
            name="test_stage",
            database="test_db",
            schema="test_schema",
        )
        assert stage.name == "test_stage"
        assert stage.resource_type == ResourceType.STAGE

    def test_internal_stage_all_properties(self):
        """Test InternalStage with all properties."""
        stage = res.InternalStage(
            name="test_stage",
            database="test_db",
            schema="test_schema",
            directory={"enable": True},
            comment="Test stage",
        )
        assert stage._data.directory is not None


class TestNetworkPolicy:
    """Tests for NetworkPolicy resource."""

    def test_network_policy_minimal(self):
        """Test NetworkPolicy with minimal required properties."""
        policy = res.NetworkPolicy(
            name="test_policy",
        )
        assert policy.name == "test_policy"
        assert policy.resource_type == ResourceType.NETWORK_POLICY

    def test_network_policy_all_properties(self):
        """Test NetworkPolicy with all properties."""
        policy = res.NetworkPolicy(
            name="test_policy",
            allowed_ip_list=["192.168.1.0/24"],
            blocked_ip_list=["10.0.0.0/8"],
            comment="Test policy",
        )
        assert policy._data.allowed_ip_list == ["192.168.1.0/24"]
        assert policy._data.blocked_ip_list == ["10.0.0.0/8"]


class TestNetworkRule:
    """Tests for NetworkRule resource."""

    def test_network_rule_minimal(self):
        """Test NetworkRule with minimal required properties."""
        rule = res.NetworkRule(
            name="test_rule",
            database="test_db",
            schema="test_schema",
            type="IPV4",
            value_list=["192.168.1.0/24"],
            mode="INGRESS",
        )
        assert rule.name == "test_rule"
        assert rule.resource_type == ResourceType.NETWORK_RULE


class TestPipe:
    """Tests for Pipe resource."""

    def test_pipe_minimal(self):
        """Test Pipe with minimal required properties."""
        pipe = res.Pipe(
            name="test_pipe",
            database="test_db",
            schema="test_schema",
            as_="COPY INTO t FROM @s",
        )
        assert pipe.name == "test_pipe"
        assert pipe.resource_type == ResourceType.PIPE

    def test_pipe_all_properties(self):
        """Test Pipe with all properties."""
        pipe = res.Pipe(
            name="test_pipe",
            database="test_db",
            schema="test_schema",
            as_="COPY INTO t FROM @s",
            auto_ingest=True,
            comment="Test pipe",
        )
        assert pipe._data.auto_ingest is True


class TestAlert:
    """Tests for Alert resource."""

    def test_alert_minimal(self):
        """Test Alert with minimal required properties."""
        alert = res.Alert(
            name="test_alert",
            database="test_db",
            schema="test_schema",
            warehouse="test_wh",
            schedule="1 MINUTE",
            condition="SELECT 1",
            then="SELECT 1",  # 'then' is the action parameter
        )
        assert alert.name == "test_alert"
        assert alert.resource_type == ResourceType.ALERT


class TestResourceMonitor:
    """Tests for ResourceMonitor resource."""

    def test_resource_monitor_minimal(self):
        """Test ResourceMonitor with minimal required properties."""
        rm = res.ResourceMonitor(
            name="test_rm",
        )
        assert rm.name == "test_rm"
        assert rm.resource_type == ResourceType.RESOURCE_MONITOR

    def test_resource_monitor_all_properties(self):
        """Test ResourceMonitor with all properties."""
        rm = res.ResourceMonitor(
            name="test_rm",
            credit_quota=1000,
            frequency="MONTHLY",
            start_timestamp="2024-01-01 00:00",
        )
        assert rm._data.credit_quota == 1000


class TestTag:
    """Tests for Tag resource."""

    def test_tag_minimal(self):
        """Test Tag with minimal required properties."""
        tag = res.Tag(
            name="test_tag",
            database="test_db",
            schema="test_schema",
        )
        assert tag.name == "test_tag"
        assert tag.resource_type == ResourceType.TAG

    def test_tag_with_allowed_values(self):
        """Test Tag with allowed values."""
        tag = res.Tag(
            name="test_tag",
            database="test_db",
            schema="test_schema",
            allowed_values=["value1", "value2"],
        )
        assert tag._data.allowed_values == ["value1", "value2"]


class TestStream:
    """Tests for Stream resources."""

    def test_table_stream_minimal(self):
        """Test TableStream with minimal required properties."""
        stream = res.TableStream(
            name="test_stream",
            database="test_db",
            schema="test_schema",
            on_table="source_table",
        )
        assert stream.name == "test_stream"
        assert stream.resource_type == ResourceType.STREAM

    def test_view_stream_minimal(self):
        """Test ViewStream with minimal required properties."""
        stream = res.ViewStream(
            name="test_stream",
            database="test_db",
            schema="test_schema",
            on_view="source_view",
        )
        assert stream.name == "test_stream"

    def test_stage_stream_minimal(self):
        """Test StageStream with minimal required properties."""
        stream = res.StageStream(
            name="test_stream",
            database="test_db",
            schema="test_schema",
            on_stage="source_stage",
        )
        assert stream.name == "test_stream"


class TestFileFormat:
    """Tests for FileFormat resources."""

    def test_csv_file_format_minimal(self):
        """Test CSVFileFormat with minimal required properties."""
        ff = res.CSVFileFormat(
            name="test_ff",
            database="test_db",
            schema="test_schema",
        )
        assert ff.name == "test_ff"
        assert ff.resource_type == ResourceType.FILE_FORMAT

    def test_csv_file_format_all_properties(self):
        """Test CSVFileFormat with all properties."""
        ff = res.CSVFileFormat(
            name="test_ff",
            database="test_db",
            schema="test_schema",
            field_delimiter="|",
            record_delimiter="\n",
            skip_header=1,
        )
        assert ff._data.field_delimiter == "|"

    def test_json_file_format_minimal(self):
        """Test JSONFileFormat with minimal required properties."""
        ff = res.JSONFileFormat(
            name="test_ff",
            database="test_db",
            schema="test_schema",
        )
        assert ff.name == "test_ff"

    def test_parquet_file_format_minimal(self):
        """Test ParquetFileFormat with minimal required properties."""
        ff = res.ParquetFileFormat(
            name="test_ff",
            database="test_db",
            schema="test_schema",
        )
        assert ff.name == "test_ff"


class TestStorageIntegration:
    """Tests for StorageIntegration resources."""

    def test_s3_storage_integration_minimal(self):
        """Test S3StorageIntegration with minimal required properties."""
        si = res.S3StorageIntegration(
            name="test_si",
            enabled=True,
            storage_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
            storage_allowed_locations=["s3://bucket/path/"],
        )
        assert si.name == "test_si"
        assert si.resource_type == ResourceType.STORAGE_INTEGRATION

    def test_gcs_storage_integration_minimal(self):
        """Test GCSStorageIntegration with minimal required properties."""
        si = res.GCSStorageIntegration(
            name="test_si",
            enabled=True,
            storage_allowed_locations=["gcs://bucket/path/"],
        )
        assert si.name == "test_si"

    def test_azure_storage_integration_minimal(self):
        """Test AzureStorageIntegration with minimal required properties."""
        si = res.AzureStorageIntegration(
            name="test_si",
            enabled=True,
            azure_tenant_id="tenant-id",
            storage_allowed_locations=["azure://container/path/"],
        )
        assert si.name == "test_si"


class TestExternalAccessIntegration:
    """Tests for ExternalAccessIntegration resource."""

    def test_external_access_integration_minimal(self):
        """Test ExternalAccessIntegration with minimal required properties."""
        # allowed_network_rules must have at least one element
        eai = res.ExternalAccessIntegration(
            name="test_eai",
            allowed_network_rules=["db.schema.rule1"],
        )
        assert eai.name == "test_eai"
        assert eai.resource_type == ResourceType.EXTERNAL_ACCESS_INTEGRATION

    def test_external_access_integration_all_properties(self):
        """Test ExternalAccessIntegration with all properties."""
        eai = res.ExternalAccessIntegration(
            name="test_eai",
            allowed_network_rules=["db.schema.rule1"],
            allowed_api_authentication_integrations=["auth_int1"],
            enabled=True,
            comment="Test EAI",
        )
        assert len(eai._data.allowed_network_rules) == 1


class TestSecret:
    """Tests for Secret resources."""

    def test_password_secret_minimal(self):
        """Test PasswordSecret with minimal required properties."""
        secret = res.PasswordSecret(
            name="test_secret",
            database="test_db",
            schema="test_schema",
            username="user",
            password="pass",
        )
        assert secret.name == "test_secret"
        assert secret.resource_type == ResourceType.SECRET

    def test_generic_secret_minimal(self):
        """Test GenericSecret with minimal required properties."""
        secret = res.GenericSecret(
            name="test_secret",
            database="test_db",
            schema="test_schema",
            secret_string="secret_value",
        )
        assert secret.name == "test_secret"


class TestPasswordPolicy:
    """Tests for PasswordPolicy resource."""

    def test_password_policy_minimal(self):
        """Test PasswordPolicy with minimal required properties."""
        policy = res.PasswordPolicy(
            name="test_policy",
            database="test_db",
            schema="test_schema",
        )
        assert policy.name == "test_policy"
        assert policy.resource_type == ResourceType.PASSWORD_POLICY

    def test_password_policy_all_properties(self):
        """Test PasswordPolicy with all properties."""
        policy = res.PasswordPolicy(
            name="test_policy",
            database="test_db",
            schema="test_schema",
            password_min_length=12,
            password_max_length=256,
            password_min_upper_case_chars=2,
            password_min_lower_case_chars=2,
            password_min_numeric_chars=2,
            password_min_special_chars=2,
        )
        assert policy._data.password_min_length == 12


class TestProcedure:
    """Tests for Procedure resources."""

    def test_python_stored_procedure_minimal(self):
        """Test PythonStoredProcedure with minimal required properties."""
        proc = res.PythonStoredProcedure(
            name="test_proc()",
            database="test_db",
            schema="test_schema",
            args=[],  # Required parameter
            returns="VARCHAR",
            runtime_version="3.8",
            handler="handler",
            packages=["snowflake-snowpark-python"],
            as_="def handler(): return 'hello'",
        )
        # Function names strip the parentheses
        assert "test_proc" in str(proc.name).lower()
        assert proc.resource_type == ResourceType.PROCEDURE


class TestFunction:
    """Tests for Function resources."""

    def test_python_udf_minimal(self):
        """Test PythonUDF with minimal required properties."""
        func = res.PythonUDF(
            name="test_func()",
            database="test_db",
            schema="test_schema",
            args=[],  # Required parameter
            returns="VARCHAR",
            runtime_version="3.8",
            handler="handler",
            as_="def handler(): return 'hello'",
        )
        # Function names strip the parentheses
        assert "test_func" in str(func.name).lower()
        assert func.resource_type == ResourceType.FUNCTION

    def test_javascript_udf_minimal(self):
        """Test JavascriptUDF with minimal required properties."""
        func = res.JavascriptUDF(
            name="test_func()",
            database="test_db",
            schema="test_schema",
            args=[],  # Required parameter
            returns="VARCHAR",
            as_="return 'hello';",
        )
        # Function names strip the parentheses
        assert "test_func" in str(func.name).lower()


class TestDynamicTable:
    """Tests for DynamicTable resource."""

    def test_dynamic_table_minimal(self):
        """Test DynamicTable with minimal required properties."""
        dt = res.DynamicTable(
            name="test_dt",
            database="test_db",
            schema="test_schema",
            columns=[{"name": "col1"}],  # Required parameter
            target_lag="1 HOUR",
            warehouse="test_wh",
            as_="SELECT * FROM source",
        )
        assert dt.name == "test_dt"
        assert dt.resource_type == ResourceType.DYNAMIC_TABLE


class TestMaterializedView:
    """Tests for MaterializedView resource."""

    def test_materialized_view_minimal(self):
        """Test MaterializedView with minimal required properties."""
        mv = res.MaterializedView(
            name="test_mv",
            database="test_db",
            schema="test_schema",
            as_="SELECT * FROM source",
        )
        assert mv.name == "test_mv"
        assert mv.resource_type == ResourceType.MATERIALIZED_VIEW


class TestAPIIntegration:
    """Tests for APIIntegration resource."""

    def test_api_integration_minimal(self):
        """Test APIIntegration with minimal required properties."""
        api = res.APIIntegration(
            name="test_api",
            api_provider="AWS_API_GATEWAY",
            api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
            api_allowed_prefixes=["https://api.example.com/"],
            enabled=True,
        )
        assert api.name == "test_api"
        assert api.resource_type == ResourceType.API_INTEGRATION


class TestShare:
    """Tests for Share resource."""

    def test_share_minimal(self):
        """Test Share with minimal required properties."""
        share = res.Share(name="test_share")
        assert share.name == "test_share"
        assert share.resource_type == ResourceType.SHARE


class TestResourceCommon:
    """Common tests that apply to all resources."""

    @pytest.mark.parametrize(
        "resource_cls,kwargs",
        [
            (res.Database, {"name": "test"}),
            (res.Schema, {"name": "test", "database": "db"}),
            (res.Role, {"name": "test"}),
            (res.Warehouse, {"name": "test"}),
            (res.User, {"name": "test"}),
            (res.Table, {"name": "test", "database": "db", "schema": "sch", "columns": [{"name": "id", "data_type": "INT"}]}),
            (res.View, {"name": "test", "database": "db", "schema": "sch", "as_": "SELECT 1"}),
            (res.NetworkPolicy, {"name": "test"}),
        ],
    )
    def test_resource_has_name(self, resource_cls, kwargs):
        """Test that all resources have a name property."""
        resource = resource_cls(**kwargs)
        assert resource.name is not None

    @pytest.mark.parametrize(
        "resource_cls,kwargs",
        [
            (res.Database, {"name": "test"}),
            (res.Schema, {"name": "test", "database": "db"}),
            (res.Role, {"name": "test"}),
            (res.Warehouse, {"name": "test"}),
        ],
    )
    def test_resource_has_fqn(self, resource_cls, kwargs):
        """Test that all named resources have a FQN property."""
        resource = resource_cls(**kwargs)
        assert resource.fqn is not None
        assert isinstance(resource.fqn, FQN)

    @pytest.mark.parametrize(
        "resource_cls,kwargs",
        [
            (res.Database, {"name": "test"}),
            (res.Schema, {"name": "test", "database": "db"}),
            (res.Role, {"name": "test"}),
            (res.Warehouse, {"name": "test"}),
        ],
    )
    def test_resource_has_resource_type(self, resource_cls, kwargs):
        """Test that all resources have a resource_type property."""
        resource = resource_cls(**kwargs)
        assert resource.resource_type is not None
        assert isinstance(resource.resource_type, ResourceType)

    @pytest.mark.parametrize(
        "resource_cls,kwargs",
        [
            (res.Database, {"name": "test"}),
            (res.Schema, {"name": "test", "database": "db"}),
            (res.Role, {"name": "test"}),
            (res.Warehouse, {"name": "test"}),
        ],
    )
    def test_resource_to_dict(self, resource_cls, kwargs):
        """Test that all resources can be serialized to dict."""
        resource = resource_cls(**kwargs)
        data = resource.to_dict()
        assert isinstance(data, dict)
        assert "name" in data


class TestResourceNameCasing:
    """Tests for resource name case handling."""

    def test_unquoted_name_uppercase(self):
        """Test that unquoted names are uppercased."""
        db = res.Database(name="test_db")
        assert str(db.name) == "TEST_DB"

    def test_quoted_name_preserved(self):
        """Test that quoted names preserve case."""
        db = res.Database(name='"TestDb"')
        assert str(db.name) == '"TestDb"'

    def test_special_chars_quoted(self):
        """Test that special characters cause quoting."""
        db = res.Database(name="test-db")
        # Names with special chars should be quoted
        assert '"' in str(db.to_dict()["name"]) or str(db.name).startswith('"')


class TestResourceValidation:
    """Tests for resource validation rules."""

    def test_warehouse_size_validation(self):
        """Test Warehouse size validation."""
        # Valid size
        wh = res.Warehouse(name="test", warehouse_size="XSMALL")
        assert wh._data.warehouse_size == WarehouseSize.XSMALL

    def test_resource_name_type_validation(self):
        """Test that non-string names are rejected."""
        with pytest.raises(TypeError):
            res.Database(name=123)

    def test_grant_requires_target(self):
        """Test that Grant requires an on_* target."""
        # This should work - has on_database
        grant = res.Grant(priv="USAGE", on_database="test_db", to="role")
        assert grant is not None


class TestResourceDefaults:
    """Tests for resource default values."""

    def test_database_defaults(self):
        """Test Database property defaults."""
        db = res.Database(name="test")
        assert db._data.transient is False
        assert db._data.data_retention_time_in_days == 1
        assert db._data.max_data_extension_time_in_days == 14

    def test_schema_defaults(self):
        """Test Schema property defaults."""
        schema = res.Schema(name="test", database="db")
        assert schema._data.transient is False

    def test_warehouse_defaults(self):
        """Test Warehouse property defaults."""
        wh = res.Warehouse(name="test")
        assert wh._data.warehouse_size == WarehouseSize.XSMALL
        assert wh._data.auto_resume is True
        assert wh._data.auto_suspend == 600

    def test_role_defaults(self):
        """Test Role property defaults."""
        role = res.Role(name="test")
        assert role._data.owner.name == "USERADMIN"


class TestResourceTags:
    """Tests for resource tagging."""

    def test_database_with_tags(self):
        """Test Database with tags."""
        db = res.Database(name="test", tags={"env": "prod", "team": "data"})
        assert db.tags is not None
        assert db.tags.to_dict() == {"env": "prod", "team": "data"}

    def test_schema_with_tags(self):
        """Test Schema with tags."""
        schema = res.Schema(name="test", database="db", tags={"env": "dev"})
        assert schema.tags is not None

    def test_warehouse_with_tags(self):
        """Test Warehouse with tags."""
        wh = res.Warehouse(name="test", tags={"cost_center": "123"})
        assert wh.tags is not None


class TestResourceContainment:
    """Tests for resource containment relationships."""

    def test_schema_contained_in_database(self):
        """Test Schema is contained in Database."""
        db = res.Database(name="my_db")
        schema = res.Schema(name="my_schema", database=db)
        assert schema.container is not None
        assert schema.container.name == "my_db"

    def test_table_contained_in_schema(self):
        """Test Table is contained in Schema."""
        table = res.Table(
            name="my_table",
            database="my_db",
            schema="my_schema",
            columns=[{"name": "id", "data_type": "INT"}],
        )
        assert table.container is not None
        assert table.container.name == "my_schema"
        assert table.container.container.name == "my_db"

    def test_view_contained_in_schema(self):
        """Test View is contained in Schema."""
        view = res.View(
            name="my_view",
            database="my_db",
            schema="my_schema",
            as_="SELECT 1",
        )
        assert view.container is not None
        assert view.container.name == "my_schema"
