import pytest

from snowcap.privs import (
    PRIVS_FOR_RESOURCE_TYPE,
    CREATE_PRIV_FOR_RESOURCE_TYPE,
    GLOBAL_PRIV_DEFAULT_OWNERS,
    Priv,
    AccountPriv,
    AlertPriv,
    DatabasePriv,
    DatabaseRolePriv,
    DirectoryTablePriv,
    EventTablePriv,
    ExternalVolumePriv,
    FailoverGroupPriv,
    FileFormatPriv,
    FunctionPriv,
    IcebergTablePriv,
    IntegrationPriv,
    MaterializedViewPriv,
    NetworkPolicyPriv,
    NetworkRulePriv,
    NotebookPriv,
    PackagesPolicyPriv,
    PasswordPolicyPriv,
    PipePriv,
    ProcedurePriv,
    ReplicationGroupPriv,
    RolePriv,
    SchemaPriv,
    SecretPriv,
    SequencePriv,
    StagePriv,
    StreamPriv,
    StreamlitPriv,
    TablePriv,
    TagPriv,
    TaskPriv,
    UserPriv,
    ViewPriv,
    WarehousePriv,
    GrantedPrivilege,
    is_ownership_priv,
    all_privs_for_resource_type,
    system_role_for_priv,
)
from snowcap.enums import ResourceType


# Pseudo-resources and meta-resources that don't have associated privileges
# These are internal resources, grants, or container types that don't support GRANT statements
PSEUDO_RESOURCE_TYPES = {
    ResourceType.ACCOUNT_PARAMETER,
    ResourceType.DATABASE_ROLE_GRANT,
    ResourceType.EXTERNAL_TABLE,  # Uses different grant syntax via parent stage
    ResourceType.EXTERNAL_VOLUME_STORAGE_LOCATION,  # Subresource of external volume
    ResourceType.MASKING_POLICY,  # Not grantable directly, applied to columns
    ResourceType.SCANNER_PACKAGE,  # Internal service resource
}


def test_resource_privs_is_complete():
    for resource_type in ResourceType:
        if resource_type in PSEUDO_RESOURCE_TYPES:
            continue
        assert resource_type in PRIVS_FOR_RESOURCE_TYPE, f"{resource_type} is missing from PRIVS_FOR_RESOURCE_TYPE"


#############################################################################
# Priv Base Class Tests
#############################################################################


class TestPrivBaseClass:
    """Tests for the Priv base class (ParseableEnum subclass)."""

    def test_priv_is_parseable_enum(self):
        """Priv inherits from ParseableEnum."""
        from snowcap.enums import ParseableEnum
        assert issubclass(Priv, ParseableEnum)

    def test_priv_subclasses(self):
        """All privilege enums inherit from Priv."""
        priv_classes = [
            AccountPriv, AlertPriv, DatabasePriv, DatabaseRolePriv,
            DirectoryTablePriv, EventTablePriv, ExternalVolumePriv,
            FailoverGroupPriv, FileFormatPriv, FunctionPriv, IcebergTablePriv,
            IntegrationPriv, MaterializedViewPriv, NetworkPolicyPriv,
            NetworkRulePriv, NotebookPriv, PackagesPolicyPriv, PasswordPolicyPriv,
            PipePriv, ProcedurePriv, ReplicationGroupPriv, RolePriv, SchemaPriv,
            SecretPriv, SequencePriv, StagePriv, StreamPriv, StreamlitPriv,
            TablePriv, TagPriv, TaskPriv, UserPriv, ViewPriv, WarehousePriv,
        ]
        for priv_class in priv_classes:
            assert issubclass(priv_class, Priv), f"{priv_class.__name__} should inherit from Priv"


#############################################################################
# AccountPriv Tests
#############################################################################


class TestAccountPriv:
    """Tests for AccountPriv enum values."""

    def test_all_privilege(self):
        """AccountPriv has ALL privilege."""
        assert AccountPriv.ALL.value == "ALL"

    def test_create_database_privilege(self):
        """AccountPriv has CREATE DATABASE privilege."""
        assert AccountPriv.CREATE_DATABASE.value == "CREATE DATABASE"

    def test_create_warehouse_privilege(self):
        """AccountPriv has CREATE WAREHOUSE privilege."""
        assert AccountPriv.CREATE_WAREHOUSE.value == "CREATE WAREHOUSE"

    def test_create_role_privilege(self):
        """AccountPriv has CREATE ROLE privilege."""
        assert AccountPriv.CREATE_ROLE.value == "CREATE ROLE"

    def test_monitor_privilege(self):
        """AccountPriv has MONITOR privilege."""
        assert AccountPriv.MONITOR.value == "MONITOR"

    def test_manage_grants_privilege(self):
        """AccountPriv has MANAGE GRANTS privilege."""
        assert AccountPriv.MANAGE_GRANTS.value == "MANAGE GRANTS"

    def test_apply_masking_policy_privilege(self):
        """AccountPriv has APPLY MASKING POLICY privilege."""
        assert AccountPriv.APPLY_MASKING_POLICY.value == "APPLY MASKING POLICY"

    def test_account_priv_count(self):
        """AccountPriv has expected number of privileges."""
        # Should have at least 40 account-level privileges
        assert len(list(AccountPriv)) >= 40


#############################################################################
# DatabasePriv Tests
#############################################################################


class TestDatabasePriv:
    """Tests for DatabasePriv enum values."""

    def test_all_privilege(self):
        """DatabasePriv has ALL privilege."""
        assert DatabasePriv.ALL.value == "ALL"

    def test_usage_privilege(self):
        """DatabasePriv has USAGE privilege."""
        assert DatabasePriv.USAGE.value == "USAGE"

    def test_create_schema_privilege(self):
        """DatabasePriv has CREATE SCHEMA privilege."""
        assert DatabasePriv.CREATE_SCHEMA.value == "CREATE SCHEMA"

    def test_ownership_privilege(self):
        """DatabasePriv has OWNERSHIP privilege."""
        assert DatabasePriv.OWNERSHIP.value == "OWNERSHIP"

    def test_modify_privilege(self):
        """DatabasePriv has MODIFY privilege."""
        assert DatabasePriv.MODIFY.value == "MODIFY"

    def test_monitor_privilege(self):
        """DatabasePriv has MONITOR privilege."""
        assert DatabasePriv.MONITOR.value == "MONITOR"


#############################################################################
# SchemaPriv Tests
#############################################################################


class TestSchemaPriv:
    """Tests for SchemaPriv enum values."""

    def test_all_privilege(self):
        """SchemaPriv has ALL privilege."""
        assert SchemaPriv.ALL.value == "ALL"

    def test_usage_privilege(self):
        """SchemaPriv has USAGE privilege."""
        assert SchemaPriv.USAGE.value == "USAGE"

    def test_create_table_privilege(self):
        """SchemaPriv has CREATE TABLE privilege."""
        assert SchemaPriv.CREATE_TABLE.value == "CREATE TABLE"

    def test_create_view_privilege(self):
        """SchemaPriv has CREATE VIEW privilege."""
        assert SchemaPriv.CREATE_VIEW.value == "CREATE VIEW"

    def test_create_function_privilege(self):
        """SchemaPriv has CREATE FUNCTION privilege."""
        assert SchemaPriv.CREATE_FUNCTION.value == "CREATE FUNCTION"

    def test_schema_priv_count(self):
        """SchemaPriv has expected number of privileges."""
        # Should have at least 30 schema-level privileges
        assert len(list(SchemaPriv)) >= 30


#############################################################################
# TablePriv Tests
#############################################################################


class TestTablePriv:
    """Tests for TablePriv enum values."""

    def test_all_privilege(self):
        """TablePriv has ALL privilege."""
        assert TablePriv.ALL.value == "ALL"

    def test_select_privilege(self):
        """TablePriv has SELECT privilege."""
        assert TablePriv.SELECT.value == "SELECT"

    def test_insert_privilege(self):
        """TablePriv has INSERT privilege."""
        assert TablePriv.INSERT.value == "INSERT"

    def test_update_privilege(self):
        """TablePriv has UPDATE privilege."""
        assert TablePriv.UPDATE.value == "UPDATE"

    def test_delete_privilege(self):
        """TablePriv has DELETE privilege."""
        assert TablePriv.DELETE.value == "DELETE"

    def test_truncate_privilege(self):
        """TablePriv has TRUNCATE privilege."""
        assert TablePriv.TRUNCATE.value == "TRUNCATE"


#############################################################################
# WarehousePriv Tests
#############################################################################


class TestWarehousePriv:
    """Tests for WarehousePriv enum values."""

    def test_all_privilege(self):
        """WarehousePriv has ALL privilege."""
        assert WarehousePriv.ALL.value == "ALL"

    def test_usage_privilege(self):
        """WarehousePriv has USAGE privilege."""
        assert WarehousePriv.USAGE.value == "USAGE"

    def test_operate_privilege(self):
        """WarehousePriv has OPERATE privilege."""
        assert WarehousePriv.OPERATE.value == "OPERATE"

    def test_monitor_privilege(self):
        """WarehousePriv has MONITOR privilege."""
        assert WarehousePriv.MONITOR.value == "MONITOR"

    def test_modify_privilege(self):
        """WarehousePriv has MODIFY privilege."""
        assert WarehousePriv.MODIFY.value == "MODIFY"


#############################################################################
# StagePriv Tests
#############################################################################


class TestStagePriv:
    """Tests for StagePriv enum values."""

    def test_all_privilege(self):
        """StagePriv has ALL privilege."""
        assert StagePriv.ALL.value == "ALL"

    def test_read_privilege(self):
        """StagePriv has READ privilege."""
        assert StagePriv.READ.value == "READ"

    def test_write_privilege(self):
        """StagePriv has WRITE privilege."""
        assert StagePriv.WRITE.value == "WRITE"

    def test_usage_privilege(self):
        """StagePriv has USAGE privilege."""
        assert StagePriv.USAGE.value == "USAGE"


#############################################################################
# is_ownership_priv() Tests
#############################################################################


class TestIsOwnershipPriv:
    """Tests for the is_ownership_priv() function."""

    def test_ownership_priv_returns_true(self):
        """is_ownership_priv returns True for OWNERSHIP privilege."""
        assert is_ownership_priv(TablePriv.OWNERSHIP) is True
        assert is_ownership_priv(DatabasePriv.OWNERSHIP) is True
        assert is_ownership_priv(SchemaPriv.OWNERSHIP) is True

    def test_ownership_string_returns_true(self):
        """is_ownership_priv returns True for 'OWNERSHIP' string."""
        assert is_ownership_priv("OWNERSHIP") is True

    def test_non_ownership_priv_returns_false(self):
        """is_ownership_priv returns False for non-OWNERSHIP privileges."""
        assert is_ownership_priv(TablePriv.SELECT) is False
        assert is_ownership_priv(DatabasePriv.USAGE) is False
        assert is_ownership_priv(SchemaPriv.ALL) is False

    def test_non_ownership_string_returns_false(self):
        """is_ownership_priv returns False for non-OWNERSHIP strings."""
        assert is_ownership_priv("SELECT") is False
        assert is_ownership_priv("USAGE") is False
        assert is_ownership_priv("ALL") is False


#############################################################################
# all_privs_for_resource_type() Tests
#############################################################################


class TestAllPrivsForResourceType:
    """Tests for the all_privs_for_resource_type() function."""

    def test_database_privs(self):
        """all_privs_for_resource_type returns non-ALL/OWNERSHIP privileges for database."""
        privs = all_privs_for_resource_type(ResourceType.DATABASE)
        assert "USAGE" in privs
        assert "MODIFY" in privs
        assert "MONITOR" in privs
        assert "ALL" not in privs
        assert "OWNERSHIP" not in privs

    def test_table_privs(self):
        """all_privs_for_resource_type returns non-ALL/OWNERSHIP privileges for table."""
        privs = all_privs_for_resource_type(ResourceType.TABLE)
        assert "SELECT" in privs
        assert "INSERT" in privs
        assert "UPDATE" in privs
        assert "DELETE" in privs
        assert "ALL" not in privs
        assert "OWNERSHIP" not in privs

    def test_schema_privs(self):
        """all_privs_for_resource_type returns non-ALL/OWNERSHIP privileges for schema."""
        privs = all_privs_for_resource_type(ResourceType.SCHEMA)
        assert "USAGE" in privs
        assert "CREATE TABLE" in privs
        assert "CREATE VIEW" in privs
        assert "ALL" not in privs
        assert "OWNERSHIP" not in privs

    def test_resource_type_with_no_privs(self):
        """all_privs_for_resource_type returns empty list for resource types with no privileges."""
        # Grant is a pseudo-resource with no privileges
        privs = all_privs_for_resource_type(ResourceType.GRANT)
        assert privs == []

    def test_warehouse_privs(self):
        """all_privs_for_resource_type returns non-ALL/OWNERSHIP privileges for warehouse."""
        privs = all_privs_for_resource_type(ResourceType.WAREHOUSE)
        assert "USAGE" in privs
        assert "OPERATE" in privs
        assert "MODIFY" in privs
        assert "MONITOR" in privs
        assert "ALL" not in privs
        assert "OWNERSHIP" not in privs


#############################################################################
# system_role_for_priv() Tests
#############################################################################


class TestSystemRoleForPriv:
    """Tests for the system_role_for_priv() function."""

    def test_create_database_returns_accountadmin(self):
        """system_role_for_priv returns ACCOUNTADMIN for CREATE DATABASE."""
        assert system_role_for_priv("CREATE DATABASE") == "ACCOUNTADMIN"

    def test_create_role_returns_useradmin(self):
        """system_role_for_priv returns USERADMIN for CREATE ROLE."""
        assert system_role_for_priv("CREATE ROLE") == "USERADMIN"

    def test_create_user_returns_useradmin(self):
        """system_role_for_priv returns USERADMIN for CREATE USER."""
        assert system_role_for_priv("CREATE USER") == "USERADMIN"

    def test_create_warehouse_returns_sysadmin(self):
        """system_role_for_priv returns SYSADMIN for CREATE WAREHOUSE."""
        assert system_role_for_priv("CREATE WAREHOUSE") == "SYSADMIN"

    def test_manage_grants_returns_securityadmin(self):
        """system_role_for_priv returns SECURITYADMIN for MANAGE GRANTS."""
        assert system_role_for_priv("MANAGE GRANTS") == "SECURITYADMIN"

    def test_create_network_policy_returns_securityadmin(self):
        """system_role_for_priv returns SECURITYADMIN for CREATE NETWORK POLICY."""
        assert system_role_for_priv("CREATE NETWORK POLICY") == "SECURITYADMIN"

    def test_invalid_priv_returns_none(self):
        """system_role_for_priv returns None for invalid privilege."""
        assert system_role_for_priv("NOT_A_REAL_PRIVILEGE") is None

    def test_non_account_priv_returns_none(self):
        """system_role_for_priv returns None for non-account privileges."""
        # SELECT is a table privilege, not an account privilege
        assert system_role_for_priv("SELECT") is None


#############################################################################
# GrantedPrivilege Tests
#############################################################################


class TestGrantedPrivilege:
    """Tests for the GrantedPrivilege dataclass."""

    def test_init_with_priv_and_on(self):
        """GrantedPrivilege can be initialized with privilege and on."""
        gp = GrantedPrivilege(privilege=TablePriv.SELECT, on="MY_TABLE")
        assert gp.privilege == TablePriv.SELECT
        assert gp.on == "MY_TABLE"

    def test_init_with_string_privilege(self):
        """GrantedPrivilege can be initialized with string privilege."""
        gp = GrantedPrivilege(privilege="SELECT", on="MY_TABLE")
        assert gp.privilege == "SELECT"
        assert gp.on == "MY_TABLE"

    def test_from_grant_with_known_resource_type(self):
        """GrantedPrivilege.from_grant creates with proper Priv enum for known resource types."""
        gp = GrantedPrivilege.from_grant(privilege="SELECT", granted_on="TABLE", name="MY_TABLE")
        assert gp.privilege == TablePriv.SELECT
        assert gp.on == "MY_TABLE"

    def test_from_grant_with_database(self):
        """GrantedPrivilege.from_grant works with database resource type."""
        gp = GrantedPrivilege.from_grant(privilege="USAGE", granted_on="DATABASE", name="MY_DB")
        assert gp.privilege == DatabasePriv.USAGE
        assert gp.on == "MY_DB"

    def test_from_grant_with_schema(self):
        """GrantedPrivilege.from_grant works with schema resource type."""
        gp = GrantedPrivilege.from_grant(privilege="USAGE", granted_on="SCHEMA", name="MY_SCHEMA")
        assert gp.privilege == SchemaPriv.USAGE
        assert gp.on == "MY_SCHEMA"

    def test_from_grant_with_no_priv_type(self):
        """GrantedPrivilege.from_grant returns string privilege for resource types without privilege enums."""
        # GRANT resource type has no associated privilege enum
        gp = GrantedPrivilege.from_grant(privilege="SOME_PRIV", granted_on="GRANT", name="MY_GRANT")
        assert gp.privilege == "SOME_PRIV"
        assert gp.on == "GRANT"

    def test_repr(self):
        """GrantedPrivilege has a useful repr."""
        gp = GrantedPrivilege(privilege=TablePriv.SELECT, on="MY_TABLE")
        # Just verify it doesn't raise
        repr_str = repr(gp)
        assert "GrantedPrivilege" in repr_str


#############################################################################
# PRIVS_FOR_RESOURCE_TYPE Mapping Tests
#############################################################################


class TestPrivsForResourceTypeMapping:
    """Tests for the PRIVS_FOR_RESOURCE_TYPE mapping."""

    def test_database_maps_to_database_priv(self):
        """DATABASE resource type maps to DatabasePriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.DATABASE] == DatabasePriv

    def test_schema_maps_to_schema_priv(self):
        """SCHEMA resource type maps to SchemaPriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.SCHEMA] == SchemaPriv

    def test_table_maps_to_table_priv(self):
        """TABLE resource type maps to TablePriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.TABLE] == TablePriv

    def test_view_maps_to_view_priv(self):
        """VIEW resource type maps to ViewPriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.VIEW] == ViewPriv

    def test_warehouse_maps_to_warehouse_priv(self):
        """WAREHOUSE resource type maps to WarehousePriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.WAREHOUSE] == WarehousePriv

    def test_stage_maps_to_stage_priv(self):
        """STAGE resource type maps to StagePriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.STAGE] == StagePriv

    def test_function_maps_to_function_priv(self):
        """FUNCTION resource type maps to FunctionPriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.FUNCTION] == FunctionPriv

    def test_procedure_maps_to_procedure_priv(self):
        """PROCEDURE resource type maps to ProcedurePriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.PROCEDURE] == ProcedurePriv

    def test_grant_maps_to_none(self):
        """GRANT resource type maps to None (pseudo-resource)."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.GRANT] is None

    def test_integration_types_map_to_integration_priv(self):
        """Integration resource types map to IntegrationPriv."""
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.API_INTEGRATION] == IntegrationPriv
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.STORAGE_INTEGRATION] == IntegrationPriv
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.NOTIFICATION_INTEGRATION] == IntegrationPriv
        assert PRIVS_FOR_RESOURCE_TYPE[ResourceType.SECURITY_INTEGRATION] == IntegrationPriv


#############################################################################
# CREATE_PRIV_FOR_RESOURCE_TYPE Mapping Tests
#############################################################################


class TestCreatePrivForResourceTypeMapping:
    """Tests for the CREATE_PRIV_FOR_RESOURCE_TYPE mapping."""

    def test_database_create_priv(self):
        """DATABASE creation requires CREATE DATABASE account privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.DATABASE] == AccountPriv.CREATE_DATABASE

    def test_schema_create_priv(self):
        """SCHEMA creation requires CREATE SCHEMA database privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.SCHEMA] == DatabasePriv.CREATE_SCHEMA

    def test_table_create_priv(self):
        """TABLE creation requires CREATE TABLE schema privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.TABLE] == SchemaPriv.CREATE_TABLE

    def test_view_create_priv(self):
        """VIEW creation requires CREATE VIEW schema privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.VIEW] == SchemaPriv.CREATE_VIEW

    def test_warehouse_create_priv(self):
        """WAREHOUSE creation requires CREATE WAREHOUSE account privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.WAREHOUSE] == AccountPriv.CREATE_WAREHOUSE

    def test_role_create_priv(self):
        """ROLE creation requires CREATE ROLE account privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.ROLE] == AccountPriv.CREATE_ROLE

    def test_user_create_priv(self):
        """USER creation requires CREATE USER account privilege."""
        assert CREATE_PRIV_FOR_RESOURCE_TYPE[ResourceType.USER] == AccountPriv.CREATE_USER


#############################################################################
# GLOBAL_PRIV_DEFAULT_OWNERS Mapping Tests
#############################################################################


class TestGlobalPrivDefaultOwners:
    """Tests for the GLOBAL_PRIV_DEFAULT_OWNERS mapping."""

    def test_create_database_owner_is_accountadmin(self):
        """CREATE DATABASE privilege is owned by ACCOUNTADMIN."""
        assert GLOBAL_PRIV_DEFAULT_OWNERS[AccountPriv.CREATE_DATABASE] == "ACCOUNTADMIN"

    def test_create_role_owner_is_useradmin(self):
        """CREATE ROLE privilege is owned by USERADMIN."""
        assert GLOBAL_PRIV_DEFAULT_OWNERS[AccountPriv.CREATE_ROLE] == "USERADMIN"

    def test_create_user_owner_is_useradmin(self):
        """CREATE USER privilege is owned by USERADMIN."""
        assert GLOBAL_PRIV_DEFAULT_OWNERS[AccountPriv.CREATE_USER] == "USERADMIN"

    def test_create_warehouse_owner_is_sysadmin(self):
        """CREATE WAREHOUSE privilege is owned by SYSADMIN."""
        assert GLOBAL_PRIV_DEFAULT_OWNERS[AccountPriv.CREATE_WAREHOUSE] == "SYSADMIN"

    def test_manage_grants_owner_is_securityadmin(self):
        """MANAGE GRANTS privilege is owned by SECURITYADMIN."""
        assert GLOBAL_PRIV_DEFAULT_OWNERS[AccountPriv.MANAGE_GRANTS] == "SECURITYADMIN"

    def test_create_network_policy_owner_is_securityadmin(self):
        """CREATE NETWORK POLICY privilege is owned by SECURITYADMIN."""
        assert GLOBAL_PRIV_DEFAULT_OWNERS[AccountPriv.CREATE_NETWORK_POLICY] == "SECURITYADMIN"


#############################################################################
# Privilege Parsing Tests
#############################################################################


class TestPrivilegeParsing:
    """Tests for privilege parsing via ParseableEnum."""

    def test_parse_account_priv_from_string(self):
        """AccountPriv can be parsed from string."""
        priv = AccountPriv("CREATE DATABASE")
        assert priv == AccountPriv.CREATE_DATABASE

    def test_parse_database_priv_from_string(self):
        """DatabasePriv can be parsed from string."""
        priv = DatabasePriv("USAGE")
        assert priv == DatabasePriv.USAGE

    def test_parse_table_priv_from_string(self):
        """TablePriv can be parsed from string."""
        priv = TablePriv("SELECT")
        assert priv == TablePriv.SELECT

    def test_invalid_priv_raises_value_error(self):
        """Parsing invalid privilege raises ValueError."""
        with pytest.raises(ValueError):
            TablePriv("NOT_A_REAL_PRIV")

    def test_priv_str_returns_value(self):
        """Privilege str() returns the value string."""
        assert str(TablePriv.SELECT) == "SELECT"
        assert str(DatabasePriv.USAGE) == "USAGE"
        assert str(AccountPriv.CREATE_DATABASE) == "CREATE DATABASE"


#############################################################################
# Additional Privilege Class Tests
#############################################################################


class TestOtherPrivilegeClasses:
    """Tests for other privilege enum classes."""

    def test_role_priv_values(self):
        """RolePriv has expected values."""
        assert RolePriv.OWNERSHIP.value == "OWNERSHIP"
        assert RolePriv.USAGE.value == "USAGE"

    def test_task_priv_values(self):
        """TaskPriv has expected values."""
        assert TaskPriv.ALL.value == "ALL"
        assert TaskPriv.OPERATE.value == "OPERATE"
        assert TaskPriv.MONITOR.value == "MONITOR"

    def test_pipe_priv_values(self):
        """PipePriv has expected values."""
        assert PipePriv.ALL.value == "ALL"
        assert PipePriv.OPERATE.value == "OPERATE"
        assert PipePriv.MONITOR.value == "MONITOR"

    def test_stream_priv_values(self):
        """StreamPriv has expected values."""
        assert StreamPriv.ALL.value == "ALL"
        assert StreamPriv.SELECT.value == "SELECT"
        assert StreamPriv.OWNERSHIP.value == "OWNERSHIP"

    def test_sequence_priv_values(self):
        """SequencePriv has expected values."""
        assert SequencePriv.ALL.value == "ALL"
        assert SequencePriv.USAGE.value == "USAGE"
        assert SequencePriv.OWNERSHIP.value == "OWNERSHIP"

    def test_alert_priv_values(self):
        """AlertPriv has expected values."""
        assert AlertPriv.ALL.value == "ALL"
        assert AlertPriv.MONITOR.value == "MONITOR"
        assert AlertPriv.OPERATE.value == "OPERATE"

    def test_file_format_priv_values(self):
        """FileFormatPriv has expected values."""
        assert FileFormatPriv.ALL.value == "ALL"
        assert FileFormatPriv.USAGE.value == "USAGE"
        assert FileFormatPriv.OWNERSHIP.value == "OWNERSHIP"

    def test_integration_priv_values(self):
        """IntegrationPriv has expected values."""
        assert IntegrationPriv.ALL.value == "ALL"
        assert IntegrationPriv.USAGE.value == "USAGE"
        assert IntegrationPriv.USE_ANY_ROLE.value == "USE_ANY_ROLE"

    def test_secret_priv_values(self):
        """SecretPriv has expected values."""
        assert SecretPriv.OWNERSHIP.value == "OWNERSHIP"
        assert SecretPriv.READ.value == "READ"
        assert SecretPriv.USAGE.value == "USAGE"

    def test_user_priv_values(self):
        """UserPriv has expected values."""
        assert UserPriv.ALL.value == "ALL"
        assert UserPriv.MONITOR.value == "MONITOR"
        assert UserPriv.OWNERSHIP.value == "OWNERSHIP"
