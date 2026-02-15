import pytest
import snowflake.connector.errors

from snowcap.identifiers import URN, parse_FQN
from snowcap.operations.export import export_resources, _format_resource_config
from snowcap.enums import ResourceType
from snowcap.data_provider import fetch_resource

pytestmark = pytest.mark.requires_snowflake

UNSUPPORTED_FEATURE = 2
FEATURE_NOT_ENABLED_ERR = 3078


def test_export_all(cursor):
    try:
        result = export_resources(session=cursor.connection)
        assert result
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE or err.errno == FEATURE_NOT_ENABLED_ERR:
            pytest.skip(f"Feature not supported on this account: {err.msg}")
        if err.errno == 2003:
            # Object does not exist - likely race condition with parallel tests
            pytest.skip(f"Resource no longer exists (parallel test race condition): {err.msg}")
        raise
    except Exception as err:
        if "Unsupported security integration type" in str(err):
            pytest.skip(f"Account has unsupported security integration type: {err}")
        if "Unsupported notification integration type" in str(err):
            pytest.skip(f"Account has unsupported notification integration type: {err}")
        if "does not exist" in str(err).lower():
            pytest.skip(f"Resource no longer exists (parallel test race condition): {err}")
        raise


def test_export_schema(cursor):
    urn = URN(ResourceType.SCHEMA, parse_FQN("STATIC_DATABASE.STATIC_SCHEMA", is_db_scoped=True))
    resource = fetch_resource(cursor, urn)
    assert resource
    resource_cfg = _format_resource_config(urn, resource, ResourceType.SCHEMA)
    assert resource_cfg
    assert "database" in resource_cfg
    assert resource_cfg["database"] == "STATIC_DATABASE"
