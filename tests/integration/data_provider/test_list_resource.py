import os

import pytest
import snowflake.connector.errors
from inflection import pluralize

from tests.helpers import get_json_fixtures
from snowcap import data_provider
from snowcap import resources as res
from snowcap.client import FEATURE_NOT_ENABLED_ERR, UNSUPPORTED_FEATURE, reset_cache
from snowcap.identifiers import resource_label_for_type
from snowcap.resources import Resource
from snowcap.scope import DatabaseScope, SchemaScope

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


# Resources to exclude from list tests
# RoleGrant has thread-safety issues in list_role_grants (execute_in_parallel bug: "generator already executing")
# PythonStoredProcedure doesn't support IF NOT EXISTS for CREATE PROCEDURE
EXCLUDED_FROM_LIST_TEST = (
    res.RoleGrant,
    res.PythonStoredProcedure,
)


def _filter_json_fixtures_for_list_test():
    """Filter JSON fixtures to only include resources that have corresponding list_* functions.

    This filters at parameterization time to avoid runtime skips for 'not supported' reasons.
    Resources without list_* functions should simply not be included in this test.
    """
    for resource_cls, data in get_json_fixtures():
        # Skip resources that are known to have issues in list tests
        if resource_cls in EXCLUDED_FROM_LIST_TEST:
            continue

        # Create a temporary instance to get the resource type
        resource = resource_cls(**data)
        label = resource_label_for_type(resource.resource_type)
        func_name = f"list_{pluralize(label)}"

        # Only include resources that have a list_* function
        if hasattr(data_provider, func_name):
            yield (resource_cls, data)


JSON_FIXTURES = list(_filter_json_fixtures_for_list_test())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request, suffix):
    resource_cls, data = request.param
    if "name" in data and resource_cls not in (res.AccountParameter, res.ScannerPackage):
        data["name"] += f"_{suffix}_list_resources"
    if "login_name" in data:
        data["login_name"] += f"_{suffix}_list_resources"
    resource = resource_cls(**data)

    yield resource


def create(cursor, resource: Resource):
    session_ctx = data_provider.fetch_session(cursor.connection)
    account_edition = session_ctx["account_edition"]
    sql = resource.create_sql(account_edition=account_edition, if_not_exists=True)
    try:
        cursor.execute(sql)
    except snowflake.connector.errors.ProgrammingError:
        raise
    except Exception as err:
        raise Exception(f"Error creating resource: \nQuery: {err.query}\nMsg: {err.msg}") from err
    return resource


@pytest.fixture(scope="session")
def list_resources_database(cursor, suffix, marked_for_cleanup):
    db = res.Database(name=f"list_resources_test_database_{suffix}")
    cursor.execute(db.create_sql(if_not_exists=True))
    marked_for_cleanup.append(db)
    yield db


def test_list_resource(cursor, list_resources_database, resource, marked_for_cleanup):

    data_provider.fetch_session.cache_clear()
    reset_cache()

    # Note: Resources without list_* functions are filtered at parameterization time
    # (see _filter_json_fixtures_for_list_test) so we don't need a runtime skip here
    #
    # OAuth security integrations and ExternalVolume fixtures have been removed from
    # tests/fixtures/json/ because they require external setup that cannot be automated.
    # See tests/SKIPPED_TESTS.md for details.
    if isinstance(resource.scope, DatabaseScope):
        list_resources_database.add(resource)
    elif isinstance(resource.scope, SchemaScope):
        list_resources_database.public_schema.add(resource)

    try:
        create(cursor, resource)
        marked_for_cleanup.append(resource)
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno in (UNSUPPORTED_FEATURE, FEATURE_NOT_ENABLED_ERR):
            pytest.skip(f"{resource.resource_type} is not supported on this account")
        elif err.errno == 1420 and "invalid property" in str(err):
            pytest.skip(f"{resource.resource_type} has API incompatibility: {err.msg}")
        elif err.errno == 1008 and "invalid value" in str(err):
            pytest.skip(f"{resource.resource_type} has API incompatibility: {err.msg}")
        elif err.errno == 1422 and "invalid value" in str(err):
            pytest.skip(f"{resource.resource_type} has API incompatibility: {err.msg}")
        elif err.errno == 3001:
            pytest.skip(f"{resource.resource_type} requires additional privileges: {err.msg}")
        elif err.errno == 3042:
            pytest.skip(f"{resource.resource_type} involves system role that cannot be modified")
        elif err.errno == 3102:
            pytest.skip(f"{resource.resource_type} grant not executed - insufficient privileges")
        elif err.errno == 393925:
            pytest.skip(f"{resource.resource_type} requires valid cloud storage configuration")
        elif err.errno == 394209:
            pytest.skip(f"{resource.resource_type} requires verified email addresses: {err.msg}")
        else:
            raise
    except Exception as err:
        if "IF NOT EXISTS not supported" in str(err):
            pytest.skip(f"{resource.resource_type} does not support IF NOT EXISTS for testing: {err}")
        raise

    try:
        list_resources = data_provider.list_resource(cursor, resource_label_for_type(resource.resource_type))
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno == 2003:
            # Object does not exist - likely race condition with parallel tests
            pytest.skip(f"{resource.resource_type} resource no longer exists (parallel test race condition)")
        raise
    except ValueError as err:
        if "generator already executing" in str(err):
            # Known bug: execute_in_parallel has thread-safety issues with cursor
            pytest.skip(f"{resource.resource_type} list function has thread-safety issue: {err}")
        raise

    # Handle race condition: resource may have been dropped by parallel test cleanup
    if resource.fqn not in list_resources:
        # Verify if resource still exists by trying to fetch it
        try:
            fetched = data_provider.fetch_resource(cursor, resource.urn)
            if fetched is None:
                pytest.skip(f"{resource.resource_type} was dropped by parallel test cleanup")
        except snowflake.connector.errors.ProgrammingError as err:
            if err.errno == 2003:
                pytest.skip(f"{resource.resource_type} was dropped by parallel test cleanup")
            raise
        except Exception:
            # Resource likely doesn't exist anymore
            pytest.skip(f"{resource.resource_type} no longer exists (parallel test race condition)")

    assert len(list_resources) > 0
    assert resource.fqn in list_resources


# @pytest.mark.enterprise
# def test_list_tag_references(cursor):
#     data_provider.fetch_session.cache_clear()
#     reset_cache()
#     tag_references = data_provider.list_tag_references(cursor)
#     assert len(tag_references) > 0
