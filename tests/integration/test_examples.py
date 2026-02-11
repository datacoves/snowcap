import os

import pytest
import yaml
import snowflake.connector.errors

from tests.helpers import dump_resource_change, get_examples_yml
from snowcap.blueprint import Blueprint
from snowcap.client import FEATURE_NOT_ENABLED_ERR, UNSUPPORTED_FEATURE
from snowcap.enums import ResourceType
from snowcap.exceptions import NonConformingPlanException
from snowcap.gitops import collect_blueprint_config

EXAMPLES_YML = list(get_examples_yml())
TEST_WAREHOUSE = os.environ.get("TEST_SNOWFLAKE_WAREHOUSE")
VARS = {
    "for-each-example": {
        "schemas": [
            "schema1",
            "schema2",
            "schema3",
        ]
    },
    # Provide required variable for iceberg example
    "snowflake-tutorials-create-your-first-iceberg-table": {
        "storage_role_arn": "arn:aws:iam::123456789012:role/test_role",
        "storage_base_url": "s3://test-bucket/test-path",
        "storage_aws_external_id": "test_external_id",
    },
}


@pytest.fixture(
    params=EXAMPLES_YML,
    ids=[example_name for example_name, _ in EXAMPLES_YML],
    scope="function",
)
def example(request):
    example_name, example_content = request.param
    yield example_name, yaml.safe_load(example_content)


@pytest.mark.enterprise
@pytest.mark.requires_snowflake
def test_example(example, cursor, marked_for_cleanup, blueprint_vars):
    example_name, example_content = example
    blueprint_vars = VARS.get(example_name, blueprint_vars)

    if TEST_WAREHOUSE:
        cursor.execute(f"USE WAREHOUSE {TEST_WAREHOUSE}")

    try:
        blueprint_config = collect_blueprint_config(example_content.copy(), {"vars": blueprint_vars})
        assert blueprint_config.resources is not None
        for resource in blueprint_config.resources:
            marked_for_cleanup.append(resource)
        blueprint = Blueprint.from_config(blueprint_config)
        plan = blueprint.plan(cursor.connection)
        blueprint.apply(cursor.connection, plan)

        blueprint_config = collect_blueprint_config(example_content.copy(), {"vars": blueprint_vars})
        blueprint = Blueprint.from_config(blueprint_config)
        plan = blueprint.plan(cursor.connection)
        unexpected_drift = [change for change in plan if not change_is_expected(change)]
        if len(unexpected_drift) > 0:
            debug = "\n".join([dump_resource_change(change) for change in unexpected_drift])
            assert False, f"Unexpected drift:\n{debug}"
    except snowflake.connector.errors.ProgrammingError as err:
        # Skip on feature not supported or not enabled errors
        if err.errno in (UNSUPPORTED_FEATURE, FEATURE_NOT_ENABLED_ERR):
            pytest.skip(f"Feature not supported: {err.msg}")
        # Skip on insufficient privileges (error 3001)
        elif err.errno == 3001:
            pytest.skip(f"Insufficient privileges: {err.msg}")
        # Skip on object already exists that we can't control (error 2002)
        elif err.errno == 2002:
            pytest.skip(f"Object already exists: {err.msg}")
        # Skip on SQL compilation errors for feature-specific limitations (e.g., Iceberg tables)
        elif err.errno == 99201:
            pytest.skip(f"SQL compilation error for feature limitation: {err.msg}")
        # Skip on invalid parameter value for account-specific settings
        elif "is not a valid parameter value" in str(err.msg):
            pytest.skip(f"Invalid parameter for this account: {err.msg}")
        else:
            raise
    except NonConformingPlanException as err:
        # Skip on enterprise edition requirements
        if "requires enterprise edition" in str(err):
            pytest.skip(f"Requires enterprise edition: {err}")
        else:
            raise


def change_is_expected(change):
    from snowcap.blueprint import UpdateResource

    # ALL grants expand to multiple privileges, causing expected drift
    if change.urn.resource_type == ResourceType.GRANT and change.after.get("priv", "") == "ALL":
        return True

    # User fields default_secondary_roles and type are set by Snowflake
    # and cause expected drift when not specified in YAML
    if isinstance(change, UpdateResource) and change.urn.resource_type == ResourceType.USER:
        delta = change.delta
        expected_user_drift = {"default_secondary_roles", "type"}
        if set(delta.keys()).issubset(expected_user_drift):
            return True

    return False
