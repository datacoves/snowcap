import logging
import pytest

from snowcap import resources as res
from snowcap import Resource
from snowcap.enums import ResourceType
from tests.helpers import get_json_fixture, camelcase_to_snakecase


logger = logging.getLogger("snowcap")


def test_internal_stage():
    data = get_json_fixture("internal_stage")
    data["resource_type"] = ResourceType.STAGE
    resource = Resource.from_dict(data)
    assert isinstance(resource, res.InternalStage)


def test_external_stage():
    data = get_json_fixture("external_stage")
    data["resource_type"] = ResourceType.STAGE
    resource = Resource.from_dict(data)
    assert isinstance(resource, res.ExternalStage)


def test_table_stream():
    data = get_json_fixture("table_stream")
    data["resource_type"] = ResourceType.STREAM
    resource = Resource.from_dict(data)
    assert isinstance(resource, res.TableStream)


def test_stage_stream():
    data = get_json_fixture("stage_stream")
    data["resource_type"] = ResourceType.STREAM
    resource = Resource.from_dict(data)
    assert isinstance(resource, res.StageStream)


def test_view_stream():
    data = get_json_fixture("view_stream")
    data["resource_type"] = ResourceType.STREAM
    resource = Resource.from_dict(data)
    assert isinstance(resource, res.ViewStream)


def enumerate_polymorphic_resources():
    """Get polymorphic resources that have resolvers (can be distinguished by data)."""
    # List of resource fixtures that have been intentionally removed because they
    # require external setup that cannot be automated in tests. See TESTING.md.
    REMOVED_FIXTURES = {
        "ExternalVolume",  # Requires valid cloud storage bucket
        "SnowflakePartnerOAuthSecurityIntegration",  # Requires external OAuth provider
        "SnowservicesOAuthSecurityIntegration",  # One per account, requires setup
        "AzureOutboundNotificationIntegration",  # Only works on Azure-hosted Snowflake
        "GCPOutboundNotificationIntegration",  # Only works on GCP-hosted Snowflake
        "EmailNotificationIntegration",  # Requires verified email addresses in account (error 394209)
    }

    resources = []
    for resource_type, class_list in Resource.__types__.items():
        # Only include resource types with multiple subtypes AND a resolver
        # Resources without resolvers (like COLUMN) cannot be distinguished by data alone
        if len(class_list) > 1 and resource_type in Resource.__resolvers__:
            for class_ in class_list:
                # Skip resources whose fixtures have been removed
                if class_.__name__ not in REMOVED_FIXTURES:
                    resources.append((resource_type, class_))
    return resources


POLYMORPHIC_RESOURCES = enumerate_polymorphic_resources()


@pytest.fixture(
    params=POLYMORPHIC_RESOURCES,
    ids=[f"{resource_type}:{class_.__name__}" for resource_type, class_ in POLYMORPHIC_RESOURCES],
    scope="function",
)
def polymorphic_resource(request):
    resource_type, class_list = request.param
    yield resource_type, class_list


def test_polymorphic_resources(polymorphic_resource):
    resource_type, class_ = polymorphic_resource

    resource_name = camelcase_to_snakecase(class_.__name__)
    try:
        data = get_json_fixture(resource_name)
    except FileNotFoundError:
        pytest.fail(f"No JSON fixture for {resource_name}")
    except ValueError:
        pytest.fail(f"Missing or malformed JSON fixture for {resource_name}")
    assert Resource.resolve_resource_cls(resource_type, data) == class_, f"{resource_name} -> {class_.__name__}"
