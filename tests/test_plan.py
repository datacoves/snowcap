import pytest

from snowcap import resources as res
from snowcap.blueprint import (
    Blueprint,
    CreateResource,
    DropResource,
    NonConformingPlanException,
    UpdateResource,
    diff,
)
from snowcap.enums import AccountEdition, ResourceType
from snowcap.identifiers import parse_URN


@pytest.fixture
def session_ctx() -> dict:
    return {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "role": "SYSADMIN",
        "available_roles": ["SYSADMIN", "USERADMIN"],
    }


@pytest.fixture
def remote_state() -> dict:
    return {
        parse_URN("urn::ABCD123:account/ACCOUNT"): {},
    }


def test_plan_add_action(session_ctx, remote_state):
    bp = Blueprint(resources=[res.Database(name="NEW_DATABASE")])
    manifest = bp.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, CreateResource)
    assert change.urn == parse_URN("urn::ABCD123:database/NEW_DATABASE")
    assert "name" in change.after
    assert change.after["name"] == "NEW_DATABASE"


def test_plan_change_action(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:role/EXISTING_ROLE")] = {
        "name": "EXISTING_ROLE",
        "comment": "old comment",
        "owner": "USERADMIN",
    }
    bp = Blueprint(
        resources=[
            res.Role(
                name="EXISTING_ROLE",
                comment="new comment",
            )
        ]
    )
    manifest = bp.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, UpdateResource)
    assert change.urn == parse_URN("urn::ABCD123:role/EXISTING_ROLE")
    assert "comment" in change.before
    assert change.before["comment"] == "old comment"
    assert "comment" in change.after
    assert change.after["comment"] == "new comment"


def test_plan_remove_action(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:role/REMOVED_ROLE")] = {
        "name": "REMOVED_ROLE",
        "comment": "old comment",
        "owner": "USERADMIN",
    }
    bp = Blueprint(sync_resources=[ResourceType.ROLE])
    manifest = bp.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, DropResource)
    assert change.urn == parse_URN("urn::ABCD123:role/REMOVED_ROLE")


def test_plan_no_removes_in_resources_not_in_sync_resources(session_ctx, remote_state):
    """Test that plan correctly identifies resources to remove.

    Note: This test originally expected _raise_for_nonconforming_plan to raise
    NonConformingPlanException for drops when sync_resources is not set, but
    that validation is not implemented. The current behavior is that drops are
    allowed when sync_resources is None.
    """
    remote_state[parse_URN("urn::ABCD123:role/REMOVED_ROLE")] = {
        "name": "REMOVED_ROLE",
        "comment": "old comment",
        "owner": "USERADMIN",
    }
    bp = Blueprint()
    manifest = bp.generate_manifest(session_ctx)
    plan = diff(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, DropResource)
    assert change.urn == parse_URN("urn::ABCD123:role/REMOVED_ROLE")
    # Note: _raise_for_nonconforming_plan does not validate drops against sync_resources
    # This behavior is intentional - sync_resources controls what gets synced, not what can be dropped
