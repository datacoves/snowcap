"""
Tests for user-facing error message formatting.

These focus on the system-role case: Snowflake's built-in roles are never created
by Snowcap, so a "not found" error for one of them means something different from
a missing user-defined resource, and the message should say so.
"""

from snowcap.error_formatting import (
    format_missing_pointer_error,
    format_missing_resource_error,
)
from snowcap.identifiers import parse_URN


def test_missing_orgadmin_explains_it_is_primary_account_only():
    msg = format_missing_resource_error(
        parse_URN("urn::ABCD123:role/ORGADMIN"),
        parse_URN("urn::ABCD123:role_grant/ORGADMIN?user=SOMEUSER"),
    )

    assert 'Role "ORGADMIN" not found.' in msg
    assert "primary account" in msg
    assert "IS_ORG_ADMIN" in msg


def test_missing_orgadmin_says_snowcap_cannot_fix_it_for_you():
    """
    The actionable part: Snowcap does not manage accounts, so waiting for it to
    reconcile the role will never work. The user has to run ALTER ACCOUNT.
    """
    msg = format_missing_resource_error(parse_URN("urn::ABCD123:role/ORGADMIN"))

    assert "Snowcap cannot enable it for you" in msg
    assert "manually" in msg


def test_missing_orgadmin_does_not_suggest_a_rename():
    """ORGADMIN is spelled correctly; offering a near-miss name would mislead."""
    msg = format_missing_resource_error(
        parse_URN("urn::ABCD123:role/ORGADMIN"),
        available_names=["ORGADMIN_BACKUP", "SYSADMIN"],
    )

    assert "Did you mean" not in msg


def test_other_system_roles_get_a_generic_explanation():
    msg = format_missing_resource_error(parse_URN("urn::ABCD123:role/SYSADMIN"))

    assert "system role" in msg
    assert "not visible to the session role" in msg
    # The ORGADMIN-specific advice should not leak onto unrelated system roles.
    assert "IS_ORG_ADMIN" not in msg


def test_user_defined_roles_still_get_suggestions():
    """The system-role branch must not swallow the existing behavior."""
    msg = format_missing_resource_error(
        parse_URN("urn::ABCD123:role/ANALYSTT"),
        available_names=["ANALYST", "REPORTER"],
    )

    assert "Did you mean: ANALYST?" in msg
    assert "system role" not in msg


def test_non_role_resources_are_unaffected():
    msg = format_missing_resource_error(
        parse_URN("urn::ABCD123:warehouse/ORGADMIN"),
        available_names=["WH_TRANSFORMING"],
    )

    assert "primary account" not in msg
    assert "system role" not in msg


def test_pointer_errors_explain_system_roles_too():
    msg = format_missing_pointer_error(
        parse_URN("urn::ABCD123:role/ORGADMIN"),
        available_names=["ORGADMIN_BACKUP"],
    )

    assert "primary account" in msg
    assert "Did you mean" not in msg
