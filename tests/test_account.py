"""
Tests for the Account resource and its is_org_admin property.

Snowcap does not create or drop accounts; the Account resource exists to manage
properties on accounts that already exist. Today that means enabling ORGADMIN.
"""

from unittest.mock import patch

import pytest

from snowcap.blueprint import Blueprint
from snowcap.data_provider import _cast_snowflake_bool, fetch_account
from snowcap.enums import AccountEdition
from snowcap.identifiers import parse_URN
from snowcap.lifecycle import create_resource, drop_resource, update_resource
from snowcap.props import Props
from snowcap.resources import Account

ACCOUNT_URN = parse_URN("urn::ABCD123:account/SNOWCAP")


@pytest.fixture
def session_ctx() -> dict:
    return {
        "account": "SOMEACCT",
        "account_edition": AccountEdition.ENTERPRISE,
        "account_locator": "ABCD123",
        "role": "ORGADMIN",
        "available_roles": ["ORGADMIN", "ACCOUNTADMIN", "SYSADMIN", "PUBLIC"],
    }


class TestAccountResource:
    def test_account_honors_the_name_it_is_given(self):
        """Previously every Account silently became the root sentinel, ACCOUNT."""
        account = Account(name="SNOWCAP", is_org_admin=True)

        assert str(account._name) == "SNOWCAP"
        assert account._data.is_org_admin is True

    def test_locator_is_optional(self):
        """Alter-only usage should not require read-only metadata to be supplied."""
        account = Account(name="SNOWCAP")

        assert account._data.locator is None

    def test_is_org_admin_defaults_to_unmanaged(self):
        """None means 'ignore the property', which the diff skips entirely."""
        assert Account(name="SNOWCAP")._data.is_org_admin is None


class TestUpdateAccount:
    def test_enabling_org_admin_emits_alter_account(self):
        sql = update_resource(ACCOUNT_URN, {"is_org_admin": True}, Props())

        assert sql == "ALTER ACCOUNT SNOWCAP SET IS_ORG_ADMIN = TRUE"

    def test_disabling_org_admin_is_refused_with_an_explanation(self):
        """Snowflake cannot set IS_ORG_ADMIN = FALSE from the current account."""
        with pytest.raises(NotImplementedError, match="cannot disable ORGADMIN"):
            update_resource(ACCOUNT_URN, {"is_org_admin": False}, Props())

    def test_read_only_properties_are_refused(self):
        with pytest.raises(NotImplementedError, match="read-only"):
            update_resource(ACCOUNT_URN, {"edition": "ENTERPRISE"}, Props())

    def test_read_only_properties_are_refused_even_alongside_is_org_admin(self):
        with pytest.raises(NotImplementedError, match="read-only"):
            update_resource(ACCOUNT_URN, {"is_org_admin": True, "locator": "XYZ"}, Props())


class TestAccountLifecycleIsAlterOnly:
    def test_create_is_refused(self):
        with pytest.raises(NotImplementedError, match="does not create"):
            create_resource(ACCOUNT_URN, {}, Props())

    def test_drop_is_refused(self):
        with pytest.raises(NotImplementedError, match="does not drop"):
            drop_resource(ACCOUNT_URN, {})


class TestFetchAccount:
    def test_root_sentinel_is_never_looked_up(self):
        """The blueprint root is named ACCOUNT and is not a real account."""
        root_urn = parse_URN("urn::ABCD123:account/ACCOUNT")

        with patch("snowcap.data_provider.execute") as execute:
            data = fetch_account(None, root_urn.fqn)

        execute.assert_not_called()
        assert data == {"name": None, "locator": None}

    def test_missing_account_returns_none(self):
        with patch("snowcap.data_provider.execute", return_value=[]):
            assert fetch_account(None, ACCOUNT_URN.fqn) is None

    def test_reads_is_org_admin(self):
        rows = [
            {
                "account_name": "SNOWCAP",
                "account_locator": "ABCD123",
                "edition": "ENTERPRISE",
                "snowflake_region": "AWS_US_WEST_2",
                "comment": None,
                "is_org_admin": "Y",
            }
        ]

        with patch("snowcap.data_provider.execute", return_value=rows):
            data = fetch_account(None, ACCOUNT_URN.fqn)

        assert data["is_org_admin"] is True
        assert data["locator"] == "ABCD123"

    def test_other_accounts_are_filtered_out(self):
        rows = [
            {
                "account_name": "OTHER",
                "account_locator": "ZZZ999",
                "edition": "STANDARD",
                "snowflake_region": "AWS_US_WEST_2",
                "comment": None,
                "is_org_admin": "Y",
            }
        ]

        with patch("snowcap.data_provider.execute", return_value=rows):
            assert fetch_account(None, ACCOUNT_URN.fqn) is None


class TestAccountPlanningIsNotYetWired:
    """
    The pieces above (spec, fetch, ALTER generation) are in place, but Blueprint
    still refuses organization-scoped resources: its graph is rooted at a single
    account, so an Account has no container to hang off.

    This test pins the current boundary. Replace it with real planning tests when
    the graph learns to carry org-scoped resources alongside the root.
    """

    def test_blueprint_still_rejects_org_scoped_resources(self, session_ctx):
        blueprint = Blueprint(name="bp", resources=[Account(name="SNOWCAP", is_org_admin=True)])

        with pytest.raises(Exception, match="cannot contain an Account resource"):
            blueprint.generate_manifest(session_ctx)


class TestCastSnowflakeBool:
    @pytest.mark.parametrize("raw", ["Y", "yes", "TRUE", "true", "t", "1", True])
    def test_truthy_spellings(self, raw):
        assert _cast_snowflake_bool(raw) is True

    @pytest.mark.parametrize("raw", ["N", "no", "FALSE", "false", "f", "0", False])
    def test_falsey_spellings(self, raw):
        assert _cast_snowflake_bool(raw) is False

    @pytest.mark.parametrize("raw", [None, "", "maybe"])
    def test_unknown_values_are_unmanaged_rather_than_false(self, raw):
        """Returning False here would make Snowcap try to disable ORGADMIN."""
        assert _cast_snowflake_bool(raw) is None
