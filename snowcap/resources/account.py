from dataclasses import dataclass

from ..builtins import ROOT_ACCOUNT_NAME  # noqa: F401  (re-exported for convenience)
from ..enums import AccountEdition, ResourceType
from ..props import Props
from ..resource_name import ResourceName
from ..scope import OrganizationScope
from .resource import NamedResource, Resource, ResourceContainer, ResourceSpec


@dataclass(unsafe_hash=True)
class _Account(ResourceSpec):
    name: ResourceName
    locator: str = None
    edition: AccountEdition = None
    region: str = None
    comment: str = None
    is_org_admin: bool = None


class Account(NamedResource, Resource, ResourceContainer):
    """
    Description:
        An account in a Snowflake organization.

        Snowcap does not create or drop accounts. This resource manages properties
        on accounts that already exist, which today means enabling the ORGADMIN
        role via `is_org_admin`.

    Fields:
        name (string, required): The name of the account.
        is_org_admin (bool): Whether the ORGADMIN role is enabled in the account.
            Leave unset to let Snowcap ignore the property entirely.
        locator (string): The account locator.
        edition (string): The Snowflake edition of the account.
        comment (string): A comment for the account.

    Python:

        ```python
        account = Account(
            name="some_account",
            is_org_admin=True,
        )
        ```

    Yaml:

        ```yaml
        accounts:
          - name: some_account
            is_org_admin: true
        ```

    Notes:
        Setting `is_org_admin` requires a session using the ORGADMIN role, on an
        account where ORGADMIN is already enabled (an organization's primary
        account). It emits:

            ALTER ACCOUNT <name> SET IS_ORG_ADMIN = TRUE

        Snowflake does not allow the property to be set to FALSE from the current
        account, so `is_org_admin: false` is rejected rather than silently ignored.
        Disable it by enabling ORGADMIN from a different account.
    """

    resource_type = ResourceType.ACCOUNT
    props = Props()
    scope = OrganizationScope()
    spec = _Account

    def __init__(
        self,
        name: str,
        locator: str = None,
        edition: AccountEdition = None,
        comment: str = None,
        is_org_admin: bool = None,
        **kwargs,
    ):
        super().__init__(name=name, **kwargs)
        self._data: _Account = _Account(
            name=self._name,
            locator=locator,
            edition=edition,
            comment=comment,
            is_org_admin=is_org_admin,
        )
