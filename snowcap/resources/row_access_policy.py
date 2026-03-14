from dataclasses import dataclass

from snowcap.enums import AccountEdition, ResourceType
from snowcap.props import Props, StringProp, ArgsProp, QueryProp, ReturnsProp
from snowcap.scope import SchemaScope
from snowcap.resource_name import ResourceName
from snowcap.resources.resource import Arg, NamedResource, Resource, ResourceSpec
from snowcap.role_ref import RoleRef


@dataclass(unsafe_hash=True)
class _RowAccessPolicy(ResourceSpec):
    name: ResourceName
    args: list[Arg]
    returns: str
    body: str
    comment: str = None
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if len(self.args) == 0:
            raise ValueError("At least one argument is required")


class RowAccessPolicy(NamedResource, Resource):
    """
    A row access policy controls which rows are visible to a query based on conditions.

    CREATE ROW ACCESS POLICY <name>
      AS ( <arg_name> <arg_type> [, ... ] ) RETURNS BOOLEAN ->
      <body>
      [ COMMENT = '<string_literal>' ]

    Example:
        Row access policy that filters by country:

        >>> RowAccessPolicy(
        ...     name="some_db.some_schema.rap_country",
        ...     args=[{"name": "country_val", "data_type": "VARCHAR"}],
        ...     body="IS_ROLE_IN_SESSION('Z_ROW_ACCESS__COUNTRY__' || UPPER(country_val))",
        ...     comment="Filters rows by country based on user role"
        ... )
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.ROW_ACCESS_POLICY
    props = Props(
        args=ArgsProp(),
        returns=ReturnsProp("returns", eq=False),
        body=QueryProp("->"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _RowAccessPolicy

    def __init__(
        self,
        name: str,
        args: list[dict],
        body: str,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _RowAccessPolicy = _RowAccessPolicy(
            name=self._name,
            args=args,
            returns="BOOLEAN",
            body=body,
            comment=comment,
            owner=owner,
        )
