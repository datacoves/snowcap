from dataclasses import dataclass, field

from ..enums import ResourceType
from ..props import IdentifierProp, Props
from ..resource_name import ResourceName
from ..scope import AccountScope
from .database import Database
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role

# ResourceType.DATABASE is polymorphic: it resolves to either Database (database.py) or
# SharedDatabase (this file), depending on whether `from_share` is present in the data. The
# resolver is registered here, alongside SharedDatabase, rather than in database.py, so anyone
# grepping for the DATABASE subtypes finds this pointer.


@dataclass(unsafe_hash=True)
class _SharedDatabase(ResourceSpec):
    name: ResourceName
    from_share: ResourceName = field(metadata={"triggers_replacement": True})
    owner: Role = "ACCOUNTADMIN"


class SharedDatabase(NamedResource, Resource):
    """
    Description:
        A database created from a Snowflake share. Shared databases are read-only: Snowflake
        replicates the provider's schemas, tables, and other objects into the consumer account,
        so snowcap cannot add schemas, tags, or params to them the way it can for a regular
        Database.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-database#create-database-from-share

    Fields:
        name (string, required): The name of the database.
        from_share (string, required): The `<provider_account>.<share_name>` the database is created from.
        owner (string or Role): The owner role of the database. Defaults to "ACCOUNTADMIN".

    Python:

        ```python
        shared_database = SharedDatabase(
            name="gong",
            from_share="provider_account.share_name",
            owner="ACCOUNTADMIN",
        )
        ```

    Yaml:

        ```yaml
        databases:
          - name: gong
            from_share: provider_account.share_name
            owner: ACCOUNTADMIN
        ```
    """

    resource_type = ResourceType.DATABASE
    props = Props(
        from_share=IdentifierProp("from share", eq=False),
    )
    scope = AccountScope()
    spec = _SharedDatabase

    def __init__(
        self,
        name: str,
        from_share: str,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _SharedDatabase = _SharedDatabase(
            name=self._name,
            from_share=from_share,
            owner=owner,
        )


# Discriminates on field presence, like stream.py's resolver -- not an enum map like
# stage.py's StageTypeMap, since there's no explicit "type" field to key off of.
def _resolver(data: dict):
    return SharedDatabase if data.get("from_share") else Database


Resource.__resolvers__[ResourceType.DATABASE] = _resolver
