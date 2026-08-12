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
    # Imported (FROM SHARE) databases are read-only in the consumer account: Snowflake
    # prevents GRANT OWNERSHIP on them, so owner is pinned to ACCOUNTADMIN and never
    # drift-tracked (SYSTEM$SHOW_IMPORTED_DATABASES' owner output is also undocumented).
    owner: Role = field(default="ACCOUNTADMIN", metadata={"fetchable": False})

    def __post_init__(self):
        super().__post_init__()
        if self.owner.name != "ACCOUNTADMIN":
            raise ValueError(
                f"SharedDatabase '{self.name}' does not support a custom owner (got '{self.owner.name}'). "
                "Imported (FROM SHARE) databases are read-only in the consumer account and Snowflake "
                "prevents GRANT OWNERSHIP on them, so ownership cannot be changed. "
                "Remove the owner field, or set it to ACCOUNTADMIN."
            )


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
        owner (string or Role): Pinned to "ACCOUNTADMIN". Snowflake prevents GRANT OWNERSHIP
            on an imported database, so a custom owner is rejected at plan time.

    Python:

        ```python
        shared_database = SharedDatabase(
            name="gong",
            from_share="provider_account.share_name",
        )
        ```

    Yaml:

        ```yaml
        databases:
          - name: gong
            from_share: provider_account.share_name
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
