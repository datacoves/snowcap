from dataclasses import dataclass
from typing import Optional

from ..enums import ResourceType
from ..props import FlagProp, IntProp, Props, StringProp, TagsProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import DatabaseScope
from ..var import VarString
from .resource import NamedResource, Resource, ResourceContainer, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _Schema(ResourceSpec):
    name: ResourceName
    transient: bool = False
    managed_access: bool = False
    data_retention_time_in_days: Optional[int] = None  # Inherit from database if not specified
    max_data_extension_time_in_days: Optional[int] = None  # Inherit from database if not specified
    default_ddl_collation: Optional[str] = None  # Inherit from database if not specified
    owner: RoleRef = "SYSADMIN"
    comment: Optional[str] = None


class Schema(NamedResource, TaggableResource, Resource, ResourceContainer):
    """
    Description:
        Represents a schema in Snowflake, which is a logical grouping of database objects such as tables, views, and stored procedures. Schemas are used to organize and manage such objects within a database.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-schema

    Fields:
        name (string, required): The name of the schema.
        transient (bool): Specifies if the schema is transient. Defaults to False.
        managed_access (bool): Specifies if the schema has managed access. Defaults to False.
        data_retention_time_in_days (int): The number of days to retain data. Inherits from database if not specified.
        max_data_extension_time_in_days (int): The maximum number of days to extend data retention. Inherits from database if not specified.
        default_ddl_collation (string): The default DDL collation setting. Inherits from database if not specified.
        tags (dict): Tags associated with the schema.
        owner (string or Role): The owner of the schema. Defaults to "SYSADMIN".
        comment (string): A comment about the schema.

    Python:

        ```python
        schema = Schema(
            name="some_schema",
            transient=True,
            managed_access=True,
            tags={"project": "analytics"},
            owner="SYSADMIN",
            comment="Schema for analytics project."
        )
        ```

    Yaml:

        ```yaml
        schemas:
          - name: some_schema
            transient: true
            managed_access: true
            tags:
              project: analytics
            owner: SYSADMIN
            comment: Schema for analytics project.
        ```
    """

    resource_type = ResourceType.SCHEMA
    props = Props(
        transient=FlagProp("transient"),
        managed_access=FlagProp("with managed access"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = DatabaseScope()
    spec = _Schema

    def __init__(
        self,
        name: str,
        transient: bool = False,
        managed_access: bool = False,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = None,
        default_ddl_collation: str = None,
        tags: dict[str, str] = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)

        if self._name == "INFORMATION_SCHEMA":
            comment = "Views describing the contents of schemas in this database"
            owner = ""

        if self._name == "PUBLIC" and not self.implicit:
            raise ValueError("PUBLIC schema is implicit and must not be explicitly created")

        self._data: _Schema = _Schema(
            name=self._name,
            transient=transient,
            managed_access=managed_access,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            default_ddl_collation=default_ddl_collation,
            owner=owner,
            comment=comment,
        )
        self.set_tags(tags)

    def _resolve_vars(self, vars: dict, resources: list):
        name_uses_var = isinstance(self._name, VarString)
        super()._resolve_vars(vars, resources)
        if name_uses_var and self._name in ["INFORMATION_SCHEMA", "PUBLIC"]:
            raise Exception("Cannot resolve vars for system schemas")
