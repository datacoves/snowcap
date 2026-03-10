from dataclasses import dataclass

from ..enums import ResourceType
from ..parse import _parse_create_header, _parse_props, _parse_table_schema
from ..props import (
    IdentifierListProp,
    Props,
    SchemaProp,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .column import Column
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _HybridTable(ResourceSpec):
    name: ResourceName
    columns: list[Column]
    constraints: list[str] = None
    indexes: list[dict] = None
    cluster_by: list[str] = None
    owner: RoleRef = "SYSADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.columns is None:
            raise ValueError("columns can't be None")
        if len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class HybridTable(NamedResource, TaggableResource, Resource):
    """
    Description:
        A hybrid table is a Snowflake table type that is optimized for hybrid transactional and operational workloads that require low latency and high throughput on small random point reads and writes.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table

    Fields:
        name (string, required): The name of the hybrid table.
        columns (list, required): The columns of the hybrid table.
        constraints (list): Table-level constraints (PRIMARY KEY, FOREIGN KEY).
        indexes (list): Index definitions. Each index is a dict with 'name', 'columns', and optional 'include'.
        cluster_by (list): Clustering keys for the hybrid table.
        tags (dict): Tags associated with the hybrid table.
        owner (string or Role): The owner role of the hybrid table. Defaults to "SYSADMIN".
        comment (string): A comment for the hybrid table.

    Python:

        ```python
        hybrid_table = HybridTable(
            name="some_hybrid_table",
            columns=[Column(name="id", data_type="INT", constraint="PRIMARY KEY")],
            indexes=[
                {"name": "idx_name", "columns": ["name"]},
                {"name": "idx_status", "columns": ["status"], "include": ["created_at"]}
            ],
            cluster_by=["id"],
            owner="SYSADMIN",
            comment="This is a hybrid table."
        )
        ```

    Yaml:

        ```yaml
        hybrid_tables:
          - name: some_hybrid_table
            columns:
              - name: id
                data_type: INT
                constraint: PRIMARY KEY
            indexes:
              - name: idx_name
                columns:
                  - name
              - name: idx_status
                columns:
                  - status
                include:
                  - created_at
            cluster_by:
              - id
            owner: SYSADMIN
            comment: This is a hybrid table.
        ```
    """

    resource_type = ResourceType.HYBRID_TABLE
    props = Props(
        columns=SchemaProp(),
        cluster_by=IdentifierListProp("cluster by", eq=False, parens=True),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _HybridTable

    def __init__(
        self,
        name: str,
        columns: list[Column],
        constraints: list[str] = None,
        indexes: list[dict] = None,
        cluster_by: list[str] = None,
        tags: dict[str, str] = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _HybridTable = _HybridTable(
            name=self._name,
            columns=columns,
            constraints=constraints,
            indexes=indexes,
            cluster_by=cluster_by,
            owner=owner,
            comment=comment,
        )
        self.set_tags(tags)

    @classmethod
    def from_sql(cls, sql):
        identifier, remainder = _parse_create_header(sql, cls.resource_type, cls.scope)
        table_schema, remainder = _parse_table_schema(remainder)
        props = _parse_props(cls.props, remainder)
        return cls(**identifier, **table_schema, **props)
