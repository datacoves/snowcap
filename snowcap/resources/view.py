import re
from dataclasses import dataclass, field

from ..enums import ResourceType
from ..props import (
    BoolProp,
    ColumnNamesProp,
    FlagProp,
    Props,
    QueryProp,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope, TableScope
from .resource import NamedResource, Resource, ResourcePointer, ResourceSpec
from .tag import TaggableResource


def _extract_table_refs_from_sql(sql: str) -> list[str]:
    """
    Extract table/view references from a SQL SELECT statement.

    Parses FROM and JOIN clauses to find referenced tables/views.
    Returns a list of fully qualified or simple table names.

    Examples:
        "SELECT * FROM my_table" -> ["my_table"]
        "SELECT * FROM db.schema.table" -> ["db.schema.table"]
        "SELECT * FROM t1 JOIN t2 ON ..." -> ["t1", "t2"]
    """
    if not sql:
        return []

    # Pattern to match table names after FROM or JOIN keywords
    # Handles: FROM table, JOIN table, LEFT JOIN table, INNER JOIN table, etc.
    # Table names can be: simple (table), qualified (schema.table), or fully qualified (db.schema.table)
    # Also handles quoted identifiers like "TABLE_NAME" or "db"."schema"."table"
    identifier = r'(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_$]*)'
    qualified_name = rf'{identifier}(?:\.{identifier})*'

    # Match FROM or any type of JOIN followed by a table name
    # Use word boundary and handle optional keywords like LATERAL, NATURAL, etc.
    pattern = rf'''
        (?:FROM|(?:CROSS|INNER|LEFT|RIGHT|FULL|NATURAL|LATERAL)\s+(?:OUTER\s+)?JOIN|JOIN)
        \s+
        ({qualified_name})
    '''

    matches = re.findall(pattern, sql, re.IGNORECASE | re.VERBOSE)

    # Clean up quoted identifiers and normalize
    result = []
    for match in matches:
        # Remove surrounding quotes from each part if present
        cleaned = ".".join(
            part.strip('"') for part in match.split(".")
        )
        result.append(cleaned)

    return result


@dataclass(unsafe_hash=True)
class _ViewColumn(ResourceSpec):
    name: ResourceName
    comment: str = None
    data_type: str = field(default=None, metadata={"known_after_apply": True})
    not_null: bool = False
    default: str = None
    constraint: str = None
    collate: str = None


class ViewColumn(NamedResource, Resource):
    resource_type = ResourceType.COLUMN
    props = Props(
        comment=StringProp("comment", eq=False),
    )
    scope = TableScope()
    spec = _ViewColumn
    serialize_inline = True

    def __init__(
        self,
        name: str,
        comment: str = None,
        data_type: str = None,
        not_null: bool = False,
        default: str = None,
        constraint: str = None,
        collate: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ViewColumn = _ViewColumn(
            name=self._name,
            comment=comment,
            data_type=data_type,
            not_null=not_null,
            default=default,
            constraint=constraint,
            collate=collate,
        )


@dataclass(unsafe_hash=True)
class _View(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    secure: bool = False
    volatile: bool = None
    recursive: bool = None
    columns: list[ViewColumn] = None
    change_tracking: bool = False
    copy_grants: bool = field(default=False, metadata={"fetchable": False})
    comment: str = None
    # TODO: remove this if parsing is feasible
    as_: str = None  # field(default=None, metadata={"fetchable": False})

    def __post_init__(self):
        super().__post_init__()
        if self.columns is not None and len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class View(NamedResource, TaggableResource, Resource):
    """
    Description:
        Represents a view in Snowflake, which is a virtual table created by a stored query on the data.
        Views are used to simplify complex queries, improve security, or enhance performance.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-view

    Fields:
        name (string, required): The name of the view.
        owner (string or Role): The owner role of the view. Defaults to "SYSADMIN".
        secure (bool): Specifies if the view is secure.
        volatile (bool): Specifies if the view is volatile.
        recursive (bool): Specifies if the view is recursive.
        columns (list): A list of dictionaries specifying column details.
        tags (dict): A dictionary of tags associated with the view.
        change_tracking (bool): Specifies if change tracking is enabled.
        copy_grants (bool): Specifies if grants should be copied from the base table.
        comment (string): A comment for the view.
        as_ (string): The SELECT statement defining the view.

    Python:

        ```python
        view = View(
            name="some_view",
            owner="SYSADMIN",
            secure=True,
            as_="SELECT * FROM some_table"
        )
        ```

    Yaml:

        ```yaml
        views:
          - name: some_view
            owner: SYSADMIN
            secure: true
            as_: "SELECT * FROM some_table"
        ```
    """

    resource_type = ResourceType.VIEW
    props = Props(
        columns=ColumnNamesProp(),
        secure=FlagProp("secure"),
        volatile=FlagProp("volatile"),
        recursive=FlagProp("recursive"),
        tags=TagsProp(),
        change_tracking=BoolProp("change_tracking"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
    )
    scope = SchemaScope()
    spec = _View

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        secure: bool = False,
        volatile: bool = None,
        recursive: bool = None,
        columns: list[dict] = None,
        tags: dict[str, str] = None,
        change_tracking: bool = False,
        copy_grants: bool = False,
        comment: str = None,
        as_: str = None,
        **kwargs,
    ):
        if "lifecycle" not in kwargs:
            lifecycle = {
                "ignore_changes": "columns",
            }
            kwargs["lifecycle"] = lifecycle

        super().__init__(name, **kwargs)
        self._data: _View = _View(
            name=self._name,
            owner=owner,
            secure=secure,
            volatile=volatile,
            recursive=recursive,
            columns=columns,
            change_tracking=change_tracking,
            copy_grants=copy_grants,
            comment=comment,
            as_=as_,
        )
        self.set_tags(tags)

        # Extract table dependencies from the SELECT statement
        # Only add dependencies for fully qualified table names (db.schema.table format)
        # Simple names like "my_table" can't be resolved to a specific resource without
        # knowing the view's container, which isn't set until later in the build process.
        if as_:
            for table_ref in _extract_table_refs_from_sql(as_):
                # Only track fully qualified names (at least schema.table format)
                if "." in table_ref:
                    self.requires(ResourcePointer(name=table_ref, resource_type=ResourceType.TABLE))
