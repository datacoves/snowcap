import json
from dataclasses import dataclass
from typing import Union

import yaml

from ..enums import ResourceType
from ..props import Props, StringProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


def normalize_mcp_specification(spec: Union[str, dict]) -> str:
    if isinstance(spec, str):
        if not spec.strip():
            raise ValueError("MCP server specification cannot be empty")
        try:
            data = yaml.safe_load(spec)
        except yaml.YAMLError as err:
            raise ValueError(f"Failed to parse MCP server specification: {err}") from err
    else:
        data = spec

    if not isinstance(data, dict):
        raise ValueError(f"MCP server specification must be a mapping, got {type(data).__name__}: {data!r}")

    # Round-trip through JSON so YAML-1.1-only types (e.g. an unquoted date-like scalar) collapse
    # to the same plain strings DESC MCP SERVER's JSON would produce, keeping the canonical form
    # independent of whether the input was written as YAML or JSON.
    data = json.loads(json.dumps(data, default=str))

    data["version"] = data.get("version", 1)
    if isinstance(data.get("tools"), list):
        # Tool order isn't semantic; sort by name (required, unique) so reordering a user's
        # config doesn't produce a diff.
        data["tools"] = sorted(data["tools"], key=lambda tool: tool["name"])

    return yaml.safe_dump(data, sort_keys=True, default_flow_style=False)


@dataclass(unsafe_hash=True)
class _MCPServer(ResourceSpec):
    name: ResourceName
    specification: str
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        self.specification = normalize_mcp_specification(self.specification)


class MCPServer(NamedResource, Resource):
    """
    Description:
        Represents a Snowflake-managed MCP (Model Context Protocol) server, which exposes a
        set of tools, resources, and prompts to MCP clients over a Snowflake-hosted endpoint.
        MCP servers are a generally available Snowflake feature.

        Snowflake has no ALTER MCP SERVER command, so a change to specification is applied by
        dropping and recreating the server with CREATE OR REPLACE. This drops any grants on the
        MCP server; grants managed by snowcap are automatically re-created in the same apply,
        but externally-managed grants must be re-applied manually. The plan output warns when a
        specification change will trigger this. Renaming an MCP server is not supported;
        changing name creates a new resource instead of altering the existing one.

        The specification is canonicalized (re-serialized as sorted YAML) before it is stored
        or compared, so semantically identical specs written with different formatting, key
        order, tool order, or JSON vs. YAML syntax normalize to the same value. Tools are
        reordered by name. This uses YAML 1.1 scalar rules, so unquoted words like yes/no/on/off
        are read as booleans; quote them (e.g. "yes") if you intend a string. Unquoted
        date/timestamp-like scalars (e.g. 2024-01-01) are read as dates and re-serialized as
        plain strings, matching how the same value round-trips through DESC MCP SERVER's JSON.
        Comments in the input are not preserved by canonicalization.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-mcp-server

    Fields:
        name (string, required): The name of the MCP server.
        specification (string, required): A YAML or JSON document describing the MCP server's tools, resources, and prompts. Canonicalized to sorted YAML with tools sorted by name; an absent top-level version key defaults to 1.
        owner (string or Role): The role that owns the MCP server. Defaults to "SYSADMIN".

    Python:

        ```python
        mcp_server = MCPServer(
            name="some_mcp_server",
            specification=\"\"\"
            tools:
              - name: query_data
                type: SYSTEM_EXECUTE_SQL
            \"\"\",
            owner="SYSADMIN",
        )
        ```

    Yaml:

        ```yaml
        mcp_servers:
          - name: some_mcp_server
            specification: |
              tools:
                - name: query_data
                  type: SYSTEM_EXECUTE_SQL
            owner: SYSADMIN
        ```
    """

    resource_type = ResourceType.MCP_SERVER
    props = Props(specification=StringProp("FROM SPECIFICATION", eq=False))
    scope = SchemaScope()
    spec = _MCPServer

    def __init__(
        self,
        name: str,
        specification: str,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _MCPServer = _MCPServer(
            name=self._name,
            specification=specification,
            owner=owner,
        )
