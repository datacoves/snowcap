---
description: >-
  A Snowflake-managed MCP server exposing tools to MCP clients.
---

# MCPServer

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-mcp-server) | Snowcap CLI label: `mcp_server`

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


## Examples

### Python

```python
mcp_server = MCPServer(
    name="some_mcp_server",
    specification="""
    tools:
      - name: query_data
        type: SYSTEM_EXECUTE_SQL
    """,
    owner="SYSADMIN",
)
```


### YAML

```yaml
mcp_servers:
  - name: some_mcp_server
    specification: |
      tools:
        - name: query_data
          type: SYSTEM_EXECUTE_SQL
    owner: SYSADMIN
```


## Fields

* `name` (string, required) - The name of the MCP server.
* `specification` (string, required) - A YAML or JSON document describing the MCP server's tools, resources, and prompts. Canonicalized to sorted YAML with tools sorted by name; an absent top-level version key defaults to 1.
* `owner` (string or [Role](role.md)) - The role that owns the MCP server. Defaults to "SYSADMIN".


