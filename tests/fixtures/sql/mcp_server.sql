CREATE MCP SERVER my_mcp_server
FROM SPECIFICATION $$
tools:
  - name: run_sql
    identifier: run_sql
    type: SYSTEM_EXECUTE_SQL
$$;
