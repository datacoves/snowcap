---
description: >-
  A hybrid table in Snowflake.
---

# HybridTable

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table)

A hybrid table is a Snowflake table type that is optimized for hybrid transactional and operational workloads that require low latency and high throughput on small random point reads and writes.


## Examples

### YAML

```yaml
hybrid_tables:
  - name: some_hybrid_table
    columns:
      - name: id
        data_type: INT
        constraint: PRIMARY KEY
      - name: name
        data_type: VARCHAR(100)
      - name: status
        data_type: VARCHAR(20)
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

### Python

```python
hybrid_table = HybridTable(
    name="some_hybrid_table",
    columns=[
        Column(name="id", data_type="INT", constraint="PRIMARY KEY"),
        Column(name="name", data_type="VARCHAR(100)"),
        Column(name="status", data_type="VARCHAR(20)"),
    ],
    indexes=[
        {"name": "idx_name", "columns": ["name"]},
        {"name": "idx_status", "columns": ["status"], "include": ["created_at"]}
    ],
    cluster_by=["id"],
    owner="SYSADMIN",
    comment="This is a hybrid table."
)
```


## Fields

* `name` (string, required) - The name of the hybrid table.
* `columns` (list, required) - The columns of the hybrid table.
* `constraints` (list) - Table-level constraints (PRIMARY KEY, FOREIGN KEY).
* `indexes` (list) - Index definitions. Each index is a dict with `name`, `columns`, and optional `include`.
* `cluster_by` (list) - Clustering keys for the hybrid table.
* `tags` (dict) - Tags associated with the hybrid table.
* `owner` (string or [Role](role.md)) - The owner role of the hybrid table. Defaults to "SYSADMIN".
* `comment` (string) - A comment for the hybrid table.


