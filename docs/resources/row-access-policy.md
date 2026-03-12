---
description: >-
  A row access policy for row-level security.
---

# RowAccessPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy)

Represents a row access policy for row-level security in Snowflake. Row access policies define which rows are visible to users based on conditions, typically checking role membership.


## Examples

### YAML

```yaml
row_access_policies:
  - name: governance.policies.rap_sales_region
    args:
      - name: region
        data_type: VARCHAR
    body: |
      CURRENT_ROLE() IN ('ADMIN', 'SALES_MANAGER')
      OR region = CURRENT_USER()
    comment: Users can only see rows for their assigned region
```

### Python

```python
from snowcap.resources import RowAccessPolicy

policy = RowAccessPolicy(
    name="governance.policies.rap_sales_region",
    args=[{"name": "region", "data_type": "VARCHAR"}],
    body="""
      CURRENT_ROLE() IN ('ADMIN', 'SALES_MANAGER')
      OR region = CURRENT_USER()
    """,
    comment="Users can only see rows for their assigned region",
)
```


## Fields

* `name` (string, required) - The fully qualified name of the row access policy (e.g., `db.schema.policy_name`).
* `args` (list, required) - List of arguments for the policy. Each argument must have `name` and `data_type` fields. These correspond to columns that will be passed when the policy is attached to a table. At least one argument is required.
* `body` (string, required) - A SQL expression that returns BOOLEAN. When TRUE, the row is visible; when FALSE, it is filtered out. Typically uses `IS_ROLE_IN_SESSION()` to check role membership.
* `comment` (string) - A comment or description for the row access policy.
* `owner` (string or [Role](role.md)) - The role that owns the row access policy. Defaults to "SYSADMIN".

**Note:** Row access policies require Enterprise Edition or higher.


## CLI

Use `row_access_policy` with `--exclude` or `--sync_resources`:

```bash
snowcap plan --config resources/ --exclude row_access_policy
```


## Attaching to Tables

After creating a row access policy, attach it to tables using `ALTER TABLE`:

```sql
ALTER TABLE my_table ADD ROW ACCESS POLICY governance.policies.rap_sales_region ON (region);
```

See [Row Access Policies](../row-access-policies.md) for a recommended pattern using role-based filtering with dbt integration.


## See Also

- [Row Access Policies Guide](../row-access-policies.md)
- [MaskingPolicy](masking-policy.md)
