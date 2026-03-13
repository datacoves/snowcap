---
description: >-
  A masking policy for column-level data protection.
---

# MaskingPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy) | Snowcap CLI label: `masking_policy`

Represents a masking policy for column-level data protection in Snowflake. Masking policies define how data is transformed when accessed by users based on their roles or other conditions.


## Examples

### YAML

```yaml
masking_policies:
  - name: governance.public.mask_pii_string
    args:
      - name: val
        data_type: VARCHAR
    returns: VARCHAR
    body: |
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE '***MASKED***'
      END
    comment: Masks PII string data
```

### Python

```python
from snowcap.resources import MaskingPolicy

policy = MaskingPolicy(
    name="governance.public.mask_pii_string",
    args=[{"name": "val", "data_type": "VARCHAR"}],
    returns="VARCHAR",
    body="""
      CASE
        WHEN IS_ROLE_IN_SESSION('Z_UNMASK__PII') THEN val
        ELSE '***MASKED***'
      END
    """,
    comment="Masks PII string data",
)
```


## Fields

* `name` (string, required) - The fully qualified name of the masking policy (e.g., `db.schema.policy_name`).
* `args` (list, required) - List of arguments for the policy. Each argument must have `name` and `data_type` fields. At least one argument is required.
* `returns` (string, required) - The return data type of the masking policy. Must match the data type of the first argument.
* `body` (string, required) - The SQL expression that defines the masking logic. Typically uses CASE expressions with role-based conditions.
* `comment` (string) - A comment or description for the masking policy.
* `exempt_other_policies` (bool) - Whether this policy exempts other policies from being applied. Defaults to False.
* `owner` (string or [Role](role.md)) - The role that owns the masking policy. Defaults to "SYSADMIN".

**Note:** Masking policies require Enterprise Edition or higher.


