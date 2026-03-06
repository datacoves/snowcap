---
description: >-

---

# MaskingPolicy

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy)

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

* `name` (string, required) - The name of the masking policy.
* `args` (list, required) - List of arguments for the policy. Each argument has `name` and `data_type` fields.
* `returns` (string, required) - The return data type of the masking policy.
* `body` (string, required) - The SQL expression that defines the masking logic.
* `comment` (string) - A comment or description for the masking policy.
* `exempt_other_policies` (bool) - Whether this policy exempts other policies. Defaults to False.
* `owner` (string) - The role that owns the masking policy. Defaults to "SYSADMIN".


