---
description: >-
  Associates a masking policy with a tag.
---

# TagMaskingPolicyReference

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/alter-tag) | Snowcap CLI label: `tag_masking_policy_reference`

Associates a masking policy with a tag. When a tag with an associated masking policy is applied to a column, the masking policy is automatically enforced on that column.

This provides a scalable way to apply data protection policies across your organization by simply tagging columns with sensitive data.


## Examples

### YAML

```yaml
tag_masking_policy_references:
  - tag_name: governance.public.pii
    masking_policy_name: governance.public.mask_pii_string
```

### Python

```python
from snowcap.resources import TagMaskingPolicyReference

ref = TagMaskingPolicyReference(
    tag_name="governance.public.pii",
    masking_policy_name="governance.public.mask_pii_string",
)
```


## SQL Generated

When creating this resource, Snowcap generates:

```sql
ALTER TAG governance.public.pii SET MASKING POLICY governance.public.mask_pii_string;
```

When removing this resource:

```sql
ALTER TAG governance.public.pii UNSET MASKING POLICY governance.public.mask_pii_string;
```


## Fields

* `tag_name` (string, required) - The fully qualified name of the tag (e.g., `governance.public.pii`).
* `masking_policy_name` (string, required) - The fully qualified name of the masking policy (e.g., `governance.public.mask_pii_string`).


## Notes

* Both the tag and masking policy must exist before creating this reference.
* A tag can have multiple masking policies associated with it (for different data types).
* The masking policy's signature (input type) should match the data type of columns where the tag will be applied.
* This feature requires Enterprise Edition or higher.


