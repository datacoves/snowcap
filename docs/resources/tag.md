---
description: >-
  A tag for labeling resources in Snowflake.
---

# Tag

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-tag) | Snowcap CLI label: `tag`

Represents a tag in Snowflake, which can be used to label various resources for better management and categorization.


## Examples

### YAML

```yaml
tags:
  - name: governance.public.pii
    comment: Personally Identifiable Information

  - name: governance.public.cost_center
    comment: Cost center for billing
    allowed_values:
      - finance
      - engineering
      - sales

  # With auto-propagation
  - name: governance.public.auto_pii
    comment: Auto-propagating PII tag
    allowed_values:
      - sensitive
      - highly_sensitive
    propagate: ON_DEPENDENCY_AND_DATA_MOVEMENT
    on_conflict: ALLOWED_VALUES_SEQUENCE
```

### Python

```python
from snowcap.resources import Tag

tag = Tag(
    name="governance.public.pii",
    comment="Personally Identifiable Information",
)

# With auto-propagation
tag = Tag(
    name="governance.public.auto_pii",
    allowed_values=["sensitive", "highly_sensitive"],
    propagate="ON_DEPENDENCY_AND_DATA_MOVEMENT",
    on_conflict="ALLOWED_VALUES_SEQUENCE",
)
```


## Fields

* `name` (string, required) - The fully qualified name of the tag (e.g., `db.schema.tag_name`).
* `owner` (string or [Role](role.md)) - The owner of the tag. Defaults to "SYSADMIN".
* `allowed_values` (list) - A list of allowed values for the tag. If specified, only these values can be assigned when applying the tag.
* `propagate` (string) - Configures automatic tag propagation (Enterprise Edition+). Values:
    - `ON_DEPENDENCY_AND_DATA_MOVEMENT` - Propagates for both dependencies and data movement
    - `ON_DEPENDENCY` - Propagates only for object dependencies
    - `ON_DATA_MOVEMENT` - Propagates only for data lineage scenarios
* `on_conflict` (string) - Behavior when propagated tag values conflict. Use `ALLOWED_VALUES_SEQUENCE` to use the first allowed value, or specify a custom string like `'CONFLICT'`.
* `comment` (string) - A comment or description for the tag.

**Note:** Tags require Enterprise Edition or higher.


