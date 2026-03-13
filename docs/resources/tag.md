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
```

### Python

```python
from snowcap.resources import Tag

tag = Tag(
    name="governance.public.pii",
    comment="Personally Identifiable Information",
)
```


## Fields

* `name` (string, required) - The fully qualified name of the tag (e.g., `db.schema.tag_name`).
* `owner` (string or [Role](role.md)) - The owner of the tag. Defaults to "SYSADMIN".
* `allowed_values` (list) - A list of allowed values for the tag. If specified, only these values can be assigned when applying the tag.
* `comment` (string) - A comment or description for the tag.

**Note:** Tags require Enterprise Edition or higher.


