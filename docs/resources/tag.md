---
description: >-
  
---

# Tag

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-tag)

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

* `name` (string, required) - The name of the tag.
* `allowed_values` (list) - A list of allowed values for the tag.
* `comment` (string) - A comment or description for the tag.


