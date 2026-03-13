---
description: >-
  A grant of a role to another role or user.
---

# RoleGrant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-role) | Snowcap CLI label: `role_grant`

Represents a grant of a role to another role or user in Snowflake.


## Examples

### YAML

```yaml
role_grants:
  # Grant multiple roles to a role
  - to_role: analyst
    roles:
      - z_db__raw
      - z_db__analytics
      - z_wh__transforming

  # Grant multiple roles to a user
  - to_user: jane_doe
    roles:
      - analyst
      - developer
```

### Python

```python
# Grant to Role:
role_grant = RoleGrant(role="somerole", to_role="someotherrole")

# Grant to User:
role_grant = RoleGrant(role="somerole", to_user="someuser")
```


## Fields

* `role` (string or [Role](role.md), required) - The role to be granted.
* `to_role` (string or [Role](role.md)) - The role to receive the grant.
* `to_user` (string or [User](user.md)) - The user to receive the grant.

**Note:** You must specify either `to_role` or `to_user`, but not both.

In YAML, you can also use the shorthand syntax with `roles` (list) to grant multiple roles at once.


