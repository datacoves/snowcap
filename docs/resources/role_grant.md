---
description: >-
  A grant of a role to another role or user. Manage it as code with Snowcap: YAML and Python examples and a reference for every field.
---

# RoleGrant

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-role) | Snowcap CLI label: `role_grant`

Represents a grant of a role to another role or user in Snowflake.

A grant to a user can also be temporary: `for_interval` keeps it for a length of time
(`GRANT ROLE ... FOR INTERVAL`), and `until` expires it at a fixed timestamp
(`GRANT ROLE ... UNTIL`). When the grant expires Snowflake revokes it automatically;
the next plan then shows it for creation again. Temporary grants require the temporary
role grants preview in Snowflake and are only supported on grants to a user.

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

  # Grant a role to a user temporarily
  - role: oncall
    to_user: jane_doe
    for_interval: 24 HOURS
  - role: oncall
    to_user: jane_doe
    until: "2026-01-01T00:00:00Z"
```

### Python

```python
# Grant to Role:
role_grant = RoleGrant(role="somerole", to_role="someotherrole")

# Grant to User:
role_grant = RoleGrant(role="somerole", to_user="someuser")

# Grant to User temporarily:
role_grant = RoleGrant(role="oncall", to_user="someuser", for_interval="24 HOURS")
role_grant = RoleGrant(role="oncall", to_user="someuser", until="2026-01-01T00:00:00Z")
```


## Fields

* `role` (string or [Role](role.md), required) - The role to be granted.
* `to_role` (string or [Role](role.md)) - The role to receive the grant.
* `to_user` (string or [User](user.md)) - The user to receive the grant.
* `for_interval` (string) - How long a grant to a user lasts before expiring, e.g. `"24 HOURS"`, `"30 MINUTES"`, or `"7 DAYS"`. Snowflake reports only the absolute expiration, so the interval itself is not compared between plans: the grant keeps the window it was created with until it lapses, then the next apply re-grants it.
* `until` (string) - When a grant to a user expires, as a timestamp with a timezone, e.g. `"2026-01-01T00:00:00Z"`. Changing it updates the expiration in place (`GRANT ROLE ... UNTIL ... RESET EXPIRATION`). Cannot be combined with `for_interval`.

**Note:** You must specify either `to_role` or `to_user`, but not both. `for_interval` and `until` can only be used with `to_user` (or `to_users`).

In YAML, you can also use the shorthand syntax with `roles` (list) to grant multiple roles at once.


