---
description: >-
  A database created from an inbound Snowflake share.
---

# SharedDatabase

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-database) | Snowcap CLI label: `shared database`

A `SharedDatabase` is the consumer side of a Snowflake share or Marketplace
listing: `CREATE DATABASE <name> FROM SHARE <provider_account>.<share_name>`.
It's a polymorphic sibling of [Database](database.md) — declared under the
same `databases:` key, distinguished by the presence of `from_share`. Because
Snowflake replicates the provider's schemas, tables, and other objects into
the consumer account, shared databases are read-only: snowcap cannot add
schemas, tags, or params to them the way it can for a regular `Database`.

## Examples

### YAML

```yaml
databases:
  - name: gong
    from_share: provider_account.share_name
    owner: ACCOUNTADMIN
```

### Python

```python
shared_database = SharedDatabase(
    name="gong",
    from_share="provider_account.share_name",
    owner="ACCOUNTADMIN",
)
```

## Fields

* `name` (string, required) - The name of the database.
* `from_share` (string, required) - The `<provider_account>.<share_name>` the database is created from. Changing this on an existing shared database is not supported by `plan`/`apply` — it errors at plan time. Drop and recreate the database manually instead.
* `owner` (string or [Role](role.md)) - The owner role of the database. Defaults to `"ACCOUNTADMIN"`.

## Full example: importing a Gong share

A common pattern is importing a Marketplace or direct share, then handing a
scoped role access to it:

```yaml
databases:
  - name: gong
    from_share: provider_account.share_name

roles:
  - name: gong_r

grants:
  - priv: IMPORTED PRIVILEGES
    on_database: gong
    to: gong_r

role_grants:
  - role: gong_r
    to_role: data_engineer
```

Which is equivalent to:

```sql
CREATE DATABASE gong FROM SHARE provider_account.share_name;
CREATE ROLE gong_r;
GRANT IMPORTED PRIVILEGES ON DATABASE gong TO ROLE gong_r;
GRANT ROLE gong_r TO ROLE data_engineer;
```

### Gotchas

- Shared databases are read-only — snowcap does not manage schemas, params,
  or tags on them.
- `CREATE DATABASE ... FROM SHARE` requires the account-level `IMPORT SHARE`
  privilege, which only `ACCOUNTADMIN` holds by default. Snowcap runs the
  creation as `ACCOUNTADMIN` regardless of the configured `owner`.
- `IMPORTED PRIVILEGES` is the only privilege that can be granted on a shared
  database. It cannot be granted `WITH GRANT OPTION`, and it can only be
  granted to account roles, not database roles.
- Snowflake's `SHOW GRANTS` reports `IMPORTED PRIVILEGES` grants on shared
  databases as `USAGE` — snowcap's fetch logic accounts for this quirk, so
  `plan`/`apply` still converge correctly.
- Changing `from_share` on an existing shared database is not supported by
  `plan`/`apply` — it errors at plan time. Drop and recreate the database
  manually if you need to point it at a different share.

## See also

- [Database](database.md) — the non-shared sibling resource
- [Grant](grant.md) — for the `IMPORTED PRIVILEGES` grant shown above
