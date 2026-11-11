---
description: >-
  An account parameter in Snowflake. Manage it as code with Snowcap, with YAML and Python examples and a reference for every field, its type and its default.
---

# AccountParameter

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/alter-account) | Snowcap CLI label: `account_parameter`

An account parameter in Snowflake that allows you to set or alter account-level parameters.

For a complete list of available parameters, see the [Snowflake Parameters Reference](https://docs.snowflake.com/en/sql-reference/parameters).


## Examples

### YAML

```yaml
account_parameters:
  - name: TIMEZONE
    value: America/New_York

  - name: STATEMENT_TIMEOUT_IN_SECONDS
    value: 3600
```


### Python

```python
account_parameter = AccountParameter(
    name="TIMEZONE",
    value="America/New_York",
)
```


## Fields

* `name` (string, required) - The name of the account parameter. See the [Snowflake Parameters Reference](https://docs.snowflake.com/en/sql-reference/parameters) for valid parameter names.
* `value` (Any, required) - The value to set for the account parameter.

**Note:** Requires ACCOUNTADMIN role. This privilege cannot be granted to other roles. See [Snowflake Permissions](../snowflake-permissions.md#resources-that-require-accountadmin) for details.


