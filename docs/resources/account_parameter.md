---
description: >-
  An account parameter in Snowflake.
---

# AccountParameter

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/alter-account)

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


