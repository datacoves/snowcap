from dataclasses import dataclass
from typing import Any

from ..enums import ResourceType
from ..error_formatting import format_invalid_key_error
from ..exceptions import InvalidKeyException
from ..parse import parse_alter_account_parameter
from ..props import Props
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _AccountParameter(ResourceSpec):
    name: ResourceName
    value: Any


class AccountParameter(NamedResource, Resource):
    """
    Description:
        An account parameter in Snowflake that allows you to set or alter account-level parameters.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/alter-account

    Fields:
        name (string, required): The name of the account parameter.
        value (Any, required): The value to set for the account parameter.

    Python:

        ```python
        account_parameter = AccountParameter(
            name="some_parameter",
            value="some_value",
        )
        ```

    Yaml:

        ```yaml
        account_parameters:
          - name: some_parameter
            value: some_value
        ```
    """

    resource_type = ResourceType.ACCOUNT_PARAMETER
    props = Props()
    scope = AccountScope()
    spec = _AccountParameter

    def __init__(self, name: str, value: Any = None, **kwargs):
        # Check for invalid kwargs before calling super().__init__
        valid_keys = ["name", "value", "database", "schema", "lifecycle", "requires"]
        invalid_keys = [k for k in kwargs.keys() if k not in valid_keys]
        if invalid_keys:
            msg, suggestions = format_invalid_key_error(
                invalid_keys=invalid_keys,
                valid_keys=valid_keys,
                resource_type="AccountParameter",
                resource_name=name,
            )
            raise InvalidKeyException(
                msg,
                invalid_keys=invalid_keys,
                valid_keys=valid_keys,
                suggestions=suggestions,
                resource_type="AccountParameter",
                resource_name=name,
            )

        # Check if 'value' is missing (required field)
        if value is None:
            raise ValueError(
                f'AccountParameter "{name}" is missing required key "value".\n'
                f"  Example:\n"
                f"    - name: {name}\n"
                f"      value: YOUR_VALUE"
            )

        super().__init__(name=name, **kwargs)
        self._data: _AccountParameter = _AccountParameter(
            name=self._name,
            value=value,
        )

    @classmethod
    def from_sql(cls, sql):
        props = parse_alter_account_parameter(sql)
        return cls(**props)
