import difflib
from typing import Any

import jinja2.exceptions
from jinja2 import Environment, StrictUndefined

from .exceptions import MissingVarException

GLOBAL_JINJA_ENV = Environment(undefined=StrictUndefined)


def _format_missing_key_error(key: str, available_keys: list[str], context: str = "") -> str:
    """Format a helpful error message for missing keys with suggestions."""
    suggestions = difflib.get_close_matches(key, available_keys, n=1, cutoff=0.6)

    if context:
        msg = f'Key "{key}" not found in {context}.'
    else:
        msg = f'Key "{key}" not found.'

    if suggestions:
        msg += f'\n  Did you mean: {suggestions[0]}?'

    if available_keys:
        msg += f'\n  Available keys: {", ".join(sorted(available_keys))}'

    return msg


class VarString:
    def __init__(self, string: str):
        self.string = string

    def to_string(self, vars: dict, parent: dict):
        try:
            return GLOBAL_JINJA_ENV.from_string(self.string).render(var=vars, parent=parent)
        except jinja2.exceptions.UndefinedError:
            raise MissingVarException(f"Missing var: {self.string}")

    def __eq__(self, other: Any):
        return False

    def __repr__(self):
        return f"VarString({self.string})"


class VarStub(dict):
    def __missing__(self, key) -> str:
        # Return the string "{{ var.key }}" if the key is not found
        return f"{{{{ var.{key} }}}}"


class ParentStub(dict):
    def __missing__(self, key) -> str:
        # Return the string "{{ parent.key }}" if the key is not found
        return f"{{{{ parent.{key} }}}}"


def __getattr__(name) -> VarString:
    # This function will be called when an attribute is not found in the module
    # You can implement your logic here to return dynamic properties
    return VarString("{{var." + name + "}}")


def string_contains_var(string: str) -> bool:
    return "{{" in string and "}}" in string


def process_for_each(resource_value: str, each_value: Any) -> str:
    vars = VarStub()
    parent = ParentStub()
    try:
        return GLOBAL_JINJA_ENV.from_string(resource_value).render(var=vars, parent=parent, each={"value": each_value})
    except jinja2.exceptions.UndefinedError as e:
        # Extract the missing key from the error message
        # Jinja2 error format: "'dict object' has no attribute 'key_name'"
        error_str = str(e)
        if "has no attribute" in error_str:
            # Extract the key name from the error message
            import re

            match = re.search(r"'(\w+)'$", error_str)
            if match and isinstance(each_value, dict):
                missing_key = match.group(1)
                available_keys = list(each_value.keys())
                raise MissingVarException(
                    _format_missing_key_error(missing_key, available_keys, context="for_each item")
                ) from e
        # Fallback to original error if we can't parse it
        raise MissingVarException(f"Error in for_each template '{resource_value}': {e}") from e
