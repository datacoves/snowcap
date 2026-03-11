"""
User-friendly error message formatting for Snowcap exceptions.
"""

import difflib
from typing import Optional

from .identifiers import URN


def _get_resource_display_name(urn: URN) -> str:
    """
    Get a display name for a resource that includes full qualification where relevant.

    For schema-scoped resources, includes database.schema.name.
    For database-scoped resources, includes database.name.
    For functions/procedures, includes arg types.
    """
    fqn = urn.fqn
    parts = []

    if fqn.database:
        parts.append(str(fqn.database))
    if fqn.schema:
        parts.append(str(fqn.schema))
    parts.append(str(fqn.name))

    name = ".".join(parts)

    # Add arg types for functions/procedures
    if fqn.arg_types is not None:
        name += f"({', '.join(fqn.arg_types)})"

    return name


def format_missing_resource_error(
    missing_urn: URN,
    required_by_urn: Optional[URN] = None,
    available_names: Optional[list[str]] = None,
) -> str:
    """
    Format a user-friendly error message for missing resources.

    Args:
        missing_urn: The URN of the resource that was not found
        required_by_urn: The URN of the resource that references the missing resource
        available_names: List of available resource names to suggest alternatives

    Returns:
        A formatted error message with context and suggestions
    """
    # Extract readable names
    resource_type = missing_urn.resource_label.replace("_", " ").title()
    resource_name = _get_resource_display_name(missing_urn)

    # Build message
    msg = f'{resource_type} "{resource_name}" not found.'

    # Add context about what references it
    if required_by_urn:
        msg += f"\n  Referenced by: {_format_reference(required_by_urn)}"

    # Add suggestions if available
    if available_names:
        # For matching, use just the base name for better suggestions
        base_name = str(missing_urn.fqn.name)
        suggestions = difflib.get_close_matches(base_name, available_names, n=3, cutoff=0.4)
        if suggestions:
            msg += f"\n  Did you mean: {', '.join(suggestions)}?"

    return msg


def _format_reference(urn: URN) -> str:
    """
    Format a URN as a human-readable reference.

    Args:
        urn: The URN to format

    Returns:
        A human-readable description of the reference
    """
    resource_type = urn.resource_label.replace("_", " ")
    params = urn.fqn.params

    # Special handling for grants
    if "grant" in resource_type:
        if "user" in params:
            return f'{resource_type} to user "{params["user"]}"'
        elif "role" in params:
            return f'{resource_type} to role "{params["role"]}"'
        # Fallback for other grant types - include what we know
        grant_info = []
        if "on" in params:
            grant_info.append(f'on "{params["on"]}"')
        if "to" in params:
            grant_info.append(f'to "{params["to"]}"')
        if grant_info:
            return f'{resource_type} {" ".join(grant_info)}'

    return f'{resource_type} "{_get_resource_display_name(urn)}"'


def format_missing_container_error(container_urn: URN) -> str:
    """
    Format a user-friendly error message for missing container resources.

    Args:
        container_urn: The URN of the container that was not found

    Returns:
        A formatted error message
    """
    resource_type = container_urn.resource_label.replace("_", " ").title()
    resource_name = _get_resource_display_name(container_urn)

    return (
        f'{resource_type} "{resource_name}" not found.\n'
        f"  This resource is referenced but doesn't exist or isn't visible in the current session."
    )


def format_missing_pointer_error(
    urn: URN,
    available_names: Optional[list[str]] = None,
) -> str:
    """
    Format a user-friendly error message for missing resource pointers.

    Args:
        urn: The URN of the missing resource
        available_names: List of available resource names to suggest alternatives

    Returns:
        A formatted error message with suggestions
    """
    resource_type = urn.resource_label.replace("_", " ").title()
    resource_name = _get_resource_display_name(urn)

    msg = f'{resource_type} "{resource_name}" not found.'

    # Add suggestions if available
    if available_names:
        # For matching, use just the base name for better suggestions
        base_name = str(urn.fqn.name)
        suggestions = difflib.get_close_matches(base_name, available_names, n=3, cutoff=0.4)
        if suggestions:
            msg += f"\n  Did you mean: {', '.join(suggestions)}?"

    return msg


def format_invalid_key_error(
    invalid_keys: list[str],
    valid_keys: list[str],
    resource_type: str,
    resource_name: Optional[str] = None,
) -> tuple[str, dict[str, str]]:
    """
    Format a user-friendly error message for invalid keys in resources.

    Args:
        invalid_keys: List of invalid key names that were provided
        valid_keys: List of valid key names for this resource type
        resource_type: The type of resource (e.g., "Database", "Role")
        resource_name: Optional name of the specific resource instance

    Returns:
        A tuple of (formatted error message, dict of suggestions mapping invalid->valid keys)
    """
    suggestions = {}
    for key in invalid_keys:
        matches = difflib.get_close_matches(key, valid_keys, n=1, cutoff=0.6)
        if matches:
            suggestions[key] = matches[0]

    if len(invalid_keys) == 1:
        key = invalid_keys[0]
        if resource_name:
            msg = f'Invalid key "{key}" in {resource_type} "{resource_name}".'
        else:
            msg = f'Invalid key "{key}" in {resource_type}.'
        if key in suggestions:
            msg += f"\n  Did you mean: {suggestions[key]}?"
    else:
        keys_str = ", ".join(f'"{k}"' for k in invalid_keys)
        if resource_name:
            msg = f'Invalid keys {keys_str} in {resource_type} "{resource_name}".'
        else:
            msg = f'Invalid keys {keys_str} in {resource_type}.'
        for key, suggestion in suggestions.items():
            msg += f'\n  "{key}" -> Did you mean: {suggestion}?'

    return msg, suggestions


def format_invalid_role_grant_keys(
    invalid_keys: set[str],
    valid_keys: set[str],
) -> str:
    """
    Format a user-friendly error message for invalid keys in role_grants config.

    Args:
        invalid_keys: Set of invalid key names that were provided
        valid_keys: Set of valid key names for role_grants

    Returns:
        A formatted error message with suggestions
    """
    suggestions = {}
    for key in invalid_keys:
        matches = difflib.get_close_matches(key, list(valid_keys), n=1, cutoff=0.6)
        if matches:
            suggestions[key] = matches[0]

    if len(invalid_keys) == 1:
        key = list(invalid_keys)[0]
        msg = f'Invalid key "{key}" in role_grant.'
        if key in suggestions:
            msg += f"\n  Did you mean: {suggestions[key]}?"
    else:
        keys_str = ", ".join(f'"{k}"' for k in sorted(invalid_keys))
        msg = f"Invalid keys {keys_str} in role_grant."
        for key in sorted(invalid_keys):
            if key in suggestions:
                msg += f'\n  "{key}" -> Did you mean: {suggestions[key]}?'

    return msg
