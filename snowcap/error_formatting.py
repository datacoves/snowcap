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
