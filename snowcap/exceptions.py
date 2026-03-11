from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    from .identifiers import URN


class MissingVarException(Exception):
    pass


class DuplicateResourceException(Exception):
    pass


class MissingResourceException(Exception):
    def __init__(
        self,
        message: str,
        missing_urn: Optional["URN"] = None,
        required_by: Optional["URN"] = None,
        suggestions: Optional[list[str]] = None,
    ):
        self.missing_urn = missing_urn
        self.required_by = required_by
        self.suggestions = suggestions
        super().__init__(message)


class MissingPrivilegeException(Exception):
    pass


class MarkedForReplacementException(Exception):
    pass


class NonConformingPlanException(Exception):
    pass


class ResourceInsertionException(Exception):
    pass


class OrphanResourceException(Exception):
    pass


class InvalidOwnerException(Exception):
    pass


class InvalidResourceException(Exception):
    pass


class WrongContainerException(Exception):
    pass


class WrongEditionException(Exception):
    pass


class ResourceHasContainerException(Exception):
    pass


class NotADAGException(Exception):
    pass


class InvalidKeyException(Exception):
    def __init__(
        self,
        message: str,
        invalid_keys: Optional[list[str]] = None,
        valid_keys: Optional[list[str]] = None,
        suggestions: Optional[dict[str, str]] = None,
        resource_type: Optional[str] = None,
        resource_name: Optional[str] = None,
    ):
        self.invalid_keys = invalid_keys or []
        self.valid_keys = valid_keys or []
        self.suggestions = suggestions or {}
        self.resource_type = resource_type
        self.resource_name = resource_name
        super().__init__(message)


class MultipleValidationErrors(Exception):
    """Exception that collects multiple validation errors to show them all at once."""

    def __init__(self, errors: Sequence[Exception]):
        self.errors = errors
        messages = []
        for i, error in enumerate(errors, 1):
            error_msg = str(error)
            # Indent multi-line errors
            error_lines = error_msg.split("\n")
            if len(error_lines) > 1:
                indented = error_lines[0] + "\n" + "\n".join("  " + line for line in error_lines[1:])
                messages.append(f"{i}. {indented}")
            else:
                messages.append(f"{i}. {error_msg}")
        super().__init__(f"Found {len(errors)} validation error(s):\n\n" + "\n\n".join(messages))
