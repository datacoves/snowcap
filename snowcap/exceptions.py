class MissingVarException(Exception):
    pass


class DuplicateResourceException(Exception):
    pass


class MissingResourceException(Exception):
    def __init__(
        self,
        message: str,
        missing_urn: "URN" = None,
        required_by: "URN" = None,
        suggestions: list[str] = None,
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
