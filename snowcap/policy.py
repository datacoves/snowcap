"""
Policies are used to enforce constraints over a Snowcap project.

All Snowcap projects use the Snowcap Standard Policy by default
"""

from enum import Enum
from typing import Callable


class EnforcementLevel(Enum):
    ADVISORY = "ADVISORY"
    MANDATORY = "MANDATORY"


class Policy:
    def __init__(self, name: str, description: str, enforcement_level: str, validate: Callable):
        self.name = name
        self.description = description
        self.enforcement_level = enforcement_level
        self.validate = validate


class OwnershipPolicy(Policy):
    pass


class PolicyPack:
    def __init__(self, name: str, policies):
        self.name = name
        self.policies = policies
