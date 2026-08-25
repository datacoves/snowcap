import base64
import binascii
import hashlib
import re
from dataclasses import dataclass, field
from typing import Optional

from ..enums import AccountEdition, ResourceType
from ..identifiers import FQN
from ..props import IntProp, Props, StringProp
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role
from .user import User

FINGERPRINT_PREFIX = "SHA256:"

_PEM_DELIMITER = re.compile(r"-{2,}[A-Z ]*-{2,}")

# Snowflake reserves these names for the keys set through the legacy RSA_PUBLIC_KEY and
# RSA_PUBLIC_KEY_2 user properties. They can't be added, modified, rotated, or removed
# with the KEY PAIR commands.
RESERVED_KEY_PAIR_NAMES = (ResourceName("PUBLIC_KEY_1"), ResourceName("PUBLIC_KEY_2"))

# Snowflake keeps the prior key of a rotation under a generated name of this shape until
# it expires. Those tombstones are not resources anyone declares.
ROTATED_KEY_PAIR_NAME = re.compile(r"_ROTATED_\d+$")


def normalize_public_key(public_key: str) -> str:
    """
    The single-line, delimiter-free form of a public key that Snowflake's SQL expects.

    Snowflake's docs are explicit that the public key delimiters are excluded from the
    SQL statement, so a key pasted straight out of a .pub file is accepted here and the
    `-----BEGIN PUBLIC KEY-----` wrapper and newlines are removed.
    """
    return "".join(_PEM_DELIMITER.sub("", public_key).split())


def public_key_fingerprint(public_key: str) -> str:
    """
    The SHA-256 fingerprint Snowflake reports for a public key.

    Snowflake never echoes a named key pair's public key back -- SHOW USER KEY PAIRS
    returns a fingerprint and nothing else -- so drift on the key itself is detected by
    computing the same fingerprint locally. The fingerprint is the base64-encoded SHA-256
    digest of the key's DER (SubjectPublicKeyInfo) bytes, which is exactly what the
    base64 body of a PEM public key decodes to. It matches:

        openssl rsa -pubin -in rsa_key.pub -outform DER | openssl dgst -sha256 -binary | openssl enc -base64

    https://docs.snowflake.com/en/user-guide/key-pair-auth
    """
    key = normalize_public_key(public_key)
    if not key:
        raise ValueError("public_key is empty")
    try:
        der = base64.b64decode(key, validate=True)
    except (binascii.Error, ValueError) as err:
        raise ValueError(f"public_key is not valid base64-encoded key material: {err}") from err
    if not der:
        raise ValueError("public_key is empty")
    return FINGERPRINT_PREFIX + base64.b64encode(hashlib.sha256(der).digest()).decode("utf-8")


def normalize_fingerprint(fingerprint: str) -> str:
    """
    A fingerprint in the `SHA256:<base64>` form snowcap compares on, whether or not
    Snowflake included the prefix.
    """
    fingerprint = fingerprint.strip()
    if fingerprint.upper().startswith(FINGERPRINT_PREFIX):
        fingerprint = fingerprint[len(FINGERPRINT_PREFIX) :]
    return FINGERPRINT_PREFIX + fingerprint


def key_pair_is_rotated_out(name: str) -> bool:
    """True for the `<name>_ROTATED_<epoch_ms>` tombstone a rotation leaves behind."""
    return ROTATED_KEY_PAIR_NAME.search(name) is not None


@dataclass(unsafe_hash=True)
class _UserKeyPair(ResourceSpec):
    name: ResourceName
    user: User
    # A key pair is not a standalone object in Snowflake and has no owner of its own.
    # This names the role that manages the user the key pair belongs to, which is the
    # role snowcap runs the ALTER USER statements as.
    owner: Role = field(default="USERADMIN", metadata={"fetchable": False})
    # Snowflake stores the public key but never returns it, so config is authoritative
    # and drift is detected through `fingerprint` instead.
    public_key: str = field(default=None, metadata={"fetchable": False})
    fingerprint: str = None
    role_restriction: Role = field(
        default=None,
        metadata={
            "triggers_replacement": True,
            "replacement_message": (
                "Snowflake cannot change the role restriction of an existing key pair. "
                "Remove the key pair from config, apply, then add it back with the new role_restriction."
            ),
        },
    )
    # Snowflake reports an absolute expires_at rather than the relative value that was
    # registered, so comparing the two would report drift on every plan. Config wins.
    days_to_expiry: int = field(default=None, metadata={"fetchable": False})
    disabled: bool = False
    comment: str = None

    def __post_init__(self):
        super().__post_init__()

        if self.name in RESERVED_KEY_PAIR_NAMES:
            raise ValueError(
                f"{self.name} is reserved by Snowflake for the legacy rsa_public_key and "
                "rsa_public_key_2 user properties. Set those on the user resource instead."
            )
        if key_pair_is_rotated_out(str(self.name)):
            raise ValueError(
                f"{self.name} names a rotated-out key pair. Snowflake generates those names "
                "during rotation and they cannot be managed directly."
            )
        if self.days_to_expiry is not None and self.days_to_expiry < 1:
            raise ValueError("days_to_expiry must be 1 or greater")

        # Vars are resolved after the resource is constructed, so a public key that is
        # still a template is left alone here and normalized in to_dict instead.
        if isinstance(self.public_key, str):
            self.public_key = normalize_public_key(self.public_key)
            self.fingerprint = public_key_fingerprint(self.public_key)

    def to_dict(self, account_edition: AccountEdition):
        serialized = super().to_dict(account_edition)
        public_key = serialized.get("public_key")
        # Recompute rather than trust the stored value: a public key given as a var is a
        # template string until vars are resolved, which happens after __post_init__.
        if isinstance(public_key, str):
            serialized["public_key"] = normalize_public_key(public_key)
            serialized["fingerprint"] = public_key_fingerprint(serialized["public_key"])
        return serialized


class UserKeyPair(NamedResource, Resource):
    """
    Description:
        A named key pair registered for a user, used for key-pair authentication.

        Named key pairs are the recommended alternative to the legacy rsa_public_key and
        rsa_public_key_2 user properties: a user can hold up to 10 of them, and each one
        can carry its own role restriction, expiration, and comment.

        Snowflake never returns the public key itself, only its SHA-256 fingerprint, so
        snowcap compares the fingerprint of the configured key against the one Snowflake
        reports. Changing public_key plans a key rotation (ALTER USER ... ROTATE KEY PAIR),
        which keeps the prior key valid for a grace period so clients can pick up the new
        one without downtime.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/alter-user-add-key-pair

    Fields:
        name (string, required): The name of the key pair.
        user (string or User, required): The user the key pair is registered for.
        public_key (string, required): The public key, with or without PEM delimiters.
        owner (string or Role): The role that manages the user. Defaults to "USERADMIN".
        role_restriction (string or Role): The role a session authenticated with this key pair
            is restricted to. The role must already be granted to the user.
        days_to_expiry (int): The number of days the key pair can be used for authentication.
        disabled (bool): Whether the key pair is disabled. Defaults to False.
        comment (string): A comment for the key pair.

    Python:

        ```python
        key_pair = UserKeyPair(
            name="my_key",
            user="some_user",
            public_key="MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgK...",
            comment="primary workload key",
        )
        ```

    Yaml:

        ```yaml
        user_key_pairs:
          - name: my_key
            user: some_user
            public_key: MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgK...
            comment: primary workload key
        ```

    """

    resource_type = ResourceType.USER_KEY_PAIR
    props = Props(
        public_key=StringProp("public_key"),
        role_restriction=StringProp("role_restriction"),
        days_to_expiry=IntProp("days_to_expiry"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _UserKeyPair

    def __init__(
        self,
        name: str,
        user: str,
        public_key: str = None,
        owner: str = "USERADMIN",
        role_restriction: str = None,
        days_to_expiry: int = None,
        disabled: bool = False,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        if public_key is None:
            raise ValueError(f"public_key is required for user key pair {name}")
        self._data: _UserKeyPair = _UserKeyPair(
            name=self._name,
            user=user,
            owner=owner,
            public_key=public_key,
            role_restriction=role_restriction,
            days_to_expiry=days_to_expiry,
            disabled=disabled,
            comment=comment,
        )
        self.requires(self._data.user)
        if self._data.role_restriction is not None:
            self.requires(self._data.role_restriction)

    def __repr__(self):  # pragma: no cover
        name = getattr(self._data, "name", "")
        user = getattr(self._data, "user", "")
        return f"UserKeyPair(name={name}, user={user})"

    @property
    def fqn(self):
        return user_key_pair_fqn(self._data)

    @property
    def user(self) -> User:
        return self._data.user

    @property
    def fingerprint(self) -> Optional[str]:
        return self._data.fingerprint


def user_key_pair_fqn(key_pair: _UserKeyPair) -> FQN:
    return FQN(name=key_pair.name, params={"user": str(key_pair.user.name)})
