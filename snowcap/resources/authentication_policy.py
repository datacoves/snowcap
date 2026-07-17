from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import EnumProp, IntProp, Props, PropSet, StringListProp, StringProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


class AuthenticationMethods(ParseableEnum):
    ALL = "ALL"
    PASSWORD = "PASSWORD"
    SAML = "SAML"
    OAUTH = "OAUTH"
    KEYPAIR = "KEYPAIR"
    PROGRAMMATIC_ACCESS_TOKEN = "PROGRAMMATIC_ACCESS_TOKEN"


class MFAEnrollment(ParseableEnum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"


class ClientTypes(ParseableEnum):
    ALL = "ALL"
    SNOWFLAKE_UI = "SNOWFLAKE_UI"
    DRIVERS = "DRIVERS"
    SNOWSQL = "SNOWSQL"


class NetworkPolicyEvaluation(ParseableEnum):
    ENFORCED_REQUIRED = "ENFORCED_REQUIRED"
    ENFORCED_NOT_REQUIRED = "ENFORCED_NOT_REQUIRED"
    NOT_ENFORCED = "NOT_ENFORCED"


# Single source of truth for the PAT_POLICY sub-key schema, shared by __post_init__
# (to derive the required-key set) and AuthenticationPolicy.props (to render/parse it).
_PAT_POLICY_PROPS = Props(
    network_policy_evaluation=EnumProp("NETWORK_POLICY_EVALUATION", NetworkPolicyEvaluation),
    default_expiry_in_days=IntProp("default_expiry_in_days"),
    max_expiry_in_days=IntProp("max_expiry_in_days"),
)


@dataclass(unsafe_hash=True)
class _AuthenticationPolicy(ResourceSpec):
    name: ResourceName
    authentication_methods: list[AuthenticationMethods] = None
    mfa_authentication_methods: list[AuthenticationMethods] = None
    mfa_enrollment: MFAEnrollment = "OPTIONAL"
    client_types: list[ClientTypes] = None
    security_integrations: list[str] = None
    pat_policy: dict = None
    comment: str = None
    owner: RoleRef = "SECURITYADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.authentication_methods is None:
            self.authentication_methods = [AuthenticationMethods.ALL]

        # NOTE: mfa_authentication_methods is DEPRECATED as of Snowflake 2025_06 bundle.
        # It has been replaced by MFA_POLICY with ALLOWED_METHODS property.
        # When None is passed, we intentionally do NOT set a default to avoid
        # generating the deprecated SQL parameter.
        if self.mfa_authentication_methods is not None:
            for method in self.mfa_authentication_methods:
                if method not in (
                    AuthenticationMethods.ALL,
                    AuthenticationMethods.SAML,
                    AuthenticationMethods.PASSWORD,
                ):
                    raise ValueError("MFA authentication methods must be either 'ALL', 'SAML', or 'PASSWORD'")
            if (
                len(self.mfa_authentication_methods) == 1
                and self.mfa_authentication_methods[0] == AuthenticationMethods.ALL
            ):
                self.mfa_authentication_methods = [AuthenticationMethods.PASSWORD, AuthenticationMethods.SAML]

        if self.client_types is None:
            self.client_types = [ClientTypes.ALL]

        if self.security_integrations is None:
            self.security_integrations = ["ALL"]

        if self.pat_policy is not None:
            self.pat_policy = {k: v for k, v in self.pat_policy.items() if k in _PAT_POLICY_PROPS.props}
            required_keys = set(_PAT_POLICY_PROPS.props)
            missing_keys = required_keys - self.pat_policy.keys()
            if missing_keys:
                raise ValueError(f"pat_policy is missing required keys: {sorted(missing_keys)}")

            for key, prop in _PAT_POLICY_PROPS.props.items():
                self.pat_policy[key] = prop.typecheck(self.pat_policy[key])
            if self.pat_policy["default_expiry_in_days"] > self.pat_policy["max_expiry_in_days"]:
                raise ValueError("pat_policy.default_expiry_in_days cannot exceed pat_policy.max_expiry_in_days")


class AuthenticationPolicy(NamedResource, Resource):
    """
    Description:
        Defines the rules and constraints for authentication within the system, ensuring they meet specific security standards.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy

    Fields:
        name (string, required): The name of the authentication policy.
        authentication_methods (list): A list of allowed authentication methods.
        mfa_authentication_methods (list): A list of authentication methods that enforce multi-factor authentication (MFA).
        mfa_enrollment (string): Determines whether a user must enroll in multi-factor authentication. Defaults to OPTIONAL.
        client_types (list): A list of clients that can authenticate with Snowflake.
        security_integrations (list): A list of security integrations the authentication policy is associated with.
        pat_policy (dict): Controls programmatic access token issuance: network_policy_evaluation, default_expiry_in_days, and max_expiry_in_days must all be given or all omitted; declaring exactly the Snowflake defaults (ENFORCED_REQUIRED, 15, 365) compares as unset.
        comment (string): A comment or description for the authentication policy.
        owner (string or Role): The owner role of the authentication policy. Defaults to SECURITYADMIN.

    Python:

        ```python
        authentication_policy = AuthenticationPolicy(
            name="some_authentication_policy",
            authentication_methods=["PASSWORD", "SAML", "PROGRAMMATIC_ACCESS_TOKEN"],
            mfa_authentication_methods=["PASSWORD"],
            mfa_enrollment="REQUIRED",
            client_types=["SNOWFLAKE_UI"],
            security_integrations=["ALL"],
            pat_policy={
                "network_policy_evaluation": "ENFORCED_NOT_REQUIRED",
                "default_expiry_in_days": 30,
                "max_expiry_in_days": 180,
            },
            comment="Policy for secure authentication."
        )
        ```

    Yaml:

        ```yaml
        authentication_policies:
          - name: some_authentication_policy
            authentication_methods:
              - PASSWORD
              - SAML
              - PROGRAMMATIC_ACCESS_TOKEN
            mfa_authentication_methods:
              - PASSWORD
            mfa_enrollment: REQUIRED
            client_types:
              - SNOWFLAKE_UI
            security_integrations:
              - ALL
            pat_policy:
              network_policy_evaluation: ENFORCED_NOT_REQUIRED
              default_expiry_in_days: 30
              max_expiry_in_days: 180
            comment: Policy for secure authentication.
        ```
    """

    resource_type = ResourceType.AUTHENTICATION_POLICY
    props = Props(
        authentication_methods=StringListProp("authentication_methods", parens=True),
        mfa_authentication_methods=StringListProp("mfa_authentication_methods", parens=True),
        mfa_enrollment=EnumProp("mfa_enrollment", MFAEnrollment),
        client_types=StringListProp("client_types", parens=True),
        security_integrations=StringListProp("security_integrations", parens=True),
        pat_policy=PropSet("PAT_POLICY", _PAT_POLICY_PROPS),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _AuthenticationPolicy

    def __init__(
        self,
        name: str,
        authentication_methods: list[str] = None,
        mfa_authentication_methods: list[str] = None,
        mfa_enrollment: str = "OPTIONAL",
        client_types: list[str] = None,
        security_integrations: list[str] = None,
        pat_policy: dict = None,
        comment: str = None,
        owner: str = "SECURITYADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _AuthenticationPolicy = _AuthenticationPolicy(
            name=self._name,
            authentication_methods=authentication_methods,
            mfa_authentication_methods=mfa_authentication_methods,
            mfa_enrollment=mfa_enrollment,
            client_types=client_types,
            security_integrations=security_integrations,
            pat_policy=pat_policy,
            comment=comment,
            owner=owner,
        )
