from dataclasses import dataclass, field

from ..builtins import ALWAYS_BLOCKED_OAUTH_ROLES
from ..enums import AccountEdition, ParseableEnum, ResourceType
from ..props import (
    BoolProp,
    EnumProp,
    IntProp,
    Props,
    StringListProp,
    StringProp,
)
from ..public_key import PEM_DELIMITER
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


def _canonicalize_role_name(name: str) -> str:
    """Snowflake echoes role names back in DESC uppercased, unless the role was
    created with a quoted, case-sensitive identifier. Normalize the same way so a
    manifest role name compares equal to what fetch returns."""
    resource_name = ResourceName(name)
    return resource_name._name if resource_name._quoted else resource_name._name.upper()


class SecurityIntegrationType(ParseableEnum):
    API_AUTHENTICATION = "API_AUTHENTICATION"
    EXTERNAL_OAUTH = "EXTERNAL_OAUTH"
    OAUTH = "OAUTH"
    SAML2 = "SAML2"
    SCIM = "SCIM"


class SAML2Provider(ParseableEnum):
    ADFS = "ADFS"
    CUSTOM = "CUSTOM"
    OKTA = "OKTA"


class OAuthClient(ParseableEnum):
    CUSTOM = "CUSTOM"
    LOOKER = "LOOKER"
    SNOWSERVICES_INGRESS = "SNOWSERVICES_INGRESS"
    TABLEAU_DESKTOP = "TABLEAU_DESKTOP"
    TABLEAU_SERVER = "TABLEAU_SERVER"


class OAuthClientType(ParseableEnum):
    CONFIDENTIAL = "CONFIDENTIAL"
    PUBLIC = "PUBLIC"


class OAuthUseSecondaryRoles(ParseableEnum):
    IMPLICIT = "IMPLICIT"
    NONE = "NONE"


class OAuthAnyRoleMode(ParseableEnum):
    DISABLE = "DISABLE"
    ENABLE = "ENABLE"
    ENABLE_FOR_PRIVILEGE = "ENABLE_FOR_PRIVILEGE"


@dataclass(unsafe_hash=True)
class _SnowflakePartnerOAuthSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.OAUTH
    enabled: bool = True
    oauth_client: OAuthClient = None
    oauth_client_secret: str = field(default=None, metadata={"fetchable": False})
    oauth_redirect_uri: str = field(default=None, metadata={"fetchable": False})
    oauth_issue_refresh_tokens: bool = True
    oauth_refresh_token_validity: int = 7776000
    comment: str = None
    owner: Role = "ACCOUNTADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.oauth_client not in [OAuthClient.LOOKER, OAuthClient.TABLEAU_DESKTOP, OAuthClient.TABLEAU_SERVER]:
            raise ValueError(f"Invalid OAuth client: {self.oauth_client}")


class SnowflakePartnerOAuthSecurityIntegration(NamedResource, Resource):
    """
    Description:
        A security integration in Snowflake designed to manage external OAuth clients for authentication purposes.
        This integration supports specific OAuth clients such as Looker, Tableau Desktop, and Tableau Server.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration

    Fields:
        name (string, required): The name of the security integration.
        enabled (bool): Specifies if the security integration is enabled. Defaults to True.
        oauth_client (string or OAuthClient): The OAuth client used for authentication. Supported clients are 'LOOKER', 'TABLEAU_DESKTOP', and 'TABLEAU_SERVER'.
        oauth_client_secret (string): The secret associated with the OAuth client.
        oauth_redirect_uri (string): The redirect URI configured for the OAuth client.
        oauth_issue_refresh_tokens (bool): Indicates if refresh tokens should be issued. Defaults to True.
        oauth_refresh_token_validity (int): The validity period of the refresh token in seconds.
        comment (string): A comment about the security integration.

    Python:

        ```python
        snowflake_partner_oauth_security_integration = SnowflakePartnerOAuthSecurityIntegration(
            name="some_security_integration",
            enabled=True,
            oauth_client="LOOKER",
            oauth_client_secret="secret123",
            oauth_redirect_uri="https://example.com/oauth/callback",
            oauth_issue_refresh_tokens=True,
            oauth_refresh_token_validity=7776000,
            comment="Integration for Looker OAuth"
        )
        ```

    Yaml:

        ```yaml
        security_integrations:
          - name: some_security_integration
            enabled: true
            oauth_client: LOOKER
            oauth_client_secret: secret123
            oauth_redirect_uri: https://example.com/oauth/callback
            oauth_issue_refresh_tokens: true
            oauth_refresh_token_validity: 7776000
            comment: Integration for Looker OAuth
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.OAUTH]),
        enabled=BoolProp("enabled"),
        oauth_client=EnumProp(
            "oauth_client", [OAuthClient.LOOKER, OAuthClient.TABLEAU_DESKTOP, OAuthClient.TABLEAU_SERVER]
        ),
        oauth_client_secret=StringProp("oauth_client_secret"),
        oauth_redirect_uri=StringProp("oauth_redirect_uri"),
        oauth_issue_refresh_tokens=BoolProp("oauth_issue_refresh_tokens"),
        oauth_refresh_token_validity=IntProp("oauth_refresh_token_validity"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SnowflakePartnerOAuthSecurityIntegration

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        oauth_client: OAuthClient = None,
        oauth_client_secret: str = None,
        oauth_redirect_uri: str = None,
        oauth_issue_refresh_tokens: bool = True,
        oauth_refresh_token_validity: int = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("owner", None)
        super().__init__(name, **kwargs)
        self._data = _SnowflakePartnerOAuthSecurityIntegration(
            name=self._name,
            enabled=enabled,
            oauth_client=oauth_client,
            oauth_client_secret=oauth_client_secret,
            oauth_redirect_uri=oauth_redirect_uri,
            oauth_issue_refresh_tokens=oauth_issue_refresh_tokens,
            oauth_refresh_token_validity=oauth_refresh_token_validity,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _SnowservicesOAuthSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.OAUTH
    oauth_client: OAuthClient = OAuthClient.SNOWSERVICES_INGRESS
    enabled: bool = True
    comment: str = None
    owner: Role = "ACCOUNTADMIN"


class SnowservicesOAuthSecurityIntegration(NamedResource, Resource):
    """
    Description:
        Manages OAuth security integrations for Snowservices in Snowflake, allowing external authentication mechanisms.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration

    Fields:
        name (string, required): The name of the security integration.
        enabled (bool): Specifies if the security integration is enabled. Defaults to True.
        comment (string): A comment about the security integration.

    Python:

        ```python
        snowservices_oauth = SnowservicesOAuthSecurityIntegration(
            name="some_security_integration",
            enabled=True,
            comment="Integration for external OAuth services."
        )
        ```

    Yaml:

        ```yaml
        snowservices_oauth:
          - name: some_security_integration
            enabled: true
            comment: Integration for external OAuth services.
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.OAUTH]),
        oauth_client=EnumProp("oauth_client", [OAuthClient.SNOWSERVICES_INGRESS]),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SnowservicesOAuthSecurityIntegration

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("oauth_client", None)
        kwargs.pop("owner", None)
        super().__init__(name, **kwargs)
        self._data = _SnowservicesOAuthSecurityIntegration(
            name=self._name,
            enabled=enabled,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _APIAuthenticationSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.API_AUTHENTICATION
    auth_type: str = "OAUTH2"
    enabled: bool = True
    oauth_token_endpoint: str = None
    oauth_client_auth_method: str = "CLIENT_SECRET_POST"
    oauth_client_id: str = None
    oauth_client_secret: str = field(default=None, metadata={"fetchable": False})
    oauth_grant: str = None
    oauth_access_token_validity: int = None
    oauth_allowed_scopes: list[str] = None
    comment: str = None
    owner: Role = "ACCOUNTADMIN"


class APIAuthenticationSecurityIntegration(NamedResource, Resource):
    """
    Description:
        Manages API authentication security integrations in Snowflake, allowing for secure API access management.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration

    Fields:
        name (string, required): The unique name of the security integration.
        auth_type (string): The type of authentication used, typically 'OAUTH2'. Defaults to 'OAUTH2'.
        oauth_token_endpoint (string): The endpoint URL for obtaining OAuth tokens.
        oauth_client_auth_method (string): The method used for client authentication, such as 'CLIENT_SECRET_POST'.
        oauth_client_id (string): The client identifier for OAuth.
        oauth_client_secret (string): The client secret for OAuth.
        oauth_grant (string): The OAuth grant type.
        oauth_access_token_validity (int): The validity period of the OAuth access token in seconds. Defaults to 0.
        oauth_allowed_scopes (list): A list of allowed scopes for the OAuth tokens.
        enabled (bool): Indicates if the security integration is enabled. Defaults to True.
        comment (string): An optional comment about the security integration.

    Python:

        ```python
        api_auth_integration = APIAuthenticationSecurityIntegration(
            name="some_api_authentication_security_integration",
            auth_type="OAUTH2",
            oauth_token_endpoint="https://example.com/oauth/token",
            oauth_client_auth_method="CLIENT_SECRET_POST",
            oauth_client_id="your_client_id",
            oauth_client_secret="your_client_secret",
            oauth_grant="client_credentials",
            oauth_access_token_validity=3600,
            oauth_allowed_scopes=["read", "write"],
            enabled=True,
            comment="Integration for external API authentication."
        )
        ```

    Yaml:

        ```yaml
        security_integrations:
        - name: some_api_authentication_security_integration
            type: api_authentication
            auth_type: OAUTH2
            oauth_token_endpoint: https://example.com/oauth/token
            oauth_client_auth_method: CLIENT_SECRET_POST
            oauth_client_id: your_client_id
            oauth_client_secret: your_client_secret
            oauth_grant: client_credentials
            oauth_access_token_validity: 3600
            oauth_allowed_scopes: [read, write]
            enabled: true
            comment: Integration for external API authentication.
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.API_AUTHENTICATION]),
        auth_type=StringProp("auth_type"),
        enabled=BoolProp("enabled"),
        oauth_token_endpoint=StringProp("oauth_token_endpoint"),
        oauth_client_auth_method=StringProp("oauth_client_auth_method"),
        oauth_client_id=StringProp("oauth_client_id"),
        oauth_client_secret=StringProp("oauth_client_secret"),
        oauth_grant=StringProp("oauth_grant"),
        oauth_access_token_validity=IntProp("oauth_access_token_validity"),
        oauth_allowed_scopes=StringListProp("oauth_allowed_scopes", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _APIAuthenticationSecurityIntegration

    def __init__(
        self,
        name: str,
        auth_type: str = "OAUTH2",
        oauth_token_endpoint: str = None,
        oauth_client_auth_method: str = "CLIENT_SECRET_POST",
        oauth_client_id: str = None,
        oauth_client_secret: str = None,
        oauth_grant: str = None,
        oauth_access_token_validity: int = 0,
        oauth_allowed_scopes: list[str] = None,
        enabled: bool = True,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("oauth_client", None)
        kwargs.pop("owner", None)
        super().__init__(name, **kwargs)
        self._data: _APIAuthenticationSecurityIntegration = _APIAuthenticationSecurityIntegration(
            name=self._name,
            auth_type=auth_type,
            oauth_token_endpoint=oauth_token_endpoint,
            oauth_client_auth_method=oauth_client_auth_method,
            oauth_client_id=oauth_client_id,
            oauth_client_secret=oauth_client_secret,
            oauth_grant=oauth_grant,
            oauth_access_token_validity=oauth_access_token_validity,
            oauth_allowed_scopes=oauth_allowed_scopes,
            enabled=enabled,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _SnowflakeCustomOAuthSecurityIntegration(ResourceSpec):
    name: ResourceName
    type: SecurityIntegrationType = SecurityIntegrationType.OAUTH
    enabled: bool = True
    oauth_client: OAuthClient = OAuthClient.CUSTOM
    oauth_client_type: OAuthClientType = field(
        default=None,
        metadata={
            "triggers_replacement": True,
            "replacement_message": (
                "Changing oauth_client_type requires recreating the security integration, "
                "which rotates the Snowflake-issued client_id and client_secret and breaks "
                "live OAuth clients. To change it, recreate the integration manually and "
                "migrate clients to the new credentials."
            ),
        },
    )
    oauth_redirect_uri: str = None
    oauth_allow_non_tls_redirect_uri: bool = False
    oauth_alternate_redirect_uris: list[str] = field(default=None, metadata={"fetchable": False})
    oauth_issue_refresh_tokens: bool = True
    oauth_refresh_token_validity: int = 7776000
    oauth_single_use_refresh_tokens_required: bool = False
    oauth_use_secondary_roles: OAuthUseSecondaryRoles = OAuthUseSecondaryRoles.NONE
    oauth_any_role_mode: OAuthAnyRoleMode = OAuthAnyRoleMode.DISABLE
    oauth_enforce_pkce: bool = False
    # CREATE-only: Snowflake accepts it at creation but DESC never echoes it back, so it
    # can't round-trip. Marking it unfetchable keeps it out of the diff (no phantom drift).
    oauth_enable_role_selection: bool = field(default=False, metadata={"fetchable": False})
    network_policy: str = None
    pre_authorized_roles_list: list[str] = None
    blocked_roles_list: list[str] = None
    comment: str = None
    owner: Role = "ACCOUNTADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.oauth_client_type is None:
            raise ValueError("oauth_client_type must be set")
        if self.oauth_redirect_uri is None:
            raise ValueError("oauth_redirect_uri must be set")
        if self.pre_authorized_roles_list is not None:
            self.pre_authorized_roles_list = sorted(
                _canonicalize_role_name(role) for role in self.pre_authorized_roles_list
            )
        if self.blocked_roles_list is not None:
            self.blocked_roles_list = sorted(_canonicalize_role_name(role) for role in self.blocked_roles_list)
            always_blocked = set(self.blocked_roles_list) & set(ALWAYS_BLOCKED_OAUTH_ROLES)
            if always_blocked:
                raise ValueError(
                    f"blocked_roles_list must not include roles Snowflake always blocks: {sorted(always_blocked)}"
                )


class SnowflakeCustomOAuthSecurityIntegration(NamedResource, Resource):
    """
    Description:
        A security integration in Snowflake for a custom OAuth client that Snowflake itself
        issues and manages. Unlike the partner integrations, Snowflake generates the OAuth
        client_id and client_secret for you; retrieve them with
        SYSTEM$SHOW_OAUTH_CLIENT_SECRETS, they cannot be set through this resource.

        ACCOUNTADMIN, ORGADMIN, GLOBALORGADMIN, and SECURITYADMIN are always blocked by
        Snowflake and must not be listed in blocked_roles_list.

        Every property except oauth_client_type is applied with ALTER SECURITY INTEGRATION
        and never recreates the integration. oauth_client_type cannot be changed after
        creation: snowcap fails the plan rather than recreating the integration, because
        recreation would rotate the Snowflake-issued client_id/client_secret and break live
        OAuth clients. To change it, recreate the integration manually and migrate clients.

        oauth_alternate_redirect_uris is CREATE-ONLY in snowcap: it is not fetchable, so
        editing it in YAML after creation produces no diff and no error. Changes must be
        applied out-of-band with
        ALTER SECURITY INTEGRATION <name> SET OAUTH_ALTERNATE_REDIRECT_URIS = (...).

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake

    Fields:
        name (string, required): The name of the security integration.
        enabled (bool): Specifies if the security integration is enabled. Defaults to True.
        oauth_client_type (string or OAuthClientType, required): The type of OAuth client. Supported values are 'CONFIDENTIAL' and 'PUBLIC'. Cannot be changed after creation.
        oauth_redirect_uri (string, required): The redirect URI the client uses to complete the OAuth flow.
        oauth_allow_non_tls_redirect_uri (bool): Allows non-HTTPS redirect URIs. Defaults to False.
        oauth_alternate_redirect_uris (list): Additional allowed redirect URIs, set at creation only.
        oauth_issue_refresh_tokens (bool): Indicates if refresh tokens should be issued. Defaults to True.
        oauth_refresh_token_validity (int): The validity period of the refresh token in seconds. Defaults to 7776000.
        oauth_single_use_refresh_tokens_required (bool): Requires refresh tokens to be single-use (rotated on each use). Defaults to False.
        oauth_use_secondary_roles (string or OAuthUseSecondaryRoles): Whether secondary roles are activated for OAuth sessions. Supported values are 'IMPLICIT' and 'NONE'. Defaults to 'NONE'.
        oauth_any_role_mode (string or OAuthAnyRoleMode): Whether a client can request any role the user has. Supported values are 'DISABLE', 'ENABLE', and 'ENABLE_FOR_PRIVILEGE'. Defaults to 'DISABLE'.
        oauth_enforce_pkce (bool): Requires clients to use PKCE during the OAuth flow. Defaults to False.
        oauth_enable_role_selection (bool): Lets the client request a role at authorization time. Defaults to False. Set at creation only (DESC does not return it, so it is not reconciled).
        network_policy (string): The network policy enforced for requests made with this integration's tokens.
        pre_authorized_roles_list (list): Roles granted access without displaying a consent screen to the user.
        blocked_roles_list (list): Roles that are not allowed to use this integration.
        comment (string): A comment about the security integration.

    Python:

        ```python
        claude_mcp_oauth = SnowflakeCustomOAuthSecurityIntegration(
            name="claude_mcp_oauth",
            enabled=True,
            oauth_client_type="CONFIDENTIAL",
            oauth_redirect_uri="https://claude.ai/api/mcp/auth_callback",
            oauth_issue_refresh_tokens=True,
            oauth_refresh_token_validity=7776000,
            oauth_use_secondary_roles="NONE",
            oauth_enforce_pkce=True,
            blocked_roles_list=["SYSADMIN"],
            comment="OAuth client for the Claude MCP connector"
        )
        ```

    Yaml:

        ```yaml
        security_integrations:
          - name: claude_mcp_oauth
            enabled: true
            oauth_client_type: CONFIDENTIAL
            oauth_redirect_uri: https://claude.ai/api/mcp/auth_callback
            oauth_issue_refresh_tokens: true
            oauth_refresh_token_validity: 7776000
            oauth_use_secondary_roles: NONE
            oauth_enforce_pkce: true
            blocked_roles_list:
              - SYSADMIN
            comment: OAuth client for the Claude MCP connector
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.OAUTH]),
        enabled=BoolProp("enabled"),
        oauth_client=EnumProp("oauth_client", [OAuthClient.CUSTOM]),
        oauth_client_type=EnumProp(
            "oauth_client_type", [OAuthClientType.CONFIDENTIAL, OAuthClientType.PUBLIC], quoted=True
        ),
        oauth_redirect_uri=StringProp("oauth_redirect_uri"),
        oauth_allow_non_tls_redirect_uri=BoolProp("oauth_allow_non_tls_redirect_uri"),
        oauth_alternate_redirect_uris=StringListProp("oauth_alternate_redirect_uris", parens=True),
        oauth_issue_refresh_tokens=BoolProp("oauth_issue_refresh_tokens"),
        oauth_refresh_token_validity=IntProp("oauth_refresh_token_validity"),
        oauth_single_use_refresh_tokens_required=BoolProp("oauth_single_use_refresh_tokens_required"),
        oauth_use_secondary_roles=EnumProp(
            "oauth_use_secondary_roles", [OAuthUseSecondaryRoles.IMPLICIT, OAuthUseSecondaryRoles.NONE]
        ),
        oauth_any_role_mode=EnumProp(
            "oauth_any_role_mode",
            [OAuthAnyRoleMode.DISABLE, OAuthAnyRoleMode.ENABLE, OAuthAnyRoleMode.ENABLE_FOR_PRIVILEGE],
        ),
        oauth_enforce_pkce=BoolProp("oauth_enforce_pkce"),
        oauth_enable_role_selection=BoolProp("oauth_enable_role_selection"),
        network_policy=StringProp("network_policy"),
        pre_authorized_roles_list=StringListProp("pre_authorized_roles_list", parens=True),
        blocked_roles_list=StringListProp("blocked_roles_list", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SnowflakeCustomOAuthSecurityIntegration

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        oauth_client_type: OAuthClientType = None,
        oauth_redirect_uri: str = None,
        oauth_allow_non_tls_redirect_uri: bool = False,
        oauth_alternate_redirect_uris: list[str] = None,
        oauth_issue_refresh_tokens: bool = True,
        oauth_refresh_token_validity: int = 7776000,
        oauth_single_use_refresh_tokens_required: bool = False,
        oauth_use_secondary_roles: OAuthUseSecondaryRoles = OAuthUseSecondaryRoles.NONE,
        oauth_any_role_mode: OAuthAnyRoleMode = OAuthAnyRoleMode.DISABLE,
        oauth_enforce_pkce: bool = False,
        oauth_enable_role_selection: bool = False,
        network_policy: str = None,
        pre_authorized_roles_list: list[str] = None,
        blocked_roles_list: list[str] = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("owner", None)
        kwargs.pop("oauth_client", None)
        super().__init__(name, **kwargs)
        self._data: _SnowflakeCustomOAuthSecurityIntegration = _SnowflakeCustomOAuthSecurityIntegration(
            name=self._name,
            enabled=enabled,
            oauth_client_type=oauth_client_type,
            oauth_redirect_uri=oauth_redirect_uri,
            oauth_allow_non_tls_redirect_uri=oauth_allow_non_tls_redirect_uri,
            oauth_alternate_redirect_uris=oauth_alternate_redirect_uris,
            oauth_issue_refresh_tokens=oauth_issue_refresh_tokens,
            oauth_refresh_token_validity=oauth_refresh_token_validity,
            oauth_single_use_refresh_tokens_required=oauth_single_use_refresh_tokens_required,
            oauth_use_secondary_roles=oauth_use_secondary_roles,
            oauth_any_role_mode=oauth_any_role_mode,
            oauth_enforce_pkce=oauth_enforce_pkce,
            oauth_enable_role_selection=oauth_enable_role_selection,
            network_policy=network_policy,
            pre_authorized_roles_list=pre_authorized_roles_list,
            blocked_roles_list=blocked_roles_list,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _SAML2SecurityIntegration(ResourceSpec):
    name: ResourceName
    # CREATE-only: Snowflake reads the IdP metadata once and populates saml2_issuer,
    # saml2_sso_url, saml2_provider and saml2_x509_cert from it, which is what DESC
    # reports back. Marking it unfetchable keeps a declared metadata_url from showing
    # as drift on every plan.
    metadata_url: str = field(default=None, metadata={"fetchable": False})
    saml2_issuer: str = None
    saml2_sso_url: str = None
    saml2_provider: SAML2Provider = None
    saml2_x509_cert: str = None
    type: SecurityIntegrationType = SecurityIntegrationType.SAML2
    enabled: bool = True
    saml2_enable_sp_initiated: bool = False
    saml2_sp_initiated_login_page_label: str = None
    saml2_snowflake_acs_url: str = None
    saml2_snowflake_issuer_url: str = None
    saml2_force_authn: bool = False
    saml2_requested_nameid_format: str = None
    saml2_post_logout_redirect_url: str = None
    saml2_sign_request: bool = False
    allowed_user_domains: list[str] = None
    allowed_email_patterns: list[str] = None
    comment: str = None
    owner: Role = "ACCOUNTADMIN"

    def to_dict(self, account_edition: AccountEdition):
        # Snowflake's SQL takes the certificate without its PEM delimiters, and DESC
        # reports it that way, so a certificate pasted straight out of the IdP would
        # otherwise be rejected on apply and read back as drift. Normalized here rather
        # than in __post_init__ because a certificate given as a var is still a template
        # until vars resolve, which happens after the resource is constructed.
        serialized = super().to_dict(account_edition)
        cert = serialized.get("saml2_x509_cert")
        if isinstance(cert, str):
            # Stripping the armor off a private key would ship it to Snowflake, where
            # DESC SECURITY INTEGRATION hands it back to anyone who can read the
            # integration. Reject it the same way normalize_public_key does.
            if "PRIVATE KEY" in cert.upper():
                raise ValueError("saml2_x509_cert contains a private key; provide only the IdP's signing certificate")
            serialized["saml2_x509_cert"] = "".join(PEM_DELIMITER.sub("", cert).split())
        return serialized


class SAML2SecurityIntegration(NamedResource, Resource):
    """
    Description:
        A SAML2 security integration, which federates Snowflake authentication to an external
        identity provider (Okta, ADFS, or any SAML2 IdP). With one in place, clients can connect
        with `authenticator = externalbrowser`.

        Describe the IdP one of two ways: set `metadata_url`, which Snowflake supports for
        Okta and Microsoft Entra ID only, or set all four of `saml2_issuer`, `saml2_sso_url`,
        `saml2_provider` and `saml2_x509_cert`. The two are mutually exclusive. `metadata_url`
        is CREATE-only in snowcap: Snowflake reads the metadata once and reports the derived
        values, so editing the URL later produces no diff and no error, and the change has to
        be applied out of band with
        `ALTER SECURITY INTEGRATION <name> SET METADATA_URL = '...'`.

        Microsoft Entra ID uses `saml2_provider: CUSTOM`. Take `saml2_issuer` from the
        enterprise app's Microsoft Entra Identifier, `saml2_sso_url` from its Login URL, and
        `saml2_x509_cert` from the Base64 SAML signing certificate. Both URLs embed the
        tenant id, so pass them as vars rather than committing them.

        Snowflake matches the IdP's NameID to the user's `login_name`, so every federated user
        needs a `login_name` that matches what the IdP sends.

        To stop the browser from opening on every new connection, also set the `ALLOW_ID_TOKEN`
        account parameter to `true`, which lets the client cache the SSO token in the local
        OS keystore. Caching also depends on the driver: the Python connector caches on Linux
        from 5.0 on, earlier versions need the `secure-local-storage` extra.

        Creating the integration and setting `ALLOW_ID_TOKEN` both require ACCOUNTADMIN.

        `saml2_x509_cert` takes either the bare base64 body of the IdP signing certificate or a
        full PEM block; the PEM header, footer and whitespace are stripped, because that is how
        Snowflake stores it.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-saml2

    Fields:
        name (string, required): The name of the security integration.
        metadata_url (string): The IdP metadata URL, in place of the four saml2_* IdP fields. Okta and Microsoft Entra ID only. Set at creation only.
        saml2_issuer (string): The EntityID or Issuer of the IdP. Required unless metadata_url is set, and rejected if it is.
        saml2_sso_url (string): The IdP SSO URL where Snowflake sends the SAML request. Required unless metadata_url is set, and rejected if it is.
        saml2_provider (string or SAML2Provider): The IdP vendor, one of 'OKTA', 'ADFS', or 'CUSTOM'. Required unless metadata_url is set, and rejected if it is.
        saml2_x509_cert (string): The IdP signing certificate, base64 encoded or as a PEM block. Required unless metadata_url is set, and rejected if it is.
        enabled (bool): Specifies if the security integration is enabled. Defaults to True.
        saml2_enable_sp_initiated (bool): Shows the IdP button on the Snowflake login page for SP-initiated login. Defaults to False.
        saml2_sp_initiated_login_page_label (string): The label of that button on the Snowflake login page.
        saml2_snowflake_acs_url (string): The Snowflake assertion consumer service URL the IdP posts to. Defaults to the account's own URL.
        saml2_snowflake_issuer_url (string): The Snowflake EntityID sent to the IdP. Defaults to the account's own URL.
        saml2_force_authn (bool): Forces the user to re-authenticate at the IdP on every login. Defaults to False.
        saml2_requested_nameid_format (string): The NameID format Snowflake requests from the IdP.
        saml2_post_logout_redirect_url (string): Where the user lands after logging out of Snowflake.
        saml2_sign_request (bool): Signs the SAML request Snowflake sends to the IdP. Defaults to False.
        allowed_user_domains (list): Email domains allowed to authenticate through this integration.
        allowed_email_patterns (list): Regular expressions for email addresses allowed to authenticate through this integration. A pattern containing a comma (a {2,10} quantifier, say) is read back split, so the plan never converges; avoid commas in patterns for now.
        comment (string): A comment about the security integration.

    Python:

        ```python
        sso_saml = SAML2SecurityIntegration(
            name="sso_saml",
            enabled=True,
            saml2_provider="OKTA",
            saml2_issuer="http://www.okta.com/exk1abcdefg",
            saml2_sso_url="https://example.okta.com/app/snowflake/exk1abcdefg/sso/saml",
            saml2_x509_cert="MIIDpDCCAoygAwIBAgIGAV2ka...",
            saml2_enable_sp_initiated=True,
            saml2_sp_initiated_login_page_label="Datacoves SSO",
        )
        ```

    Yaml:

        ```yaml
        # The IdP values identify your tenant and rotate independently of this config,
        # so keep them out of the repo: SNOWCAP_VAR_SAML2_X509_CERT becomes
        # {{ var.saml2_x509_cert }}. Declaring them here with no default makes a missing
        # value fail the run before it reaches Snowflake.
        vars:
          - name: saml2_issuer
            type: string
          - name: saml2_sso_url
            type: string
          - name: saml2_x509_cert
            type: string

        security_integrations:
          - name: sso_saml
            type: SAML2
            enabled: true
            saml2_provider: OKTA
            saml2_issuer: "{{ var.saml2_issuer }}"
            saml2_sso_url: "{{ var.saml2_sso_url }}"
            saml2_x509_cert: "{{ var.saml2_x509_cert }}"
            saml2_enable_sp_initiated: true
            saml2_sp_initiated_login_page_label: Datacoves SSO
        ```
    """

    resource_type = ResourceType.SECURITY_INTEGRATION
    props = Props(
        type=EnumProp("type", [SecurityIntegrationType.SAML2]),
        enabled=BoolProp("enabled"),
        metadata_url=StringProp("metadata_url"),
        saml2_issuer=StringProp("saml2_issuer"),
        saml2_sso_url=StringProp("saml2_sso_url"),
        saml2_provider=EnumProp("saml2_provider", SAML2Provider, quoted=True),
        saml2_x509_cert=StringProp("saml2_x509_cert"),
        saml2_enable_sp_initiated=BoolProp("saml2_enable_sp_initiated"),
        saml2_sp_initiated_login_page_label=StringProp("saml2_sp_initiated_login_page_label"),
        saml2_snowflake_acs_url=StringProp("saml2_snowflake_acs_url"),
        saml2_snowflake_issuer_url=StringProp("saml2_snowflake_issuer_url"),
        saml2_force_authn=BoolProp("saml2_force_authn"),
        saml2_requested_nameid_format=StringProp("saml2_requested_nameid_format"),
        saml2_post_logout_redirect_url=StringProp("saml2_post_logout_redirect_url"),
        saml2_sign_request=BoolProp("saml2_sign_request"),
        allowed_user_domains=StringListProp("allowed_user_domains", parens=True),
        allowed_email_patterns=StringListProp("allowed_email_patterns", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SAML2SecurityIntegration

    def __init__(
        self,
        name: str,
        metadata_url: str = None,
        saml2_issuer: str = None,
        saml2_sso_url: str = None,
        saml2_provider: SAML2Provider = None,
        saml2_x509_cert: str = None,
        enabled: bool = True,
        saml2_enable_sp_initiated: bool = False,
        saml2_sp_initiated_login_page_label: str = None,
        saml2_snowflake_acs_url: str = None,
        saml2_snowflake_issuer_url: str = None,
        saml2_force_authn: bool = False,
        saml2_requested_nameid_format: str = None,
        saml2_post_logout_redirect_url: str = None,
        saml2_sign_request: bool = False,
        allowed_user_domains: list[str] = None,
        allowed_email_patterns: list[str] = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("owner", None)
        # Checked here rather than on the spec because blueprint hydrates remote state
        # with spec(**data): a spec-level raise would abort the whole plan on whatever
        # DESC happened to return, instead of reporting drift.
        idp_fields = {
            "saml2_issuer": saml2_issuer,
            "saml2_sso_url": saml2_sso_url,
            "saml2_provider": saml2_provider,
            "saml2_x509_cert": saml2_x509_cert,
        }
        if metadata_url is None:
            for field_name, value in idp_fields.items():
                if value is None:
                    raise ValueError(f"{field_name} must be set, or set metadata_url instead")
        else:
            set_too = [f for f, v in idp_fields.items() if v is not None]
            if set_too:
                raise ValueError(f"metadata_url cannot be combined with {', '.join(set_too)}")
        super().__init__(name, **kwargs)
        self._data: _SAML2SecurityIntegration = _SAML2SecurityIntegration(
            name=self._name,
            metadata_url=metadata_url,
            saml2_issuer=saml2_issuer,
            saml2_sso_url=saml2_sso_url,
            saml2_provider=saml2_provider,
            saml2_x509_cert=saml2_x509_cert,
            enabled=enabled,
            saml2_enable_sp_initiated=saml2_enable_sp_initiated,
            saml2_sp_initiated_login_page_label=saml2_sp_initiated_login_page_label,
            saml2_snowflake_acs_url=saml2_snowflake_acs_url,
            saml2_snowflake_issuer_url=saml2_snowflake_issuer_url,
            saml2_force_authn=saml2_force_authn,
            saml2_requested_nameid_format=saml2_requested_nameid_format,
            saml2_post_logout_redirect_url=saml2_post_logout_redirect_url,
            saml2_sign_request=saml2_sign_request,
            allowed_user_domains=allowed_user_domains,
            allowed_email_patterns=allowed_email_patterns,
            comment=comment,
        )


def _resolver(data: dict):
    security_integration_type = SecurityIntegrationType(data["type"])
    if security_integration_type == SecurityIntegrationType.API_AUTHENTICATION:
        return APIAuthenticationSecurityIntegration
    elif security_integration_type == SecurityIntegrationType.SAML2:
        return SAML2SecurityIntegration
    elif security_integration_type == SecurityIntegrationType.OAUTH:
        oauth_client = OAuthClient(data["oauth_client"])
        if oauth_client in [
            OAuthClient.LOOKER,
            OAuthClient.TABLEAU_DESKTOP,
            OAuthClient.TABLEAU_SERVER,
        ]:
            return SnowflakePartnerOAuthSecurityIntegration
        elif oauth_client == OAuthClient.CUSTOM:
            return SnowflakeCustomOAuthSecurityIntegration
        elif oauth_client == OAuthClient.SNOWSERVICES_INGRESS:
            return SnowservicesOAuthSecurityIntegration
    return None


Resource.__resolvers__[ResourceType.SECURITY_INTEGRATION] = _resolver
