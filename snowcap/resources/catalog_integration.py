from dataclasses import dataclass, field

from .resource import Resource, ResourceSpec, NamedResource
from .role import Role
from ..enums import ParseableEnum, ResourceType
from ..props import Props, EnumProp, StringProp, BoolProp, IntProp, PropSet
from ..resource_name import ResourceName
from ..scope import AccountScope


class CatalogSource(ParseableEnum):
    GLUE = "GLUE"
    OBJECT_STORE = "OBJECT_STORE"
    ICEBERG_REST = "ICEBERG_REST"


class CatalogApiType(ParseableEnum):
    AWS_GLUE = "AWS_GLUE"
    AWS_API_GATEWAY = "AWS_API_GATEWAY"
    PUBLIC = "PUBLIC"


class AccessDelegationMode(ParseableEnum):
    VENDED_CREDENTIALS = "VENDED_CREDENTIALS"


class RestAuthenticationType(ParseableEnum):
    SIGV4 = "SIGV4"
    OAUTH = "OAUTH"
    BEARER = "BEARER"
    NONE = "NONE"


class CatalogTableFormat(ParseableEnum):
    ICEBERG = "ICEBERG"


@dataclass(unsafe_hash=True)
class _GlueCatalogIntegration(ResourceSpec):
    name: ResourceName
    glue_aws_role_arn: str
    glue_catalog_id: str
    catalog_namespace: str
    enabled: bool
    catalog_source: CatalogSource = CatalogSource.GLUE
    table_format: CatalogTableFormat = CatalogTableFormat.ICEBERG
    glue_region: str = None
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.catalog_source not in [CatalogSource.GLUE]:
            raise ValueError(f"Invalid catalog source: {self.catalog_source}")
        if self.table_format not in [CatalogTableFormat.ICEBERG]:
            raise ValueError(f"Invalid table format: {self.table_format}")


class GlueCatalogIntegration(NamedResource, Resource):
    """
    Description:
        Manages the integration of AWS Glue as a catalog in Snowflake, supporting the ICEBERG table format.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration

    Fields:
        name (string, required): The name of the catalog integration.
        table_format (string or CatalogTableFormat, required): The format of the table, defaults to ICEBERG.
        glue_aws_role_arn (string, required): The ARN for the AWS role to assume.
        glue_catalog_id (string, required): The Glue catalog ID.
        catalog_namespace (string, required): The namespace of the catalog.
        enabled (bool, required): Specifies whether the catalog integration is enabled.
        glue_region (string): The AWS region of the Glue catalog. Defaults to None.
        owner (string or Role): The owner role of the catalog integration. Defaults to "ACCOUNTADMIN".
        comment (string): An optional comment describing the catalog integration.

    Python:

        ```python
        glue_catalog_integration = GlueCatalogIntegration(
            name="some_catalog_integration",
            table_format="ICEBERG",
            glue_aws_role_arn="arn:aws:iam::123456789012:role/SnowflakeAccess",
            glue_catalog_id="some_glue_catalog_id",
            catalog_namespace="some_namespace",
            enabled=True,
            glue_region="us-west-2",
            comment="Integration for AWS Glue with Snowflake."
        )
        ```

    Yaml:

        ```yaml
        catalog_integrations:
          - name: some_catalog_integration
            table_format: ICEBERG
            glue_aws_role_arn: arn:aws:iam::123456789012:role/SnowflakeAccess
            glue_catalog_id: some_glue_catalog_id
            catalog_namespace: some_namespace
            enabled: true
            glue_region: us-west-2
            comment: Integration for AWS Glue with Snowflake.
        ```
    """

    resource_type = ResourceType.CATALOG_INTEGRATION
    props = Props(
        catalog_source=EnumProp("catalog_source", CatalogSource),
        table_format=EnumProp("table_format", CatalogTableFormat),
        glue_aws_role_arn=StringProp("glue_aws_role_arn"),
        glue_catalog_id=StringProp("glue_catalog_id"),
        catalog_namespace=StringProp("catalog_namespace"),
        glue_region=StringProp("glue_region"),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GlueCatalogIntegration

    def __init__(
        self,
        name: str,
        table_format: CatalogTableFormat,
        glue_aws_role_arn: str,
        glue_catalog_id: str,
        catalog_namespace: str,
        enabled: bool,
        glue_region: str = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("catalog_source", None)
        super().__init__(name, **kwargs)
        self._data: _GlueCatalogIntegration = _GlueCatalogIntegration(
            name=self._name,
            glue_aws_role_arn=glue_aws_role_arn,
            glue_catalog_id=glue_catalog_id,
            catalog_namespace=catalog_namespace,
            table_format=table_format,
            glue_region=glue_region,
            enabled=enabled,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _ObjectStoreCatalogIntegration(ResourceSpec):
    name: ResourceName
    catalog_source: CatalogSource = CatalogSource.OBJECT_STORE
    table_format: CatalogTableFormat = CatalogTableFormat.ICEBERG
    enabled: bool = True
    comment: str = None
    owner: Role = "ACCOUNTADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.catalog_source not in [CatalogSource.OBJECT_STORE]:
            raise ValueError(f"Invalid catalog source: {self.catalog_source}")
        if self.table_format not in [CatalogTableFormat.ICEBERG]:
            raise ValueError(f"Invalid table format: {self.table_format}")


class ObjectStoreCatalogIntegration(NamedResource, Resource):
    """
    Description:
        Manages the integration of an object store as a catalog in Snowflake, supporting the ICEBERG table format.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration

    Fields:
        name (string, required): The name of the catalog integration.
        table_format (string or CatalogTableFormat, required): The format of the table, defaults to ICEBERG.
        enabled (bool): Specifies whether the catalog integration is enabled. Defaults to True.
        comment (string): An optional comment describing the catalog integration.

    Python:

        ```python
        object_store_catalog_integration = ObjectStoreCatalogIntegration(
            name="some_catalog_integration",
            table_format="ICEBERG",
            enabled=True,
            comment="Integration for object storage."
        )
        ```

    Yaml:

        ```yaml
        catalog_integrations:
          - name: some_catalog_integration
            table_format: ICEBERG
            enabled: true
            comment: Integration for object storage.
        ```
    """

    resource_type = ResourceType.CATALOG_INTEGRATION
    props = Props(
        catalog_source=EnumProp("catalog_source", CatalogSource),
        table_format=EnumProp("table_format", CatalogTableFormat),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _ObjectStoreCatalogIntegration

    def __init__(
        self,
        name: str,
        table_format: CatalogTableFormat,
        enabled: bool = True,
        comment: str = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        kwargs.pop("catalog_source", None)
        super().__init__(name, **kwargs)
        self._data: _ObjectStoreCatalogIntegration = _ObjectStoreCatalogIntegration(
            name=self._name,
            table_format=table_format,
            enabled=enabled,
            comment=comment,
            owner=owner,
        )


@dataclass(unsafe_hash=True)
class _IcebergRestCatalogIntegration(ResourceSpec):
    name: ResourceName
    # rest_config / rest_authentication contain auto-populated fields when
    # fetched (Snowflake echoes WAREHOUSE = CATALOG_NAME when not explicitly
    # set, etc.) so we mark them non-fetchable: YAML is authoritative. Same
    # precedent as Stage.encryption.
    rest_config: dict = field(default=None, metadata={"fetchable": False})
    rest_authentication: dict = field(default=None, metadata={"fetchable": False})
    catalog_namespace: str = None
    enabled: bool = True
    refresh_interval_seconds: int = None
    catalog_source: CatalogSource = CatalogSource.ICEBERG_REST
    table_format: CatalogTableFormat = CatalogTableFormat.ICEBERG
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.catalog_source not in [CatalogSource.ICEBERG_REST]:
            raise ValueError(f"Invalid catalog source: {self.catalog_source}")
        if self.table_format not in [CatalogTableFormat.ICEBERG]:
            raise ValueError(f"Invalid table format: {self.table_format}")
        if not self.rest_config:
            raise ValueError("rest_config is required for IcebergRestCatalogIntegration")
        if "catalog_uri" not in self.rest_config:
            raise ValueError("rest_config.catalog_uri is required")
        if not self.rest_authentication:
            raise ValueError("rest_authentication is required for IcebergRestCatalogIntegration")
        if "type" not in self.rest_authentication:
            raise ValueError("rest_authentication.type is required (e.g., SIGV4, OAUTH, BEARER, NONE)")


class IcebergRestCatalogIntegration(NamedResource, Resource):
    """
    Description:
        Manages an Apache Iceberg REST catalog integration in Snowflake. This is the
        right choice for AWS S3 Tables (federated catalogs reachable via the Glue
        Iceberg REST endpoint) and any other Iceberg REST-compatible catalog.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest

    Fields:
        name (string, required): The name of the catalog integration.
        rest_config (dict, required): Iceberg REST configuration. Required keys:
            ``catalog_uri``. Common keys: ``catalog_api_type`` (AWS_GLUE,
            AWS_API_GATEWAY, PUBLIC), ``catalog_name``, ``warehouse``, ``prefix``,
            ``access_delegation_mode`` (VENDED_CREDENTIALS).
        rest_authentication (dict, required): Authentication block. Required key:
            ``type`` (SIGV4, OAUTH, BEARER, NONE). For SIGV4 against AWS, also set
            ``sigv4_iam_role`` (and optionally ``sigv4_signing_region``,
            ``sigv4_external_id``). For OAUTH set ``oauth_token_uri``,
            ``oauth_client_id``, ``oauth_client_secret``, optionally
            ``oauth_allowed_scopes``. For BEARER set ``bearer_token``.
        catalog_namespace (string): The default namespace for tables that reference
            this catalog integration (e.g., a Glue database / S3 Tables namespace).
        enabled (bool): Whether the catalog integration is enabled. Defaults to True.
        refresh_interval_seconds (int): Optional metadata refresh interval.
        table_format (string): The table format. Only ICEBERG is supported.
        owner (string or Role): The owner role. Defaults to ``ACCOUNTADMIN``.
        comment (string): Optional comment.

    Python:

        ```python
        catalog = IcebergRestCatalogIntegration(
            name="ci_s3_tables_dev",
            catalog_namespace="my_namespace",
            rest_config={
                "catalog_uri": "https://glue.us-east-1.amazonaws.com/iceberg",
                "catalog_api_type": "AWS_GLUE",
                "catalog_name": "123456789012:s3tablescatalog/my_table_bucket",
                "access_delegation_mode": "VENDED_CREDENTIALS",
            },
            rest_authentication={
                "type": "SIGV4",
                "sigv4_iam_role": "arn:aws:iam::123456789012:role/snowflake-s3-tables-read",
                "sigv4_signing_region": "us-east-1",
            },
            enabled=True,
        )
        ```

    Yaml:

        ```yaml
        catalog_integrations:
          - name: ci_s3_tables_dev
            catalog_source: ICEBERG_REST
            catalog_namespace: my_namespace
            rest_config:
              catalog_uri: https://glue.us-east-1.amazonaws.com/iceberg
              catalog_api_type: AWS_GLUE
              catalog_name: '123456789012:s3tablescatalog/my_table_bucket'
              access_delegation_mode: VENDED_CREDENTIALS
            rest_authentication:
              type: SIGV4
              sigv4_iam_role: arn:aws:iam::123456789012:role/snowflake-s3-tables-read
              sigv4_signing_region: us-east-1
            enabled: true
        ```
    """

    resource_type = ResourceType.CATALOG_INTEGRATION
    props = Props(
        catalog_source=EnumProp("catalog_source", CatalogSource),
        table_format=EnumProp("table_format", CatalogTableFormat),
        catalog_namespace=StringProp("catalog_namespace"),
        rest_config=PropSet(
            "rest_config",
            Props(
                catalog_uri=StringProp("catalog_uri"),
                catalog_api_type=EnumProp("catalog_api_type", CatalogApiType),
                catalog_name=StringProp("catalog_name"),
                warehouse=StringProp("warehouse"),
                prefix=StringProp("prefix"),
                access_delegation_mode=EnumProp("access_delegation_mode", AccessDelegationMode),
            ),
        ),
        rest_authentication=PropSet(
            "rest_authentication",
            Props(
                type=EnumProp("type", RestAuthenticationType),
                sigv4_iam_role=StringProp("sigv4_iam_role"),
                sigv4_signing_region=StringProp("sigv4_signing_region"),
                sigv4_external_id=StringProp("sigv4_external_id"),
                oauth_token_uri=StringProp("oauth_token_uri"),
                oauth_client_id=StringProp("oauth_client_id"),
                oauth_client_secret=StringProp("oauth_client_secret"),
                oauth_allowed_scopes=StringProp("oauth_allowed_scopes"),
                bearer_token=StringProp("bearer_token"),
            ),
        ),
        enabled=BoolProp("enabled"),
        refresh_interval_seconds=IntProp("refresh_interval_seconds"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _IcebergRestCatalogIntegration

    def __init__(
        self,
        name: str,
        rest_config: dict,
        rest_authentication: dict,
        catalog_namespace: str = None,
        enabled: bool = True,
        refresh_interval_seconds: int = None,
        table_format: CatalogTableFormat = CatalogTableFormat.ICEBERG,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("catalog_source", None)
        super().__init__(name, **kwargs)
        self._data: _IcebergRestCatalogIntegration = _IcebergRestCatalogIntegration(
            name=self._name,
            rest_config=rest_config,
            rest_authentication=rest_authentication,
            catalog_namespace=catalog_namespace,
            enabled=enabled,
            refresh_interval_seconds=refresh_interval_seconds,
            table_format=table_format,
            owner=owner,
            comment=comment,
        )


CatalogIntegrationMap = {
    CatalogSource.GLUE: GlueCatalogIntegration,
    CatalogSource.OBJECT_STORE: ObjectStoreCatalogIntegration,
    CatalogSource.ICEBERG_REST: IcebergRestCatalogIntegration,
}


def _resolver(data: dict):
    return CatalogIntegrationMap[CatalogSource(data["catalog_source"])]


Resource.__resolvers__[ResourceType.CATALOG_INTEGRATION] = _resolver
