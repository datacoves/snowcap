---
description: >-
  An Iceberg REST catalog integration in Snowflake.
---

# IcebergRestCatalogIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest) | Snowcap CLI label: `iceberg_rest_catalog_integration`

Manages an Apache Iceberg REST catalog integration in Snowflake. This is the right choice for AWS S3 Tables (federated catalogs reachable via the Glue Iceberg REST endpoint) and any other Iceberg REST-compatible catalog.


## Examples

### YAML

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


### Python

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


## Fields

* `name` (string, required) - The name of the catalog integration.
* `rest_config` (dict, required) - Iceberg REST configuration. Required key: `catalog_uri`. Optional keys: `catalog_api_type`, `catalog_name`, `warehouse`, `prefix`, `access_delegation_mode`.
* `rest_authentication` (dict, required) - Authentication block. Required key: `type` (one of `SIGV4`, `OAUTH`, `BEARER`, `NONE`). Auth-specific fields: `sigv4_iam_role`, `sigv4_signing_region`, `sigv4_external_id` (SIGV4); `oauth_client_id`, `oauth_client_secret`, `oauth_token_uri`, `oauth_allowed_scopes` (OAUTH); `bearer_token` (BEARER).
* `catalog_namespace` (string) - Default namespace for tables referencing this catalog.
* `enabled` (bool) - Whether the integration is enabled. Defaults to True.
* `refresh_interval_seconds` (int) - Optional metadata refresh interval.
* `table_format` (string or CatalogTableFormat) - Table format. Only `ICEBERG` is supported. Defaults to `ICEBERG`.
* `owner` (string or [Role](role.md)) - The owner role of the catalog integration. Defaults to "ACCOUNTADMIN".
* `comment` (string) - An optional comment describing the catalog integration.
