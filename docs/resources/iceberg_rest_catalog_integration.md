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


### Minimal end-to-end example: AWS S3 Tables behind Lake Formation

This is the configuration we run in production at Building Controls & Solutions for our Epicor P21 → S3 Tables → Snowflake pipeline. It documents the four AWS-side pieces a Snowflake reader needs *outside* Snowcap, and the two Snowcap resources that bind them to Snowflake.

**The pieces (AWS side, configured once per environment):**

1. **S3 Tables bucket** — e.g., `bcs-iceberg-raw-prd`. Created via `aws s3tables create-table-bucket`.
2. **Lake Formation S3 Tables integration** — enabling it auto-creates a *federated* Glue catalog named `s3tablescatalog/<bucket-name>` under the bucket-owner account. This is what Snowflake's REST catalog will talk to over HTTPS.
3. **Lake Formation grants** — the IAM role below needs at minimum `DESCRIBE` on the federated catalog and `SELECT` (and `DESCRIBE`) on every namespace/table you want Snowflake to read.
4. **Cross-account IAM role** (e.g., `snowflake-s3-tables-read`) — Snowflake's account assumes this via SIGV4. It needs `glue:GetCatalog`, `glue:GetDatabase*`, `glue:GetTable*`, `lakeformation:GetDataAccess`, and `s3tables:Get*`/`s3tables:List*` on the bucket, with a trust policy that lets the Snowflake account assume it (use the `STORAGE_AWS_EXTERNAL_ID` Snowflake gives you after `CREATE STORAGE INTEGRATION`).

**Snowcap side (declarative):**

```yaml
# catalog_integrations: tells Snowflake where Iceberg *metadata* lives.
# CATALOG_NAME is the federated `<account>:s3tablescatalog/<bucket>` form
# that Lake Formation auto-creates — note this DOES NOT work with the
# legacy GlueCatalogIntegration / CATALOG_SOURCE=GLUE path; you must use
# ICEBERG_REST + CATALOG_API_TYPE=AWS_GLUE for S3 Tables.
catalog_integrations:
  - name: ci_p21_iceberg_prd
    catalog_source: ICEBERG_REST
    table_format: ICEBERG
    catalog_namespace: p21
    rest_config:
      catalog_uri: https://glue.us-east-1.amazonaws.com/iceberg
      catalog_api_type: AWS_GLUE
      catalog_name: '123456789012:s3tablescatalog/bcs-iceberg-raw-prd'
      access_delegation_mode: VENDED_CREDENTIALS
    rest_authentication:
      type: SIGV4
      sigv4_iam_role: arn:aws:iam::123456789012:role/snowflake-s3-tables-read
      sigv4_signing_region: us-east-1
    enabled: true
    comment: 'P21 raw Iceberg tables (PRD) - S3 Tables federated catalog via ICEBERG_REST.'

# storage_integrations: tells Snowflake where the Iceberg *data files* live.
# Bucket-level allow so any namespace (p21, spire, future sources) under
# the same bucket is reachable without per-namespace edits. The catalog
# integration's CATALOG_NAMESPACE controls which tables are actually exposed.
storage_integrations:
  - name: si_p21_raw_prd
    storage_provider: S3
    enabled: true
    storage_aws_role_arn: arn:aws:iam::123456789012:role/snowflake-s3-tables-read
    storage_allowed_locations:
      - 's3://bcs-iceberg-raw-prd/'
    storage_aws_object_acl: bucket-owner-full-control
    comment: 'Snowflake read access to PRD raw Iceberg bucket (all namespaces).'
```

**Using it from Snowflake** (post-deploy, in a SQL worksheet — these statements are not managed by Snowcap):

```sql
CREATE OR REPLACE ICEBERG TABLE raw_prd.p21.oe_hdr
  CATALOG          = 'CI_P21_ICEBERG_PRD'   -- catalog_integration name, uppercased
  EXTERNAL_VOLUME  = 'SI_P21_RAW_PRD'        -- storage_integration name, uppercased
  CATALOG_TABLE_NAME = 'oe_hdr';             -- table inside namespace `p21`
```

**Verifying the integration is reachable** before pointing tables at it:

```sql
DESC CATALOG INTEGRATION ci_p21_iceberg_prd;
SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('CI_P21_ICEBERG_PRD');
```

**Gotchas we hit during onboarding:**

* `CATALOG_SOURCE = GLUE` (the legacy `GlueCatalogIntegration` path) rejects the federated `<account>:s3tablescatalog/<bucket>` form for `GLUE_CATALOG_ID` with SQL compilation error 22023/1008. S3 Tables *must* go through `ICEBERG_REST` with `CATALOG_API_TYPE = AWS_GLUE` — that's why this resource exists.
* Lake Formation grants are easy to forget. Without `DESCRIBE` on the federated catalog and `SELECT` on the namespace, `DESC CATALOG INTEGRATION` succeeds but `CREATE ICEBERG TABLE ... FROM CATALOG` fails with a vague 403.
* `access_delegation_mode: VENDED_CREDENTIALS` is required if the writer (e.g., a `pyiceberg-rest` loader on EC2) relies on Lake Formation to vend temporary S3 credentials; without it the catalog returns data-file URIs the SIGV4 role can't read.
* The Glue Iceberg REST endpoint is regional — match `sigv4_signing_region` to the S3 Tables bucket region.


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
