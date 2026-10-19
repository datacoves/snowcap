# Snowflake coverage findings

Which Snowflake objects and properties can snowcap manage? This checklist answers that for every object in the Snowflake docs. `report.py` generates it from `inventory.yaml`, so do not edit it by hand. A box is checked when snowcap supports the object, or when every tested level of the property passes. After a fix, commit it, rerun `live_audit.py` for the object and then `report.py`, and commit `inventory.yaml` and this file.

Scope: GA and public preview. Deprecated, private preview, and classes are excluded. Docs read on 2026-10-09. Live results come from a test account with edition ENTERPRISE on AWS US_WEST_2, at snowcap `584e44b`, from 2026-10-16 to 2026-10-17. Each object section names the commit and date of its own run.

## How each property is tested

| Level | Passes when |
|---|---|
| L1 Modeled | snowcap has a field for the property |
| L2 Created | CREATE sets the value in Snowflake |
| L3 Fetched | snowcap reads the value back correctly |
| L4 Updated | a change lands, or snowcap refuses a change that ALTER cannot make |
| L5 Round trip | the plan is empty after the create and after the change |

## Summary

| | Count |
|---|---|
| Objects and variants in scope | 164 |
| Supported by snowcap | 83 |
| Not supported | 81 |
| Properties of supported objects that pass | 279 |
| Properties that fail L2 to L5 | 268 |
| Properties snowcap does not model | 600 |
| Modeled properties without a live sample | 381 |

## Failures by kind

One fix can close several items when they share a cause.

| What happens | Properties | Objects |
|---|---|---|
| snowcap sends change SQL that Snowflake rejects | 60 | [AGGREGATION POLICY](#aggregation-policy) (1), [ALERT](#alert) (3), [API INTEGRATION:aws_api_gateway](#api-integrationaws_api_gateway) (1), [API INTEGRATION:git_https_api](#api-integrationgit_https_api) (1), [API INTEGRATION:google_api_gateway](#api-integrationgoogle_api_gateway) (1), [CATALOG INTEGRATION:glue](#catalog-integrationglue) (3), [DATABASE](#database) (1), [DYNAMIC TABLE](#dynamic-table) (5), [EVENT TABLE](#event-table) (1), [FUNCTION:javascript](#functionjavascript) (5), [GIT REPOSITORY](#git-repository) (1), [ICEBERG TABLE:snowflake](#iceberg-tablesnowflake) (2), [MASKING POLICY](#masking-policy) (4), [MATERIALIZED VIEW](#materialized-view) (2), [NETWORK RULE](#network-rule) (2), [NOTIFICATION INTEGRATION:inbound_azure](#notification-integrationinbound_azure) (1), [NOTIFICATION INTEGRATION:inbound_gcp](#notification-integrationinbound_gcp) (1), [PIPE](#pipe) (2), [PROCEDURE:python](#procedurepython) (5), [RESOURCE MONITOR](#resource-monitor) (2), [ROW ACCESS POLICY](#row-access-policy) (3), [SECURITY INTEGRATION:api_auth_client_credentials](#security-integrationapi_auth_client_credentials) (1), [SEQUENCE](#sequence) (1), [STAGE:external_azure](#stageexternal_azure) (1), [STAGE:external_gcs](#stageexternal_gcs) (1), [STAGE:external_onelake](#stageexternal_onelake) (1), [STAGE:external_s3](#stageexternal_s3) (1), [STAGE:internal](#stageinternal) (1), [TABLE](#table) (3), [TASK](#task) (1), [VIEW](#view) (2) |
| CREATE leaves out the value or fails | 73 | [CATALOG INTEGRATION:object_storage](#catalog-integrationobject_storage) (1), [DATABASE ROLE](#database-role) (1), [EVENT TABLE](#event-table) (1), [EXTERNAL FUNCTION](#external-function) (15), [FUNCTION:javascript](#functionjavascript) (1), [FUNCTION:python](#functionpython) (2), [HYBRID TABLE](#hybrid-table) (18), [ICEBERG TABLE:snowflake](#iceberg-tablesnowflake) (4), [MATERIALIZED VIEW](#materialized-view) (3), [PROCEDURE:python](#procedurepython) (2), [SECURITY INTEGRATION:api_auth_jwt_bearer](#security-integrationapi_auth_jwt_bearer) (8), [STAGE:external_s3compat](#stageexternal_s3compat) (6), [STREAMLIT](#streamlit) (5), [TABLE](#table) (4), [TASK](#task) (1), [VIEW](#view) (1) |
| snowcap does not read the value back | 59 | [API INTEGRATION:aws_api_gateway](#api-integrationaws_api_gateway) (1), [API INTEGRATION:google_api_gateway](#api-integrationgoogle_api_gateway) (1), [CATALOG INTEGRATION:iceberg_rest](#catalog-integrationiceberg_rest) (1), [EVENT TABLE](#event-table) (2), [EXTERNAL VOLUME](#external-volume) (3), [FUNCTION:javascript](#functionjavascript) (1), [ICEBERG TABLE:snowflake](#iceberg-tablesnowflake) (2), [NOTEBOOK](#notebook) (1), [NOTIFICATION INTEGRATION:inbound_azure](#notification-integrationinbound_azure) (1), [ROLE](#role) (1), [SECRET:generic_string](#secretgeneric_string) (1), [SECRET:oauth2_authorization_code](#secretoauth2_authorization_code) (1), [SECRET:password](#secretpassword) (1), [SECURITY INTEGRATION:api_auth_authorization_code](#security-integrationapi_auth_authorization_code) (1), [SECURITY INTEGRATION:api_auth_client_credentials](#security-integrationapi_auth_client_credentials) (1), [SECURITY INTEGRATION:snowflake_oauth_custom](#security-integrationsnowflake_oauth_custom) (2), [STAGE:external_azure](#stageexternal_azure) (3), [STAGE:external_gcs](#stageexternal_gcs) (3), [STAGE:external_onelake](#stageexternal_onelake) (3), [STAGE:external_s3](#stageexternal_s3) (8), [STAGE:internal](#stageinternal) (1), [STORAGE INTEGRATION:azure](#storage-integrationazure) (1), [STREAM:table](#streamtable) (2), [STREAM:view](#streamview) (2), [TABLE](#table) (8), [USER](#user) (4), [VIEW](#view) (2), [WAREHOUSE](#warehouse) (1) |
| A change to the value plans nothing | 21 | [CATALOG INTEGRATION:iceberg_rest](#catalog-integrationiceberg_rest) (8), [ICEBERG TABLE:snowflake](#iceberg-tablesnowflake) (5), [MATERIALIZED VIEW](#materialized-view) (1), [TABLE](#table) (5), [VIEW](#view) (2) |
| snowcap crashes while it plans the change | 2 | [SECRET:oauth2_authorization_code](#secretoauth2_authorization_code) (2) |
| snowcap crashes while it reads the object | 30 | [AUTHENTICATION POLICY](#authentication-policy) (8), [FUNCTION:python](#functionpython) (12), [SECRET:oauth2_client_credentials](#secretoauth2_client_credentials) (3), [SECURITY INTEGRATION:api_auth_client_credentials](#security-integrationapi_auth_client_credentials) (1), [SECURITY INTEGRATION:snowflake_oauth_partner](#security-integrationsnowflake_oauth_partner) (6) |
| The change runs but does not land | 6 | [EXTERNAL VOLUME](#external-volume) (6) |
| Plan is not empty after create or change | 17 | [EXTERNAL VOLUME](#external-volume) (2), [FUNCTION:javascript](#functionjavascript) (1), [NOTIFICATION INTEGRATION:inbound_azure](#notification-integrationinbound_azure) (2), [USER](#user) (12) |

## Objects

- [x] [ACCOUNT](#account)
- [ ] AGENT: not supported, 22 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-agent))
- [x] [AGGREGATION POLICY](#aggregation-policy)
- [x] [ALERT](#alert)
- [x] [API INTEGRATION:aws_api_gateway](#api-integrationaws_api_gateway)
- [x] [API INTEGRATION:azure_api_management](#api-integrationazure_api_management)
- [ ] API INTEGRATION:external_mcp: not supported, 20 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration))
- [x] [API INTEGRATION:git_https_api](#api-integrationgit_https_api)
- [x] [API INTEGRATION:google_api_gateway](#api-integrationgoogle_api_gateway)
- [ ] APPLICATION: not supported, 20 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-application))
- [ ] APPLICATION PACKAGE: not supported, 21 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-application-package))
- [x] [AUTHENTICATION POLICY](#authentication-policy)
- [ ] BACKUP POLICY: not supported, 10 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-backup-policy))
- [ ] BACKUP SET: not supported, 15 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-backup-set))
- [ ] CATALOG INTEGRATION:delta_sharing: not supported, 19 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-delta-sharing))
- [x] [CATALOG INTEGRATION:glue](#catalog-integrationglue)
- [x] [CATALOG INTEGRATION:iceberg_rest](#catalog-integrationiceberg_rest)
- [x] [CATALOG INTEGRATION:object_storage](#catalog-integrationobject_storage)
- [ ] CATALOG INTEGRATION:open_catalog: not supported, 19 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-open-catalog))
- [ ] CODE BUNDLE: not supported, 7 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-code-bundle))
- [x] [COMPUTE POOL](#compute-pool)
- [ ] CONNECTION: not supported, 10 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-connection))
- [ ] CONTACT: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-contact))
- [ ] CORTEX SEARCH SERVICE:multi_index: not supported, 24 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-cortex-search))
- [ ] CORTEX SEARCH SERVICE:single_index: not supported, 22 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-cortex-search))
- [ ] DATA METRIC FUNCTION: not supported, 15 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-data-metric-function))
- [ ] DATA MOVEMENT POLICY: not supported, 12 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-data-movement-policy))
- [ ] DATA MOVEMENT RULE: not supported, 8 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-data-movement-rule))
- [x] [DATABASE](#database)
- [x] [DATABASE ROLE](#database-role)
- [ ] DATABASE:catalog_linked: not supported, 20 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-database-catalog-linked))
- [x] [DATABASE:from_share](#databasefrom_share)
- [ ] DATASET: not supported, 10 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dataset))
- [ ] DBT PROJECT: not supported, 13 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dbt-project))
- [ ] DCM PROJECT: not supported, 8 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dcm-project))
- [x] [DYNAMIC TABLE](#dynamic-table)
- [ ] DYNAMIC TABLE:iceberg: not supported, 54 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#label-create-dt-iceberg-syntax))
- [x] [EVENT TABLE](#event-table)
- [ ] EXPERIMENT: not supported, 7 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-experiment))
- [x] [EXTERNAL ACCESS INTEGRATION](#external-access-integration)
- [ ] EXTERNAL AGENT: not supported, 8 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-agent))
- [x] [EXTERNAL FUNCTION](#external-function)
- [ ] EXTERNAL TABLE: not supported, 48 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-table))
- [x] [EXTERNAL VOLUME](#external-volume)
- [x] [FAILOVER GROUP](#failover-group)
- [ ] FEATURE POLICY: not supported, 14 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-feature-policy))
- [ ] FILE FORMAT:avro: not supported, 12 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
- [x] [FILE FORMAT:csv](#file-formatcsv)
- [x] [FILE FORMAT:json](#file-formatjson)
- [ ] FILE FORMAT:orc: not supported, 11 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
- [x] [FILE FORMAT:parquet](#file-formatparquet)
- [ ] FILE FORMAT:xml: not supported, 15 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
- [ ] FUNCTION:java: not supported, 31 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function))
- [x] [FUNCTION:javascript](#functionjavascript)
- [x] [FUNCTION:python](#functionpython)
- [ ] FUNCTION:scala (preview): not supported, 30 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function))
- [ ] FUNCTION:sql: not supported, 24 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function))
- [ ] GATEWAY: not supported, 13 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-gateway))
- [x] [GIT REPOSITORY](#git-repository)
- [x] [HYBRID TABLE](#hybrid-table)
- [ ] ICEBERG TABLE:aws_glue: not supported, 24 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-aws-glue))
- [ ] ICEBERG TABLE:catalog_linked: not supported, 32 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest#create-iceberg-table-catalog-linked-database))
- [ ] ICEBERG TABLE:delta: not supported, 23 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-delta))
- [ ] ICEBERG TABLE:iceberg_files: not supported, 20 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-iceberg-files))
- [ ] ICEBERG TABLE:rest: not supported, 30 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-rest))
- [x] [ICEBERG TABLE:snowflake](#iceberg-tablesnowflake)
- [x] [IMAGE REPOSITORY](#image-repository)
- [ ] INTERACTIVE TABLE: not supported, 25 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table))
- [ ] INTERACTIVE WAREHOUSE: not supported, 20 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-interactive-warehouse))
- [ ] JOIN POLICY (preview): not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-join-policy))
- [ ] LISTING: not supported, 32 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-listing))
- [ ] MAINTENANCE POLICY: not supported, 7 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-maintenance-policy))
- [x] [MASKING POLICY](#masking-policy)
- [x] [MATERIALIZED VIEW](#materialized-view)
- [x] [MCP SERVER](#mcp-server)
- [ ] MODEL MONITOR:gateway: not supported, 20 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-model-monitor))
- [ ] MODEL MONITOR:model_version: not supported, 23 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-model-monitor))
- [ ] MODEL:from_model: not supported, 14 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-model))
- [ ] MODEL:from_stage: not supported, 15 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-model))
- [x] [NETWORK POLICY](#network-policy)
- [x] [NETWORK RULE](#network-rule)
- [x] [NOTEBOOK](#notebook)
- [x] [NOTIFICATION INTEGRATION:email](#notification-integrationemail)
- [x] [NOTIFICATION INTEGRATION:inbound_azure](#notification-integrationinbound_azure)
- [x] [NOTIFICATION INTEGRATION:inbound_gcp](#notification-integrationinbound_gcp)
- [ ] NOTIFICATION INTEGRATION:multi_queue: not supported, 16 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-multi-queue))
- [x] [NOTIFICATION INTEGRATION:outbound_aws](#notification-integrationoutbound_aws)
- [x] [NOTIFICATION INTEGRATION:outbound_azure](#notification-integrationoutbound_azure)
- [x] [NOTIFICATION INTEGRATION:outbound_gcp](#notification-integrationoutbound_gcp)
- [ ] NOTIFICATION INTEGRATION:webhook: not supported, 11 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-webhooks))
- [ ] ONLINE FEATURE TABLE: not supported, 15 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-online-feature-table))
- [ ] ORGANIZATION LISTING: not supported, 26 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-organization-listing))
- [ ] ORGANIZATION PROFILE: not supported, 13 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-organization-profile))
- [x] [PACKAGES POLICY](#packages-policy)
- [x] [PASSWORD POLICY](#password-policy)
- [x] [PIPE](#pipe)
- [ ] POSTGRES INSTANCE: not supported, 19 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-postgres-instance))
- [ ] PRIVACY POLICY: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-privacy-policy))
- [ ] PROCEDURE:java: not supported, 25 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure#java-handler))
- [ ] PROCEDURE:javascript: not supported, 18 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
- [x] [PROCEDURE:python](#procedurepython)
- [ ] PROCEDURE:scala: not supported, 24 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure#scala-handler))
- [ ] PROCEDURE:sql: not supported, 17 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
- [ ] PROJECTION POLICY: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-projection-policy))
- [ ] PROVISIONED THROUGHPUT: not supported, 10 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-provisioned-throughput))
- [x] [REPLICATION GROUP](#replication-group)
- [x] [RESOURCE MONITOR](#resource-monitor)
- [ ] RESTRICTED SESSION SCOPE: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-restricted-session-scope))
- [x] [ROLE](#role)
- [x] [ROW ACCESS POLICY](#row-access-policy)
- [x] [SCHEMA](#schema)
- [ ] SECRET:cloud_provider_token: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-secret))
- [x] [SECRET:generic_string](#secretgeneric_string)
- [x] [SECRET:oauth2_authorization_code](#secretoauth2_authorization_code)
- [x] [SECRET:oauth2_client_credentials](#secretoauth2_client_credentials)
- [x] [SECRET:password](#secretpassword)
- [ ] SECRET:symmetric_key: not supported, 7 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-secret))
- [ ] SECRET:workload_identity_federation: not supported, 7 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-secret))
- [x] [SECURITY INTEGRATION:api_auth_authorization_code](#security-integrationapi_auth_authorization_code)
- [x] [SECURITY INTEGRATION:api_auth_client_credentials](#security-integrationapi_auth_client_credentials)
- [x] [SECURITY INTEGRATION:api_auth_jwt_bearer](#security-integrationapi_auth_jwt_bearer)
- [ ] SECURITY INTEGRATION:aws_iam: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-aws-iam))
- [ ] SECURITY INTEGRATION:external_oauth: not supported, 22 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-external))
- [ ] SECURITY INTEGRATION:external_secret_provider: not supported, 17 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth-external-secret-provider))
- [ ] SECURITY INTEGRATION:oidc_custom: not supported, 22 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oidc))
- [ ] SECURITY INTEGRATION:oidc_google: not supported, 12 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oidc))
- [ ] SECURITY INTEGRATION:oidc_microsoft: not supported, 12 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oidc))
- [ ] SECURITY INTEGRATION:saml2: not supported, 25 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-saml2))
- [ ] SECURITY INTEGRATION:scim: not supported, 12 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-scim))
- [x] [SECURITY INTEGRATION:snowflake_oauth_custom](#security-integrationsnowflake_oauth_custom)
- [x] [SECURITY INTEGRATION:snowflake_oauth_partner](#security-integrationsnowflake_oauth_partner)
- [ ] SEMANTIC VIEW: not supported, 46 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view))
- [x] [SEQUENCE](#sequence)
- [x] [SERVICE](#service)
- [ ] SESSION POLICY: not supported, 14 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-session-policy))
- [x] [SHARE](#share)
- [ ] SNAPSHOT: not supported, 10 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-snapshot))
- [x] [STAGE:external_azure](#stageexternal_azure)
- [x] [STAGE:external_gcs](#stageexternal_gcs)
- [x] [STAGE:external_onelake](#stageexternal_onelake)
- [x] [STAGE:external_s3](#stageexternal_s3)
- [x] [STAGE:external_s3compat](#stageexternal_s3compat)
- [x] [STAGE:internal](#stageinternal)
- [x] [STORAGE INTEGRATION:azure](#storage-integrationazure)
- [x] [STORAGE INTEGRATION:gcs](#storage-integrationgcs)
- [ ] STORAGE INTEGRATION:multi_location: not supported, 18 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration))
- [ ] STORAGE INTEGRATION:postgres_external (preview): not supported, 13 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration))
- [ ] STORAGE INTEGRATION:postgres_internal (preview): not supported, 8 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration-postgres-internal))
- [x] [STORAGE INTEGRATION:s3](#storage-integrations3)
- [ ] STORAGE LIFECYCLE POLICY: not supported, 12 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-lifecycle-policy))
- [x] [STREAM:directory_table](#streamdirectory_table)
- [x] [STREAM:dynamic_table](#streamdynamic_table)
- [ ] STREAM:event_table: not supported, 9 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream))
- [ ] STREAM:external_table: not supported, 11 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream))
- [x] [STREAM:table](#streamtable)
- [x] [STREAM:view](#streamview)
- [x] [STREAMLIT](#streamlit)
- [x] [TABLE](#table)
- [x] [TAG](#tag)
- [x] [TASK](#task)
- [ ] TYPE: not supported, 7 properties ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-type))
- [x] [USER](#user)
- [x] [VIEW](#view)
- [x] [WAREHOUSE](#warehouse)

## Supported objects

### ACCOUNT

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 5 pass, 0 fail, 38 not modeled, 153 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-account) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-account) [show](https://docs.snowflake.com/en/sql-reference/sql/show-accounts) [describe](None)

- Combined change: untested (needs two or more fields)
- [ ] Model `drop_old_url`: `DROP OLD URL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `drop_old_organization_url` (preview): `DROP OLD ORGANIZATION URL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `edition`: `EDITION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `is_org_admin`: `IS_ORG_ADMIN` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `tenant_type`: `TENANT_TYPE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `domain_names[]`: `DOMAIN_NAMES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `admin_name`: `ADMIN_NAME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `admin_password`: `ADMIN_PASSWORD` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `admin_rsa_public_key`: `ADMIN_RSA_PUBLIC_KEY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `admin_user_type`: `ADMIN_USER_TYPE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `first_name`: `FIRST_NAME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `last_name`: `LAST_NAME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `email`: `EMAIL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `must_change_password`: `MUST_CHANGE_PASSWORD` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `region_group`: `REGION_GROUP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `region`: `REGION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `polaris`: `POLARIS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-account))
- [ ] Model `login_idp_redirect`: `LOGIN_IDP_REDIRECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `object_visibility` (preview): `OBJECT_VISIBILITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `resource_monitor`: `RESOURCE_MONITOR` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `organization_user_groups[]`: `ORGANIZATION USER GROUP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `authentication_policy`: `AUTHENTICATION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `authentication_policy_person_users`: `AUTHENTICATION POLICY ... FOR ALL PERSON USERS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `authentication_policy_service_users`: `AUTHENTICATION POLICY ... FOR ALL SERVICE USERS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `session_policy`: `SESSION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `session_policy_person_users`: `SESSION POLICY ... FOR ALL PERSON USERS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `session_policy_service_users`: `SESSION POLICY ... FOR ALL SERVICE USERS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `feature_policy_applications`: `FEATURE POLICY ... FOR ALL APPLICATIONS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `feature_policy_databases`: `FEATURE POLICY ... FOR ALL DATABASES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `feature_policy_personal_databases`: `FEATURE POLICY ... FOR ALL PERSONAL DATABASES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `maintenance_policy_applications`: `MAINTENANCE POLICY ... FOR ALL APPLICATIONS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `data_movement_policy`: `DATA MOVEMENT POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `packages_policy`: `PACKAGES POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `password_policy`: `PASSWORD POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `multi_party_approval_policy`: `MULTI PARTY APPROVAL POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `contacts`: `CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [x] `statement_timeout_in_seconds`
- [x] `client_timestamp_type_mapping`
- [x] `geography_output_format`
- [x] `json_indent`
- [x] `odbc_treat_decimal_as_int`

Modeled, without a live sample: `network_policy`, `account_level_file_extensions_allow_list_for_private_workspaces`, `account_level_file_extensions_allow_list_for_shared_workspaces`, `allow_bind_values_access`, `allow_client_mfa_caching`, `allow_id_token`, `allowed_spcs_workload_types`, `client_encryption_key_size`, `coco_cloud_agents_non_snowflake_egress_disabled`, `coco_snowsight_allow_all_permission_options_disabled`, `cortex_enabled_cross_region`, `cortex_models_allowlist`, `default_dbt_version`, `default_streamlit_compute_pool`, `disable_user_privilege_grants`, `disallowed_spcs_workload_types`, `enable_automatic_sensitive_data_classification_log`, `enable_budget_event_logging`, `enable_dual_stack_load_balancer`, `enable_egress_cost_optimizer`, `enable_identifier_first_login`, `enable_internal_stages_privatelink`, `enable_snowflake_managed_storage_volume_privatelink`, `enable_per_account_app_service_url`, `enable_per_account_app_service_privatelink_url`, `enable_spcs_block_storage_snowflake_full_encryption_enforcement`, `enable_tag_propagation_event_logging`, `enable_tri_secret_and_rekey_opt_out_for_image_repository`, `enable_workspace_file_content_search`, `enforce_network_rules_for_internal_stages`, `enforce_network_rules_for_snowflake_managed_storage_volume`, `enforce_tag_propagation_for_data_movement_policies`, `external_oauth_add_privileged_roles_to_blocked_list`, `initial_replication_size_limit_in_tb`, `listing_auto_fulfillment_initial_refresh_size_limit_in_tb`, `listing_auto_fulfillment_replication_refresh_schedule`, `llm_inference_parse_document_presigned_url_expiry_seconds`, `map_scim_username_to_enterprise_attr`, `min_data_retention_time_in_days`, `oauth_add_privileged_roles_to_blocked_list`, `periodic_data_rekeying`, `read_consistency_mode`, `require_storage_integration_for_stage_creation`, `require_storage_integration_for_stage_operation`, `sql_trace_query_text`, `use_workspaces_for_sql`, `base_location_prefix`, `catalog`, `catalog_sync`, `data_retention_time_in_days`, `default_ddl_collation`, `default_metadata_write_format`, `default_notebook_compute_pool_cpu`, `default_notebook_compute_pool_gpu`, `default_python_artifact_repository`, `default_streamlit_notebook_warehouse`, `enable_data_compaction`, `event_table`, `external_volume`, `iceberg_default_ddl_collation`, `iceberg_merge_on_read_behavior`, `iceberg_version_default`, `max_concurrency_level`, `max_data_extension_time_in_days`, `oauth_authorization_server`, `oauth_scopes_supported`, `pipe_execution_paused`, `replace_invalid_characters`, `serverless_task_max_statement_size`, `serverless_task_min_statement_size`, `service_caller_token_validity_secs`, `storage_serialization_policy`, `suspend_alert_after_num_failures`, `suspend_task_after_num_failures`, `task_auto_retry_attempts`, `user_task_managed_initial_warehouse_size`, `user_task_minimum_trigger_interval_in_seconds`, `user_task_timeout_ms`, `cortex_code_cli_daily_est_credit_limit_per_user`, `cortex_code_desktop_daily_est_credit_limit_per_user`, `cortex_code_snowsight_daily_est_credit_limit_per_user`, `disable_ui_download_button`, `enable_notebook_creation_in_personal_db`, `enable_unredacted_query_syntax_error`, `enable_unredacted_secure_object_error`, `prevent_unload_to_inline_url`, `prevent_unload_to_internal_stages`, `log_level`, `log_event_level`, `metric_level`, `trace_level`, `quoted_identifiers_ignore_case`, `statement_queued_timeout_in_seconds`, `abort_detached_query`, `active_python_profiler`, `autocommit`, `binary_input_format`, `binary_output_format`, `client_enable_log_info_statement_parameters`, `client_memory_limit`, `client_metadata_request_use_connection_ctx`, `client_metadata_use_session_database`, `client_prefetch_threads`, `client_result_chunk_size`, `client_result_column_case_insensitive`, `client_session_keep_alive`, `client_session_keep_alive_heartbeat_frequency`, `csv_timestamp_format`, `date_input_format`, `date_output_format`, `default_null_ordering`, `enable_get_ddl_use_data_type_alias`, `enable_unhandled_exceptions_reporting`, `enable_unload_physical_type_optimization`, `error_on_nondeterministic_merge`, `error_on_nondeterministic_update`, `geometry_output_format`, `hybrid_table_lock_timeout`, `iceberg_timestamp_default_precision`, `jdbc_enable_put_get`, `jdbc_treat_decimal_as_int`, `jdbc_treat_timestamp_ntz_as_utc`, `jdbc_use_session_timezone`, `js_treat_integer_as_bigint`, `lock_timeout`, `multi_statement_count`, `noorder_sequence_as_default`, `opt_out_error_logging`, `python_profiler_target_stage`, `python_profiler_modules`, `query_tag`, `rows_per_resultset`, `s3_stage_vpce_dns_name`, `search_path`, `simulated_data_sharing_consumer`, `strict_json_output`, `timestamp_day_is_always_24h`, `timestamp_input_format`, `timestamp_ltz_output_format`, `timestamp_ntz_output_format`, `timestamp_output_format`, `timestamp_type_mapping`, `timestamp_tz_output_format`, `timezone`, `time_input_format`, `time_output_format`, `transaction_abort_on_error`, `transaction_default_isolation_level`, `two_digit_century_start`, `unsupported_ddl_action`, `use_cached_result`, `week_of_year_policy`, `week_start`

### AGGREGATION POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 1 fail, 2 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-aggregation-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-aggregation-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-aggregation-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-aggregation-policy)

- [ ] `body` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 82 unexpected '->'. on: ALTER AGGREGATION POLICY <db>.PUBLIC.R00_BODY SET -> AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 10)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-aggregation-policy#parameters))
  - snowcap sent: `ALTER AGGREGATION POLICY <db>.PUBLIC.R00_BODY SET -> AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 10)`
- Combined change: untested (needs two or more fields)
- [ ] Model `comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-aggregation-policy#parameters))
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-aggregation-policy#parameters))

Modeled, without a live sample: `owner`

### ALERT

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 3 fail, 5 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-alert) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-alert) [show](https://docs.snowflake.com/en/sql-reference/sql/show-alerts) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-alert)

- [ ] `condition` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 63 unexpected '('. on: ALTER ALERT <db>.PUBLIC.R03_CONDITION SET IF(EXISTS( SELECT 2 ))` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-alert))
  - snowcap sent: `ALTER ALERT <db>.PUBLIC.R03_CONDITION SET IF(EXISTS( SELECT 2 ))`
- [ ] `action` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 58 unexpected 'then'. on: ALTER ALERT <db>.PUBLIC.R04_ACTION SET then SELECT 2` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-alert))
  - snowcap sent: `ALTER ALERT <db>.PUBLIC.R04_ACTION SET then SELECT 2`
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R05_TAGS?domain=ALERT SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-alert))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R05_TAGS?domain=ALERT SET`
- [x] Combined change of `comment`, `schedule`, `warehouse` in one plan
- [ ] Model `config`: `CONFIG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-alert))
- [ ] Model `runbook`: `RUNBOOK` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-alert))
- [ ] Model `suspend_alert_after_num_failures`: `SUSPEND_ALERT_AFTER_NUM_FAILURES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-suspend-alert-after-num-failures))
- [ ] Model `from_template`: `FROM TEMPLATE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-alert#create-alert-from-template))
- [ ] Model `template_params`: `TEMPLATE_PARAMS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-alert))
- [x] `warehouse`
- [x] `schedule`
- [x] `comment`

Modeled, without a live sample: `state`, `owner`

### API INTEGRATION:aws_api_gateway

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 5 pass, 2 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `api_provider` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'API_PROVIDER' for 'INTEGRATION - EXTERNAL_API' on: ALTER API INTEGRATION <db>_R00_API_PROVIDER SET api_provider = AWS_PRIVATE_API_GATEWAY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-amazon-api-gateway))
  - snowcap sent: `ALTER API INTEGRATION <db>_R00_API_PROVIDER SET api_provider = AWS_PRIVATE_API_GATEWAY`
- [ ] `api_key` fails L3, L4, L5: `declared '{secret:api_key_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-all-integrations))
- [ ] Combined change of `api_allowed_prefixes`, `api_aws_role_arn`, `api_blocked_prefixes`, `api_key`, `comment`, `enabled` in one plan: `missed api_key`
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration#parameters))
- [x] `api_aws_role_arn`
- [x] `api_allowed_prefixes`
- [x] `api_blocked_prefixes`
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `owner`

### API INTEGRATION:azure_api_management

Docs only: CREATE validates the allowed prefix against a live Azure API Management host, so a placeholder fails: "URL 'https\://coverage.azure-api.net/a' is invalid. Make sure the URL host is reachable and that the URL is for Azure API Management service.". Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration#parameters))

Modeled, without a live sample: `api_provider`, `azure_tenant_id`, `azure_ad_application_id`, `api_key`, `api_allowed_prefixes`, `api_blocked_prefixes`, `enabled`, `comment`, `owner`

### API INTEGRATION:git_https_api

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 4 pass, 1 fail, 13 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `api_provider` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'API_PROVIDER' for 'INTEGRATION - EXTERNAL_API' on: ALTER API INTEGRATION <db>_R00_API_PROVIDER SET api_provider = AWS_API_GATEWAY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
  - snowcap sent: `ALTER API INTEGRATION <db>_R00_API_PROVIDER SET api_provider = AWS_API_GATEWAY`
- [x] Combined change of `api_allowed_prefixes`, `api_blocked_prefixes`, `comment`, `enabled` in one plan
- [ ] Model `allowed_authentication_secrets`: `ALLOWED_AUTHENTICATION_SECRETS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.type`: `API_USER_AUTHENTICATION = ( TYPE = ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_authorization_endpoint`: `OAUTH_AUTHORIZATION_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_token_endpoint`: `OAUTH_TOKEN_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_client_id`: `OAUTH_CLIENT_ID` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_client_secret`: `OAUTH_CLIENT_SECRET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_access_token_validity`: `OAUTH_ACCESS_TOKEN_VALIDITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_refresh_token_validity`: `OAUTH_REFRESH_TOKEN_VALIDITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_allowed_scopes[]`: `OAUTH_ALLOWED_SCOPES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `api_user_authentication.oauth_username`: `OAUTH_USERNAME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `tls_trusted_certificates[]`: `TLS_TRUSTED_CERTIFICATES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-git-repository))
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration#parameters))
- [x] `api_allowed_prefixes`
- [x] `api_blocked_prefixes`
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `api_key`, `owner`

### API INTEGRATION:google_api_gateway

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 5 pass, 2 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `api_provider` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'API_PROVIDER' for 'INTEGRATION - EXTERNAL_API' on: ALTER API INTEGRATION <db>_R00_API_PROVIDER SET api_provider = AWS_API_GATEWAY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-google-cloud-api-gateway))
  - snowcap sent: `ALTER API INTEGRATION <db>_R00_API_PROVIDER SET api_provider = AWS_API_GATEWAY`
- [ ] `api_key` fails L3, L4, L5: `declared '{secret:api_key_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-api-integration#for-all-integrations))
- [ ] Combined change of `api_allowed_prefixes`, `api_blocked_prefixes`, `api_key`, `comment`, `enabled` in one plan: `missed api_key`
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-api-integration#parameters))
- [x] `google_audience`
- [x] `api_allowed_prefixes`
- [x] `api_blocked_prefixes`
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `owner`

### AUTHENTICATION POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 8 fail, 9 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-authentication-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-authentication-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-authentication-policy)

- [ ] `authentication_methods[]` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `client_types[]` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `mfa_enrollment` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `pat_policy.default_expiry_in_days` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `pat_policy.max_expiry_in_days` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `pat_policy.network_policy_evaluation` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `pat_policy.require_role_restriction_for_service_users` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] `comment` fails L3, L4, L5: `snowcap fetch crashes: KeyError: 'mfa_authentication_methods'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-authentication-policy#parameters))
- [ ] Combined change of `authentication_methods`, `client_types`, `comment`, `mfa_enrollment` in one plan: `Crashed: KeyError: 'mfa_authentication_methods'`
- [ ] Model `client_policy[].client_type`: `CLIENT_POLICY = ( <client_type> = ( ... ) )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `client_policy[].minimum_version`: `MINIMUM_VERSION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `mfa_policy.allowed_methods[]`: `MFA_POLICY = ( ALLOWED_METHODS = ( ... ) )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `mfa_policy.enforce_mfa_on_external_authentication`: `MFA_POLICY = ( ENFORCE_MFA_ON_EXTERNAL_AUTHENTICATION = ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `workload_identity_policy.allowed_providers[]`: `WORKLOAD_IDENTITY_POLICY = ( ALLOWED_PROVIDERS = ( ... ) )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `workload_identity_policy.allowed_aws_accounts[]`: `WORKLOAD_IDENTITY_POLICY = ( ALLOWED_AWS_ACCOUNTS = ( ... ) )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `workload_identity_policy.allowed_azure_issuers[]`: `WORKLOAD_IDENTITY_POLICY = ( ALLOWED_AZURE_ISSUERS = ( ... ) )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `workload_identity_policy.allowed_oidc_issuers[]`: `WORKLOAD_IDENTITY_POLICY = ( ALLOWED_OIDC_ISSUERS = ( ... ) )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-authentication-policy#optional-parameters))
- [ ] Model `dcm_project`: `UNSET DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-authentication-policy#parameters))

Modeled, without a live sample: `security_integrations[]`, `owner`

### CATALOG INTEGRATION:glue

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 3 fail, 1 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-catalog-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-catalog-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-catalog-integration)

- [ ] `glue_aws_role_arn` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'GLUE_AWS_ROLE_ARN' for 'INTEGRATION - CATALOG' on: ALTER CATALOG INTEGRATION <db>_R00_GLUE_AWS_ROLE_ARN SET GLUE_AWS_ROLE_ARN = $$arn:aws:iam::1234567890 ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue#required-parameters))
  - snowcap sent: `ALTER CATALOG INTEGRATION <db>_R00_GLUE_AWS_ROLE_ARN SET GLUE_AWS_ROLE_ARN = $$arn:aws:iam::123456789012:role/coverage2$$`
- [ ] `glue_catalog_id` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'GLUE_CATALOG_ID' for 'INTEGRATION - CATALOG' on: ALTER CATALOG INTEGRATION <db>_R01_GLUE_CATALOG_ID SET GLUE_CATALOG_ID = $$210987654321$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue#required-parameters))
  - snowcap sent: `ALTER CATALOG INTEGRATION <db>_R01_GLUE_CATALOG_ID SET GLUE_CATALOG_ID = $$210987654321$$`
- [ ] `glue_region` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'GLUE_REGION' for 'INTEGRATION - CATALOG' on: ALTER CATALOG INTEGRATION <db>_R02_GLUE_REGION SET GLUE_REGION = $$us-east-1$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-glue#optional-parameters))
  - snowcap sent: `ALTER CATALOG INTEGRATION <db>_R02_GLUE_REGION SET GLUE_REGION = $$us-east-1$$`
- Combined change: untested (needs two or more fields)
- [ ] Model `refresh_interval_seconds`: `REFRESH_INTERVAL_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-catalog-integration#parameters))
- [x] `catalog_namespace`
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `catalog_source`, `table_format`, `owner`

### CATALOG INTEGRATION:iceberg_rest

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 4 pass, 9 fail, 1 not modeled, 8 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-catalog-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-catalog-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-catalog-integration)

- [ ] `rest_config.catalog_uri` fails L4, L5: `a change to {'catalog_uri': 'https://catalog2.example.com/api/catalog', 'catalog_api_type': 'AWS_API_GATEWAY', 'catalog_name': 'coverage'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-configuration-parameters-restconfigparams))
- [ ] `rest_config.prefix` fails L4, L5: `a change to {'catalog_uri': 'https://catalog.example.com/api/catalog', 'catalog_api_type': 'AWS_API_GATEWAY', 'catalog_name': 'coverage', 'prefix': 'p2'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-configuration-parameters-restconfigparams))
- [ ] `rest_config.catalog_name` fails L4, L5: `a change to {'catalog_uri': 'https://catalog.example.com/api/catalog', 'catalog_api_type': 'AWS_API_GATEWAY', 'catalog_name': 'coverage2'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-configuration-parameters-restconfigparams))
- [ ] `rest_config.catalog_api_type` fails L4, L5: `a change to {'catalog_uri': 'https://catalog.example.com/api/catalog', 'catalog_api_type': 'AWS_GLUE', 'catalog_name': 'coverage'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-configuration-parameters-restconfigparams))
- [ ] `rest_config.access_delegation_mode` fails L4, L5: `a change to {'catalog_uri': 'https://catalog.example.com/api/catalog', 'catalog_api_type': 'AWS_API_GATEWAY', 'catalog_name': 'coverage', 'access_delegation_mode': 'EXTERNAL_VOLUME_CREDENTIALS'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-configuration-parameters-restconfigparams))
- [ ] `rest_authentication.type` fails L4, L5: `a change to {'type': 'BEARER', 'bearer_token': '{secret:bearer_token_1}'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-authentication-parameters-restauthenticationparams))
- [ ] `rest_authentication.sigv4_iam_role` fails L4, L5: `a change to {'type': 'SIGV4', 'sigv4_iam_role': 'arn:aws:iam::123456789012:role/coverage2'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-authentication-parameters-restauthenticationparams))
- [ ] `rest_authentication.sigv4_signing_region` fails L4, L5: `a change to {'type': 'SIGV4', 'sigv4_iam_role': 'arn:aws:iam::123456789012:role/coverage', 'sigv4_signing_region': 'us-east-1'} plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-authentication-parameters-restauthenticationparams))
- [ ] `rest_authentication.sigv4_external_id` fails L3, L4, L5: `declared 'coverage-ext-1', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-authentication-parameters-restauthenticationparams))
- [x] Combined change of `comment`, `refresh_interval_seconds` in one plan
- [ ] Model `rest_authentication.oauth_api_type`: `REST_AUTHENTICATION = ( OAUTH_API_TYPE )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-rest#rest-authentication-parameters-restauthenticationparams))
- [x] `catalog_namespace`
- [x] `enabled`
- [x] `refresh_interval_seconds`
- [x] `comment`

Modeled, without a live sample: `catalog_source`, `table_format`, `rest_authentication.oauth_token_uri`, `rest_authentication.oauth_client_id`, `rest_authentication.oauth_client_secret`, `rest_authentication.oauth_allowed_scopes`, `rest_authentication.bearer_token`, `owner`

### CATALOG INTEGRATION:object_storage

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 1 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-object-storage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-catalog-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-catalog-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-catalog-integration)

- [ ] `table_format` fails L2: `create failed: Crashed: snowcap rejects the declaration: TypeError: Expected ObjectStoreCatalogIntegration.table_format to be one of (ICEBERG), got 'DELTA' instead` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-object-storage#required-parameters))
- Combined change: untested (needs two or more fields)
- [ ] Model `refresh_interval_seconds`: `REFRESH_INTERVAL_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-catalog-integration-object-storage#optional-parameters))
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `catalog_source`, `owner`

### COMPUTE POOL

Docs only: cost: the audit scope skips Snowpark Container Services objects (compute pool nodes consume credits). Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-compute-pool) [show](https://docs.snowflake.com/en/sql-reference/sql/show-compute-pools) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-compute-pool)

- [ ] Model `for_application`: `FOR APPLICATION <app-name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool#optional-parameters))
- [ ] Model `placement_group`: `PLACEMENT_GROUP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-compute-pool#parameters))
- [ ] Model `backup_instance_families[]`: `BACKUP_INSTANCE_FAMILIES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool#optional-parameters))
- [ ] Model `state`: `{ SUSPEND | RESUME }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-compute-pool#parameters))
- [ ] Model `tags`: `WITH TAG / SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-compute-pool#syntax))

Modeled, without a live sample: `min_nodes`, `max_nodes`, `instance_family`, `auto_resume`, `initially_suspended`, `auto_suspend_secs`, `comment`, `owner`

### DATABASE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 5 pass, 1 fail, 45 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-database) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-database) [show](https://docs.snowflake.com/en/sql-reference/sql/show-databases) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-database)

- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>_R02_TAGS'. on: ALTER TAG REFERENCE <db>_R02_TAGS?domain=DATABASE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
  - snowcap sent: `ALTER TAG REFERENCE <db>_R02_TAGS?domain=DATABASE SET`
- [x] Combined change of `comment`, `data_retention_time_in_days`, `default_ddl_collation`, `max_data_extension_time_in_days` in one plan
- [ ] Model `object_visibility` (preview): `OBJECT_VISIBILITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `catalog_sync_namespace_mode`: `CATALOG_SYNC_NAMESPACE_MODE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-database))
- [ ] Model `catalog_sync_namespace_flatten_delimiter`: `CATALOG_SYNC_NAMESPACE_FLATTEN_DELIMITER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-database))
- [ ] Model `classification_profile`: `CLASSIFICATION_PROFILE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `replicable_with_failover_groups`: `REPLICABLE_WITH_FAILOVER_GROUPS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `data_quality_monitoring_settings` (preview): `DATA_QUALITY_MONITORING_SETTINGS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `enable_replication`: `ENABLE REPLICATION TO ACCOUNTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `disable_replication`: `DISABLE REPLICATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `enable_failover`: `ENABLE FAILOVER TO ACCOUNTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `disable_failover`: `DISABLE FAILOVER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `primary`: `PRIMARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `contacts`: `WITH CONTACT / CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `feature_policy`: `FEATURE POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `iceberg_default_ddl_collation`: `ICEBERG_DEFAULT_DDL_COLLATION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-iceberg-default-ddl-collation))
- [ ] Model `iceberg_version_default`: `ICEBERG_VERSION_DEFAULT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-iceberg-version-default))
- [ ] Model `iceberg_merge_on_read_behavior`: `ICEBERG_MERGE_ON_READ_BEHAVIOR` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [ ] Model `replace_invalid_characters`: `REPLACE_INVALID_CHARACTERS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-replace-invalid-characters))
- [ ] Model `storage_serialization_policy`: `STORAGE_SERIALIZATION_POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-storage-serialization-policy))
- [ ] Model `catalog_sync`: `CATALOG_SYNC` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `enable_data_compaction`: `ENABLE_DATA_COMPACTION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `oauth_authorization_server`: `OAUTH_AUTHORIZATION_SERVER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `oauth_scopes_supported`: `OAUTH_SCOPES_SUPPORTED` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_notebook_compute_pool_cpu`: `DEFAULT_NOTEBOOK_COMPUTE_POOL_CPU` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_notebook_compute_pool_gpu`: `DEFAULT_NOTEBOOK_COMPUTE_POOL_GPU` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `metric_level`: `METRIC_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-metric-level))
- [ ] Model `trace_level`: `TRACE_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-trace-level))
- [ ] Model `event_table`: `EVENT_TABLE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-event-table))
- [ ] Model `base_location_prefix`: `BASE_LOCATION_PREFIX` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-base-location-prefix))
- [ ] Model `default_streamlit_notebook_warehouse`: `DEFAULT_STREAMLIT_NOTEBOOK_WAREHOUSE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_metadata_write_format`: `DEFAULT_METADATA_WRITE_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `purge_on_drop_table`: `PURGE_ON_DROP_TABLE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `row_timestamp`: `ROW_TIMESTAMP` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `row_timestamp_default`: `ROW_TIMESTAMP_DEFAULT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `serverless_task_max_statement_size`: `SERVERLESS_TASK_MAX_STATEMENT_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `serverless_task_min_statement_size`: `SERVERLESS_TASK_MIN_STATEMENT_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `service_caller_token_validity_secs`: `SERVICE_CALLER_TOKEN_VALIDITY_SECS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `suspend_alert_after_num_failures`: `SUSPEND_ALERT_AFTER_NUM_FAILURES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `suspend_task_after_num_failures`: `SUSPEND_TASK_AFTER_NUM_FAILURES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `task_auto_retry_attempts`: `TASK_AUTO_RETRY_ATTEMPTS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `user_task_managed_initial_warehouse_size`: `USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `user_task_minimum_trigger_interval_in_seconds`: `USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `user_task_timeout_ms`: `USER_TASK_TIMEOUT_MS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [x] `transient`
- [x] `comment`
- [x] `data_retention_time_in_days`
- [x] `max_data_extension_time_in_days`
- [x] `default_ddl_collation`

Modeled, without a live sample: `owner`, `external_volume`, `catalog`

### DATABASE ROLE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 1 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-database-role) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-database-role) [show](https://docs.snowflake.com/en/sql-reference/sql/show-database-roles) [describe](None)

- [ ] `tags` fails L2: `create failed: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 2 at position 86 unexpected '.'. syntax error line 4 at position 18 unexpected '<EOF>'. on: SELECT * FROM table(<db>.R01_TAGS.information_schema.tag_references( '<db>.R01_TAGS', ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database-role))
- Combined change: untested (needs two or more fields)
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database-role))
- [x] `comment`

Modeled, without a live sample: `database`, `owner`

### DATABASE:from_share

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 0 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-database) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-database) [show](https://docs.snowflake.com/en/sql-reference/sql/show-databases) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-database)

- Combined change: untested (needs two or more fields)
- [ ] Model `comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-database))
- [x] `share`

Modeled, without a live sample: `owner`

### DYNAMIC TABLE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 5 fail, 35 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table) [show](https://docs.snowflake.com/en/sql-reference/sql/show-dynamic-tables) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-dynamic-table)

- [ ] `query` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 73 unexpected 'as'. on: ALTER DYNAMIC TABLE <db>.PUBLIC.R00_QUERY SET as SELECT ID, NAME FROM <db>.PUBLIC.COVE ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#required-parameters))
  - snowcap sent: `ALTER DYNAMIC TABLE <db>.PUBLIC.R00_QUERY SET as SELECT ID, NAME FROM <db>.PUBLIC.COVERAGE_T WHERE ID > 0`
- [ ] `columns[].name` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 82 unexpected '('. on: ALTER DYNAMIC TABLE <db>.PUBLIC.R01_COLUMNS___NAME SET (ID2, NAME)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
  - snowcap sent: `ALTER DYNAMIC TABLE <db>.PUBLIC.R01_COLUMNS___NAME SET (ID2, NAME)`
- [ ] `columns[].comment` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 85 unexpected '('. on: ALTER DYNAMIC TABLE <db>.PUBLIC.R02_COLUMNS___COMMENT SET (ID COMMENT 'b', NAME)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#table-comment-actions-tablecommentaction))
  - snowcap sent: `ALTER DYNAMIC TABLE <db>.PUBLIC.R02_COLUMNS___COMMENT SET (ID COMMENT 'b', NAME)`
- [ ] `refresh_mode` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'refresh_mode' for 'DYNAMIC_TABLE' on: ALTER DYNAMIC TABLE <db>.PUBLIC.R05_REFRESH_MODE SET refresh_mode = INCREMENTAL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
  - snowcap sent: `ALTER DYNAMIC TABLE <db>.PUBLIC.R05_REFRESH_MODE SET refresh_mode = INCREMENTAL`
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=DYNAMIC TABLE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=DYNAMIC TABLE SET`
- [x] Combined change of `target_lag`, `warehouse` in one plan
- [ ] Model `transient`: `TRANSIENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `refresh_using`: `REFRESH USING ( <dml_statement> )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#required-parameters))
- [ ] Model `columns[].data_type`: `<col_type>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `columns[].collate`: `COLLATE '<collation_specification>'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table#label-create-or-alter-dt-limitations))
- [ ] Model `columns[].masking_policy`: `MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].projection_policy`: `PROJECTION POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].tags`: `TAG ( <tag_name> = '<tag_value>' [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].contacts`: `WITH CONTACT ( <purpose> = <contact_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `scheduler`: `SCHEDULER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `initialization_warehouse`: `INITIALIZATION_WAREHOUSE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `cluster_by`: `CLUSTER BY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#clustering-actions-clusteringaction))
- [ ] Model `automatic_clustering`: `SUSPEND | RESUME RECLUSTER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#clustering-actions-clusteringaction))
- [ ] Model `scheduling_state`: `SUSPEND | RESUME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `data_retention_time_in_days`: `DATA_RETENTION_TIME_IN_DAYS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `max_data_extension_time_in_days`: `MAX_DATA_EXTENSION_TIME_IN_DAYS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-max-data-extension-time-in-days))
- [ ] Model `default_ddl_collation`: `DEFAULT_DDL_COLLATION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-default-ddl-collation))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-event-level))
- [ ] Model `row_timestamp`: `ROW_TIMESTAMP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `data_metric_schedule`: `DATA_METRIC_SCHEDULE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `copy_grants`: `COPY GRANTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `copy_tags`: `COPY TAGS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `row_access_policy`: `ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `aggregation_policy`: `AGGREGATION POLICY <policy_name> [ ENTITY KEY ( <col_name> [ , ... ] ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `storage_lifecycle_policy`: `STORAGE LIFECYCLE POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `contacts`: `WITH CONTACT / SET CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `require_user`: `REQUIRE USER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `frozen_where`: `FROZEN WHERE ( <expr> )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `backfill_from`: `BACKFILL FROM <table_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `start_at`: `START AT ( { STREAM | TIMESTAMP | STATEMENT | OFFSET } => ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `execute_as_user`: `EXECUTE AS USER <user_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `use_secondary_roles`: `USE SECONDARY ROLES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `data_metric_functions`: `WITH DATA METRIC FUNCTION ( <dmf_name> ON ( <col_name> [ , ... ] ) [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table))
- [ ] Model `search_optimization`: `ADD SEARCH OPTIMIZATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-dynamic-table))
- [x] `target_lag`
- [x] `warehouse`
- [x] `comment`

Modeled, without a live sample: `initialize`, `owner`

### EVENT TABLE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 4 fail, 8 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-event-table) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table) [show](https://docs.snowflake.com/en/sql-reference/sql/show-event-tables) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-event-table)

- [ ] `cluster_by` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 76 unexpected 'BY'. syntax error line 1 at position 98 unexpected ')'. on: ALTER TABLE <db>.PUBLIC.R00_CLUSTER_BY SET CLUSTER BY (OBSERVED_TIMESTAMP)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#clustering-actions-clusteringaction))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R00_CLUSTER_BY SET CLUSTER BY (OBSERVED_TIMESTAMP)`
- [ ] `max_data_extension_time_in_days` fails L3, L5: `Snowflake reports max_data_extension_time_in_days='10' (declared form 10), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-max-data-extension-time-in-days))
- [ ] `default_ddl_collation` fails L3, L4, L5: `Snowflake reports default_ddl_collation='en-ci' (declared form 'en-ci'), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-default-ddl-collation))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R04_DEFAULT_DDL_COLLATION SET DEFAULT_DDL_COLLATION = $$en-cs$$`
- [ ] `tags` fails L2: `create failed: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 12 unexpected 'TABLE'. on: ALTER EVENT TABLE <db>.PUBLIC.R06_TAGS SET TAG <db>.PUBLIC.COVERAGE_TAG='a'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [x] Combined change of `change_tracking`, `comment`, `data_retention_time_in_days`, `max_data_extension_time_in_days` in one plan
- [ ] Model `automatic_clustering`: `SUSPEND | RESUME RECLUSTER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#clustering-actions-clusteringaction))
- [ ] Model `row_access_policy`: `ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `aggregation_policy`: `SET AGGREGATION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `join_policy`: `SET JOIN POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `contacts`: `WITH CONTACT / SET CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table))
- [ ] Model `data_metric_functions`: `WITH DATA METRIC FUNCTION ( <dmf_name> ON ( <col_name> [ , ... ] ) [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-event-table))
- [ ] Model `data_metric_schedule`: `DATA_METRIC_SCHEDULE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `search_optimization`: `ADD SEARCH OPTIMIZATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-event-table#search-optimization-actions-searchoptimizationaction))
- [x] `data_retention_time_in_days`
- [x] `change_tracking`
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `owner`

### EXTERNAL ACCESS INTEGRATION

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 5 pass, 0 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-external-access-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-external-access-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [x] Combined change of `allowed_api_authentication_integrations`, `allowed_authentication_secrets`, `allowed_network_rules`, `comment`, `enabled` in one plan
- [ ] Model `tags`: `TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-external-access-integration#parameters))
- [x] `allowed_network_rules[]`
- [x] `allowed_api_authentication_integrations[]`
- [x] `allowed_authentication_secrets[]`
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `owner`

### EXTERNAL FUNCTION

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 15 fail, 2 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-external-function) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-function) [show](https://docs.snowflake.com/en/sql-reference/sql/show-external-functions) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-function)

- [ ] `arguments[].name` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#required-parameters))
- [ ] `arguments[].data_type` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#required-parameters))
- [ ] `returns` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#required-parameters))
- [ ] `return_not_null` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] `null_handling` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] `volatility` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] `secure` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- [ ] `comment` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- [ ] `api_integration` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function#external-functions))
- [ ] `url` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#required-parameters))
- [ ] `headers[]` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] `max_batch_rows` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] `compression` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] `request_translator` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function#external-functions))
- [ ] `response_translator` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_external_function'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function#external-functions))
- Combined change: untested (needs two or more fields)
- [ ] Model `context_headers[]`: `CONTEXT_HEADERS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-function#optional-parameters))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))

Modeled, without a live sample: `owner`

### EXTERNAL VOLUME

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 11 fail, 5 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-external-volume) [show](https://docs.snowflake.com/en/sql-reference/sql/show-external-volumes) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-external-volume)

- [ ] `add_storage_location` fails L3, L4, L5: `declared [{'name': 'loc1', 'storage_provider': 'S3', 'storage_base_url': 's3://coverage-audit-bucket/a/', 'encryption': {'type': 'NONE'}, 'storage_aws_role_arn': 'arn:aws:iam::001234567890:role/coverage', 'storage_aws_external_id': None}], snowcap fetches [{'name': 'loc1', 'storage_provider': 'S3',  ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-external-volume#parameters))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R00_ADD_STORAGE_LOCATION SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'NONE') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage$$), (NAME = $$loc2$$ storage_provid ...`
- [ ] `remove_storage_location` fails L3, L4, L5: `declared [{'name': 'loc1', 'storage_provider': 'S3', 'storage_base_url': 's3://coverage-audit-bucket/a/', 'encryption': {'type': 'NONE'}, 'storage_aws_role_arn': 'arn:aws:iam::001234567890:role/coverage', 'storage_aws_external_id': None}, {'name': 'loc2', 'storage_provider': 'S3', 'storage_base_url' ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-external-volume#parameters))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R01_REMOVE_STORAGE_LOCATION SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'NONE') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage$$))`
- [ ] `storage_locations[].name` fails L4, L5: `apply ran but the change did not land: Snowflake reports loc1_name='loc1'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#required-parameters))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R02_STORAGE_LOCATIONS___NAME SET storage_locations = ((NAME = $$loc1b$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'NONE') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage$$))`
- [ ] `storage_locations[].storage_provider` fails L4, L5: `apply ran but the change did not land: Snowflake reports loc1_storage_provider='S3'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R03_STORAGE_LOCATIONS___STORAGE_PROVIDER SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'GCS' STORAGE_BASE_URL = $$gcs://coverage-audit-bucket/a/$$ encryption = (type = 'NONE')))`
- [ ] `storage_locations[].storage_base_url` fails L4, L5: `apply ran but the change did not land: Snowflake reports loc1_storage_base_url='s3://coverage-audit-bucket/a/'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R04_STORAGE_LOCATIONS___STORAGE_BASE_URL SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a2/$$ encryption = (type = 'NONE') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage$$))`
- [ ] `storage_locations[].storage_aws_role_arn` fails L4, L5: `apply ran but the change did not land: Snowflake reports loc1_storage_aws_role_arn='arn:aws:iam::001234567890:role/coverage'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R05_STORAGE_LOCATIONS___STORAGE_AWS_ROLE_ARN SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'NONE') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage2$$))`
- [ ] `storage_locations[].storage_aws_external_id` fails L4, L5: `apply ran but the change did not land: Snowflake reports loc1_storage_aws_external_id='coverage-ext-1'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R06_STORAGE_LOCATIONS___STORAGE_AWS_EXTERNAL_ID SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'NONE') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage$$ STORAGE_AW ...`
- [ ] `storage_locations[].encryption.type` fails L4, L5: `apply ran but the change did not land: Snowflake reports loc1_encryption_type='NONE'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R07_STORAGE_LOCATIONS___ENCRYPTION_TYPE SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'AWS_SSE_S3') STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::001234567890:role/coverage$$))`
- [ ] `storage_locations[].encryption.kms_key_id` fails L3, L4, L5: `declared ['coverage-kms-1'], snowcap fetches [None]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
  - snowcap sent: `ALTER EXTERNAL VOLUME <db>_R08_STORAGE_LOCATIONS___ENCRYPTION_KMS_KEY_ID SET storage_locations = ((NAME = $$loc1$$ storage_provider = 'S3' STORAGE_BASE_URL = $$s3://coverage-audit-bucket/a/$$ encryption = (type = 'AWS_SSE_KMS' KMS_KEY_ID = $$coverage-kms-2$$) STORAGE_AWS_ROLE_ARN = $$arn:aws:iam::00 ...`
- [ ] `allow_writes` fails L5: `plan after create: ["update ['storage_locations']"]; plan after change: ["update ['storage_locations']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-external-volume#parameters))
- [ ] `comment` fails L5: `plan after create: ["update ['storage_locations']"]; plan after change: ["update ['storage_locations']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-external-volume#parameters))
- Combined change: untested (needs two or more fields)
- [ ] Model `storage_locations[].storage_aws_access_point_arn`: `STORAGE_AWS_ACCESS_POINT_ARN` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
- [ ] Model `storage_locations[].use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
- [ ] Model `storage_locations[].azure_tenant_id`: `AZURE_TENANT_ID` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#cloud-provider-parameters-cloudproviderparams))
- [ ] Model `storage_locations[].storage_endpoint`: `STORAGE_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-external-volume#s3-compatible-storage-parameters-s3compatiblestorageparams))
- [ ] Model `storage_locations[].credentials`: `CREDENTIALS = ( AWS_KEY_ID = ... AWS_SECRET_KEY = ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-external-volume#parameters))

Modeled, without a live sample: `owner`

### FAILOVER GROUP

Docs only: edition: failover groups need Business Critical and the test account is ENTERPRISE; a placeholder CREATE FAILOVER GROUP fails with "003078 (55000): Replication is not enabled for your account.". Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-failover-group) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group) [show](https://docs.snowflake.com/en/sql-reference/sql/show-failover-groups) [describe](None)

- [ ] Model `as_replica_of`: `AS REPLICA OF <org_name>.<source_account_name>.<name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-failover-group#parameters))
- [ ] Model `allowed_external_volumes` (preview): `ALLOWED_EXTERNAL_VOLUMES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-failover-group#parameters))
- [ ] Model `allowed_external_volumes[]` (preview): `ADD / REMOVE ... ALLOWED_EXTERNAL_VOLUMES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `optimized_refresh`: `OPTIMIZED_REFRESH` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `rpo_assurance`: `RPO_ASSURANCE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `error_integration`: `ERROR_INTEGRATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `primary`: `PRIMARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `secondary_state`: `SUSPEND / RESUME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))
- [ ] Model `tags`: `WITH TAG / SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-failover-group#syntax))

Modeled, without a live sample: `object_types`, `allowed_databases`, `allowed_databases[]`, `allowed_shares`, `allowed_shares[]`, `allowed_integration_types`, `allowed_accounts`, `ignore_edition_check`, `replication_schedule`, `owner`

### FILE FORMAT:csv

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 22 pass, 0 fail, 2 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-file-format) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-file-format) [show](https://docs.snowflake.com/en/sql-reference/sql/show-file-formats) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-file-format)

- [x] Combined change of `binary_format`, `comment`, `compression`, `date_format`, `empty_field_as_null`, `encoding`, `error_on_column_count_mismatch`, `escape`, `escape_unenclosed_field`, `field_delimiter`, `field_optionally_enclosed_by`, `file_extension`, `null_if`, `parse_header`, `record_delimiter`, `replace_invalid_characters`, `skip_blank_lines`, `skip_byte_order_mark`, `skip_header`, `time_format`, `timestamp_format`, `trim_space` in one plan
- [ ] Model `temporary`: `TEMP | TEMPORARY | VOLATILE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
- [ ] Model `multi_line`: `MULTI_LINE = TRUE | FALSE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-csv))
- [x] `compression`
- [x] `record_delimiter`
- [x] `field_delimiter`
- [x] `file_extension`
- [x] `parse_header`
- [x] `skip_header`
- [x] `skip_blank_lines`
- [x] `date_format`
- [x] `time_format`
- [x] `timestamp_format`
- [x] `binary_format`
- [x] `escape`
- [x] `escape_unenclosed_field`
- [x] `trim_space`
- [x] `field_optionally_enclosed_by`
- [x] `null_if[]`
- [x] `error_on_column_count_mismatch`
- [x] `replace_invalid_characters`
- [x] `empty_field_as_null`
- [x] `skip_byte_order_mark`
- [x] `encoding`
- [x] `comment`

Modeled, without a live sample: `type`, `owner`

### FILE FORMAT:json

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 16 pass, 0 fail, 2 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-file-format) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-file-format) [show](https://docs.snowflake.com/en/sql-reference/sql/show-file-formats) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-file-format)

- [x] Combined change of `allow_duplicate`, `binary_format`, `comment`, `compression`, `date_format`, `enable_octal`, `file_extension`, `ignore_utf8_errors`, `null_if`, `replace_invalid_characters`, `skip_byte_order_mark`, `strip_null_values`, `strip_outer_array`, `time_format`, `timestamp_format`, `trim_space` in one plan
- [ ] Model `temporary`: `TEMP | TEMPORARY | VOLATILE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
- [ ] Model `multi_line`: `MULTI_LINE = TRUE | FALSE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-json))
- [x] `compression`
- [x] `date_format`
- [x] `time_format`
- [x] `timestamp_format`
- [x] `binary_format`
- [x] `trim_space`
- [x] `null_if[]`
- [x] `file_extension`
- [x] `enable_octal`
- [x] `allow_duplicate`
- [x] `strip_outer_array`
- [x] `strip_null_values`
- [x] `replace_invalid_characters`
- [x] `ignore_utf8_errors`
- [x] `skip_byte_order_mark`
- [x] `comment`

Modeled, without a live sample: `type`, `owner`

### FILE FORMAT:parquet

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 6 pass, 0 fail, 3 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-file-format) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-file-format) [show](https://docs.snowflake.com/en/sql-reference/sql/show-file-formats) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-file-format)

- [x] Combined change of `binary_as_text`, `comment`, `compression`, `null_if`, `replace_invalid_characters`, `trim_space` in one plan
- [ ] Model `temporary`: `TEMP | TEMPORARY | VOLATILE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format))
- [ ] Model `use_logical_type`: `USE_LOGICAL_TYPE = TRUE | FALSE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-parquet))
- [ ] Model `use_vectorized_scanner`: `USE_VECTORIZED_SCANNER = TRUE | FALSE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-file-format#type-parquet))
- [x] `compression`
- [x] `binary_as_text`
- [x] `trim_space`
- [x] `replace_invalid_characters`
- [x] `null_if[]`
- [x] `comment`

Modeled, without a live sample: `type`, `owner`

### FUNCTION:javascript

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 8 fail, 8 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-function) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-function) [show](https://docs.snowflake.com/en/sql-reference/sql/show-user-functions) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-function)

- [ ] `arguments[].name` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 101 unexpected '('. on: ALTER FUNCTION <db>.PUBLIC.R00_ARGUMENTS___NAME(VARCHAR, FLOAT) SET (X VARCHAR, C2 FLOAT)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
  - snowcap sent: `ALTER FUNCTION <db>.PUBLIC.R00_ARGUMENTS___NAME(VARCHAR, FLOAT) SET (X VARCHAR, C2 FLOAT)`
- [ ] `arguments[].data_type` fails L5: `plan after create: []; plan after change: ['plan crashes: Exception: Found multiple functions matching <db>.PUBLIC.R01_ARGUMENTS___DATA_TYPE(VARCHAR, VARCHAR)']` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `returns` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 93 unexpected 'FLOAT'. on: ALTER FUNCTION <db>.PUBLIC.R02_RETURNS(VARCHAR) SET returns FLOAT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
  - snowcap sent: `ALTER FUNCTION <db>.PUBLIC.R02_RETURNS(VARCHAR) SET returns FLOAT`
- [ ] `returns_table[]` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 101 unexpected 'TABLE'. syntax error line 1 at position 110 unexpected 'VARCHAR'. syntax error line 1 at position 121 unexpected 'FLOAT'. syntax error line 1 at position 126 unexpected ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
  - snowcap sent: `ALTER FUNCTION <db>.PUBLIC.R03_RETURNS_TABLE__(VARCHAR) SET returns TABLE (A VARCHAR, B FLOAT)`
- [ ] `null_handling` fails L3, L4, L5: `Snowflake reports null handling='RETURNS NULL ON NULL INPUT' (declared form 'RETURNS NULL ON NULL INPUT'), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
  - snowcap sent: `ALTER FUNCTION <db>.PUBLIC.R04_NULL_HANDLING(VARCHAR) SET CALLED ON NULL INPUT`
- [ ] `volatility` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 96 unexpected '<EOF>'. on: ALTER FUNCTION <db>.PUBLIC.R05_VOLATILITY(VARCHAR) SET VOLATILE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
  - snowcap sent: `ALTER FUNCTION <db>.PUBLIC.R05_VOLATILITY(VARCHAR) SET VOLATILE`
- [ ] `body` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 82 unexpected 'AS'. on: ALTER FUNCTION <db>.PUBLIC.R06_BODY(VARCHAR) SET AS $$return X + 'b';$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
  - snowcap sent: `ALTER FUNCTION <db>.PUBLIC.R06_BODY(VARCHAR) SET AS $$return X + 'b';$$`
- [ ] `secure` fails L2: `create failed: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 72 unexpected 'SECURE'. on: CREATE FUNCTION <db>.PUBLIC.R07_SECURE SECURE (X VARCHAR) returns VARCHAR language JAVASCRIPT AS $$return X ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- Combined change: untested (needs two or more fields)
- [ ] Model `arguments[].default`: `DEFAULT <default_value>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] Model `return_not_null`: `[ NOT ] NULL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `trace_level`: `TRACE_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-trace-level))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-event-level))
- [ ] Model `metric_level`: `METRIC_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-metric-level))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- [x] `comment`

Modeled, without a live sample: `language`, `copy_grants`, `owner`

### FUNCTION:python

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 0 pass, 14 fail, 10 not modeled, 4 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-function) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-function) [show](https://docs.snowflake.com/en/sql-reference/sql/show-user-functions) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-function)

- [ ] `arguments[].name` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `arguments[].data_type` fails L3, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `returns` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `null_handling` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `volatility` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `runtime_version` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#python))
- [ ] `handler` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#python))
- [ ] `body` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] `imports[]` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#python))
- [ ] `packages[]` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#python))
- [ ] `external_access_integrations[]` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- [ ] `secrets[]` fails L2, L3, L4, L5: `declared {'cred': '<db>.PUBLIC.COVERAGE_S1'}, Snowflake reports secrets='{}'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
  - snowcap sent: `CREATE FUNCTION <db>.PUBLIC.R11_SECRETS__ (X VARCHAR) returns VARCHAR language PYTHON RUNTIME_VERSION = $$3.11$$ HANDLER = $$main$$ EXTERNAL_ACCESS_INTEGRATIONS = (<db>_EAI1) AS $$def main(x, *rest): return x def main2(x, *rest): return x $$`
- [ ] `secure` fails L2: `create failed: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 68 unexpected 'SECURE'. on: CREATE FUNCTION <db>.PUBLIC.R12_SECURE SECURE (X VARCHAR) returns VARCHAR language PYTHON RUNTIME_VERSION = $$3 ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- [ ] `comment` fails L3, L4, L5: `snowcap fetch crashes: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))
- [ ] Combined change of `comment`, `external_access_integrations`, `secrets` in one plan: `Crashed: TypeError: _PythonUDF.__init__() missing 2 required positional arguments: 'runtime_version' and 'handler'`
- [ ] Model `arguments[].default`: `DEFAULT <default_value>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] Model `aggregate`: `AGGREGATE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#python))
- [ ] Model `return_not_null`: `[ NOT ] NULL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#all-languages))
- [ ] Model `artifact_repository`: `ARTIFACT_REPOSITORY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-function#python))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `trace_level`: `TRACE_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-trace-level))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-event-level))
- [ ] Model `metric_level`: `METRIC_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-metric-level))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-function))

Modeled, without a live sample: `language`, `returns_table[]`, `copy_grants`, `owner`

### GIT REPOSITORY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 1 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-git-repository) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-git-repository) [show](https://docs.snowflake.com/en/sql-reference/sql/show-git-repositories) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-git-repository)

- [ ] `origin` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'ORIGIN' for 'GIT_REPOSITORY' on: ALTER GIT REPOSITORY <db>.PUBLIC.R00_ORIGIN SET ORIGIN = $$https://github.com/datacoves/workshop__snowcap.git$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-git-repository))
  - snowcap sent: `ALTER GIT REPOSITORY <db>.PUBLIC.R00_ORIGIN SET ORIGIN = $$https://github.com/datacoves/workshop__snowcap.git$$`
- [x] Combined change of `api_integration`, `comment` in one plan
- [ ] Model `tags`: `WITH TAG / SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-git-repository))
- [x] `api_integration`
- [x] `comment`

Modeled, without a live sample: `git_credentials`, `owner`

### HYBRID TABLE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 18 fail, 9 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-table) [show](https://docs.snowflake.com/en/sql-reference/sql/show-hybrid-tables) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-table)

- [ ] `columns[].name` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#required-parameters))
- [ ] `columns[]` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table))
- [ ] `columns[].type` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column))
- [ ] `columns[].default` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#optional-parameters))
- [ ] `columns[].not_null` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column))
- [ ] `columns[].collate` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-collations-disable))
- [ ] `columns[].comment` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#optional-parameters))
- [ ] `columns[].tags` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] `primary_key` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-notes-on-constraints))
- [ ] `unique_constraints[]` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#add-and-drop-constraints-on-an-existing-hybrid-table))
- [ ] `foreign_keys[]` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-notes-on-constraints))
- [ ] `check_constraints[]` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-check-constraints))
- [ ] `constraints[].name` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-check-constraints))
- [ ] `constraints[].properties` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-notes-on-constraints))
- [ ] `indexes[]` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-index))
- [ ] `indexes[].include` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#optional-parameters))
- [ ] `comment` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#optional-parameters))
- [ ] `tags` fails L2: `create failed: Crashed: AttributeError: module 'snowcap.data_provider' has no attribute 'fetch_hybrid_table'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- Combined change: untested (needs two or more fields)
- [ ] Model `columns[].autoincrement`: `{ AUTOINCREMENT | IDENTITY } [ ( <start_num> , <step_num> ) | START <num> INCREMENT <num> ] [ ORDER | NOORDER ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#optional-parameters))
- [ ] Model `columns[].masking_policy`: `MASKING POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].projection_policy`: `PROJECTION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `default_ddl_collation`: `DEFAULT_DDL_COLLATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table#label-hybrid-table-collations-disable))
- [ ] Model `data_retention_time_in_days`: `DATA_RETENTION_TIME_IN_DAYS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `row_access_policy`: `ROW ACCESS POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `aggregation_policy`: `SET AGGREGATION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `join_policy`: `SET JOIN POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `contacts`: `SET CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table))

Modeled, without a live sample: `owner`

### ICEBERG TABLE:snowflake

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 13 fail, 21 not modeled, 4 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table) [show](https://docs.snowflake.com/en/sql-reference/sql/show-iceberg-tables) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-iceberg-table)

- [ ] `external_volume` fails L2: `create failed: Refused: MissingResourceException: External Volume "SNOWFLAKE_MANAGED" not found. Referenced by: iceberg table "<db>.PUBLIC.R00_EXTERNAL_VOLUME"` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] `columns[].name` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(10,0)'}, {'name': 'NAME2', 'data_type': 'VARCHAR(134217728)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#table-column-actions-tablecolumnaction))
- [ ] `columns[].type` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(19,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(134217728)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#table-column-actions-tablecolumnaction))
- [ ] `columns[].structured_type` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(10,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(134217728)'}, {'name': 'O', 'data_type': 'OBJECT(A NUMBER(10,0), B NUMBER(10,0))'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table-alter-column-set-data-type))
- [ ] `columns[].default` fails L2: `create failed: ProgrammingError: 002262 (22000): SQL compilation error: Default value data type does not match data type for column ID on: CREATE ICEBERG TABLE <db>.PUBLIC.R04_COLUMNS___DEFAULT (ID NUMBER(10,0) DEFAULT '1', NAME VARCHAR(13421 ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] `columns[].not_null` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(10,0)', 'not_null': False}, {'name': 'NAME', 'data_type': 'VARCHAR(134217728)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#table-column-actions-tablecolumnaction))
- [ ] `columns[].comment` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(10,0)', 'comment': 'b'}, {'name': 'NAME', 'data_type': 'VARCHAR(134217728)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#table-column-actions-tablecolumnaction))
- [ ] `columns[].collate` fails L2, L4, L5: `declared [{'name': 'ID', 'data_type': 'NUMBER(10,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(134217728)', 'collate': 'en-ci'}], Snowflake reports name_type='VARCHAR(134217728)'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#table-column-actions-tablecolumnaction))
  - snowcap sent: `CREATE ICEBERG TABLE <db>.PUBLIC.R07_COLUMNS___COLLATE (ID NUMBER(10,0), NAME VARCHAR(134217728)) CATALOG = $$SNOWFLAKE$$ storage_serialization_policy = OPTIMIZED`
- [ ] `columns[].tags` fails L2, L4, L5: `declared [{'name': 'ID', 'data_type': 'NUMBER(10,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(134217728)', 'tags': {'<db>.PUBLIC.COVERAGE_TAG': 'a'}}], Snowflake reports name_tag=None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
  - snowcap sent: `CREATE ICEBERG TABLE <db>.PUBLIC.R08_COLUMNS___TAGS (ID NUMBER(10,0), NAME VARCHAR(134217728)) CATALOG = $$SNOWFLAKE$$ storage_serialization_policy = OPTIMIZED`
- [ ] `constraints[]` fails L3, L4, L5: `declared ['PRIMARY KEY', None], snowcap fetches [None, None]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] `storage_serialization_policy` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 093657 (42601): SQL Compilation error: Storage serialization policy is immutable once the Iceberg table is created. on: ALTER ICEBERG TABLE <db>.PUBLIC.R10_STORAGE_SERIALIZATION_POLICY SET storage_serialization_policy = OPT ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
  - snowcap sent: `ALTER ICEBERG TABLE <db>.PUBLIC.R10_STORAGE_SERIALIZATION_POLICY SET storage_serialization_policy = OPTIMIZED`
- [ ] `change_tracking` fails L3, L4, L5: `Snowflake reports change_tracking='ON' (declared form True), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R15_TAGS?domain=ICEBERG TABLE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R15_TAGS?domain=ICEBERG TABLE SET`
- Combined change: untested (needs two or more fields)
- [ ] Model `transient`: `TRANSIENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] Model `columns[].masking_policy`: `[ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].projection_policy`: `[ WITH ] PROJECTION POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `partition_by`: `PARTITION BY ( partitionExpression [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table-partition-evolution))
- [ ] Model `path_layout`: `PATH_LAYOUT = { FLAT | HIERARCHICAL }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [ ] Model `cluster_by`: `CLUSTER BY ( <expr> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#clustering-actions-clusteringaction))
- [ ] Model `recluster`: `{ SUSPEND | RESUME } RECLUSTER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#clustering-actions-clusteringaction))
- [ ] Model `target_file_size`: `TARGET_FILE_SIZE = '{ AUTO | 16MB | 32MB | 64MB | 128MB }'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [ ] Model `copy_grants`: `COPY GRANTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] Model `copy_tags`: `COPY TAGS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] Model `error_logging`: `ERROR_LOGGING = { TRUE | FALSE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [ ] Model `iceberg_version`: `ICEBERG_VERSION = <integer>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake#optional-parameters))
- [ ] Model `iceberg_merge_on_read_behavior`: `ICEBERG_MERGE_ON_READ_BEHAVIOR = { 'AUTO' | 'ENABLED' | 'DISABLED' }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [ ] Model `enable_data_compaction`: `ENABLE_DATA_COMPACTION = { TRUE | FALSE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [ ] Model `enable_schema_evolution`: `ENABLE_SCHEMA_EVOLUTION = { TRUE | FALSE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [ ] Model `search_optimization`: `{ ADD | DROP } SEARCH OPTIMIZATION [ ON <search_method_with_target> [ , ... ] ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#search-optimization-actions-searchoptimizationaction))
- [ ] Model `data_metric_schedule`: `DATA_METRIC_SCHEDULE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#usage-notes))
- [ ] Model `row_access_policy`: `[ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `aggregation_policy`: `[ WITH ] AGGREGATION POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `join_policy` (preview): `SET JOIN POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `contacts`: `WITH CONTACT ( <purpose> = <contact_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-iceberg-table#parameters))
- [x] `data_retention_time_in_days`
- [x] `max_data_extension_time_in_days`
- [x] `comment`

Modeled, without a live sample: `catalog`, `base_location`, `catalog_sync`, `owner`

### IMAGE REPOSITORY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 0 fail, 3 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-image-repository) [alter](None) [show](https://docs.snowflake.com/en/sql-reference/sql/show-image-repositories) [describe](None)

- Combined change: untested (needs two or more fields)
- [ ] Model `encryption.type`: `ENCRYPTION = ( TYPE = ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-image-repository))
- [ ] Model `comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-image-repository))
- [ ] Model `tags`: `WITH TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-image-repository))

Modeled, without a live sample: `owner`

### MASKING POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 4 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-masking-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-masking-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-masking-policy)

- [ ] `signature[].arg_name` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 90 unexpected '('. on: ALTER MASKING POLICY <db>.PUBLIC.R00_SIGNATURE___ARG_NAME SET (VAL VARCHAR, C2 NUMBER)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy#required-parameters))
  - snowcap sent: `ALTER MASKING POLICY <db>.PUBLIC.R00_SIGNATURE___ARG_NAME SET (VAL VARCHAR, C2 NUMBER)`
- [ ] `signature[].arg_type` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 90 unexpected '('. on: ALTER MASKING POLICY <db>.PUBLIC.R01_SIGNATURE___ARG_TYPE SET (VAL VARCHAR, C1 VARCHAR)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy#required-parameters))
  - snowcap sent: `ALTER MASKING POLICY <db>.PUBLIC.R01_SIGNATURE___ARG_TYPE SET (VAL VARCHAR, C1 VARCHAR)`
- [ ] `return_type` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 81 unexpected 'returns'. on: ALTER MASKING POLICY <db>.PUBLIC.R02_RETURN_TYPE SET returns NUMBER(38,0)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy#required-parameters))
  - snowcap sent: `ALTER MASKING POLICY <db>.PUBLIC.R02_RETURN_TYPE SET returns NUMBER(38,0)`
- [ ] `exempt_other_policies` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'EXEMPT_OTHER_POLICIES' for 'MASKING_POLICY' on: ALTER MASKING POLICY <db>.PUBLIC.R05_EXEMPT_OTHER_POLICIES SET EXEMPT_OTHER_POLICIES = TRUE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-masking-policy#optional-parameters))
  - snowcap sent: `ALTER MASKING POLICY <db>.PUBLIC.R05_EXEMPT_OTHER_POLICIES SET EXEMPT_OTHER_POLICIES = TRUE`
- Combined change: untested (needs two or more fields)
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-masking-policy#parameters))
- [x] `body`
- [x] `comment`

Modeled, without a live sample: `owner`

### MATERIALIZED VIEW

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 6 fail, 12 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view) [show](https://docs.snowflake.com/en/sql-reference/sql/show-materialized-views) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-materialized-view)

- [ ] `as` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 78 unexpected 'as'. on: ALTER MATERIALIZED VIEW <db>.PUBLIC.R00_AS SET as SELECT ID, NAME FROM <db>.PU ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#required-parameters))
  - snowcap sent: `ALTER MATERIALIZED VIEW <db>.PUBLIC.R00_AS SET as SELECT ID, NAME FROM <db>.PUBLIC.COVERAGE_T WHERE ID > 0`
- [ ] `secure` fails L2: `create failed: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 86 unexpected 'as'. on: CREATE MATERIALIZED VIEW <db>.PUBLIC.R01_SECURE SECURE as SELECT ID, NAME FROM COVERAGE_MATERIALIZED_VIEW_832 ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#parameters))
- [ ] `columns[].name` fails L4, L5: `a change to [{'name': 'ID2', 'data_type': 'NUMBER(38,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#optional-parameters))
- [ ] `columns[].tags` fails L2, L4, L5: `declared [{'name': 'ID', 'data_type': 'NUMBER(38,0)', 'tags': {'<db>.PUBLIC.COVERAGE_TAG': 'a'}}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}], Snowflake reports id_tag=None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#usage-notes))
  - snowcap sent: `CREATE MATERIALIZED VIEW <db>.PUBLIC.R04_COLUMNS___TAGS (ID, NAME) as SELECT ID, NAME FROM <db>.PUBLIC.COVERAGE_T`
- [ ] `cluster_by` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 86 unexpected 'CLUSTER'. on: ALTER MATERIALIZED VIEW <db>.PUBLIC.R05_CLUSTER_BY SET CLUSTER BY (NAME)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#parameters))
  - snowcap sent: `ALTER MATERIALIZED VIEW <db>.PUBLIC.R05_CLUSTER_BY SET CLUSTER BY (NAME)`
- [ ] `tags` fails L2: `create failed: ProgrammingError: 391806 (0A000): SQL compilation error: Invalid value MATERIALIZED_VIEW for argument OBJECT_TYPE. Please use object type TABLE for all kinds of table-like objects. on: SELECT * FROM table(<db>.information_schema.tag_references( '<db>.PUBLI` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#usage-notes))
- Combined change: untested (needs two or more fields)
- [ ] Model `interactive`: `INTERACTIVE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#optional-parameters))
- [ ] Model `columns[].masking_policy`: `<col> [ WITH ] MASKING POLICY <policy_name> [ USING ( <col1> , <cond_col1> , ... ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#usage-notes))
- [ ] Model `columns[].masking_policy_using`: `USING ( <col1> , <cond_col1> , ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#optional-parameters))
- [ ] Model `columns[].projection_policy`: `<col> [ WITH ] PROJECTION POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#optional-parameters))
- [ ] Model `row_access_policy`: `[ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#usage-notes))
- [ ] Model `aggregation_policy`: `[ WITH ] AGGREGATION POLICY <policy_name> [ ENTITY KEY ( <col_name> [ , ... ] ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#optional-parameters))
- [ ] Model `recluster`: `{ SUSPEND | RESUME } RECLUSTER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#parameters))
- [ ] Model `suspended`: `{ SUSPEND | RESUME }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#parameters))
- [ ] Model `data_metric_functions[]`: `WITH DATA METRIC FUNCTION ( <dmf_name> ON ( <col_name> [ , ... ] ) [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#usage-notes))
- [ ] Model `data_metric_functions[].expectations[]`: `EXPECTATION <expectation_name> ( <expression> )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-materialized-view#optional-parameters))
- [ ] Model `data_metric_schedule`: `DATA_METRIC_SCHEDULE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#parameters))
- [ ] Model `contacts`: `WITH CONTACT / SET CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-materialized-view#parameters))
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `owner`

### MCP SERVER

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 7 pass, 0 fail, 0 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-mcp-server) [alter](None) [show](https://docs.snowflake.com/en/sql-reference/sql/show-mcp-servers) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-mcp-server)

- Combined change: untested (needs two or more fields)
- [x] `specification`
- [x] `tools[].name`
- [x] `tools[].type`
- [x] `tools[].title`
- [x] `tools[].description`
- [x] `tools[].identifier`
- [x] `tools[].config`

Modeled, without a live sample: `owner`

### NETWORK POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 7 pass, 0 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-network-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-network-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-network-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-network-policy)

- [x] Combined change of `allowed_ip_list`, `allowed_network_rule_list`, `blocked_ip_list`, `blocked_network_rule_list`, `comment` in one plan
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-network-policy#syntax))
- [x] `allowed_network_rule_list[]`
- [x] `blocked_network_rule_list[]`
- [x] `add_network_rule_list`
- [x] `remove_network_rule_list`
- [x] `allowed_ip_list[]`
- [x] `blocked_ip_list[]`
- [x] `comment`

Modeled, without a live sample: `owner`

### NETWORK RULE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 2 fail, 0 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-network-rule) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-network-rule) [show](https://docs.snowflake.com/en/sql-reference/sql/show-network-rules) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-network-rule)

- [ ] `type` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'type' for 'NETWORK_RULE' on: ALTER NETWORK RULE <db>.PUBLIC.R00_TYPE SET type = AZURELINKID` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-network-rule))
  - snowcap sent: `ALTER NETWORK RULE <db>.PUBLIC.R00_TYPE SET type = AZURELINKID`
- [ ] `mode` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'mode' for 'NETWORK_RULE' on: ALTER NETWORK RULE <db>.PUBLIC.R01_MODE SET mode = INTERNAL_STAGE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-network-rule))
  - snowcap sent: `ALTER NETWORK RULE <db>.PUBLIC.R01_MODE SET mode = INTERNAL_STAGE`
- [x] Combined change of `comment`, `value_list` in one plan
- [x] `value_list[]`
- [x] `comment`

Modeled, without a live sample: `owner`

### NOTEBOOK

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 3 pass, 1 fail, 5 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notebook) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notebook) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notebooks) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notebook)

- [ ] `from` fails L3, L4, L5: `Snowflake reports default_version_source_location_uri='@<db>.PUBLIC.COVERAGE_STG1' (declared form '@<db>.PUBLIC.COVERAGE_STG1'), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notebook))
- [x] Combined change of `comment`, `query_warehouse` in one plan
- [ ] Model `idle_auto_shutdown_time_seconds`: `IDLE_AUTO_SHUTDOWN_TIME_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notebook))
- [ ] Model `runtime_name`: `RUNTIME_NAME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notebook))
- [ ] Model `compute_pool`: `COMPUTE_POOL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notebook))
- [ ] Model `warehouse`: `WAREHOUSE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notebook))
- [ ] Model `secrets`: `SECRETS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notebook))
- [x] `main_file`
- [x] `comment`
- [x] `query_warehouse`

Modeled, without a live sample: `owner`

### NOTIFICATION INTEGRATION:email

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 0 fail, 3 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-email) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-email) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notification-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notification-integration)

- [x] Combined change of `comment`, `enabled` in one plan
- [ ] Model `default_recipients`: `DEFAULT_RECIPIENTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-email#parameters))
- [ ] Model `default_subject`: `DEFAULT_SUBJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-email#parameters))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-email#syntax))
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `type`, `allowed_recipients`, `owner`

### NOTIFICATION INTEGRATION:inbound_azure

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 4 fail, 2 not modeled, 4 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-inbound-azure) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-inbound-azure) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notification-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notification-integration)

- [ ] `azure_storage_queue_primary_uri` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'AZURE_STORAGE_QUEUE_PRIMARY_URI' for 'INTEGRATION - QUEUE - AZURE_STORAGE_QUEUE' on: ALTER NOTIFICATION INTEGRATION <db>_R00_AZURE_STORAGE_QUEUE_PRIMARY_URI SET AZURE_STORAG ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-inbound-azure#required-parameters))
  - snowcap sent: `ALTER NOTIFICATION INTEGRATION <db>_R00_AZURE_STORAGE_QUEUE_PRIMARY_URI SET AZURE_STORAGE_QUEUE_PRIMARY_URI = $$https://coverageplaceholder.queue.core.windows.net/coverage-b$$ AZURE_TENANT_ID = $$00000000-0000-0000-0000-000000000001$$`
- [ ] `azure_tenant_id` fails L3, L5: `Snowflake reports azure_tenant_id='00000000-0000-0000-0000-000000000001' (declared form '00000000-0000-0000-0000-000000000001'), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-inbound-azure#required-parameters))
- [ ] `enabled` fails L5: `plan after create: ["update ['azure_tenant_id']"]; plan after change: ["update ['azure_tenant_id']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-inbound-azure#parameters))
- [ ] `comment` fails L5: `plan after create: ["update ['azure_tenant_id']"]; plan after change: ["update ['azure_tenant_id']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-inbound-azure#parameters))
- [x] Combined change of `comment`, `enabled` in one plan
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-inbound-azure#optional-parameters))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-inbound-azure#syntax))

Modeled, without a live sample: `type`, `notification_provider`, `direction`, `owner`

### NOTIFICATION INTEGRATION:inbound_gcp

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 1 fail, 1 not modeled, 4 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-inbound-gcp) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-inbound-gcp) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notification-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notification-integration)

- [ ] `gcp_pubsub_subscription_name` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'GCP_PUBSUB_SUBSCRIPTION_NAME' for 'INTEGRATION - QUEUE - GCP_PUBSUB' on: ALTER NOTIFICATION INTEGRATION <db>_R00_GCP_PUBSUB_SUBSCRIPTION_NAME SET GCP_PUBSUB_SUBSCR ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-inbound-gcp#required-parameters))
  - snowcap sent: `ALTER NOTIFICATION INTEGRATION <db>_R00_GCP_PUBSUB_SUBSCRIPTION_NAME SET GCP_PUBSUB_SUBSCRIPTION_NAME = $$projects/coverage-placeholder/subscriptions/coverage-b$$`
- [x] Combined change of `comment`, `enabled` in one plan
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-inbound-gcp#syntax))
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `type`, `notification_provider`, `direction`, `owner`

### NOTIFICATION INTEGRATION:outbound_aws

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 4 pass, 0 fail, 1 not modeled, 4 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-outbound-aws) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-outbound-aws) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notification-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notification-integration)

- [x] Combined change of `aws_sns_role_arn`, `aws_sns_topic_arn`, `comment`, `enabled` in one plan
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-outbound-aws#syntax))
- [x] `aws_sns_topic_arn`
- [x] `aws_sns_role_arn`
- [x] `enabled`
- [x] `comment`

Modeled, without a live sample: `type`, `direction`, `notification_provider`, `owner`

### NOTIFICATION INTEGRATION:outbound_azure

Docs only: Outbound Azure Event Grid integrations need a Snowflake account on Azure; on this AWS account a CREATE with placeholder values fails with "001008 (22023): SQL compilation error: invalid value \[QUEUE - AZURE\_EVENT\_GRID\] for parameter 'Integration Type'". Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-outbound-azure) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-outbound-azure) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notification-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notification-integration)

- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-outbound-azure#syntax))

Modeled, without a live sample: `type`, `direction`, `notification_provider`, `azure_event_grid_topic_endpoint`, `azure_tenant_id`, `enabled`, `comment`, `owner`

### NOTIFICATION INTEGRATION:outbound_gcp

Docs only: Outbound Google Pub/Sub integrations need a Snowflake account on Google Cloud; on this AWS account a CREATE with placeholder values fails with "001422 (22023): SQL compilation error: invalid value 'OUTBOUND' for property 'Direction'". Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration-queue-outbound-gcp) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-outbound-gcp) [show](https://docs.snowflake.com/en/sql-reference/sql/show-notification-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-notification-integration)

- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-notification-integration-queue-outbound-gcp#syntax))

Modeled, without a live sample: `type`, `direction`, `notification_provider`, `gcp_pubsub_topic_name`, `enabled`, `comment`, `owner`

### PACKAGES POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 4 pass, 0 fail, 0 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-packages-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-packages-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-packages-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-packages-policy)

- [x] Combined change of `additional_creation_blocklist`, `allowlist`, `blocklist`, `comment` in one plan
- [x] `allowlist`
- [x] `blocklist`
- [x] `additional_creation_blocklist`
- [x] `comment`

Modeled, without a live sample: `language`, `owner`

### PASSWORD POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 12 pass, 0 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-password-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-password-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-password-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-password-policy)

- [x] Combined change of `comment`, `password_history`, `password_lockout_time_mins`, `password_max_age_days`, `password_max_length`, `password_max_retries`, `password_min_age_days`, `password_min_length`, `password_min_lower_case_chars`, `password_min_numeric_chars`, `password_min_special_chars`, `password_min_upper_case_chars` in one plan
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-password-policy#parameters))
- [x] `password_min_length`
- [x] `password_max_length`
- [x] `password_min_upper_case_chars`
- [x] `password_min_lower_case_chars`
- [x] `password_min_numeric_chars`
- [x] `password_min_special_chars`
- [x] `password_min_age_days`
- [x] `password_max_age_days`
- [x] `password_max_retries`
- [x] `password_lockout_time_mins`
- [x] `password_history`
- [x] `comment`

Modeled, without a live sample: `owner`

### PIPE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 2 fail, 3 not modeled, 4 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-pipe) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-pipe) [show](https://docs.snowflake.com/en/sql-reference/sql/show-pipes) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-pipe)

- [ ] `copy_statement` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 64 unexpected 'as'. on: ALTER PIPE <db>.PUBLIC.R00_COPY_STATEMENT SET as COPY INTO <db>.PUBLIC.COVERAGE_T FROM @COVERAGE_PIPE_86 ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-pipe))
  - snowcap sent: `ALTER PIPE <db>.PUBLIC.R00_COPY_STATEMENT SET as COPY INTO <db>.PUBLIC.COVERAGE_T FROM @<db>.PUBLIC.COVERAGE_STG2`
- [ ] `error_integration` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 091116 (0A000): Cannot reset ERROR_INTEGRATION property on Pipe R01_ERROR_INTEGRATION. Please create a new one. on: ALTER PIPE <db>.PUBLIC.R01_ERROR_INTEGRATION SET ERROR_INTEGRATION = $$<db>_NI2$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-pipe))
  - snowcap sent: `ALTER PIPE <db>.PUBLIC.R01_ERROR_INTEGRATION SET ERROR_INTEGRATION = $$<db>_NI2$$`
- [ ] Combined change of `comment`, `error_integration` in one plan: `ProgrammingError: 091116 (0A000): Cannot reset ERROR_INTEGRATION property on Pipe COMBINED. Please create a new one. on: ALTER PIPE <db>.PUBLIC.COMBINED SET ERROR_INTEGRATION = $$<db>_NI2$$ COMMENT = $$b$$`
- [ ] Model `pipe_execution_paused`: `PIPE_EXECUTION_PAUSED` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-pipe-execution-paused))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-pipe))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-pipe))
- [x] `comment`

Modeled, without a live sample: `auto_ingest`, `aws_sns_topic`, `integration`, `owner`

### PROCEDURE:python

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 4 pass, 7 fail, 8 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-procedure#python-handler) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-procedure) [show](https://docs.snowflake.com/en/sql-reference/sql/show-procedures) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-procedure)

- [ ] `returns` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 84 unexpected 'NUMBER'. on: ALTER PROCEDURE <db>.PUBLIC.R00_RETURNS() SET returns NUMBER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
  - snowcap sent: `ALTER PROCEDURE <db>.PUBLIC.R00_RETURNS() SET returns NUMBER`
- [ ] `null_handling` fails L2, L4, L5: `declared 'RETURNS NULL ON NULL INPUT', Snowflake reports null handling='CALLED ON NULL INPUT'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
  - snowcap sent: `CREATE PROCEDURE <db>.PUBLIC.R01_NULL_HANDLING () returns VARCHAR language PYTHON RUNTIME_VERSION = $$3.11$$ PACKAGES = ($$snowflake-snowpark-python$$) HANDLER = $$main$$ COMMENT = $$user-defined procedure$$ execute as CALLER AS $$def main(session): return 'a' def main2(session): return 'b' $$`
- [ ] `body` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 73 unexpected 'AS'. on: ALTER PROCEDURE <db>.PUBLIC.R04_BODY() SET AS $$def main(session): return 'c' def main2(session): return 'd' $ ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
  - snowcap sent: `ALTER PROCEDURE <db>.PUBLIC.R04_BODY() SET AS $$def main(session): return 'c' def main2(session): return 'd' $$`
- [ ] `secure` fails L2: `create failed: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 70 unexpected 'SECURE'. on: CREATE PROCEDURE <db>.PUBLIC.R06_SECURE SECURE () returns VARCHAR language PYTHON RUNTIME_VERSION = $$3.11$$ P ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
- [ ] `runtime_version` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 000002 (0A000): Unsupported feature 'ALTER PROCEDURE SET RUNTIME_VERSION'. on: ALTER PROCEDURE <db>.PUBLIC.R07_RUNTIME_VERSION() SET RUNTIME_VERSION = $$3.12$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
  - snowcap sent: `ALTER PROCEDURE <db>.PUBLIC.R07_RUNTIME_VERSION() SET RUNTIME_VERSION = $$3.12$$`
- [ ] `packages` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 000002 (0A000): Unsupported feature 'ALTER PROCEDURE SET PACKAGES'. on: ALTER PROCEDURE <db>.PUBLIC.R08_PACKAGES() SET PACKAGES = ($$snowflake-snowpark-python$$, $$pyyaml$$)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
  - snowcap sent: `ALTER PROCEDURE <db>.PUBLIC.R08_PACKAGES() SET PACKAGES = ($$snowflake-snowpark-python$$, $$pyyaml$$)`
- [ ] `handler` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'HANDLER' for 'PROCEDURE' on: ALTER PROCEDURE <db>.PUBLIC.R10_HANDLER() SET HANDLER = $$main2$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
  - snowcap sent: `ALTER PROCEDURE <db>.PUBLIC.R10_HANDLER() SET HANDLER = $$main2$$`
- Combined change: untested (needs two or more fields)
- [ ] Model `tags`: `TAG ( <tag_name> = '<tag_value>' , ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-procedure))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-procedure))
- [ ] Model `trace_level`: `TRACE_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-procedure))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `metric_level`: `METRIC_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `temporary`: `TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
- [ ] Model `artifact_repository`: `ARTIFACT_REPOSITORY = <repository>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-procedure))
- [ ] Model `secrets`: `SECRETS = ( '<secret_variable_name>' = <secret_name> , ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-procedure))
- [x] `comment`
- [x] `execute_as`
- [x] `owner`
- [x] `imports`

Modeled, without a live sample: `external_access_integrations`

### REPLICATION GROUP

Docs only: org: CREATE REPLICATION GROUP ... OBJECT\_TYPES = DATABASES ALLOWED\_ACCOUNTS = \<the only other account in the organization\> fails with "003078 (55000): Replication is not enabled for your account.", and enabling it needs ORGADMIN, which the audit does not use. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-replication-group) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-replication-group) [show](https://docs.snowflake.com/en/sql-reference/sql/show-replication-groups) [describe](None)

- [ ] Model `as_replica_of`: `AS REPLICA OF <org_name>.<source_account_name>.<name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-replication-group#parameters))
- [ ] Model `allowed_external_volumes` (preview): `ALLOWED_EXTERNAL_VOLUMES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-replication-group#parameters))
- [ ] Model `allowed_external_volumes[]` (preview): `ADD / REMOVE ... ALLOWED_EXTERNAL_VOLUMES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-replication-group#syntax))
- [ ] Model `error_integration`: `ERROR_INTEGRATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-replication-group#syntax))
- [ ] Model `comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-replication-group#syntax))
- [ ] Model `secondary_state`: `SUSPEND / RESUME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-replication-group#syntax))
- [ ] Model `tags`: `WITH TAG / SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-replication-group#syntax))

Modeled, without a live sample: `object_types`, `allowed_databases`, `allowed_databases[]`, `allowed_shares`, `allowed_shares[]`, `allowed_integration_types`, `allowed_accounts`, `ignore_edition_check`, `replication_schedule`, `owner`

### RESOURCE MONITOR

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 2 fail, 2 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-resource-monitor) [show](https://docs.snowflake.com/en/sql-reference/sql/show-resource-monitors) [describe](None)

- [ ] `frequency` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 090259 (42601): Must specify frequency and start time together. on: ALTER RESOURCE MONITOR <db>_R01_FREQUENCY SET frequency = WEEKLY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor))
  - snowcap sent: `ALTER RESOURCE MONITOR <db>_R01_FREQUENCY SET frequency = WEEKLY`
- [ ] `start_timestamp` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 090259 (42601): Must specify frequency and start time together. on: ALTER RESOURCE MONITOR <db>_R02_START_TIMESTAMP SET START_TIMESTAMP = $$2049-02-01 00:00$$` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor))
  - snowcap sent: `ALTER RESOURCE MONITOR <db>_R02_START_TIMESTAMP SET START_TIMESTAMP = $$2049-02-01 00:00$$`
- [x] Combined change of `credit_quota`, `end_timestamp`, `frequency`, `start_timestamp` in one plan
- [ ] Model `triggers[]`: `TRIGGERS ON <threshold> PERCENT DO { SUSPEND | SUSPEND_IMMEDIATE | NOTIFY }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-resource-monitor))
- [ ] Model `account_assignment`: `RESOURCE_MONITOR (on ACCOUNT)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-account))
- [x] `credit_quota`
- [x] `end_timestamp`

Modeled, without a live sample: `notify_users[]`, `owner`

### ROLE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 1 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-role) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-role) [show](https://docs.snowflake.com/en/sql-reference/sql/show-roles) [describe](None)

- [ ] `tags` fails L3, L5: `declared {'<db>.PUBLIC.COVERAGE_TAG': 'a'}, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-role))
- Combined change: untested (needs two or more fields)
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-role))
- [x] `comment`

Modeled, without a live sample: `owner`

### ROW ACCESS POLICY

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 3 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-row-access-policy) [show](https://docs.snowflake.com/en/sql-reference/sql/show-row-access-policies) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-row-access-policy)

- [ ] `signature[].arg_name` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 96 unexpected '('. on: ALTER ROW ACCESS POLICY <db>.PUBLIC.R00_SIGNATURE___ARG_NAME SET (V VARCHAR, W2 NUMBER)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy#required-parameters))
  - snowcap sent: `ALTER ROW ACCESS POLICY <db>.PUBLIC.R00_SIGNATURE___ARG_NAME SET (V VARCHAR, W2 NUMBER)`
- [ ] `signature[].arg_type` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 96 unexpected '('. on: ALTER ROW ACCESS POLICY <db>.PUBLIC.R01_SIGNATURE___ARG_TYPE SET (V VARCHAR, W1 VARCHAR)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-row-access-policy#required-parameters))
  - snowcap sent: `ALTER ROW ACCESS POLICY <db>.PUBLIC.R01_SIGNATURE___ARG_TYPE SET (V VARCHAR, W1 VARCHAR)`
- [ ] `body` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 80 unexpected '->'. on: ALTER ROW ACCESS POLICY <db>.PUBLIC.R02_BODY SET -> V = 'B'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-row-access-policy#parameters))
  - snowcap sent: `ALTER ROW ACCESS POLICY <db>.PUBLIC.R02_BODY SET -> V = 'B'`
- Combined change: untested (needs two or more fields)
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-row-access-policy#parameters))
- [x] `comment`

Modeled, without a live sample: `return_type`, `owner`

### SCHEMA

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 7 pass, 0 fail, 37 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-schema) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-schema) [show](https://docs.snowflake.com/en/sql-reference/sql/show-schemas) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-schema)

- [x] Combined change of `comment`, `data_retention_time_in_days`, `default_ddl_collation`, `max_data_extension_time_in_days` in one plan
- [ ] Model `contacts`: `WITH CONTACT / CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-schema))
- [ ] Model `external_volume`: `EXTERNAL_VOLUME` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-external-volume))
- [ ] Model `catalog`: `CATALOG` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-catalog))
- [ ] Model `iceberg_default_ddl_collation`: `ICEBERG_DEFAULT_DDL_COLLATION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-iceberg-default-ddl-collation))
- [ ] Model `iceberg_version_default`: `ICEBERG_VERSION_DEFAULT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-iceberg-version-default))
- [ ] Model `iceberg_merge_on_read_behavior`: `ICEBERG_MERGE_ON_READ_BEHAVIOR` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-schema))
- [ ] Model `replace_invalid_characters`: `REPLACE_INVALID_CHARACTERS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `storage_serialization_policy`: `STORAGE_SERIALIZATION_POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `classification_profile`: `CLASSIFICATION_PROFILE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-schema))
- [ ] Model `catalog_sync`: `CATALOG_SYNC` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `object_visibility` (preview): `OBJECT_VISIBILITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-schema))
- [ ] Model `enable_data_compaction`: `ENABLE_DATA_COMPACTION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `oauth_authorization_server`: `OAUTH_AUTHORIZATION_SERVER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `oauth_scopes_supported`: `OAUTH_SCOPES_SUPPORTED` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_notebook_compute_pool_cpu`: `DEFAULT_NOTEBOOK_COMPUTE_POOL_CPU` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_notebook_compute_pool_gpu`: `DEFAULT_NOTEBOOK_COMPUTE_POOL_GPU` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `trace_level`: `TRACE_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-trace-level))
- [ ] Model `replicable_with_failover_groups`: `REPLICABLE_WITH_FAILOVER_GROUPS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-schema))
- [ ] Model `base_location_prefix`: `BASE_LOCATION_PREFIX` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_streamlit_notebook_warehouse`: `DEFAULT_STREAMLIT_NOTEBOOK_WAREHOUSE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `default_metadata_write_format`: `DEFAULT_METADATA_WRITE_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `metric_level`: `METRIC_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `purge_on_drop_table`: `PURGE_ON_DROP_TABLE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `pipe_execution_paused`: `PIPE_EXECUTION_PAUSED` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `row_timestamp`: `ROW_TIMESTAMP` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `row_timestamp_default`: `ROW_TIMESTAMP_DEFAULT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `serverless_task_max_statement_size`: `SERVERLESS_TASK_MAX_STATEMENT_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `serverless_task_min_statement_size`: `SERVERLESS_TASK_MIN_STATEMENT_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `service_caller_token_validity_secs`: `SERVICE_CALLER_TOKEN_VALIDITY_SECS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `suspend_alert_after_num_failures`: `SUSPEND_ALERT_AFTER_NUM_FAILURES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `suspend_task_after_num_failures`: `SUSPEND_TASK_AFTER_NUM_FAILURES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `task_auto_retry_attempts`: `TASK_AUTO_RETRY_ATTEMPTS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `user_task_managed_initial_warehouse_size`: `USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `user_task_minimum_trigger_interval_in_seconds`: `USER_TASK_MINIMUM_TRIGGER_INTERVAL_IN_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `user_task_timeout_ms`: `USER_TASK_TIMEOUT_MS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [x] `transient`
- [x] `managed_access`
- [x] `owner`
- [x] `data_retention_time_in_days`
- [x] `max_data_extension_time_in_days`
- [x] `default_ddl_collation`
- [x] `comment`

Modeled, without a live sample: `tags`

### SECRET:generic_string

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 1 pass, 1 fail, 0 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-secret) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-secret) [show](https://docs.snowflake.com/en/sql-reference/sql/show-secrets) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-secret)

- [ ] `secret_string` fails L3, L4, L5: `declared '{secret:secret_string_6}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#generic-string-parameters))
- [ ] Combined change of `comment`, `secret_string` in one plan: `missed secret_string`
- [x] `comment`

Modeled, without a live sample: `type`, `owner`

### SECRET:oauth2_authorization_code

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 0 pass, 3 fail, 0 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-secret) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-secret) [show](https://docs.snowflake.com/en/sql-reference/sql/show-secrets) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-secret)

- [ ] `oauth_refresh_token` fails L3, L4, L5: `declared '{secret:oauth_refresh_token_2}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#oauth-with-authorization-code-grant-flow-parameters))
- [ ] `oauth_refresh_token_expiry_time` fails L4, L5: `snowcap crashes: KeyError: 'oauth_scopes'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#oauth-with-authorization-code-grant-flow-parameters))
- [ ] `comment` fails L4, L5: `snowcap crashes: KeyError: 'oauth_scopes'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#common-parameters-all-syntaxes))
- [ ] Combined change of `comment`, `oauth_refresh_token`, `oauth_refresh_token_expiry_time` in one plan: `Crashed: KeyError: 'oauth_scopes'`

Modeled, without a live sample: `type`, `api_authentication`, `owner`

### SECRET:oauth2_client_credentials

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 0 pass, 3 fail, 0 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-secret) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-secret) [show](https://docs.snowflake.com/en/sql-reference/sql/show-secrets) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-secret)

- [ ] `api_authentication` fails L3, L4, L5: `snowcap fetch crashes: TypeError: Expected OAuthSecret.oauth_scopes to be list[str], got '[read]' instead` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-secret#oauth-with-client-credentials-flow-required-parameters))
- [ ] `oauth_scopes[]` fails L3, L4, L5: `snowcap fetch crashes: TypeError: Expected OAuthSecret.oauth_scopes to be list[str], got '[read]' instead` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#oauth-with-client-credentials-flow-parameters))
- [ ] `comment` fails L3, L4, L5: `snowcap fetch crashes: TypeError: Expected OAuthSecret.oauth_scopes to be list[str], got '[read]' instead` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#common-parameters-all-syntaxes))
- [ ] Combined change of `comment`, `oauth_scopes` in one plan: `Crashed: TypeError: Expected OAuthSecret.oauth_scopes to be list[str], got '[read]' instead`

Modeled, without a live sample: `type`, `owner`

### SECRET:password

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 2 pass, 1 fail, 0 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-secret) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-secret) [show](https://docs.snowflake.com/en/sql-reference/sql/show-secrets) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-secret)

- [ ] `password` fails L3, L4, L5: `declared '{secret:password_2}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-secret#basic-authentication-parameters))
- [ ] Combined change of `comment`, `password`, `username` in one plan: `missed password`
- [x] `username`
- [x] `comment`

Modeled, without a live sample: `type`, `owner`

### SECURITY INTEGRATION:api_auth_authorization_code

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 7 pass, 1 fail, 3 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `oauth_client_secret` fails L3, L4, L5: `declared '{secret:oauth_client_secret_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Combined change of `comment`, `enabled`, `oauth_access_token_validity`, `oauth_client_auth_method`, `oauth_client_id`, `oauth_client_secret`, `oauth_grant`, `oauth_token_endpoint` in one plan: `missed oauth_client_secret`
- [ ] Model `oauth_authorization_endpoint`: `OAUTH_AUTHORIZATION_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Model `oauth_refresh_token_validity`: `OAUTH_REFRESH_TOKEN_VALIDITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth#parameters))
- [x] `oauth_grant`
- [x] `enabled`
- [x] `oauth_token_endpoint`
- [x] `oauth_client_auth_method`
- [x] `oauth_client_id`
- [x] `oauth_access_token_validity`
- [x] `comment`

Modeled, without a live sample: `type`, `auth_type`, `owner`

### SECURITY INTEGRATION:api_auth_client_credentials

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 7 pass, 3 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `auth_type` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'AUTH_TYPE' for 'INTEGRATION - API_AUTHENTICATION' on: ALTER SECURITY INTEGRATION <db>_R00_AUTH_TYPE SET AUTH_TYPE = $$WORKLOAD_IDENT ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#required-parameters))
  - snowcap sent: `ALTER SECURITY INTEGRATION <db>_R00_AUTH_TYPE SET AUTH_TYPE = $$WORKLOAD_IDENTITY_FEDERATION$$`
- [ ] `oauth_client_secret` fails L3, L4, L5: `declared '{secret:oauth_client_secret_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `oauth_allowed_scopes` fails L3, L4, L5: `snowcap fetch crashes: AttributeError: 'list' object has no attribute 'strip'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Combined change of `comment`, `enabled`, `oauth_access_token_validity`, `oauth_allowed_scopes`, `oauth_client_auth_method`, `oauth_client_id`, `oauth_client_secret`, `oauth_grant`, `oauth_token_endpoint` in one plan: `Crashed: AttributeError: 'list' object has no attribute 'strip'`
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth#parameters))
- [x] `oauth_grant`
- [x] `enabled`
- [x] `oauth_token_endpoint`
- [x] `oauth_client_auth_method`
- [x] `oauth_client_id`
- [x] `oauth_access_token_validity`
- [x] `comment`

Modeled, without a live sample: `type`, `owner`

### SECURITY INTEGRATION:api_auth_jwt_bearer

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 0 pass, 8 fail, 4 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `oauth_grant` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R00_OAUTH_GRANT type = API_AUTHENTICATION AUTH_TYPE = ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `enabled` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R01_ENABLED type = API_AUTHENTICATION AUTH_TYPE = $$O ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth#parameters))
- [ ] `oauth_token_endpoint` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R02_OAUTH_TOKEN_ENDPOINT type = API_AUTHENTICATION AU ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `oauth_client_auth_method` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R03_OAUTH_CLIENT_AUTH_METHOD type = API_AUTHENTICATIO ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `oauth_client_id` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R04_OAUTH_CLIENT_ID type = API_AUTHENTICATION AUTH_TY ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `oauth_client_secret` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R05_OAUTH_CLIENT_SECRET type = API_AUTHENTICATION AUT ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `oauth_access_token_validity` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R06_OAUTH_ACCESS_TOKEN_VALIDITY type = API_AUTHENTICA ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] `comment` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_R07_COMMENT type = API_AUTHENTICATION AUTH_TYPE = $$O ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth#parameters))
- [ ] Combined change of `comment`, `enabled`, `oauth_access_token_validity`, `oauth_client_auth_method`, `oauth_client_id`, `oauth_client_secret`, `oauth_grant`, `oauth_token_endpoint` in one plan: `ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): [OAUTH_ASSERTION_ISSUER] on: CREATE SECURITY INTEGRATION <db>_COMBINED type = API_AUTHENTICATION AUTH_TYPE = $$OAUT ...`
- [ ] Model `oauth_assertion_issuer`: `OAUTH_ASSERTION_ISSUER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Model `oauth_authorization_endpoint`: `OAUTH_AUTHORIZATION_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Model `oauth_refresh_token_validity`: `OAUTH_REFRESH_TOKEN_VALIDITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-api-auth#optional-parameters))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-api-auth#parameters))

Modeled, without a live sample: `type`, `auth_type`, `owner`

### SECURITY INTEGRATION:snowflake_oauth_custom

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 14 pass, 2 fail, 7 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-oauth-snowflake) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `oauth_alternate_redirect_uris` fails L3, L4, L5: `Snowflake reports alternate_redirect_uris_list="['https://example.com/oauth/alt1']" (declared form ['https://example.com/oauth/alt1']), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] `oauth_enable_role_selection` fails L3, L4, L5: `declared True, snowcap fetches False` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] Combined change of `blocked_roles_list`, `comment`, `enabled`, `network_policy`, `oauth_allow_non_tls_redirect_uri`, `oauth_alternate_redirect_uris`, `oauth_any_role_mode`, `oauth_enable_role_selection`, `oauth_enforce_pkce`, `oauth_issue_refresh_tokens`, `oauth_redirect_uri`, `oauth_refresh_token_validity`, `oauth_single_use_refresh_tokens_required`, `oauth_use_secondary_roles`, `pre_authorized_roles_list` in one plan: `missed oauth_alternate_redirect_uris`
- [ ] Model `allowed_roles_list`: `ALLOWED_ROLES_LIST` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] Model `oauth_access_token_validity`: `OAUTH_ACCESS_TOKEN_VALIDITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] Model `oauth_client_rsa_public_key`: `OAUTH_CLIENT_RSA_PUBLIC_KEY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] Model `oauth_client_rsa_public_key_2`: `OAUTH_CLIENT_RSA_PUBLIC_KEY_2` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] Model `is_agentic`: `IS_AGENTIC` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-custom-clients))
- [ ] Model `use_privatelink_for_authorization_endpoint`: `USE_PRIVATELINK_FOR_AUTHORIZATION_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#optional-parameters-all-oauth-clients))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-oauth-snowflake#syntax))
- [x] `oauth_client_type`
- [x] `oauth_redirect_uri`
- [x] `enabled`
- [x] `oauth_allow_non_tls_redirect_uri`
- [x] `oauth_enforce_pkce`
- [x] `oauth_single_use_refresh_tokens_required`
- [x] `oauth_use_secondary_roles`
- [x] `pre_authorized_roles_list`
- [x] `blocked_roles_list`
- [x] `oauth_any_role_mode`
- [x] `oauth_issue_refresh_tokens`
- [x] `oauth_refresh_token_validity`
- [x] `network_policy`
- [x] `comment`

Modeled, without a live sample: `type`, `oauth_client`, `owner`

### SECURITY INTEGRATION:snowflake_oauth_partner

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 6 fail, 8 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-oauth-snowflake) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `oauth_client` fails L3, L4, L5: `snowcap fetch crashes: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_R00_OAUTH_CLIENT (oauth_client=LOOKER): fetch is not implemented for partner OAuth` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#required-parameters-all-oauth-clients))
- [ ] `oauth_redirect_uri` fails L3, L4, L5: `snowcap fetch crashes: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_R01_OAUTH_REDIRECT_URI (oauth_client=LOOKER): fetch is not implemented for partner OAuth` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#required-parameters-all-oauth-clients))
- [ ] `enabled` fails L3, L4, L5: `snowcap fetch crashes: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_R02_ENABLED (oauth_client=LOOKER): fetch is not implemented for partner OAuth` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#optional-parameters-all-oauth-clients))
- [ ] `oauth_issue_refresh_tokens` fails L3, L4, L5: `snowcap fetch crashes: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_R03_OAUTH_ISSUE_REFRESH_TOKENS (oauth_client=LOOKER): fetch is not implemented for partner OAuth` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] `oauth_refresh_token_validity` fails L3, L4, L5: `snowcap fetch crashes: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_R04_OAUTH_REFRESH_TOKEN_VALIDITY (oauth_client=LOOKER): fetch is not implemented for partner OAuth` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] `comment` fails L3, L4, L5: `snowcap fetch crashes: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_R05_COMMENT (oauth_client=LOOKER): fetch is not implemented for partner OAuth` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] Combined change of `comment`, `enabled`, `oauth_issue_refresh_tokens`, `oauth_redirect_uri`, `oauth_refresh_token_validity` in one plan: `Crashed: Exception: snowcap cannot read back partner OAuth integration Resource:<db>_COMBINED (oauth_client=LOOKER): fetch is not implemented for partner OAuth`
- [ ] Model `oauth_access_token_validity`: `OAUTH_ACCESS_TOKEN_VALIDITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] Model `oauth_single_use_refresh_tokens_required`: `OAUTH_SINGLE_USE_REFRESH_TOKENS_REQUIRED` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#optional-parameters-all-oauth-clients))
- [ ] Model `oauth_use_secondary_roles`: `OAUTH_USE_SECONDARY_ROLES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] Model `allowed_roles_list`: `ALLOWED_ROLES_LIST` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] Model `blocked_roles_list`: `BLOCKED_ROLES_LIST` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#additional-optional-parameters-partner-applications))
- [ ] Model `network_policy`: `NETWORK_POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#optional-parameters-all-oauth-clients))
- [ ] Model `use_privatelink_for_authorization_endpoint`: `USE_PRIVATELINK_FOR_AUTHORIZATION_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-security-integration-oauth-snowflake#optional-parameters-all-oauth-clients))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-security-integration-oauth-snowflake#syntax))

Modeled, without a live sample: `type`, `oauth_client_secret`, `owner`

### SEQUENCE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 1 fail, 1 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-sequence) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-sequence) [show](https://docs.snowflake.com/en/sql-reference/sql/show-sequences) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-sequence)

- [ ] `start` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001420 (22023): SQL compilation error: invalid property 'SEQUENCE_START' for 'SEQUENCE' on: ALTER SEQUENCE <db>.PUBLIC.R00_START SET START 10` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-sequence#optional-parameters))
  - snowcap sent: `ALTER SEQUENCE <db>.PUBLIC.R00_START SET START 10`
- Combined change: untested (needs two or more fields)
- [ ] Model `order`: `{ ORDER | NOORDER }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-sequence#parameters))
- [x] `increment`
- [x] `comment`

Modeled, without a live sample: `owner`

### SERVICE

Docs only: cost: the audit scope skips Snowpark Container Services objects (compute pool nodes consume credits). Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-service) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-service) [show](https://docs.snowflake.com/en/sql-reference/sql/show-services) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-service)

- [ ] Model `specification_template`: `FROM { SPECIFICATION_TEMPLATE_FILE = '<path>' | @<stage> SPECIFICATION_TEMPLATE_FILE = '<path>' | SPECIFICATION_TEMPLATE <specification_text> }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-service#parameters))
- [ ] Model `using[]`: `USING ( <key> => <value> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-service#required-parameters))
- [ ] Model `min_ready_instances`: `MIN_READY_INSTANCES` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-service#parameters))
- [ ] Model `auto_suspend_secs` (preview): `AUTO_SUSPEND_SECS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-service#parameters))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-event-level))
- [ ] Model `service_caller_token_validity_secs`: `SERVICE_CALLER_TOKEN_VALIDITY_SECS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters))
- [ ] Model `state`: `{ SUSPEND | RESUME }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-service#parameters))
- [ ] Model `owner`: `OWNERSHIP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/grant-ownership))

Modeled, without a live sample: `compute_pool`, `specification`, `min_instances`, `max_instances`, `auto_resume`, `external_access_integrations[]`, `query_warehouse`, `comment`, `tags`

### SHARE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 1 pass, 0 fail, 6 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-share) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-share) [show](https://docs.snowflake.com/en/sql-reference/sql/show-shares) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-share)

- Combined change: untested (needs two or more fields)
- [ ] Model `secure_objects_only`: `SECURE_OBJECTS_ONLY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-share#optional-parameters))
- [ ] Model `accounts`: `ACCOUNTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-share#parameters))
- [ ] Model `accounts[]`: `ADD | REMOVE ACCOUNTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-share#parameters))
- [ ] Model `share_restrictions`: `SHARE_RESTRICTIONS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-share#parameters))
- [ ] Model `objects_granted`: `GRANT <privilege> ON <object> TO SHARE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/grant-privilege-share))
- [ ] Model `tags`: `SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-share#syntax))
- [x] `comment`

Modeled, without a live sample: `owner`

### STAGE:external_azure

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 4 pass, 4 fail, 10 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stage) [show](https://docs.snowflake.com/en/sql-reference/sql/show-stages) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stage)

- [ ] `encryption.type` fails L3, L4, L5: `declared 'AZURE_CSE', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `encryption.master_key` fails L3, L4, L5: `declared '{secret:master_key_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `directory.refresh_on_create` fails L3, L4, L5: `declared False, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
  - snowcap sent: `ALTER STAGE <db>.PUBLIC.R05_DIRECTORY_REFRESH_ON_CREATE SET directory = (ENABLE = TRUE REFRESH_ON_CREATE = TRUE)`
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=STAGE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=STAGE SET`
- [x] Combined change of `comment`, `storage_integration`, `url` in one plan
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `credentials.azure_sas_token`: `CREDENTIALS = ( AZURE_SAS_TOKEN = '<string>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT = { TRUE | FALSE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] Model `directory.auto_refresh`: `DIRECTORY = ( AUTO_REFRESH = { TRUE | FALSE } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `directory.notification_integration`: `DIRECTORY = ( NOTIFICATION_INTEGRATION = '<notification_integration_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_name`: `FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] Model `file_format.type`: `FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_type_options[]`: `formatTypeOptions` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions))
- [ ] Model `copy_options`: `COPY_OPTIONS = ( ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [x] `url`
- [x] `storage_integration`
- [x] `directory.enable`
- [x] `comment`

Modeled, without a live sample: `owner`

### STAGE:external_gcs

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 4 pass, 4 fail, 9 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stage) [show](https://docs.snowflake.com/en/sql-reference/sql/show-stages) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stage)

- [ ] `encryption.type` fails L3, L4, L5: `declared 'GCS_SSE_KMS', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `encryption.kms_key_id` fails L3, L4, L5: `declared 'coverage-kms-key1', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `directory.refresh_on_create` fails L3, L4, L5: `declared False, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
  - snowcap sent: `ALTER STAGE <db>.PUBLIC.R05_DIRECTORY_REFRESH_ON_CREATE SET directory = (ENABLE = TRUE REFRESH_ON_CREATE = TRUE)`
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=STAGE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=STAGE SET`
- [x] Combined change of `comment`, `storage_integration`, `url` in one plan
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT = { TRUE | FALSE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] Model `directory.auto_refresh`: `DIRECTORY = ( AUTO_REFRESH = { TRUE | FALSE } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `directory.notification_integration`: `DIRECTORY = ( NOTIFICATION_INTEGRATION = '<notification_integration_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_name`: `FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] Model `file_format.type`: `FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_type_options[]`: `formatTypeOptions` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions))
- [ ] Model `copy_options`: `COPY_OPTIONS = ( ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [x] `url`
- [x] `storage_integration`
- [x] `directory.enable`
- [x] `comment`

Modeled, without a live sample: `owner`

### STAGE:external_onelake

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 4 pass, 4 fail, 7 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stage) [show](https://docs.snowflake.com/en/sql-reference/sql/show-stages) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stage)

- [ ] `encryption.type` fails L3, L4, L5: `declared 'AZURE_CSE', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `encryption.master_key` fails L3, L4, L5: `declared '{secret:master_key_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `directory.refresh_on_create` fails L3, L4, L5: `declared False, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
  - snowcap sent: `ALTER STAGE <db>.PUBLIC.R05_DIRECTORY_REFRESH_ON_CREATE SET directory = (ENABLE = TRUE REFRESH_ON_CREATE = TRUE)`
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=STAGE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R07_TAGS?domain=STAGE SET`
- Combined change: untested (needs two or more fields)
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `credentials.azure_sas_token`: `CREDENTIALS = ( AZURE_SAS_TOKEN = '<string>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] Model `file_format.format_name`: `FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] Model `file_format.type`: `FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_type_options[]`: `formatTypeOptions` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions))
- [ ] Model `copy_options`: `COPY_OPTIONS = ( ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [x] `url`
- [x] `storage_integration`
- [x] `directory.enable`
- [x] `comment`

Modeled, without a live sample: `owner`

### STAGE:external_s3

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 4 pass, 9 fail, 10 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stage) [show](https://docs.snowflake.com/en/sql-reference/sql/show-stages) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stage)

- [ ] `credentials.aws_key_id` fails L3, L4, L5: `declared '{secret:aws_key_id_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `credentials.aws_secret_key` fails L3, L4, L5: `declared '{secret:aws_secret_key_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `credentials.aws_token` fails L3, L4, L5: `declared '{secret:aws_token_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `credentials.aws_role` fails L3, L4, L5: `declared 'arn:aws:iam::001234567890:role/coverage', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `encryption.type` fails L3, L4, L5: `declared 'AWS_SSE_S3', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `encryption.master_key` fails L3, L4, L5: `declared '{secret:master_key_1}', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `encryption.kms_key_id` fails L3, L4, L5: `declared 'coverage-kms-key1', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] `directory.refresh_on_create` fails L3, L4, L5: `declared False, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
  - snowcap sent: `ALTER STAGE <db>.PUBLIC.R10_DIRECTORY_REFRESH_ON_CREATE SET directory = (ENABLE = TRUE REFRESH_ON_CREATE = TRUE)`
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R12_TAGS?domain=STAGE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R12_TAGS?domain=STAGE SET`
- [x] Combined change of `comment`, `storage_integration`, `url` in one plan
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `aws_access_point_arn`: `AWS_ACCESS_POINT_ARN = '<string>'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT = { TRUE | FALSE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#external-stage-parameters-externalstageparams))
- [ ] Model `directory.auto_refresh`: `DIRECTORY = ( AUTO_REFRESH = { TRUE | FALSE } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `directory.aws_sns_topic`: `DIRECTORY = ( AWS_SNS_TOPIC = '<sns_topic_arn>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_name`: `FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] Model `file_format.type`: `FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_type_options[]`: `formatTypeOptions` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions))
- [ ] Model `copy_options`: `COPY_OPTIONS = ( ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [x] `url`
- [x] `storage_integration`
- [x] `directory.enable`
- [x] `comment`

Modeled, without a live sample: `owner`

### STAGE:external_s3compat

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 0 pass, 6 fail, 7 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stage) [show](https://docs.snowflake.com/en/sql-reference/sql/show-stages) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stage)

- [ ] `url` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): ENDPOINT on: CREATE STAGE <db>.PUBLIC.R00_URL URL = $$s3compat://coverage-audit-bucket/path/$$ credentials = (AWS_KE ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] `credentials.aws_key_id` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): ENDPOINT on: CREATE STAGE <db>.PUBLIC.R01_CREDENTIALS_AWS_KEY_ID URL = $$s3compat://coverage-audit-bucket/path/$$ cr ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] `credentials.aws_secret_key` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): ENDPOINT on: CREATE STAGE <db>.PUBLIC.R02_CREDENTIALS_AWS_SECRET_KEY URL = $$s3compat://coverage-audit-bucket/path/$ ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] `directory.enable` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): ENDPOINT on: CREATE STAGE <db>.PUBLIC.R03_DIRECTORY_ENABLE URL = $$s3compat://coverage-audit-bucket/path/$$ credenti ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage#directory-table-parameters))
- [ ] `comment` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): ENDPOINT on: CREATE STAGE <db>.PUBLIC.R04_COMMENT URL = $$s3compat://coverage-audit-bucket/path/$$ credentials = (AW ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] `tags` fails L2: `create failed: ProgrammingError: 002029 (42601): SQL compilation error: Missing option(s): ENDPOINT on: CREATE STAGE <db>.PUBLIC.R05_TAGS URL = $$s3compat://coverage-audit-bucket/path/$$ credentials = (AWS_K ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- Combined change: untested (needs two or more fields)
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `endpoint`: `ENDPOINT = '<s3_api_compatible_endpoint>'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_name`: `FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] Model `file_format.type`: `FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_type_options[]`: `formatTypeOptions` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions))
- [ ] Model `copy_options`: `COPY_OPTIONS = ( ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))

Modeled, without a live sample: `owner`

### STAGE:internal

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 2 fail, 7 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stage) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stage) [show](https://docs.snowflake.com/en/sql-reference/sql/show-stages) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stage)

- [ ] `encryption.type` fails L3, L4, L5: `declared 'SNOWFLAKE_SSE', snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#internal-stage-parameters-internalstageparams))
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R03_TAGS?domain=STAGE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R03_TAGS?domain=STAGE SET`
- Combined change: untested (needs two or more fields)
- [ ] Model `temporary`: `TEMP | TEMPORARY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `directory.auto_refresh` (preview): `DIRECTORY = ( AUTO_REFRESH = { TRUE | FALSE } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_name`: `FILE_FORMAT = ( FORMAT_NAME = '<file_format_name>' )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [ ] Model `file_format.type`: `FILE_FORMAT = ( TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML | CUSTOM } )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `file_format.format_type_options[]`: `formatTypeOptions` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage#format-type-options-formattypeoptions))
- [ ] Model `copy_options`: `COPY_OPTIONS = ( ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stage))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stage))
- [x] `directory.enable`
- [x] `comment`

Modeled, without a live sample: `owner`

### STORAGE INTEGRATION:azure

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 4 pass, 1 fail, 2 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-storage-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [ ] `storage_blocked_locations[]` fails L3, L5: `Snowflake reports storage_blocked_locations="['azure://coverageplaceholder.blob.core.windows.net/a/x/']" (declared form ['azure://coverageplaceholder.blob.core.windows.net/a/x/']), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration#optional-parameters))
- [x] Combined change of `azure_tenant_id`, `comment`, `enabled`, `storage_allowed_locations`, `storage_blocked_locations` in one plan
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration#cloud-provider-parameters-cloudproviderparams))
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-storage-integration#parameters))
- [x] `azure_tenant_id`
- [x] `enabled`
- [x] `storage_allowed_locations[]`
- [x] `comment`

Modeled, without a live sample: `type`, `storage_provider`, `owner`

### STORAGE INTEGRATION:gcs

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 4 pass, 0 fail, 1 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-storage-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [x] Combined change of `comment`, `enabled`, `storage_allowed_locations`, `storage_blocked_locations` in one plan
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-storage-integration#parameters))
- [x] `enabled`
- [x] `storage_allowed_locations[]`
- [x] `storage_blocked_locations[]`
- [x] `comment`

Modeled, without a live sample: `type`, `storage_provider`, `owner`

### STORAGE INTEGRATION:s3

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 6 pass, 0 fail, 3 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-storage-integration) [show](https://docs.snowflake.com/en/sql-reference/sql/show-integrations) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-integration)

- [x] Combined change of `comment`, `enabled`, `storage_allowed_locations`, `storage_aws_object_acl`, `storage_aws_role_arn`, `storage_blocked_locations` in one plan
- [ ] Model `storage_aws_external_id`: `STORAGE_AWS_EXTERNAL_ID` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration#cloud-provider-parameters-cloudproviderparams))
- [ ] Model `use_privatelink_endpoint`: `USE_PRIVATELINK_ENDPOINT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration#cloud-provider-parameters-cloudproviderparams))
- [ ] Model `tags`: `SET TAG / UNSET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-storage-integration#parameters))
- [x] `storage_aws_role_arn`
- [x] `storage_aws_object_acl`
- [x] `enabled`
- [x] `storage_allowed_locations[]`
- [x] `storage_blocked_locations[]`
- [x] `comment`

Modeled, without a live sample: `type`, `storage_provider`, `owner`

### STREAM:directory_table

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 0 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stream) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stream) [show](https://docs.snowflake.com/en/sql-reference/sql/show-streams) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stream)

- Combined change: untested (needs two or more fields)
- [ ] Model `tags`: `[ WITH ] TAG ( <tag_name> = '<tag_value>' [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stream#parameters))
- [x] `on_stage`
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `owner`

### STREAM:dynamic_table

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 0 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stream) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stream) [show](https://docs.snowflake.com/en/sql-reference/sql/show-streams) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stream)

- Combined change: untested (needs two or more fields)
- [ ] Model `tags`: `[ WITH ] TAG ( <tag_name> = '<tag_value>' [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stream#parameters))
- [x] `on_dynamic_table`
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `owner`

### STREAM:table

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 2 fail, 2 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stream) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stream) [show](https://docs.snowflake.com/en/sql-reference/sql/show-streams) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stream)

- [ ] `at_before` fails L3, L4, L5: `declared {'offset': '-1'}, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream#optional-parameters))
- [ ] `show_initial_rows` fails L3, L4, L5: `declared True, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream#optional-parameters))
- Combined change: untested (needs two or more fields)
- [ ] Model `insert_only`: `INSERT_ONLY = TRUE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream#optional-parameters))
- [ ] Model `tags`: `[ WITH ] TAG ( <tag_name> = '<tag_value>' [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stream#parameters))
- [x] `on_table`
- [x] `append_only`
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `owner`

### STREAM:view

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 3 pass, 2 fail, 1 not modeled, 2 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-stream) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-stream) [show](https://docs.snowflake.com/en/sql-reference/sql/show-streams) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-stream)

- [ ] `at_before` fails L3, L4, L5: `declared {'offset': '-1'}, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream#optional-parameters))
- [ ] `show_initial_rows` fails L3, L4, L5: `declared True, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-stream#optional-parameters))
- Combined change: untested (needs two or more fields)
- [ ] Model `tags`: `[ WITH ] TAG ( <tag_name> = '<tag_value>' [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-stream#parameters))
- [x] `on_view`
- [x] `append_only`
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `owner`

### STREAMLIT

Tested live at snowcap `584e44b` on 2026-10-17, Snowflake 10.37.102. 0 pass, 5 fail, 6 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit) [show](https://docs.snowflake.com/en/sql-reference/sql/show-streamlits) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-streamlit)

- [ ] `from` fails L2: `create failed: ProgrammingError: 002003 (02000): SQL compilation error: COMPUTE_POOL 'SYSTEM_COMPUTE_POOL_CPU' does not exist or not authorized.{2} on: CREATE STREAMLIT <db>.PUBLIC.R00_FROM FROM @<db>.PUBLIC.COVERAGE_STG1 MAIN_FILE = $$coverage_a ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit))
- [ ] `main_file` fails L2: `create failed: ProgrammingError: 002003 (02000): SQL compilation error: COMPUTE_POOL 'SYSTEM_COMPUTE_POOL_CPU' does not exist or not authorized.{2} on: CREATE STREAMLIT <db>.PUBLIC.R01_MAIN_FILE FROM @<db>.PUBLIC.COVERAGE_STG1 MAIN_FILE = $$cover ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] `query_warehouse` fails L2: `create failed: ProgrammingError: 002003 (02000): SQL compilation error: COMPUTE_POOL 'SYSTEM_COMPUTE_POOL_CPU' does not exist or not authorized.{2} on: CREATE STREAMLIT <db>.PUBLIC.R02_QUERY_WAREHOUSE FROM @<db>.PUBLIC.COVERAGE_STG1 MAIN_FILE = $ ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] `comment` fails L2: `create failed: ProgrammingError: 002003 (02000): SQL compilation error: COMPUTE_POOL 'SYSTEM_COMPUTE_POOL_CPU' does not exist or not authorized.{2} on: CREATE STREAMLIT <db>.PUBLIC.R03_COMMENT FROM @<db>.PUBLIC.COVERAGE_STG1 MAIN_FILE = $$coverag ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] `title` fails L2: `create failed: ProgrammingError: 002003 (02000): SQL compilation error: COMPUTE_POOL 'SYSTEM_COMPUTE_POOL_CPU' does not exist or not authorized.{2} on: CREATE STREAMLIT <db>.PUBLIC.R04_TITLE FROM @<db>.PUBLIC.COVERAGE_STG1 MAIN_FILE = $$coverage_ ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] Combined change of `comment`, `main_file`, `query_warehouse`, `title` in one plan: `ProgrammingError: 002003 (02000): SQL compilation error: COMPUTE_POOL 'SYSTEM_COMPUTE_POOL_CPU' does not exist or not authorized.{2} on: CREATE STREAMLIT <db>.PUBLIC.COMBINED FROM @<db>.PUBLIC.COVERAGE_STG1 MAIN_FILE = $$coverage_a ...`
- [ ] Model `root_location`: `ROOT_LOCATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] Model `runtime_name`: `RUNTIME_NAME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] Model `compute_pool`: `COMPUTE_POOL` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] Model `imports[]`: `IMPORTS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] Model `external_access_integrations`: `EXTERNAL_ACCESS_INTEGRATIONS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))
- [ ] Model `secrets`: `SECRETS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-streamlit))

Modeled, without a live sample: `owner`

### TABLE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 5 pass, 20 fail, 29 not modeled, 0 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-table) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-table) [show](https://docs.snowflake.com/en/sql-reference/sql/show-tables) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-table)

- [ ] `table_type` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 61 unexpected '<EOF>'. on: ALTER TABLE <db>.PUBLIC.R00_TABLE_TYPE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#optional-parameters))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R00_TABLE_TYPE SET`
- [ ] `columns[].type` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(38,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(20)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column))
- [ ] `columns[].not_null` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(38,0)', 'not_null': False}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column))
- [ ] `columns[].collation` fails L2, L4, L5: `declared [{'name': 'ID', 'data_type': 'NUMBER(38,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)', 'collate': 'en-ci'}], Snowflake reports name_collation=None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#optional-parameters))
  - snowcap sent: `CREATE TABLE <db>.PUBLIC.R03_COLUMNS___COLLATION (ID NUMBER(38,0), NAME VARCHAR(16777216)) ENABLE_SCHEMA_EVOLUTION = FALSE CHANGE_TRACKING = FALSE`
- [ ] `columns[].default` fails L2: `create failed: ProgrammingError: 002262 (22000): SQL compilation error: Default value data type does not match data type for column ID on: CREATE TABLE <db>.PUBLIC.R04_COLUMNS___DEFAULT (ID NUMBER(38,0) DEFAULT '<db>.PUBLIC.COVERAGE_SEQ1.NEX ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column))
- [ ] `columns[].comment` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(38,0)', 'comment': 'b'}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table-column))
- [ ] `columns[].tags` fails L2, L4, L5: `declared [{'name': 'ID', 'data_type': 'NUMBER(38,0)', 'tags': {'<db>.PUBLIC.COVERAGE_TAG': 'a'}}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}], Snowflake reports id_tag=None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
  - snowcap sent: `CREATE TABLE <db>.PUBLIC.R06_COLUMNS___TAGS (ID NUMBER(38,0), NAME VARCHAR(16777216)) ENABLE_SCHEMA_EVOLUTION = FALSE CHANGE_TRACKING = FALSE`
- [ ] `columns[].add` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(38,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}, {'name': 'EXTRA', 'data_type': 'NUMBER(38,0)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#syntax))
- [ ] `columns[].drop` fails L4, L5: `a change to [{'name': 'ID', 'data_type': 'NUMBER(38,0)'}, {'name': 'NAME', 'data_type': 'VARCHAR(16777216)'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#syntax))
- [ ] `constraints[].name` fails L3, L4, L5: `declared ['CONSTRAINT C1 PRIMARY KEY (ID)'], snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#constraint-actions-constraintaction))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R09_CONSTRAINTS___NAME SET`
- [ ] `constraints[].type` fails L3, L4, L5: `declared ['CONSTRAINT C1 PRIMARY KEY (ID)'], snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R10_CONSTRAINTS___TYPE SET`
- [ ] `constraints[].columns` fails L3, L4, L5: `declared ['CONSTRAINT C1 UNIQUE (ID)'], snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R11_CONSTRAINTS___COLUMNS SET`
- [ ] `constraints[].referenced_table` fails L3, L4, L5: `declared ['CONSTRAINT C1 FOREIGN KEY (ID) REFERENCES <db>.PUBLIC.COVERAGE_REF1 (ID)'], snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R12_CONSTRAINTS___REFERENCED_TABLE SET`
- [ ] `constraints[].referenced_columns` fails L3, L4, L5: `declared ['CONSTRAINT C1 FOREIGN KEY (ID) REFERENCES <db>.PUBLIC.COVERAGE_REF1 (ID)'], snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R13_CONSTRAINTS___REFERENCED_COLUMNS SET`
- [ ] `constraints[].expression` fails L3, L4, L5: `declared ['CONSTRAINT C1 CHECK (ID > 0)'], snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-check-constraints-only))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R14_CONSTRAINTS___EXPRESSION SET`
- [ ] `cluster_by` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 70 unexpected 'BY'. syntax error line 1 at position 78 unexpected ')'. on: ALTER TABLE <db>.PUBLIC.R15_CLUSTER_BY SET CLUSTER BY (NAME)` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#clustering-actions-clusteringaction))
  - snowcap sent: `ALTER TABLE <db>.PUBLIC.R15_CLUSTER_BY SET CLUSTER BY (NAME)`
- [ ] `data_retention_time_in_days` fails L3, L5: `Snowflake reports retention_time='3' (declared form 3), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-data-retention-time-in-days))
- [ ] `max_data_extension_time_in_days` fails L3, L5: `Snowflake reports max_data_extension_time_in_days='10' (declared form 10), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-max-data-extension-time-in-days))
- [ ] `tags` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 20 unexpected '<db>'. on: ALTER TAG REFERENCE <db>.PUBLIC.R22_TAGS?domain=TABLE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
  - snowcap sent: `ALTER TAG REFERENCE <db>.PUBLIC.R22_TAGS?domain=TABLE SET`
- [ ] `row_access_policy` fails L2, L4, L5: `declared {'<db>.PUBLIC.COVERAGE_RAP1': ['ID']}, Snowflake reports row_access_policy=None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
  - snowcap sent: `CREATE TABLE <db>.PUBLIC.R23_ROW_ACCESS_POLICY (ID NUMBER(38,0), NAME VARCHAR(16777216)) ENABLE_SCHEMA_EVOLUTION = FALSE CHANGE_TRACKING = FALSE`
- [x] Combined change of `change_tracking`, `comment`, `data_retention_time_in_days`, `default_ddl_collation`, `enable_schema_evolution`, `max_data_extension_time_in_days` in one plan
- [ ] Model `read_only`: `READ ONLY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#optional-parameters))
- [ ] Model `columns[].generated_expression`: `[ GENERATED ALWAYS ] AS ( <expr> ) [ VIRTUAL ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#syntax))
- [ ] Model `columns[].autoincrement`: `AUTOINCREMENT | IDENTITY [ ( <start>, <step> ) | START <n> INCREMENT <n> ] [ ORDER | NOORDER ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#syntax))
- [ ] Model `columns[].masking_policy`: `[ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name>, <condition_col> ... ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].projection_policy`: `[ WITH ] PROJECTION POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `constraints[].enforced`: `[ NOT ] ENFORCED` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-all-constraints))
- [ ] Model `constraints[].deferrable`: `{ DEFERRABLE | NOT DEFERRABLE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-all-constraints))
- [ ] Model `constraints[].initially`: `INITIALLY { DEFERRED | IMMEDIATE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-all-constraints))
- [ ] Model `constraints[].enabled`: `{ ENABLE | DISABLE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-all-constraints))
- [ ] Model `constraints[].validate`: `{ VALIDATE | NOVALIDATE }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-all-constraints))
- [ ] Model `constraints[].rely`: `{ RELY | NORELY }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-all-constraints))
- [ ] Model `constraints[].match`: `MATCH { FULL | SIMPLE | PARTIAL }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-foreign-key-constraints-only))
- [ ] Model `constraints[].on_update`: `ON UPDATE { CASCADE | SET NULL | SET DEFAULT | RESTRICT | NO ACTION }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-foreign-key-constraints-only))
- [ ] Model `constraints[].on_delete`: `ON DELETE { CASCADE | SET NULL | SET DEFAULT | RESTRICT | NO ACTION }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#properties-for-foreign-key-constraints-only))
- [ ] Model `constraints[].comment`: `COMMENT '<string_literal>'` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table-constraint#comments-on-constraints))
- [ ] Model `reclustering`: `{ SUSPEND | RESUME } RECLUSTER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#clustering-actions-clusteringaction))
- [ ] Model `search_optimization`: `ADD SEARCH OPTIMIZATION [ ON <search_method_with_target> [ , ... ] ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#search-optimization-actions-searchoptimizationaction))
- [ ] Model `error_logging`: `ERROR_LOGGING` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#optional-parameters))
- [ ] Model `row_timestamp`: `ROW_TIMESTAMP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#optional-parameters))
- [ ] Model `data_metric_schedule`: `DATA_METRIC_SCHEDULE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-data-metric-schedule))
- [ ] Model `data_metric_functions`: `ADD | DROP | MODIFY DATA METRIC FUNCTION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].anomaly_detection` (preview): `ANOMALY_DETECTION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].sensitivity` (preview): `SENSITIVITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].data_quality_notification` (preview): `DATA_QUALITY_NOTIFICATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `contacts`: `WITH CONTACT ( <purpose> = <contact_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#syntax))
- [ ] Model `aggregation_policy`: `WITH AGGREGATION POLICY <policy_name> [ ENTITY KEY ( <col_name> [ , ... ] ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `join_policy`: `WITH JOIN POLICY <policy_name> [ ALLOWED JOIN KEYS ( <col_name> [ , ... ] ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `storage_lifecycle_policy`: `WITH STORAGE LIFECYCLE POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-table#parameters))
- [ ] Model `tags_copy`: `COPY TAGS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-table#optional-parameters))
- [x] `change_tracking`
- [x] `default_ddl_collation`
- [x] `enable_schema_evolution`
- [x] `comment`
- [x] `ownership`

### TAG

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 6 pass, 0 fail, 3 not modeled, 1 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-tag) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-tag) [show](https://docs.snowflake.com/en/sql-reference/sql/show-tags) [describe](None)

- [ ] Combined change of `allowed_values`, `comment`, `on_conflict`, `propagate` in one plan: `ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 80 unexpected 'propagate'. on: ALTER TAG <db>.PUBLIC.COMBINED SET ALLOWED_VALUES $$a$$, $$c$$ propagate = ON_DEPENDENCY_AND_DATA_MOVEMENT ON_CONFLICT = $$ ...`
- [ ] Model `multi_value`: `MULTI_VALUE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-tag))
- [ ] Model `data_movement_policy`: `SET DATA MOVEMENT POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-tag))
- [ ] Model `unset_dcm_project`: `UNSET DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-tag))
- [x] `allowed_values[]`
- [x] `add_allowed_values`
- [x] `drop_allowed_values`
- [x] `propagate`
- [x] `on_conflict`
- [x] `comment`

Modeled, without a live sample: `owner`

### TASK

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 15 pass, 2 fail, 8 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-task) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-task) [show](https://docs.snowflake.com/en/sql-reference/sql/show-tasks) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-task)

- [ ] `finalize` fails L2: `create failed: ProgrammingError: 091449 (55000): Cannot set parameter SUSPEND_TASK_AFTER_NUM_FAILURES on non-root task R08_FINALIZE. Non-root tasks use the parameter setting from their root task. Please set this on the root task for this task instead. on: CREATE TASK <db>.PUBLIC.R08_FINALIZE user_ta ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-task))
- [ ] `after[]` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 63 unexpected '<db>'. syntax error line 1 at position 131 unexpected '.'. syntax error line 1 at position 138 unexpected '.'. syntax error line 1 at position 153 unexpected '<EOF>'. on ...` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-task))
  - snowcap sent: `ALTER TASK <db>.PUBLIC.R14_AFTER__ SET AFTER <db>.PUBLIC.COVERAGE_PRED1, <db>.PUBLIC.COVERAGE_PRED2`
- [x] Combined change of `comment`, `config`, `error_integration`, `schedule`, `serverless_task_max_statement_size`, `serverless_task_min_statement_size`, `success_integration`, `suspend_task_after_num_failures`, `target_completion_interval`, `task_auto_retry_attempts`, `user_task_managed_initial_warehouse_size`, `user_task_minimum_trigger_interval_in_seconds`, `user_task_timeout_ms` in one plan
- [ ] Model `overlap_policy`: `OVERLAP_POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-task))
- [ ] Model `session_parameters[]`: `<session_parameter> = <value>` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-session-parameters))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-event-level))
- [ ] Model `execute_as_user`: `EXECUTE AS USER` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-task))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-task))
- [ ] Model `tags`: `WITH TAG / SET TAG` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-task))
- [ ] Model `contacts`: `WITH CONTACT / CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-task))
- [x] `user_task_managed_initial_warehouse_size`
- [x] `schedule`
- [x] `config`
- [x] `user_task_timeout_ms`
- [x] `suspend_task_after_num_failures`
- [x] `error_integration`
- [x] `success_integration`
- [x] `comment`
- [x] `task_auto_retry_attempts`
- [x] `user_task_minimum_trigger_interval_in_seconds`
- [x] `target_completion_interval`
- [x] `serverless_task_min_statement_size`
- [x] `serverless_task_max_statement_size`
- [x] `when`
- [x] `definition`

Modeled, without a live sample: `state`, `warehouse`, `owner`

### USER

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 0 pass, 16 fail, 98 not modeled, 13 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-user) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-user) [show](https://docs.snowflake.com/en/sql-reference/sql/show-users) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-user)

- [ ] `login_name` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `display_name` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `first_name` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `middle_name` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `last_name` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `email` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `disabled` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `allowed_interfaces[]` fails L3, L5: `Snowflake reports allowed_interfaces_list="['STREAMLIT']" (declared form ['STREAMLIT']), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `days_to_expiry` fails L3, L4, L5: `Snowflake reports days_to_expiry_ceil='10' (declared form 10), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `mins_to_unlock` fails L3, L4, L5: `Snowflake reports mins_to_unlock_ceil='10' (declared form 10), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] `default_warehouse` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `default_namespace` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `default_role` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `default_secondary_roles` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `comment` fails L5: `plan after create: ["update ['type']"]; plan after change: ["update ['type']"]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] `tags` fails L3, L5: `declared {'<db>.PUBLIC.COVERAGE_TAG': 'a'}, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Combined change of `allowed_interfaces`, `comment`, `days_to_expiry`, `default_namespace`, `default_role`, `default_secondary_roles`, `default_warehouse`, `disabled`, `display_name`, `email`, `first_name`, `last_name`, `login_name`, `middle_name`, `mins_to_unlock` in one plan: `missed days_to_expiry, mins_to_unlock`
- [ ] Model `disable_mfa`: `DISABLE_MFA` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `rsa_public_key_fp`: `RSA_PUBLIC_KEY_FP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] Model `rsa_public_key_2_fp`: `RSA_PUBLIC_KEY_2_FP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-user))
- [ ] Model `delegated_authorizations[]`: `DELEGATED AUTHORIZATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `default_mfa_method`: `DEFAULT_MFA_METHOD` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `mfa_methods[]`: `REMOVE MFA METHOD` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `mfa_methods[].comment`: `MODIFY MFA METHOD ... SET COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `mfa_otp`: `ADD MFA METHOD OTP` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `programmatic_access_tokens[]`: `ADD PROGRAMMATIC ACCESS TOKEN` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-add-programmatic-access-token))
- [ ] Model `programmatic_access_tokens[].role_restriction`: `ROLE_RESTRICTION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-add-programmatic-access-token))
- [ ] Model `programmatic_access_tokens[].days_to_expiry`: `DAYS_TO_EXPIRY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-add-programmatic-access-token))
- [ ] Model `programmatic_access_tokens[].name`: `MODIFY PAT ... RENAME TO` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-modify-programmatic-access-token))
- [ ] Model `programmatic_access_tokens[].disabled`: `DISABLED` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-modify-programmatic-access-token))
- [ ] Model `programmatic_access_tokens[].mins_to_bypass_network_policy_requirement`: `MINS_TO_BYPASS_NETWORK_POLICY_REQUIREMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-modify-programmatic-access-token))
- [ ] Model `programmatic_access_tokens[].comment`: `COMMENT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user-modify-programmatic-access-token))
- [ ] Model `authentication_policy`: `AUTHENTICATION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `password_policy`: `PASSWORD POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `session_policy`: `SESSION POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-user))
- [ ] Model `enable_unredacted_query_syntax_error`: `ENABLE_UNREDACTED_QUERY_SYNTAX_ERROR` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-enable-unredacted-query-syntax-error))
- [ ] Model `enable_unredacted_secure_object_error`: `ENABLE_UNREDACTED_SECURE_OBJECT_ERROR` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-enable-unredacted-secure-object-error))
- [ ] Model `prevent_unload_to_inline_url`: `PREVENT_UNLOAD_TO_INLINE_URL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-prevent-unload-to-inline-url))
- [ ] Model `prevent_unload_to_internal_stages`: `PREVENT_UNLOAD_TO_INTERNAL_STAGES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-prevent-unload-to-internal-stages))
- [ ] Model `enable_notebook_creation_in_personal_db`: `ENABLE_NOTEBOOK_CREATION_IN_PERSONAL_DB` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-enable-notebook-creation-in-personal-db))
- [ ] Model `cortex_code_cli_daily_est_credit_limit_per_user`: `CORTEX_CODE_CLI_DAILY_EST_CREDIT_LIMIT_PER_USER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-cortex-code-cli-daily-est-credit-limit-per-user))
- [ ] Model `cortex_code_desktop_daily_est_credit_limit_per_user`: `CORTEX_CODE_DESKTOP_DAILY_EST_CREDIT_LIMIT_PER_USER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-cortex-code-desktop-daily-est-credit-limit-per-user))
- [ ] Model `cortex_code_snowsight_daily_est_credit_limit_per_user`: `CORTEX_CODE_SNOWSIGHT_DAILY_EST_CREDIT_LIMIT_PER_USER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-cortex-code-snowsight-daily-est-credit-limit-per-user))
- [ ] Model `disable_ui_download_button`: `DISABLE_UI_DOWNLOAD_BUTTON` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-disable-ui-download-button))
- [ ] Model `log_level`: `LOG_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-level))
- [ ] Model `log_event_level`: `LOG_EVENT_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-log-event-level))
- [ ] Model `metric_level`: `METRIC_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-metric-level))
- [ ] Model `trace_level`: `TRACE_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-trace-level))
- [ ] Model `quoted_identifiers_ignore_case`: `QUOTED_IDENTIFIERS_IGNORE_CASE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-quoted-identifiers-ignore-case))
- [ ] Model `statement_queued_timeout_in_seconds`: `STATEMENT_QUEUED_TIMEOUT_IN_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-statement-queued-timeout-in-seconds))
- [ ] Model `statement_timeout_in_seconds`: `STATEMENT_TIMEOUT_IN_SECONDS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-statement-timeout-in-seconds))
- [ ] Model `abort_detached_query`: `ABORT_DETACHED_QUERY` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-abort-detached-query))
- [ ] Model `active_python_profiler`: `ACTIVE_PYTHON_PROFILER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-active-python-profiler))
- [ ] Model `autocommit`: `AUTOCOMMIT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-autocommit))
- [ ] Model `binary_input_format`: `BINARY_INPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-binary-input-format))
- [ ] Model `binary_output_format`: `BINARY_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-binary-output-format))
- [ ] Model `client_enable_log_info_statement_parameters`: `CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-enable-log-info-statement-parameters))
- [ ] Model `client_memory_limit`: `CLIENT_MEMORY_LIMIT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-memory-limit))
- [ ] Model `client_metadata_request_use_connection_ctx`: `CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-metadata-request-use-connection-ctx))
- [ ] Model `client_metadata_use_session_database`: `CLIENT_METADATA_USE_SESSION_DATABASE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-metadata-use-session-database))
- [ ] Model `client_prefetch_threads`: `CLIENT_PREFETCH_THREADS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-prefetch-threads))
- [ ] Model `client_result_chunk_size`: `CLIENT_RESULT_CHUNK_SIZE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-result-chunk-size))
- [ ] Model `client_result_column_case_insensitive`: `CLIENT_RESULT_COLUMN_CASE_INSENSITIVE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-result-column-case-insensitive))
- [ ] Model `client_session_keep_alive`: `CLIENT_SESSION_KEEP_ALIVE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-session-keep-alive))
- [ ] Model `client_session_keep_alive_heartbeat_frequency`: `CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-session-keep-alive-heartbeat-frequency))
- [ ] Model `client_timestamp_type_mapping`: `CLIENT_TIMESTAMP_TYPE_MAPPING` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-client-timestamp-type-mapping))
- [ ] Model `csv_timestamp_format`: `CSV_TIMESTAMP_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-csv-timestamp-format))
- [ ] Model `date_input_format`: `DATE_INPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-date-input-format))
- [ ] Model `date_output_format`: `DATE_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-date-output-format))
- [ ] Model `default_null_ordering`: `DEFAULT_NULL_ORDERING` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-default-null-ordering))
- [ ] Model `enable_get_ddl_use_data_type_alias`: `ENABLE_GET_DDL_USE_DATA_TYPE_ALIAS` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-enable-get-ddl-use-data-type-alias))
- [ ] Model `enable_unhandled_exceptions_reporting`: `ENABLE_UNHANDLED_EXCEPTIONS_REPORTING` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-enable-unhandled-exceptions-reporting))
- [ ] Model `enable_unload_physical_type_optimization`: `ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-enable-unload-physical-type-optimization))
- [ ] Model `error_on_nondeterministic_merge`: `ERROR_ON_NONDETERMINISTIC_MERGE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-error-on-nondeterministic-merge))
- [ ] Model `error_on_nondeterministic_update`: `ERROR_ON_NONDETERMINISTIC_UPDATE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-error-on-nondeterministic-update))
- [ ] Model `geography_output_format`: `GEOGRAPHY_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-geography-output-format))
- [ ] Model `geometry_output_format`: `GEOMETRY_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-geometry-output-format))
- [ ] Model `hybrid_table_lock_timeout`: `HYBRID_TABLE_LOCK_TIMEOUT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-hybrid-table-lock-timeout))
- [ ] Model `iceberg_timestamp_default_precision`: `ICEBERG_TIMESTAMP_DEFAULT_PRECISION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-iceberg-timestamp-default-precision))
- [ ] Model `jdbc_enable_put_get`: `JDBC_ENABLE_PUT_GET` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-jdbc-enable-put-get))
- [ ] Model `jdbc_treat_decimal_as_int`: `JDBC_TREAT_DECIMAL_AS_INT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-jdbc-treat-decimal-as-int))
- [ ] Model `jdbc_treat_timestamp_ntz_as_utc`: `JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-jdbc-treat-timestamp-ntz-as-utc))
- [ ] Model `jdbc_use_session_timezone`: `JDBC_USE_SESSION_TIMEZONE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-jdbc-use-session-timezone))
- [ ] Model `json_indent`: `JSON_INDENT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-json-indent))
- [ ] Model `js_treat_integer_as_bigint`: `JS_TREAT_INTEGER_AS_BIGINT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-js-treat-integer-as-bigint))
- [ ] Model `lock_timeout`: `LOCK_TIMEOUT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-lock-timeout))
- [ ] Model `multi_statement_count`: `MULTI_STATEMENT_COUNT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-multi-statement-count))
- [ ] Model `noorder_sequence_as_default`: `NOORDER_SEQUENCE_AS_DEFAULT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-noorder-sequence-as-default))
- [ ] Model `odbc_treat_decimal_as_int`: `ODBC_TREAT_DECIMAL_AS_INT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-odbc-treat-decimal-as-int))
- [ ] Model `opt_out_error_logging`: `OPT_OUT_ERROR_LOGGING` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-opt-out-error-logging))
- [ ] Model `python_profiler_target_stage`: `PYTHON_PROFILER_TARGET_STAGE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-python-profiler-target-stage))
- [ ] Model `python_profiler_modules`: `PYTHON_PROFILER_MODULES` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-python-profiler-modules))
- [ ] Model `query_tag`: `QUERY_TAG` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-query-tag))
- [ ] Model `rows_per_resultset`: `ROWS_PER_RESULTSET` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-rows-per-resultset))
- [ ] Model `s3_stage_vpce_dns_name`: `S3_STAGE_VPCE_DNS_NAME` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-s3-stage-vpce-dns-name))
- [ ] Model `search_path`: `SEARCH_PATH` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-search-path))
- [ ] Model `simulated_data_sharing_consumer`: `SIMULATED_DATA_SHARING_CONSUMER` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-simulated-data-sharing-consumer))
- [ ] Model `strict_json_output`: `STRICT_JSON_OUTPUT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-strict-json-output))
- [ ] Model `timestamp_day_is_always_24h`: `TIMESTAMP_DAY_IS_ALWAYS_24H` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-day-is-always-24h))
- [ ] Model `timestamp_input_format`: `TIMESTAMP_INPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-input-format))
- [ ] Model `timestamp_ltz_output_format`: `TIMESTAMP_LTZ_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-ltz-output-format))
- [ ] Model `timestamp_ntz_output_format`: `TIMESTAMP_NTZ_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-ntz-output-format))
- [ ] Model `timestamp_output_format`: `TIMESTAMP_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-output-format))
- [ ] Model `timestamp_type_mapping`: `TIMESTAMP_TYPE_MAPPING` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-type-mapping))
- [ ] Model `timestamp_tz_output_format`: `TIMESTAMP_TZ_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-tz-output-format))
- [ ] Model `timezone`: `TIMEZONE` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-timezone))
- [ ] Model `time_input_format`: `TIME_INPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-time-input-format))
- [ ] Model `time_output_format`: `TIME_OUTPUT_FORMAT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-time-output-format))
- [ ] Model `transaction_abort_on_error`: `TRANSACTION_ABORT_ON_ERROR` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-transaction-abort-on-error))
- [ ] Model `transaction_default_isolation_level`: `TRANSACTION_DEFAULT_ISOLATION_LEVEL` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-transaction-default-isolation-level))
- [ ] Model `two_digit_century_start`: `TWO_DIGIT_CENTURY_START` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-two-digit-century-start))
- [ ] Model `unsupported_ddl_action`: `UNSUPPORTED_DDL_ACTION` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-unsupported-ddl-action))
- [ ] Model `use_cached_result`: `USE_CACHED_RESULT` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-use-cached-result))
- [ ] Model `week_of_year_policy`: `WEEK_OF_YEAR_POLICY` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-week-of-year-policy))
- [ ] Model `week_start`: `WEEK_START` ([docs](https://docs.snowflake.com/en/sql-reference/parameters#label-week-start))

Modeled, without a live sample: `type`, `password`, `must_change_password`, `mins_to_bypass_mfa`, `rsa_public_key`, `rsa_public_key_2`, `workload_identity.type`, `workload_identity.arn`, `workload_identity.issuer`, `workload_identity.subject`, `workload_identity.oidc_audience_list[]`, `network_policy`, `owner`

### VIEW

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 2 pass, 7 fail, 20 not modeled, 3 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-view) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-view) [show](https://docs.snowflake.com/en/sql-reference/sql/show-views) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-view)

- [ ] `temporary` fails L3, L4, L5: `Snowflake reports is_volatile='true' (declared form True), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-view#optional-parameters))
  - snowcap sent: `ALTER VIEW <db>.PUBLIC.R00_TEMPORARY SET`
- [ ] `recursive` fails L3, L4, L5: `Snowflake reports is_recursive='true' (declared form True), snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-view#optional-parameters))
  - snowcap sent: `ALTER VIEW <db>.PUBLIC.R01_RECURSIVE SET`
- [ ] `as` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 52 unexpected 'as'. on: ALTER VIEW <db>.PUBLIC.R02_AS SET as SELECT ID, NAME FROM <db>.PUBLIC.COVERAGE_T WHERE ID > 0` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-view#required-parameters))
  - snowcap sent: `ALTER VIEW <db>.PUBLIC.R02_AS SET as SELECT ID, NAME FROM <db>.PUBLIC.COVERAGE_T WHERE ID > 0`
- [ ] `secure` fails L4, L5: `Snowflake rejects the SQL: ProgrammingError: 001003 (42000): SQL compilation error: syntax error line 1 at position 55 unexpected '<EOF>'. on: ALTER VIEW <db>.PUBLIC.R03_SECURE SET` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#parameters))
  - snowcap sent: `ALTER VIEW <db>.PUBLIC.R03_SECURE SET`
- [ ] `columns[].name` fails L4, L5: `a change to [{'name': 'ID2'}, {'name': 'NAME'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-view#optional-parameters))
- [ ] `columns[].comment` fails L4, L5: `a change to [{'name': 'ID', 'comment': 'b'}, {'name': 'NAME'}] plans nothing` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-view#optional-parameters))
- [ ] `tags` fails L2: `create failed: ProgrammingError: 391806 (0A000): SQL compilation error: Invalid value VIEW for argument OBJECT_TYPE. Please use object type TABLE for all kinds of table-like objects. on: SELECT * FROM table(<db>.information_schema.tag_references( '<db>.PUBLIC.R08_TAGS', 'VIEW' ))` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- Combined change: untested (needs two or more fields)
- [ ] Model `copy_tags`: `COPY TAGS` ([docs](https://docs.snowflake.com/en/sql-reference/sql/create-view#optional-parameters))
- [ ] Model `columns[].masking_policy`: `<col> [ WITH ] MASKING POLICY <policy_name> [ USING ( <col1> , <cond_col1> , ... ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].masking_policy_using`: `USING ( <col1> , <cond_col1> , ... )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].projection_policy`: `<col> [ WITH ] PROJECTION POLICY <policy_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `columns[].tags`: `<col> [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `row_access_policy`: `[ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `aggregation_policy`: `[ WITH ] AGGREGATION POLICY <policy_name> [ ENTITY KEY ( <col_name> [ , ... ] ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `join_policy` (preview): `[ WITH ] JOIN POLICY <policy_name> [ ALLOWED JOIN KEYS ( <col_name> [ , ... ] ) ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-governance-policy-and-tag-actions-datagovnpolicytagaction))
- [ ] Model `data_metric_functions[]`: `WITH DATA METRIC FUNCTION ( <dmf_name> ON ( <col_name> [ , ... ] ) [ , ... ] )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].execute_as_role`: `EXECUTE AS ROLE <role_name>` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].expectations[]`: `EXPECTATION <expectation_name> ( <expression> )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].filter`: `FILTER ( <predicate> )` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].within_group`: `WITHIN GROUP ( <group_col> [ , ... ] ) [ GROUP LIMIT ]` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#syntax))
- [ ] Model `data_metric_functions[].suspended`: `{ SUSPEND | RESUME }` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].anomaly_detection` (preview): `ANOMALY_DETECTION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].sensitivity` (preview): `SENSITIVITY` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_functions[].data_quality_notification` (preview): `DATA_QUALITY_NOTIFICATION` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `data_metric_schedule`: `DATA_METRIC_SCHEDULE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#data-metric-function-actions-datametricfunctionaction))
- [ ] Model `contacts`: `WITH CONTACT / SET CONTACT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#parameters))
- [ ] Model `dcm_project`: `UNSET DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-view#parameters))
- [x] `change_tracking`
- [x] `comment`

Modeled, without a live sample: `copy_grants`, `columns[]`, `owner`

### WAREHOUSE

Tested live at snowcap `584e44b` on 2026-10-16, Snowflake 10.37.102. 14 pass, 1 fail, 3 not modeled, 6 without a live sample. Docs: [create](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse) [alter](https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse) [show](https://docs.snowflake.com/en/sql-reference/sql/show-warehouses) [describe](https://docs.snowflake.com/en/sql-reference/sql/desc-warehouse)

- [ ] `tags` fails L3, L5: `declared {'<db>.PUBLIC.COVERAGE_TAG': 'a'}, snowcap fetches None` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse))
- [x] Combined change of `auto_resume`, `auto_suspend`, `comment`, `enable_query_acceleration`, `generation`, `max_cluster_count`, `max_concurrency_level`, `min_cluster_count`, `query_acceleration_max_scale_factor`, `resource_monitor`, `scaling_policy`, `statement_queued_timeout_in_seconds`, `statement_timeout_in_seconds`, `warehouse_size` in one plan
- [ ] Model `suspend_resume`: `SUSPEND | RESUME` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse))
- [ ] Model `enable_disable`: `ENABLE | DISABLE` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse))
- [ ] Model `dcm_project`: `DCM PROJECT` ([docs](https://docs.snowflake.com/en/sql-reference/sql/alter-warehouse))
- [x] `warehouse_size`
- [x] `generation`
- [x] `max_cluster_count`
- [x] `min_cluster_count`
- [x] `scaling_policy`
- [x] `auto_suspend`
- [x] `auto_resume`
- [x] `enable_query_acceleration`
- [x] `query_acceleration_max_scale_factor`
- [x] `comment`
- [x] `max_concurrency_level`
- [x] `statement_queued_timeout_in_seconds`
- [x] `statement_timeout_in_seconds`
- [x] `resource_monitor`

Modeled, without a live sample: `warehouse_type`, `resource_constraint`, `initially_suspended`, `max_query_performance_level`, `query_throughput_multiplier`, `owner`

## Other code findings

- [ ] `snowcap/resources/session_policy.py` cannot be imported: it uses `ResourceType.SESSION_POLICY`, which does not exist, so snowcap has no session policy resource. (`snowcap/resources/session_policy.py`)
- [ ] Service never renders `stage` or `yaml_file_stage_path`: its props declare `specification_file`, which is not a spec field. (`snowcap/resources/service.py`)
- [ ] `Props.render` silently drops dict keys that a resource does not declare, so a missing prop leaves a value out of the SQL without an error. (`snowcap/props.py`)
- [ ] ExternalFunction has its `not_null` prop commented out, so `RETURNS ... NOT NULL` is never rendered. (`snowcap/resources/external_function.py`)
- [ ] `Manifest.add` logs a duplicate-resource warning with conflicting data on every plan for some resource types (procedures, file formats, packages policies), although the blueprint holds one resource. (`snowcap/blueprint.py`)

## Limits of these results

- A change to any input of an object, docs fields included, clears that object's live results until the next live run.
- List values are compared as strings, and normalization strips quotes. A sample that differs only in quotes is a weak test.
- An `args[].data_type` L4 pass creates a new function overload. L5 shows this.
- The combined test cannot mix fields that exclude each other.
- `owner` rows are not sampled: the harness sets the owner to the test role.
- Objects that need paid compute, another account, or another edition are docs only, with the reason in their section.

## How to update

Live tests need the test account credentials in `tests/.env`. They create objects in throwaway databases and drop them, also on failure.

```sh
uv run python tools/snowflake_coverage/static_map.py            # L1 and static candidates
uv run python tools/snowflake_coverage/live_audit.py 'TABLE'    # L2 to L5 for one or more objects
uv run python tools/snowflake_coverage/report.py                # regenerate this file
```
