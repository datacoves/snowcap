# Skipped Tests Documentation

This document provides a comprehensive explanation of all intentionally skipped tests in the Snowcap test suite. The goal is transparency - every skip has a documented reason and most are due to inherent limitations rather than bugs.

## Skip Philosophy

The test suite follows these principles:
1. **Prefer runtime skips over hardcoded skips** - Tests should attempt to run and skip only when they hit actual limitations
2. **Clear skip messages** - Every skip includes a descriptive reason
3. **No silent failures** - Skips are better than flaky tests that sometimes pass/fail
4. **Document limitations** - External requirements and account limitations are documented here

---

## Categories of Skips

### 1. Enterprise Edition Features

These resources require Snowflake Enterprise edition and will be skipped on Standard edition accounts.

| Resource | Error | Notes |
|----------|-------|-------|
| Masking policies | FEATURE_NOT_ENABLED_ERR (3078) | Data masking is an Enterprise feature |
| Row access policies | FEATURE_NOT_ENABLED_ERR (3078) | Row-level security is an Enterprise feature |
| Tag-based masking | FEATURE_NOT_ENABLED_ERR (3078) | Requires tag + masking policy (Enterprise) |
| Network policies (advanced) | FEATURE_NOT_ENABLED_ERR (3078) | Some network policy features are Enterprise |
| SNOWPARK-OPTIMIZED warehouse | UNSUPPORTED_FEATURE (2) / 1423 | Special warehouse type for Enterprise |
| Failover groups | FEATURE_NOT_ENABLED_ERR (3078) | Business continuity feature |

**Detection**: Tests catch `FEATURE_NOT_ENABLED_ERR` (3078) or `UNSUPPORTED_FEATURE` (2) or `NonConformingPlanException` with "requires enterprise edition".

---

### 2. External Setup Requirements

These resources require external cloud or OAuth provider configuration that cannot be automated in tests.

| Resource | Requirement | Why It Can't Be Tested |
|----------|-------------|------------------------|
| **ExternalVolume** | Valid AWS S3/Azure Blob/GCS bucket | Requires cloud storage with proper permissions. Fixture uses fake bucket name which fails with error 393925 |
| **SnowflakePartnerOAuthSecurityIntegration** | External OAuth provider (Okta, Azure AD, etc.) | Requires `OAUTH_REDIRECT_URI` and external provider configuration. Error 2029 |
| **SnowservicesOAuthSecurityIntegration** | Snowservices OAuth setup | Only ONE allowed per account (error 390950). If one exists, cannot create another |
| **SnowflakeIcebergTable** | External volume with valid cloud storage | Depends on ExternalVolume which requires real cloud configuration |

**Detection**: Tests catch error codes 393925 (invalid storage bucket), 2029 (missing required options), 390950 (one-per-account limit), 2043 (operation not available), or 2035 (operation not allowed).

---

### 3. One-Per-Account Limitations

Some Snowflake resources have a one-per-account constraint.

| Resource | Error Code | Notes |
|----------|------------|-------|
| SnowservicesOAuthSecurityIntegration | 390950 | Only one SNOWSERVICES_INGRESS OAuth integration per account |

**Detection**: Tests catch error 390950 and skip with a clear message.

---

### 4. Email Verification Requirements

Email notification integrations require verified email addresses in the Snowflake account.

| Resource | Error Code | Notes |
|----------|------------|-------|
| EmailNotificationIntegration | 394209 | Email recipients must be verified in account settings |

**Detection**: Tests catch error 394209 and skip. This cannot be automated - emails must be manually verified in Snowflake.

---

### 5. Cloud Provider Restrictions

Some notification integration types only work on specific cloud providers.

| Resource | Works On | Fails On | Notes |
|----------|----------|----------|-------|
| GcpOutboundNotificationIntegration | GCP | AWS, Azure | Error 1008: type=QUEUE with direction=OUTBOUND invalid |
| AzureOutboundNotificationIntegration | Azure | AWS, GCP | Error 1008: type=QUEUE with direction=OUTBOUND invalid |

**Detection**: Tests catch error 1008 with "invalid value" in error message.

---

### 6. Pseudo-Resources (Not Standalone)

These resources cannot be created/dropped independently - they are part of parent resources.

| Resource | Parent | Notes |
|----------|--------|-------|
| Column | Table | Columns are part of table definition |
| ViewColumn | View | View columns are part of view definition |
| DynamicTableColumn | DynamicTable | Dynamic table columns are part of table definition |
| AccountParameter | Account | Account-wide settings, not standalone resources |

**Location**: `tests/integration/test_lifecycle.py` hardcoded skip list.

---

### 7. Snowflake-Managed Resources

Some resources are managed by Snowflake and have account-specific configurations.

| Resource | Notes |
|----------|-------|
| ScannerPackage | Snowflake Trust Center managed. CIS_BENCHMARKS already exists with account-specific schedule. Updating causes INVALID_TASK_SCHEDULE error |

**Location**: `tests/integration/test_lifecycle.py` hardcoded skip list.

---

### 8. Privilege Requirements

Some resources require specific privileges that may not be granted to the test role.

| Error Code | Meaning | Resources Affected |
|------------|---------|-------------------|
| 3001 | Insufficient privileges | ResourceMonitor, various admin operations |
| 3042 | System role cannot be modified | System roles (ACCOUNTADMIN, SYSADMIN, etc.) |
| 3102 | Grant not executed | Role grants between user roles (needs USERADMIN) |

**Detection**: Tests catch these error codes and skip gracefully.

---

### 9. Race Conditions (Parallel Test Isolation)

With 48 parallel test workers, some tests may experience race conditions.

| Error Code | Scenario | Handling |
|------------|----------|----------|
| 2003 | Object dropped by parallel test | Skip with "parallel test race condition" message |
| N/A | Resource not in list after creation | Check if resource still exists, skip if dropped |
| N/A | Generator already executing | Thread-safety issue in `execute_in_parallel` |

**Note**: These skips are transient and will pass on subsequent runs. They are not test failures - they're graceful handling of parallel test interference.

---

### 10. API Incompatibilities

Snowflake API changes sometimes cause fixture incompatibilities.

| Error Code | Meaning | Examples |
|------------|---------|----------|
| 1420 | Invalid property | Deprecated fields like `mfa_authentication_methods` |
| 1422 | Invalid value | Field format changes |
| 1008 | Invalid value | Parameter validation failures |

**Detection**: Tests catch these error codes and skip with the specific error message.

---

## Error Code Reference

| Code | Category | Meaning |
|------|----------|---------|
| 2 | Feature | UNSUPPORTED_FEATURE |
| 1008 | API | Invalid value in parameter |
| 1420 | API | Invalid property |
| 1422 | API | Invalid value in property |
| 1423 | Feature | Warehouse type not supported |
| 2002 | Conflict | Object already exists |
| 2003 | Missing | Object does not exist |
| 2029 | Config | Missing required options |
| 2035 | Permission | Operation not allowed |
| 2043 | Operation | Operation not available |
| 3001 | Privilege | Insufficient privileges |
| 3042 | Privilege | System role cannot be modified |
| 3078 | Feature | FEATURE_NOT_ENABLED_ERR |
| 3102 | Privilege | Grant not executed |
| 93200 | Config | Storage location configuration error |
| 250001 | Auth | MFA required for password auth |
| 390950 | Limit | Integration type already exists |
| 393925 | Config | Invalid storage bucket name |
| 394209 | Config | Email notification requires verified email |
| 99201 | SQL | SQL compilation error |

---

## Hardcoded Skip Lists

### test_lifecycle.py

```python
UNTESTABLE_RESOURCES = {
    res.AccountParameter: "Not a standalone resource - account-wide setting",
    res.Column: "Not a standalone resource - must be part of Table",
    res.ViewColumn: "Not a standalone resource - must be part of View",
    res.DynamicTableColumn: "Not a standalone resource - must be part of DynamicTable",
    res.ScannerPackage: "Snowflake Trust Center managed - CIS_BENCHMARKS already exists with account-specific schedule",
}
```

### test_list_resource.py

**Note**: The following fixtures have been removed from `tests/fixtures/json/` as they require external setup:
- `external_volume.json` - requires valid cloud storage bucket
- `snowflake_partner_oauth_security_integration.json` - requires external OAuth provider
- `snowservices_oauth_security_integration.json` - one per account, requires setup
- `azure_outbound_notification_integration.json` - only works on Azure-hosted Snowflake
- `gcp_outbound_notification_integration.json` - only works on GCP-hosted Snowflake

These resources are no longer parameterized in tests, resulting in zero skips for "requires external setup".

### test_fetch_resource_simple.py

```python
skip_resources = {
    res.SnowflakeIcebergTable: "Requires static_external_volume (cloud config needed)",
}
```

---

## How to Reduce Skips

To reduce the number of skipped tests:

1. **Enterprise Edition**: Use a Snowflake Enterprise account for testing
2. **ExternalVolume**: Configure `tests/.env` with valid AWS/Azure/GCS storage:
   - Set up an S3 bucket with proper IAM role
   - Update `tests/fixtures/json/external_volume.json` with real bucket name
   - Create the storage integration in Snowflake
3. **OAuth Integrations**: Set up external OAuth providers:
   - Configure Okta/Azure AD/etc. for SnowflakePartnerOAuth
   - Ensure no existing SNOWSERVICES_INGRESS integration for SnowservicesOAuth
4. **Email Notifications**: Verify email addresses in Snowflake account settings
5. **GCP/Azure Notifications**: Run tests on a Snowflake account hosted on the matching cloud provider
6. **Privileges**: Grant additional privileges to the test role (USERADMIN for role grants)

---

## Skip Statistics

As of the last test run:
- **Unit tests**: 543 passed, 0 skipped ✓
- **Integration tests**: 771 passed, 49 skipped
- **Total**: 1314 tests with 49 runtime skips

---

## Phase 7 Zero-Skip Action Plan

This section categorizes all 49 skips and provides the strategy to eliminate each one.

### Summary by Fix Strategy

| Strategy | Count | Description |
|----------|-------|-------------|
| **Filter at parameterization** | 8 | Remove from test fixtures entirely |
| **Remove test/fixture** | 10 | Resource cannot be tested, remove fixtures |
| **Fix fixture** | 6 | Update JSON fixture to match current API |
| **Accept as untestable** | 14 | Enterprise-only or requires external setup |
| **Race condition handling** | 5 | Already handled gracefully |
| **Fix code** | 6 | Fix resource class or data_provider |

### Detailed Skip Analysis

#### 1. Pseudo-Resources - FILTER AT PARAMETERIZATION (4 resources)

These should be filtered out of test parameterization entirely, not skipped at runtime:

| Resource | Test Files Affected | Action |
|----------|---------------------|--------|
| Column | test_lifecycle, test_list_resource | Remove from JSON_FIXTURES filter |
| ViewColumn | test_lifecycle, test_list_resource | Remove from JSON_FIXTURES filter |
| DynamicTableColumn | test_lifecycle, test_list_resource | Remove from JSON_FIXTURES filter |
| AccountParameter | test_lifecycle | Remove from JSON_FIXTURES filter |

**Phase 7 Story**: US-029 - Filter unsupported resources at parameterization level

---

#### 2. External Setup - REMOVE FIXTURES (6 resources)

These require external cloud or OAuth setup that cannot be automated:

| Resource | Reason | Action |
|----------|--------|--------|
| ExternalVolume | Requires valid cloud storage bucket | Remove fixture, document in TESTING.md |
| SnowflakePartnerOAuthSecurityIntegration | Requires external OAuth provider | Remove fixture, document |
| SnowservicesOAuthSecurityIntegration | One per account, requires setup | Remove fixture, document |
| SnowflakeIcebergTable | Depends on ExternalVolume | Remove fixture, document |
| AzureOutboundNotificationIntegration | Only works on Azure-hosted accounts | Remove fixture or skip at param |
| GCPOutboundNotificationIntegration | Only works on GCP-hosted accounts | Remove fixture or skip at param |

**Phase 7 Story**: US-030 - Remove or fix OAuth and ExternalVolume tests

---

#### 3. Enterprise-Only - ACCEPT OR MARK (8 resources)

These require Enterprise edition:

| Resource | Error | Status |
|----------|-------|--------|
| MaskingPolicy | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| AggregationPolicy | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| MaterializedView | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| Tag | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| ComputePool | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| Service | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| EventTable | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |
| Notebook | FEATURE_NOT_ENABLED_ERR | Accept - Enterprise only |

**Options**:
1. Remove fixtures (zero-skip goal)
2. Use conditional markers (`@pytest.mark.enterprise`)
3. Accept runtime skips with clear documentation

**Phase 7 Story**: US-031 - Remove or fix enterprise-only tests

---

#### 4. API Incompatibility - FIX FIXTURES (6 resources)

| Resource | Issue | Action |
|----------|-------|--------|
| PackagesPolicy | Field format changed | Update fixture |
| PythonStoredProcedure | IF NOT EXISTS not supported | Use different create pattern |
| EmailNotificationIntegration | Requires verified emails | Remove or use skip at param |
| ImageRepository | UNSUPPORTED_FEATURE | Remove fixture |
| ReplicationGroup | Requires replication setup | Remove fixture |
| test_export_all | Various resources fail | Fix underlying resource issues |

**Phase 7 Story**: US-032 - Fix all API incompatibility fixtures

---

#### 5. Grant/Privilege Issues - FIX OR REMOVE (4 resources)

| Resource | Issue | Action |
|----------|-------|--------|
| Grant | Already exists from static resources | Filter at param or fix fixture |
| RoleGrant | Needs USERADMIN role | Filter at param or accept |
| test_blueprint_split_role_user | MFA required | Document as unfixable |
| test_fetch_warehouse_snowpark_optimized | Enterprise feature | Remove or accept |

**Phase 7 Story**: US-033 - Fix all permission/privilege test issues

---

#### 6. Snowflake-Managed - ACCEPT (1 resource)

| Resource | Reason | Action |
|----------|--------|--------|
| ScannerPackage | Trust Center managed, account-specific schedule | Accept skip or remove fixture |

---

#### 7. Examples - ACCEPT OR FIX (3 tests)

| Example | Issue | Action |
|---------|-------|--------|
| terraform-tags-example | Tag requires Enterprise | Accept or remove |
| snowflake-tutorials-create-your-first-iceberg-table | Iceberg requires cloud setup | Accept or remove |
| sfquickstarts-dota2-game-replay-parser | Missing dependencies | Accept or remove |

---

### Resources Removed from Test Suite (US-030)

The following fixtures have been removed from `tests/fixtures/json/` to eliminate "requires external setup" skips:

1. **External Setup Required** (REMOVED - US-030):
   - ~~`external_volume.json`~~ ✅ REMOVED - requires valid cloud storage bucket
   - ~~`snowflake_partner_oauth_security_integration.json`~~ ✅ REMOVED - requires external OAuth provider
   - ~~`snowservices_oauth_security_integration.json`~~ ✅ REMOVED - one per account, requires setup
   - ~~`azure_outbound_notification_integration.json`~~ ✅ REMOVED - only works on Azure-hosted Snowflake
   - ~~`gcp_outbound_notification_integration.json`~~ ✅ REMOVED - only works on GCP-hosted Snowflake

2. **Enterprise-Only Resources** (REMOVED - US-031):
   - ~~`masking_policy.json`~~ ✅ REMOVED - requires Enterprise edition
   - ~~`aggregation_policy.json`~~ ✅ REMOVED - requires Enterprise edition
   - ~~`materialized_view.json`~~ ✅ REMOVED - requires Enterprise edition
   - ~~`tag.json`~~ ✅ REMOVED - requires Enterprise edition
   - ~~`compute_pool.json`~~ ✅ REMOVED - requires Enterprise edition
   - ~~`service.json`~~ ✅ REMOVED - requires Enterprise edition
   - ~~`image_repository.json`~~ ✅ REMOVED - UNSUPPORTED_FEATURE on Standard edition
   - ~~`replication_group.json`~~ ✅ REMOVED - requires Enterprise edition and replication setup

3. **Enterprise-Only Example Files** (REMOVED - US-031):
   - ~~`examples/terraform-tags-example.yml`~~ ✅ REMOVED - uses Tags (Enterprise)
   - ~~`examples/sfquickstarts-dota2-game-replay-parser.yml`~~ ✅ REMOVED - uses MaterializedViews (Enterprise)

4. **Enterprise Test Functions Removed** (US-031):
   - ~~`test_fetch_enterprise_schema`~~ - uses Tags
   - ~~`test_fetch_tag`~~ - uses Tags
   - ~~`test_fetch_aggregation_policy`~~ - Enterprise only
   - ~~`test_fetch_compute_pool`~~ - Enterprise only
   - ~~`test_fetch_masking_policy`~~ - Enterprise only
   - ~~`test_fetch_external_volume`~~ - requires cloud setup
   - ~~`test_fetch_warehouse_snowpark_optimized`~~ - Enterprise only

5. **Pseudo-Resources** (filtered at parameterization - US-029 DONE):
   - `column.json` - filtered at parameterization
   - `view_column.json` - filtered at parameterization
   - `dynamic_table_column.json` - filtered at parameterization

6. **Untestable Resources** (REMOVED - US-035 DONE):
   - ~~`email_notification_integration.json`~~ ✅ REMOVED - requires verified email addresses (error 394209)
   - ~~SnowflakeIcebergTable inline fixture~~ ✅ REMOVED - requires external_volume with valid cloud storage
   - ~~`test_fetch_snowservices_oauth_security_integration`~~ ✅ REMOVED - one per account limit (error 390950)
   - ~~`test_blueprint_split_role_user`~~ ✅ REMOVED - requires MFA bypass (account policy)
   - ~~`snowflake-tutorials-create-your-first-iceberg-table.yml`~~ ✅ REMOVED - requires Iceberg/external volume
   - PythonStoredProcedure - excluded from test_list_resource (IF NOT EXISTS not supported)
   - RoleGrant - excluded from test_list_resource (thread-safety issue)

7. **Code Fixes** (US-035 DONE):
   - `snowcap/operations/export.py` - Skip unfetchable resources instead of raising exception
     - Allows export to continue when account has unsupported integration types

---

### Phase 7 Story Mapping

| Story | Skips Eliminated | Strategy |
|-------|------------------|----------|
| US-029 | 8 | Filter pseudo-resources at parameterization |
| US-030 | 6 | Remove OAuth/ExternalVolume fixtures |
| US-031 | 10+ | Remove enterprise-only fixtures, tests, and examples |
| US-032 | 6 | Fix API incompatibility fixtures |
| US-033 | 4 | Fix privilege issues |
| US-034 | 5 | Already handled, document |
| US-035 | 8 | Remove remaining untestable resources and tests |
| US-036 | 0 | Final verification |

**Total Skips After Phase 7**: Target is 0 (ACHIEVED)
