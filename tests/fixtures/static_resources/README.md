# Static Resources for Integration Tests

This folder contains snowcap configuration to create the static resources required by integration tests.

## Prerequisites

1. Create `tests/.env` with your Snowflake credentials (see `tests/.env.example`)

2. Ensure you have the required permissions (typically ACCOUNTADMIN)

## Usage

### Preview changes (plan)

```bash
./tests/fixtures/static_resources/plan.sh
```

### Apply changes

```bash
./tests/fixtures/static_resources/apply.sh
```

### Run integration tests

```bash
pytest tests/ --snowflake
```

## Resources Created

- **STATIC_USER** - User for tests (used by ResourceMonitor fixture)
- **STATIC_DATABASE** - Database for tests
- **STATIC_ROLE** - Role for tests (granted to SYSADMIN)
- **STATIC_WAREHOUSE** - XSmall warehouse for tests
- **STATIC_DATABASE.STATIC_SCHEMA** - Additional schema
- **STATIC_DATABASE.STATIC_DATABASE_ROLE** - Database role
- **STATIC_DATABASE.PUBLIC.STATIC_TABLE** - Simple table with ID column
- **STATIC_DATABASE.PUBLIC.STATIC_VIEW** - Simple view
- **STATIC_DATABASE.PUBLIC.STATIC_STAGE** - Internal stage
- **STATIC_DATABASE.PUBLIC.STATIC_STREAM** - Stream on static table
- **STATIC_DATABASE.PUBLIC.STATIC_NETWORK_RULE** - Network rule (INGRESS)
- **STATIC_DATABASE.PUBLIC.STATIC_NETWORK_RULE_EGRESS** - Network rule (EGRESS)
- **STATIC_DATABASE.PUBLIC.STATIC_SECRET** - Password secret
- **STATIC_DATABASE.PUBLIC.STATIC_TAG** - Tag for tests
- **STATIC_SECURITY_INTEGRATION** - OAuth2 API authentication integration
- Various grants to STATIC_ROLE
