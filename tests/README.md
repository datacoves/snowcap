# Running Tests

## Quick Start

```bash
# Run unit tests only (no Snowflake connection required)
make test

# Run all tests including integration tests
make integration EDITION=standard
```

## Test Types

### Unit Tests

Unit tests don't require a Snowflake connection and run quickly:

```bash
# Run all unit tests
python -m pytest tests/ --ignore=tests/integration

# Run a specific test file
python -m pytest tests/test_blueprint.py

# Run a specific test
python -m pytest tests/test_blueprint.py::test_resource_type_needs_params -v
```

### Integration Tests

Integration tests require a live Snowflake connection and test actual Snowflake operations.

```bash
# Run all integration tests
python -m pytest tests/ --snowflake

# Run specific integration test
python -m pytest tests/integration/test_blueprint.py::test_blueprint_database_params_passed_to_public_schema -v --snowflake

# Run only standard edition tests (excludes enterprise-only features)
python -m pytest tests/ --snowflake -m standard

# Run enterprise edition tests
python -m pytest tests/ --snowflake -m enterprise
```

## Setup for Integration Tests

### 1. Configure Snowflake Credentials

Copy the example env file and fill in your credentials:

```bash
cp tests/.env.example tests/.env
```

Edit `tests/.env` with your Snowflake connection details:

```bash
# Required
TEST_SNOWFLAKE_ACCOUNT=your_account_identifier
TEST_SNOWFLAKE_USER=your_username
TEST_SNOWFLAKE_ROLE=ACCOUNTADMIN
TEST_SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Authentication - choose one:
# Option 1: Key-pair (recommended)
TEST_SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/your/rsa_key.p8

# Option 2: Password
# TEST_SNOWFLAKE_PASSWORD=your_password
```

### 2. Create Static Test Resources

Integration tests require certain static resources to exist in Snowflake:

```bash
make setup-test-resources
```

This creates:
- `STATIC_DATABASE`, `STATIC_SCHEMA`
- `STATIC_ROLE`, `STATIC_USER`
- `STATIC_WAREHOUSE`
- `STATIC_TABLE`, `STATIC_VIEW`, `STATIC_STAGE`
- And other test fixtures

See `tests/fixtures/static_resources/README.md` for details.

### 3. Run Integration Tests

```bash
make integration EDITION=standard
```

## Useful pytest Options

```bash
# Verbose output
python -m pytest -v

# Stop on first failure
python -m pytest -x

# Show print statements
python -m pytest -s

# Run tests in parallel (faster)
python -m pytest -n auto

# Run tests matching a pattern
python -m pytest -k "blueprint"

# Show slowest tests
python -m pytest --durations=10
```

## Troubleshooting

### Tests are skipped

If integration tests show as "skipped", you need the `--snowflake` flag:

```bash
python -m pytest tests/integration/ --snowflake
```

### Connection errors

1. Verify your `tests/.env` credentials are correct
2. Check that your IP is allowed (if using network policies)
3. Ensure the private key path is correct and the key is valid

### Missing static resources

If tests fail with "Missing required static test resources":

```bash
make setup-test-resources
```

### Changing Snowflake accounts

When switching to a different Snowflake account:

1. Update `tests/.env` with new credentials
2. Run `make setup-test-resources` to create static resources in the new account
