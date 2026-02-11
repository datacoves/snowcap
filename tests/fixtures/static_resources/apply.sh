#!/bin/bash
# Apply static resources needed for integration tests

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load environment variables from tests/.env
ENV_FILE="$SCRIPT_DIR/../../.env"
if [ -f "$ENV_FILE" ]; then
    echo "Loading environment from $ENV_FILE"
    set -a
    source "$ENV_FILE"
    set +a
else
    echo "Error: $ENV_FILE not found"
    echo "Please create tests/.env with the following variables:"
    echo ""
    echo "TEST_SNOWFLAKE_ACCOUNT=your_account"
    echo "TEST_SNOWFLAKE_USER=your_user"
    echo "TEST_SNOWFLAKE_ROLE=ACCOUNTADMIN"
    echo "TEST_SNOWFLAKE_WAREHOUSE=your_warehouse"
    echo "TEST_SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/key.p8"
    echo ""
    exit 1
fi

# Map test env vars to snowcap env vars
export SNOWFLAKE_ACCOUNT="$TEST_SNOWFLAKE_ACCOUNT"
export SNOWFLAKE_USER="$TEST_SNOWFLAKE_USER"
export SNOWFLAKE_ROLE="$TEST_SNOWFLAKE_ROLE"
export SNOWFLAKE_WAREHOUSE="$TEST_SNOWFLAKE_WAREHOUSE"
export SNOWFLAKE_PRIVATE_KEY_PATH="$TEST_SNOWFLAKE_PRIVATE_KEY_PATH"
export SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT"

if [ -n "$TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE" ]; then
    export SNOWFLAKE_PRIVATE_KEY_FILE_PWD="$TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"
fi

echo "Applying static resources for integration tests..."
echo "Account: $SNOWFLAKE_ACCOUNT"
echo "User: $SNOWFLAKE_USER"
echo "Role: $SNOWFLAKE_ROLE"
echo ""

# Run snowcap from the repo root
cd "$SCRIPT_DIR/../../.."
python -m snowcap apply --config tests/fixtures/static_resources/
