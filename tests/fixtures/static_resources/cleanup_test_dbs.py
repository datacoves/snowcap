#!/usr/bin/env python
"""
Clean up leftover test databases from interrupted test runs.

Usage:
    python tests/fixtures/static_resources/cleanup_test_dbs.py
"""

import os
import sys

from dotenv import load_dotenv
import snowflake.connector

# Load test environment
load_dotenv("tests/.env")


def connection_params():
    params = {
        "account": os.environ["TEST_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["TEST_SNOWFLAKE_USER"],
        "role": os.environ.get("TEST_SNOWFLAKE_ROLE"),
    }

    private_key_path = os.environ.get("TEST_SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path:
        params["private_key_file"] = private_key_path
        passphrase = os.environ.get("TEST_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        if passphrase:
            params["private_key_file_pwd"] = passphrase
    else:
        params["password"] = os.environ["TEST_SNOWFLAKE_PASSWORD"]

    warehouse = os.environ.get("TEST_SNOWFLAKE_WAREHOUSE")
    if warehouse:
        params["warehouse"] = warehouse

    return params


def main():
    conn = snowflake.connector.connect(**connection_params())
    cur = conn.cursor(snowflake.connector.DictCursor)

    # Find old test databases
    cur.execute("SHOW DATABASES LIKE 'TEST_DB_RUN_%'")
    old_dbs = [row["name"] for row in cur.fetchall()]

    if not old_dbs:
        print("No test databases to clean up.")
        return 0

    print(f"Found {len(old_dbs)} test database(s) to clean up:")
    for db in old_dbs:
        print(f"  - {db}")

    response = input("\nDrop these databases? [y/N] ")
    if response.lower() != "y":
        print("Aborted.")
        return 1

    for db in old_dbs:
        try:
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
            print(f"  Dropped: {db}")
        except Exception as e:
            print(f"  Failed to drop {db}: {e}")

    print("Cleanup complete!")
    cur.close()
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
