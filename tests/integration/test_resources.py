import pytest
import snowflake.connector.errors
from snowcap import data_provider, lifecycle
from snowcap import resources as res
from snowcap.blueprint import Blueprint
from snowcap.resource_name import ResourceName
from snowcap.identifiers import parse_URN

from tests.helpers import (
    TEST_PUBLIC_KEY,
    TEST_PUBLIC_KEY_2,
    TEST_PUBLIC_KEY_2_FINGERPRINT,
    TEST_PUBLIC_KEY_FINGERPRINT,
    safe_fetch,
)

pytestmark = pytest.mark.requires_snowflake

UNSUPPORTED_FEATURE = 2
FEATURE_NOT_ENABLED_ERR = 3078


def test_user_name_defaults(cursor, suffix, marked_for_cleanup):
    user_name = f"user{suffix}_name_defaults"
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == user_name
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_naked_quoted_name(cursor, suffix, marked_for_cleanup):
    user_name = f"~user{suffix}_naked_quoted_name"
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == user_name
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_quoted_name(cursor, suffix, marked_for_cleanup):
    user_name = f'"user{suffix}_quoted_name"'
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == ResourceName(user_name)._name.upper()
    assert user._data.display_name == ResourceName(user_name)._name
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_name_intentionally_left_blank(cursor, suffix, marked_for_cleanup):
    user_name = f"user{suffix}_intentionally_left_blank"
    user = res.User(name=user_name, display_name="", login_name="")
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == ""
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_grant_on_all(cursor, suffix, marked_for_cleanup):
    test_db = f"GRANT_ON_ALL_{suffix}"
    database = res.Database(name=test_db)
    cursor.execute(database.create_sql())
    marked_for_cleanup.append(database)
    schemas = ["schema_1", "schema_2", "schema_3"]
    for schema in schemas:
        schema = res.Schema(name=schema, database=database)
        cursor.execute(schema.create_sql())
        marked_for_cleanup.append(schema)

    grant = res.Grant(
        priv="USAGE",
        on=["ALL", "SCHEMAS", database],
        to="STATIC_ROLE",
    )
    cursor.execute(grant.create_sql())

    schema_1_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/GRANT?priv=USAGE&on=schema/{test_db}.SCHEMA_1&to=role/STATIC_ROLE")
    )
    assert schema_1_usage_grant is not None
    assert schema_1_usage_grant["priv"] == "USAGE"
    assert schema_1_usage_grant["to"] == "STATIC_ROLE"
    assert schema_1_usage_grant["on"] == f"{test_db}.SCHEMA_1"
    assert schema_1_usage_grant["on_type"] == "SCHEMA"

    schema_2_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/GRANT?priv=USAGE&on=schema/{test_db}.SCHEMA_2&to=role/STATIC_ROLE")
    )
    assert schema_2_usage_grant is not None
    assert schema_2_usage_grant["priv"] == "USAGE"
    assert schema_2_usage_grant["to"] == "STATIC_ROLE"
    assert schema_2_usage_grant["on"] == f"{test_db}.SCHEMA_2"
    assert schema_2_usage_grant["on_type"] == "SCHEMA"

    schema_3_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/GRANT?priv=USAGE&on=schema/{test_db}.SCHEMA_3&to=role/STATIC_ROLE")
    )
    assert schema_3_usage_grant is not None
    assert schema_3_usage_grant["priv"] == "USAGE"
    assert schema_3_usage_grant["to"] == "STATIC_ROLE"
    assert schema_3_usage_grant["on"] == f"{test_db}.SCHEMA_3"
    assert schema_3_usage_grant["on_type"] == "SCHEMA"


def test_snowflake_builtin_database_role_grant(cursor, suffix, marked_for_cleanup):
    drg = res.DatabaseRoleGrant(database_role="SNOWFLAKE.CORTEX_USER", to_role="STATIC_ROLE")
    marked_for_cleanup.append(drg)
    cursor.execute(drg.create_sql())

    dbr = res.DatabaseRole(name=f"TEST_GRANT_DATABASE_ROLE_{suffix}", database="STATIC_DATABASE")
    drg = res.DatabaseRoleGrant(database_role=dbr, to_database_role="STATIC_DATABASE.STATIC_DATABASE_ROLE")
    marked_for_cleanup.append(dbr)
    marked_for_cleanup.append(drg)
    cursor.execute(dbr.create_sql())
    cursor.execute(drg.create_sql())


def test_user_key_pair_lifecycle(cursor, suffix, marked_for_cleanup):
    user = res.User(name=f"USER_KEY_PAIR_{suffix}", type="SERVICE")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)

    key_pair = res.UserKeyPair(
        name="MY_KEY",
        user=user,
        public_key=TEST_PUBLIC_KEY,
        comment="primary workload key",
    )
    cursor.execute(key_pair.create_sql())

    data = safe_fetch(cursor, key_pair.urn)
    assert data is not None
    assert data["name"] == "MY_KEY"
    assert data["user"] == str(user.name).upper()
    # Snowflake returns the fingerprint, never the key, so this is the comparison drift
    # detection rests on.
    assert data["fingerprint"] == TEST_PUBLIC_KEY_FINGERPRINT
    assert data["comment"] == "primary workload key"
    assert data["disabled"] is False

    # Rotating the key keeps the name and leaves the prior key behind as a tombstone,
    # which snowcap must not report as an unmanaged key pair. Revoke the prior key
    # immediately -- the response to a leaked private key, and it keeps the test account
    # clean.
    rotated = res.UserKeyPair(
        name="MY_KEY",
        user=user,
        public_key=TEST_PUBLIC_KEY_2,
        expire_rotated_key_pair_after_hours=0,
    )
    for sql in lifecycle.update_resource(
        key_pair.urn,
        {"fingerprint": TEST_PUBLIC_KEY_2_FINGERPRINT},
        res.UserKeyPair.props,
        after=rotated.to_dict(),
    ):
        cursor.execute(sql)

    data = safe_fetch(cursor, key_pair.urn)
    assert data is not None
    assert data["fingerprint"] == TEST_PUBLIC_KEY_2_FINGERPRINT

    listed = data_provider.list_user_key_pairs(cursor)
    assert key_pair.fqn in listed
    assert not [fqn for fqn in listed if "_ROTATED_" in str(fqn.name)]

    for sql in lifecycle.update_resource(
        key_pair.urn,
        {"disabled": True, "comment": "retired"},
        res.UserKeyPair.props,
        after=rotated.to_dict(),
    ):
        cursor.execute(sql)

    data = safe_fetch(cursor, key_pair.urn)
    assert data is not None
    assert data["disabled"] is True
    assert data["comment"] == "retired"

    cursor.execute(key_pair.drop_sql(if_exists=True))
    assert safe_fetch(cursor, key_pair.urn) is None


def test_user_key_pair_blueprint(cursor, suffix, marked_for_cleanup):
    user = res.User(
        name=f"USER_KEY_PAIR_BP_{suffix}",
        type="SERVICE",
        key_pairs=[{"name": "MY_KEY", "public_key": TEST_PUBLIC_KEY}],
    )
    marked_for_cleanup.append(user)

    resources = [user] + user.process_shortcuts()
    blueprint = Blueprint(resources=resources)
    plan = blueprint.plan(cursor.connection)
    assert len(plan) == 2
    blueprint.apply(cursor.connection, plan)

    key_pair_urn = parse_URN(f"urn:::user_key_pair/MY_KEY?user={str(user.name).upper()}")
    data = safe_fetch(cursor, key_pair_urn)
    assert data is not None
    assert data["fingerprint"] == TEST_PUBLIC_KEY_FINGERPRINT

    # An unchanged config plans nothing, which is what proves the fingerprint comparison
    # doesn't report perpetual drift on a key Snowflake never echoes back.
    assert len(blueprint.plan(cursor.connection)) == 0

    # Rotating the key in config plans exactly one update.
    rotated_user = res.User(
        name=user.name,
        type="SERVICE",
        key_pairs=[{"name": "MY_KEY", "public_key": TEST_PUBLIC_KEY_2}],
    )
    blueprint = Blueprint(resources=[rotated_user] + rotated_user.process_shortcuts())
    plan = blueprint.plan(cursor.connection)
    assert len(plan) == 1
    blueprint.apply(cursor.connection, plan)

    data = safe_fetch(cursor, key_pair_urn)
    assert data is not None
    assert data["fingerprint"] == TEST_PUBLIC_KEY_2_FINGERPRINT
