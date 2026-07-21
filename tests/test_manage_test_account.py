"""
Unit tests for tools/manage_test_account.py

tools/ is not a package, so the module is loaded via importlib and registered in
sys.modules as "manage_test_account" so that @patch("manage_test_account.<attr>") and
monkeypatch.setattr(manage_test_account, "<attr>", ...) resolve correctly.

Everything here runs without a live Snowflake org: no network connections are opened,
and no test writes inside the repo tree (file-touching tests use tmp_path and
monkeypatch the module's REPO_ROOT).
"""

import importlib.util
import os
import pathlib
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
import snowflake.connector
from click.testing import CliRunner
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

REPO_ROOT = pathlib.Path(__file__).parent.parent.resolve()

_spec = importlib.util.spec_from_file_location(
    "manage_test_account", REPO_ROOT / "tools" / "manage_test_account.py"
)
manage_test_account = importlib.util.module_from_spec(_spec)
sys.modules["manage_test_account"] = manage_test_account
_spec.loader.exec_module(manage_test_account)

from snowcap.enums import AccountEdition  # noqa: E402


# =============================================================================
# create_account_sql
# =============================================================================


def test_create_account_sql_renders_fields_in_order():
    sql = manage_test_account.create_account_sql(
        "TEST_ACCT", "TEST_ADMIN", "PUBKEYDATA", "a@example.com", "STANDARD", "US_WEST_2"
    )
    assert sql.split("\n") == [
        "CREATE ACCOUNT TEST_ACCT",
        "ADMIN_NAME = 'TEST_ADMIN'",
        "ADMIN_RSA_PUBLIC_KEY = 'PUBKEYDATA'",
        "ADMIN_USER_TYPE = SERVICE",
        "EMAIL = 'a@example.com'",
        "EDITION = STANDARD",
        "REGION = US_WEST_2",
    ]


def test_create_account_sql_includes_region_group_only_when_given():
    without_group = manage_test_account.create_account_sql(
        "TEST_ACCT", "TEST_ADMIN", "KEY", "a@example.com", "STANDARD", "US_WEST_2"
    )
    assert "REGION_GROUP" not in without_group

    with_group = manage_test_account.create_account_sql(
        "TEST_ACCT", "TEST_ADMIN", "KEY", "a@example.com", "STANDARD", "US_WEST_2", "PUBLIC"
    )
    assert with_group.split("\n")[-2:] == ["REGION_GROUP = PUBLIC", "REGION = US_WEST_2"]


def test_create_account_sql_escapes_single_quotes_in_email():
    sql = manage_test_account.create_account_sql(
        "TEST_ACCT", "TEST_ADMIN", "KEY", "o'brien@example.com", "STANDARD", "US_WEST_2"
    )
    assert "EMAIL = 'o''brien@example.com'" in sql


@pytest.mark.parametrize("name", ["BAD-NAME", "1BAD", "BAD NAME", "BAD'NAME"])
def test_create_account_sql_rejects_invalid_account_name(name):
    with pytest.raises(click.ClickException):
        manage_test_account.create_account_sql(name, "ADMIN", "KEY", "a@example.com", "STANDARD", "US_WEST_2")


def test_create_account_sql_rejects_invalid_admin_name():
    with pytest.raises(click.ClickException):
        manage_test_account.create_account_sql(
            "ACCT", "BAD-ADMIN", "KEY", "a@example.com", "STANDARD", "US_WEST_2"
        )


def test_create_account_sql_rejects_invalid_edition():
    with pytest.raises(click.ClickException):
        manage_test_account.create_account_sql("ACCT", "ADMIN", "KEY", "a@example.com", "NOT_AN_EDITION", "US_WEST_2")


@pytest.mark.parametrize("region", ["US_WEST_2; DROP ACCOUNT X", "'US_WEST_2'"])
def test_create_account_sql_rejects_injection_shaped_region(region):
    with pytest.raises(click.ClickException):
        manage_test_account.create_account_sql("ACCT", "ADMIN", "KEY", "a@example.com", "STANDARD", region)


@pytest.mark.parametrize("region_group", ["PUBLIC; DROP ACCOUNT X", "'PUBLIC'"])
def test_create_account_sql_rejects_injection_shaped_region_group(region_group):
    with pytest.raises(click.ClickException):
        manage_test_account.create_account_sql(
            "ACCT", "ADMIN", "KEY", "a@example.com", "STANDARD", "US_WEST_2", region_group
        )


# =============================================================================
# drop_account_sql
# =============================================================================


def test_drop_account_sql_renders_grace_period():
    assert manage_test_account.drop_account_sql("TEST_ACCT", 5) == "DROP ACCOUNT TEST_ACCT GRACE_PERIOD_IN_DAYS = 5"


def test_drop_account_sql_rejects_injection_shaped_name():
    with pytest.raises(click.ClickException):
        manage_test_account.drop_account_sql("FOO; DROP ACCOUNT BAR", 3)


# =============================================================================
# resolve_region
# =============================================================================


def test_resolve_region_defaults_to_detected_cloud_and_region():
    assert manage_test_account.resolve_region("AWS", "US_WEST_2", None, None) == "AWS_US_WEST_2"


def test_resolve_region_override_keeps_detected_cloud():
    assert manage_test_account.resolve_region("AWS", "US_WEST_2", None, "EU_WEST_1") == "AWS_EU_WEST_1"


def test_resolve_region_cloud_without_region_raises():
    with pytest.raises(click.ClickException):
        manage_test_account.resolve_region("AWS", "US_WEST_2", "gcp", None)


def test_resolve_region_cloud_and_region_compose_uppercased_cloud():
    assert manage_test_account.resolve_region("AWS", "US_WEST_2", "gcp", "EUROPE_WEST4") == "GCP_EUROPE_WEST4"


# =============================================================================
# render_test_env
# =============================================================================


def test_render_test_env_contains_expected_lines():
    output = manage_test_account.render_test_env(
        "ORG-TEST_ACCT",
        "TEST_ADMIN",
        "/path/to/key.p8",
        {"static_user_mfa_password": "mfa-pw-placeholder", "storage_base_url": "s3://x"},
    )
    assert "TEST_SNOWFLAKE_ACCOUNT=ORG-TEST_ACCT" in output
    assert "TEST_SNOWFLAKE_USER=TEST_ADMIN" in output
    assert "TEST_SNOWFLAKE_ROLE=ACCOUNTADMIN" in output
    assert "TEST_SNOWFLAKE_WAREHOUSE=STATIC_WAREHOUSE" in output
    assert "TEST_SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/key.p8" in output
    assert "VAR_STATIC_USER_MFA_PASSWORD=mfa-pw-placeholder" in output
    assert "VAR_STORAGE_BASE_URL=s3://x" in output


def test_render_test_env_never_leaks_org_credentials():
    org_env = {
        "SNOWFLAKE_ORG_ACCOUNT": "ORG_ACCT_SECRET",
        "SNOWFLAKE_ORG_USER": "ORG_USER_SECRET",
        "SNOWFLAKE_ORG_PASSWORD": "org-pw-placeholder",
    }
    with patch.dict(os.environ, org_env):
        output = manage_test_account.render_test_env("ACCT", "ADMIN", "/key.p8", {})
    assert "SNOWFLAKE_ORG" not in output


# =============================================================================
# generate_rsa_keypair
# =============================================================================


def test_generate_rsa_keypair_writes_pkcs8_pem_with_restrictive_mode(tmp_path):
    key_path = tmp_path / "k.p8"

    public_der_b64 = manage_test_account.generate_rsa_keypair(key_path)

    assert oct(key_path.stat().st_mode & 0o777) == "0o600"
    private_key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
    assert isinstance(private_key, rsa.RSAPrivateKey)
    public_key = serialization.load_der_public_key(manage_test_account.base64.b64decode(public_der_b64))
    assert public_key.public_numbers() == private_key.public_key().public_numbers()


def test_generate_rsa_keypair_without_path_writes_no_file(tmp_path):
    result = manage_test_account.generate_rsa_keypair(None)

    assert isinstance(result, str)
    assert list(tmp_path.iterdir()) == []


# =============================================================================
# _generate_mfa_password
# =============================================================================


def test_generate_mfa_password_meets_complexity_and_requested_length():
    password = manage_test_account._generate_mfa_password(24)

    assert len(password) == 24
    assert any(c.islower() for c in password)
    assert any(c.isupper() for c in password)
    assert any(c.isdigit() for c in password)


# =============================================================================
# _validate_identifier
# =============================================================================


@pytest.mark.parametrize("value", ["A1_$", "_abc", "ABC123"])
def test_validate_identifier_accepts_valid_names(value):
    assert manage_test_account._validate_identifier(value) == value


@pytest.mark.parametrize("value", ["bad-name", "bad name", "bad'name", "1bad"])
def test_validate_identifier_rejects_invalid_names(value):
    with pytest.raises(click.ClickException):
        manage_test_account._validate_identifier(value)


# =============================================================================
# _archive_if_exists
# =============================================================================


def test_archive_if_exists_returns_none_when_absent(tmp_path):
    assert manage_test_account._archive_if_exists(tmp_path / "missing.txt", "bak") is None


def test_archive_if_exists_renames_present_file(tmp_path):
    path = tmp_path / "present.txt"
    path.write_text("data")

    archived = manage_test_account._archive_if_exists(path, "bak")

    assert not path.exists()
    assert archived.read_text() == "data"
    assert archived.name.startswith("present.txt.bak.")


# =============================================================================
# get_org_connection
# =============================================================================


def test_get_org_connection_missing_env_names_every_missing_var():
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(click.ClickException) as exc_info:
            manage_test_account.get_org_connection()

    for var in ["SNOWFLAKE_ORG_ACCOUNT", "SNOWFLAKE_ORG_USER", "SNOWFLAKE_ORG_PASSWORD"]:
        assert var in str(exc_info.value)


def test_get_org_connection_uses_org_env_vars_with_orgadmin_role():
    org_env = {
        "SNOWFLAKE_ORG_ACCOUNT": "ORG_ACCT",
        "SNOWFLAKE_ORG_USER": "ORG_USER",
        "SNOWFLAKE_ORG_PASSWORD": "org-pw-placeholder",
    }
    with patch.dict(os.environ, org_env, clear=True):
        with patch("manage_test_account.snowflake.connector.connect") as mock_connect:
            manage_test_account.get_org_connection()

    mock_connect.assert_called_once_with(
        account="ORG_ACCT", user="ORG_USER", password="org-pw-placeholder", role="ORGADMIN"
    )


# =============================================================================
# poll_for_connection
# =============================================================================


def test_poll_for_connection_succeeds_after_initial_failures():
    mock_conn = MagicMock()
    failures = [snowflake.connector.errors.Error("not ready"), snowflake.connector.errors.Error("not ready")]
    with patch("manage_test_account.snowflake.connector.connect", side_effect=[*failures, mock_conn]):
        result = manage_test_account.poll_for_connection("ACCT", "ADMIN", "/key.p8", timeout=1, interval=0.01)

    assert result is mock_conn


def test_poll_for_connection_raises_on_timeout_mentioning_timeout():
    error = snowflake.connector.errors.Error("still not ready")
    with patch("manage_test_account.snowflake.connector.connect", side_effect=error):
        with pytest.raises(click.ClickException, match=r"Timed out after 0\.05s"):
            manage_test_account.poll_for_connection("ACCT", "ADMIN", "/key.p8", timeout=0.05, interval=0.01)


def test_poll_for_connection_propagates_unexpected_exception_immediately():
    with patch("manage_test_account.snowflake.connector.connect", side_effect=TypeError("boom")):
        with pytest.raises(TypeError):
            manage_test_account.poll_for_connection("ACCT", "ADMIN", "/key.p8", timeout=5, interval=0.01)


# =============================================================================
# drop_test_account
# =============================================================================


def _org_connection(org_name="ORG"):
    conn = MagicMock()
    conn.cursor.return_value.execute.return_value.fetchone.return_value = [org_name]
    return conn


def test_drop_test_account_env_mismatch_without_yes_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(manage_test_account, "REPO_ROOT", tmp_path)
    env_path = tmp_path / "tests" / ".env"
    env_path.parent.mkdir(parents=True)
    env_path.write_text("TEST_SNOWFLAKE_ACCOUNT=ORG-OTHER_ACCT\n")

    with patch.object(manage_test_account, "get_org_connection", return_value=_org_connection()):
        with pytest.raises(click.ClickException):
            manage_test_account.drop_test_account("TEST_ACCT", 3, yes=False)


def test_drop_test_account_missing_env_without_yes_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(manage_test_account, "REPO_ROOT", tmp_path)

    with patch.object(manage_test_account, "get_org_connection", return_value=_org_connection()):
        with pytest.raises(click.ClickException):
            manage_test_account.drop_test_account("TEST_ACCT", 3, yes=False)


def test_drop_test_account_with_yes_executes_drop_and_archives_matching_env(tmp_path, monkeypatch):
    monkeypatch.setattr(manage_test_account, "REPO_ROOT", tmp_path)
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    env_path = tests_dir / ".env"
    env_path.write_text("TEST_SNOWFLAKE_ACCOUNT=ORG-TEST_ACCT\n")
    org_conn = _org_connection()

    with patch.object(manage_test_account, "get_org_connection", return_value=org_conn):
        manage_test_account.drop_test_account("TEST_ACCT", 3, yes=True)

    assert not env_path.exists()
    dropped = list(tests_dir.glob(".env.dropped.*"))
    assert len(dropped) == 1
    executed_sql = [call.args[0] for call in org_conn.cursor.return_value.execute.call_args_list]
    assert "DROP ACCOUNT TEST_ACCT GRACE_PERIOD_IN_DAYS = 3" in executed_sql


# =============================================================================
# CLI defaults
# =============================================================================


def _param_default(command, param_name):
    return next(p.default for p in command.params if p.name == param_name)


def test_provision_and_drop_name_defaults_match():
    assert manage_test_account.DEFAULT_ACCOUNT_NAME == "SNOWCAP_TEST"
    assert _param_default(manage_test_account.main.commands["provision"], "name") == "SNOWCAP_TEST"
    assert _param_default(manage_test_account.main.commands["drop"], "name") == "SNOWCAP_TEST"


def test_provision_cli_without_email_exits_nonzero():
    result = CliRunner().invoke(manage_test_account.main, ["provision"])

    assert result.exit_code != 0
    assert "--email" in result.output


# =============================================================================
# provision_test_account orchestration
# =============================================================================


@pytest.fixture
def provision_deps(tmp_path, monkeypatch):
    """Wires every external dependency of provision_test_account to in-memory mocks."""
    monkeypatch.setattr(manage_test_account, "REPO_ROOT", tmp_path)
    (tmp_path / "tests" / "fixtures" / "static_resources").mkdir(parents=True)

    conn = MagicMock()
    monkeypatch.setattr(manage_test_account, "get_connection", lambda: conn)
    monkeypatch.setattr(
        manage_test_account,
        "fetch_session",
        lambda c: {"cloud": "AWS", "cloud_region": "US_WEST_2", "account_edition": AccountEdition.STANDARD},
    )
    monkeypatch.setattr(manage_test_account, "fetch_region", lambda c: {"CURRENT_REGION()": "AWS_US_WEST_2"})

    org_cursor = MagicMock()
    org_cursor.execute.return_value.fetchone.return_value = ["ORG"]
    dict_cursor = MagicMock()
    dict_cursor.fetchall.return_value = []

    org_conn = MagicMock()
    org_conn.cursor.side_effect = lambda *args, **kwargs: dict_cursor if args or kwargs else org_cursor
    monkeypatch.setattr(manage_test_account, "get_org_connection", lambda: org_conn)

    new_conn = MagicMock()
    poll_for_connection = MagicMock(return_value=new_conn)
    monkeypatch.setattr(manage_test_account, "poll_for_connection", poll_for_connection)
    monkeypatch.setattr(manage_test_account, "reset_test_account", MagicMock())

    fake_subprocess = MagicMock()
    fake_subprocess.CalledProcessError = subprocess.CalledProcessError
    fake_subprocess.run.return_value = MagicMock(returncode=0)
    monkeypatch.setattr(manage_test_account, "subprocess", fake_subprocess)

    return SimpleNamespace(
        conn=conn,
        org_conn=org_conn,
        org_cursor=org_cursor,
        dict_cursor=dict_cursor,
        new_conn=new_conn,
        poll_for_connection=poll_for_connection,
        subprocess=fake_subprocess,
    )


def _provision(key_path, name="SNOWCAP_TEST"):
    manage_test_account.provision_test_account(
        name, "a@example.com", None, None, None, "SNOWCAP_ADMIN", str(key_path)
    )


def test_provision_resume_skips_create_and_does_not_regenerate_key(provision_deps, tmp_path):
    key_path = tmp_path / "key.p8"
    key_path.write_text("EXISTING_KEY_CONTENTS")
    provision_deps.dict_cursor.fetchall.return_value = [{"name": "SNOWCAP_TEST"}]

    _provision(key_path)

    assert key_path.read_text() == "EXISTING_KEY_CONTENTS"
    executed_sql = [call.args[0] for call in provision_deps.org_cursor.execute.call_args_list]
    assert not any(sql.startswith("CREATE ACCOUNT") for sql in executed_sql)


def test_provision_backs_up_existing_key_before_fresh_create(provision_deps, tmp_path):
    key_path = tmp_path / "key.p8"
    key_path.write_text("OLD_KEY")

    _provision(key_path)

    backups = list(tmp_path.glob("key.p8.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_text() == "OLD_KEY"
    assert key_path.read_text() != "OLD_KEY"


def test_provision_backs_up_existing_tests_env(provision_deps, tmp_path):
    key_path = tmp_path / "key.p8"
    env_path = tmp_path / "tests" / ".env"
    env_path.write_text("OLD_ENV")

    _provision(key_path)

    backups = list((tmp_path / "tests").glob(".env.bak.*"))
    assert len(backups) == 1
    assert backups[0].read_text() == "OLD_ENV"
    assert env_path.exists()
    assert env_path.read_text() != "OLD_ENV"


def test_provision_surfaces_apply_sh_failure(provision_deps, tmp_path, capsys):
    key_path = tmp_path / "key.p8"
    provision_deps.subprocess.run.side_effect = subprocess.CalledProcessError(
        1, ["apply.sh"], output="stdout contents", stderr="stderr contents"
    )

    with pytest.raises(click.ClickException) as exc_info:
        _provision(key_path)

    assert "stdout contents" in str(exc_info.value)
    assert "stderr contents" in str(exc_info.value)
    assert "Recovery" in capsys.readouterr().err


def test_provision_invalid_name_fails_before_touching_key_file(provision_deps, tmp_path):
    key_path = tmp_path / "key.p8"

    with pytest.raises(click.ClickException):
        _provision(key_path, name="bad-name")

    assert not key_path.exists()
    provision_deps.conn.close.assert_not_called()


def test_provision_resume_without_key_file_fails_fast_without_polling(provision_deps, tmp_path):
    key_path = tmp_path / "key.p8"
    provision_deps.dict_cursor.fetchall.return_value = [{"name": "SNOWCAP_TEST"}]

    with pytest.raises(click.ClickException, match="no admin key was found"):
        _provision(key_path)

    provision_deps.poll_for_connection.assert_not_called()
