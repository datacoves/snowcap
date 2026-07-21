import base64
import os
import pathlib
import re
import secrets
import string
import subprocess
import time
from datetime import datetime, timezone

import click
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from dotenv import dotenv_values

from snowcap.blueprint import Blueprint, print_plan
from snowcap.blueprint_config import print_blueprint_config
from snowcap.data_provider import fetch_region, fetch_session
from snowcap.enums import AccountCloud, AccountEdition, ResourceType
from snowcap.gitops import (
    collect_blueprint_config,
    collect_vars_from_environment,
    merge_configs,
    read_config,
)
from snowcap.parse import parse_region

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()
REPO_ROOT = SCRIPT_DIR.parent

# Shared default so `provision --name` and `drop --name` always agree.
DEFAULT_ACCOUNT_NAME = "SNOWCAP_TEST"

# Snowflake's unquoted-identifier grammar; also doubles as our SQL-injection guard.
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

# The enums are the single source of truth for the valid CREATE ACCOUNT value sets.
VALID_EDITIONS = {e.value for e in AccountEdition}

POLL_INTERVAL_SECONDS = 10
POLL_TIMEOUT_SECONDS = 600


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


def get_org_connection():
    required = ["SNOWFLAKE_ORG_ACCOUNT", "SNOWFLAKE_ORG_USER", "SNOWFLAKE_ORG_PASSWORD"]
    missing = [var for var in required if var not in os.environ]
    if missing:
        raise click.ClickException(f"Missing required environment variable(s): {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ORG_ACCOUNT"],
        user=os.environ["SNOWFLAKE_ORG_USER"],
        password=os.environ["SNOWFLAKE_ORG_PASSWORD"],
        role="ORGADMIN",
    )


def read_test_account_config(config_path: str):
    config = read_config(f"{SCRIPT_DIR}/test_account_configs/{config_path}")
    return config or {}


def get_config(session_ctx):
    config = read_test_account_config("base.yml")

    if session_ctx["account_edition"] == AccountEdition.ENTERPRISE:
        config = merge_configs(config, read_test_account_config("enterprise.yml"))
    elif session_ctx["account_edition"] == AccountEdition.BUSINESS_CRITICAL:
        config = merge_configs(config, read_test_account_config("business_critical.yml"))

    if session_ctx["cloud"] == AccountCloud.AWS:
        config = merge_configs(config, read_test_account_config("aws.yml"))
    elif session_ctx["cloud"] == AccountCloud.GCP:
        config = merge_configs(config, read_test_account_config("gcp.yml"))
    elif session_ctx["cloud"] == AccountCloud.AZURE:
        config = merge_configs(config, read_test_account_config("azure.yml"))
    else:
        raise ValueError(f"Unknown cloud: {session_ctx['cloud']}")

    if session_ctx["cloud"] == AccountCloud.AWS and session_ctx["account_edition"] != AccountEdition.STANDARD:
        config = merge_configs(config, read_test_account_config("compute_pools.yml"))

    return config


def reset_test_account(conn=None, snowcap_vars=None):
    conn = conn if conn is not None else get_connection()
    session_ctx = fetch_session(conn)
    config = get_config(session_ctx)
    snowcap_vars = snowcap_vars if snowcap_vars is not None else collect_vars_from_environment()
    blueprint_config = collect_blueprint_config(config, {"vars": snowcap_vars})
    print_blueprint_config(blueprint_config)

    bp = Blueprint.from_config(blueprint_config)
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


def teardown_test_account():
    conn = get_connection()
    session_ctx = fetch_session(conn)
    config = get_config(session_ctx)
    snowcap_vars = collect_vars_from_environment()
    blueprint_config = collect_blueprint_config(config, {"vars": snowcap_vars})
    # will break when BlueprintConfig is frozen
    blueprint_config.resources = []
    blueprint_config.sync_resources = [
        item
        for item in blueprint_config.sync_resources
        if item not in [ResourceType.USER, ResourceType.ROLE_GRANT, ResourceType.WAREHOUSE]
    ]
    print_blueprint_config(blueprint_config)

    bp = Blueprint.from_config(blueprint_config)
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validate_identifier(value: str) -> str:
    if not IDENTIFIER_PATTERN.match(value):
        raise click.ClickException(f"Invalid identifier {value!r}: must match {IDENTIFIER_PATTERN.pattern}")
    return value


def _archive_if_exists(path: pathlib.Path, suffix: str) -> pathlib.Path | None:
    """Rename an existing file to a timestamped sibling before it gets overwritten, returning the new path."""
    if not path.exists():
        return None
    archived_path = path.with_name(f"{path.name}.{suffix}.{_utc_timestamp()}")
    path.rename(archived_path)
    return archived_path


def create_account_sql(
    name: str,
    admin_name: str,
    admin_rsa_public_key: str,
    email: str,
    edition: str,
    region: str,
    region_group: str | None = None,
) -> str:
    _validate_identifier(name)
    _validate_identifier(admin_name)
    _validate_identifier(region)
    if region_group:
        _validate_identifier(region_group)
    if edition not in VALID_EDITIONS:
        raise click.ClickException(f"Invalid edition {edition!r}: must be one of {sorted(VALID_EDITIONS)}")

    lines = [
        f"CREATE ACCOUNT {name}",
        f"ADMIN_NAME = {_quote(admin_name)}",
        f"ADMIN_RSA_PUBLIC_KEY = {_quote(admin_rsa_public_key)}",
        "ADMIN_USER_TYPE = SERVICE",
        f"EMAIL = {_quote(email)}",
        f"EDITION = {edition}",
    ]
    if region_group:
        lines.append(f"REGION_GROUP = {region_group}")
    lines.append(f"REGION = {region}")
    return "\n".join(lines)


def drop_account_sql(name: str, grace_period_in_days: int) -> str:
    _validate_identifier(name)
    return f"DROP ACCOUNT {name} GRACE_PERIOD_IN_DAYS = {grace_period_in_days}"


def resolve_region(
    detected_cloud: str,
    detected_region: str,
    cloud_override: str | None,
    region_override: str | None,
) -> str:
    if cloud_override and not region_override:
        raise click.ClickException("--cloud requires --region to also be specified")
    cloud = cloud_override.upper() if cloud_override else detected_cloud
    region = region_override or detected_region
    return f"{cloud}_{region}"


def render_test_env(account_identifier: str, admin_user: str, private_key_path: str, fixture_vars: dict) -> str:
    lines = [
        f"TEST_SNOWFLAKE_ACCOUNT={account_identifier}",
        f"TEST_SNOWFLAKE_USER={admin_user}",
        "TEST_SNOWFLAKE_ROLE=ACCOUNTADMIN",
        "TEST_SNOWFLAKE_WAREHOUSE=STATIC_WAREHOUSE",
        f"TEST_SNOWFLAKE_PRIVATE_KEY_PATH={private_key_path}",
        "",
    ]
    lines += [f"VAR_{key.upper()}={value}" for key, value in fixture_vars.items()]
    return "\n".join(lines) + "\n"


def generate_rsa_keypair(key_path: pathlib.Path | None = None) -> str:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    if key_path is not None:
        pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(pem)

    public_der = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return base64.b64encode(public_der).decode("ascii")


def _generate_mfa_password(length: int = 20) -> str:
    alphabet = string.ascii_letters + string.digits
    while True:
        password = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(c.islower() for c in password)
            and any(c.isupper() for c in password)
            and any(c.isdigit() for c in password)
        ):
            return password


def poll_for_connection(
    account: str,
    user: str,
    private_key_path,
    timeout: int = POLL_TIMEOUT_SECONDS,
    interval: int = POLL_INTERVAL_SECONDS,
):
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return snowflake.connector.connect(
                account=account,
                user=user,
                private_key_file=str(private_key_path),
                role="ACCOUNTADMIN",
            )
        except snowflake.connector.errors.Error as e:
            last_error = e
            time.sleep(interval)
    raise click.ClickException(f"Timed out after {timeout}s waiting for {account} to accept connections: {last_error}")


def provision_test_account(name, email, edition, cloud, region, admin_name, key_path):
    key_path = pathlib.Path(key_path)

    # Validate every input feeding create_account_sql up front, before any file or ORGADMIN
    # mutation, so a typo can never destroy a working key with no way back.
    _validate_identifier(name)
    _validate_identifier(admin_name)

    conn = get_connection()
    try:
        session_ctx = fetch_session(conn)
        region_row = fetch_region(conn)
    finally:
        conn.close()
    detected_region_group = parse_region(next(iter(region_row.values()))).get("region_group")
    composed_region = resolve_region(session_ctx["cloud"], session_ctx["cloud_region"], cloud, region)
    resolved_edition = (edition or session_ctx["account_edition"].value).upper()
    if resolved_edition not in VALID_EDITIONS:
        raise click.ClickException(f"Invalid edition {resolved_edition!r}: must be one of {sorted(VALID_EDITIONS)}")

    org_conn = get_org_connection()
    try:
        org_cursor = org_conn.cursor()
        org_name = org_cursor.execute("SELECT CURRENT_ORGANIZATION_NAME()").fetchone()[0]
        target_identifier = f"{org_name}-{name}"

        dict_cursor = org_conn.cursor(snowflake.connector.DictCursor)
        dict_cursor.execute("SHOW ACCOUNTS")
        existing_names = {row["name"].upper() for row in dict_cursor.fetchall()}
        account_exists = name.upper() in existing_names

        key_backup_path = None
        if account_exists:
            click.echo(f"Account {target_identifier} already exists — resuming (skipping CREATE ACCOUNT).")
            if not key_path.exists():
                raise click.ClickException(
                    f"Account {target_identifier} exists but no admin key was found at {key_path}. "
                    "Recover the key out-of-band, or run "
                    f"`python tools/manage_test_account.py drop --name {name}` and recreate the account."
                )
        else:
            key_backup_path = _archive_if_exists(key_path, "bak")
            if key_backup_path:
                click.echo(f"Backed up existing key to {key_backup_path}")
            admin_rsa_public_key = generate_rsa_keypair(key_path)
            sql = create_account_sql(
                name, admin_name, admin_rsa_public_key, email, resolved_edition, composed_region,
                detected_region_group,
            )
            org_cursor.execute(sql)
            click.echo(f"CREATE ACCOUNT issued for {target_identifier}")
    finally:
        org_conn.close()

    recovery_hint = (
        f"Recovery: re-run `python tools/manage_test_account.py provision --name {name} --email {email}` to "
        f"resume, or `python tools/manage_test_account.py drop --name {name}` to abandon this account."
    )

    new_conn = None
    env_backup_path = None
    try:
        click.echo(f"Waiting for {target_identifier} to accept connections (up to {POLL_TIMEOUT_SECONDS}s)...")
        new_conn = poll_for_connection(target_identifier, admin_name, key_path)

        # Explicit, generated-only bootstrap vars — never sourced from the contributor's environment.
        fixture_vars = {
            "static_user_rsa_public_key": generate_rsa_keypair(None),
            "static_user_mfa_password": _generate_mfa_password(),
            "storage_base_url": "s3://snowcap-test-placeholder/",
            "storage_role_arn": "arn:aws:iam::000000000000:role/snowcap-test-placeholder",
            "storage_aws_external_id": "snowcap-test",
        }
        reset_test_account(new_conn, fixture_vars)

        env_path = REPO_ROOT / "tests" / ".env"
        env_backup_path = _archive_if_exists(env_path, "bak")
        if env_backup_path:
            click.echo(f"Backed up existing tests/.env to {env_backup_path}")
        env_path.write_text(render_test_env(target_identifier, admin_name, str(key_path), fixture_vars))

        apply_script = REPO_ROOT / "tests" / "fixtures" / "static_resources" / "apply.sh"
        try:
            subprocess.run([str(apply_script)], check=True, cwd=REPO_ROOT, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise click.ClickException(f"apply.sh failed (exit {e.returncode}):\n{e.stdout}\n{e.stderr}")
    except Exception as e:
        click.echo(recovery_hint, err=True)
        if isinstance(e, click.ClickException):
            raise
        raise click.ClickException(str(e)) from e
    finally:
        if new_conn is not None:
            new_conn.close()

    click.echo(f"Provisioned {target_identifier}")
    click.echo(f"Admin key: {key_path}")
    if key_backup_path:
        click.echo(f"Previous key backed up to: {key_backup_path}")
    if env_backup_path:
        click.echo(f"Previous tests/.env backed up to: {env_backup_path}")
    click.echo("Next: pytest tests/ --snowflake")


def drop_test_account(name, grace_period_in_days, yes):
    org_conn = get_org_connection()
    try:
        org_name = org_conn.cursor().execute("SELECT CURRENT_ORGANIZATION_NAME()").fetchone()[0]
        target_identifier = f"{org_name}-{name}"
        click.echo(f"Target account: {target_identifier}")

        env_path = REPO_ROOT / "tests" / ".env"
        env_matches_target = False
        if env_path.exists():
            env_matches_target = dotenv_values(env_path).get("TEST_SNOWFLAKE_ACCOUNT") == target_identifier
            if not env_matches_target and not yes:
                raise click.ClickException(
                    f"tests/.env's TEST_SNOWFLAKE_ACCOUNT does not match {target_identifier}; pass --yes to override."
                )
        elif not yes:
            raise click.ClickException("tests/.env not found; pass --yes to drop without a local-state check.")

        if not yes:
            click.confirm(f"Drop account {target_identifier}? This cannot be undone.", abort=True)

        org_conn.cursor().execute(drop_account_sql(name, grace_period_in_days))
    finally:
        org_conn.close()

    if env_matches_target:
        dropped_path = _archive_if_exists(env_path, "dropped")
        if dropped_path:
            click.echo(f"Moved tests/.env to {dropped_path}")

    click.echo(
        f"Dropped {target_identifier} with a {grace_period_in_days}-day grace period: it remains locked and "
        "still counts against your org's account quota until the grace period lapses, and is restorable via "
        "ORGADMIN until then."
    )


@click.group()
def main():
    pass


@main.command()
def reset():
    reset_test_account()


@main.command()
def teardown():
    teardown_test_account()


@main.command("teardown-and-reset")
def teardown_and_reset():
    teardown_test_account()
    reset_test_account()


@main.command()
@click.option("--name", default=DEFAULT_ACCOUNT_NAME, show_default=True, help="Account name to create or resume.")
@click.option("--email", required=True, help="Email address for the new account's admin user.")
@click.option(
    "--edition",
    type=click.Choice([e.value.lower() for e in AccountEdition]),
    default=None,
    help="Defaults to the edition detected from your current connection.",
)
@click.option(
    "--cloud",
    type=click.Choice([e.value.lower() for e in AccountCloud]),
    default=None,
    help="Defaults to the cloud detected from your current connection.",
)
@click.option(
    "--region",
    default=None,
    help="Cloud region id without the cloud prefix, e.g. US_WEST_2. Defaults to the detected region.",
)
@click.option("--admin-name", default="SNOWCAP_ADMIN", show_default=True, help="Admin user name on the new account.")
@click.option(
    "--key-path",
    default=str(REPO_ROOT / "tests" / ".snowcap_test_account_rsa_key.p8"),
    show_default=True,
    help="Where to write the new admin's private key.",
)
def provision(name, email, edition, cloud, region, admin_name, key_path):
    provision_test_account(name, email, edition, cloud, region, admin_name, key_path)


@main.command()
@click.option("--name", default=DEFAULT_ACCOUNT_NAME, show_default=True, help="Account name to drop.")
@click.option("--grace-period-in-days", type=click.IntRange(3, 90), default=3, show_default=True)
@click.option("--yes", is_flag=True, help="Skip the tests/.env agreement check and confirmation prompt.")
def drop(name, grace_period_in_days, yes):
    drop_test_account(name, grace_period_in_days, yes)


if __name__ == "__main__":
    main()
    reset_test_account()


if __name__ == "__main__":
    main()
