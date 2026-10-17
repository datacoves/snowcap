"""
Run L2 to L5 on the test account for each inventory row that has samples, and write the results
into inventory.yaml.

For each row the script creates one object with the first sample value, then:

- L2 Created: Snowflake reports the declared value (truth queries). Without a truth key, L2 passes
  only through L3.
- L3 Fetched: snowcap's fetch, read through the spec as plan does, equals Snowflake's value (truth
  queries) or the declared value. When Snowflake reports no value, L3 is untested.
- L4 Updated: a change to the second sample value lands in Snowflake. When ALTER cannot change the
  property (docs `alter: none`), L4 passes only if snowcap refuses the change with an error before
  it sends SQL. A refusal is an error that snowcap raises on purpose: a bare Exception,
  NotImplementedError, or a snowcap.exceptions class, raised outside snowcap's fetch code. Any other
  error is a crash and fails.
- L5 Round trip: plan shows no change after the create and after the change. When snowcap
  correctly refuses the change, nothing is applied, so L5 is n/a.

A nested row (`columns[].comment`, `credentials.aws_key_id`) declares and plans the field its path heads,
and L3 compares the values the path names. A tags row tests the TagReference that snowcap plans for the object.

It also runs one combined test per object: every combinable SET property changes in one plan, and
each value must land.

Every object runs in its own throwaway database. The object's setup SQL runs in it first. Account-level test
objects are dropped after each row. The final cleanup runs also on failure: it drops the remaining test objects,
then the database, then runs the object's teardown SQL, which drops the account-level prerequisites that setup
created.

Usage: uv run python tools/snowflake_coverage/live_test.py [OBJECT[:variant] ...]
"""

import contextlib
import copy
import datetime
import io
import re
import subprocess
import sys
import traceback
import uuid
from pathlib import Path

import snowflake.connector

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from common import (  # noqa: E402
    build_resource,
    fill,
    live_inputs_digest,
    load_inventory,
    object_label,
    path_steps,
    replace_text,
    resource_class,
    save_inventory,
    select_objects,
    testable_rows,
    top_field,
)
from conftest import TEST_ROLE, TEST_WAREHOUSE, connection_params  # noqa: E402

from snowcap import data_provider, lifecycle  # noqa: E402
from snowcap.blueprint import (  # noqa: E402
    Blueprint,
    CreateResource,
    TransferOwnership,
    UpdateResource,
    compile_plan_to_sql,
)
from snowcap.client import reset_cache  # noqa: E402
from snowcap.resources import AccountParameter  # noqa: E402
from snowcap.resources.tag import tag_reference_for_resource  # noqa: E402
from snowcap.scope import AccountScope  # noqa: E402

BOOLEAN_WORDS = {"y": "true", "yes": "true", "on": "true", "n": "false", "no": "false", "off": "false"}


class Refused(Exception):
    """snowcap stopped the change on purpose, with its own error, before it sent SQL."""


class Crashed(Exception):
    """snowcap failed with an internal error, for example a KeyError in an update handler, or its resource class
    rejected a value that Snowflake accepts."""


def snowcap_error(err: Exception) -> Exception:
    """Classify an error that snowcap raised while it planned or compiled a change.

    A read failure is never a refusal: data_provider raises bare Exception for fetches it does not implement
    (partner security integration oauth_client), and that must not pass L4.
    """
    deliberate = type(err) in (Exception, NotImplementedError) or type(err).__module__ == "snowcap.exceptions"
    reading = any(
        frame.f_globals.get("__name__") == "snowcap.data_provider" for frame, _ in traceback.walk_tb(err.__traceback__)
    )
    return (Refused if deliberate and not reading else Crashed)(short(err))


class NotPlanned(Exception):
    """A changed value produced no change for the field in the plan."""


def short(err: BaseException) -> str:
    text = " ".join(str(err).split())
    return f"{type(err).__name__}: {text[:300]}"


def norm(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    text = str(value).strip().strip("'\"").lower().replace("_", " ")
    return BOOLEAN_WORDS.get(text, text)


class Account:
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor(snowflake.connector.DictCursor)
        session = data_provider.fetch_session(conn)
        self.edition = session["account_edition"]
        self.version = session["version"]
        # The facts every live result depends on.
        self.facts = {
            "edition": str(session["account_edition"]),
            "cloud": str(session["cloud"]),
            "cloud_region": session["cloud_region"],
            "test_role": str(session["role"]),
        }
        self.locator = session["account_locator"]
        # Account-level test objects, which the throwaway database drop does not remove.
        self.account_objects: dict[str, object] = {}

    def redact(self, value):
        """The results are published, and snowcap urns and generated ids carry the account locator."""
        return replace_text(value, self.locator, "<account>")

    def plan(self, resource) -> tuple[Blueprint, list]:
        blueprint = Blueprint(resources=[resource])
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                return blueprint, blueprint.plan(self.conn)
        except snowflake.connector.errors.Error:
            raise
        except Exception as err:
            raise snowcap_error(err) from err

    def sql(self, plan: list) -> list[str]:
        """The SQL that apply would run."""
        try:
            # Some update handlers pop fields off change.delta, so compile a copy and leave the plan for apply.
            compiled, _ = compile_plan_to_sql(data_provider.fetch_session(self.conn), copy.deepcopy(plan))
        except Exception as err:
            raise snowcap_error(err) from err
        return [command for change in compiled for command in change["commands"]]

    def apply(self, blueprint: Blueprint, plan: list) -> None:
        """Run the plan. sql() already compiled it, so any snowcap error here comes after the SQL started."""
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                blueprint.apply(self.conn, plan)
        except snowflake.connector.errors.Error:
            raise
        except Exception as err:
            raise Crashed(short(err)) from err
        finally:
            self.cur.execute(f"USE ROLE {TEST_ROLE}")

    def create(self, resource) -> list[str]:
        blueprint, plan = self.plan(resource)
        sql = self.sql(plan)
        if isinstance(type(resource).scope, AccountScope):
            self.account_objects[str(resource.urn)] = resource
        self.apply(blueprint, plan)
        return sql

    def clean_up(self, teardown: list[str]) -> None:
        """Drop the account-level test objects with snowcap's own DROP SQL, newest first, then run teardown SQL.

        Every statement is attempted. Raises afterward when one failed, because its object is left in the account.
        """
        drops = [
            lifecycle.drop_resource(resource.urn, resource.to_dict(self.edition), if_exists=True)
            for resource in reversed(list(self.account_objects.values()))
        ]
        self.account_objects.clear()
        failed = []
        for statement in drops + teardown:
            try:
                self.cur.execute(statement)
            except snowflake.connector.errors.Error as err:
                failed.append(f"{statement}: {short(err)}")
        if failed:
            raise RuntimeError("cleanup left account objects behind: " + "; ".join(failed))

    def truth(self, obj: dict, resource, database: str) -> dict:
        """Snowflake's own view of the object, from the inventory's truth queries."""
        name = str(resource.name).strip('"')
        values = {
            "name": name,
            "database": database,
            "container": f"{database}.PUBLIC" if "schema" in obj["scope_level"] else database,
            "fqn": str(resource.fqn),
        }
        truth = {}
        for query in obj["snowcap"]["truth"]:
            # Plain replacement, because truth SQL can contain literal braces.
            for key, value in values.items():
                query = query.replace(f"{{{key}}}", value)
            for row in self.cur.execute(query).fetchall():
                row = {k.lower(): v for k, v in row.items()}
                if "key" in row and "value" in row:
                    truth[row["key"].lower()] = row["value"]
                elif "property" in row and "value" in row:
                    truth[row["property"].lower()] = row["value"]
                elif str(row.get("name", "")).upper() == name.upper():
                    truth.update(row)
        return truth

    def fetched(self, resource):
        """The field values snowcap reads back, normalized through the spec the way plan does."""
        reset_cache()
        try:
            data = data_provider.fetch_resource(self.conn, resource.urn)
            return None if data is None else type(resource).spec(**data).to_dict(self.edition)
        except snowflake.connector.errors.Error:
            raise
        except Exception as err:
            raise Crashed(short(err)) from err


def declare(obj: dict, name: str, database: str, values: dict):
    """The snowcap resource for the test object. A sample is a value Snowflake accepts, so a constructor that
    rejects it (object_storage table_format DELTA) is a snowcap failure, recorded like any other crash."""
    try:
        return build_resource(obj, name, database, TEST_ROLE, values)
    except Exception as err:
        raise Crashed(f"snowcap rejects the declaration: {short(err)}") from err


def unique_name(obj: dict, database: str, label: str) -> str:
    """An account-level object outlives the throwaway database, so its name carries the database name to stay unique."""
    return f"{database}_{label}" if isinstance(resource_class(obj).scope, AccountScope) else label


def subject(resource, path: str) -> tuple:
    """The resource and spec field that hold a row's value in a plan and a fetch.

    snowcap plans tags as a separate TagReference, and a nested path such as `columns[].comment` lives in its head field.
    """
    if path == "tags":
        return tag_reference_for_resource(resource, resource.tags), "tags"
    return resource, top_field(path)


def leaf(value, path: str):
    """The part of a head field's value that a row path names: `columns[].comment` gives each column's comment.

    Other parts stay out of the comparison, for example a view column's data_type, which the declaration leaves
    unknown until apply.
    """

    def pick(value, steps: list[str]):
        if not steps or value is None:
            return value
        if steps[0] == "[]":
            return [pick(item, steps[1:]) for item in value]
        return pick(value.get(steps[0]), steps[1:])

    return pick(value, path_steps(path)[1:])


def read_back(account: Account, resource, path: str) -> tuple:
    """The row's value as the resource declares it and as snowcap fetches it."""
    held_by, field = subject(resource, path)
    fetched = (account.fetched(held_by) or {}).get(field)
    return leaf(held_by.to_dict(account.edition)[field], path), leaf(fetched, path)


def is_plain(path: str) -> bool:
    """A top-level spec field, whose fetched value compares directly to the value Snowflake reports."""
    return path != "tags" and path == top_field(path)


def is_for(change, resource) -> bool:
    # resource.urn has an empty account locator and plan URNs carry the real one, so compare the rest.
    return change.urn.resource_type == resource.resource_type and change.urn.fqn == resource.urn.fqn


def changes_for(plan: list, resource) -> list[str]:
    described = []
    for change in plan:
        if not is_for(change, resource):
            continue
        if isinstance(change, UpdateResource):
            described.append(f"update {sorted(change.delta)}")
        else:
            described.append(type(change).__name__)
    return described


def touches(plan: list, resource, field: str) -> bool:
    """Whether the plan sends the field's value. A create sends every field: snowcap plans one when its fetch
    misses the existing object, for example a TagReference it cannot read back."""
    return any(
        is_for(change, resource)
        and (
            isinstance(change, CreateResource)
            or (isinstance(change, UpdateResource) and field in change.delta)
            or (field == "owner" and isinstance(change, TransferOwnership))
        )
        for change in plan
    )


def drift(plan: list, resource, path: str) -> list[str]:
    """The changes a plan still holds for the resource or for the subject that holds the row's value."""
    held_by = subject(resource, path)[0]
    return changes_for(plan, resource) + (changes_for(plan, held_by) if held_by is not resource else [])


def plan_drift(account: Account, resource, path: str) -> list[str]:
    """The drift of a new plan for the resource. A plan that snowcap stops is drift too: a fetch crash, or a refusal
    such as a forced replacement when the fetch misreads a field that cannot change."""
    try:
        return drift(account.plan(resource)[1], resource, path)
    except Refused as err:
        return [f"plan refuses: {err}"]
    except Crashed as err:
        return [f"plan crashes: {err}"]


def landed(account: Account, obj: dict, resource, database: str, path: str, expected, truth_key) -> tuple[bool, str]:
    """Whether Snowflake holds the row's value. Uses the truth query when the row has a key."""
    if truth_key:
        reported = account.truth(obj, resource, database).get(truth_key)
        return norm(reported) == norm(expected), f"Snowflake reports {truth_key}={reported!r}"
    declared, fetched = read_back(account, resource, path)
    return fetched == declared, f"snowcap fetches {fetched!r}"


def test_row(account: Account, obj: dict, row: dict, database: str, index: int) -> None:
    meta = row["snowcap"]
    path, truth_key = meta["field"], meta.get("truth")
    before, after = fill(meta["sample"], database)
    expect = fill(meta.get("expect", meta["sample"]), database)
    if resource_class(obj) is AccountParameter:
        # Each row is one account parameter, and the resource name is the parameter.
        name = row["name"].upper()
    else:
        name = unique_name(obj, database, f"R{index:02d}_{re.sub(r'[^A-Za-z0-9]', '_', row['name']).upper()}")
    levels = meta.setdefault("levels", {})
    evidence, failures = {}, {}
    meta["evidence"], meta["failures"] = evidence, failures

    def fail(level: str, kind: str, detail: str) -> None:
        levels[level], evidence[level], failures[level] = "fail", detail, kind

    def make(value):
        # A blueprint finalizes its resources, so each plan needs a new instance.
        return declare(obj, name, database, {top_field(path): value})

    try:
        resource = make(before)
        evidence["create_sql"] = account.create(resource)
    except (Refused, Crashed, snowflake.connector.errors.Error) as err:
        levels.update(L3="untested", L4="untested", L5="untested")
        fail("L2", "create", f"create failed: {short(err)}")
        return

    reported = account.truth(obj, resource, database).get(truth_key) if truth_key else None
    if truth_key:
        # L2 and L3 are separate checks against Snowflake's own value.
        if norm(reported) == norm(expect[0]):
            levels["L2"] = "pass"
        else:
            fail("L2", "create", f"declared {before!r}, Snowflake reports {truth_key}={reported!r}")
    try:
        declared, fetched = read_back(account, resource, path)
    except Crashed as err:
        fail("L3", "fetch_crash", f"snowcap fetch crashes: {err}")
    else:
        if not truth_key or (not is_plain(path) and levels["L2"] == "pass"):
            # No truth for the path's values: either no truth key at all, or a truth that reports one value of a
            # nested or tag path, which CREATE wrote. Compare the path's values to the declaration.
            if fetched == declared:
                levels["L3"] = "pass"
            else:
                fail("L3", "not_read", f"declared {declared!r}, snowcap fetches {fetched!r}")
        elif is_plain(path) and reported is not None:
            # Snowflake can spell a value differently from the declaration (warehouse_size 'X-Small' for 'XSMALL'),
            # so a reported value that `expect` lists compares in its sample form.
            reference = {norm(e): s for e, s in zip(expect, (before, after))}.get(norm(reported), reported)
            if norm(fetched) == norm(reference):
                levels["L3"] = "pass"
            else:
                fail(
                    "L3",
                    "not_read",
                    f"Snowflake reports {truth_key}={reported!r} (declared form {reference!r}), "
                    f"snowcap fetches {fetched!r}",
                )
        elif is_plain(path):
            levels["L3"] = "untested"
            evidence["L3"] = f"Snowflake reports no {truth_key}, so the read-back has no reference"
        else:
            levels["L3"] = "untested"
            evidence["L3"] = "CREATE did not write the value, so the read-back of the path has no reference"
    if not truth_key:
        # Without an independent read, a matching fetch is the only proof that CREATE wrote the value.
        levels["L2"] = "pass" if levels["L3"] == "pass" else "untested"

    drift_after_create = plan_drift(account, make(before), path)

    # When CREATE did not write the first value, Snowflake can already hold the second one, so a
    # change toward it is a no-op. Change toward the first value instead: Snowflake does not hold it.
    target, expected = (before, expect[0]) if levels["L2"] == "fail" else (after, expect[1])
    applied = False
    try:
        changed = make(target)
        blueprint, plan = account.plan(changed)
        if not touches(plan, *subject(changed, path)):
            raise NotPlanned(f"a change to {target!r} plans {drift(plan, changed, path) or 'nothing'}")
        evidence["L4_sql"] = account.sql(plan)
        account.apply(blueprint, plan)
        applied = True
        ok, detail = landed(account, obj, changed, database, path, expected, truth_key)
        if not ok:
            fail("L4", "not_landed", f"apply ran but the change did not land: {detail}")
        else:
            levels["L4"] = "pass"
            if row["alter"] == "none":
                evidence["L4"] = (
                    "the docs say ALTER cannot change it, but snowcap's change SQL (L4_sql) ran and the change landed"
                )
    except Refused as err:
        if row["alter"] == "none":
            levels["L4"], evidence["L4"] = "pass", f"snowcap refuses: {err}"
        else:
            fail("L4", "refuses_supported", f"snowcap refuses a change ALTER supports: {err}")
    except Crashed as err:
        fail("L4", "update_crash", f"snowcap crashes: {err}")
    except snowflake.connector.errors.Error as err:
        fail("L4", "bad_sql", f"Snowflake rejects the SQL: {short(err)}")
    except NotPlanned as err:
        fail("L4", "not_planned", str(err))

    drift_after_change = plan_drift(account, make(target), path) if applied else []
    if drift_after_create or drift_after_change:
        fail("L5", "drift", f"plan after create: {drift_after_create}; plan after change: {drift_after_change}")
    elif not applied and levels["L4"] == "pass":
        levels["L5"] = "n/a"
        evidence["L5"] = "no round trip: the property is immutable and snowcap correctly refuses the change"
    else:
        levels["L5"] = levels["L4"]
        if levels["L5"] == "fail":
            evidence["L5"] = "fails because L4 fails"


def test_combined(account: Account, obj: dict, database: str) -> dict:
    """Change every combinable SET property in one plan, and report the values that did not land.

    Only top-level fields take part: two nested rows can share one head field, so their samples cannot combine.
    Rows that share a field, such as the `value` of each account parameter, count as one field.
    """
    rows = [
        r
        for r in testable_rows(obj)
        if r["alter"] == "set" and r["combinable"] is True and is_plain(r["snowcap"]["field"])
    ]
    before = {r["snowcap"]["field"]: r["snowcap"]["sample"][0] for r in rows}
    after = {r["snowcap"]["field"]: r["snowcap"]["sample"][1] for r in rows}
    result = {"fields": sorted(after)}
    if len(after) < 2:
        return result | {"result": "untested (needs two or more fields)"}
    name = unique_name(obj, database, "COMBINED")
    try:
        account.create(declare(obj, name, database, before))
        changed = declare(obj, name, database, after)
        blueprint, plan = account.plan(changed)
        result["sql"] = account.sql(plan)
        account.apply(blueprint, plan)
    except (Refused, Crashed, snowflake.connector.errors.Error) as err:
        result["result"] = "fail"
        result["error"] = short(err)
        return result
    missed = [
        r["snowcap"]["field"]
        for r in rows
        if not landed(
            account,
            obj,
            changed,
            database,
            r["snowcap"]["field"],
            fill(r["snowcap"].get("expect", r["snowcap"]["sample"])[1], database),
            r["snowcap"].get("truth"),
        )[0]
    ]
    result["result"] = "fail" if missed else "pass"
    if missed:
        result["missed"] = missed
    return result


def test_object(account: Account, obj: dict, snowcap_commit: str) -> None:
    label = object_label(obj)
    database = f"COVERAGE_{re.sub(r'[^A-Za-z0-9]', '_', label).upper()}_{uuid.uuid4().hex[:8].upper()}"
    account.cur.execute(f"CREATE DATABASE {database}")
    try:
        for statement in fill(obj["snowcap"].get("setup", []), database):
            account.cur.execute(statement)
        for index, row in enumerate(testable_rows(obj)):
            test_row(account, obj, row, database, index)
            row["snowcap"]["evidence"] = account.redact(row["snowcap"]["evidence"])
            # Some account types allow one object per account (partner OAuth integrations), and others hold
            # account-unique values (a user's login_name), so each row's account object must go before the next.
            account.clean_up([])
            levels = row["snowcap"]["levels"]
            print(f"{label}.{row['name']}: " + " ".join(f"{k}={v}" for k, v in levels.items()), flush=True)
        obj["snowcap"]["combined"] = account.redact(test_combined(account, obj, database))
        obj["snowcap"]["tested"] = {
            "inputs": live_inputs_digest(obj),
            "snowcap_commit": snowcap_commit,
            "snowflake_version": account.version,
            "checked_on": datetime.date.today().isoformat(),
        }
        print(f"{label} combined: {obj['snowcap']['combined']['result']}", flush=True)
    finally:
        account.cur.execute(f"USE ROLE {TEST_ROLE}")
        # CREATE WAREHOUSE makes the new warehouse current, and the queries after its drop need a warehouse.
        if TEST_WAREHOUSE:
            account.cur.execute(f"USE WAREHOUSE {TEST_WAREHOUSE}")
        # Dependents go first. Database objects can hold setup's account-level prerequisites (an OAuth secret
        # holds its security integration), and Snowflake drops a database whose objects an integration references.
        account.clean_up([f"DROP DATABASE IF EXISTS {database}", *fill(obj["snowcap"].get("teardown", []), database)])


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True, check=True).stdout.strip()


def main(labels: list[str]) -> None:
    # Each result records the snowcap commit it ran on, which is true only when the package has no local changes.
    if changes := git("status", "--porcelain", "--", "snowcap"):
        sys.exit(f"Commit the snowcap changes before a live run, so the results name the code they tested:\n{changes}")
    snowcap_commit = git("rev-parse", "--short", "HEAD")
    inventory = load_inventory()
    conn = snowflake.connector.connect(**connection_params())
    try:
        conn.cursor().execute("ALTER SESSION SET QUERY_TAG = 'snowcap_coverage_audit'")
        account = Account(conn)
        inventory["account"] = account.facts
        for obj in select_objects(inventory, labels):
            if obj["snowcap"]["test"] == "live":
                test_object(account, obj, snowcap_commit)
                save_inventory(inventory)
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
