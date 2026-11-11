import types

import live_audit
import pytest
from common import LIVE_LEVELS
from live_audit import Account
from snowflake.connector.errors import ProgrammingError

from snowcap import data_provider
from snowcap.enums import AccountEdition, ResourceType
from snowcap.identifiers import URN, parse_FQN


class Cursor:
    def __init__(self, failing: set[str]):
        self.failing, self.ran = failing, []

    def execute(self, statement: str):
        self.ran.append(statement)
        if statement in self.failing:
            raise RuntimeError("Snowflake refuses")


class Resource:
    def __init__(self, urn: str, readable: bool = True):
        self.urn, self.readable = urn, readable

    def to_dict(self, edition):
        if not self.readable:
            raise KeyError("name")
        return {}


def test_clean_up_attempts_every_object_and_keeps_the_failed_ones(monkeypatch):
    monkeypatch.setattr(live_audit.lifecycle, "drop_resource", lambda urn, data, if_exists: f"DROP {urn}")
    account = Account.__new__(Account)
    account.edition, account.cur = None, Cursor(failing={"DROP B", "TEARDOWN 1"})
    resources = {urn: Resource(urn, readable=urn != "C") for urn in "ABCD"}
    account.account_objects = dict(resources)

    with pytest.raises(RuntimeError, match="cleanup left account objects behind"):
        account.clean_up(["TEARDOWN 1", "TEARDOWN 2"])

    assert account.cur.ran == ["DROP D", "DROP B", "DROP A", "TEARDOWN 1", "TEARDOWN 2"]
    assert account.account_objects == {"B": resources["B"], "C": resources["C"]}


def test_main_isolates_a_failed_object(monkeypatch, capsys):
    def obj(name: str) -> dict:
        return {
            "object": name,
            "variant": None,
            "snowcap": {"class": "Table", "test": "live", "tested": {"inputs": "x"}},
            "properties": [{"snowcap": {"levels": {"L2": "pass"}, "evidence": {"L2": "ok"}}}],
        }

    inventory = {"objects": [obj("A"), obj("B"), obj("C")]}
    ran, saves = [], []

    def test_object(account, o, snowcap_commit):
        ran.append((o["object"], dict(account.account_objects)))
        if o["object"] in "AC":
            account.account_objects[f"urn-{o['object']}"] = Resource(f"urn-{o['object']}")
            raise RuntimeError("cleanup left account objects behind")

    class Connection:
        def cursor(self):
            return Cursor(failing=set())

        def close(self):
            pass

    monkeypatch.setattr(live_audit, "git", lambda *args: "")
    monkeypatch.setattr(live_audit, "load_inventory", lambda: inventory)
    monkeypatch.setattr(live_audit, "connection_params", dict)
    monkeypatch.setattr(live_audit.snowflake.connector, "connect", lambda **params: Connection())
    monkeypatch.setattr(live_audit, "Account", lambda conn: types.SimpleNamespace(account_objects={}))
    monkeypatch.setattr(live_audit, "test_object", test_object)
    monkeypatch.setattr(live_audit, "save_inventory", lambda inv: saves.append(inv))

    with pytest.raises(SystemExit, match="A, C$"):
        live_audit.main([])

    # A failed object's leftovers do not reach the next object's cleanup, and the run names them.
    assert ran == [("A", {}), ("B", {}), ("C", {})] and len(saves) == 3
    assert "Left in the account, drop by hand: urn-A, urn-C" in capsys.readouterr().out
    a, b, _ = inventory["objects"]
    assert "tested" not in a["snowcap"] and a["properties"][0]["snowcap"] == {
        "levels": dict.fromkeys(LIVE_LEVELS, "untested")
    }
    assert b["snowcap"]["tested"] == {"inputs": "x"}


def test_redact_removes_account_data():
    account = Account.__new__(Account)
    account.locator = "AB12345"
    evidence = {"L3": "snowcap fetches {'storage_aws_external_id': 'AB12345_SFCRole=4_AAAAAAAAAAAAAAAAAAAAAAAAAAA='}"}

    assert account.redact(evidence) == {"L3": "snowcap fetches {'storage_aws_external_id': '<account>_SFCRole=<id>'}"}


class ScriptedAccount(Account):
    """An Account whose Snowflake side is scripted: each plan is empty, and Snowflake reports `reported`."""

    def __init__(self, plan_error=None, apply_error=None, reported=None):
        self.edition, self.account_objects = AccountEdition.ENTERPRISE, {}
        self.plan_error, self.apply_error, self.reported = plan_error, apply_error, reported

    def plan(self, resource):
        if self.plan_error:
            raise self.plan_error
        return None, []

    def sql(self, plan):
        return ["CREATE SQL"]

    def apply(self, blueprint, plan):
        if self.apply_error:
            raise self.apply_error

    def truth(self, obj, resource, database):
        # Snowflake reports what was declared, unless the test scripts another answer.
        return resource.to_dict(self.edition) if self.reported is None else self.reported

    def fetched(self, resource):
        return resource.to_dict(self.edition)


def warehouse(rows: list[tuple[str, list, list]]) -> dict:
    """A Warehouse object with a row (field, sample, expect) for each field."""
    return {
        "object": "WAREHOUSE",
        "variant": None,
        "scope_level": "account",
        "snowcap": {"class": "Warehouse", "test": "live", "create_args": {}, "truth": []},
        "properties": [
            {
                "name": f"{field}_{i}",
                "alter": "set",
                "combinable": True,
                "snowcap": {"field": field, "truth": field, "sample": sample, "expect": expect},
            }
            for i, (field, sample, expect) in enumerate(rows)
        ],
    }


@pytest.fixture(autouse=True)
def test_role(monkeypatch):
    monkeypatch.setattr(live_audit, "TEST_ROLE", "SYSADMIN")


def test_a_failed_read_while_planning_the_create_fails_l3_not_l2():
    obj = warehouse([("comment", ["a", "b"], ["a", "b"])])
    row = obj["properties"][0]
    read_error = live_audit.ReadFailed("ProgrammingError: Invalid value VIEW for argument OBJECT_TYPE")

    live_audit.test_row(ScriptedAccount(plan_error=read_error), obj, row, "DB", 0)

    meta = row["snowcap"]
    assert meta["levels"] == {"L2": "untested", "L3": "fail", "L4": "untested", "L5": "untested"}
    assert meta["failures"] == {"L3": "fetch_crash"}
    assert "OBJECT_TYPE" in meta["evidence"]["L3"]


def test_plan_reports_snowflake_errors_and_fetch_crashes_as_failed_reads(monkeypatch):
    def plans_with(error):
        class FailingBlueprint:
            def __init__(self, resources):
                pass

            def plan(self, conn):
                raise error()

        monkeypatch.setattr(live_audit, "Blueprint", FailingBlueprint)
        account = Account.__new__(Account)
        account.conn = None
        with pytest.raises(live_audit.ReadFailed):
            account.plan(None)

    plans_with(lambda: ProgrammingError("syntax error on: SELECT * FROM table(information_schema.tag_references())"))

    # data_provider has no fetch function for some resource types, so fetch_resource raises inside its own module.
    def fetch_crash():
        try:
            data_provider.fetch_resource(None, URN(ResourceType.EXTERNAL_FUNCTION, parse_FQN("DB.PUBLIC.F()"), ""))
        except Exception as err:
            return err

    plans_with(fetch_crash)


def test_a_create_that_snowflake_rejects_keeps_the_sql_it_sent():
    obj = warehouse([("comment", ["a", "b"], ["a", "b"])])
    row = obj["properties"][0]

    live_audit.test_row(ScriptedAccount(apply_error=ProgrammingError("rejected")), obj, row, "DB", 0)

    assert row["snowcap"]["failures"] == {"L2": "create"}
    assert row["snowcap"]["evidence"]["create_sql"] == ["CREATE SQL"]


def test_a_value_snowflake_does_not_report_never_passes():
    # The expected value NONE must not match a truth key that reports nothing.
    obj = warehouse([("comment", ["a", "b"], ["NONE", "NONE"])])
    row = obj["properties"][0]
    account = ScriptedAccount(reported={})

    live_audit.test_row(account, obj, row, "DB", 0)

    assert row["snowcap"]["levels"]["L2"] == "untested"
    ok, detail = live_audit.landed(account, obj, None, "DB", "comment", "NONE", "comment")
    assert ok is None and "reports no comment" in detail


def test_combined_leaves_out_rows_that_share_a_field():
    obj = warehouse(
        [
            ("comment", ["a", "b"], ["a", "b"]),
            ("auto_suspend", [60, 120], [60, 120]),
            # One declaration holds one value per field, so these two rows cannot both be applied.
            ("max_cluster_count", [1, 2], [1, 2]),
            ("max_cluster_count", [3, 4], [3, 4]),
        ]
    )

    result = live_audit.test_combined(ScriptedAccount(), obj, "DB")

    assert result["result"] == "pass"
    assert result["fields"] == ["auto_suspend", "comment"] and result["shared"] == ["max_cluster_count"]


def test_combined_of_rows_that_all_share_one_field_is_untested():
    # ACCOUNT: every account parameter row maps to `value`.
    obj = warehouse([("comment", ["a", "b"], ["a", "b"]), ("comment", ["c", "d"], ["c", "d"])])

    result = live_audit.test_combined(ScriptedAccount(), obj, "DB")

    assert result["result"].startswith("untested") and "share" in result["result"]


def test_account_parameter_restores_put_back_only_customized_values():
    class ShowParameters:
        rows = {
            "STATEMENT_TIMEOUT_IN_SECONDS": {
                "key": "STATEMENT_TIMEOUT_IN_SECONDS",
                "value": "3600",
                "level": "ACCOUNT",
                "type": "NUMBER",
            },
            "GEOGRAPHY_OUTPUT_FORMAT": {
                "key": "GEOGRAPHY_OUTPUT_FORMAT",
                "value": "GeoJSON",
                "level": "",
                "type": "STRING",
            },
            "TIMEZONE": {"key": "TIMEZONE", "value": "Europe/O'Brien", "level": "ACCOUNT", "type": "STRING"},
        }

        def execute(self, statement):
            name = statement.split("'")[1]
            # LIKE treats _ as a wildcard, so SHOW can return other parameters too.
            self.result = [self.rows[name], {"key": "OTHER", "value": "x", "level": "ACCOUNT", "type": "STRING"}]
            return self

        def fetchall(self):
            return self.result

    account = Account.__new__(Account)
    account.cur = ShowParameters()

    assert account.parameter_restores(list(ShowParameters.rows)) == [
        "ALTER ACCOUNT SET STATEMENT_TIMEOUT_IN_SECONDS = 3600",
        "ALTER ACCOUNT SET TIMEZONE = 'Europe/O''Brien'",
    ]
