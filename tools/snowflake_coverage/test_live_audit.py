import types

import live_audit
import pytest
from common import LIVE_LEVELS
from live_audit import Account


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
