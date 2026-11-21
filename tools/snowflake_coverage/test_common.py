import common
import pytest
from common import fill, save_inventory, secret, select_objects


def test_saved_results_hold_secret_placeholders_not_run_values(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "INVENTORY", tmp_path / "inventory.yaml")
    monkeypatch.setattr(common, "_secrets", {})
    placeholders = ["{secret:first}", "{secret:second}", "{secret:third}"]
    inputs = {
        "create_args": {"field": placeholders[0], placeholders[1]: "key"},
        "setup": [f"CREATE SECRET S TYPE = GENERIC_STRING SECRET_STRING = '{placeholders[2]}' -- {{database}}"],
    }
    filled = fill(inputs, "DB")
    values = [
        filled["create_args"]["field"],
        *(filled["create_args"].keys() - {"field"}),
        filled["setup"][0].split("'")[1],
    ]
    assert len(set(values)) == 3
    assert fill(placeholders[0], "DB") == values[0]
    assert "{secret:" not in str(filled) and filled["setup"][0].endswith("-- DB")

    # Results repeat run values inside rendered SQL, error text, and the repr of dicts.
    save_inventory({"evidence": {"sql": filled["setup"], "error": f"rejects {filled['create_args']!r}", values[0]: 1}})

    saved = common.INVENTORY.read_text()
    assert not any(value in saved for value in values)
    assert all(placeholder in saved for placeholder in placeholders)


def test_testable_rows_skips_a_sampled_row_that_snowcap_does_not_model():
    obj = {
        "snowcap": {"class": "Warehouse"},
        "properties": [
            {"snowcap": {"field": "comment", "sample": ["a", "b"]}},
            # A contributor adds a sample to a row before snowcap models it.
            {"snowcap": {"field": None, "sample": ["a", "b"]}},
        ],
    }

    assert common.testable_rows(obj) == obj["properties"][:1]


def test_select_objects_refuses_a_label_that_names_no_modeled_object():
    inventory = {
        "objects": [
            {"object": "TABLE", "variant": None, "snowcap": {"class": "Table"}},
            {"object": "SERVICE", "variant": None},
        ]
    }

    assert [o["object"] for o in select_objects(inventory, ["TABLE"])] == ["TABLE"]
    with pytest.raises(SystemExit, match="SERVICE, TABLES"):
        select_objects(inventory, ["TABLE", "TABLES", "SERVICE"])


def test_secret_refuses_a_name_past_the_values_it_can_encode(monkeypatch):
    monkeypatch.setattr(common, "_secrets", {})
    values = [secret(f"name_{i}") for i in range(61)]

    assert all(v.isalnum() for v in values) and secret("name_0") == values[0]
    with pytest.raises(ValueError, match="61"):
        secret("name_61")
