import common
from common import fill, save_inventory


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
