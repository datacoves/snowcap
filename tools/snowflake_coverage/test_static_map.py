from static_map import fetched_keys, l1, returned_keys

from snowcap.resources import Table, UserKeyPair


def dict_literal():
    return {"a": 1}


def named_dict():
    data = {"a": 1}
    data["b"] = 2
    return data


def delegates():
    return named_dict()


def test_returned_keys_reads_each_return_shape():
    assert returned_keys(dict_literal) == {"a"}
    assert returned_keys(named_dict) == {"a", "b"}
    assert returned_keys(delegates) == {"a", "b"}


def test_fetched_keys_reads_through_a_helper():
    # fetch_user_key_pair returns _user_key_pair_to_dict(data).
    assert {"name", "user", "fingerprint", "disabled", "comment"} <= fetched_keys(UserKeyPair)


def first():
    return second(True)


def second(again: bool):
    if again:
        return first()
    return {"c": 3}


def test_returned_keys_stops_at_a_cycle():
    assert returned_keys(first) == {"c"}


def test_l1_models_a_list_field_path():
    assert l1(Table, "columns[]") and l1(Table, "columns[].comment") and not l1(Table, "columns[].nope")
