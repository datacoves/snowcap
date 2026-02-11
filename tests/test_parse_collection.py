import pytest

from snowcap.parse import parse_collection_string


# Test cases for parse_collection_string
def test_parse_database_level():
    assert parse_collection_string("SOME_DATABASE.<TABLE>") == {
        "on": "SOME_DATABASE",
        "on_type": "database",
        "items_type": "TABLE",
    }


def test_parse_schema_level():
    assert parse_collection_string("SOME_DATABASE.SOME_SCHEMA.<VIEW>") == {
        "on": "SOME_DATABASE.SOME_SCHEMA",
        "on_type": "schema",
        "items_type": "VIEW",
    }


def test_parse_invalid_format():
    with pytest.raises(ValueError):
        parse_collection_string("SOME_DATABASE")


def test_parse_incorrect_brackets():
    with pytest.raises(ValueError):
        parse_collection_string("SOME_DATABASE.<TABLE")
