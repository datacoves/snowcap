from report import limits, object_section


def obj(test: str, **snowcap) -> dict:
    return {
        "object": "TABLE",
        "variant": None,
        "docs": {"create": "https://docs.snowflake.com/create-table"},
        "snowcap": {"class": "Table", "test": test, **snowcap},
        "properties": [],
    }


def test_object_section_names_each_test_value():
    assert "Docs only: needs paid compute." in object_section(obj("docs_only", test_reason="needs paid compute"))[2]
    assert "Not tested live." in object_section(obj("untested"))[2]
    assert "Not tested live: no sample yet." in object_section(obj("untested", test_reason="no sample yet"))[2]


def test_limits_name_the_objects_whose_owner_is_sampled():
    sampled, unsampled = obj("live"), obj("live")
    sampled["properties"] = [{"snowcap": {"field": "owner", "sample": ["ACCOUNTADMIN", "SYSADMIN"]}}]
    unsampled["object"], unsampled["properties"] = "VIEW", [{"snowcap": {"field": "owner"}}]

    owner = next(limit for limit in limits([sampled, unsampled]) if "`owner`" in limit)

    assert "TABLE" in owner and "VIEW" not in owner
