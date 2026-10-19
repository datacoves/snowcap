"""
Join inventory.yaml to snowcap's code without a Snowflake connection.

For each modeled object, write L1 (does the spec have the field?) and a list of candidate gaps
for each row. Candidates are leads for live_audit.py, not results:

- L2: neither the props nor the resource's own create handler render the field.
- L3: the fetch function does not return the field.
- L4: ALTER cannot change the property but neither triggers_replacement nor the resource's
  own update handler covers the change, or the
  ALTER that snowcap renders does not match the docs syntax.

Also report spec fields that no inventory row maps, so the inventory stays complete.

Usage: uv run python tools/snowflake_coverage/static_map.py [OBJECT[:variant] ...]
"""

import ast
import inspect
import re
import sys
from dataclasses import fields
from typing import Optional, get_args, get_type_hints, is_typeddict

from common import (
    LEVELS,
    build_resource,
    load_inventory,
    object_label,
    path_steps,
    resource_class,
    save_inventory,
    select_objects,
    top_field,
)

from snowcap import data_provider, lifecycle
from snowcap.identifiers import resource_label_for_type
from snowcap.resources.tag import TaggableResource

# Fields that every resource carries outside its props: the name is in the CREATE header and
# ownership moves through GRANT OWNERSHIP.
HEADER_FIELDS = {"name", "owner"}


def fetched_keys(cls) -> Optional[set[str]]:
    """String keys of the dicts that fetch_<label> returns, or None when the function does not exist."""
    fetch = getattr(data_provider, f"fetch_{resource_label_for_type(cls.resource_type)}", None)
    return None if fetch is None else returned_keys(fetch)


def returned_keys(func) -> set[str]:
    """String keys of the dicts that func returns: a dict literal, a named dict that it builds with a literal and item
    assignments, or the result of another function in its module that returns one of these."""
    nodes = list(ast.walk(ast.parse(inspect.getsource(func).lstrip())))
    returns = [n.value for n in nodes if isinstance(n, ast.Return) and n.value is not None]
    returned_names = {r.id for r in returns if isinstance(r, ast.Name)}
    dicts = [r for r in returns if isinstance(r, ast.Dict)]
    keys = set()
    for r in returns:
        if (
            isinstance(r, ast.Call)
            and isinstance(r.func, ast.Name)
            and inspect.isfunction(helper := func.__globals__.get(r.func.id))
        ):
            keys |= returned_keys(helper)
    for node in nodes:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id in returned_names for t in node.targets
        ):
            if isinstance(node.value, ast.Dict):
                dicts.append(node.value)
        elif (
            isinstance(node, ast.Subscript)
            and isinstance(node.ctx, ast.Store)
            and isinstance(node.value, ast.Name)
            and node.value.id in returned_names
            and isinstance(node.slice, ast.Constant)
        ):
            keys.add(node.slice.value)
    for d in dicts:
        keys |= {k.value for k in d.keys if isinstance(k, ast.Constant)}
    return keys


def handler_keys(cls, action: str) -> set[str]:
    """Field names that a resource-specific lifecycle handler (create_<label>, update_<label>) handles by name.

    The handler reads them from data directly or compares the changed attribute to them, outside props.
    """
    handler = getattr(lifecycle, f"{action}_{resource_label_for_type(cls.resource_type)}", None)
    if handler is None:
        return set()
    keys = set()
    for node in ast.walk(ast.parse(inspect.getsource(handler))):
        if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            keys.add(node.slice.value)
        elif isinstance(node, ast.Compare):
            keys |= {c.value for c in node.comparators if isinstance(c, ast.Constant)}
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("get", "pop")
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            keys.add(node.args[0].value)
    return keys


def head_pattern(alter_syntax: str):
    """Regex for the ALTER action that follows the object name in a docs syntax line."""
    match = re.search(r"<name>\s*(?:\(\s*<arg_types>\s*\))?\s*(.*)", alter_syntax)
    if not match:
        return None
    parts = []
    for token in re.findall(r"\{[^}]*\}|\S+", match.group(1)):
        if token.startswith(("<", "(", "=", "'", "[")):
            break
        if token.startswith("{"):
            options = [re.escape(o.strip()) for o in token.strip("{}").split("|")]
            parts.append("(?:" + "|".join(options) + ")")
        else:
            parts.append(re.escape(token))
    return re.compile(r"\s+".join(parts), re.IGNORECASE) if parts else None


def l1(cls, field: str) -> bool:
    """Whether the spec models the row path. A `head[].x` path also needs x in the list's element type:
    Table columns are Column, which has tags, and View columns are ViewColumn, which does not."""
    if field == "tags":
        # Tags are a relationship: snowcap models them on the resource class (snowcap/resources/tag.py), not in the spec.
        return issubclass(cls, TaggableResource)
    types = {f.name: f.type for f in fields(cls.spec)}
    head, *rest = path_steps(field)
    if head not in types:
        return False
    if rest[:1] != ["[]"]:
        return True
    element, key = get_args(types[head])[0], rest[1]
    if hasattr(element, "spec"):
        return key in {f.name for f in fields(element.spec)}
    if is_typeddict(element):
        return key in get_type_hints(element)
    # A list of plain dicts (HybridTable indexes) does not declare its keys.
    return True


def l4_candidates(obj: dict, row: dict, cls) -> list[str]:
    field = row["snowcap"]["field"]
    if row["alter"] == "none":
        if not cls.spec.get_metadata(field).triggers_replacement and field not in handler_keys(cls, "update"):
            return [
                "L4: ALTER cannot change it, but neither triggers_replacement nor the update handler covers it,"
                " so snowcap plans an ALTER"
            ]
        return []
    sample = row["snowcap"].get("sample")
    if not sample or field in HEADER_FIELDS:
        return []
    before = build_resource(obj, "P", "COVERAGE_STATIC", "ACCOUNTADMIN", {field: sample[0]})
    after = build_resource(obj, "P", "COVERAGE_STATIC", "ACCOUNTADMIN", {field: sample[1]})
    try:
        rendered = lifecycle.update_resource(before.urn, {field: after.to_dict()[field]}, cls.props, after.to_dict())
    except Exception as err:
        return [f"L4: the update handler raises {type(err).__name__}: {err}"]
    statements = [rendered] if isinstance(rendered, str) else rendered
    fqn = str(before.urn.fqn).upper()
    actions = [" ".join(s.split()).upper().split(fqn, 1)[-1].strip() for s in statements]
    pattern = head_pattern(row["alter_syntax"])
    if pattern and not any(pattern.match(a) for a in actions):
        return [f"L4: snowcap renders {statements!r}, docs syntax is {row['alter_syntax']!r}"]
    return []


def map_object(obj: dict) -> None:
    cls = resource_class(obj)
    keys = fetched_keys(cls)
    rendered = set(cls.props.props) | HEADER_FIELDS | handler_keys(cls, "create")
    spec_fields = {f.name for f in fields(cls.spec)}
    mapped = set()
    for row in obj["properties"]:
        meta = row["snowcap"]
        field = meta["field"]
        if field:
            mapped.add(top_field(field))
        if meta.get("n_a"):
            meta.pop("levels", None)
            continue
        levels = meta.get("levels") or dict.fromkeys(LEVELS, "untested")
        candidates = []
        if not field:
            levels.update(L1="fail")
        else:
            levels["L1"] = "pass" if l1(cls, field) else "fail"
            top = top_field(field)
            if levels["L1"] == "pass" and top in spec_fields:
                if top not in rendered:
                    candidates.append(
                        f"L2: neither {cls.__name__}.props nor the create handler renders '{top}', so CREATE omits it"
                    )
                if cls.spec.get_metadata(top).fetchable:
                    if keys is None:
                        label = resource_label_for_type(cls.resource_type)
                        candidates.append(f"L3: data_provider has no fetch_{label}, so fetch_resource raises")
                    elif top not in keys:
                        candidates.append(f"L3: the fetch function does not return '{top}'")
                if field == top:
                    candidates += l4_candidates(obj, row, cls)
        meta["levels"] = levels
        meta["candidates"] = candidates
    unmapped = sorted(spec_fields - mapped - set(obj["snowcap"].get("field_notes", {})))
    obj["snowcap"]["unmapped_fields"] = unmapped


def main(labels: list[str]) -> None:
    inventory = load_inventory()
    for obj in select_objects(inventory, labels):
        map_object(obj)
        label = object_label(obj)
        if obj["snowcap"]["unmapped_fields"]:
            print(f"{label}: spec fields with no inventory row: {obj['snowcap']['unmapped_fields']}")
        for row in obj["properties"]:
            meta = row["snowcap"]
            if meta.get("n_a"):
                continue
            if meta["levels"]["L1"] == "fail":
                print(f"{label}.{row['name']}: L1 fail (not modeled)")
            for candidate in meta["candidates"]:
                print(f"{label}.{row['name']}: {candidate}")
    save_inventory(inventory)


if __name__ == "__main__":
    main(sys.argv[1:])
