"""
Shared helpers for the Snowflake coverage audit.

inventory.yaml is the coverage source of truth. Each object entry holds the Snowflake docs facts
(from the CREATE, ALTER, SHOW, and DESCRIBE pages) and a `snowcap` block:

    snowcap:
      class: Table            # snowcap resource class, or null when snowcap does not model the object
      test: live              # live | docs_only | untested
      test_reason: reason     # why a docs_only object is not tested live
      setup: [SQL, ...]       # prerequisites created in the throwaway database before the tests
      teardown: [SQL, ...]    # drops the account-level prerequisites from setup after the tests
      create_args: {...}      # the minimum arguments to create a test object
      truth: [SQL, ...]       # queries that report Snowflake's own values; {name} {database} {container} {fqn}
      field_notes: {copy_grants: reason}  # why a spec field has no docs row (for example a CREATE option)

Strings in setup, teardown, create_args, and samples can use {database}, the throwaway database, and {files}, the
absolute path of the fixture files in files/ (for PUT). All placeholders are replaced as plain text, so SQL braces need
no escaping.

Every credential input (a password, key, token, client id, or client secret) is a {secret:NAME} placeholder, so
the inventory holds no credential-shaped value, real or made up. fill() gives each NAME its own value for the run,
and save_inventory() turns those values back into their placeholders wherever a result repeats them.

Each property row has its own `snowcap` block:

    snowcap:
      field: data_retention_time_in_days  # spec field path, or null when snowcap does not model the property:
                                          # `credentials.aws_key_id` is a key in a field, `columns[].comment`
                                          # a column field
      note: reason                        # why the row maps to a differently named field, or what snowcap lacks
      n_a: reason                         # set when the row is not a manageable state of this object
      sample: [3, 5]                      # create value, changed value (rows without it stay untested);
                                          # values of the field the path heads, for example two `columns` lists
      truth: retention_time               # key in the truth query output
      expect: [3, 5]                      # the values Snowflake reports under the truth key (default: sample)
      levels: {L1: pass, L2: ..., L5: ...}
      evidence: {L3: ...}
      failures: {L3: not_read}            # how each failed live level failed (report.py KINDS)
      candidates: [...]                   # static findings to confirm live

static_map.py writes L1, candidates, and the object's `unmapped_fields`. live_audit.py writes L2 to L5, evidence,
failures, the object's `combined` result, and its `tested` block: `inputs`, a digest of the inputs that produced
those results, and the facts of the run (account edition, cloud, region, and test role, snowcap commit, Snowflake
version, and date). Every other value is an input.
"""

import base64
import hashlib
import json
import re
from dataclasses import fields
from pathlib import Path
from typing import Iterator, Optional

import yaml

from snowcap import resources
from snowcap.resources.tag import TaggableResource
from snowcap.scope import DatabaseScope, SchemaScope

INVENTORY = Path(__file__).with_name("inventory.yaml")
FILES = Path(__file__).with_name("files")
LEVELS = ("L1", "L2", "L3", "L4", "L5")
LIVE_LEVELS = LEVELS[1:]
OBJECT_OUTPUTS = ("unmapped_fields", "combined", "tested")
ROW_OUTPUTS = ("levels", "candidates", "evidence", "failures")
SECRET = re.compile(r"\{secret:(\w+)\}")
# The value of each {secret:NAME} in this run: 16 equal bytes in base64 without padding, 22 letters and digits. It is
# a valid AES key for client-side encryption (Snowflake accepts it unpadded), and AWS accepts it as a key id, which
# padding breaks. It has no entropy, so even a fragment of one in a truncated error message is not credential-shaped.
# Byte values 1 to 61 encode without + or /, which covers the first 61 names.
_secrets: dict[str, str] = {}


def load_inventory() -> dict:
    """Read the inventory and clear the live results of every object whose test inputs changed since its run."""
    inventory = yaml.safe_load(INVENTORY.read_text())
    for obj in select_objects(inventory, None):
        tested = obj["snowcap"].get("tested")
        if tested and tested["inputs"] != live_inputs_digest(obj):
            clear_live_results(obj)
            print(f"{object_label(obj)}: test inputs changed since the live run, live results cleared")
    return inventory


def live_inputs_digest(obj: dict) -> str:
    """A short digest of the object's inventory entry without the script outputs, so any input edit clears live results."""
    inputs = obj | {
        "snowcap": {k: v for k, v in obj["snowcap"].items() if k not in OBJECT_OUTPUTS},
        "properties": [
            row | {"snowcap": {k: v for k, v in row["snowcap"].items() if k not in ROW_OUTPUTS}}
            for row in obj["properties"]
        ],
    }
    return hashlib.sha256(json.dumps(inputs, sort_keys=True, default=str).encode()).hexdigest()[:12]


def clear_live_results(obj: dict) -> None:
    for key in ("tested", "combined"):
        obj["snowcap"].pop(key, None)
    for row in obj["properties"]:
        row["snowcap"].pop("evidence", None)
        row["snowcap"].pop("failures", None)
        if "levels" in row["snowcap"]:
            row["snowcap"]["levels"].update(dict.fromkeys(LIVE_LEVELS, "untested"))


def save_inventory(inventory: dict) -> None:
    if _secrets:
        placeholders = {value: f"{{secret:{name}}}" for name, value in _secrets.items()}
        values = re.compile("|".join(map(re.escape, placeholders)))
        inventory = map_strings(inventory, lambda text: values.sub(lambda m: placeholders[m[0]], text))
    INVENTORY.write_text(yaml.safe_dump(inventory, sort_keys=False, width=120, allow_unicode=True))


def object_label(obj: dict) -> str:
    return obj["object"] + (f":{obj['variant']}" if obj["variant"] else "")


def select_objects(inventory: dict, labels: Optional[list[str]]) -> Iterator[dict]:
    """Yield the modeled objects whose label (OBJECT or OBJECT:variant) is in labels, or all when labels is empty.

    Objects without a snowcap block have docs rows only and are not mapped to snowcap yet.
    """
    for obj in inventory["objects"]:
        if obj.get("snowcap", {}).get("class") and (not labels or object_label(obj) in labels):
            yield obj


def resource_class(obj: dict) -> type:
    return getattr(resources, obj["snowcap"]["class"])


def path_steps(field: str) -> list[str]:
    """The steps of a row path: `storage_locations[].encryption.type` is storage_locations, [], encryption, type."""
    return re.findall(r"\[\]|[^.\[\]]+", field)


def top_field(field: str) -> str:
    """The spec field that a row path names: `credentials.aws_key_id` and `columns[].comment` both name their head."""
    return path_steps(field)[0]


def map_strings(value, change):
    """Apply change to every string of value, dict keys included."""
    if isinstance(value, str):
        return change(value)
    if isinstance(value, list):
        return [map_strings(v, change) for v in value]
    if isinstance(value, dict):
        return {map_strings(k, change): map_strings(v, change) for k, v in value.items()}
    return value


def secret(name: str) -> str:
    """The run's value for {secret:name}."""
    return _secrets.setdefault(name, base64.b64encode(bytes([len(_secrets) + 1]) * 16).decode().rstrip("="))


def fill(value, database: str):
    """Replace {database}, {files}, and every {secret:NAME} in every string of value. Plain replacement, because SQL
    and code bodies contain braces."""
    return map_strings(
        value,
        lambda text: SECRET.sub(
            lambda m: secret(m[1]), text.replace("{database}", database).replace("{files}", str(FILES))
        ),
    )


def build_resource(obj: dict, name: str, database: str, owner: str, values: Optional[dict] = None):
    """Create the snowcap resource for a test object in database, with the given spec field values."""
    cls = resource_class(obj)
    kwargs = fill(obj["snowcap"]["create_args"], database)
    kwargs.update(name=name)
    if "owner" in {f.name for f in fields(cls.spec)}:
        kwargs.update(owner=owner)
    if isinstance(cls.scope, SchemaScope):
        kwargs.update(database=database, schema="PUBLIC")
    elif isinstance(cls.scope, DatabaseScope):
        kwargs.update(database=database)
    kwargs.update(fill(values or {}, database))
    return cls(**kwargs)


def testable_rows(obj: dict) -> list[dict]:
    """Rows with samples whose path heads a spec field, plus tags rows of a TaggableResource."""
    cls = resource_class(obj)
    spec_fields = {f.name for f in fields(cls.spec)}
    if issubclass(cls, TaggableResource):
        spec_fields.add("tags")
    return [
        p
        for p in obj["properties"]
        if p["snowcap"].get("sample")
        and not p["snowcap"].get("n_a")
        and top_field(p["snowcap"]["field"]) in spec_fields
    ]
