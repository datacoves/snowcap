"""
Write FINDINGS.md, the coverage checklist, from inventory.yaml.

Every checkbox is derived from the inventory, so the file is never edited by hand. A box is checked when snowcap
supports the object, or when every tested level of the property passes. After a fix, commit it, rerun
live_audit.py for the object, then this script, and commit inventory.yaml and FINDINGS.md: the fixed items show as
checked.

Usage: uv run python tools/snowflake_coverage/report.py
"""

import collections
import re
import sys

from common import INVENTORY, LIVE_LEVELS, load_inventory, object_label, select_objects

FINDINGS = INVENTORY.with_name("FINDINGS.md")

# How a live level failed: the `failures` keys that live_audit.py writes, in table order.
KINDS = {
    "bad_sql": "snowcap sends change SQL that Snowflake rejects",
    "create": "CREATE leaves out the value or fails",
    "not_read": "snowcap does not read the value back",
    "not_planned": "A change to the value plans nothing",
    "update_crash": "snowcap crashes while it plans the change",
    "fetch_crash": "snowcap crashes while it reads the object",
    "not_landed": "The change runs but does not land",
    "refuses_supported": "snowcap refuses a change that ALTER supports",
    "drift": "Plan is not empty after create or change",
}

LIMITS = [
    "A change to any input of an object, docs fields included, clears that object's live results until the next "
    "live run.",
    "List values are compared as strings, and normalization strips quotes. A sample that differs only in quotes is a "
    "weak test.",
    "An `args[].data_type` L4 pass creates a new function overload. L5 shows this.",
    "The combined test cannot mix fields that exclude each other.",
    "`owner` rows are not sampled: the harness sets the owner to the test role.",
    "Objects that need paid compute, another account, or another edition are docs only, with the reason in their "
    "section.",
]

# Throwaway names carry the test database name, for example COVERAGE_TABLE_96DBABC6.
THROWAWAY = re.compile(r"COVERAGE_[A-Z0-9_]*?_[0-9A-F]{8}")


def state(row: dict) -> str:
    levels = row["snowcap"].get("levels")
    if not levels:
        return "n/a"
    if levels["L1"] == "fail":
        return "unmodeled"
    if any(levels[level] == "fail" for level in LIVE_LEVELS):
        return "fail"
    if any(levels[level] in ("pass", "n/a") for level in LIVE_LEVELS):
        return "pass"
    return "untested"


def kind(row: dict) -> str:
    """How the row's first failed level failed."""
    failures = row["snowcap"]["failures"]
    return next(failures[level] for level in LIVE_LEVELS if level in failures)


def clean(text) -> str:
    return THROWAWAY.sub("<db>", " ".join(str(text).split()))


def prose(text, limit: int = 400) -> str:
    """Inventory text as Markdown text: punctuation that Markdown or HTML would interpret is escaped."""
    text = clean(text)
    text = text if len(text) <= limit else text[:limit] + " ..."
    # GitHub links bare URLs, and an escaped colon stops that.
    return re.sub(r"([\\`*_\[\]<>#|&~])", r"\\\1", text).replace("://", "\\://")


def code(text, limit: int = 300) -> str:
    """Machine output, such as evidence and SQL, as a code span: GitHub renders it verbatim and links no URL."""
    text = clean(text).replace("`", "'")
    return "`" + (text if len(text) <= limit else text[:limit] + " ...") + "`"


def anchor(heading: str) -> str:
    """The id GitHub gives a heading: lowercase, punctuation other than - and _ removed, spaces as hyphens."""
    return re.sub(r"[^a-z0-9 _-]", "", heading.lower()).replace(" ", "-")


def failing_item(row: dict) -> list[str]:
    levels, evidence = row["snowcap"]["levels"], row["snowcap"].get("evidence", {})
    failed = [level for level in LIVE_LEVELS if levels[level] == "fail"]
    cause = next((evidence[level] for level in failed if level in evidence), "")
    lines = [f"- [ ] `{row['name']}` fails {', '.join(failed)}: {code(cause)} ([docs]({row['docs']}))"]
    # The SQL explains the failure only when the create or the change failed.
    sql_key = "create_sql" if levels["L2"] == "fail" else "L4_sql" if levels["L4"] == "fail" else None
    if sent := evidence.get(sql_key):
        lines.append(f"  - snowcap sent: {code(' ; '.join(map(str, sent)))}")
    return lines


def object_section(obj: dict) -> list[str]:
    label, snowcap = object_label(obj), obj["snowcap"]
    rows = collections.defaultdict(list)
    for row in obj["properties"]:
        rows[state(row)].append(row)
    docs = " ".join(f"[{page}]({url})" for page, url in obj["docs"].items())
    if snowcap["test"] == "live":
        tested = snowcap["tested"]
        status = (
            f"Tested live at snowcap `{tested['snowcap_commit']}` on {tested['checked_on']}, Snowflake "
            f"{tested['snowflake_version']}. {len(rows['pass'])} pass, {len(rows['fail'])} fail, "
            f"{len(rows['unmodeled'])} not modeled, {len(rows['untested'])} without a live sample."
        )
    else:
        status = f"Docs only: {prose(snowcap['test_reason']).rstrip('.')}."
    lines = [f"### {label}", "", f"{status} Docs: {docs}", ""]
    for row in rows["fail"]:
        lines += failing_item(row)
    combined = snowcap.get("combined", {})
    if combined.get("result") in ("pass", "fail"):
        box = "x" if combined["result"] == "pass" else " "
        detail = combined.get("error") or (f"missed {', '.join(combined['missed'])}" if combined.get("missed") else "")
        lines.append(
            f"- [{box}] Combined change of {', '.join(f'`{f}`' for f in combined['fields'])} in one plan"
            + (f": {code(detail)}" if detail else "")
        )
    elif combined:
        lines.append(f"- Combined change: {combined['result']}")
    for row in rows["unmodeled"]:
        preview = " (preview)" if row["status"] == "preview" else ""
        lines.append(f"- [ ] Model `{row['name']}`{preview}: {code(row['sql'], 160)} ([docs]({row['docs']}))")
    lines += [f"- [x] `{row['name']}`" for row in rows["pass"]]
    if rows["untested"]:
        lines += ["", "Modeled, without a live sample: " + ", ".join(f"`{row['name']}`" for row in rows["untested"])]
    return lines + [""]


def build(inventory: dict) -> str:
    objects = inventory["objects"]
    supported = [o for o in objects if o.get("snowcap", {}).get("class")]
    states = collections.Counter(state(row) for obj in supported for row in obj["properties"])
    kinds = collections.defaultdict(lambda: collections.defaultdict(int))
    for obj in supported:
        for row in obj["properties"]:
            if state(row) == "fail":
                kinds[kind(row)][object_label(obj)] += 1
    meta = inventory["meta"]
    runs = [o["snowcap"]["tested"] for o in supported if o["snowcap"]["test"] == "live"]
    accounts = ", ".join(sorted({f"edition {r['edition']} on {r['cloud']} {r['cloud_region']}" for r in runs}))
    commits = ", ".join(f"`{c}`" for c in sorted({run["snowcap_commit"] for run in runs}))
    first, last = min(run["checked_on"] for run in runs), max(run["checked_on"] for run in runs)
    dates = f"on {first}" if first == last else f"from {first} to {last}"

    lines = [
        "# Snowflake coverage findings",
        "",
        "Which Snowflake objects and properties can snowcap manage? This checklist answers that for every object in "
        "the Snowflake docs. `report.py` generates it from `inventory.yaml`, so do not edit it by hand. A box is "
        "checked when snowcap supports the object, or when every tested level of the property passes. After a fix, "
        "commit it, rerun `live_audit.py` for the object and then `report.py`, and commit `inventory.yaml` and this "
        "file.",
        "",
        f"Scope: {meta['scope']} Docs read on {meta['docs_read_on']}. Live results come from a test account with "
        f"{accounts}, at snowcap {commits}, "
        f"{dates}. Each object section names the commit and date of its own run.",
        "",
        "## How each property is tested",
        "",
        "| Level | Passes when |",
        "|---|---|",
        "| L1 Modeled | snowcap has a field for the property |",
        "| L2 Created | CREATE sets the value in Snowflake |",
        "| L3 Fetched | snowcap reads the value back correctly |",
        "| L4 Updated | a change lands, or snowcap refuses a change that ALTER cannot make |",
        "| L5 Round trip | the plan is empty after the create and after the change |",
        "",
        "## Summary",
        "",
        "| | Count |",
        "|---|---|",
        f"| Objects and variants in scope | {len(objects)} |",
        f"| Supported by snowcap | {len(supported)} |",
        f"| Not supported | {len(objects) - len(supported)} |",
        f"| Properties of supported objects that pass | {states['pass']} |",
        f"| Properties that fail L2 to L5 | {states['fail']} |",
        f"| Properties snowcap does not model | {states['unmodeled']} |",
        f"| Modeled properties without a live sample | {states['untested']} |",
        "",
        "## Failures by kind",
        "",
        "One fix can close several items when they share a cause.",
        "",
        "| What happens | Properties | Objects |",
        "|---|---|---|",
    ]
    for name, description in KINDS.items():
        if kinds[name]:
            links = ", ".join(f"[{label}](#{anchor(label)}) ({n})" for label, n in sorted(kinds[name].items()))
            lines.append(f"| {description} | {sum(kinds[name].values())} | {links} |")
    lines += ["", "## Objects", ""]
    for obj in sorted(objects, key=object_label):
        label = object_label(obj)
        status = f" ({obj['status']})" if obj["status"] == "preview" else ""
        if obj.get("snowcap", {}).get("class"):
            lines.append(f"- [x] [{label}](#{anchor(label)}){status}")
        else:
            lines.append(
                f"- [ ] {label}{status}: not supported, {len(obj['properties'])} properties ([docs]({obj['docs']['create']}))"
            )
    lines += ["", "## Supported objects", ""]
    for obj in sorted(supported, key=object_label):
        lines += object_section(obj)
    lines += ["## Other code findings", ""]
    lines += [f"- [{'x' if f['fixed'] else ' '}] {f['finding']} (`{f['path']}`)" for f in meta["code_findings"]]
    lines += ["", "## Limits of these results", ""]
    lines += [f"- {limit}" for limit in LIMITS]
    lines += [
        "",
        "## How to update",
        "",
        "Live tests need the test account credentials in `tests/.env`. They create objects in throwaway databases "
        "and drop them, also on failure.",
        "",
        "```sh",
        "uv run python tools/snowflake_coverage/static_map.py            # L1 and static candidates",
        "uv run python tools/snowflake_coverage/live_audit.py 'TABLE'    # L2 to L5 for one or more objects",
        "uv run python tools/snowflake_coverage/report.py                # regenerate this file",
        "```",
        "",
    ]
    return "\n".join(lines)


if __name__ == "__main__":
    inventory = load_inventory()
    # An input edit clears an object's live results, and the checklist must not present them as untested properties.
    live = [o for o in select_objects(inventory, None) if o["snowcap"]["test"] == "live"]
    if stale := [object_label(o) for o in live if "tested" not in o["snowcap"]]:
        sys.exit("Rerun live_audit.py first for: " + ", ".join(stale))
    FINDINGS.write_text(build(inventory))
