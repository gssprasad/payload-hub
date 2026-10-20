"""Helpers behind the workflow designer: nested step lists, copies, unique names, insert points, diagram."""
from ui import workflows as w

STEPS = [
    {"type": "db", "label": "Find", "save_as": "customer", "operation": "select_one", "table": "customers"},
    {"type": "branch", "label": "Known?", "expression": "customer != null",
     "then": [{"type": "api_call", "label": "Profile", "save_as": "profile", "retry_attempts": 3}],
     "else": [{"type": "transform", "label": "Unknown", "save_as": "unknown", "expression": "@"}]},
]


def with_uids():
    return w._with_uids(STEPS)


def test_uids_are_added_everywhere_and_removed_on_save():
    steps = with_uids()
    assert all("_uid" in s for s in w._walk(steps))
    assert w._clean(steps) == STEPS


def test_free_name_avoids_names_inside_branches():
    assert w._free_name("profile", with_uids()) == "profile_2"
    assert w._free_name("fresh", with_uids()) == "fresh"


def test_copy_gets_new_ids_and_names():
    steps = with_uids()
    copy = w._copy(steps[1], steps)
    assert copy["_uid"] != steps[1]["_uid"]
    assert copy["then"][0]["save_as"] == "profile_2" and copy["else"][0]["save_as"] == "unknown_2"


def test_locate_steps_and_branch_path_ends():
    steps = with_uids()
    inner = steps[1]["then"][0]
    lst, i = w._locate(steps, inner["_uid"])
    assert lst is steps[1]["then"] and i == 0
    lst, i = w._locate(steps, f"end:{steps[1]['_uid']}:else")
    assert lst is steps[1]["else"] and i == 1
    assert w._locate(steps, "missing") == (None, None)


def test_summary_mentions_what_matters():
    assert w._summary(STEPS[0]) == "get one row · customers · → customer"
    assert "🔁 3 tries" in w._summary(STEPS[1]["then"][0])


def test_diagram_has_branch_paths():
    dot = w._diagram(STEPS)
    assert dot.startswith("digraph") and 'label="yes"' in dot and 'label="no"' in dot and "shape=diamond" in dot
