"""Workflow definitions are checked before saving, and templates fill values from the run's context."""
import pytest

from app.workflows import StepError, render, validate, value

CTX = {"trigger": {"id": 7, "name": "Asha"}, "order": {"total": 1250.5, "lines": [1, 2]}}


def db_step(**kw):
    return {"type": "db", "connection": "builtin", "table": "t", "operation": "select_one", "save_as": "row", **kw}


def test_valid_workflow_with_branch_is_cleaned():
    steps = validate([db_step(_uid="ui-only"), {"type": "branch", "expression": "row != null", "_uid": "x",
                                                "then": [{"type": "transform", "expression": "@", "save_as": "a"}]}])
    assert "_uid" not in steps[0] and "_uid" not in steps[1]
    assert steps[1]["else"] == [] and steps[1]["then"][0]["save_as"] == "a"


def test_names_must_be_unique_across_branches():
    with pytest.raises(StepError, match="already used"):
        validate([db_step(), {"type": "branch", "expression": "x",
                              "else": [{"type": "transform", "expression": "@", "save_as": "row"}]}])


def test_trigger_is_reserved():
    with pytest.raises(StepError, match="reserved"):
        validate([db_step(save_as="trigger")])


def test_steps_with_results_need_a_name():
    with pytest.raises(StepError, match="name"):
        validate([{"type": "transform", "expression": "@"}])


def test_write_operations_do_not_need_a_name():
    assert validate([db_step(operation="insert", save_as=None, values={"a": 1})])[0]["save_as"] is None


def test_unknown_type():
    with pytest.raises(StepError, match="unknown type"):
        validate([{"type": "teleport"}])


def test_db_step_needs_table_and_operation():
    with pytest.raises(StepError, match="table"):
        validate([db_step(table="")])
    with pytest.raises(StepError, match="what the database step does"):
        validate([db_step(operation="merge")])


@pytest.mark.parametrize("op", ["update", "delete"])
def test_update_and_delete_need_conditions(op):
    with pytest.raises(StepError, match="every row"):
        validate([db_step(operation=op, save_as=None, values={"a": 1})])
    assert validate([db_step(operation=op, save_as=None, values={"a": 1}, allow_all_rows=True)])


def test_branch_needs_condition():
    with pytest.raises(StepError, match="condition"):
        validate([{"type": "branch", "expression": " "}])


def test_retry_fields_are_numbers():
    assert validate([db_step(retry_attempts="3", retry_delay="2")])[0]["retry_attempts"] == 3
    with pytest.raises(StepError, match="number"):
        validate([db_step(retry_attempts="many")])


def test_single_expression_keeps_its_type():
    assert value("{{ order.total }}", CTX) == 1250.5
    assert value("{{ order.lines }}", CTX) == [1, 2]
    assert value(42, CTX) == 42


def test_mixed_template_renders_text():
    assert value("ID-{{ trigger.id }}", CTX) == "ID-7"
    assert render("{{ order.total | money('$') }}", CTX) == "$1,250.50"


def test_unknown_variable_fails_loudly():
    with pytest.raises(Exception):
        render("{{ nope.field }}", CTX)
