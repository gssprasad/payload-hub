"""The workflow DB connector: queries built from settings (table, conditions, values), on SQLite."""
import sqlite3

import pytest

from app.adapters import AdapterError, Database


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "shop.db"
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, country TEXT, email TEXT);
        INSERT INTO customers VALUES (1, 'Asha', 'IN', 'asha@example.com'), (2, 'Ben', 'GB', NULL),
                                     (3, 'Chen', 'SG', 'chen@example.com');
    """)
    con.commit()
    con.close()
    db = Database({"engine": "sqlite", "database": str(path)}, "")
    return db


def spec(**kw):
    return {"table": "customers", "operation": "select_all", **kw}


async def rows(db, **kw):
    return (await db.run_structured(spec(**kw)))["rows"]


async def test_select_with_conditions_order_and_limit(database):
    got = await rows(database, columns=["name"], where=[{"column": "id", "op": ">=", "value": 2}],
                     order_by="id", order_desc=True, limit=1)
    assert got == [{"name": "Chen"}]


@pytest.mark.parametrize("cond, names", [
    ({"column": "country", "op": "=", "value": "IN"}, ["Asha"]),
    ({"column": "country", "op": "!=", "value": "IN"}, ["Ben", "Chen"]),
    ({"column": "country", "op": "in", "value": "GB, SG"}, ["Ben", "Chen"]),
    ({"column": "country", "op": "in", "value": ["IN"]}, ["Asha"]),
    ({"column": "name", "op": "like", "value": "%e%"}, ["Ben", "Chen"]),
    ({"column": "email", "op": "is null"}, ["Ben"]),
    ({"column": "email", "op": "is not null"}, ["Asha", "Chen"]),
    ({"column": "id", "op": "<", "value": 2}, ["Asha"]),
])
async def test_condition_operators(database, cond, names):
    assert [r["name"] for r in await rows(database, where=[cond], order_by="id")] == names


async def test_insert_returns_new_key(database):
    result = await database.run_structured({"table": "customers", "operation": "insert",
                                            "values": {"name": "Dev", "country": "IN"}})
    assert result["rowcount"] == 1 and result["key"] == 4


async def test_update_and_delete(database):
    upd = await database.run_structured({"table": "customers", "operation": "update",
                                         "where": [{"column": "country", "op": "=", "value": "IN"}],
                                         "values": {"email": "new@example.com"}})
    assert upd["rowcount"] == 1
    dele = await database.run_structured({"table": "customers", "operation": "delete",
                                          "where": [{"column": "email", "op": "is null"}]})
    assert dele["rowcount"] == 1
    assert [r["name"] for r in await rows(database, order_by="id")] == ["Asha", "Chen"]


@pytest.mark.parametrize("op", ["update", "delete"])
async def test_refuses_to_change_every_row_without_conditions(database, op):
    with pytest.raises(AdapterError, match="every row"):
        await database.run_structured({"table": "customers", "operation": op, "values": {"country": "XX"}})


async def test_all_rows_when_explicitly_allowed(database):
    r = await database.run_structured({"table": "customers", "operation": "update", "allow_all_rows": True,
                                       "values": {"country": "XX"}})
    assert r["rowcount"] == 3


async def test_unknown_table_and_column_have_clear_errors(database):
    with pytest.raises(AdapterError, match="no table 'nope'"):
        await database.run_structured({"table": "nope", "operation": "select_all"})
    with pytest.raises(AdapterError, match="no column 'age'"):
        await rows(database, where=[{"column": "age", "op": "=", "value": 1}])


async def test_values_are_parameters_not_sql(database):
    sneaky = "x'); DROP TABLE customers; --"
    assert await rows(database, where=[{"column": "name", "op": "=", "value": sneaky}]) == []
    sql = await database.preview_structured(spec(where=[{"column": "name", "op": "=", "value": sneaky}]))
    assert "DROP" not in sql and "?" in sql
    assert len(await rows(database)) == 3


async def test_preview_renders_in_lists(database):
    sql = await database.preview_structured(spec(where=[{"column": "id", "op": "in", "value": "1,2"}]))
    assert "POSTCOMPILE" not in sql and "IN" in sql
