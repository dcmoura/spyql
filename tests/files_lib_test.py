import json
from tempfile import gettempdir

from spyql.query import Query
from spyql.utils import join_paths
from spyql.nulltype import NULL
from spyql.qdict import qdict
import os
import math

raw_data = [
    {"name": "A", "age": 20, "salary": 30.0},
    {"name": "B", "age": 30, "salary": 12.0},
    {"name": "C", "age": 40, "salary": 6.0},
    {"name": "D", "age": 50, "salary": 0.40},
]


def make_json():
    json_fpath = join_paths(gettempdir(), "spyql_test.jsonl")
    with open(json_fpath, "w") as f:
        for d in raw_data:
            f.write(json.dumps(d) + "\n")
    return json_fpath


def make_csv():
    csv_fpath = join_paths(gettempdir(), "spyql_test.csv")
    with open(csv_fpath, "w") as f:
        f.write(
            """name, age, salary
    A, 20, 30.
    B, 30, 12.
    C, 40, 6.
    D, 50, 0.40"""
        )
    return csv_fpath


def test_query_result():
    query = Query(
        "SELECT row.name as first_name, row.age as user_age FROM data WHERE"
        " row.age > 30"
    )
    out = query(data=raw_data)
    assert out == (
        {"first_name": "C", "user_age": 40},
        {"first_name": "D", "user_age": 50},
    )
    assert out.first_name == ("C", "D")
    assert out.col("first_name") == ("C", "D")
    assert out.col(0) == ("C", "D")
    assert out[0:2].first_name == ("C", "D")
    assert out.first_name[0] == "C"
    assert out[0].first_name == "C"
    assert out.user_age == (40, 50)
    assert out.col("user_age") == (40, 50)
    assert out.col(1) == (40, 50)
    assert out[0:2].user_age == (40, 50)
    assert out.user_age[-1] == 50
    assert out[-1].user_age == 50
    assert out.colnames() == ("first_name", "user_age")
    assert out.I_do_not_exist == (NULL, NULL)
    assert out[0].I_do_not_exist is NULL
    try:
        out.col(1000)
        assert False
    except IndexError:
        assert True
    assert query.stats() == {"rows_in": 4, "rows_out": 2}


def test_json_read():
    json_fpath = make_json()
    query = Query(
        "SELECT json.name as first_name, json.age as user_age FROM"
        f" json('{json_fpath}') WHERE json.age > 30"
    )
    out = query()
    assert out == (
        {"first_name": "C", "user_age": 40},
        {"first_name": "D", "user_age": 50},
    )
    os.remove(json_fpath)


def test_csv_read():
    csv_fpath = make_csv()
    query = Query(
        f"SELECT name as first_name, age as user_age FROM csv('{csv_fpath}') WHERE age"
        " > 30"
    )
    out = query()
    assert out == (
        {"first_name": "C", "user_age": 40},
        {"first_name": "D", "user_age": 50},
    )
    os.remove(csv_fpath)


def test_csv_write():
    csv_fpath = make_csv()
    target_csv = join_paths(gettempdir(), "spyql_test_write.csv")
    query = Query(
        f"SELECT name, age FROM csv('{csv_fpath}') WHERE age > 30 TO"
        f" csv('{target_csv}')"
    )
    query()

    with open(target_csv, "r") as f:
        out = f.read()

    assert out.strip().replace("\n", " ") == "name,age C,40 D,50"

    os.remove(csv_fpath)
    os.remove(target_csv)


def read_json(filename):
    with open(filename, "r") as f:
        return [json.loads(l) for l in f.read().strip().splitlines()]


def test_json_write():
    csv_fpath = make_csv()
    target_json = join_paths(gettempdir(), "spyql_test_write1.jsonl")
    Query(
        f"SELECT name, age FROM csv('{csv_fpath}') WHERE age > 30 TO"
        f" json('{target_json}')"
    )()
    assert read_json(target_json) == [
        {"name": "C", "age": 40},
        {"name": "D", "age": 50},
    ]
    os.remove(csv_fpath)
    os.remove(target_json)


def test_csv_read_json_write():
    csv_fpath = make_csv()
    target_json = join_paths(gettempdir(), "spyql_test_write2.jsonl")
    Query(
        f"SELECT name, age FROM csv('{csv_fpath}') WHERE age > 30 TO"
        f" json('{target_json}')"
    )()
    assert read_json(target_json) == [
        {"name": "C", "age": 40},
        {"name": "D", "age": 50},
    ]
    os.remove(csv_fpath)
    os.remove(target_json)


def test_read_invalid_file():
    try:
        Query("SELECT * FROM csv('where is this file???')")()
        assert False
    except FileNotFoundError:
        assert True


def test_write_invalid_file():
    try:
        Query("SELECT * FROM range(10) TO csv('this * is a bad / file name')")()
        assert False
    except FileNotFoundError:
        assert True


def test_equi_join():
    query = Query("SELECT row.name, names[row.name] AS ext_name FROM data")
    out = query(data=raw_data, names=qdict({"A": "Alice", "C": "Chris", "D": "Daniel"}))
    assert out == (
        {"name": "A", "ext_name": "Alice"},
        {"name": "B", "ext_name": NULL},
        {"name": "C", "ext_name": "Chris"},
        {"name": "D", "ext_name": "Daniel"},
    )


def test_globals():
    # accessing `os`` module and `raw_data` via `globals()`
    query = Query(
        "SELECT row.name as first_name, row.age as user_age, os.getpid() > 0 as test_os"
        " FROM raw_data WHERE row.age > 30"
    )
    out = query(**globals())
    assert out == (
        {"first_name": "C", "user_age": 40, "test_os": True},
        {"first_name": "D", "user_age": 50, "test_os": True},
    )


def test_readme():
    # TODO test all recipes in the README

    query = Query(
        'IMPORT hashlib as hl SELECT hl.md5(col1.encode("utf-8")).hexdigest() as h FROM'
        " data"
    )
    out = query(data=["a", "b", "c"])
    assert out == (
        {"h": "0cc175b9c0f1b6a831c399e269772661"},
        {"h": "92eb5ffee6ae2fec3ad71c777531578f"},
        {"h": "4a8a08f09d37b73795649038408b5f33"},
    )
    assert query.stats() == {"rows_in": 3, "rows_out": 3}

    query = Query(
        "SELECT row.invoice_num AS id, row.items_sold.name AS name,"
        " row.items_sold.price AS price FROM data EXPLODE row.items_sold"
    )
    out = query(
        data=[
            {
                "invoice_num": 1028,
                "items_sold": [
                    {"name": "tomatoes", "price": 1.5},
                    {"name": "bananas", "price": 2.0},
                ],
            },
            {"invoice_num": 1029, "items_sold": [{"name": "peaches", "price": 3.12}]},
        ]
    )

    assert out == (
        {"id": 1028, "name": "tomatoes", "price": 1.5},
        {"id": 1028, "name": "bananas", "price": 2.0},
        {"id": 1029, "name": "peaches", "price": 3.12},
    )

    expectation = tuple(
        [10 * math.cos(col1 * ((math.pi * 4) / 90)) for col1 in range(80)]
    )
    out = Query("SELECT 10 * cos(col1 * ((pi * 4) / 90)) FROM range(80)")()
    assert out.col(0) == expectation
    assert len(out.colnames()) == 1

    out = Query("SELECT * FROM [10 * cos(i * ((pi * 4) / 90)) for i in range(80)]")()
    assert out.col(0) == expectation
    assert len(out.colnames()) == 1
