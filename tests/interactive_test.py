import logging

logging.basicConfig(level=logging.DEBUG)

import json
from tempfile import gettempdir

from spyql.query import Query
from spyql import log
from spyql.utils import join_paths


def eq_test_nrows(query, expectation, **kwargs):
    log.user_debug(f"----")
    q = Query(query)
    log.user_debug(f"{q}")
    res = q(**kwargs)
    log.user_debug(f"{len(expectation)} vs {len(res)} => {res} ")
    assert len(res) == len(expectation)
    # TODO: test actual output against expectation!


def eq_test_1row(query, expectation, **kwargs):
    eq_test_nrows(query, [expectation], **kwargs)


def exception_test(query, anexception, **kwargs):
    q = Query(query)
    res = q(**kwargs)
    assert res.exit_code != 0
    assert isinstance(res.exception, anexception)


# new tests

raw_data = [
    {"name": "A", "age": 20, "salary": 30.0},
    {"name": "B", "age": 30, "salary": 12.0},
    {"name": "C", "age": 40, "salary": 6.0},
    {"name": "D", "age": 50, "salary": 0.40},
]
json_fpath = join_paths(gettempdir(), "spyql_test.jsonl")
with open(json_fpath, "w") as f:
    for d in raw_data:
        f.write(json.dumps(d) + "\n")

csv_fpath = join_paths(gettempdir(), "spyql_test.csv")
with open(csv_fpath, "w") as f:
    f.write(
        """name, age, salary
A, 20, 30.
B, 30, 12.
C, 40, 6.
D, 50, 0.40"""
    )


def test_ux():
    _q = Query(
        "SELECT row->name as first_name, row->age as user_age FROM data WHERE"
        " row->age > 30"
    )
    log.user_debug(f"Query: {_q}")
    out = _q(data=raw_data)
    log.user_debug(f"Output by Query: {out}")
    assert len(out) == 2  # [('C', 40), ('D', 50)]

    out = Query(
        "SELECT row->name as first_name, row->age as user_age FROM data WHERE"
        " row->age < 30"
    )(data=raw_data)
    log.user_debug(f"Output functional: {out}")
    assert len(out) == 1  # [('A', 20)]

    # get mean of salaries of people whose age is greater than 30
    def get_mean_salary_math(data):
        salary = [d["salary"] for d in data if d["age"] >= 30]
        return sum(salary) / len(salary)

    log.user_debug(f"Mean salary by math: {get_mean_salary_math(raw_data): .3f}")

    # using SpyQL
    out = Query(
        "SELECT sum_agg(row->salary) / len(data) as sum_salary FROM data WHERE"
        " row->age >= 30"
    )(data=raw_data)
    log.user_debug(f"Mean salary by math: {out}")


def test_json_read():
    query = Query(
        "SELECT json->name as first_name, json->age as user_age FROM"
        f" json('{json_fpath}') WHERE json->age > 30"
    )
    out = query()
    assert out == (
        {"first_name": "C", "user_age": 40},
        {"first_name": "D", "user_age": 50},
    )


def test_csv_read():
    query = Query(
        f"SELECT name as first_name, age as user_age FROM csv('{csv_fpath}') WHERE age"
        " > 30"
    )
    out = query()
    assert out == (
        {"first_name": "C", "user_age": 40},
        {"first_name": "D", "user_age": 50},
    )


def test_csv_write():
    target_csv = join_paths(gettempdir(), "spyql_test_write.csv")
    query = Query(
        f"SELECT name, age FROM csv('{csv_fpath}') WHERE age > 30 TO"
        f" csv('{target_csv}')"
    )
    query()

    with open(target_csv, "r") as f:
        out = f.read()

    assert out.strip().replace("\n", " ") == "name,age C,40 D,50"


def test_json_write():
    target_json = join_paths(gettempdir(), "spyql_test_write.jsonl")
    query = Query(
        f"SELECT name, age FROM csv('{csv_fpath}') WHERE age > 30 TO"
        f" json('{target_json}')"
    )
    query()

    with open(target_json, "r") as f:
        data = []
        for l in f.read().strip().splitlines():
            data.append(json.loads(l))

    assert data == [{"name": "C", "age": 40}, {"name": "D", "age": 50}]


def test_csv_read_json_write():
    target_json = join_paths(gettempdir(), "spyql_test_write.jsonl")
    query = Query(
        f"SELECT name, age FROM csv('{csv_fpath}') WHERE age > 30 TO"
        f" json('{target_json}')"
    )
    query()

    with open(target_json, "r") as f:
        data = []
        for l in f.read().strip().splitlines():
            data.append(json.loads(l))

    assert data == [{"name": "C", "age": 40}, {"name": "D", "age": 50}]


def test_complex_interactive():
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


def test_readme():
    # all the tests given in the README
    query = Query(
        "SELECT row->invoice_num AS id, row->items->name AS name, row->items->price"
        " AS price FROM data EXPLODE row->items"
    )
    out = query(
        data=[
            {
                "invoice_num": 1028,
                "items": [
                    {"name": "tomatoes", "price": 1.5},
                    {"name": "bananas", "price": 2.0},
                ],
            },
            {"invoice_num": 1029, "items": [{"name": "peaches", "price": 3.12}]},
        ]
    )
    assert out == (
        {"id": 1028, "name": "tomatoes", "price": 1.5},
        {"id": 1028, "name": "bananas", "price": 2.0},
        {"id": 1029, "name": "peaches", "price": 3.12},
    )

    query = Query("SELECT 10 * cos(col1 * ((pi * 4) / 90)) FROM range(80)")
    out = query()

    query = Query("SELECT * FROM [10 * cos(i * ((pi * 4) / 90)) for i in range(80)]")
    out = query()
