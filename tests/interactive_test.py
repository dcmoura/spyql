import logging
logging.basicConfig(level=logging.DEBUG)

import json
from tempfile import gettempdir

from spyql.interactive import Q
from spyql import log
from spyql.utils import join_paths
from spyql.nulltype import NULL, NullSafeDict

# ported from main_test.py

def eq_test_nrows(query, expectation, **kwargs):
  log.user_debug(f"----")
  q = Q(query)
  log.user_debug(f"{q}")
  res = q(**kwargs)
  log.user_debug(f"{len(expectation)} vs {len(res)} => {res} ")
  assert len(res) == len(expectation)

def eq_test_1row(query, expectation, **kwargs):
  eq_test_nrows(query, [expectation], **kwargs)

def exception_test(query, anexception, **kwargs):
  q = Q(query)
  res = q(**kwargs)
  assert res.exit_code != 0
  assert isinstance(res.exception, anexception)


def test_return_values():
  eq_test_1row("SELECT 1", 1)   # {"_1": 1})
  eq_test_1row("SELECT 1+2", 3) # {"_1_2": 3})

  # float
  eq_test_1row("SELECT 1.1", 1.1)   # {"_1_1": 1.1})
  eq_test_1row("SELECT 1+0.2", 1.2) # {"_1_0_2": 1.2})

  # text
  eq_test_1row("SELECT '1'", "1")       # {"_1": "1"})
  eq_test_1row("SELECT '1'+'2'", "12"), # {"_1_2": "12"})

  # two columns with differemt data types
  eq_test_1row("SELECT '1', 2", ["1", 2]) # {"_1": "1", "_2": 2})

  # alias
  eq_test_1row("SELECT '1' as a, 2 AS Ola", {"a": "1", "Ola": 2})

  # strings with commas and reserved keywords
  eq_test_1row(
      "SELECT 'isto, from you' as 'era uma vez', 2 AS Ola",
      {"era uma vez": "isto, from you", "Ola": 2},
  )

def test_star_literals():
  # star over a literal
  eq_test_1row("SELECT * FROM 1", {"col1": 1})

  # star over a list
  eq_test_1row("SELECT * FROM [1]", {"col1": 1})
  # TODO: star over JSON, CSV, DIC?, DF? ZIPPED LIST ...

  # get all elements from a list/iterator
  eq_test_nrows("SELECT * FROM [1,2,3]", [{"col1": 1}, {"col1": 2}, {"col1": 3}])

  # where clause
  eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 2", [{"col1": 2}, {"col1": 3}])

  # where filters out all rows
  eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 10", [])

  # where + limit all
  eq_test_nrows(
      "SELECT * FROM [1,2,3] WHERE col1 >= 2 LIMIT ALL", [{"col1": 2}, {"col1": 3}]
  )

  # where + large limit
  eq_test_nrows(
      "SELECT * FROM [1,2,3] WHERE col1 >= 2 LIMIT 1000", [{"col1": 2}, {"col1": 3}]
  )

  # limit
  eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2", [{"col1": 1}, {"col1": 2}])

  # limit and offset 0
  eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2 OFFSET 0", [{"col1": 1}, {"col1": 2}])

  # limit and negative offset
  eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2 OFFSET -2", [{"col1": 1}, {"col1": 2}])

  # limit and offset
  eq_test_nrows(
      "SELECT * FROM [10,20,30,40,50,60] LIMIT 2 OFFSET 3",
      [{"col1": 40}, {"col1": 50}],
  )

  # offset only
  eq_test_nrows(
      "SELECT * FROM [10,20,30,40,50,60] OFFSET 2",
      [{"col1": 30}, {"col1": 40}, {"col1": 50}, {"col1": 60}],
  )

  # limit 0
  eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 0", [])

  # negative limit
  eq_test_nrows("SELECT * FROM [1,2,3] LIMIT -10", [])

def test_complex():
  # complex expressions with commas and different types of brackets
  eq_test_1row(
      "SELECT (col1 + 3) + ({'a': 1}).get('b', 6) + [10,20,30][(1+(3-2))-1] AS calc,"
      " 2 AS two FROM [1]",
      {"calc": 30, "two": 2},
  )

  # import
  eq_test_1row(
      "IMPORT sys SELECT sys.version_info.major AS major_ver", {"major_ver": 3}
  )

  eq_test_1row(
      "IMPORT numpy AS np, sys SELECT (np.array([1,2,3])+1).tolist() AS a",
      {"a": [2, 3, 4]},
    )

def test_orderby():
  # order by (1 col)
  eq_test_nrows(
    "SELECT * FROM [1,-2,3] ORDER BY 1", [{"col1": -2}, {"col1": 1}, {"col1": 3}]
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,3] ORDER BY 1 DESC",
    [{"col1": 3}, {"col1": 1}, {"col1": -2}],
  )
  eq_test_nrows(
    "SELECT abs(col1) as col1 FROM [1,-2,3] ORDER BY 1",
    [{"col1": 1}, {"col1": 2}, {"col1": 3}],
  )
  eq_test_nrows(
    "SELECT abs(col1) as col1 FROM [1,-2,3] ORDER BY 1 DESC",
    [{"col1": 3}, {"col1": 2}, {"col1": 1}],
  )
  eq_test_nrows(
    "SELECT col1 FROM [1,-2,3] ORDER BY col1",
    [{"col1": -2}, {"col1": 1}, {"col1": 3}],
  )
  eq_test_nrows(
    "SELECT col1 FROM [1,-2,3] ORDER BY abs(col1)",
    [{"col1": 1}, {"col1": -2}, {"col1": 3}],
  )
  eq_test_nrows(
    "SELECT col1 FROM [1,-2,3] ORDER BY abs(col1) DESC",
    [{"col1": 3}, {"col1": -2}, {"col1": 1}],
  )

  # order by (1 col, NULL)
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1",
    [{"col1": -2}, {"col1": 1}, {"col1": 3}, {"col1": NULL}],
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1 NULLS LAST",
    [{"col1": -2}, {"col1": 1}, {"col1": 3}, {"col1": NULL}],
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1 NULLS FIRST",
    [{"col1": NULL}, {"col1": -2}, {"col1": 1}, {"col1": 3}],
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1 DESC",
    [{"col1": NULL}, {"col1": 3}, {"col1": 1}, {"col1": -2}],
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1 DESC NULLS FIRST",
    [{"col1": NULL}, {"col1": 3}, {"col1": 1}, {"col1": -2}],
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1 DESC NULLS LAST",
    [{"col1": 3}, {"col1": 1}, {"col1": -2}, {"col1": NULL}],
  )

  # order by (multi-cols)
  eq_test_nrows(
    "SELECT col1 as a, col2 as b FROM list(zip([1,2,3,1,2],[2,2,0,1,4])) ORDER BY 1"
    " ASC,  2 ASC",
    [
      {"a": 1, "b": 1},
      {"a": 1, "b": 2},
      {"a": 2, "b": 2},
      {"a": 2, "b": 4},
      {"a": 3, "b": 0},
    ],
  )
  eq_test_nrows(
    "SELECT col1 as a, col2 as b FROM list(zip([1,2,3,1,2],[2,2,0,1,4])) ORDER BY 1"
    " DESC, 2 DESC",
    list(
      reversed(
          [
            {"a": 1, "b": 1},
            {"a": 1, "b": 2},
            {"a": 2, "b": 2},
            {"a": 2, "b": 4},
            {"a": 3, "b": 0},
          ]
      )
    ),
  )
  eq_test_nrows(
    "SELECT col1 as a, col2 as b FROM list(zip([1,2,3,1,2],[2,2,0,1,4])) ORDER BY 1"
    " ASC,  2 DESC",
    [
      {"a": 1, "b": 2},
      {"a": 1, "b": 1},
      {"a": 2, "b": 4},
      {"a": 2, "b": 2},
      {"a": 3, "b": 0},
    ],
  )
  eq_test_nrows(
    "SELECT col1 as a, col2 as b FROM list(zip([1,2,3,1,2],[2,2,0,1,4])) ORDER"
    " BY 2,1",
    [
      {"a": 3, "b": 0},
      {"a": 1, "b": 1},
      {"a": 1, "b": 2},
      {"a": 2, "b": 2},
      {"a": 2, "b": 4},
    ],
  )
  eq_test_nrows(
    "SELECT col1 as a, col2 as b FROM list(zip([1,2,3,1,2],[2,2,0,1,4])) ORDER BY 2"
    " DESC, 1 DESC",
    list(
      reversed(
        [
          {"a": 3, "b": 0},
          {"a": 1, "b": 1},
          {"a": 1, "b": 2},
          {"a": 2, "b": 2},
          {"a": 2, "b": 4},
        ]
      )
    ),
  )

  # order by (with limit / offset / where)
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] WHERE col1 > 0 ORDER BY 1 DESC NULLS LAST",
    [{"col1": 3}, {"col1": 1}],
  )
  eq_test_nrows(
    "SELECT abs(col1) as col1 FROM [1,-2,NULL,3] ORDER BY 1 LIMIT 2",
    [{"col1": 1}, {"col1": 2}],
  )
  eq_test_nrows(
    "SELECT abs(col1) as col1 FROM [1,-2,NULL,3] ORDER BY 1 LIMIT 2 OFFSET 1",
    [{"col1": 2}, {"col1": 3}],
  )
  eq_test_nrows(
    "SELECT * FROM [1,-2,NULL,3] ORDER BY 1 LIMIT 0",
    [],
  )

def test_agg():
  import math
  from functools import reduce

  # aggregate functions (overall)
  funcs = (
    # sql func, python func, remove nulls on python func?
    ("sum_agg(col1)", lambda x: sum(x) if x else NULL, True),
    (
      "prod_agg(col1)",
      lambda x: reduce(lambda a, b: a * b, x) if x else NULL,
      True,
    ),
    ("count_agg(col1)", len, True),
    ("count_agg(*)", len, False),
    ("avg_agg(col1)", lambda x: sum(x) / len(x) if x else NULL, True),
    ("min_agg(col1)", lambda x: min(x) if x else NULL, True),
    ("max_agg(col1)", lambda x: max(x) if x else NULL, True),
    ("list_agg(col1)", lambda x: list(x), False),
    ("list_agg(col1, False)", lambda x: list(x), True),
    ('string_agg(col1,",")', lambda x: ",".join(map(str, x)), True),
    ('string_agg(col1,",",True)', lambda x: ",".join(map(str, x)), False),
    (
      "sorted(list(set_agg(col1)), key=lambda x: (x is NULL, x))",
      lambda y: sorted(list(set(y)), key=lambda x: (x is NULL, x)),
      False,
    ),
    ("sorted(list(set_agg(col1, False)))", lambda y: sorted(list(set(y))), True),
    ("first_agg(col1)", lambda x: x[0] if x else NULL, False),
    ("first_agg(col1, False)", lambda x: x[0] if x else NULL, True),
    ("last_agg(col1)", lambda x: x[-1] if x else NULL, False),
    ("last_agg(col1, False)", lambda x: x[-1] if x else NULL, True),
    ("lag_agg(col1)", lambda x: x[-2] if len(x) > 1 else NULL, False),
    ("lag_agg(col1,2)", lambda x: x[-3] if len(x) > 2 else NULL, False),
    ("count_distinct_agg(col1)", lambda x: len(set(x)), True),
    ("count_distinct_agg(*)", lambda x: len(set(x)), False),
    (
      "any_agg(col1 == 1)",
      lambda x: any(map(lambda y: y == 1, x)) if x else NULL,
      True,
    ),
    (
      "every_agg(col1 > 0)",
      lambda x: all(map(lambda y: y > 0, x)) if x else NULL,
      True,
    ),
  )

  tst_lists = [
    [NULL],
    [NULL, NULL, NULL, NULL],
    [NULL, 11, NULL],
    [NULL, 11, 5, 10, NULL, 3, 3, 10, 4],
    [12],
    range(1, 21),
    range(-10, 6),
    [int(math.cos(x) * 100) / 100.0 for x in range(100)],
  ]
  for tst_list in tst_lists:
    tst_list_clean = list(filter(lambda x: x is not NULL, tst_list))
    for sql_func, tst_func, ignore_nulls in funcs:
      col_name = sql_func[:5]
      lst = tst_list_clean if ignore_nulls else tst_list
      eq_test_1row(
        f"SELECT {sql_func} as {col_name} FROM {tst_list}",
        {col_name: tst_func(lst)},
      )
      eq_test_1row(
        f"SELECT {sql_func} as a, {sql_func}*2 as b FROM {tst_list}",
        {"a": tst_func(lst), "b": tst_func(lst) * 2},
      )

  # this would return a row with 0 in standard SQL, but in SpyQL returns no rows
  eq_test_nrows("SELECT count(*) FROM []", [])

  # partials
  for tst_list in tst_lists:
    eq_test_nrows(
      "SELECT PARTIALS list_agg(col1) as a, count_agg(col1) as c1, count_agg(*)"
      f" as c2, first_agg(col1) as f, lag_agg(col1) as l FROM {tst_list}",
      [
        {
          "a": list(tst_list[:n]),
          "c1": len(list(filter(lambda el: el is not NULL, tst_list[:n]))),
          "c2": n,
          "f": tst_list[0],
          "l": NULL if n < 2 else tst_list[n - 2],
        }
        for n in range(1, len(tst_list) + 1)
      ],
    )

def test_groupby():
  eq_test_1row("SELECT 1 as a FROM range(1) GROUP BY col1", {"a": 1})
  eq_test_1row(
      "SELECT 1 as a, count_agg(*) as c FROM range(1) GROUP BY 1", {"a": 1, "c": 1}
  )
  eq_test_1row(
      "SELECT 1 as a, count_agg(*) as c FROM range(10) GROUP BY 1", {"a": 1, "c": 10}
  )
  eq_test_1row(
      "SELECT 1 as a, 2 as b, count_agg(*) as c FROM range(100) GROUP BY 1, 2",
      {"a": 1, "b": 2, "c": 100},
  )
  eq_test_nrows(
      "SELECT col1 % 2 as a, 2 as b, count_agg(*) as c, min_agg(col1) as mn,"
      " max_agg(col1) as mx FROM range(101) GROUP BY 1, 1+1",
      [
          {"a": 0, "b": 2, "c": 51, "mn": 0, "mx": 100},
          {"a": 1, "b": 2, "c": 50, "mn": 1, "mx": 99},
      ],
  )
  eq_test_nrows(
      "SELECT col1 % 3 as a, 2 as b, max_agg(col1) as mx FROM range(100) GROUP BY 1,"
      " 2 ORDER BY 1 DESC",
      [
          {"a": 2, "b": 2, "mx": 98},
          {"a": 1, "b": 2, "mx": 97},
          {"a": 0, "b": 2, "mx": 99},
      ],
  )
  eq_test_nrows(
      "SELECT col1 % 3 as a, 2 as b, max_agg(col1) as mx FROM range(100) GROUP BY 1,"
      " 2 ORDER BY max_agg(col1)",
      [
          {"a": 1, "b": 2, "mx": 97},
          {"a": 2, "b": 2, "mx": 98},
          {"a": 0, "b": 2, "mx": 99},
      ],
  )

def test_distinct():
  eq_test_1row("SELECT DISTINCT 1 as a FROM range(1)", {"a": 1})
  eq_test_1row("SELECT DISTINCT 1 as a FROM range(10)", {"a": 1})
  eq_test_1row("SELECT DISTINCT 1 as a, 2 as b FROM range(100)", {"a": 1, "b": 2})
  eq_test_nrows(
    "SELECT DISTINCT col1 % 2 as a, 2 as b FROM range(100)",
    [{"a": 0, "b": 2}, {"a": 1, "b": 2}],
  )
  eq_test_nrows(
    "SELECT DISTINCT col1 % 3 as a, 2 as b FROM range(100) ORDER BY 1 DESC",
    [{"a": 2, "b": 2}, {"a": 1, "b": 2}, {"a": 0, "b": 2}],
  )
  eq_test_nrows(
    "SELECT DISTINCT -(col1%3) as a, -(col1%2) as b FROM range(90) ORDER BY 1,2",
    [
      {"a": -2, "b": -1},
      {"a": -2, "b": 0},
      {"a": -1, "b": -1},
      {"a": -1, "b": 0},
      {"a": 0, "b": -1},
      {"a": 0, "b": 0},
    ],
  )

def test_distinct_exploding():
  # Distinct jsons
  target = join_paths(gettempdir(), 'sample.json')
  res = Q(
    f"SELECT DISTINCT data FROM data EXPLODE data->a TO {target}",
  )(
    data = [
      {"a": [1, 2, 2], "b": "three",},
      {"a": [], "b": "none",},
      {"a": [4], "b": "four",},
    ]
  )
  with open(target, "r") as f:
    out = json.load(f)
  assert out == [
    {"a": 1, "b": "three"},
    {"a": 2, "b": "three"},
    {"a": 4, "b": "four"},
  ]

def test_null():
  eq_test_1row("SELECT NULL", {"NULL": NULL})
  eq_test_1row("SELECT NULL+1", {"NULL_1": NULL})
  eq_test_1row("SELECT int('')", {"int": NULL})
  eq_test_1row("SELECT coalesce(NULL,2)", {"coalesce_NULL_2": 2})
  eq_test_1row("SELECT coalesce(3,2)", {"coalesce_3_2": 3})
  eq_test_1row("SELECT nullif(1,1)", {"nullif_1_1": NULL})
  eq_test_1row("SELECT nullif(3,2)", {"nullif_3_2": 3})

def test_custom_syntax():
    # easy access to dic fields
    eq_test_1row(
        "SELECT col1->three * 2 as six, col1->'twenty one' + 3 AS twentyfour,"
        " col1->hello->world.upper() AS caps "
        "FROM [[{'three': 3, 'twenty one': 21,"
        " 'hello':{'world': 'hello world'}}]] "
        "WHERE col1->three > 0 "
        "ORDER BY col1->three",
        {"six": 6, "twentyfour": 24, "caps": "HELLO WORLD"},
    )


# new tests

raw_data = [
  {"name": "A", "age": 20, "salary": 30.},
  {"name": "B", "age": 30, "salary": 12.},
  {"name": "C", "age": 40, "salary": 6.},
  {"name": "D", "age": 50, "salary": 0.40},
]
json_fpath = join_paths(gettempdir(), "spyql_test.jsonl")
with open(json_fpath, "w") as f:
  for d in raw_data:
    f.write(json.dumps(d) + "\n")

csv_fpath = join_paths(gettempdir(), "spyql_test.csv")
with open(csv_fpath, "w") as f:
  f.write('''name, age, salary
A, 20, 30.
B, 30, 12.
C, 40, 6.
D, 50, 0.40''')

def test_ux():
  _q = Q('SELECT data->name as first_name, data->age as user_age FROM data WHERE data->age > 30')
  log.user_debug(f"Query: {_q}")
  out = _q(data = raw_data)
  log.user_debug(f"Output by Query: {out}")
  assert len(out) == 2 # [('C', 40), ('D', 50)]

  out = Q(
    'SELECT data->name as first_name, data->age as user_age FROM data WHERE data->age < 30'
  )(data = raw_data)
  log.user_debug(f"Output functional: {out}")
  assert len(out) == 1 # [('A', 20)]

  # get mean of salaries of people whose age is greater than 30
  def get_mean_salary_math(data):
    salary = [d["salary"] for d in data if d["age"] >= 30]
    return sum(salary) / len(salary)
  log.user_debug(f"Mean salary by math: {get_mean_salary_math(raw_data): .3f}")

  # using SpyQL
  out = Q(
    'SELECT sum_agg(data->salary) / len(data) as sum_salary FROM data WHERE data->age >= 30'
  )(data = raw_data)
  log.user_debug(f"Mean salary by math: {out}")

def test_json_read():
  query = Q(f'SELECT json->name as first_name, json->age as user_age FROM {json_fpath} WHERE json->age > 30')
  out = query()
  assert out ==  [['C', 40], ['D', 50]]

def test_csv_read():
  query = Q(f'SELECT name as first_name, age as user_age FROM {csv_fpath} WHERE age > 30')
  out = query()
  assert out == [['C', 40], ['D', 50]]

def test_csv_write():
  target_csv = join_paths(gettempdir(), "spyql_test_write.csv")
  query = Q(f'SELECT name, age FROM {csv_fpath} WHERE age > 30 TO {target_csv}')
  query()

  with open(target_csv, "r") as f:
    out = f.read()

  assert out.strip().replace("\n", " ") == 'name,age C,40 D,50'

def test_json_write():
  target_json = join_paths(gettempdir(), "spyql_test_write.jsonl")
  query = Q(f'SELECT name, age FROM {csv_fpath} WHERE age > 30 TO {target_json}')
  query()

  with open(target_json, "r") as f:
    data = []
    for l in f.read().strip().splitlines():
      data.append(json.loads(l))

  assert data == [{'name': 'C', 'age': 40}, {'name': 'D', 'age': 50}]

def test_csv_read_json_write():
  target_json = join_paths(gettempdir(), "spyql_test_write.jsonl")
  query = Q(f'SELECT name, age FROM {csv_fpath} WHERE age > 30 TO {target_json}')
  query()

  with open(target_json, "r") as f:
    data = []
    for l in f.read().strip().splitlines():
      data.append(json.loads(l))

  assert data == [{'name': 'C', 'age': 40}, {'name': 'D', 'age': 50}]

def test_complex_interactive():
  query = Q('IMPORT hashlib as hl SELECT hl.md5(col1.encode("utf-8")).hexdigest() FROM data')
  out = query(data = ["a", "b", "c"])
  assert out == [
    ['0cc175b9c0f1b6a831c399e269772661',],
    ['92eb5ffee6ae2fec3ad71c777531578f',],
    ['4a8a08f09d37b73795649038408b5f33',]
  ]

def test_readme():
  # all the tests given in the README
  query = Q(
    "SELECT data->invoice_num AS id, data->items->name AS name, data->items->price AS price FROM data EXPLODE data->items"
  )
  out = query(
    data = [
      {"invoice_num" : 1028, "items": [{"name": "tomatoes", "price": 1.5}, {"name": "bananas", "price": 2.0}]},
      {"invoice_num" : 1029, "items": [{"name": "peaches", "price": 3.12}]}
    ]
  )
  assert out == [[1028, 'tomatoes', 1.5], [1028, 'bananas', 2.0], [1029, 'peaches', 3.12]]

  query = Q(
    'SELECT 10 * cos(col1 * ((pi * 4) / 90)) FROM range(80)'
  )
  out = query()

  query = Q(
    'SELECT * FROM [10 * cos(i * ((pi * 4) / 90)) for i in range(80)]'
  )
  out = query()

# from main_test.py
test_return_values()
test_star_literals()
test_orderby()
test_groupby()
test_distinct()
test_agg()
test_null()
test_custom_syntax()

# FAILING:
# test_distinct_exploding()

# interactive specific tests
test_complex()
test_ux()
test_json_read()
test_csv_read()
test_csv_write()
test_json_write()
test_complex_interactive()
test_readme()
test_csv_read_json_write()
