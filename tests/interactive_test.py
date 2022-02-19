import logging
logging.basicConfig(level=logging.INFO)

import json
from tempfile import gettempdir

from spyql.interactive import Q
from spyql import log
from spyql.utils import folder, join

def eq_test_nrows(query, expectation, **kwargs):
  log.user_info(f"----")
  q = Q(query)
  log.user_info(f"{q}")
  res = q(**kwargs)
  log.user_info(f"{len(expectation)} vs {len(res)} => {res} ")
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


def test_ux():
  data = [
    {"name": "A", "age": 20, "salary": 30.},
    {"name": "B", "age": 30, "salary": 12.},
    {"name": "C", "age": 40, "salary": 6.},
    {"name": "D", "age": 50, "salary": 0.40},
  ]

  _q = Q('SELECT data->name as first_name, data->age as user_age FROM data WHERE data->age > 30')
  log.user_info(f"Query: {_q}")
  out = _q(data = data)
  log.user_info(f"Output by Query: {out}")
  assert len(out) == 2 # [('C', 40), ('D', 50)]

  out = Q(
    'SELECT data->name as first_name, data->age as user_age FROM data WHERE data->age < 30'
  )(data = data)
  log.user_info(f"Output functional: {out}")
  assert len(out) == 1 # [('A', 20)]

  # get mean of salaries of people whose age is greater than 30
  def get_mean_salary_math(data):
    salary = [d["salary"] for d in data if d["age"] >= 30]
    return sum(salary) / len(salary)
  log.user_info(f"Mean salary by math: {get_mean_salary_math(data): .3f}")

  # using SpyQL
  out = Q(
    'SELECT sum_agg(data->salary) / len(data) as sum_salary FROM data WHERE data->age >= 30'
  )(data = data)
  log.user_info(f"Mean salary by math: {out}")


def test_json():
  data = [
    {"name": "A", "age": 20, "salary": 30.},
    {"name": "B", "age": 30, "salary": 12.},
    {"name": "C", "age": 40, "salary": 6.},
    {"name": "D", "age": 50, "salary": 0.40},
  ]
  fpath = join(gettempdir(), "spyql_test.jsonl")
  with open(fpath, "w") as f:
    for d in data:
      f.write(json.dumps(d) + "\n")

  query = Q(
    f'SELECT json->name as first_name, json->age as user_age FROM {fpath} WHERE json->age > 30'
  )
  out = query()
  assert out == [('C', 40), ('D', 50)]


def test_csv():
  data = '''
name, age, salary
A, 20, 30.
B, 30, 12.
C, 40, 6.
D, 50, 0.40
  '''.strip()
  fpath = join(gettempdir(), "spyql_test.csv")
  with open(fpath, "w") as f:
    f.write(data)

  query = Q(
    f'SELECT name as first_name, age as user_age FROM {fpath} WHERE age > 30'
  )
  out = query()
  assert out == [('C', 40), ('D', 50)]
  

test_return_values()
test_star_literals()
test_complex()

test_ux()
test_json()
test_csv()
