from spyql.interactive import Q, q

data = [
  {"name": "Alpha", "age": 20, "salary": 30.},
  {"name": "Beta", "age": 30, "salary": 12.},
  {"name": "Gamma", "age": 40, "salary": 6.},
  {"name": "Delta", "age": 50, "salary": 0.40},
]

_q = Q('SELECT data->name as first_name, data->age as user_age FROM data WHERE data->age > 30')
print("Query:", _q)

out = _q(data = data)
print("Output by Query:", out)

out = q('SELECT data->name as first_name, data->age as user_age FROM data WHERE data->age < 30', data = data)
print("Output functional:", out)

# get mean of salaries of people whose age is greater than 30
def get_mean_salary_math(data):
  salary = [d["salary"] for d in data if d["age"] >= 30]
  return sum(salary) / len(salary)
print(f"Mean salary by math: {get_mean_salary_math(data): .3f}")

# using SpyQL
out = q('SELECT sum_agg(data->salary) / len(data) as sum_salary FROM data WHERE data->age >= 30', data = data)
print(out)


# from unittest import TestCase, main

# from spyql.interactive import q

# def eq_test_nrows(query, expectation, **kwargs):
#   res = q(query + " TO json", **kwargs)
#   # assert len(res) == len(expectation)

# def eq_test_1row(query, expectation, **kwargs):
#   eq_test_nrows(query, [expectation], **kwargs)

# def exception_test(query, anexception, **kwargs):
#   res = q(query, **kwargs)
#   assert res.exit_code != 0
#   assert isinstance(res.exception, anexception)

# class InteractiveTester(TestCase):
#   def test_basic(self):
#     # eq_test_1row("SELECT 1", 1) # {"_1": 1})

#     eq_test_nrows("SELECT sum_agg(data->salary) FROM data WHERE data->name == 'akash'", [{"_1": 30}])
#     # eq_test_1row("SELECT 1+2", 3) # {"_1_2": 3})

#     # # float
#     # eq_test_1row("SELECT 1.1", 1.1) # {"_1_1": 1.1})
#     # eq_test_1row("SELECT 1+0.2", 1.2) # {"_1_0_2": 1.2})

#     # # text
#     # eq_test_1row("SELECT '1'", "1") # {"_1": "1"})
#     # eq_test_1row("SELECT '1'+'2'", "12"), # {"_1_2": "12"})

#     # # two columns with differemt data types
#     # eq_test_1row("SELECT '1', 2", [])# {"_1": "1", "_2": 2})

#     # # alias
#     # eq_test_1row("SELECT '1' as a, 2 AS Ola", {"a": "1", "Ola": 2})

#     # # strings with commas and reserved keywords
#     # eq_test_1row(
#     #     "SELECT 'isto, from you' as 'era uma vez', 2 AS Ola",
#     #     {"era uma vez": "isto, from you", "Ola": 2},
#     # )

#     # # star over a literal
#     # eq_test_1row("SELECT * FROM 1", {"col1": 1})

#     # # star over a list
#     # eq_test_1row("SELECT * FROM [1]", {"col1": 1})
#     # # TODO: star over JSON, CSV, DIC?, DF? ZIPPED LIST ...

#     # # get all elements from a list/iterator
#     # eq_test_nrows("SELECT * FROM [1,2,3]", [{"col1": 1}, {"col1": 2}, {"col1": 3}])

#     # # where clause
#     # eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 2", [{"col1": 2}, {"col1": 3}])

#     # # where filters out all rows
#     # eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 10", [])

#     # # where + limit all
#     # eq_test_nrows(
#     #     "SELECT * FROM [1,2,3] WHERE col1 >= 2 LIMIT ALL", [{"col1": 2}, {"col1": 3}]
#     # )

#     # # where + large limit
#     # eq_test_nrows(
#     #     "SELECT * FROM [1,2,3] WHERE col1 >= 2 LIMIT 1000", [{"col1": 2}, {"col1": 3}]
#     # )

#     # # limit
#     # eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2", [{"col1": 1}, {"col1": 2}])

#     # # limit and offset 0
#     # eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2 OFFSET 0", [{"col1": 1}, {"col1": 2}])

#     # # limit and negative offset
#     # eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2 OFFSET -2", [{"col1": 1}, {"col1": 2}])

#     # # limit and offset
#     # eq_test_nrows(
#     #     "SELECT * FROM [10,20,30,40,50,60] LIMIT 2 OFFSET 3",
#     #     [{"col1": 40}, {"col1": 50}],
#     # )

#     # # offset only
#     # eq_test_nrows(
#     #     "SELECT * FROM [10,20,30,40,50,60] OFFSET 2",
#     #     [{"col1": 30}, {"col1": 40}, {"col1": 50}, {"col1": 60}],
#     # )

#     # # limit 0
#     # eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 0", [])

#     # # negative limit
#     # eq_test_nrows("SELECT * FROM [1,2,3] LIMIT -10", [])

#     # # complex expressions with commas and different types of brackets
#     # eq_test_1row(
#     #     "SELECT (col1 + 3) + ({'a': 1}).get('b', 6) + [10,20,30][(1+(3-2))-1] AS calc,"
#     #     " 2 AS two FROM [1]",
#     #     {"calc": 30, "two": 2},
#     # )

#     # # import
#     # eq_test_1row(
#     #     "IMPORT sys SELECT sys.version_info.major AS major_ver", {"major_ver": 3}
#     # )

#     # eq_test_1row(
#     #     "IMPORT numpy AS np, sys SELECT (np.array([1,2,3])+1).tolist() AS a",
#     #     {"a": [2, 3, 4]},
#     # )

# main()
