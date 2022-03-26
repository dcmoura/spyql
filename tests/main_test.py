import logging

logging.basicConfig(level=logging.INFO)


from click.testing import CliRunner
import spyql.cli
import spyql.log
from spyql.writer import SpyWriter
from spyql.processor import SpyProcessor
from spyql.nulltype import NULL
from spyql.qdict import qdict
from tabulate import tabulate
import json
import csv
import io
import sqlite3
import math
from functools import reduce
import sys


# --------  AUX FUNCTIONS  --------
def json_output(out):
    return [
        json.loads(line, object_hook=lambda x: qdict(x, dirty=False))
        for line in out.splitlines()
    ]


def txt_output(out, has_header=False):
    if has_header and out.count("\n") == 1:
        return []  # special case when outputs is the header row (output has no data)
    return out.splitlines()


def list_of_struct2pretty(rows):
    if not rows:
        return []
    vals = [  # NULLs should be replaced by Nones before calling tabulate
        {key: (None if val is NULL else val) for (key, val) in row.items()}
        for row in rows
    ]
    return (tabulate(vals, headers="keys", tablefmt="simple") + "\n").splitlines()


def list_of_struct2csv(vals):
    if not vals:
        return []
    out_str = io.StringIO()
    writer = csv.DictWriter(out_str, vals[0].keys())
    writer.writeheader()
    for val in vals:
        writer.writerow(val)
    return out_str.getvalue().splitlines()


def list_of_struct2py(vals):
    if not vals:
        return []
    header = [str(list(vals[0].keys()))]
    rows = [str(tuple(val.values())) for val in vals]
    return header + rows


def spy2py(lines):
    return [str(SpyProcessor.unpack_line(line)) for line in lines]


def run_query(query, data, **kw_options):
    bk = sys.stdin
    sys.stdin = io.StringIO(data)
    res = spyql.query.Query(query, **kw_options)()
    sys.stdin = bk
    return res


def run_cli(query="", options=[], data=None, runner=CliRunner(), exception=True):
    res = runner.invoke(spyql.cli.main, options + [query], input=data)
    return res


def make_cli_options(kw_options):
    options = []
    for option, value in kw_options.items():
        if option.endswith("put_options"):
            options.extend(
                [
                    f"-{option[0].upper()}{opt}" + ("" if val is None else f"={val}")
                    for opt, val in value.items()
                ]
            )
        elif option == "unbuffered":
            if value:
                options.extend(["-u"])
        elif option == "warning_flag":
            options.extend([f"-W{value}"])
        elif option == "v":
            options.extend([f"-v{value}"])
        else:
            options.extend([f"--{option}"])
    return options


def eq_test_nrows(query, expectation, data=None, **kw_options):
    runner = CliRunner()
    spyql.log.user_info("Running query: {}".format(query))

    options = make_cli_options(kw_options)

    res = run_cli(query + " TO json", options, data, runner)
    assert json_output(res.output) == expectation
    assert res.exit_code == 0

    res = run_cli(query + " TO csv", options, data, runner)
    assert txt_output(res.output, True) == list_of_struct2csv(expectation)
    assert res.exit_code == 0

    res = run_cli(query + " TO spy", options, data, runner)
    assert spy2py(txt_output(res.output, True)) == list_of_struct2py(expectation)
    assert res.exit_code == 0

    res = run_cli(query + " TO pretty", options, data, runner)
    assert txt_output(res.output, True) == list_of_struct2pretty(expectation)
    assert res.exit_code == 0

    res = run_query(query + " TO memory", data, **kw_options)
    assert res == tuple(expectation)


def eq_test_1row(query, expectation, **kwargs):
    eq_test_nrows(query, [expectation], **kwargs)


def exception_test(query, anexception, **kw_options):
    res = run_cli(query, make_cli_options(kw_options))
    assert res.exit_code != 0
    assert isinstance(res.exception, anexception)


# --------  TESTS  --------
def test_basic():

    # single column
    # int
    eq_test_1row("SELECT 1", {"_1": 1})
    eq_test_1row("SELECT 1+2", {"_1_2": 3})

    # float
    eq_test_1row("SELECT 1.1", {"_1_1": 1.1})
    eq_test_1row("SELECT 1+0.2", {"_1_0_2": 1.2})

    # text
    eq_test_1row("SELECT '1'", {"_1": "1"})
    eq_test_1row("SELECT '1'+'2'", {"_1_2": "12"})

    # two columns with differemt data types
    eq_test_1row("SELECT '1', 2", {"_1": "1", "_2": 2})

    # alias
    eq_test_1row("SELECT '1' as a, 2 AS Ola", {"a": "1", "Ola": 2})

    # strings with commas and reserved keywords
    eq_test_1row(
        "SELECT 'isto, from you' as 'era uma vez', 2 AS Ola",
        {"era uma vez": "isto, from you", "Ola": 2},
    )

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

    # Distinct jsons
    res = run_cli(
        "SELECT DISTINCT json FROM json EXPLODE json.a TO json",
        data=(
            '{"a": [1, 2, 2], "b": "three"}\n{"a": [], "b": "none"}\n{"a": [4], "b":'
            ' "four"}\n'
        ),
    )
    assert json_output(res.output) == [
        {"a": 1, "b": "three"},
        {"a": 2, "b": "three"},
        {"a": 4, "b": "four"},
    ]
    assert res.exit_code == 0


def test_null():
    eq_test_1row("SELECT NULL", {"NULL": NULL})
    eq_test_1row("SELECT NULL+1", {"NULL_1": NULL})
    eq_test_1row("SELECT int('')", {"int": NULL})
    eq_test_1row("SELECT coalesce(NULL,2)", {"coalesce_NULL_2": 2})
    eq_test_1row("SELECT coalesce(3,2)", {"coalesce_3_2": 3})
    eq_test_1row("SELECT nullif(1,1)", {"nullif_1_1": NULL})
    eq_test_1row("SELECT nullif(3,2)", {"nullif_3_2": 3})


def test_processors():
    # JSON input and NULLs
    eq_test_1row("SELECT json.a FROM json", {"a": 1}, data='{"a": 1}\n')
    eq_test_1row("SELECT json.a FROM json", {"a": NULL}, data='{"a": null}\n')
    eq_test_1row("SELECT json.b FROM json", {"b": NULL}, data='{"a": 1}\n')

    # JSON EXPLODE
    eq_test_nrows(
        "SELECT json.a, json.b FROM json EXPLODE json.a",
        [
            {"a": 1, "b": "three"},
            {"a": 2, "b": "three"},
            {"a": 3, "b": "three"},
            {"a": 4, "b": "four"},
        ],
        data=(
            '{"a": [1, 2, 3], "b": "three"}\n{"a": [], "b": "none"}\n{"a": [4], "b":'
            ' "four"}\n'
        ),
    )
    eq_test_nrows("SELECT * FROM json", [], data="")

    # CSV input and NULLs
    eq_test_nrows(
        "SELECT a as a FROM csv",
        [{"a": 1}, {"a": 4}, {"a": 7}],
        data="a,b,c\n1,2,3\n4,5,6\n7,8,9",
    )
    eq_test_nrows(
        "SELECT a as a FROM csv",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="a,b,c\n,2,3\n4,5,6\n,8,9",
    )
    eq_test_nrows(
        "SELECT a as a FROM csv",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="a,b,c\n,2,3\n4,5,6\n,8,9",
        input_options={"delimiter": ","},
    )
    eq_test_nrows(
        "SELECT int(a) as a FROM csv",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="a,b,c\n,2,3\n4,5,6\noops,8,9",
        input_options={"delimiter": ",", "infer_dtypes": False},
    )
    eq_test_nrows(
        "SELECT a as a FROM csv",
        [{"a": ""}, {"a": "4"}, {"a": ""}],
        data="a,b,c\n,2,3\n4,5,6\n,8,9",
        input_options={"delimiter": ",", "infer_dtypes": False},
    )
    eq_test_nrows(
        "SELECT col1 as a FROM csv",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data=",2,3\n4,5,6\n,8,9",
        input_options={"delimiter": ",", "header": False},
    )
    eq_test_nrows(
        "SELECT col1 as a, col2 as b, col3 as c FROM csv",  # type inference test
        [
            {"a": NULL, "b": 2.0, "c": "3"},
            {"a": 4, "b": 5.0, "c": "ola"},
            {"a": NULL, "b": NULL, "c": ""},
        ],
        data=",2,3\n4,5.0,ola\n,,",
        input_options={"delimiter": ",", "header": False},
    )
    eq_test_nrows("SELECT * FROM csv", [], data="")

    # Text input and NULLs
    eq_test_nrows(
        "SELECT int(col1) as a FROM text",
        [{"a": 1}, {"a": 4}, {"a": 7}],
        data="1\n4\n7",
    )
    eq_test_nrows(
        "SELECT int(col1) as a FROM text",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="\n4\noops",
    )
    eq_test_nrows("SELECT * FROM text", [], data="")

    # SPy input and NULLs
    eq_test_nrows(
        "SELECT a as a FROM spy",
        [{"a": 1}, {"a": 4}, {"a": 7}],
        data="".join(
            [
                SpyWriter.pack(line)
                for line in [["a", "b", "c"], [1, 2, 3], [4, 5, 6], [7, 8, 9]]
            ]
        ),
    )
    eq_test_nrows(
        "SELECT int(a) as a FROM spy",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="".join(
            [
                SpyWriter.pack(line)
                for line in [["a", "b", "c"], [NULL, 2, 3], [4, 5, 6], ["oops", 8, 9]]
            ]
        ),
    )
    eq_test_nrows(
        "SELECT a as a FROM spy",
        [{"a": {"aa": [11, 12, 13]}}, {"a": 4}, {"a": "ok"}],
        data="".join(
            [
                SpyWriter.pack(line)
                for line in [
                    ["a", "b", "c"],
                    [{"aa": [11, 12, 13]}, 2, 3],
                    [4, 5, 6],
                    ["ok", 8, 9],
                ]
            ]
        ),
    )
    eq_test_nrows("SELECT * FROM spy", [], data="")


def test_row_access():
    # JSON input and NULLs
    eq_test_1row("SELECT row.a FROM json", {"a": 1}, data='{"a": 1}\n')
    eq_test_1row("SELECT row.a FROM json", {"a": NULL}, data='{"a": null}\n')
    eq_test_1row("SELECT row.b FROM json", {"b": NULL}, data='{"a": 1}\n')

    # JSON EXPLODE
    eq_test_nrows(
        "SELECT row.a, row.b FROM json EXPLODE row.a",
        [
            {"a": 1, "b": "three"},
            {"a": 2, "b": "three"},
            {"a": 3, "b": "three"},
            {"a": 4, "b": "four"},
        ],
        data=(
            '{"a": [1, 2, 3], "b": "three"}\n{"a": [], "b": "none"}\n{"a": [4], "b":'
            ' "four"}\n'
        ),
    )

    # CSV input
    eq_test_nrows(
        "SELECT row.a FROM csv",
        [{"a": 1}, {"a": 4}, {"a": 7}],
        data="a,b,c\n1,2,3\n4,5,6\n7,8,9",
    )
    eq_test_nrows(
        "SELECT row.a FROM csv",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="a,b,c\n,2,3\n4,5,6\n,8,9",
    )

    # Text input and NULLs
    eq_test_nrows(
        "SELECT int(row.col1) as a FROM text",
        [{"a": 1}, {"a": 4}, {"a": 7}],
        data="1\n4\n7",
    )

    # SPy input and NULLs
    eq_test_nrows(
        "SELECT row.a FROM spy",
        [{"a": 1}, {"a": 4}, {"a": 7}],
        data="".join(
            [
                SpyWriter.pack(line)
                for line in [["a", "b", "c"], [1, 2, 3], [4, 5, 6], [7, 8, 9]]
            ]
        ),
    )
    eq_test_nrows(
        "SELECT int(row.a) as a FROM spy",
        [{"a": NULL}, {"a": 4}, {"a": NULL}],
        data="".join(
            [
                SpyWriter.pack(line)
                for line in [["a", "b", "c"], [NULL, 2, 3], [4, 5, 6], ["oops", 8, 9]]
            ]
        ),
    )

    # Python processor
    eq_test_nrows(
        'SELECT row.a FROM [{"a": 1, "b": 3}, {"a": 4}, {"a": 7}]',
        [{"a": 1}, {"a": 4}, {"a": 7}],
    )


def test_metadata():
    eq_test_nrows(
        "SELECT cols, _values, _names, row FROM csv",
        [
            {
                "cols": [NULL, 2, 3],
                "_values": ["", "2", "3"],
                "_names": ["a", "b", "c"],
                "row": {"a": NULL, "b": 2, "c": 3},
            },
            {
                "cols": [4, 5, 6],
                "_values": ["4", "5", "6"],
                "_names": ["a", "b", "c"],
                "row": {"a": 4, "b": 5, "c": 6},
            },
            {
                "cols": [NULL, 8, 9],
                "_values": ["", "8", "9"],
                "_names": ["a", "b", "c"],
                "row": {"a": NULL, "b": 8, "c": 9},
            },
        ],
        data="a,b,c\n,2,3\n4,5,6\n,8,9",
    )
    eq_test_nrows(
        "SELECT cols, _values, _names, row FROM csv",
        [
            {
                "cols": [NULL, 2, 3],
                "_values": ["", "2", "3"],
                "_names": ["col1", "col2", "col3"],
                "row": {"col1": NULL, "col2": 2, "col3": 3},
            },
            {
                "cols": [4, 5, 6],
                "_values": ["4", "5", "6"],
                "_names": ["col1", "col2", "col3"],
                "row": {"col1": 4, "col2": 5, "col3": 6},
            },
            {
                "cols": [NULL, 8, 9],
                "_values": ["", "8", "9"],
                "_names": ["col1", "col2", "col3"],
                "row": {"col1": NULL, "col2": 8, "col3": 9},
            },
        ],
        data=",2,3\n4,5,6\n,8,9",
        input_options={"delimiter": ",", "header": False},
    )
    eq_test_1row(
        "SELECT cols, _values, _names, row FROM json",
        {
            "cols": [{"a": 1}],
            "_values": [{"a": 1}],
            "_names": ["json"],
            "row": {"a": 1},
        },
        data='{"a": 1}\n',
    )
    eq_test_1row(
        "SELECT cols, _values, _names, row FROM text",
        {
            "cols": ["hello"],
            "_values": ["hello"],
            "_names": ["col1"],
            "row": {"col1": "hello"},
        },
        data="hello\n",
    )
    eq_test_nrows(
        "SELECT cols, _values, _names, row FROM spy",
        [
            {
                "cols": [1, NULL, 3],
                "_values": [1, NULL, 3],
                "_names": ["a", "b", "c"],
                "row": {"a": 1, "b": NULL, "c": 3},
            },
            {
                "cols": [4, 5, 6],
                "_values": [4, 5, 6],
                "_names": ["a", "b", "c"],
                "row": {"a": 4, "b": 5, "c": 6},
            },
            {
                "cols": [7, 8, 9],
                "_values": [7, 8, 9],
                "_names": ["a", "b", "c"],
                "row": {"a": 7, "b": 8, "c": 9},
            },
        ],
        data="".join(
            [
                SpyWriter.pack(line)
                for line in [["a", "b", "c"], [1, NULL, 3], [4, 5, 6], [7, 8, 9]]
            ]
        ),
    )


def test_custom_syntax():
    # easy access to dic fields using ->
    eq_test_1row(
        "SELECT col1->three * 2 as six, col1->'twenty one' + 3 AS twentyfour,"
        " col1->hello->world.upper() AS caps "
        "FROM [[{'three': 3, 'twenty one': 21,"
        " 'hello':{'world': 'hello world'}}]] "
        "WHERE col1->three > 0 "
        "ORDER BY col1->three",
        {"six": 6, "twentyfour": 24, "caps": "HELLO WORLD"},
    )

    # easy access to dic fields using .
    eq_test_1row(
        "SELECT col1.three * 2 as six, col1.hello.world.upper() AS caps "
        "FROM {'three': 3, 'twenty one': 21,"
        " 'hello': {'world': 'hello world'}} "
        "WHERE col1.three > 0 "
        "ORDER BY col1.three",
        {"six": 6, "caps": "HELLO WORLD"},
    )


def test_errors():
    # TODO find way to test custom error output
    exception_test("SELECT 2 + ''", TypeError)
    exception_test("SELECT abc", NameError)
    exception_test("SELECT ,1", SyntaxError)
    exception_test("SELECT 'abcde ", SyntaxError)
    exception_test("SELECT 1 SELECT 2", SyntaxError)
    exception_test("SELECT 1 WHERE True FROM [1]", SyntaxError)
    exception_test("WHERE True", SyntaxError)
    exception_test("SELECT 1 TO _this_writer_does_not_exist_", SyntaxError)
    exception_test("SELECT 1 FROM [1,2,,]]", SyntaxError)
    exception_test("IMPORT _this_module_does_not_exist_ SELECT 1", ModuleNotFoundError)
    exception_test(
        "SELECT 1 TO csv", TypeError, output_options={"nonexisting_option": 1}
    )
    exception_test(
        "SELECT 1 TO plot", TypeError, output_options={"nonexisting_option": 1}
    )
    exception_test(
        "SELECT 1 FROM csv", TypeError, input_options={"nonexisting_option": 1}
    )
    exception_test(
        "SELECT 1 FROM spy", TypeError, input_options={"nonexisting_option": 1}
    )
    exception_test("SELECT 1 TO csv", TypeError, output_options={"delimiter": "bad"})
    exception_test("SELECT 1 FROM csv", TypeError, input_options={"delimiter": "bad"})
    exception_test("SELECT 1 TO csv", SystemExit, output_options={"la": None})
    exception_test("SELECT int('abcde')", ValueError, warning_flag="error")
    exception_test("SELECT float('')", ValueError, warning_flag="error")
    exception_test("SELECT float('')", ValueError, warning_flag="error")
    exception_test("SELECT DISTINCT count_agg(1)", SyntaxError)
    exception_test("SELECT count_agg(1) GROUP BY 1", SyntaxError)
    exception_test("SELECT 1 FROM range(3) WHERE max_agg(col1) > 0", SyntaxError)
    exception_test(
        "SELECT row.a FROM [{'a':1},{'a':2},{'a':3}] EXPLODE row.a", TypeError
    )


def test_sql_output():
    """
    Writes to an in memory sqlite DB and reads back to test
    """
    conn = sqlite3.connect(":memory:")
    conn.cursor().execute(
        """CREATE TABLE test1(
        aint int,
        afloat numeric(2,1),
        aintnull int,
        astrnull text,
        astr text,
        alist text,
        adict text)
    """
    )

    query = """
        SELECT
            col1 as aint,
            col1/2 + 0.01 as afloat,
            NULL if col1==2 else 100 as aintnull,
            NULL if col1==1 else 'hello' as astrnull,
            'abc' + str(col1) as astr,
            [1,2,3] as alist,
            {'a':col1, 'a2': col1*2} as adict
        FROM [1,2,3]
        TO sql
        """

    res = run_cli(query, ["-Otable=test1", "-Ochunk_size=2"])
    assert res.exit_code == 0
    for insert_stat in res.output.splitlines():
        # run inserts from spyql in sqlite
        conn.cursor().execute(insert_stat)

    result = conn.cursor().execute("select * from test1").fetchall()
    conn.close()
    expectation = [
        (1, 0.51, 100, None, "abc1", "[1, 2, 3]", "{'a': 1, 'a2': 2}"),
        (2, 1.01, None, "hello", "abc2", "[1, 2, 3]", "{'a': 2, 'a2': 4}"),
        (3, 1.51, 100, "hello", "abc3", "[1, 2, 3]", "{'a': 3, 'a2': 6}"),
    ]
    assert expectation == result


def test_plot_output():
    # just checking that it does not break...
    res = run_cli("SELECT col1 as abc, col1*2 FROM range(20) TO plot")
    assert res.exit_code == 0
    res = run_cli("SELECT col1 FROM [1,2,NULL,3,None,4] TO plot")
    assert res.exit_code == 0
