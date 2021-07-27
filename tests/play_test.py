from spyql.spyql import run
from spyql.nulltype import NULL, NullSafeDict
import pytest
import sys
import json
import csv
import io

#def test_query_no_input(query, expected) 

def get_json_output(capsys):
    return [json.loads(
                line,
                object_hook=lambda x: NullSafeDict(x)
            ) for line in capsys.readouterr().out.splitlines()]

def get_output(capsys):
    out = capsys.readouterr().out
    if out.count('\n') == 1:
        return ""  ## special case when outputs is the header row (output has no data)
    return out

def list_of_struct2csv_str(vals):
    if not vals:
        return ""
    out_str = io.StringIO()
    writer = csv.DictWriter(out_str, vals[0].keys())
    writer.writeheader()
    for val in vals:
        writer.writerow(val)    
    return out_str.getvalue()

def list_of_struct2py_str(vals):
    if not vals:
        return ""
    header = [str(list(vals[0].keys()))]
    rows = [str(list(val.values())) for val in vals]
    return '\n'.join(header + rows + [""])

def test_myoutput(capsys, monkeypatch):
    def eq_test_nrows(query, expectation, data = None):
        if data:
            monkeypatch.setattr('sys.stdin', io.StringIO(data))
        run(query + " TO json")    
        assert get_json_output(capsys) == expectation
        if data:
            monkeypatch.setattr('sys.stdin', io.StringIO(data))
        run(query + " TO csv")    
        assert get_output(capsys) == list_of_struct2csv_str(expectation)
        if data:
            monkeypatch.setattr('sys.stdin', io.StringIO(data))
        run(query + " TO py")    
        assert get_output(capsys) == list_of_struct2py_str(expectation)

    def eq_test_1row(query, expectation, data = None):
        eq_test_nrows(query, [expectation], data)
    
    ## single column
    # int
    eq_test_1row("SELECT 1", {"out1": 1})
    eq_test_1row("SELECT 1+2", {"out1": 3})

    # float 
    eq_test_1row("SELECT 1.1", {"out1": 1.1})
    eq_test_1row("SELECT 1+0.2", {"out1": 1.2})

    # text
    eq_test_1row("SELECT '1'", {"out1": '1'})
    eq_test_1row("SELECT '1'+'2'", {"out1": '12'})
    
    
    # two columns with differemt data types
    eq_test_1row("SELECT '1', 2", {"out1": '1', "out2": 2})

    # alias
    eq_test_1row("SELECT '1' as a, 2 AS Ola", {"a": '1', "Ola": 2})

    # strings with commas and reserved keywords
    eq_test_1row("SELECT 'isto, from you' as 'era uma vez', 2 AS Ola", {"era uma vez": 'isto, from you', "Ola": 2})

    # star over a literal
    eq_test_1row("SELECT * FROM 1", {"col1": 1})

    # star over a list
    eq_test_1row("SELECT * FROM [1]", {"col1": 1})
    #TODO: star over JSON, CSV, DIC?, DF? ZIPPED LIST ...

    # get all elements from a list/iterator
    eq_test_nrows("SELECT * FROM [1,2,3]",[ {"col1": 1},  {"col1": 2},  {"col1": 3}])

    # where clause
    eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 2",[{"col1": 2},  {"col1": 3}])

    # where filters out all rows
    eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 10",[])
    
    # where + limit all
    eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 2 LIMIT ALL",[{"col1": 2},  {"col1": 3}])

    # where + large limit 
    eq_test_nrows("SELECT * FROM [1,2,3] WHERE col1 >= 2 LIMIT 1000",[{"col1": 2},  {"col1": 3}])

    # limit
    eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2",[{"col1": 1},  {"col1": 2}])

    # limit and offset 0
    eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2 OFFSET 0",[{"col1": 1},  {"col1": 2}])

    # limit and negative offset
    eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 2 OFFSET -2",[{"col1": 1},  {"col1": 2}])

    # limit and offset
    eq_test_nrows("SELECT * FROM [10,20,30,40,50,60] LIMIT 2 OFFSET 3",[{"col1": 40},  {"col1": 50}])

    # offset only
    eq_test_nrows("SELECT * FROM [10,20,30,40,50,60] OFFSET 2",[{"col1": 30}, {"col1": 40}, {"col1": 50},  {"col1": 60}])

    # limit 0
    eq_test_nrows("SELECT * FROM [1,2,3] LIMIT 0",[])

    # negative limit
    eq_test_nrows("SELECT * FROM [1,2,3] LIMIT -10",[])

    # complex expressions with commas and different types of brackets  
    eq_test_1row("SELECT (col1 + 3) + ({'a': 1}).get('b', 6) + [10,20,30][(1+(3-2))-1] AS calc, 2 AS two FROM [1]", {"calc": 30, "two": 2})
    
    # NULL
    eq_test_1row("SELECT NULL", {"out1": NULL})
    eq_test_1row("SELECT NULL+1", {"out1": NULL})
    eq_test_1row("SELECT int('')", {"out1": NULL})
    eq_test_1row("SELECT coalesce(NULL,2)", {"out1": 2})
    eq_test_1row("SELECT coalesce(3,2)", {"out1": 3})
    eq_test_1row("SELECT nullif(1,1)", {"out1": NULL})
    eq_test_1row("SELECT nullif(3,2)", {"out1": 3})

    # JSON input and NULLs
    eq_test_1row("SELECT json->a FROM json", {"out1": 1}, data = '{"a": 1}\n')
    eq_test_1row("SELECT json->a FROM json", {"out1": NULL}, data = '{"a": null}\n')
    eq_test_1row("SELECT json->b FROM json", {"out1": NULL}, data = '{"a": 1}\n')

    # CSV input and NULLs
    eq_test_nrows("SELECT int(a) as a FROM csv", [{"a": 1},{"a": 4},{"a": 7}], data = 'a,b,c\n1,2,3\n4,5,6\n7,8,9')
    eq_test_nrows("SELECT int(a) as a FROM csv", [{"a": NULL},{"a": 4},{"a": NULL}], data = 'a,b,c\n,2,3\n4,5,6\noops,8,9')

    # Text input and NULLs
    eq_test_nrows("SELECT int(col1) as a FROM text", [{"a": 1},{"a": 4},{"a": 7}], data = '1\n4\n7')
    eq_test_nrows("SELECT int(col1) as a FROM text", [{"a": NULL},{"a": 4},{"a": NULL}], data = '\n4\noops')

    ## custom syntax
    # easy access to dic fields
    eq_test_1row("SELECT col1->three * 2 as six, col1->'twenty one' + 3 AS twentyfour, col1->hello->world.upper() AS caps FROM [[{'three': 3, 'twenty one': 21, 'hello':{'world': 'hello world'}}]]", {"six": 6, "twentyfour": 24, "caps": "HELLO WORLD"})

    # TODO: 
    # explode
    # invalid sentences
    # special functions 
    # JSON input + explode
    # CSV without header




