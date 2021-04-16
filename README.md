# SpyQL: SQL with Python in the midle

> **ATTENTION**: SpySQL is in its early days and has not been release yet. It is not yet ready for final users. Feel free to reach me out if you would like to contribute (take a look at the TODO list at then end)!

## Concept
SpyQL is as query language that combines:
* the simplicity and structure of SQL
* with the power and readability of Python 

```SQL
SELECT
    date.fromtimestamp(purchase_ts) AS purchase_date,
    price * quantity AS total 
FROM csv 
WHERE department.upper() == 'IT' 
TO json 
```

With the SpyQL command line tool you can make SQL-like SELECTs powered by Python on top of text data (e.g. CSV and JSON). Data can come from files but also from data streams, such as as Kafka, or from databases such as PostgreSQL. Basically, data can come from any command that ouptuts text :-). More, data can be genererate by a Python iterator! Take a look at the examples section to see how to query parquet, make API calls, transverse directories of zipped jsons, among many other things.

SpyQL also allows you to easily convert between text data formats: 
* `FROM`: CSV, JSON, TEXT and Python iterators (YES, you can use a list comprehension as the data source)
* `TO`: CSV, JSON, TEXT, SQL (INSERT statments), pretty terminal printing, and terminal plotting. 


## Principles

Right now the focus is on bulding a command line tool that follow these core principles:
* **Simple**: simple to use with a straifghtforward implementation
* **Familiar**: you should feel at home if you are acquainted with SQL and Python
* **Light**: small memory footprint that allows you to process large data that fit into your machine
* **Useful**: it should make your life easier, filling a gap in the eco-system

## Syntax
```SQL
SELECT 
    [ * | python_expression [ AS output_column_name ] [, ...] ]    
    [ FROM csv | spy | text | python_expression | json [ EXPLODE path ] ]
    [ WHERE python_expression ]
    [ LIMIT row_count ]
    [ OFFSET num_rows_to_skip ]
    [ TO csv | json | text | spy | sql | pretty | plot ]
```

## Notable differences to SQL
In SpyQL:
* there is guarantee that the order of the output rows is the same as in the input 
* the `AS` keyword must preceed a column alias definition (it is not optional as in SQL)
* you can always access the nth column by using the default column names `colN` (e.g. `col1` for the first column)
* currenty only a small subset of SQL is supported, namely `SELECT` statments without: subqueries, joins, set operations, etc (check the [Sintax](#syntax) section)
* sub-queries are achieved by piping (see the [Examples](#examples)
 section)
* expressions are pure Python:

| SQL | SpySQL |
| ------------- | ------------- |
| x = y | x == y |
| x LIKE y | like(x, y)  |
| x IS NULL  | x == None  |
| x BETWEEN a AND b  |  a <= x <= b | 
| CAST(x AS INTEGER) | int(x) |

## Notable differences to Python
We added additional syntax for making querying easier: 
* Easy access of elements in JSONs and dicts: `json->hello->'planet earth'` translates into `json['hello']['planet earth']`


## Examples

### CSV to JSON (flat)

### CSV to JSON (hierarchical)

### JSON to CSV

### Explode JSON to CSV

### Python iterator/list/comprehension to JSON

### Python multi column iterator to CSV

### Queries on Parquet with directories

### Queries on json.gz with directories

### Kafka to PostegreSQL pipeline

### Subqueries (piping)

### Queries on APIs

### Pretty printing

### Plotting to the terminal
```sh
spyql " \ 
    SELECT col1 \ 
    FROM [10 * cos(i * ((pi * 4) / 90)) for i in range(80)] \
    TO plot"
```

### Plotting with gnuplot

```sh
spyql " \ 
    SELECT col1 \ 
    FROM [10 * cos(i * ((pi * 4) / 90)) for i in range(80)] \
    TO csv" | \ 
sed 1d | \ 
feedgnuplot --terminal 'dumb 80,30' --exit --lines 
```

```sh
spyql " \ 
    SELECT col1 \ 
    FROM [10 * cos(i * ((pi * 4) / 90)) for i in range(80)] \
    TO csv" | \ 
sed 1d | \ 
feedgnuplot --lines --points --exit
```



## TODO list before official release

- [ ] AS table to identify relation (e.g. when writting to SQL)
- [ ] reading NULL/None + tests
- [ ] writing NULL/None + tests
- [ ] limit the scope of variables when evaluating (https://stackoverflow.com/questions/2220699/whats-the-difference-between-eval-exec-and-compile)
- [ ] RE match function
- [ ] `like` function
- [ ] go through SQL opertators/keywords, create functions and include them in tests
- [ ] options for CSVs and alike (in)
- [ ] options for CSVs and alike (out)
- [ ] add option to disable buffering (-u) by calling flush every write
- [ ] infer data types on csv
- [ ] friendly exception reporting and errors messages
- [ ] clean-up repo: https://docs.python-guide.org/writing/structure/
- [ ] installation package, e.g.: https://github.com/glamp/bashplotlib/blob/master/setup.py
- [ ] argument parsing: https://stackabuse.com/command-line-arguments-in-python/
   --unbuffered (-u)
   --sample-size (-s? CSV, JSON, ..), 
   --batch-size (SQL)
   --quiet -q, --verbose -v
- [ ] command line help   
- [ ] 90% code coverage

#### Tentative
- [ ] write/load to/from py (write lists, or csv with header with python data types)
- [ ] create a variable for each JSON field: requires reading a sample, parsing it, store parsed objs, and collecting all fields to a Set
OR
create a syntax to simplify reading json: e.g. `json$vehicle_id, json$data$start_ts` VS. `json['vehicle_id'], json['data']['start_ts']`
- [ ] ORDER BY (in memory for now)
- [ ] add badges to Readme.md: https://dev.to/ananto30/how-to-add-some-badges-in-your-git-readme-github-gitlab-etc-3ne9
- [ ] Better handling of dates constants. e.g. instead of `datetime.datetime(2020,11,10,19,55,03)` use `$2020-11-10 19:55:04$` or `to_datetime('2020-11-10 19:55:04')`


#### Next

- [ ] `GROUP BY` and aggregation functions (in memory for now)
- [ ] `SELECT DISTINCT`


### Known bugs
- [ ] CSV reader does not work well when input is a single row
- [ ] header from CSV is not reaching the writer 

