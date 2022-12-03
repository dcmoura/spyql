Queries
=======

A query reads lines from an input datasource (default is ``stdin`` in the CLI) and writes results to a destination (default is ``stdout`` in the CLI).
The line is the basic I/O unit in SPyQL (1 line = 1 record).
SPyQL reads a line, processes it, and immediately writes the result to the destination (except for special cases where the result can only be written after processing all input data).
The ``FROM`` clause specifies the input format (e.g. CSV, JSON) while the ``TO`` clause defines the output format. Input and Output format options can be passed as arguments (e.g. ``FROM csv(delimiter=';', header=False)``).

Query syntax
------------

.. code-block::

   [ IMPORT python_module [ AS identifier ] [, ...] ]
   SELECT [ DISTINCT | PARTIALS ]
       [ * | python_expression [ AS output_column_name ] [, ...] ]
       [ FROM csv | spy | text | python_expression | json [ EXPLODE path ] ]
       [ WHERE python_expression ]
       [ GROUP BY output_column_number | python_expression  [, ...] ]
       [ ORDER BY output_column_number | python_expression
           [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
       [ LIMIT row_count ]
       [ OFFSET num_rows_to_skip ]
       [ TO csv | json | spy | sql | pretty | plot ]


Querying data
-------------

Querying CSV and other text-delimited data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

SPyQL supports querying text-delimited data with automatic detection of header, dialect and column type. Internally, SPyQL uses the Python's `csv module <https://docs.python.org/3/library/csv.html>`_ to parse CSV data. The formatting parameters available in the CSV module can be passed when specifying the datasource (e.g. ``SELECT * FROM csv(delimiter=' ')`` for columns separated by spaces). When formatting parameters are omitted, SPyQL tries to detect the dialect using the `Sniffer <https://docs.python.org/3/library/csv.html#csv.Sniffer>`_ class. In addition to formatting parameters, the following input options are available:

* ``header``: boolean telling if the input has a header row with column names. If omitted, SPyQL tries to detect if a header exits using the `Sniffer <https://docs.python.org/3/library/csv.html#csv.Sniffer>`_ class.
* ``infer_dtypes``: boolean telling if the data types of each column should be inferred (default) or if columns are read as strings. Currently, the supported types are ``ìnt`` , ``float``, ``complex`` and ``string``.
* ``sample_size``: int defining the number of lines to read for detection of header and dialect and data type inference. Default is 10.

When a header row is available, columns can be referenced by their name:

.. code-block:: sql

    SELECT my_column_name FROM csv

Columns can always be referenced by their column number using the syntax ``colX`` where X is the number of the column, staring in 1. For example, to select the 1st column you can write:

.. code-block:: sql

    SELECT col1 FROM csv

A list with values for all columns is also available, ``cols``. Finally, columns can be accessed using the ``row`` dictionary or the `.` operator.

.. code-block:: sql

    SELECT .my_column_name       FROM csv
    SELECT row.my_column_name    FROM csv
    SELECT row['my_column_name'] FROM csv


While currently this is the least efficient option, it is the most flexible regarding the data source, since you can use the ``row`` dictionary or the `.` operator to query JSON data.


Querying JSON data
^^^^^^^^^^^^^^^^^^

SPyQL supports querying `JSON lines <https://jsonlines.org>`_ (1 line = 1 JSON). Internally, SPyQL uses Python's `json module <https://docs.python.org/3/library/json.html>`_ to parse each line from the input. Reading as JSON results in a single column containing a dictionary. We recommend using the ``.`` operator or the ``row`` dictionary to access JSON fields. The following queries are equivalent:

.. code-block:: sql

    SELECT .my_key        FROM json
    SELECT row.my_key     FROM json
    SELECT row['my_key']  FROM json

SPyQL also supports `orjson <https://github.com/ijl/orjson>`_, a fast, correct JSON library for Python (you need to `install it <https://github.com/ijl/orjson#install>`_ separately):

.. code-block:: sql

    SELECT .my_key        FROM orjson

Use ``orjson`` if you are working with large JSON files and want to decrease computation time.


Querying plain text
^^^^^^^^^^^^^^^^^^^

SPyQL allows reading lines as strings:

.. code-block:: sql

    SELECT col1 FROM text



Querying Python expressions
^^^^^^^^^^^^^^^^^^^^^^^^^^^

SPyQL allows reading data that result from the evaluation of a Python expression:

.. code-block:: sql

    SELECT .name
    FROM [
        {"name": "Alice", "age": 20, "salary": 30.0},
        {"name": "Bob", "age": 30, "salary": 12.0},
        {"name": "Charles", "age": 40, "salary": 6.0},
        {"name": "Daniel", "age": 43, "salary": 0.40},
    ]
    WHERE .age > 30

.. code-block:: sql

    SELECT col1 FROM range(10)



Query output
------------

The results of executing a query may be written to a file (or stdout), or to an in-memory data structure. When writting to a file, two generic options are available to all formats:

* ``path``: the destination path of the output file (e.g. ``"../myfile.json"``). If ommited the output is written to stdout.
* ``unbuffered``: if output should be writen immediatly (default: ``False``)

Examples:

.. code-block:: sql

    SELECT 1 AS num TO json                   -- writes to stdout
    SELECT 1 AS num TO json("hello.json")     -- writes to a file
    SELECT 1 AS num TO json(unbuffered=True)  -- writes immdediatly to stdout


CSV Output
^^^^^^^^^^^^^

CSV is the default output format of the CLI. SPyQL leverages Python's `csv module <https://docs.python.org/3/library/csv.html>`_ to write CSV data. The formatting parameters available in the CSV module can be passed when specifying the output. Here is an example for setting the column delimiter to a space:

.. code-block:: sql

    SELECT .name, .age
    FROM json
    TO csv(delimiter=' ')


In addition, the following options are available:

* ``header``: if a header with the column names should be included as first line of the output (default: ``True``)

JSON Output
^^^^^^^^^^^^^

The JSON output produces `JSON lines <https://jsonlines.org>`_ (one JSON object per line). There are two alternative output  specifications:

* ``TO json``:  uses Python's `json module <https://docs.python.org/3/library/json.html>`_
* ``TO orjson``:  uses `orjson <https://github.com/ijl/orjson>`_, a fast, correct JSON library for Python (you need to `install it <https://github.com/ijl/orjson#install>`_ separately).

When writting JSONs, the output columns are converted to JSON properties:

.. code-block:: sql

    SELECT col1 AS x, col1**2 AS x2
    FROM [1,2,3]
    TO json

Outputs:

.. code-block:: json

    {"x": 1, "x2": 1}
    {"x": 2, "x2": 4}
    {"x": 3, "x2": 9}

When the output is a single column (and is a dictionary), you can choose having a JSON with a single field on the top, or you can choose to have the column treated as the JSON output by calling it ``row`` or ``json``.

.. code-block:: sql

    SELECT {'x': col1, 'x2': col1**2} AS a
    FROM [1,2,3]
    TO json

Outputs:

.. code-block:: json

    {"a": {"x": 1, "x2": 1}}
    {"a": {"x": 2, "x2": 4}}
    {"a": {"x": 3, "x2": 9}}

While:

.. code-block:: sql

    SELECT {'x': col1, 'x2': col1**2} AS json
    FROM [1,2,3]
    TO json

Outputs:

.. code-block:: json

    {"x": 1, "x2": 1}
    {"x": 2, "x2": 4}
    {"x": 3, "x2": 9}


SQL Output
^^^^^^^^^^^^^

The SQL output produces ``INSERT`` statements that can be pipped into a SQL database like PostgreSQL, MySQL or SQLite, to name a few. Given the following input:

.. code-block:: json

    {"id":23635,"name":"Jerry Green","comment":"Imported from facebook."}
    {"id":23636,"name":"John Wayne","comment":"Imported from facebook."}

the query:

.. code-block:: SQL

    SELECT .id, .name, .comment
    FROM json
    TO sql(table='customer')

would output:

.. code-block:: SQL

    INSERT INTO "customer"("id","name","comment") VALUES (23635,'Jerry Green','Imported from facebook.'),(23636,'John Wayne','Imported from facebook.');

The following options are available:

* ``table``: the name of the output table (where the data will be inserted);
* ``chunk_size``: maximum number of records per ``INSERT`` statement (default is 1000).

Note that the table must exist in the database. Currently, SPyQL does not support creating the table automatically.


SPy Output
^^^^^^^^^^^^^

The SPy output was created to pipe results from a spyql query into another. It passes rows in SPyQL's internal representation so that the following query does not need to do any kind of inference. It also allows to pass any serializable type like lists or sets.

Pretty Output
^^^^^^^^^^^^^

Pretty printing is useful for visualizing the results of a query in a more human-friendly way. It loads the full results set into memory, so it is meant to be used for small outputs.

.. code-block:: SQL

    SELECT .id, .name
    FROM json
    TO pretty

.. code-block::

       id  name
    -----  -----------
    23635  Jerry Green
    23636  John Wayne

Pretty printing leverages the `tabulate <https://https://github.com/astanin/python-tabulate>`_ module. The available options are:

* ``header``: if the header should be printed (default: ``True``)
* ``tablefmt``: the format of the output table (see `tabulates' README <https://github.com/astanin/python-tabulate#table-format>`_ for a full list)


Plot Output
^^^^^^^^^^^^^

Simple ASCII plots are made available via the `asciichart <https://github.com/kroitor/asciichart>`_ module. The available options are:

* ``header``: if a legend should be printed (default: ``True``);
* ``height``: number of lines of the plot in the terminal (default: 20).


Memory Output
^^^^^^^^^^^^^

The memory output is the default when using the SPyQL python module. It returns results in a `QueryResult <reference.html#spyql.query_result.QueryResult>`_ object.


Query processing
----------------

A query retrieves rows from a data source, and processes them one row at a time. SPyQL writes outputs as soon as possible. The flow is the following:

#. IMPORT: before anything else, any python module required for processing the query is loaded.
#. FROM: column names and input processing methods are defined based on the data source type (e.g. CSV, JSON). Then, the data source is processed one row at a time. If an EXPLODE clause is defined (with an array field as argument), the row is replicated for each element in the array.
#. WHERE: the where clause condition determines if an input row is eligible for further processing (or if it is skipped).
#. SELECT: every python expression defined in the select clause is evaluated for the current row. If the query is a ``SELECT DISTINCT``,  duplicated rows are discarded (only the first occurrence goes through). If the query is a ``SELECT PARTIALS``,  partial results from aggregations are written to the output (instead of the final aggregations).  Results are immediately written, unless one of the following:
    * If this is an aggregate query, results are hold until processing all rows (unless the query is a ``SELECT PARTIALS``);
    * If there is an ``ORDER BY`` clause, results are hold until processing all rows.
#. GROUP BY: results are aggregated into groups. There will be one output row per observed group that will be written after processing all input rows (unless the query is a ``SELECT PARTIALS``). Aggregates functions define how to summarize several inputs into a single output per group. When no aggregate function is used, the last processed value of the group holds.
#. ORDER BY: after processing all rows, rows are sorted and then written one by one.
#. OFFSET: the first N rows are skipped.
#. LIMIT: as soon as M rows are written the query finishes executing.
#. TO: defines the format of the output. While some formats immediately write results line by line (e.g. CSV, JSON), some formats might require having all rows before rendering (e.g. pretty printing) or might chunk outputs rows for the sake of performance (e.g. SQL writer).



Clauses
-------

IMPORT clause
^^^^^^^^^^^^^
Single IMPORT clause in the form:

.. code-block:: sql

    module1 as alias1, module 2 as alias2, module3

Example:

.. code-block:: sql

    IMPORT pendulum AS p, random

The form ``from module import identifier`` is not supported.

FROM clause
^^^^^^^^^^^^

The FROM clause specifies the input and can take 2 main forms:

* an input format (e.g. json, csv) and optional input options (e.g. path to file, delimiter);
* a python expression (e.g. a variable, a list comprehension).

Examples
~~~~~~~~

Reading the 1st column of a csv from stdin with default options (auto-detection of header, column types and dialect):

.. code-block:: sql

    SELECT col1 FROM csv

Reading the 1st column of a csv from stdin, forcing the delimiter:

.. code-block:: sql

    SELECT col1 FROM csv(delimiter=';')

Reading the 1st column of a csv from the file ``myfile.csv``, forcing the delimiter:

.. code-block:: sql

    SELECT col1 FROM csv('myfile.csv', delimiter=';')

Generating a sequence of integers using a python expression:

.. code-block:: sql

    SELECT col1 FROM range(10)

Reading from a list of dicts using a python expression:

.. code-block:: sql

    SELECT .name
    FROM [
        {"name": "Alice", "age": 20, "salary": 30.0},
        {"name": "Bob", "age": 30, "salary": 12.0},
        {"name": "Charles", "age": 40, "salary": 6.0},
        {"name": "Daniel", "age": 43, "salary": 0.40},
    ]
    WHERE .age > 30



EXPLODE clause
^^^^^^^^^^^^^^

EXPLODE takes a path to a field in a dictionary that should be iterable (e.g. a list), creating one row for each element in the field. Example:

.. code-block:: sql

    SELECT .name, .departments
    FROM [
        {"name": "Alice", "departments": [1,4]},
        {"name": "Bob", "departments": [2]},
        {"name": "Charles", "departments": []}
    ]
    EXPLODE .departments
    TO json

Results in:

.. code-block:: json

    {"name": "Alice", "departments": 1}
    {"name": "Alice", "departments": 4}
    {"name": "Bob", "departments": 2}




WHERE clause
^^^^^^^^^^^^^^

The WHERE clause thakes a single Python expression that is evaluated as a boolean. Rows that do not hold ``True`` are skipped.


SELECT statement
^^^^^^^^^^^^^^^^

The SELECT comprehends a set of python expressions to be computed over the input data to produce the output. Each expression can be followed by an ``AS alias`` to set the output column name, otherwise spyql generated a column name automatically.

The SELECT can also include a special expresion ``*`` that includes in the output all columns from the input with their original name.

The SELECT keyword can be followed by one of two optional modifiers that change the behaviour of the processing:

* ``DISTINCT``: only outputs unique rows (i.e. rows with the exact same values are skipped). The first unique row from the input data is kept and the remaining duplicated rows are skipped. If an ORDER BY clause is not present, as soon as a new unique row is processed the ouput is written;
* ``PARTIALS``: changes the default behaviour of aggregation queries to ouptut 1 row with partial/cumulative aggregations for each processed row (instead of the default behaviour of 1 output row per group).


Examples
~~~~~~~~

Select all rows:

.. code-block:: sql

   SELECT * FROM [5,10,1,10]

.. code-block::

   col1
   5
   10
   1
   10

Select all distinct rows:

.. code-block:: sql

   SELECT DISTINCT * FROM [5,10,1,10]

.. code-block::

   col1
   5
   10
   1

Aggregate all rows:

.. code-block:: sql

   SELECT sum_agg(col1) AS total_sum FROM [5,10,1,10]

.. code-block::

   total_sum
   26

Partial aggregates:

.. code-block:: sql

   SELECT PARTIALS sum_agg(col1) AS run_sum FROM [5,10,1,10]

.. code-block::

   run_sum
   5
   15
   16
   26


GROUP BY clause
^^^^^^^^^^^^^^^

The GROUP BY clause defines a key that identifies the group each row belongs to. Aggregation functions track each group seperately, producing independent results for each group.

Each element of the GROUP BY key can be:

* an integer ``n`` ranging from 1 to the number of output columns, identifying the nth output column
* a python expression (which can simply be an input column like ``col1``).

Examples
~~~~~~~~

Group by the 2 first output columns:

.. code-block:: sql

    GROUP BY 1,2

Group by the columns with name ``department``:

.. code-block:: sql

    GROUP BY department

.. code-block:: sql

    GROUP BY .department

Group rows using a calculation:

.. code-block:: sql

    GROUP BY col1 % 2



ORDER BY clause
^^^^^^^^^^^^^^^

The ORDER BY clause defines how the output rows are sorted. Each element can be an integer (the nth output column, 1-based) or a python expression, followed by a sorting criteria:

* ``ASC | DESC``: from the smallest to the larget value (default) or from the largest to the smaller
* ``NULLS { FIRST | LAST }``: if NULL values should be on the top (default for desceding order) or at the bottom (default for ascending order)

Examples
~~~~~~~~

Order by the 2 first output columns in ascending order with NULLs at the bottom:

.. code-block:: sql

    ORDER BY 1,2


Order by the first output column in ascending order with NULLs at the top:

.. code-block:: sql

    ORDER BY 1 NULLS FIRST

Order by the ``age`` column in desceding order with NULLs at the bottom, and then by name in asceding order:

.. code-block:: sql

    ORDER BY age DESC NULLS LAST, name

.. code-block:: sql

    ORDER BY .age DESC NULLS LAST, .name


LIMIT clause
^^^^^^^^^^^^

Terminates the query execution as soon as a number of rows are written to the output.

Example, top 5 scores:

.. code-block:: sql

    SELECT .name, .score
    FROM json
    ORDER BY .score DESC NULLS LAST
    LIMIT 5


OFFSET clause
^^^^^^^^^^^^^

Skips the first rows that are written to output.

Example, top 5 scores, except the highest score:

.. code-block:: sql

    SELECT .name, .score
    FROM json
    ORDER BY .score DESC NULLS LAST
    LIMIT 5
    OFFSET 1


TO clause
^^^^^^^^^

Defines the output format and optional output options, including the path to the output file. Default is ``CSV`` in the SPyQL CLI and ``MEMORY`` in the SPyQL module. The ``MEMORY`` output is used for retrieving an in-memory datastructure containing the result of the query when executed in a Python script.

Examples
~~~~~~~~

Output to CSV on stdout:

.. code-block:: sql

    TO csv

Output to CSV ``myfile.csv``

.. code-block:: sql

    TO csv('myfile.csv')

Output to CSV ``myfile.csv`` without header:

.. code-block:: sql

    TO csv('myfile.csv', header=False)



Comments
---------

SPyQL follows Python's approach to comments, everything after a ``#`` is ignored until finding a line break.
``#`` characters are allowed in strings and no escaping is needed, just like in Python.

.. code-block:: python

    # generates a sequence from 1 to 10
    SELECT col1  # we output each value returned by range
    FROM range(1,11)  # interval of range is closed in the beginning and open in the end