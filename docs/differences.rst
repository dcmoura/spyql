Differences to SQL and Python
------------------------------

SPyQL is the result of joining Python and SQL in the same language. We have tried to make SPyQL as faithful as possible to the two but, still, there are differences that should be highlighted.

Differences to SQL
^^^^^^^^^^^^^^^^^^^

In SPyQL:


* there is guarantee that the order of the output rows is the same as in the input (if no reordering is done)
* the ``AS`` keyword must precede a column alias definition (it is not optional as in SQL)
* you can always access the nth input column by using the default column names ``colN`` (e.g. ``col1`` for the first column)
* currently only a small subset of SQL is supported, namely ``SELECT`` statements without: sub-queries, joins, set operations, etc (check the `Syntax <#syntax>`_ section)
* sub-queries are achieved by piping and joins by dictionary lookups (see the `Command line examples <#command line examples>`_ section)
* comments follow Python's syntax  (``# line comment``) instead of the SQL standard (``-- line comment``, ``/* multi-line comment */``)
* in SQL ``SELECT count(1) WHERE False`` returns 1 row with 1 column with value ``0``, while in SPyQL an equivalent query would not return any row
* counting the number of distinct values is done using ``SELECT count_distinct_agg(x)`` instead of ``SELECT count(DISTINCT x)``
* aggregation functions have the suffix ``_agg`` to avoid conflicts with python's built-in functions (e.g. SPyQL uses ``sum_agg`` instead of ``sum`` to avoid conflicts with Python's built-in function):

.. list-table::
   :header-rows: 1

   * - Operation
     - PostgreSQL
     - SPyQL
   * - Sum all values of a column
     - ``SELECT sum(col_name)``
     - ``SELECT sum_agg(col_name)``
   * - Sum an array
     - ``SELECT sum(a) FROM (SELECT unnest(array[1,2,3]) AS a) AS t``
     - ``SELECT sum([1,2,3])``



* expressions are pure Python:

.. list-table::
   :header-rows: 1

   * - SQL
     - SpySQL
   * - ``x = y``
     - ``x == y``
   * - ``x BETWEEN a AND b``
     - ``a <= x <= b``
   * - ``CAST(x AS INTEGER)``
     - ``int(x)``
   * - ``CASE WHEN x > 0 THEN 1 ELSE -1 END``
     - ``1 if x > 0 else -1``
   * - ``upper('hello')``
     - ``'hello'.upper()``


Differences to Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Additional syntax
~~~~~~~~~~~~~~~~~

We added additional syntax for making querying easier (the original python syntax is supported):

.. list-table::
   :header-rows: 1

   * - Python
     - SpySQL shortcut
     - Purpose
   * - ``row['hello']['world']``
     - ``.hello.world``
     - Easy access of elements in  dicts (e.g. JSONs)
   * - ``row['hello']['planet earth']``
     - ``.hello['planet earth']``
     - Easy access of elements in  dicts (e.g. JSONs)

NULL datatype
~~~~~~~~~~~~~

Python's ``None`` generates exceptions when making operations on missing data, breaking query execution (e.g. ``None + 1`` throws a ``TypeError``\ ). To overcome this, we created a ``NULL`` type that has the same behavior as in SQL (e.g. ``NULL + 1`` returns ``NULL``\ ), allowing for queries to continue processing data.

.. list-table::
   :header-rows: 1

   * - Operation
     - Native Python throws
     - SpySQL returns
     - SpySQL warning
   * - ``NULL + 1``
     - ``NameError``
     - ``NULL``
     -
   * - ``a_dict['inexistent_key']``
     - ``KeyError``
     - ``NULL``
     - yes
   * - ``int('')``
     - ``ValueError``
     - ``NULL``
     - yes
   * - ``int('abc')``
     - ``ValueError``
     - ``NULL``
     - yes


The above dictionary key access only returns ``NULL`` if the dict is an instance of ``qdict``. SPyQL adds ``qdict``\ , which extends python's native ``dict``. JSONs are automatically loaded as ``qdict``. Unless you are creating dictionaries on the fly you do not need to worry about this.
