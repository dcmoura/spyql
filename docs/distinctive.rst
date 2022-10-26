Distinctive features
-----------------------------

This page highlights some of the characteristics of SPyQL that make it unique.

Row order guarantee
^^^^^^^^^^^^^^^^^^^

Unlike in most SQL engines, SPyQL guarantees that the order of the output is the same as the input (if no reordering is done). This is a core feature in SPyQL as it allows for:


* an unique way of working with analytical functions
* deterministic behavior in aggregation queries when a column is not aggregated.

In addition, when reordering data (using the ``ORDER BY`` clause), SPyQL uses a stable algorithm to sort rows, guaranteeing that in case of tie on the sorting criteria, the natural order of the rows prevails.

Natural window for aggregations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On many SQL engines, functions to get the first or last value of a group require analytic functions on top of windows defining the sorting criteria (because there are no guarantees about the processing order of input data). Since SPyQL respects the natural ordering of data, all aggregation functions work on top of a natural window where the order is the input row order, and partitions are defined by the ``GROUP BY`` clause. This allows to have aggregate functions that get the first and last values. Here is a comparison against PostgreSQL for getting the first and last value from a column as well as its sum, when there is guarantee of chronological ordering  (column ``ts``\ ) of the input:

SPyQL:

.. code-block:: sql

   SELECT
       id,
       first_agg(a_column) AS fst,
       last_agg(a_column) AS lst,
       sum_agg(a_column) AS total
   FROM csv
   GROUP BY id

PostgreSQL (one possible solution):

.. code-block:: sql

   SELECT id, fst, lst, sum(a_column) AS total
   FROM (
       SELECT
           auth_name AS id,
           first_value(a_column) over (PARTITION BY auth_name ORDER BY ts) AS fst,
           first_value(a_column) over (PARTITION BY auth_name ORDER BY ts DESC) AS lst,
           a_column
       FROM a_table
   ) AS fl
   GROUP BY id, fst, lst;

No distinction between aggregate and window functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Window and aggregate functions have a lot in common. The main distinction is that aggregate functions collapse the input rows into one row per group while window functions return one row per input row. Despite similarities, SQL adopts very different syntax, requiring the definition of windows with an ``over`` clause.

In SPyQL this has been simplified. The same exact syntax of aggregates is used, with the ``GROUP BY`` clause defining the window partitions, and the order being the natural order of the input (if you need a different order you need to sort data in a former query). To tell that we want one row per input row (window behavior) instead of one row per group, we just need to include the ``PARTIALS`` modifier in the ``SELECT`` clause. Here's an example for getting the total sum of a value vs the running sum.

Aggregation:

.. code-block:: sql

   SELECT sum_agg(col1) AS total_sum FROM [5,10,1]

Output:

.. code-block::

   total_sum
   16

Analytic:

.. code-block:: sql

   SELECT PARTIALS sum_agg(col1) AS run_sum FROM [5,10,1]

Output:

.. code-block::

   run_sum
   5
   15
   16

IMPORT clause
^^^^^^^^^^^^^

SPyQL is all about leveraging the Python ecosystem. So, naturally, it offers an ``IMPORT`` clause to allow importing any Python modules/packages and using them in the query.

.. code-block:: sql

   IMPORT pendulum AS p
   SELECT p.now('Europe/Lisbon').add(days=2)

Natural support for lists, sets, dictionaries, objects, etc
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Practically everything you do in Python you can do in SPyQL. Handling lists or dictionaries, which can be unintuitive or unpractical in many SQL engines becomes a breeze in SPyQL. Compare summing the elements of an array in SPyQL and PostgreSQL:

SPyQL:

.. code-block:: sql

   SELECT sum(array_col) FROM ...

PostgreSQL:

.. code-block:: sql

   SELECT (SELECT sum(a) FROM unnest(array_col) AS a) FROM ...

Run queries on top of files, streams, or python variables/expressions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Every command-line tool that outputs data in JSON/CSV can be piped into SPyQL. 
In addition, you can use python expressions (e.g. list comprehensions) to generate the input data.
