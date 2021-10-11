"""
.. code-block:: sql

    SELECT *
    FROM json

"""

import operator
from spyql.nulltype import Null


def _init_aggs():
    """Initializes aggregates tracking mechanism"""
    global _agg_idx
    global _agg_key
    global _aggs
    _agg_idx = 0  # pointer to the current aggregate tracker, reset every new row
    _agg_key = ()  # aggregation key of the current row (identifies the group)
    _aggs = dict()  # cumulative of each aggregation function call


def _start_new_agg_row(key):
    """Resets the aggregates tracking mechanism for starting a new row"""
    global _agg_idx
    global _agg_key
    _agg_idx = 0  # reset aggregate call tracker
    _agg_key = key  # set the group


def _get_aggs():
    global _aggs
    return _aggs


def _agg_op(op, val, default=Null):
    """
    Generic aggregation function.
    `val` is the value for the current aggregation of the current row (ignores NULLs).
    `op` should be `function(cumulative_from_prev_rows, value_for_cur_row)`.
    Current mechanism is based on the order of aggregate function calls in the query.
    This might fail if there are flow control statements, which is not checked by the
    parser! (e.g. `SELECT max_agg(x) if x>0 else 0, count_agg(*)` produces unpredictable
    results because in some rows the max function is executed and in others is not.
    Therefore, in some rows the count_agg will have agg_idx 1 and on others agg_idx 0).
    """
    # TODO prevent/detect erroneous aggregation behavior due to flow control statements
    global _agg_idx
    global _agg_key
    global _aggs
    key = (_agg_key, _agg_idx)
    _agg_idx += 1  # moves to the next aggregation (before any return)
    prev_val = _aggs.get(key, default)
    if val is Null:
        return prev_val
    new_val = val if prev_val is Null else op(prev_val, val)
    _aggs[key] = new_val
    return new_val


# Aggregation functions


def sum_agg(val):
    """Sum across all non-null input values"""
    return _agg_op(operator.add, val)


def prod_agg(val):
    """Product across all non-null input values"""
    return _agg_op(operator.mul, val)


def count_agg(val):
    """Count all non-null input values"""
    """`count_agg(*)` counts the number of input rows"""
    return sum_agg(0 if val is Null else 1)


def avg_agg(val):
    """Average all non-null input values"""
    return sum_agg(val) / count_agg(val)


def min_agg(val):
    """Minimum value across all non-null input values"""
    return _agg_op(min, val)


def max_agg(val):
    """Maximum value across all non-null input values"""
    return _agg_op(max, val)


def list_agg(val, respect_nulls=True):
    """
    Collects all input values into a list.
    Filters out NULLs when `respect_nulls` is `False`.
    """
    vals = sum_agg([val] if respect_nulls or val is not Null else Null)
    return [] if vals is Null else vals  # guarantees that result is always an array


def string_agg(val, sep, respect_nulls=False):
    """
    Concatenates all input values into a string.
    Uses `sep` to separate values in the string.
    Filters out NULLs when `respect_nulls` is `False` (default).
    """
    return str(sep).join(
        sum_agg([str(val)] if respect_nulls or val is not Null else Null)
    )


def set_agg(val, respect_nulls=True):
    """
    Collects all distinct input values into a set.
    Filters out NULLs when `respect_nulls` is `False`.
    """
    return _agg_op(operator.or_, {val} if respect_nulls or val is not Null else Null)


def first_agg(val, respect_nulls=True):
    """
    Returns the first value.
    Returns the first non-null value when `respect_nulls` is `False`.
    """
    return _agg_op(
        lambda prev, _: prev, [val] if respect_nulls or val is not Null else Null
    )[0]


def last_agg(val, respect_nulls=True):
    """
    Returns the last value.
    Returns the last non-null value when `respect_nulls` is `False`.
    """
    return _agg_op(
        lambda _, cur: cur, [val] if respect_nulls or val is not Null else Null
    )[0]


def lag_agg(val, offset=1, default=Null):
    """
    Returns the value at `offset` rows before the last row. Returns `default` if there
    is no such row.
    Especially useful with `SELECT PARTIAL` to return the value at `offset` rows before
    the current row.
    """
    res = _agg_op(lambda prev, cur: (cur + prev)[: offset + 1], [val], default)
    return res[-1] if len(res) > offset else Null


def count_distinct_agg(val):
    """Count the number of unique (non-null) input values."""
    """`count_distinct_agg(*)` counts the number of distinct rows."""
    return len(_agg_op(operator.or_, Null if val is Null else {val}))


def any_agg(val):
    """Returns True when there is at least one True value, ignoring NULLs"""
    return _agg_op(operator.or_, Null if val is Null else bool(val))


def every_agg(val):
    """Returns True when all non-null values are True"""
    return _agg_op(operator.and_, Null if val is Null else bool(val))
