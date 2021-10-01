import operator
from spyql.nulltype import Null


def init_aggs():
    global _agg_idx
    global _agg_key
    global _aggs

    _agg_idx = 0  # pointer to the current aggregate tracker, to reset every new row
    _agg_key = ()  # aggregation key of the current row
    _aggs = dict()  # cumulatives of all aggregations


def start_new_agg_row(key):
    global _agg_idx
    global _agg_key
    _agg_idx = 0
    _agg_key = key


def get_aggs():
    global _aggs
    return _aggs


def _agg_op(op, val):
    """
    Generic aggregation function.
    `val` is the value for the current aggregation of the current row (ignores NULLs).
    `op` should be function(cumulative_from_prev_rows, value_for_cur_row).
    Current mechanism is based on the order of aggregate functions in the query. This
    might fail if there are flow control statements, which is not checked by the parser!
    (e.g. `SELECT max_agg(x) if x>0 else 0, count_agg(*)` produces unpredictable results
    because in some rows the max function is executed and in others is not. Therefore,
    in some rows the count_agg will have agg_idx 1 and on others agg_idx 0)
    """
    # TODO prevent/detect erroneous aggregation behaviour due to flow control statments
    global _agg_idx
    global _agg_key
    global _aggs
    key = (_agg_key, _agg_idx)
    _agg_idx += 1  # moves to the next aggregation (before any return)
    prev_val = _aggs.get(key, Null)
    if val is Null:
        return prev_val
    new_val = val if prev_val is Null else op(prev_val, val)
    _aggs[key] = new_val
    return new_val


# Aggreation functions


def sum_agg(val):
    return _agg_op(operator.add, val)


def prod_agg(val):
    return _agg_op(operator.mul, val)


def count_agg(val):
    return sum_agg(0 if val is Null else 1)


def avg_agg(val):
    return sum_agg(val) / count_agg(val)


def min_agg(val):
    return _agg_op(min, val)


def max_agg(val):
    return _agg_op(max, val)


def array_agg(val, respect_nulls=True):
    vals = sum_agg([val] if respect_nulls or val is not Null else Null)
    return [] if vals is Null else vals  # guarantees that result is always an array


def string_agg(val, sep, respect_nulls=False):
    return str(sep).join(
        sum_agg([str(val)] if respect_nulls or val is not Null else Null)
    )


def set_agg(val, respect_nulls=True):
    return _agg_op(operator.or_, {val} if respect_nulls or val is not Null else Null)


def first_agg(val, respect_nulls=True):
    return _agg_op(
        lambda prev, _: prev, [val] if respect_nulls or val is not Null else Null
    )[0]


def last_agg(val, respect_nulls=True):
    return _agg_op(
        lambda _, cur: cur, [val] if respect_nulls or val is not Null else Null
    )[0]


def count_distinct_agg(val):
    return len(_agg_op(operator.or_, Null if val is Null else {val}))


def any_agg(val):
    return _agg_op(operator.or_, Null if val is Null else bool(val))


def every_agg(val):
    return _agg_op(operator.and_, Null if val is Null else bool(val))
