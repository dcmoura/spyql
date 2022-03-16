import re
import os


def quote_ifstr(s):
    return f"'{s}'" if isinstance(s, str) else s


def make_str_valid_varname(s):
    # remove invalid characters (except spaces in-between)
    s = re.sub(r"[^0-9a-zA-Z_\s]", " ", s).strip()

    # replace spaces by underscores (instead of dropping spaces) for readability
    s = re.sub(r"\s+", "_", s)

    # if first char is not letter or underscore then add underscore to make it valid
    if not re.match("^[a-zA-Z_]", s):
        s = "_" + s

    return s


def try2eval(val, globals={}, locals={}):
    try:
        return eval(val, globals, locals)
    except Exception:
        return val


def isiterable(x):
    """Returns True if `x` is iterable that is not a string or dict and is not null"""
    from spyql.nulltype import Null
    from collections.abc import Iterable

    return (
        isinstance(x, Iterable)
        and x is not Null
        and not isinstance(x, str)
        and not isinstance(x, dict)
    )


def is_row_collapsable(row, colnames):
    """
    Returns True if `row` only has a single column of type dict with a default name.
    In this case, a row (of type dict) can take the value of the first column.
    """
    return (
        len(row) == 1
        and isinstance(row[0], dict)
        and colnames[0] in {"col1", "json", "row"}
    )


def join_paths(x, *args):
    """convienience function for os.path.join"""
    return os.path.join(x, *args)
