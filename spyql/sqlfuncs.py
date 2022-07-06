from spyql.nulltype import NULL
import spyql.log

# functions that support NULLs (and that need to be replaced in the query)
NULL_SAFE_FUNCS = {
    "int": "int_",
    "float": "float_",
    "str": "str_",
    "complex": "complex_",
}


# returns default if val is NULL otherwise returns val
def coalesce(val, default):
    if val is NULL:
        return default
    return val


ifnull = coalesce  # alias


# returns NULL if a equals b otherwise returns a
def nullif(a, b):
    if a == b:
        return NULL
    return a


# returns NULL if any argument equals NULL
def null_safe_call(fun, *args, **kwargs):
    if NULL in args or NULL in kwargs.values():
        return NULL
    return fun(*args, **kwargs)


# NULL-safe functions
def float_(a):
    if a is NULL:
        return NULL
    try:
        return float(a)
    except ValueError as e:
        spyql.log.conversion_warning("float", e, a)
        return NULL


def int_(a, *args, **kwargs):
    if a is NULL or NULL in args or NULL in kwargs.values():
        return NULL
    try:
        return int(a, *args, **kwargs)
    except ValueError as e:
        spyql.log.conversion_warning("int", e, a, **kwargs)
        return NULL


def complex_(*args):
    if NULL in args:
        return NULL
    try:
        return complex(*args)
    except ValueError as e:
        spyql.log.conversion_warning("complex", e, *args)
        return NULL


def str_(*args, **kwargs):
    if NULL in args or NULL in kwargs.values():
        return NULL
    return str(*args, **kwargs)
