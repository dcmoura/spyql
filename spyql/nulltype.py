# Defines the NULL datatype that mimics SQL's NULL
# Unlike None, which throws exceptions, operations with NULLs return NULL
# Allows getting items so that `a.get('b', NULL).get('c', NULL)`` returns NULL
#  when `b` does not exist, and `NULL[x]` returns NULL

import spyql.log


class NullType:
    def __new__(cls):
        return NULL

    def __reduce__(self):
        return (NullType, ())

    def __copy__(self):
        return NULL

    def __deepcopy__(self, a):
        return NULL

    def __call__(self, default):
        pass

    def __repr__(self):
        return "NULL"

    def __str__(self):
        return ""

    def __hash__(self):
        return hash("NULL")

    def __bool__(self):
        return False

    def __lt__(self, other):
        return self

    def __le__(self, other):
        return self

    def __eq__(self, other):
        return self

    def __ne__(self, other):
        return self

    def __ge__(self, other):
        return self

    def __gt__(self, other):
        return self

    def __abs__(self):
        return self

    def __add__(self, other):
        return self

    def __and__(self, other):
        return self

    def __floordiv__(self, other):
        return self

    def __invert__(self):
        return self

    def __lshift__(self, other):
        return self

    def __mod__(self, other):
        return self

    def __mul__(self, other):
        return self

    def __matmul__(self, other):
        return self

    def __neg__(self):
        return self

    def __or__(self, other):
        return self

    def __pos__(self):
        return self

    def __pow__(self, other):
        return self

    def __rshift__(self, other):
        return self

    def __sub__(self, other):
        return self

    def __truediv__(self, other):
        return self

    def __xor__(self, other):
        return self

    def __contains__(self, other):
        return False

    def __delitem__(self, other):
        pass

    def __getitem__(self, other):
        return self

    def __setitem__(self, other, val):
        pass

    def __radd__(self, other):
        return self

    def __rand__(self, other):
        return self

    def __rfloordiv__(self, other):
        return self

    def __rlshift__(self, other):
        return self

    def __rmod__(self, other):
        return self

    def __rmul__(self, other):
        return self

    def __rmatmul__(self, other):
        return self

    def __ror__(self, other):
        return self

    def __rpow__(self, other):
        return self

    def __rrshift__(self, other):
        return self

    def __rsub__(self, other):
        return self

    def __rtruediv__(self, other):
        return self

    def __rxor__(self, other):
        return self

    def __iadd__(self, other):
        return self

    def __iand__(self, other):
        return self

    def __ifloordiv__(self, other):
        return self

    def __ilshift__(self, other):
        return self

    def __imod__(self, other):
        return self

    def __imul__(self, other):
        return self

    __array_priority__ = 10000

    def __imatmul__(self, other):
        return self

    def __ior__(self, other):
        return self

    def __ipow__(self, other):
        return self

    def __irshift__(self, other):
        return self

    def __isub__(self, other):
        return self

    def __itruediv__(self, other):
        return self

    def __ixor__(self, other):
        return self

    def __len__(self):
        return 0

    def __iter__(self):
        return [].__iter__()

    def __round__(self, ndigits=0):
        return self

    def __trunc__(self):
        return self

    def __floor__(self):
        return self

    def __ceil__(self):
        return self

    def get(self, *args, **kwargs):
        return self


# singleton
try:
    NULL
    # explanation here:
    # https://stackoverflow.com/questions/41048643/how-to-create-a-second-none-in-python-making-a-singleton-object-where-the-id-is
except NameError:
    NULL = object.__new__(NullType)
    Null = NULL  # alias
    null = NULL  # alias

# functions that support NULLs (and that need to be replaced in the query)
NULL_SAFE_FUNCS = {
    "int": "int_",
    "float": "float_",
    "str": "str_",
    "complex": "complex_",
}


class NullSafeDict(dict):
    __slots__ = ()  # no __dict__

    @staticmethod
    def __none2null(value):
        if type(value) is list:
            return [NULL if x is None else x for x in value]
        return NULL if value is None else value

    @staticmethod
    def __none2null_dict(adic):
        # TODO: this should work with a list of pairs and not only with a dict
        return {k: NullSafeDict.__none2null(v) for k, v in adic.items()}

    def __init__(self, adic, dirty=True, **kwargs):
        # dirty option keeps None values in dict instead of converting to NULL
        self.update(adic if dirty else NullSafeDict.__none2null_dict(adic), **kwargs)

    def __getitem__(self, key):
        try:
            # none2null is just needed when `dirty` is True (default)...
            return NullSafeDict.__none2null(dict.__getitem__(self, key))
        except KeyError:
            return self.__missing__(key)

    def values(self):
        # attention: does not return a view
        return tuple([NullSafeDict.__none2null(x) for x in super().values()])

    def items(self):
        # attention: does not return a view
        return zip(self.keys(), self.values())

    # returns NULL when key is not found
    def __missing__(self, key):
        spyql.log.user_warning4func("key not found", KeyError(key), key)
        return NULL

    def __hash__(self):
        # TODO make dict immutable
        import json

        # TODO check if this is sufficienly efficient...
        # This only needs to guarantee that two equivalent dicts have the same hash
        return hash(json.dumps(self, default=str, sort_keys=True))


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
