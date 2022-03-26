from spyql.nulltype import NULL
import spyql.log


class qdict(dict):
    """
    A dictionary that supports :data:`~spyql.nulltype.NULL` where items can
    be accessed like attributes::

        mydict = qdict({
            "a": 1,
            "b": {
                "c": 2
            },
            "d": None
        })
        mydict.a    # returns 1, same as mydict["a"]
        mydict.z    # returns NULL whenever a key is not found
        mydict.b.c  # returns 2, neested dicts also support attribute access
        mydict.b.x  # returns NULL, neested dicts are null-safe too
        mydict.d    # returns NULL, Nones are converted to NULLs)
    """

    @staticmethod
    def __none2null(value):
        if type(value) is list:
            # TODO consider conversion of tuples/sets/etc
            return [NULL if x is None else x for x in value]
        return NULL if value is None else value

    @staticmethod
    def __none2null_dict(adic):
        # TODO: this should work with a list of pairs and not only with a dict
        return {k: qdict.__none2null(v) for k, v in adic.items()}

    def __init__(self, adic, dirty=True, **kwargs):
        # dirty option keeps None values in dict instead of converting to NULL
        self.update(adic if dirty else qdict.__none2null_dict(adic), **kwargs)
        self.__dict__["_dirty"] = dirty

    def __getitem__(self, key):
        try:
            item = dict.__getitem__(self, key)
            if type(item) is dict:
                # lazy convertion of dicts
                # TODO consider convertion of lists of dicts
                return qdict(item, self._dirty)
            if self._dirty:
                # lazy convertion
                return qdict.__none2null(item)
            return item
        except KeyError:
            return self.__missing__(key)

    def __getattr__(self, key):
        if (
            not key.startswith("__") or key in self
        ):  # because of special methods like __getstate__
            return self[key]
        else:
            raise AttributeError

    def __setattr__(self, key, value):
        self[key] = value

    def values(self):
        """
        Returns a tuple of the dict values.
        Attention: does not return a view like dict!
        """
        return tuple([qdict.__none2null(x) for x in super().values()])

    def items(self):
        """
        Returns a zip of keys and values.
        Attention: does not return a view like dict!
        """
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
