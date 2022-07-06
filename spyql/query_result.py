from typing import Any


class QueryResult(tuple):
    """
    Result of a query that writes outputs to memory.
    Tuple of dictionaries with easy access of columns by name as attributes.

    Accessing the value of the `age` column in the first row::

        result[0].age
        result[0]["age"]

    Collecting the age for all rows as a tuple::

        result.age
        result.col("age")

    Collecting the age for a subset of rows as a tuple::

        result[1:3].age
        result[1:3].col("age")

    Collecting the value of the first column for all rows as a tuple::

        result.col(0)

    Iterating over rows::

        for row in result:
            print(row.age, row.another_column)

    """

    def __new__(cls, __values, __colnames):
        return super(QueryResult, cls).__new__(cls, __values)

    def __init__(self, __values, __colnames):
        self.__colnames = tuple(__colnames)

    def col(self, idx):
        """
        Collects and returns a tuple with all values of the col defined by
        `idx`.

        :param idx: if `idx` is an integer it refers to the nth column
            (0-based indexing). If `idx` is a string it refers to the name of
            the column.
        """
        if isinstance(idx, int):
            idx = self.__colnames[idx]
        if isinstance(idx, str):
            return tuple([r[idx] for r in self])
        return TypeError(f"Column index of type {type(idx)} not supported")

    def __getattr__(self, name: str) -> Any:
        return self.col(name)

    def __getitem__(self, idx):
        res = super().__getitem__(idx)
        return QueryResult(res, self.__colnames) if isinstance(res, tuple) else res

    def colnames(self):
        """Returns a tuple with the name of each column.

        :rtype: tuple[str]"""
        return self.__colnames
