import csv
import json as jsonlib
import pickle
from tabulate import tabulate  # https://pypi.org/project/tabulate/
import asciichartpy as chart
from math import nan
import sys
import io

from spyql.log import user_debug, user_error, user_error
from spyql.nulltype import NULL
from spyql.qdict import qdict
from spyql.query_result import QueryResult
from spyql.utils import is_row_collapsable


class Writer:
    @staticmethod
    def output_writers():
        return {
            "JSON": SimpleJSONWriter,
            "CSV": CSVWriter,
            "MEMORY": MemoryWriter,
            "SPY": SpyWriter,
            "PRETTY": PrettyWriter,
            "SQL": SQLWriter,
            "PLOT": PlotWriter,
        }

    @staticmethod
    def make_writer(to_clause, output_options={}):
        """
        Factory for making an output writer based on the parsed query
        """
        try:
            writer_name = to_clause
            if not to_clause:  # not TO clause, defaults to CSV
                writer_name = "CSV"
                return CSVWriter(**output_options)
            elif isinstance(to_clause, dict):  # there's an output data writer
                writer_name = to_clause["name"]
                writer = Writer.output_writers()[writer_name.upper()]
                output_options.update(to_clause["kwargs"])
                return writer(*to_clause["args"], **output_options)
            else:
                user_error(
                    f"Unknown writer '{writer_name}'",
                    SyntaxError("Error parsing TO statement"),
                    writer_name,
                )
        except TypeError as e:
            user_error(f"Could not create '{writer_name}' writer", e)

    def __init__(self, path=None, unbuffered=False):
        user_debug(f"Loading writer {self.__class__.__name__}")
        self.header = []
        self.path = path
        try:
            self.outputfile = open(path, "w") if path else sys.stdout
            if unbuffered:
                self.outputfile = io.TextIOWrapper(
                    open(self.outputfile.fileno(), "wb", 0), write_through=True
                )
        except Exception as e:
            user_error(f"Could not open output file {path}", e)

    def close(self):
        if self.path:
            self.outputfile.close()

    def writeheader(self, header):
        self.header = header

    def writerow(self, row):
        raise NotImplementedError

    def writerows(self, rows):
        for r in rows:
            self.writerow(r)

    def flush(self):
        pass

    def result(self) -> QueryResult:
        """Gets query result, in case of writing to memory"""
        return None


class CSVWriter(Writer):
    def __init__(self, path=None, unbuffered=False, header=True, **options):
        super().__init__(path, unbuffered)
        self.header_on = header
        self.csv = csv.writer(self.outputfile, **options)

    def writeheader(self, header):
        if self.header_on:
            self.csv.writerow(header)

    def writerow(self, row):
        self.csv.writerow(row)

    def writerows(self, rows):
        self.csv.writerows(rows)


class SimpleJSONWriter(Writer):
    def __init__(self, path=None, unbuffered=False, **options):
        super().__init__(path, unbuffered)
        jsonlib.dumps({"a": 1}, **options)  # test options
        self.options = options

    def writerow(self, row):
        self.outputfile.write(self.makerow(row) + "\n")

    def makerow(self, row):
        obj = (
            row[0]
            if is_row_collapsable(row, self.header)
            else dict(zip(self.header, row))
        )
        return jsonlib.dumps(
            obj, default=lambda x: None if x is NULL else str(x), **self.options
        )


class CollectWriter(Writer):
    """
    Abstract writer that collects all records into a (in-memory) list and dumps all
    the output records at the end.
    Child classes must implement the `dumprows` method.
    """

    def __init__(self, path=None, unbuffered=False):
        super().__init__(path, unbuffered)
        self.all_rows = []  # needs to store output in memory

    def transformvalue(self, value):
        return None if value is NULL else value

    def transformrow(self, row):
        return [self.transformvalue(val) for val in row]

    def writerow(self, row):
        self.all_rows.append(self.transformrow(row))  # accumulates

    def dumprows(self, rows):
        raise NotImplementedError

    def flush(self):
        if self.all_rows:
            self.dumprows(self.all_rows)  # dumps


class MemoryWriter(CollectWriter):
    def transformrow(self, row):
        if not self.all_rows:
            # makes decision to collapse based on the first row
            self.__colapse = is_row_collapsable(row, self.header)
        return row[0] if self.__colapse else qdict(zip(self.header, row))

    def result(self):
        return QueryResult(self.all_rows, self.header)

    def dumprows(self, rows):
        pass


class PrettyWriter(CollectWriter):
    def __init__(self, path=None, unbuffered=False, header=True, **options):
        super().__init__(path, unbuffered)
        tabulate([[1, 2, 3]], **options)  # test options
        self.header_on = header
        self.options = options

    def dumprows(self, rows):
        # TODO handle default tablefmt
        self.outputfile.write(
            tabulate(
                rows,
                self.header if self.header_on else [],
                **self.options,
            )
        )
        self.outputfile.write("\n")


class PlotWriter(CollectWriter):
    def __init__(self, path=None, unbuffered=False, header=True, height=20):
        super().__init__(path, unbuffered)
        self.header_on = header
        self.height = height

    def transformvalue(self, value):
        return nan if value is NULL or value is None else value

    def dumprows(self, rows):
        colors = [
            chart.cyan,
            chart.red,
            chart.magenta,
            chart.lightgray,
            chart.green,
            chart.blue,
        ]
        config = {"height": self.height, "colors": colors}

        # first transpose rows into cols
        cols = list(map(list, zip(*rows)))

        self.outputfile.write(chart.plot(cols, config))
        if self.header and self.header_on:
            self.outputfile.write("\n\nLegend: ")
            for i in range(len(self.header)):
                self.outputfile.write(
                    "\t"
                    + colors[i % len(colors)]
                    + "─── "
                    + self.header[i]
                    + chart.reset
                    + " "
                )
            self.outputfile.write("\n")


class SpyWriter(Writer):
    def __init__(self, path=None, unbuffered=False):
        super().__init__(path, unbuffered)

    @staticmethod
    def pack(row):
        return pickle.dumps(row).hex() + "\n"

    def writeheader(self, header):
        # TODO first line is a dict with list of cols, version, etc
        self.outputfile.write(self.pack(header))

    def writerow(self, row):
        self.outputfile.write(SpyWriter.pack(row))


class SQLWriter(Writer):
    def __init__(
        self, path=None, unbuffered=False, chunk_size=1000, table="table_name"
    ):
        super().__init__(path, unbuffered)
        self.chunk_size = chunk_size
        self.table_name = table
        self.chunk = []

    def writeheader(self, header):
        self.statement = (
            f'INSERT INTO "{self.table_name}"('
            + ",".join(['"{}"'.format(h) for h in header])
            + ") VALUES {};\n"
        )

    def writerow(self, row):
        self.chunk.append(
            "({})".format(
                ",".join(
                    [
                        str(v)
                        if isinstance(v, int) or isinstance(v, float)
                        else (
                            "NULL"
                            if v is NULL or v is None
                            else "'{}'".format(str(v).replace("'", "''"))
                        )
                        for v in row
                    ]
                )
            )
        )

        if len(self.chunk) >= self.chunk_size:
            self.writestatement()

    def writestatement(self):
        self.outputfile.write(self.statement.format(",".join(self.chunk)))
        self.chunk = []

    def flush(self):
        if self.chunk:  # write leftovers...
            self.writestatement()
