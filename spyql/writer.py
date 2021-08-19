import csv
import json as jsonlib
import pickle
from tabulate import tabulate  # https://pypi.org/project/tabulate/
import asciichartpy as chart

from spyql.log import user_error
from spyql.nulltype import NULL


class Writer:
    @staticmethod
    def make_writer(writer_name, outputfile, options):
        try:
            if not writer_name:
                return CSVWriter(outputfile, **options)
            writer_name = writer_name.upper()
            if writer_name == "CSV":
                return CSVWriter(outputfile, **options)
            if writer_name == "JSON":
                return SimpleJSONWriter(outputfile, **options)
            if writer_name == "PRETTY":
                return PrettyWriter(outputfile, **options)
            if writer_name == "SPY":
                return SpyWriter(outputfile, **options)
            if writer_name == "SQL":
                return SQLWriter(outputfile, **options)
            if writer_name == "PLOT":
                return PlotWriter(outputfile, **options)
        except TypeError as e:
            user_error(f"Could not create '{writer_name}' writer", e)
        user_error(
            f"Unknown writer '{writer_name}'",
            SyntaxError("Error parsing TO statement"),
            writer_name,
        )

    def __init__(self, outputfile):
        self.outputfile = outputfile

    def writeheader(self, header):
        self.header = header

    def writerow(self, row):
        raise NotImplementedError

    def writerows(self, rows):
        for r in rows:
            self.writerow(r)

    def flush(self):
        pass


class CSVWriter(Writer):
    def __init__(self, outputfile, header=True, **options):
        super().__init__(outputfile)
        self.header_on = header
        self.csv = csv.writer(outputfile, **options)

    def writeheader(self, header):
        if self.header_on:
            self.csv.writerow(header)

    def writerow(self, row):
        self.csv.writerow(row)

    def writerows(self, rows):
        self.csv.writerows(rows)


class SimpleJSONWriter(Writer):
    def __init__(self, outputfile, **options):
        super().__init__(outputfile)
        jsonlib.dumps({"a": 1}, **options)  # test options
        self.options = options

    def writerow(self, row):
        self.outputfile.write(self.makerow(row) + "\n")

    def makerow(self, row):
        single_dict = (
            self.header in [["col1"], ["json"]]
            and len(row) == 1
            and isinstance(row[0], dict)
        )
        obj = row[0] if single_dict else dict(zip(self.header, row))
        return jsonlib.dumps(
            obj, default=lambda x: None if x is NULL else str(x), **self.options
        )


class CollectWriter(Writer):
    """
    Abstract writer that collects all records into a (in-memory) list and dumps all
    the output records at the end.
    Child classes must implement the `writerows` method.
    """

    def __init__(self, outputfile):
        super().__init__(outputfile)
        self.all_rows = []  # needs to store output in memory

    def writerow(self, row):
        self.all_rows.append(row)  # accumulates

    def writerows(self, rows):
        raise NotImplementedError

    def flush(self):
        if self.all_rows:
            self.writerows(self.all_rows)  # dumps


class PrettyWriter(CollectWriter):
    def __init__(self, outputfile, header=True, **options):
        super().__init__(outputfile)
        tabulate([[1, 2, 3]], **options)  # test options
        self.header_on = header
        self.options = options

    def writerows(self, rows):
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
    def __init__(self, outputfile, header=True, height=20):
        super().__init__(outputfile)
        self.header_on = header
        self.height = height

    def writerows(self, rows):
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
    def __init__(self, outputfile):
        super().__init__(outputfile)

    @staticmethod
    def pack(row):
        return pickle.dumps(row).hex() + "\n"

    def writeheader(self, header):
        # TODO first line is a dict with list of cols, version, etc
        self.outputfile.write(self.pack(header))

    def writerow(self, row):
        self.outputfile.write(self.pack(row))


class SQLWriter(Writer):
    def __init__(self, outputfile, chunk_size=1000, table="table_name"):
        super().__init__(outputfile)
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
