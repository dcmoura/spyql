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
        if not writer_name:
            return CSVWriter(outputfile, options)
        writer_name = writer_name.upper()
        if writer_name == "CSV":
            return CSVWriter(outputfile, options)
        if writer_name == "JSON":
            return SimpleJSONWriter(outputfile, options)
        if writer_name == "PRETTY":
            return PrettyWriter(outputfile, options)
        if writer_name == "SPY":
            return SpyWriter(outputfile, options)
        if writer_name == "SQL":
            return SQLWriter(outputfile, options)
        if writer_name == "PLOT":
            return PlotWriter(outputfile, options)
        user_error(
            "Unknown writer", SyntaxError("Error parsing TO statement"), writer_name
        )

    def __init__(self, outputfile, options):
        self.outputfile = outputfile
        self.options = options

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
    def __init__(self, outputfile, options):
        super().__init__(outputfile, options)
        self.csv = csv.writer(outputfile, **options)

    # TODO allow specifying output CSV parameters
    def writeheader(self, header):
        self.csv.writerow(header)

    def writerow(self, row):
        self.csv.writerow(row)

    def writerows(self, rows):
        self.csv.writerows(rows)


class SimpleJSONWriter(Writer):
    def __init__(self, outputfile, options):
        super().__init__(outputfile, options)

    def writerow(self, row):
        self.outputfile.write(self.makerow(row) + "\n")

    def makerow(self, row):
        single_dict = (
            self.header in [["out1"], ["col1"], ["json"]]
            and len(row) == 1
            and isinstance(row[0], dict)
        )
        obj = row[0] if single_dict else dict(zip(self.header, row))
        return jsonlib.dumps(
            obj, default=lambda x: None if x is NULL else str(x), **self.options
        )


class PrettyWriter(Writer):
    def __init__(self, outputfile, options):
        super().__init__(outputfile, options)
        self.all_rows = []  # needs to store output in memory
        # TODO force a limit on the output and warn user

    def writerow(self, row):
        self.all_rows.append(row)  # accumulates

    def writerows(self, rows):
        # TODO handle default tablefmt
        self.outputfile.write(
            tabulate(rows, self.header, tablefmt="simple", **self.options)
        )
        self.outputfile.write("\n")

    def flush(self):
        if self.all_rows:
            self.writerows(self.all_rows)  # dumps


class PlotWriter(PrettyWriter):
    def __init__(self, outputfile, options):
        super().__init__(outputfile, options)

    def writerows(self, rows):
        colors = [
            chart.cyan,
            chart.red,
            chart.magenta,
            chart.lightgray,
            chart.green,
            chart.blue,
        ]
        config = {"height": 20, "colors": colors}

        # first transpose rows into cols
        cols = list(map(list, zip(*rows)))

        self.outputfile.write(chart.plot(cols, config))
        if self.header:
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
    def __init__(self, outputfile, options):
        super().__init__(outputfile, options)

    @staticmethod
    def pack(row):
        return pickle.dumps(row).hex() + "\n"

    def writeheader(self, header):
        # TODO first line is a dict with list of cols, version, etc
        self.outputfile.write(self.pack(header))

    def writerow(self, row):
        self.outputfile.write(self.pack(row))


class SQLWriter(Writer):
    def __init__(self, outputfile, options):
        super().__init__(outputfile, options)
        self.chunk_size = 10000  # TODO: move to options!
        self.table_name = "table_name"  # TODO: move to options!
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
                            if v is NULL or v == None
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
