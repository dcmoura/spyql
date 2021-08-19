import csv
import json as jsonlib
import pickle
import sys
import io
import re
from collections.abc import Iterable
from itertools import islice, chain
from io import StringIO

from spyql.writer import Writer
from spyql.output_handler import OutputHandler
import spyql.nulltype
import spyql.log
from spyql.utils import make_str_valid_varname

# imports for user queries # TODO move to config file
from datetime import datetime, date, timezone  # noqa: F401
from spyql.nulltype import *  # noqa
from math import *  # noqa


# TODO need to find some way to add user imports...
# e.g. ~/.spyql.py file with python code to run at startup


class Processor:
    @staticmethod
    def make_processor(prs, strings, input_options):
        """
        Factory for making a file processor based on the parsed query
        """
        try:
            processor_name = prs["from"]
            if not processor_name:
                return Processor(prs, strings, **input_options)

            processor_name = processor_name.upper()

            if processor_name == "JSON":
                return JSONProcessor(prs, strings, **input_options)
            if processor_name == "CSV":
                return CSVProcessor(prs, strings, **input_options)
            if processor_name == "TEXT":  # single col
                return TextProcessor(prs, strings, **input_options)
            if processor_name == "SPY":
                return SpyProcessor(prs, strings, **input_options)

            return PythonExprProcessor(prs, strings, **input_options)
        except TypeError as e:
            spyql.log.user_error(f"Could not create '{processor_name}' processor", e)

    def __init__(self, prs, strings):
        self.prs = prs  # parsed query
        self.strings = strings  # quoted strings
        self.input_col_names = []  # column names of the input data
        self.translations = (
            spyql.nulltype.NULL_SAFE_FUNCS
        )  # map for alias, functions to be renamed...
        self.has_header = False

    def reading_data(self):
        """
        Returns True after reading header, metadata, etc in input file
        """
        return True

    def handle_header_row(self, row):
        """
        Action for header row (e.g. column name definition)
        """
        pass

    def handle_1st_data_row(self, row):
        """
        Action for handling the first row of data
        """
        self.n_input_cols = len(row) if row else 0

        # dictionary to translate col names to accesses to `_values`
        self.translations.update(
            {
                self.default_col_name(_i): f"_values[{_i}]"
                for _i in range(self.n_input_cols)
            }
        )
        if self.input_col_names:
            # TODO check if len(input_col_names) == self.n_input_cols
            self.translations.update(
                {
                    self.input_col_names[_i]: f"_values[{_i}]"
                    for _i in range(self.n_input_cols)
                }
            )

    def make_out_cols_names(self, out_cols_names):
        """
        Creates list of output column names
        """
        input_col_names = self.input_col_names
        if not input_col_names:
            input_col_names = [
                self.default_col_name(i) for i in range(self.n_input_cols)
            ]
        out_cols_names = [
            (input_col_names if name == "*" else [name]) for name in out_cols_names
        ]
        out_cols_names = [
            name for sublist in out_cols_names for name in sublist
        ]  # flatten
        return out_cols_names

    def get_input_iterator(self):
        """
        Returns iterator over input (e.g. list if rows)
        Each row is list with one value per column
        e.g.
            [[1] ,[2], [3]]:                3 rows with a single col
            [[1,'a'], [2,'b'], [3,'c']]:    3 rows with 2 cols
        """
        return [[None]]  # default: returns a single line with a 'null' column

    def default_col_name(self, idx):
        """
        Default column names, e.g. col1 for the first column
        """
        return f"col{idx+1}"

    def prepare_expression(self, expr):
        """
        Replaces identifiers (column names) in sql expressions by references to
        `_values` and put (quoted) strings back
        """
        if expr == "*":
            return [f"_values[{idx}]" for idx in range(self.n_input_cols)]

        for id, replacement in self.translations.items():
            pattern = rf"\b({id})\b"
            expr = re.compile(pattern).sub(replacement, expr)

        return [self.strings.put_strings_back(expr)]

    def is_clause_single(self, clause):
        """
        True if clause can only have a single expression
        """
        return clause not in ["select"]

    def compile_clause(self, clause, clause_modifier=None, mode="eval"):
        """
        Compiles a clause of the select statment
        """
        prs_clause = self.prs[clause]
        if not prs_clause:
            return None  # empty clause

        if clause_modifier:
            prs_clause = clause_modifier.format(prs_clause)

        single = self.is_clause_single(clause)
        clause_exprs = None
        if single:  # a clause with a single expression like WHERE
            clause_exprs = self.prepare_expression(prs_clause)
            if len(clause_exprs) > 1:
                spyql.log.user_error(
                    f"could not compile {clause.upper()} clause",
                    SyntaxError(
                        f"{clause.upper()} clause should not have more than 1"
                        " expression"
                    ),
                )
            clause_exprs = clause_exprs[0]
        else:  # a clause with multiple expressions like SELECT
            clause_exprs = [self.prepare_expression(c["expr"]) for c in prs_clause]
            clause_exprs = [
                item for sublist in clause_exprs for item in sublist
            ]  # flatten (because of '*')
            clause_exprs = "[" + ",".join(clause_exprs) + "]"

        try:
            return compile(clause_exprs, f"<{clause}>", mode)
        except Exception as main_exception:
            if not single:
                # breaks down clause into expressions and tries
                # compiling one by one to detect in which expression
                # the error happened
                for c in range(len(prs_clause)):
                    try:
                        expr = prs_clause[c]["expr"]
                        translation = self.prepare_expression(expr)
                        for trans in translation:
                            if not trans.strip():
                                raise SyntaxError("empty expression")
                            compile(trans, f"<{clause}>", mode)
                    except Exception as expr_exception:
                        spyql.log.user_error(
                            f"could not compile {clause.upper()} expression #{c+1}",
                            expr_exception,
                            self.strings.put_strings_back(expr),
                        )

            spyql.log.user_error(
                f"could not compile {clause.upper()} clause", main_exception
            )

    def eval_clause(self, clause, clause_exprs, vars, mode="eval"):
        """
        Evaluates/executes a previously compiled clause
        """
        cmd = eval if mode == "eval" else exec
        try:
            return cmd(clause_exprs, {}, vars)
        except Exception as main_exception:
            prs_clause = self.prs[clause]
            if not self.is_clause_single(clause):
                # breaks down clause into expressions and tries
                # evaluating/executing one by one to detect
                # in which expression the error happened
                for c in range(len(prs_clause)):
                    try:
                        expr = prs_clause[c]["expr"]
                        translation = self.prepare_expression(expr)
                        for trans in translation:
                            cmd(trans, {}, vars)
                    except Exception as expr_exception:
                        spyql.log.user_error(
                            f"could not evaluate {clause.upper()} expression #{c+1}",
                            expr_exception,
                            self.strings.put_strings_back(expr),
                            vars,
                        )

            spyql.log.user_error(
                f"could not evaluate {clause.upper()} clause", main_exception, vars=vars
            )

    # main
    def go(self, output_options):
        output_handler = OutputHandler.make_handler(self.prs)
        writer = Writer.make_writer(self.prs["to"], sys.stdout, output_options)
        output_handler.set_writer(writer)
        nrows_in, nrows_out = self._go(output_handler)
        output_handler.finish()
        spyql.log.user_info("#rows  in", nrows_in)
        spyql.log.user_info("#rows out", nrows_out)

    def _go(self, output_handler):
        select_expr = []
        where_expr = None
        explode_expr = None
        explode_cmd = None
        explode_its = [None]  # 1 element by default (no explosion)
        _values = []
        row_number = 0
        input_row_number = 0

        vars = globals()  # to do: filter out not useful/internal vars

        # gets user-defined output cols names (with AS alias)
        out_cols_names = [c["name"] for c in self.prs["select"]]

        # should not accept more than 1 source, joins, etc (at least for now)
        for _values in self.get_input_iterator():
            input_row_number = input_row_number + 1

            vars["_values"] = _values
            vars["input_row_number"] = input_row_number

            if not self.reading_data():
                self.handle_header_row(_values)
                continue

            # print header
            if not select_expr:  # 1st input data row
                self.handle_1st_data_row(_values)
                output_handler.writer.writeheader(
                    self.make_out_cols_names(out_cols_names)
                )
                if output_handler.is_done():
                    return (0, 0)  # in case of `limit 0`

                select_expr = self.compile_clause("select")
                where_expr = self.compile_clause("where")
                explode_expr = self.compile_clause("explode")

            if explode_expr:
                explode_its = self.eval_clause("explode", explode_expr, vars)

            for explode_it in explode_its:
                if explode_expr:
                    vars["explode_it"] = explode_it
                    if not explode_cmd:
                        explode_cmd = self.compile_clause(
                            "explode", "{} = explode_it", mode="exec"
                        )
                    self.eval_clause("explode", explode_cmd, vars, mode="exec")
                    vars["_values"] = _values

                # filter (opt: eventually could be done before exploding)
                if not where_expr or self.eval_clause("where", where_expr, vars):
                    # input line is eligeble
                    row_number = row_number + 1
                    vars["row_number"] = row_number

                    # calculate outputs
                    _res = self.eval_clause("select", select_expr, vars)

                    output_handler.handle_result(_res)  # deal with output
                    if output_handler.is_done():
                        # e.g. when reached limit
                        return (
                            input_row_number - (1 if self.has_header else 0),
                            row_number,
                        )

        return (input_row_number - (1 if self.has_header else 0), row_number)


class PythonExprProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)

    # input is a Python expression
    def get_input_iterator(self):
        e = self.eval_clause("from", self.compile_clause("from"), globals())
        if e:
            if not isinstance(e, Iterable):
                e = [e]
            if not isinstance(e[0], Iterable):
                e = [[el] for el in e]
        return e


class TextProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)

    # reads a text row as a row with 1 column
    def get_input_iterator(self):
        # to do: suport files
        return ([line.rstrip("\n\r")] for line in sys.stdin)


class SpyProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)
        self.has_header = True

    def reading_data(self):
        return self.input_col_names

    def handle_header_row(self, row):
        self.input_col_names = row

    @staticmethod
    def unpack_line(line):
        return pickle.loads(bytes.fromhex(line))

    # input is a serialized Python list converted to hex
    def get_input_iterator(self):
        # to do: suport files
        return (self.unpack_line(line[0:-1]) for line in sys.stdin)


class JSONProcessor(Processor):
    def __init__(self, prs, strings, **options):
        super().__init__(prs, strings)
        jsonlib.loads('{"a": 1}', **options)  # test options
        self.options = options
        self.translations.update({"json": "_values[0]"})  # first column alias as json

    # 1 row = 1 json
    def get_input_iterator(self):
        # TODO suport files

        # this might not be the most efficient way of converting None -> NULL, look at:
        # https://stackoverflow.com/questions/27695901/python-jsondecoder-custom-translation-of-null-type
        return (
            [
                jsonlib.loads(
                    line,
                    object_hook=lambda x: spyql.nulltype.NullSafeDict(x),
                    **self.options,
                )
            ]
            for line in sys.stdin
        )


# CSV
class CSVProcessor(Processor):
    def __init__(self, prs, strings, sample_size=10, header=None, **options):
        super().__init__(prs, strings)
        self.sample_size = sample_size
        self.has_header = header
        self.options = options
        csv.reader(StringIO("test"), **self.options)  # test options

    def get_input_iterator(self):
        # Part 1 reads sample to detect dialect and if has header
        # TODO infer data type
        # TODO force linedelimiter to be new line char set

        # saves sample to a string
        # NOTE if dialect is given and type detection is off we should not need a sample
        sample = io.StringIO()
        for line in list(islice(sys.stdin, self.sample_size)):  # TODO: support files
            sample.write(line)
        sample_val = sample.getvalue()
        if not sample_val:
            return []
        if not self.options:
            # CSV dialect and header detection
            try:
                self.options = {"dialect": csv.Sniffer().sniff(sample_val)}
            except Exception as e:
                spyql.log.user_error("Could not detect CSV dialect from input", e)
            if self.has_header is None:
                try:
                    self.has_header = csv.Sniffer().has_header(sample_val)
                except Exception as e:
                    spyql.log.user_error("Could not detect if input CSV has header", e)
        elif self.has_header is None:
            self.has_header = True  # default if dialect is not automatically detected

        sample.seek(0)  # rewinds the sample
        return chain(
            csv.reader(
                sample, **self.options
            ),  # goes through sample again (for reading input data)
            csv.reader(sys.stdin, **self.options),
        )  # continues to the rest of the file

    def reading_data(self):
        return (not self.has_header) or (self.input_col_names)

    def handle_header_row(self, row):
        self.input_col_names = [make_str_valid_varname(val) for val in row]
