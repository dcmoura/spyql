import csv
import json as jsonlib
import pickle
import io
import sys
import re
import os
from itertools import islice, chain
from io import StringIO
import copy

from spyql.writer import Writer
from spyql.output_handler import OutputHandler
from spyql.query_result import QueryResult
import spyql.nulltype
import spyql.sqlfuncs
import spyql.qdict
import spyql.log
from spyql.utils import make_str_valid_varname, isiterable, is_row_collapsable
import spyql.agg


def init_vars(user_query_vars={}):
    """Initializes dict of variables for user queries"""
    vars = dict()
    # imports for user queries (TODO move to init.py when mature)
    exec(
        "from datetime import datetime, date, timezone\n"
        "from spyql.nulltype import *\n"
        "from spyql.sqlfuncs import *\n"
        "from spyql.qdict import *\n"
        "from spyql.agg import *\n"
        "from math import *\n"
        "import re\n",
        {},
        vars,
    )

    try:
        # user defined imports, functions, etc
        config_home = os.environ.get(
            "XDG_CONFIG_HOME", os.path.expanduser(os.path.join("~", ".config"))
        )
        init_fname = os.path.join(config_home, "spyql", "init.py")
        with open(init_fname) as f:
            exec(f.read(), {}, vars)
            spyql.log.user_debug(f"Succesfully loaded {init_fname}")
    except FileNotFoundError:
        spyql.log.user_debug(f"Init file not found: {init_fname}")
    except Exception as e:
        spyql.log.user_warning(f"Could not load {init_fname}", e)

    # update the accessible vars with user defined vars, if overlap, warn the user
    for x in set(vars.keys()) & set(user_query_vars.keys()):
        spyql.log.user_warning(
            f"Overloading builtin name '{x}', somethings may not work!"
        )
    vars.update(user_query_vars)

    return vars


class Processor:
    @staticmethod
    def input_processors():
        return {
            "JSON": JSONProcessor,
            "CSV": CSVProcessor,
            "TEXT": TextProcessor,
            "SPY": SpyProcessor,
        }

    @staticmethod
    def make_processor(prs, strings, input_options={}):
        """
        Factory for making an input processor based on the parsed query
        """
        try:
            from_clause = prs["from"]
            processor_name = ""
            if not from_clause:  # no from close, single select eval
                processor_name = "default"
                return Processor(prs, strings, **input_options)
            elif isinstance(from_clause, dict):  # there's an input data processor
                processor_name = from_clause["name"]
                processor = Processor.input_processors()[processor_name.upper()]
                input_options.update(from_clause["kwargs"])
                return processor(prs, strings, *from_clause["args"], **input_options)
            else:  # python expression
                processor_name = "python"
                return PythonExprProcessor(prs, strings, **input_options)
        except TypeError as e:
            spyql.log.user_error(f"Could not create '{processor_name}' processor", e)

    def __init__(self, prs, strings, path=None):
        spyql.log.user_debug(f"Loading {self.__class__.__name__}")
        self.prs = prs  # parsed query
        spyql.log.user_debug(self.prs)
        self.strings = strings  # quoted strings
        self.path = path
        try:
            self.input_file = open(path, "r") if path else sys.stdin
        except FileNotFoundError as e:
            spyql.log.user_error(f"Input file not found: {path}", e)
        except Exception as e:
            spyql.log.user_error(f"Could not load {path}", e)

        self.input_col_names = []  # column names of the input data
        self.translations = copy.deepcopy(
            spyql.sqlfuncs.NULL_SAFE_FUNCS
        )  # map for alias, functions to be renamed...
        self.has_header = False
        self.casts = dict()
        self.col_values_exprs = []
        self.writer = None
        self.query_has_reference2row = prs["hints"]["has_reference2row"]

    def close(self):
        if self.path:
            self.input_file.close()

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

        default_col_names = [
            self.default_col_name(_i) for _i in range(self.n_input_cols)
        ]
        self.col_values_exprs = [
            f"{self.casts[_i]}(_values[{_i}])" if _i in self.casts else f"_values[{_i}]"
            for _i in range(self.n_input_cols)
        ]
        # dictionary to translate col names to accesses to `_values`
        self.translations.update(dict(zip(default_col_names, self.col_values_exprs)))
        if self.input_col_names:
            # TODO check if len(input_col_names) == self.n_input_cols
            self.translations.update(
                dict(zip(self.input_col_names, self.col_values_exprs))
            )

        # metadata: list of column names
        _names = self.input_col_names if self.input_col_names else default_col_names
        self.vars["_names"] = _names
        # list of [col1,col2,...]
        cols_expr = "[" + ",".join(self.col_values_exprs) + "]"
        self.translations["cols"] = cols_expr

        # code for instantiating the `row` variable, a dict of {col1: value1, ...} }
        # if the result is a single column of type dict, then returns that dict instead
        # TODO extend collapsing to Pandas, NumPy arrays, etc
        row_expr = (
            self.col_values_exprs[0]
            if is_row_collapsable(row, _names)
            else f"qdict(zip(_names, {cols_expr}))"
        )

        self.row_expr = compile(row_expr, "row_expr", "eval")

    def make_row_meta(self):
        return eval(self.row_expr, self.vars, self.vars)

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
        e.g.::

            [[1] ,[2], [3]]               # 3 rows with a single col
            [[1,'a'], [2,'b'], [3,'c']]   # 3 rows with 2 cols
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
            return self.col_values_exprs

        if isinstance(expr, int):
            # special case: expression is out col number (1-based)
            return [f"_res[{expr-1}]"]  # reuses existing result

        for id, replacement in self.translations.items():
            pattern = rf"(?<![\w\.])({id})\b"
            expr = re.compile(pattern).sub(replacement, expr)

        return [self.strings.put_strings_back(expr)]

    def is_clause_single(self, clause):
        """
        True if clause can only have a single expression
        """
        return clause not in {
            "select",
            "group by",
            "order by",
        }

    def compile_clause(self, clause, clause_modifier=None, mode="eval"):
        """
        Compiles a clause of the query
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
            clause_exprs = ",".join(clause_exprs) + ","  # tuple constructor

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

    def eval_clause(self, clause, clause_exprs, mode="eval"):
        """
        Evaluates/executes a previously compiled clause
        """
        if not clause_exprs:
            return
        cmd = eval if mode == "eval" else exec
        try:
            return cmd(clause_exprs, self.vars, self.vars)

        except Exception as main_exception:
            # this code is useful for debugging and not the actual processing
            prs_clause = self.prs[clause]
            if not self.is_clause_single(clause):
                # breaks down clause into expressions and tries
                # evaluating/executing one by one to detect
                # in which expression the error happened
                for c in range(len(prs_clause)):
                    try:
                        expr = prs_clause[c]["expr"]
                        spyql.log.user_debug("expression", expr)
                        translation = self.prepare_expression(expr)
                        for trans in translation:
                            spyql.log.user_debug("translated expression", trans)
                            cmd(trans, self.vars, self.vars)
                    except Exception as expr_exception:
                        spyql.log.user_error(
                            f"could not evaluate {clause.upper()} expression #{c+1}",
                            expr_exception,
                            self.strings.put_strings_back(expr),
                            self.vars,
                        )

            spyql.log.user_error(
                f"could not evaluate {clause.upper()} clause",
                main_exception,
                vars=self.vars,
            )

    # main
    def go(self, output_options, user_query_vars={}) -> QueryResult:
        output_handler = OutputHandler.make_handler(self.prs)
        self.writer = Writer.make_writer(self.prs["to"], output_options)
        output_handler.set_writer(self.writer)
        nrows_in = self._go(output_handler, user_query_vars)
        output_handler.finish()
        spyql.log.user_info("#rows  in", nrows_in)
        spyql.log.user_info("#rows out", output_handler.rows_written)

        stats = {"rows_in": nrows_in, "rows_out": output_handler.rows_written}
        return self.writer.result(), stats

    def _go(self, output_handler, user_query_vars):
        select_expr = None
        where_expr = None
        explode_expr = None
        explode_cmd = None
        explode_its = [None]  # 1 element by default (no explosion)
        groupby_expr = None
        orderby_expr = None
        _values = []
        _res = tuple()
        _group_res = tuple()
        _sort_res = tuple()
        row_number = 0
        input_row_number = 0

        self.vars = init_vars(user_query_vars)
        spyql.agg._init_aggs()

        # import user modules
        self.eval_clause(
            "import",
            self.compile_clause("import", "import {}", mode="exec"),
            mode="exec",
        )

        # gets user-defined output cols names (with AS alias)
        out_cols_names = [c["name"] for c in self.prs["select"]]

        # should not accept more than 1 source, joins, etc (at least for now)
        for _values in self.get_input_iterator():
            input_row_number = input_row_number + 1

            self.vars["_values"] = _values
            self.vars["input_row_number"] = input_row_number

            if not self.reading_data():
                self.handle_header_row(_values)
                continue

            # print header
            if select_expr is None:  # 1st input data row
                self.handle_1st_data_row(_values)
                output_handler.writer.writeheader(
                    self.make_out_cols_names(out_cols_names)
                )
                if output_handler.is_done():
                    return 0  # in case of `limit 0`

                select_expr = self.compile_clause("select")
                where_expr = self.compile_clause("where")
                explode_expr = self.compile_clause("explode")
                groupby_expr = self.compile_clause("group by")
                orderby_expr = self.compile_clause("order by")

            if self.query_has_reference2row:
                # only builds the row variable if there is a reference to it
                self.vars["row"] = self.make_row_meta()

            if explode_expr:
                explode_its = self.eval_clause("explode", explode_expr)
                if not isiterable(explode_its):
                    spyql.log.user_error(
                        "Invalid EXPLODE clause",
                        TypeError(
                            f"{self.prs['explode']} has type {type(explode_its)}, which"
                            " is not iterable"
                        ),
                        vars=self.vars,
                    )
            for explode_it in explode_its:
                if explode_expr:
                    self.vars["explode_it"] = explode_it
                    if not explode_cmd:
                        explode_cmd = self.compile_clause(
                            "explode", "{} = explode_it", mode="exec"
                        )
                    self.eval_clause("explode", explode_cmd, mode="exec")
                    self.vars["_values"] = _values

                # filter (opt: eventually could be done before exploding)
                if not where_expr or self.eval_clause("where", where_expr):
                    # input line is eligible
                    row_number = row_number + 1
                    self.vars["row_number"] = row_number

                    if groupby_expr:
                        # group by can ref output columns, but does not depend on the
                        # execution of the select clause: refs to output columns are
                        # replaced by the correspondent expression
                        _group_res = self.eval_clause("group by", groupby_expr)
                        # we need to set the group key before running the select because
                        # aggregate functions need to know the group key beforehand
                        spyql.agg._start_new_agg_row(_group_res)

                    # calculate outputs
                    _res = self.eval_clause("select", select_expr)

                    if orderby_expr:
                        # in the order by clause, references to output columns use the
                        # outputs of the evaluation of the select expression
                        self.vars["_res"] = _res
                        _sort_res = self.eval_clause("order by", orderby_expr)

                    is_done = output_handler.handle_result(
                        _res, _sort_res, _group_res
                    )  # deal with output
                    if is_done:
                        # e.g. when reached limit
                        return input_row_number - (1 if self.has_header else 0)

        return input_row_number - (1 if self.has_header else 0)


class PythonExprProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)

    # input is a Python expression or a ref that is passed in the vars.
    def get_input_iterator(self):
        spyql.log.user_debug(f"Trying to read as python expression")
        e = self.eval_clause("from", self.compile_clause("from"))
        if e:
            if not isiterable(e):
                e = [e]
            if not isiterable(e[0]):
                e = [[el] for el in e]
            # casting dict to qdict based on data on the first row
            for i, val in enumerate(e[0]):
                if type(val) is dict:
                    self.casts[i] = "qdict"
        return e


class TextProcessor(Processor):
    def __init__(self, prs, strings, path=None):
        super().__init__(prs, strings, path)

    # reads a text row as a row with 1 column
    def get_input_iterator(self):
        return ([line.rstrip("\n\r")] for line in self.input_file)


class SpyProcessor(Processor):
    def __init__(self, prs, strings, path=None):
        super().__init__(prs, strings, path)
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
        return (self.unpack_line(line[0:-1]) for line in self.input_file)


class JSONProcessor(Processor):
    def __init__(self, prs, strings, path=None, **options):
        super().__init__(prs, strings, path)
        jsonlib.loads('{"a": 1}', **options)  # test options
        self.options = options
        self.input_col_names = ["json"]

    # 1 row = 1 json
    def get_input_iterator(self):
        # this might not be the most efficient way of converting None -> NULL, look at:
        # https://stackoverflow.com/questions/27695901/python-jsondecoder-custom-translation-of-null-type
        decoder = jsonlib.JSONDecoder(
            object_pairs_hook=spyql.qdict.qdict,
            **self.options,
        )

        return ([decoder.decode(line)] for line in self.input_file)


# CSV
class CSVProcessor(Processor):
    def __init__(
        self,
        prs,
        strings,
        path=None,
        sample_size=10,
        header=None,
        infer_dtypes=True,
        **options,
    ):
        super().__init__(prs, strings, path)
        self.sample_size = sample_size
        self.has_header = header
        self.infer_dtypes = infer_dtypes
        self.options = options
        csv.reader(StringIO("test"), **self.options)  # test options

    def _test_dtype(self, v):
        v = v.strip()
        if not v:
            return (-100, None)  # empty string: do not cast
        try:
            int(v)
            return (10, "int_")
        except ValueError:
            try:
                float(v)
                return (20, "float_")
            except ValueError:
                try:
                    complex(v)
                    return (30, "complex_")
                except ValueError:
                    return (100, None)  # not a basic type: do not cast

    def _infer_dtypes(self, reader):
        if self.has_header:
            next(reader, None)  # skip header
        dtypes_rows = [[self._test_dtype(col) for col in line] for line in reader]
        if dtypes_rows and dtypes_rows[0]:
            dtypes = [
                max([row[c] if c < len(row) else (-100, None) for row in dtypes_rows])
                for c in range(len(dtypes_rows[0]))
            ]
            for c in range(len(dtypes)):
                cast = dtypes[c][1]
                if cast:
                    self.casts[c] = cast

    def get_input_iterator(self):
        # Part 1 reads sample to detect dialect and if has header
        # TODO force linedelimiter to be new line char set

        # saves sample to a string
        # NOTE if dialect is given and type detection is off we should not need a sample
        sample = io.StringIO()
        for line in list(islice(self.input_file, self.sample_size)):
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
        if self.infer_dtypes:
            self._infer_dtypes(csv.reader(sample, **self.options))
            sample.seek(0)  # rewinds the sample again

        return chain(
            csv.reader(
                sample, **self.options
            ),  # goes through sample again (for reading input data)
            csv.reader(self.input_file, **self.options),
        )  # continues to the rest of the file

    def reading_data(self):
        return (not self.has_header) or (self.input_col_names)

    def handle_header_row(self, row):
        self.input_col_names = [make_str_valid_varname(val) for val in row]
