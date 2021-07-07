import csv
import json as jsonlib
import sys
import io
import re
from math import *
from collections.abc import Iterable
from itertools import islice, chain
import traceback

from spyql.writer import Writer
from spyql.output_handler import OutputHandler
from spyql.nulltype import *
from spyql.log import *

from datetime import datetime, date, timezone
import pytz


# extra... need to find some way to add user imports...
# e.g. ~/.spyql.py file with python code to run at startup



class Processor:

    @staticmethod
    def make_processor(prs, strings):
        processor_name = prs['from']
        if not processor_name:
            return Processor(prs, strings)

        processor_name = processor_name.upper()

        if processor_name == 'JSON':
            return JSONProcessor(prs, strings)
        if processor_name == 'CSV':
            return CSVProcessor(prs, strings)
        if processor_name == 'TEXT': #single col
            return TextProcessor(prs, strings)

        return PythonExprProcessor(prs, strings)
        # if not reader_name or reader_name == 'CSV':
        #     return CSVWriter(inputfile, options)

        # if reader_name == 'PY':
        #     return PyWriter(inputfile, options)
        # if reader_name == 'SQL':
        #     return SQLWriter(inputfile, options)
        # raise Exception(f"Unknown reader: {reader_name}")



    def __init__(self, prs, strings):
        self.prs = prs #parsed query
        self.strings = strings #quoted strings
        self.input_col_names = [] #column names of the input data
        self.translations = NULL_SAFE_FUNCS #map for column alias, functions that have to be renamed, etc
        self.has_header = False

    # True after header, metadata, etc in input file
    def reading_data(self):
        return True

    # Action for header row (e.g. column name definition)
    def handle_header_row(self, row):
        pass

    # Action for handling the first row of data
    def handle_1st_data_row(self, row):
        self.n_input_cols = len(row) if row else 0

        #dictionary to translate col names to indexes in `_values`
        self.translations.update({self.default_col_name(_i): f'_values[{_i}]' for _i in range(self.n_input_cols)})
        if self.input_col_names:
            #TODO check if len(input_col_names) == self.n_input_cols
            self.translations.update({self.input_col_names[_i]: f'_values[{_i}]' for _i in range(self.n_input_cols)})

    # Create list of output column names
    def make_out_cols_names(self, out_cols_names):
        input_col_names = self.input_col_names
        if not input_col_names:
            input_col_names = [self.default_col_name(i) for i in range(self.n_input_cols)]
        out_cols_names = [(input_col_names if name == '*' else [name]) for name in out_cols_names]
        out_cols_names = [name for sublist in out_cols_names for name in sublist] #flatten
        return out_cols_names

    # Returns iterator over input (e.g. list if rows)
    # Each row is list with one value per column
    # e.g.
    #   [[1] ,[2], [3]]:                3 rows with a single col
    #   [[1,'a'], [2,'b'], [3,'c']]:    3 rows with 2 cols
    def get_input_iterator(self):
        return [[None]] #default: returns a single line with a 'null' column

    # Default column names, e.g. col1 for the first column
    def default_col_name(self, idx):
        return f"col{idx+1}"

    # replace identifiers (column names) in sql expressions by references to `_values`
    # and put (quoted) strings back
    def prepare_expression(self, expr):
        if expr == '*':
            return [f"_values[{idx}]" for idx in range(self.n_input_cols)]

        for id, replacement in self.translations.items():
            pattern = rf"\b({id})\b"
            expr = re.compile(pattern).sub(replacement, expr)

        return [self.strings.put_strings_back(expr)]

    def is_clause_single(self, clause):
        return clause not in ['select']

    def compile_clause(self, clause, clause_modifier = None, mode = 'eval'):
        """
        Compiles a clause of the select statment.
        """
        prs_clause = self.prs[clause]
        if not prs_clause:
            return None # empty clause

        if clause_modifier:
            prs_clause = clause_modifier.format(prs_clause)

        single = self.is_clause_single(clause)
        clause_exprs = None
        if single: # a clause with a single expression like WHERE
            clause_exprs = self.prepare_expression(prs_clause)
            if len(clause_exprs) > 1:
                user_error(
                    f"could not compile {clause.upper()} clause",
                    SyntaxError(f"{clause.upper()} clause should not have more than 1 expression")
                )
            clause_exprs = clause_exprs[0]
        else: #a clause with multiple expressions like SELECT
            clause_exprs = [self.prepare_expression(c['expr']) for c in prs_clause]
            clause_exprs = [item for sublist in clause_exprs for item in sublist] #flatten (because of '*')
            clause_exprs = '[' + ','.join(clause_exprs) + ']'

        try:
            return compile(clause_exprs, f'<{clause}>', mode)
        except Exception as main_exception:
            if not single:
                # breaks down clause into expressions and tries
                # compiling one by one to detect in which expression
                # the error happened
                for c in range(len(prs_clause)):
                    try:
                        expr = prs_clause[c]['expr']
                        translation = self.prepare_expression(expr)
                        for trans in translation:
                            if not trans.strip():
                                raise SyntaxError("empty expression")
                            compile(trans, f'<{clause}>', mode)
                    except Exception as expr_exception:
                        user_error(
                            f"could not compile {clause.upper()} expression #{c+1}",
                            expr_exception,
                            self.strings.put_strings_back(expr))

            user_error(f"could not compile {clause.upper()} clause", main_exception)

    def eval_clause(self, clause, clause_exprs, vars, mode = 'eval'):
        """
        Evaluates/executes a previously compiled clause.
        """
        #TODO check performance
        cmd = eval if mode == 'eval' else exec
        try:
            return cmd(clause_exprs,{},vars)
        except Exception as main_exception:
            prs_clause = self.prs[clause]
            if not self.is_clause_single(clause):
                # breaks down clause into expressions and tries
                # evaluating/executing one by one to detect
                # in which expression the error happened
                for c in range(len(prs_clause)):
                    try:
                        expr = prs_clause[c]['expr']
                        translation = self.prepare_expression(expr)
                        for trans in translation:
                            cmd(trans,{},vars)
                    except Exception as expr_exception:
                        user_error(
                            f"could not evaluate {clause.upper()} expression #{c+1}",
                            expr_exception,
                            self.strings.put_strings_back(expr),
                            vars)

            user_error(
                f"could not evaluate {clause.upper()} clause",
                main_exception,
                vars = vars)

    # main
    def go(self):
        output_handler = OutputHandler.make_handler(self.prs)
        writer = Writer.make_writer(self.prs['to'], sys.stdout, {}) #todo: add options, file output
        output_handler.set_writer(writer)
        self._go(output_handler)
        output_handler.finish()

    def _go(self, output_handler):
        select_expr = []
        where_expr = None
        explode_expr = None
        explode_cmd = None
        explode_its = [None] # 1 element by default (no explosion)
        _values = []
        row_number = 0
        input_row_number = 0

        vars = globals() # to do: filter out not useful/internal vars

        # gets user-defined output cols names (with AS alias)
        out_cols_names = [c['name'] for c in self.prs['select']]

        # should not accept more than 1 source, joins, etc (at least for now)
        for _values in self.get_input_iterator():
            input_row_number = input_row_number + 1

            vars["_values"] = _values
            vars["input_row_number"] = input_row_number

            if not self.reading_data():
                self.handle_header_row(_values)
                continue

            # print header
            if not select_expr: #1st input data row
                self.handle_1st_data_row(_values)
                output_handler.writer.writeheader(self.make_out_cols_names(out_cols_names))
                if output_handler.is_done():
                    return # in case of `limit 0`

                select_expr = self.compile_clause('select')
                where_expr = self.compile_clause('where')
                explode_expr = self.compile_clause('explode')

            if explode_expr:
                explode_its = self.eval_clause('explode', explode_expr, vars)

            for explode_it in explode_its:
                if explode_expr:
                    vars['explode_it'] = explode_it
                    if not explode_cmd:
                        explode_cmd = self.compile_clause('explode', '{} = explode_it', mode = 'exec')
                    self.eval_clause('explode', explode_cmd, vars, mode = 'exec')
                    vars["_values"] = _values

                #filter (opt: eventually could be done before exploding)
                if not where_expr or self.eval_clause('where', where_expr, vars):
                    # input line is eligeble
                    row_number = row_number + 1
                    vars["row_number"] = row_number

                    # calculate outputs
                    _res = self.eval_clause('select', select_expr, vars)

                    output_handler.handle_result(_res) #deal with output
                    if output_handler.is_done():
                        return #e.g. when reached limit

        user_info("#rows  in", input_row_number - (1 if self.has_header else 0))
        user_info("#rows out", row_number)


class PythonExprProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)

    # input is a Python expression
    def get_input_iterator(self):
        e = eval(self.strings.put_strings_back(self.prs['from']))
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
        #to do: suport files
        return ([line.rstrip("\n\r")] for line in sys.stdin)


class JSONProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)
        self.translations.update({"json": "_values[0]"}) # first column alias as json

    # 1 row = 1 json
    def get_input_iterator(self):
        #to do: suport files
        #return [[jsonlib.loads(line)] for line in sys.stdin]

        #this might not be the most efficient way of converting None -> NULL
        #look at: https://stackoverflow.com/questions/27695901/python-jsondecoder-custom-translation-of-null-type
        return ([
            jsonlib.loads(
                line,
                object_hook=lambda x: NullSafeDict(x)
            )] for line in sys.stdin)


## CSV
class CSVProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)

    def get_input_iterator(self):
        # Part 1 reads sample to detect dialect and if has header
        # TODO: infer data type
        sample_size = 10 #make a input parameter
        #saves sample
        sample = io.StringIO()
        for line in list(islice(sys.stdin, sample_size)): # TODO: support files
            sample.write(line)
        sample_val = sample.getvalue()
        if not sample_val:
            return []
        dialect = csv.Sniffer().sniff(sample_val)
        self.has_header = csv.Sniffer().has_header(sample_val)
        #print(self.has_header)
        #print(dialect)
        sample.seek(0) #rewinds the sample
        return chain(
            csv.reader(sample, dialect), #goes through sample again (for reading input data)
            csv.reader(sys.stdin, dialect)) #continues to the rest of the file
            #TODO: suport files

    def reading_data(self):
        return (not self.has_header) or (self.input_col_names)

    def handle_header_row(self, row):
        self.input_col_names = [self.make_str_valid_varname(val) for val in row]

    def make_str_valid_varname(self, s):
        # remove invalid characters (except spaces in-between)
        s = re.sub(r'[^0-9a-zA-Z_\s]', '', s).strip()

        # remove leading characters that are not letters or underscore
        #s = re.sub(r'^[^a-zA-Z_]+', '', s)

        # if first char is not letter or underscore then add underscore to make it valid
        if not re.match("^[a-zA-Z_]", s):
            s = "_" + s

        # replace spaces by underscores (instead of dropping spaces) for readability
        s = re.sub(r'\s+', '_', s)

        return s
