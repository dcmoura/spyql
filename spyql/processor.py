import csv
import json as jsonlib
import sys
import io
import re
from math import * 
import logging
from collections.abc import Iterable
from itertools import islice, chain

from spyql.writer import Writer
from spyql.output_handler import OutputHandler

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
        self.strings = strings
        # by default, a row does not need to be wrapped (only single cols need)
        self.input_col_names = []
        self.colnames2idx = {}
        

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
        self.colnames2idx.update({self.default_col_name(_i): _i for _i in range(self.n_input_cols)})
        if self.input_col_names:
            #TODO check if len(input_col_names) == self.n_input_cols 
            self.colnames2idx.update({self.input_col_names[_i]: _i for _i in range(self.n_input_cols)})
     

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

        for id, idx in self.colnames2idx.items():
            pattern = rf"\b({id})\b"
            replacement = f"_values[{idx}]"
            expr = re.compile(pattern).sub(replacement, expr)

        return [self.strings.put_strings_back(expr)]


    # main
    def go(self):        
        output_handler = OutputHandler.make_handler(self.prs)
        writer = Writer.make_writer(self.prs['to'], sys.stdout, {}) #todo: add options, file output
        output_handler.set_writer(writer)
        self._go(output_handler)
        output_handler.finish()

    def _go(self, output_handler):
        vars = globals() # to do: filter out not useful/internal vars

        _values = []
        row_number = 0
        vars_script = None
        #json = {}

        # gets user-defined output cols names (with AS alias)
        out_cols_names = [c[0] for c in self.prs['select']]
        
        cmds = []
        
        explode_it_cmd = None
        explode_inst_cmd = None
        explode_path = self.prs['explode']    
        if (explode_path):
            explode_it_cmd = compile(explode_path, '', 'eval')
            explode_inst_cmd = compile(f'{explode_path} = explode_it', '', 'exec')            
        
        where = None
        explode_its = [None] # 1 element by default (no explosion)

        logging.info("-- RESULT --")        

        # should not accept more than 1 source, joins, etc (at least for now)    
        for _values in self.get_input_iterator():
            
            if not self.reading_data():
                self.handle_header_row(_values)
                continue
                                    
            # print header
            if row_number == 0:
                self.handle_1st_data_row(_values)
                output_handler.writer.writeheader(self.make_out_cols_names(out_cols_names))
                if output_handler.is_done():
                    return # in case of `limit 0`
            
                # TODO: move to function(s)
                # compiles expressions for calculating outputs
                cmds = [self.prepare_expression(c[1]) for c in self.prs['select']]  #todo: rename cmds to out_expressions        
                cmds = [item for sublist in cmds for item in sublist] #flatten (because of '*')                    
                cmds = compile('[' + ','.join(cmds) + ']', '<select>', 'eval')                    
                where = self.prs['where']                
                if (where):
                    #TODO: check if * is not used in where... or pass argument
                    where = compile(self.prepare_expression(where)[0], '<where>', 'eval') 
            
            if explode_path:
                explode_its = eval(explode_it_cmd)
                            
            for explode_it in explode_its:  
                if explode_path:
                    exec(explode_inst_cmd)

                row_number = row_number + 1

                vars["_values"] = _values

                if not where or eval(where,{},vars): #filter (opt: eventually could be done before exploding)
                    # input line is eligeble 
                    
                    # calculate outputs
                    _res = eval(cmds,{},vars)

                    output_handler.handle_result(_res) #deal with output
                    if output_handler.is_done():
                        return #e.g. when reached limit


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
        return [[line.rstrip("\n\r")] for line in sys.stdin]

    
class JSONProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)
        self.colnames2idx.update({"json": 0}) # first column alias as json

    # 1 row = 1 json
    def get_input_iterator(self):
        #to do: suport files
        return [[jsonlib.loads(line)] for line in sys.stdin]

## CSV
class CSVProcessor(Processor):
    def __init__(self, prs, strings):
        super().__init__(prs, strings)
        self.has_header = False

    def get_input_iterator(self):
        # Part 1 reads sample to detect dialect and if has header
        # TODO: infer data type
        sample_size = 10 #make a input parameter
        #saves sample
        sample = io.StringIO()
        for line in list(islice(sys.stdin, sample_size)): # TODO: support files
            sample.write(line)        
        sample_val = sample.getvalue()
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

