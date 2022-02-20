import os

from .cli import clean_query, parse, file_ext2type
from .processor import Processor
from .writer import Writer
from .log import *

class Q:
  def __init__(self, query: str, input_options: dict = {}, output_options: dict = {}) -> None:
    """
    Make spyql interactive.

    [ IMPORT python_module [ AS identifier ] [, ...] ]
    SELECT [ DISTINCT | PARTIALS ]
        [ * | python_expression [ AS output_column_name ] [, ...] ]
        [ FROM csv | spy | text | python_expression | json [ EXPLODE path ] ]
        [ WHERE python_expression ]
        [ GROUP BY output_column_number | python_expression  [, ...] ]
        [ ORDER BY output_column_number | python_expression
            [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] ]
        [ LIMIT row_count ]
        [ OFFSET num_rows_to_skip ]
        [ TO csv | json | spy | sql | pretty | plot ]

    Usage
    -----

    .. code-block:: python

      >>> q = Q("IMPORT numpy SELECT numpy.mean(data->salary) FROM data WHERE data->name == 'akash'")
      >>> q(data = data)

    Args
    ----

      query(str): SpyQL string
      input_opt/output_opt: kwargs for the input and writers, in this case of interactive mode we can
        ignore these
    """
    self.query = query
    self.parsed, self.strings = parse(clean_query(query))
    self.output_path = None
    self.output_options = output_options

    # FROM logic:
    #   if nothing then it might be just a SELECT method
    #   if such a path exists then load the correct writer
    #   else assume it is a python object to be loaded by user
    _from = self.parsed["from"]
    if _from == None:
      # SELECT 1
      pass
    elif os.path.exists(_from):
      # SELECT * FROM /tmp/spyql.jsonl
      processor = Processor._ext2filetype.get(_from.split(".")[-1].lower(), None)
      if processor == None:
        raise SyntaxError(f"Invalid FROM statement: '{_to}'")

      self.parsed["from"] = processor
      input_options = {"filepath": _from}
    else:
      # SELECT * FROM data
      input_options = {"source": _from}

    # TO logic:
    #   if nothing is determined meaning return
    #   if is a string
    #     if is a filepath -> write to file
    _to = self.parsed["to"]
    if _to == None:
      self.parsed["to"] = "PYTHON" # force return to python
    elif isinstance(_to, str):
      # TO /tmp/spyql.jsonl
      if _to.upper() in Writer._valid_writers:
        raise SyntaxError(f"Cannot export to a writer format in interactive mode: '{_to}'")
      writer = Writer._ext2filetype.get(_to.split(".")[-1].lower(), None)
      if writer == None:
        raise SyntaxError(f"Invalid TO statement: '{_to}'")

      self.parsed["to"] = writer
      self.output_path = _to
    else:
      raise SyntaxError(f"Unsupported output type: '{_to}'")

    # make the processor
    self.processor = Processor.make_processor(
      prs = self.parsed,
      strings = self.strings,
      input_options = input_options
    )

  def __repr__(self) -> str:
    return f"Q(\"{self.query}\")"

  def __call__(self, **kwargs):
    # kwargs can take in multiple data sources as input in the future
    f = open(self.output_path, "w") if self.output_path != None else None
    out = self.processor.go(
      output_file = f,
      output_options = self.output_options,
      user_query_vars = kwargs
    )
    return out.get("output")
