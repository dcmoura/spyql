import os

from .cli import clean_query, parse
from .processor import Processor
from .log import *

class Q:
  def __init__(self, query: str) -> None:
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

    # from logic has to be improved to include the files, python datastructures
    _from = self.parsed["from"]
    input_options = {}
    interactive = False
    if _from == None:
      # SELECT 1
      pass
    elif os.path.exists(_from):
      # SELECT * FROM /tmp/spyql.jsonl

      ext = _from.split(".")[-1].lower()
      processor_from = {"jsonl": "JSON", "csv": "CSV"}.get(ext, None)
      if processor_from == None:
        user_warning(f"Unsupported file extension: '{ext}', loading as 'TEXT'")
        processor_from = "TEXT"

      self.parsed["from"] = processor_from
      interactive = True
      input_options = {"filepath": _from}
    else:
      # SELECT * FROM data
      self.parsed["interactive"] = True
      interactive = True
      input_options = {"source": _from}
    
    self.parsed["to"] = "PYTHON" # force return to python

    self.processor = Processor.make_processor(
      prs = self.parsed,
      strings = self.strings,
      interactive = interactive,
      input_options = input_options
    )

  def __repr__(self) -> str:
    return f"Q(\"{self.query}\")"

  def __call__(self, **kwargs):
    # kwargs can take in multiple data sources as input in the future
    out = self.processor.go(None, None, kwargs)
    return out
