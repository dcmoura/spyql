from .cli import clean_query, parse
from .processor import Processor, InteractiveProcessor
from .quotes_handler import QuotesHandler


class Q:
  def __init__(self, query: str) -> None:
    """
    Make spyql interactive.

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
    self.prs, self.strings = parse(clean_query(query))
    self.prs["to"] = "PYTHON" # force to yield/return
    self.processor =  InteractiveProcessor(self.prs, self.strings)

  def __repr__(self) -> str:
    return f"Q(\"{self.query}\")"

  def __call__(self, **kwargs):
    out = self.processor.go("./file", None, kwargs)
    return out

def q(query, **kwargs):
  """Convenience for functional style.
  
  Usage
  -----

  .. code-block:: python

    >>> q = Q("IMPORT numpy SELECT numpy.mean(data->salary) FROM data WHERE data->age > 30", data = data)
  """
  return Q(query)(**kwargs)
