import os
import sys
import io
import logging
from .parser import parse
from .processor import Processor
from .writer import Writer
import spyql.log


class Query:
    def __init__(
        self,
        query: str,
        input_options: dict = {},
        output_options: dict = {},
        unbuffered=False,
        warning_flag="default",
        verbose=0,
        default_to_clause="MEMORY",
    ) -> None:
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
            [ TO csv | json | spy | sql | pretty | plot | memory ]

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

        logging.basicConfig(level=(3 - verbose) * 10, format="%(message)s")
        spyql.log.error_on_warning = warning_flag == "error"

        self.query = query
        self.parsed, self.strings = parse(query, default_to_clause)
        self.output_options = output_options
        self.input_options = input_options
        self.unbuffered = unbuffered
        self.__stats = None

        spyql.log.user_debug_dict("Parsed query", self.parsed)
        spyql.log.user_debug_dict("Strings", self.strings.strings)

    def __repr__(self) -> str:
        return f'Query("{self.query}")'

    def __call__(self, **kwargs):
        # kwargs can take in multiple data sources as input in the future
        processor = None
        result = None
        self.__stats = None
        try:
            # make the processor
            processor = Processor.make_processor(
                self.parsed,
                self.strings,
                self.input_options,
            )
            result, self.__stats = processor.go(
                output_options=self.output_options,
                user_query_vars=kwargs,
            )
        finally:
            # makes sure files are closed
            if processor:
                processor.close()
                if processor.writer:
                    processor.writer.close()
        return result

    def stats(self):
        return self.__stats
