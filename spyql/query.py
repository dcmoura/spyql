import logging
from spyql.parser import parse
from spyql.processor import Processor
import spyql.log


class Query:
    '''
    A SPyQL query than can be executed on top of a file or variables producing a file or a :class:`~spyql.query_result.QueryResult`.
    Example::

        query = Query("""
            SELECT row.name as first_name, row.age
            FROM data
            WHERE row.age > 30
        """)

        result = query(data=[
            {"name": "Alice", "age": 20, "salary": 30.0},
            {"name": "Bob", "age": 30, "salary": 12.0},
            {"name": "Charles", "age": 40, "salary": 6.0},
            {"name": "Daniel", "age": 43, "salary": 0.40},
        ])

        ## result:
        # (
        #    {"first_name": "Charles", "age": 40},
        #    {"first_name": "Daniel", "age": 43},
        # )
    '''

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
        Creates a ``Query`` object (does not execute the query).

        :param query: a spyql query
        :type query: str
        :param input_options: options to be passed to the input processor.
            e.g. ``{"delimiter": ","}``
        :type input_options: dict, optional
        :param output_options: options to be passed to the output writer.
            e.g. ``{"delimiter": ";", "header": False}``
        :type output_options: dict, optional
        :param unbuffered: forces output to be unbuffered.
        :type unbuffered: bool, optional
        :param warning_flag: set to "error" to turn warnings into errors
            (halting execution)
        :type warning_flag: str, optional
        :param verbose: set the verbosity level:
            -2 to supress errors and warnings;
            -1 to supress warnings;
            0 to only show errors and warnings (default);
            1 to show additional info messages;
            2 to show additional debug messages.
        :type verbose: int, optional
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
        """
        Executes the query.
        ``kwargs`` can take in multiple variables that are included in the
        scope of the query.
        """
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
        """
        Returns a dictionary with statistics about the query execution,
        namely the number of rows in the input and output.

        :rtype: dict
        """
        return self.__stats
