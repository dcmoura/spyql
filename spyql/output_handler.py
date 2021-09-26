import spyql.log
from spyql.nulltype import Null


class OutputHandler:
    """Mediates data processing with data writting"""

    @staticmethod
    def make_handler(prs):
        if prs["order by"]:
            # TODO otimisation: use special handler that only keeps the top n elements
            #   in memory when LIMIT is defined
            # TODO group by handler
            return DelayedOutSortAtEnd(prs["order by"], prs["limit"], prs["offset"])
        return LineInLineOut(prs["limit"], prs["offset"])

    def __init__(self, limit, offset):
        self.limit = limit
        self.rows_written = 0
        self.offset = offset if offset else 0

    def set_writer(self, writer):
        self.writer = writer

    def handle_result(self, result, sort_keys=None):
        # to be implemented by child classes
        return self.is_done()

    def is_done(self):
        return self.limit is not None and self.rows_written >= self.limit

    def write(self, row):
        if self.offset > 0:
            self.offset = self.offset - 1
        else:
            self.writer.writerow(row)
            self.rows_written = self.rows_written + 1

    def finish(self):
        self.writer.flush()


class LineInLineOut(OutputHandler):
    """Simple handler that immediatly writes every processed row"""

    def handle_result(self, result, sort_keys=None):
        self.write(result)
        return self.is_done()

    def finish(self):
        super().finish()


class DelayedOutSortAtEnd(OutputHandler):
    """
    Only writes after collecting and sorting all data.
    Temporary implementation that reads every processed row into memory.
    """

    def __init__(self, orderby, limit, offset):
        super().__init__(limit, offset)
        self.orderby = orderby
        self.output_rows = []
        spyql.log.user_info(
            "Current implementation of ORDER BY loads all output records into memory"
        )

    def handle_result(self, result, sort_keys=None):
        self.output_rows.append({"data": result, "sort_keys": sort_keys})
        # TODO use temporary files to write `output_rows` whenever it gets too large
        # TODO sort intermediate results before writing to a temporary file
        return False  # no premature endings here

    def finish(self):
        # TODO read and merge previously sorted temporary files (look into heapq.merge)
        # 1. sorts everything
        for i in reversed(range(len(self.orderby))):
            # taking advange of list.sort() being stable to sort elememts from minor to
            # major criteria (not be the most efficient way but it is straightforward)
            self.output_rows.sort(
                key=lambda row: (
                    # handle of NULLs based on NULLS FIRST/LAST specification
                    (row["sort_keys"][i] is Null) != self.orderby[i]["rev_nulls"],
                    row["sort_keys"][i],
                ),
                reverse=self.orderby[i]["rev"],  # handles ASC/DESC order
            )
        # 2. writes sorted rows to output
        for row in self.output_rows:
            # it would be more efficient to slice `output_rows` based on limit/offset
            # however, this is more generic with less repeated logic and this is a
            # temporary implementation
            if self.is_done():
                break
            self.write(row["data"])
        super().finish()
