import spyql.log
from spyql.nulltype import Null


class OutputHandler:
    @staticmethod
    def make_handler(prs):
        if prs["order by"]:
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
    def handle_result(self, result, sort_keys=None):
        self.write(result)
        return self.is_done()

    def finish(self):
        super().finish()


class DelayedOutSortAtEnd(OutputHandler):
    def __init__(self, orderby, limit, offset):
        super().__init__(limit, offset)
        self.orderby = orderby
        self.output_rows = []
        spyql.log.user_info(
            "Current implementation of ORDER BY loads all output records into memory"
        )

    def handle_result(self, result, sort_keys=None):
        self.output_rows.append({"data": result, "sort_keys": sort_keys})
        return False  # no premature endings here

    def finish(self):
        for i in reversed(range(len(self.orderby))):
            self.output_rows.sort(
                key=lambda row: (
                    (row["sort_keys"][i] is Null) != self.orderby[i]["rev_nulls"],
                    row["sort_keys"][i],
                ),
                reverse=self.orderby[i]["rev"],
            )
        for row in self.output_rows:
            # it would be more efficient to slice the output_rows list based on limit and offset
            # however, this is more generic with less repeated logic and it is a temporary implementation
            if self.is_done():
                break
            self.write(row['data'])
        super().finish()
