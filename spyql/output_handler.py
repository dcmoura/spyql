from spyql.nulltype import Null


class OutputHandler:
    """Mediates data processing with data writting"""

    @staticmethod
    def make_handler(prs):
        """
        Chooses the right handler depending on the kind of query
        and eventual optimization opportunities
        """
        if prs["group by"] and not prs["partials"]:
            return GroupByDelayedOutSortAtEnd(
                prs["order by"], prs["limit"], prs["offset"]
            )
        if prs["order by"]:
            # TODO optimization: use special handler that only keeps the top n elements
            #   in memory when LIMIT is defined
            if prs["distinct"]:
                return DistinctDelayedOutSortAtEnd(
                    prs["order by"], prs["limit"], prs["offset"]
                )
            return DelayedOutSortAtEnd(prs["order by"], prs["limit"], prs["offset"])
        if prs["distinct"]:
            return LineInDistinctLineOut(prs["limit"], prs["offset"])
        return LineInLineOut(prs["limit"], prs["offset"])

    def __init__(self, limit, offset):
        self.limit = limit
        self.rows_written = 0
        self.offset = offset if offset else 0

    def set_writer(self, writer):
        self.writer = writer

    def handle_result(self, result, group_key, sort_keys):
        """
        To be implemented by child classes to handle a new output row (aka result).
        All inputs should be tuples.
        """
        return self.is_done()

    def is_done(self):
        # premature ending
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
    """Simple handler that immediately writes every processed row"""

    def handle_result(self, result, *_):
        self.write(result)
        return self.is_done()

    def finish(self):
        super().finish()


class LineInDistinctLineOut(OutputHandler):
    """In-memory distinct handler that immediately writes every non-duplicated row"""

    def __init__(self, limit, offset):
        super().__init__(limit, offset)
        self.output_rows = set()

    def handle_result(self, result, *_):
        # uses a dict to store distinct results instead of storing all rows
        if result in self.output_rows:
            return False  # duplicate

        self.output_rows.add(result)
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

    def handle_result(self, result, sort_keys, *_):
        self.output_rows.append({"data": result, "sort_keys": sort_keys})
        # TODO use temporary files to write `output_rows` whenever it gets too large
        # TODO sort intermediate results before writing to a temporary file
        return False  # no premature endings here

    def finish(self):
        # TODO read and merge previously sorted temporary files (look into heapq.merge)
        # 1. sorts everything
        if self.orderby:
            for i in reversed(range(len(self.orderby))):
                # taking advantage of list.sort being stable to sort elements from minor
                # to major criteria (not be the most efficient way but straightforward)
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


class GroupByDelayedOutSortAtEnd(DelayedOutSortAtEnd):
    """
    Extends `DelayedOutSortAtEnd` to only store intermediate group by results instead of
    keeping all rows in memory
    """

    def __init__(self, orderby, limit, offset):
        super().__init__(orderby, limit, offset)
        self.output_rows = dict()

    def handle_result(self, result, sort_keys, group_key):
        # uses a dict to store intermidiate group by results instead of storing all rows
        self.output_rows[group_key] = {"data": result, "sort_keys": sort_keys}
        return False  # no premature endings here

    def finish(self):
        #  converts output_rows dict to list so that it can be sorted and written
        self.output_rows = list(self.output_rows.values())
        super().finish()


class DistinctDelayedOutSortAtEnd(DelayedOutSortAtEnd):
    """
    Alters `DelayedOutSortAtEnd` to only store distinct results instead of
    keeping all rows in memory
    """

    def __init__(self, orderby, limit, offset):
        super().__init__(orderby, limit, offset)
        self.output_rows = dict()

    def handle_result(self, result, sort_keys, *_):
        # uses a dict to store distinct results instead of storing all rows
        if result not in self.output_rows:
            self.output_rows[result] = sort_keys
        return False  # no premature endings here

    def finish(self):
        # converts output_rows dict to list so that it can be sorted and written
        self.output_rows = [
            {"data": k, "sort_keys": v} for k, v in self.output_rows.items()
        ]
        super().finish()
