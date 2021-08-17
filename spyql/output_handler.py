class OutputHandler:
    @staticmethod
    def make_handler(prs):
        return LineInLineOut(prs["limit"], prs["offset"])

    def __init__(self, limit, offset):
        self.limit = limit
        self.rows_written = 0
        self.offset = offset if offset else 0

    def set_writer(self, writer):
        self.writer = writer

    def handle_result(self, result):
        pass

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
    def handle_result(self, result):
        self.write(result)

    def finish(self):
        super().finish()
