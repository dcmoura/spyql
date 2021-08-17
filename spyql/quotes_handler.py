import re
import random
import string

STRING_PLACEHOLDER_LEN = 32


class QuotesHandler:
    def __init__(self):
        self.strings = {}

    # replaces quoted strings by placeholders to make parsing easier
    # populates dictionary of placeholders and the strings they hold
    def extract_strings(self, query):
        res = []
        quotes = [
            r"\'(\'\'|\\.|[^'])*\'",
            r'\"(\"\"|\\.|[^"])*\"'  # ,
            #    r'\`(\`\`|\\.|[^`])*\`'
        ]

        spans = [(0, 0)]
        for quote in quotes:
            spans.extend([m.span() for m in re.finditer(quote, query)])
        spans.append((len(query), 0))

        # print(spans)

        self.strings = {}
        for i in range(len(spans) - 1):
            if i > 0:
                sid = "".join(
                    random.choice(string.ascii_letters)
                    for _ in range(STRING_PLACEHOLDER_LEN)
                )
                sid = f"_{sid}_"
                res.append(sid)
                self.strings[sid] = query[spans[i][0] + 1 : spans[i][1] - 1]

            res.append(query[spans[i][1] : spans[i + 1][0]])

        return "".join(res)

    @staticmethod
    def string_placeholder_re():
        return r"\_\w{%d}\_" % (STRING_PLACEHOLDER_LEN)

    # replace string placeholders by their actual strings
    def put_strings_back(self, text, quote=True):
        quote_char = '"' if quote else ""
        sids = {m.group(0) for m in re.finditer(self.string_placeholder_re(), text)}
        for sid in sids:
            text = text.replace(sid, f"{quote_char}{self.strings[sid]}{quote_char}")
        return text
