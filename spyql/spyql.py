# setup:
# > export PYTHONPATH=/a/dir/spyql/
# > pip3 install [all the libs :-P]
#
# running:
# > python3 -m spyql.spyql [query]
#
# e.g.:
# > python3 -m spyql.spyql "SELECT 'hello world' TO json"
#
# running tests + coverage:
# > pytest --cov=spyql tests
#

from spyql.processor import Processor
from spyql.quotes_handler import QuotesHandler
import spyql.log
import logging
import sys
import re




query_struct_keywords = ['select', 'from', 'explode', 'where', 'limit', 'offset', 'to']

#makes sure that queries start with a space (required for parse_structure)
def clean_query(q):
    q = " " + q.strip()
    return q


#parse the supported keywords, which must follow a given order
def parse_structure(q):
    keys = query_struct_keywords
    last_pos = 0
    key_matches = []
    for key in keys:
        entry = re.compile(fr"\s+{key}\s+", re.IGNORECASE).search(q, last_pos)
        if entry:
            entry = entry.span()
            last_pos = entry[1]
        key_matches.append(entry)
    #key_matches = [re.search(fr"\s+{key}\s+", q, re.IGNORECASE) for key in keys]
    #key_matches = [(m.span() if m else None)  for m in key_matches]

    d = {}
    for i in range(len(query_struct_keywords)):
        if not key_matches[i]:
            d[keys[i]] = None
            continue
        st = key_matches[i][1]
        nd = len(q)
        for j in range(i+1, len(keys)):
            if key_matches[j]:
                nd = key_matches[j][0]
                break

        # this list should be empty, otherwise order of clauses was not respected
        misplaced_keys = list(filter(None, [re.compile(fr"\s+{k}\s+", re.IGNORECASE).search(q[st:nd]) for k in keys]))
        if misplaced_keys:
            spyql.log.user_error(
                "could not parse query",
                SyntaxError(f"misplaced {misplaced_keys[0][0].strip()} clause")
            )
        d[keys[i]] = q[st:nd]

    return d

# replaces sql/custom syntax by python syntax
def pythonize(s):
    #todo: check for special SQL stuff such as in, is, like
    #s = re.compile(r"([^=<>])={1}([^=])").sub(r"\1==\2", s)
    #DECISION: expressions are PURE python code :-)
    #eventual exceptions: "IS NULL" by "== None" and "IS NOT NULL ..."

    #easy shortcut for navigating through dics (of dics)
    #e.g.   `json->hello->'planet hearth'` converts into
    #       `json['hello']['planet hearth']`

    # first replace quoted keys (they do not need quotes)
    s = re.compile(r"->(%s)"%(QuotesHandler.string_placeholder_re())).sub(r"[\1]", s)
    #then replace unquoted keys (they need quotes)
    s = re.compile(r"->([^\d\W]\w*)").sub(r"['\1']", s)

    return s

def custom_sel_split(s):
    sin = list(s)
    sep = [-1]
    rb = 0 # ()
    cb = 0 # {}
    sb = 0 # []
    for i in range(len(sin)):
        c = sin[i]
        if c == '(':
            rb = rb + 1
        elif c == ')':
            rb = rb - 1
        elif c == '{':
            cb = cb + 1
        elif c == '}':
            cb = cb - 1
        elif c == '[':
            sb = sb + 1
        elif c == ']':
            sb = sb - 1
        elif c == ',' and rb == 0 and cb == 0 and sb == 0:
            sep.append(i)
    sep.append(None)
    parts = [s[sep[i]+1:sep[i+1]].strip() for i in range(len(sep)-1)]

#    print()
#    print(parts)
#    print()

    return parts


# devides the select into columns and find their names
def parse_select(sel, strings):
    #TODO: support column alias without AS

    sel = [c.strip() for c in custom_sel_split(sel)]
    new_sel = []
    as_pattern = re.compile(r"\s+AS\s+", re.IGNORECASE)
    for i in range(len(sel)):
        c = sel[i]
        sas = re.search(as_pattern, c)
        name = f"out{i+1}"
        if sas:
            name = c[(sas.span()[1]):].strip()
            c = c[:(sas.span()[0])]

        if c.strip() == '*':
            c = "*"
            name = '*'
        else:
            name = strings.put_strings_back(name, quote=False)
            c = f"{make_expr_ready(c, strings)}"

        #new_sel[name] = c
        new_sel.append({"name": name, "expr": c})

    return new_sel

def make_expr_ready(expr, strings):
    return pythonize(expr).strip()

# parse entry point
def parse(query):
    strings = QuotesHandler()
    query = strings.extract_strings(query)

   # (query, strings) = get_query_strings(query)
    #print(query)
    #print(strings)
    prs = parse_structure(query)

    if not prs['select']:
        spyql.log.user_error(
            'could not parse query',
            SyntaxError('SELECT keyword is missing'))

    prs['select'] = parse_select(prs['select'], strings)

    # TODO: generalize
    if (prs['from']):
        prs['from'] = make_expr_ready(prs['from'], strings)

    if (prs['explode']):
        prs['explode'] = make_expr_ready(prs['explode'], strings)

    if (prs['where']):
        prs['where'] = make_expr_ready(prs['where'], strings)

    if (prs['limit']):
        val = prs['limit']
        if val.strip().upper() == "ALL":
            prs['limit'] = None
        else:
            val = int(val)
            prs['limit'] = val if val > 0 else 0

    if (prs['offset']):
        val = int(prs['offset'])
        prs['offset'] = val if val > 0 else 0

    if (prs['to']):
        prs['to'] = make_expr_ready(prs['to'], strings)

    #TO DO: check for special SQL stuff such as in, is, like

    return (prs, strings)



def re_search_first(*argv):
    return re.search(*argv).group(0)

###############
# run
###############
def run(query):
    query = clean_query(query)

    prs, strings = parse(query)

    spyql.log.user_debug_dict("Parsed query", prs)
    spyql.log.user_debug_dict("Strings", strings.strings)

    processor = Processor.make_processor(prs, strings)

    processor.go()





def print_select_syntax():
    print("""
  SELECT
    [ * | python_expression [ AS output_column_name ] [, ...] ]
    [ FROM csv | qy | text | arff | python_expression | json [ EXPLODE path ] ]
    [ FILE path ]  ??
    [ WHERE python_expression ]
    [ LIMIT row_count ]
    [ OFFSET num_rows_to_skip ]
    [ TO csv | json | text | arff | py | sql | pretty | plot ]
    """)


def main():
    log_format = "%(message)s"
    log_level = logging.DEBUG
    #log_level = logging.INFO
    #log_level = logging.WARN
    #log_level = logging.ERROR
    logging.basicConfig(level=log_level, format = log_format)
    spyql.log.error_on_warning = False

    #default query for simple testing:
    #query = 'select *, \'single quote\', pow(2, col1) as p, 1+2+3 = 3 * 2 as a, 10%2=0,  not 20 > 30 as b, 0 = 10%2, "a is from b",  1600365679, "this is where it goes", datetime.fromtimestamp(1600365679) FROM [x*2-1 for x in range(5)]'
    query = 'select *, \'single , ; quote\' AS olá mundo, pow(2, col1) as p, 1+2+3 == 3 * 2 as a, 10%2==0,  not 20 > 30 as b, 0 == 10%2, "a is from b",  1600365679, "this is where ", date.fromtimestamp(1600365679) FROM [x*2-1 for x in range(5)] LIMIT 2 TO pretty '
    #query = 'select *, \'single , ; quote\' AS olá mundo, 1+2+3 == 3 * 2 as a, 10%2==0,  not 20 > 30 as b, 0 == 10%2, "a is from b",  1600365679, "this is where ", date.fromtimestamp(1600365679) TO pretty'
    if len(sys.argv) > 1:
        query = sys.argv[1]

    run(query)
    #TODO: catch exception and
    #   print_select_syntax()


if __name__ == "__main__":
    main()

    ## For profiling:
    #
    # import cProfile
    # import pstats
    # from pstats import SortKey
    # cProfile.run('main()', 'spyql.stats')
    # p = pstats.Stats('spyql.stats').strip_dirs()

    # p.sort_stats(SortKey.CUMULATIVE).dump_stats('spyql.stats.cum')
    # p.sort_stats(SortKey.TIME).dump_stats('spyql.stats.time')
