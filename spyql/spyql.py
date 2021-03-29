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
import sys
import re

import logging
import random
import string


query_struct_keywords = ['select', 'from', 'explode', 'where', 'limit', 'offset', 'to']

#makes sure that queries start with a space (required for parse_structure)
def clean_query(q):
    q = " " + q.strip()
    return q

# replaces quoted strings by placeholders to make parsing easier
# also returns dictionary of placeholders and the strings they hold
def get_query_strings(query):    
    res = []
    quotes = [
        r"\'(\'\'|\\.|[^'])*\'",
        r'\"(\"\"|\\.|[^"])*\"'#,
    #    r'\`(\`\`|\\.|[^`])*\`'
    ]
    
    spans = [(0,0)]
    for quote in quotes:
        spans.extend([m.span() for m in re.finditer(quote, query)])
    spans.append((len(query), 0))
    
    #print(spans)
    
    strings = {}
    for i in range(len(spans)-1):
        if i>0:            
            sid = ''.join(random.choice(string.ascii_letters) for _ in range(32) )
            sid = f"_{sid}_"
            res.append(sid)
            strings[sid] = query[spans[i][0]+1:spans[i][1]-1] 
            
        res.append(query[spans[i][1]:spans[i+1][0]])
        
    return ("".join(res), strings)
          
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
        d[keys[i]] = q[st:nd]

    return d

# replaces sql syntax by python syntax
def pythonize(s):
    #todo: check for special SQL stuff such as in, is, like    
    #s = re.compile(r"([^=<>])={1}([^=])").sub(r"\1==\2", s)
    #DECISION: expressions are PURE python code :-)
    #eventual exceptions: "IS NULL" by "== None" and "IS NOT NULL ..."
    return s

# replace string placeholders by their actual strings
def put_strings_back(text, strings, quote=True):
    quote_char = '"' if quote else ''
    sids = {m.group(0) for m in re.finditer(r'\_\w{32}\_', text)}    
    for sid in sids:
        text = text.replace(sid, f'{quote_char}{strings[sid]}{quote_char}')
    return text    
        
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
            c = "_values"
            name = '*'
        else:            
            name = put_strings_back(name, strings, quote=False)
            c = f"[{make_expr_ready(c, strings)}]" 
        
        #new_sel[name] = c
        new_sel.append((name,c))
    
    return new_sel

def make_expr_ready(expr, strings):
    return put_strings_back(pythonize(expr), strings).strip()

# parse entry point
def parse(query):
    (query, strings) = get_query_strings(query)
    #print(query)
    #print(strings)
    prs = parse_structure(query)
    
    if not prs['select']:        
        raise SyntaxError('SELECT keyword is missing')

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
        
    logging.info(prs)

    processor = Processor.make_processor(prs)

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
    #sys.tracebacklimit = 0 # no exception traces
    #logging.basicConfig(level=logging.INFO)
    #logging.basicConfig(level=logging.DEBUG)

    #default query for simple testing:
    #query = 'select *, \'single quote\', pow(2, col1) as p, 1+2+3 = 3 * 2 as a, 10%2=0,  not 20 > 30 as b, 0 = 10%2, "a is from b",  1600365679, "this is where it goes", datetime.fromtimestamp(1600365679) FROM [x*2-1 for x in range(5)]'
    query = 'select *, \'single , ; quote\' AS olá mundo, pow(2, col1) as p, 1+2+3 == 3 * 2 as a, 10%2==0,  not 20 > 30 as b, 0 == 10%2, "a is from b",  1600365679, "this is where ", date.fromtimestamp(1600365679) FROM [x*2-1 for x in range(5)] LIMIT 2 TO pretty '
    #query = 'select *, \'single , ; quote\' AS olá mundo, 1+2+3 == 3 * 2 as a, 10%2==0,  not 20 > 30 as b, 0 == 10%2, "a is from b",  1600365679, "this is where ", date.fromtimestamp(1600365679) TO pretty'
    if len(sys.argv) > 1:
        query = sys.argv[1]        
    logging.info(query)
    
    run(query)
    #TODO: catch exception and 
    #   print_select_syntax()


if __name__ == "__main__":    
    main()
