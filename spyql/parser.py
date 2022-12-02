from spyql.quotes_handler import QuotesHandler
from spyql import agg, log, utils
from spyql.processor import Processor
from spyql.writer import Writer
import re
import inspect
from typing import Dict, List, Optional

query_struct_keywords = [
    "import",
    "select",
    "from",
    "explode",
    "where",
    "group by",
    "order by",
    "limit",
    "offset",
    "to",
]


def get_agg_funcs():
    # TODO replace this by register mechanism to allow for user-defined aggregation
    funcs = inspect.getmembers(agg, inspect.isfunction)
    return {f[0] for f in funcs}


agg_funcs = get_agg_funcs()


def extract_funcs(expr):
    return re.findall(r"(\w+)\s*\(", expr)


def has_agg_func(expr):
    return agg_funcs.intersection(extract_funcs(expr))


def throw_error_if_has_agg_func(expr, clause_name):
    a = has_agg_func(expr)
    if has_agg_func(expr):
        log.user_error(
            f"aggregate functions are not allowed in {clause_name} clause",
            SyntaxError("bad query"),
            ",".join(a),
        )


def has_reference2row(expr):
    return re.search(r"\brow\b", make_expr_ready(expr)) is not None


class KeywordOrderValidator:
    """
    Check if the keyword position is valid
    """

    def __init__(self, keywords):
        self.keywords = keywords
        self.present_keyword_idx = -1

    def run(self, keyword: Optional[str]):
        # eg. invalid if 'from' clause appear after 'where' clause
        if self.present_keyword_idx >= self.keywords.index(keyword):
            log.user_error(
                "could not parse query",
                SyntaxError(f"misplaced '{keyword}' clause"),
            )

        self.present_keyword_idx = self.keywords.index(keyword)


def parse_structure(query: str):
    """parse the supported keywords, which must follow a given order"""
    tokens: List[str] = []
    for line in query.splitlines():
        if "#" in line:
            # Remove comment from parse targets
            line = line.split("#", 1)[0]

        tokens += line.split()

    tokens = [t for t in tokens if t != ""]

    query_struct: Dict[Optional[str], Optional[str]] = {
        kw: None for kw in query_struct_keywords
    }

    # {"select": ["select"], ..., "group": ["group", "by"], ...}
    query_struct_keyword_dict: Dict[str, List[str]] = {
        mk[0]: mk for mk in [k.split() for k in query_struct_keywords]
    }

    validator = KeywordOrderValidator(query_struct_keywords)

    matchable_tokens: List[Optional[List[str]]] = [
        query_struct_keyword_dict.get(token.lower()) for token in tokens
    ]
    keyword_positions = [
        (i, matchable_token)
        for i, matchable_token in enumerate(matchable_tokens)
        if matchable_token
        and [t.lower() for t in tokens[i : i + len(matchable_token)]] == matchable_token
    ]

    # when no keywords are found, returns an empty struct
    # (will raise a 'SELECT keyword is missing' error)
    if not keyword_positions:
        return query_struct

    if keyword_positions[0][0] > 0:
        log.user_error(
            "could not parse query",
            SyntaxError(f"misplaced '{tokens[0]}' at the beginning of the query"),
        )

    for i, (pos, kw) in enumerate(keyword_positions):
        kw_str = " ".join(kw)
        validator.run(kw_str)
        pos_next = (
            keyword_positions[i + 1][0] if i < len(keyword_positions) - 1 else None
        )
        query_struct[kw_str] = " ".join(tokens[pos + len(kw) : pos_next])

    return query_struct


def pythonize(s):
    """replaces sql/custom syntax by python syntax"""
    # TODO check for special SQL stuff such as in, is, like
    # s = re.compile(r"([^=<>])={1}([^=])").sub(r"\1==\2", s)
    # DECISION: expressions are PURE python code :-)

    # make sure the `as` keyword is always lowcase
    # (currently only needed for imports)
    s = re.compile(r"\s+AS\s+", re.IGNORECASE).sub(" as ", s)

    # replace count_agg(*) and count_distinct_agg(*) by appropriate calls
    s = re.compile(r"\bcount\_agg\s*\(\s*\*\s*\)").sub("count_agg(1)", s)
    s = re.compile(r"\bcount\_distinct\_agg\s*\(\s*\*\s*\)").sub(
        "count_distinct_agg(tuple(_values))", s
    )

    # universal access syntax
    # `.column` is converted to `row.column`
    s = re.compile(r"(^|[^\)\]\}\.\w\"\'\`])\.(?=[A-Za-z_])").sub(r"\1row.", s)
    # `.` is converted to `row`
    s = re.compile(r"(^|[^\)\]\}\.\w\"\'\`])\.(?!\d)").sub(r"\1row", s)

    # easy shortcut for navigating through dics (of dics)
    # e.g.   `json->hello->'planet hearth'` converts into
    #       `json['hello']['planet hearth']`

    # first replace quoted keys (they do not need quotes)
    s = re.compile(r"->(%s)" % (QuotesHandler.string_placeholder_re())).sub(r"[\1]", s)
    # then replace unquoted keys (they need quotes)
    s = re.compile(r"->([^\d\W]\w*)").sub(r"['\1']", s)

    return s


def split_multi_expr_clause(s):
    """
    Transforms "abc, (123 + 1) * 2, f(a,b)"
    into ["abc", "(123 + 1) * 2", "f(a,b)"]
    """
    sin = list(s)
    sep = [-1]
    rb = 0  # ()
    cb = 0  # {}
    sb = 0  # []
    for i in range(len(sin)):
        c = sin[i]
        if c == "(":
            rb = rb + 1
        elif c == ")":
            rb = rb - 1
        elif c == "{":
            cb = cb + 1
        elif c == "}":
            cb = cb - 1
        elif c == "[":
            sb = sb + 1
        elif c == "]":
            sb = sb - 1
        elif c == "," and rb == 0 and cb == 0 and sb == 0:
            sep.append(i)
    sep.append(None)
    parts = [s[sep[i] + 1 : sep[i + 1]].strip() for i in range(len(sep) - 1)]

    return parts


def parse_select(sel, strings):
    """splits the SELECT clause into columns and find their names"""
    # TODO support column alias without AS
    modif_pattern = r"^\s*(?:(DISTINCT)\s+)?(?:(PARTIALS)\s+)?"
    modifs = re.search(modif_pattern, sel.upper())
    has_distinct = "DISTINCT" in modifs.groups()
    has_partials = "PARTIALS" in modifs.groups()
    sel = sel[modifs.span()[1] :]  # remove modifiers from expression

    res = []
    as_pattern = re.compile(r"\s+AS\s+", re.IGNORECASE)
    for expr in split_multi_expr_clause(sel):
        sas = re.search(as_pattern, expr)
        name = ""
        if sas:
            name = expr[(sas.span()[1]) :].strip()
            expr = expr[: (sas.span()[0])]
        else:
            # automatic output column name from expression
            # removes json/row 'variables' reference (visual garbage)
            name = re.compile(r"(\b)(?:json|row)(->|\[|\.)").sub(
                r"\1", make_expr_ready(expr)
            )
            # makes the string a valid python variable name
            name = utils.make_str_valid_varname(strings.put_strings_back(name))

        if expr.strip() == "*":
            expr = "*"
            name = "*"
        else:
            name = strings.put_strings_back(name, quote=False)
            expr = f"{make_expr_ready(expr)}"

        res.append({"name": name, "expr": expr})

    return res, has_distinct, has_partials


def parse_orderby(clause, strings):
    """splits the ORDER BY clause and handles modifiers"""

    res = []
    mod_pattern = re.compile(r"(?:\s+(DESC|ASC))?(?:\s+NULLS\s+(FIRST|LAST)\s*)?$")
    for expr in split_multi_expr_clause(clause):
        modifs = re.search(mod_pattern, expr.upper())
        rev = "DESC" in modifs.groups()
        rev_nulls = ((not rev) and "FIRST" in modifs.groups()) or (
            rev and "LAST" in modifs.groups()
        )
        expr = expr[: (modifs.span()[0])]  # remove modifiers
        try:
            expr = int(expr)  # special case: expression is output column number
        except ValueError:
            expr = make_expr_ready(expr)

        res.append({"expr": expr, "rev": rev, "rev_nulls": rev_nulls})

    return res


def parse_groupby(clause, select, strings):
    """splits the GROUP BY clause"""

    res = []
    for expr in split_multi_expr_clause(clause):
        try:
            expr = int(expr)  # special case: expression is output column number
            # in the case of group by, the expression is copied from select to avoid
            # group by depending on select (see spyql.processor._go)
            expr = select[expr - 1]["expr"]
        except ValueError:
            expr = make_expr_ready(expr)
        throw_error_if_has_agg_func(expr, "GROUP BY")
        res.append({"expr": expr})

    return res


def extract_args(*args, **kwargs):
    return {"args": args, "kwargs": kwargs}


def parse_fromto(clause, strings, formats_list):
    formats_pattern = re.compile(rf"^(\w+)\s*(\(?.*$)")
    sformats = re.search(formats_pattern, clause)
    if not sformats or sformats.groups()[0].upper() not in formats_list:
        return make_expr_ready(clause)  # python expression
    sformats = sformats.groups()
    res = eval(
        "extract_args"
        + (
            strings.put_strings_back(make_expr_ready(sformats[1]), strings)
            if sformats[1]
            else "()"
        )
    )
    res["name"] = sformats[0]
    return res


def make_expr_ready(expr):
    return pythonize(expr).strip()


def parse(query, default_to_clause="MEMORY"):
    """parses the spyql query"""
    strings = QuotesHandler()
    query = strings.extract_strings(query)
    query_has_agg_funcs = has_agg_func(query)
    prs = parse_structure(query)
    prs["hints"] = {"has_reference2row": has_reference2row(query)}
    if not prs["to"]:
        prs["to"] = default_to_clause

    if not prs["select"]:
        log.user_error(
            "could not parse query", SyntaxError("SELECT keyword is missing")
        )

    prs["select"], prs["distinct"], prs["partials"] = parse_select(
        prs["select"], strings
    )

    for clause in set(query_struct_keywords) - {
        "select",
        "limit",
        "offset",
        "group by",
        "order by",
    }:
        if prs[clause]:
            if clause in {"where", "from"}:
                throw_error_if_has_agg_func(prs[clause], clause.upper())
            if clause == "from":
                prs[clause] = parse_fromto(
                    prs[clause], strings, Processor.input_processors().keys()
                )
            elif clause == "to":
                prs[clause] = parse_fromto(
                    prs[clause], strings, Writer.output_writers().keys()
                )
            else:
                prs[clause] = make_expr_ready(prs[clause])

    for clause in {"group by"}:
        if prs[clause]:
            prs[clause] = parse_groupby(prs[clause], prs["select"], strings)
        elif query_has_agg_funcs:
            # creates a dummy group by with a constant if there are agg functions
            # e.g. `select count_agg(*) from csv`
            prs[clause] = [{"expr": "'_OVERALL_'"}]
            if prs["order by"]:
                log.user_warning(
                    "ORDER BY is useless since output will have a single result"
                )
        if prs[clause] and prs["distinct"]:
            # This is feasible to implement but currently not supported
            log.user_error(
                "DISTINCT cannot be used in aggregation queries",
                SyntaxError("bad query"),
            )

    for clause in {"order by"}:
        if prs[clause]:
            prs[clause] = parse_orderby(prs[clause], strings)

    for clause in {"limit", "offset"}:
        if prs[clause]:
            try:
                val = int(prs[clause])
                prs[clause] = val if val > 0 else 0
            except ValueError:
                prs[clause] = None

    return (prs, strings)
