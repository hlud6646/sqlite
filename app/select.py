import sqlparse
import sqlparse.tokens as T

"where age > 18"


def make_predicate(where_condition):
    cond = where_condition.split()
    assert cond[0].lower() == "where"
    cond.pop(0)
    column_name = cond.pop(0)
    operator = cond.pop(0)
    right = " ".join(cond)[1:-1]
    if operator == "=":
        operator = "=="

    def p(left):
        return eval(f"'{left}' {operator} '{right}'")

    return (column_name, p)


def parse_select_exprs(exprs):
    """
    Helper function for the parser below, to parse something
    like "min(age), surname" into a list of (callable, column name) tuples.
    This is future-proofing, since calls like min(age) are not required at
    the moment.
    """
    identity = lambda x: x
    out = []
    for e in exprs.value.split():
        # This feels ugly
        e = e.replace(",", "")
        if "(" in e:
            e = e.split("(")
            e[1] = e[1][:-1]  # Drop closing parenth.
        else:
            e = ("identity", e)
        out.append(tuple(e))
    return out


def parse_select(sql):
    sql = sqlparse.parse(sql)[0]
    # Parse the 'SELECT' keyword.
    assert sql[0].ttype == T.Keyword.DML and sql[0].value.lower() == "select"
    assert sql[1].ttype == T.Whitespace
    select_exprs = sql[2]
    # This is now a list of (function name or None, column name).
    select_exprs = parse_select_exprs(select_exprs)
    assert sql[3].ttype == T.Whitespace
    assert sql[4].ttype == T.Keyword and sql[4].value.lower() == "from"
    assert sql[5].ttype == T.Whitespace
    # Parse table name.
    table_name = sql[6].value
    if len(sql.tokens) > 7:
        assert sql[7].ttype == T.Whitespace
        condition = sql[8].value
    else:
        condition = None
    return (select_exprs, table_name, condition)
