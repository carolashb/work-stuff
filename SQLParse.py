import regex as re
import pandas as pd
from collections import defaultdict

def tables_in_query(sql_str):

    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # split on blanks, parens, semicolons, and commas
    tokens = re.split(r"[\s)(;,]+", q)

    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).

    result = set()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.add(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]

    return result

sample_query = """SELECT
                c.calendar_date,
                c.calendar_year,
                c.calendar_month,
                c.calendar_dayname,
                COUNT(DISTINCT co.order_id) AS num_orders,
                COUNT(ol.book_id) AS num_books,
                SUM(ol.price) AS total_price,
                SUM(COUNT(ol.book_id)) OVER (
                  PARTITION BY c.calendar_year, c.calendar_month
                  ORDER BY c.calendar_date
                ) AS running_total_num_books,
                LAG(COUNT(ol.book_id), 7) OVER (ORDER BY c.calendar_date) AS prev_books
                FROM calendar_days c
                LEFT JOIN cust_order co ON c.calendar_date = DATE(co.order_date)
                LEFT JOIN order_line ol ON co.order_id = ol.order_id
                GROUP BY c.calendar_date, c.calendar_year, c.calendar_month, c.calendar_dayname
                ORDER BY c.calendar_date ASC;"""

table_names = tables_in_query(sample_query)

def GetAliases(sample_query):
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sample_query)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # split on blanks, parens, semicolons, and commas
    tokens = re.split(r"[\s)(;,]+", q)

    it = iter(tokens)
    tokens[:] =  [f"{i}:{next(it)}" if i in table_names else i for i in it]

    aliases = []
    temp = r'(%s.*)' % ':.*|'.join(table_names)
    for i in tokens:
        match = re.findall(temp,i)
        if len(match) >0 :
            aliases.append(re.sub(r"[\['\]]", "", f'{match}'))
    aliases

    aliases_dict = {}
    for aliase in aliases:
        aliases_dict[aliase.split(":")[0]] = aliase.split(":")[1]
        
    return aliases_dict

def GetQueryColumns(tables):
    aliases_dict = GetAliases(sample_query)
    columns = []
    column_string = r'(^%s\W\w*)' % '\W\w*|^'.join(aliases_dict.values())
    for i in tokens:
        match = re.findall(column_string,i)
        if len(match) > 0 :
            columns.append(re.sub(r"[\['\]]", "", f"{match}"))
    column_names = set(columns)

    aliases = []
    columns = []
    for x in column_names:
        aliases.append(x.split('.')[0])
        columns.append(x.split('.')[1])

    combined_dict = defaultdict(list)
    for k, v in zip(aliases,columns):
        combined_dict[k].append(v)
        
    return combined_dict

query2 = """
          SELECT ProductID, Name, SellStartDate FROM SalesLT.Product  
          WHERE year(SellStartDate)='2005';
         """

def GetFilters(sql_string):
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_string)

    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]

    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])

    # split on blanks, parens, semicolons, and commas
    tokens = re.split(r"[\s)(;,]+", q)
    
    # scan the tokens. if we see a FROM or JOIN, we set the get_next
    # flag, and grab the next one (unless it's SELECT).
    filters = re.findall(r'WHERE(.*);', sql_string)

    return filters