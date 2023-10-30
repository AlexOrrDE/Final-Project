import pg8000.dbapi

get_conn = pg8000.dbapi.connect(user="project_user_1", host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com", database="totesys", port=5432, password="WfAsWSh4nvEUEOw6")

def get_staff(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM staff")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]
    results = [dict(zip(keys, row)) for row in rows]

    return results


staff_list = get_staff(get_conn)


def get_address(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM address")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]

    results = [dict(zip(keys, row)) for row in rows]

    return results

address_list = get_address(get_conn)

def get_department(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM department")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]

    results = [dict(zip(keys, row)) for row in rows]

    return results

department_list = get_department(get_conn)

def get_currency(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM currency")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]

    results = [dict(zip(keys, row)) for row in rows]

    return results

currency_list = get_currency(get_conn)


def get_design(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM design")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]

    results = [dict(zip(keys, row)) for row in rows]

    return results

design_list = get_design(get_conn)


def get_counterparty(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM counterparty")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]

    results = [dict(zip(keys, row)) for row in rows]

    return results

counterparty_list = get_counterparty(get_conn)


def get_sales_order(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sales_order")

    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]

    results = [dict(zip(keys, row)) for row in rows]

    return results

sales_order_list = get_sales_order(get_conn)


# print(staff_list, "STAFF DATA\n")
print(address_list[0], "ADDRESS DATA \n")
# print(department_list, "DEPARTMENT DATA \n")
# print(currency_list, "CURRENCY DATA \n")
# print(design_list, "DESIGN DATA \n")
# print(counterparty_list, "COUNTERPART DATA \n")
# print(sales_order_list, "SALES ORDER DATA \n")
