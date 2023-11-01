import pg8000.dbapi
import boto3
from datetime import datetime

get_conn = pg8000.dbapi.connect(user="project_user_1", host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com", database="totesys", port=5432, password="WfAsWSh4nvEUEOw6")


def get_data(conn, table):
    # set to last month to check it picks up data, should be set to now() //  
    timestamp = datetime(2023, 9, 30, 12)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT * FROM {table}")
    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]
    results = {f"{table}{timestamp}": [dict(zip(keys, row)) for row in rows]}

    return results


# client = boto3.client('s3')
# client.put_object(Body=more_binary_data, Bucket='my_bucket_name', Key='my/key/including/anotherfilename.txt')


staff_data = get_data(get_conn, "staff")
address_data = get_data(get_conn, "address")
department_data = get_data(get_conn, "department")
currency_data = get_data(get_conn, "currency")
design_data = get_data(get_conn, "design")
counterparty_data = get_data(get_conn, "counterparty")
sales_order_data = get_data(get_conn, "sales_order")

print(staff_data)
# print(address_data)
# print(department_data)
# print(currency_data)
# print(design_data)
# print(counterparty_data)
# print(sales_order_data)

def check_data(conn, table, previous_instance):
    time = list(previous_instance.keys())[0].split(f"{table}")[1]
    cursor = conn.cursor()

    cursor.execute(f"SELECT * FROM {table} WHERE last_updated > '{time}';")
    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]
    results = [dict(zip(keys, row)) for row in rows]

    # if len(results) > 0: run extraction lambda to get new dataset

    return results


print(check_data(get_conn, "staff", staff_data), "NEW STAFF DATA")
print(check_data(get_conn, "address", address_data), "NEW ADDRESS DATA")
print(check_data(get_conn, "department", department_data), "NEW DEPARTMENT DATA")
print(check_data(get_conn, "currency", currency_data), "NEW CURRENCY DATA")
print(check_data(get_conn, "design", design_data), "NEW DESIGN DATA")
print(check_data(get_conn, "counterparty", counterparty_data), "NEW COUNTERPARTY DATA")
print(check_data(get_conn, "sales_order", sales_order_data), "NEW SALES ORDER DATA")
# table_names = ['staff', 'address', 'department', 'currency', 'design', 'counterparty', 'sales_order']