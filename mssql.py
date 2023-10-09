import pyodbc
from config import MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USERNAME, MSSQL_PASSWORD
from utils import save_order_to_mssql as generate_sql_for_order

def insert_order_to_mssql(order, email):
    conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD}')
    cursor = conn.cursor()
    sql = generate_sql_for_order(order,email)  # Utils dosyasındaki bir yardımcı fonksiyon
    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()
