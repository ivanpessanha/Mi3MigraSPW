import pyodbc
import psycopg2

def get_sql_server_connection(database):
    connection_string = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER=192.168.0.80\\BD02_CONFEF;
    DATABASE={database};
    UID=INFO;
    PWD=ONte(*(#98U;
    """
    return pyodbc.connect(connection_string)

def get_postgresql_connection(database):
    return psycopg2.connect(
        host="localhost",
        user="postgres",
        password="Pretzel25%",
        database=database
    )