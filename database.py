import pyodbc
import psycopg2

def get_freetds_connection(database, instance):
    connection_string = (
        f"DSN={instance};"
        f"DATABASE={database};"
        f"UID=INFO;"
        f"PWD=ONte(*(#98U;"
    )
    return pyodbc.connect(connection_string)

def get_sql_server_connection(database,instance):
    connection_string = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER=192.168.0.80\\{instance};
    DATABASE={database};
    UID=INFO;
    PWD=ONte(*(#98U;
    """
    return pyodbc.connect(connection_string)

def get_postgresql_connection(database):
    return psycopg2.connect(
        host="/var/run/postgresql",
        user="informatica",
        password="yqT7<}Z4K>Nb",
        database=database
    )