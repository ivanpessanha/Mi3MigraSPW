import pyodbc
import pymysql

# SQL Server connection
sql_server_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=192.168.0.80\\BD01_CREFs;'
    'DATABASE=CREF_RJ_SCF;'
    'UID=INFO;'
    'PWD=ONte(*(#98U'
)

# MySQL connection using pymysql

mysql_conn = pymysql.connect(
    host="localhost",
    user="root",
    password="Pretzel25%",
    database="cadastro_spw"
)
