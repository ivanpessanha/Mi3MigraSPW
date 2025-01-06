import pyodbc
#import pymysql
import psycopg2

# SQL Server connection
sql_server_conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=192.168.0.80\\BD01_CREFs;'
    'DATABASE=CREF_RJ_SCF;'
    'UID=INFO;'
    'PWD=ONte(*(#98U'
)

# MySQL connection using pymysql
"""
mysql_conn = pymysql.connect(
    host="192.168.0.5",
    user="root",
    password="Pretzel25%",
    database="cadastro_spw"
)
"""

# PostgreSQL connection using psycopg2
postgresql_conn = psycopg2.connect(
    host="192.168.0.5",
    user="informatica",
    password="yqT7<}Z4K>Nb",
    database="efcontrol_cadastro"
)