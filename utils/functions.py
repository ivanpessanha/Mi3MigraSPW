import sys
import os
import re
import pymysql
from tabulate import tabulate
from termcolor import colored

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import sql_server_conn

def get_short_tables():
    cursor = sql_server_conn.cursor()
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE' 
    AND table_schema = 'dbo' 
    AND LEN(table_name) <= 7
    """
    cursor.execute(query)
    tables = cursor.fetchall()
    cursor.close()
    return [table[0] for table in tables]

def get_table_columns(table_name):
    cursor = sql_server_conn.cursor()
    query = f"""
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    """
    cursor.execute(query)
    columns = cursor.fetchall()
    cursor.close()
    return {column[0]: {'type': column[1], 'length': column[2]} for column in columns}

def normalize_column_name(column_name):
    # Remove special characters, convert to lowercase, and replace spaces with underscores
    normalized_name = re.sub(r'[^a-zA-Z0-9\s]', '', column_name).lower().replace(' ', '_')
    return normalized_name

def convert_sql_server_to_mysql(data_type, length):
    # Define a mapping from SQL Server data types to MySQL data types
    type_mapping = {
        'bigint': 'BIGINT',
        'binary': f'BINARY({length})',
        'bit': 'BOOLEAN',
        'char': f'CHAR({length})',
        'date': 'DATE',
        'datetime': 'DATETIME',
        'datetime2': 'DATETIME',
        'datetimeoffset': 'DATETIME',
        'decimal': f'DECIMAL({length})',
        'float': 'FLOAT',
        'image': 'LONGBLOB',
        'int': 'INT',
        'money': 'DECIMAL(19,4)',
        'nchar': f'CHAR({length})',
        'ntext': 'LONGTEXT',
        'numeric': f'DECIMAL({length})',
        'nvarchar': f'VARCHAR({length})',
        'real': 'FLOAT',
        'smalldatetime': 'DATETIME',
        'smallint': 'SMALLINT',
        'smallmoney': 'DECIMAL(10,4)',
        'text': 'LONGTEXT',
        'time': 'TIME',
        'timestamp': 'TIMESTAMP',
        'tinyint': 'TINYINT',
        'uniqueidentifier': 'CHAR(36)',
        'varbinary': f'VARBINARY({length})',
        'varchar': f'VARCHAR({length})',
        'xml': 'LONGTEXT',
        # Add more mappings as needed
    }
    return type_mapping.get(data_type.lower(), 'TEXT')  # Default to TEXT if type not found

def process_column(column_name, data_type, length):
    normalized_name = normalize_column_name(column_name)
    mysql_data_type = convert_sql_server_to_mysql(data_type, length)
    return {
        'normalized_name': normalized_name,
        'mysql_data_type': mysql_data_type
    }

def generate_mysql_table_ddl(table_name):
    columns = get_table_columns(table_name)
    column_definitions = []
    
    for column_name, column_info in columns.items():
        processed_column = process_column(column_name, column_info['type'], column_info['length'])
        column_definitions.append(f"{processed_column['normalized_name']} {processed_column['mysql_data_type']}")
    
    normalized_table_name = table_name.lower()
    create_table_query = f"CREATE TABLE {normalized_table_name} (\n    {',\n    '.join(column_definitions)}\n);"
    
    print(colored(f"DDL for table {normalized_table_name}:", 'cyan'))
    print(colored(create_table_query, 'green'))