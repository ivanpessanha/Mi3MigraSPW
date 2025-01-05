import sys
import os
import re
import pymysql
from tabulate import tabulate
from termcolor import colored
from tqdm import tqdm

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import sql_server_conn

VALID_MYSQL_TYPES = {'int', 'varchar', 'text', 'date', 'datetime', 'decimal', 'float', 'double', 'char', 'tinyint', 'smallint', 'mediumint', 'bigint', 'bit', 'boolean', 'serial', 'blob', 'mediumblob', 'longblob', 'enum', 'set', 'json'}
VALID_MYSQL_TYPES_WITH_LENGTH = {'varchar', 'char', 'decimal', 'double'}
VALID_MYSQL_TYPES_WITHOUT_LENGTH = {'int', 'text', 'date', 'datetime', 'tinyint', 'smallint', 'mediumint', 'bigint', 'bit', 'boolean', 'serial', 'blob', 'mediumblob', 'longblob', 'enum', 'set', 'json', 'float'}

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

def process_column(column_name, column_info):
    column_type = column_info.get('type', 'varchar')
    column_length = column_info.get('length', 255)  # Default length if not provided
    
    # Map SQL Server 'money' type to MySQL 'decimal(19,4)'
    if column_type.lower() == 'money':
        column_type = 'decimal'
        column_length = '19,4'
    
    # Ensure the column type is valid
    if column_type.lower() not in VALID_MYSQL_TYPES_WITH_LENGTH and column_type.lower() not in VALID_MYSQL_TYPES_WITHOUT_LENGTH:
        raise ValueError(f"Unknown data type: '{column_type}'")
    
    # Adjust the column processing logic to handle precision and scale
    if column_type.lower() in VALID_MYSQL_TYPES_WITH_LENGTH:
        if isinstance(column_length, str):
            mysql_data_type = f"{column_type}({column_length})"
        else:
            if column_length is None or column_length <= 0 or column_length > 65:
                column_length = 65  # Maximum precision for MySQL
            mysql_data_type = f"{column_type}({column_length})"
    else:
        mysql_data_type = column_type
    
    # Normalize column name: replace spaces with underscores and remove invalid characters
    normalized_name = re.sub(r'\W+', '_', column_name.lower())
    normalized_name = re.sub(r'_+', '_', normalized_name)  # Remove consecutive underscores
    
    return {
        'normalized_name': normalized_name,
        'mysql_data_type': mysql_data_type
    }

def generate_mysql_table_ddl(table_name):
    columns = get_table_columns(table_name)
    column_definitions = []
    
    for column_name, column_info in columns.items():
        try:
            processed_column = process_column(column_name, column_info)
            column_definitions.append(f"{processed_column['normalized_name']} {processed_column['mysql_data_type']}")
        except ValueError as e:
            print(f"Skipping column {column_name} due to error: {e}")
    
    normalized_table_name = re.sub(r'\W+', '_', table_name.lower())
    normalized_table_name = re.sub(r'_+', '_', normalized_table_name)  # Remove consecutive underscores
    create_table_query = f"CREATE TABLE {normalized_table_name} (\n    {',\n    '.join(column_definitions)}\n);"
    
    return create_table_query

def create_mysql_tables(table_names):
    connection = pymysql.connect(
        host="localhost",
        user="root",
        password="Pretzel25%",
        database="cadastro_spw"
    )
    
    with connection.cursor() as cursor:
        for table_name in tqdm(table_names, desc="Creating tables", unit="table"):
            drop_table_query = f"DROP TABLE IF EXISTS {table_name.lower()};"
            cursor.execute(drop_table_query)
            create_table_query = generate_mysql_table_ddl(table_name)
            print(f"DDL Query for {table_name}:\n{create_table_query}")  # Print the DDL query
            try:
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table {table_name} created successfully.")
            except (ValueError, pymysql.ProgrammingError) as e:
                print(f"Error creating table {table_name}: {e}")
                print(f"DDL Query: {create_table_query}")
    
    connection.close()