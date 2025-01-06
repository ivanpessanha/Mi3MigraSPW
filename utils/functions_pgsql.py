import sys
import os
import re
import psycopg2
from tqdm import tqdm
import psycopg2.extras

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from database import sql_server_conn
from database import postgresql_conn

VALID_PGSQL_TYPES_WITH_LENGTH = {'varchar', 'char', 'decimal', 'numeric'}
VALID_PGSQL_TYPES_WITHOUT_LENGTH = {'int', 'text', 'date', 'timestamp', 'smallint', 'bigint', 'boolean', 'bytea', 'json', 'jsonb', 'uuid', 'serial', 'bigserial', 'real', 'double precision'}

SQL_SERVER_TO_PGSQL_TYPE_MAP = {
    'bigint': 'bigint',
    'binary': 'bytea',
    'bit': 'boolean',
    'char': 'char',
    'date': 'date',
    'datetime': 'timestamp',
    'datetime2': 'timestamp',
    'datetimeoffset': 'timestamptz',
    'decimal': 'decimal',
    'float': 'double precision',
    'image': 'bytea',
    'int': 'int',
    'money': 'numeric(19,4)',
    'nchar': 'char',
    'ntext': 'text',
    'numeric': 'numeric',
    'nvarchar': 'varchar',
    'real': 'real',
    'smalldatetime': 'timestamp',
    'smallint': 'smallint',
    'smallmoney': 'numeric(10,4)',
    'text': 'text',
    'time': 'time',
    'timestamp': 'bytea',
    'tinyint': 'smallint',
    'uniqueidentifier': 'uuid',
    'varbinary': 'bytea',
    'varchar': 'varchar',
    'xml': 'xml'
}

def process_column(column_name, column_info):
    column_type = column_info.get('type', 'varchar').lower()
    column_length = column_info.get('length', 255)  # Default length if not provided
    
    # Handle varchar(max) by mapping to text in PostgreSQL
    if column_length == -1 and column_type in {'varchar', 'nvarchar'}:
        column_type = 'text'
        column_length = None
    
    # Map SQL Server types to PostgreSQL types
    if column_type in SQL_SERVER_TO_PGSQL_TYPE_MAP:
        pgsql_data_type = SQL_SERVER_TO_PGSQL_TYPE_MAP[column_type]
        if column_type in {'nvarchar', 'nchar', 'varchar', 'char'} and column_length is not None:
            pgsql_data_type = f"{pgsql_data_type}({column_length})"
    else:
        # Ensure the column type is valid
        if column_type not in VALID_PGSQL_TYPES_WITH_LENGTH and column_type not in VALID_PGSQL_TYPES_WITHOUT_LENGTH:
            raise ValueError(f"Unknown data type: '{column_type}'")
        
        # Adjust the column processing logic to handle precision and scale
        if column_type in VALID_PGSQL_TYPES_WITH_LENGTH:
            if isinstance(column_length, str):
                pgsql_data_type = f"{column_type}({column_length})"
            else:
                if column_length is None or column_length <= 0 or column_length > 1000:
                    column_length = 255  # Default length if invalid
                pgsql_data_type = f"{column_type}({column_length})"
        else:
            pgsql_data_type = column_type
    
    # Normalize column name: replace spaces with underscores and remove invalid characters
    normalized_name = re.sub(r'\W+', '_', column_name.lower())
    normalized_name = re.sub(r'_+', '_', normalized_name)  # Remove consecutive underscores
    
    return {
        'normalized_name': normalized_name,
        'pgsql_data_type': pgsql_data_type
    }

def get_table_columns(table_name):
    cursor = sql_server_conn.cursor()
    query = f"""
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    """
    cursor.execute(query)
    columns = cursor.fetchall()
    column_info = {}
    for column_name, data_type, character_maximum_length in columns:
        column_info[column_name] = {
            'type': data_type,
            'length': character_maximum_length
        }
    return column_info

def generate_pgsql_table_ddl(table_name, schema):
    columns = get_table_columns(table_name)
    column_definitions = []
    
    for column_name, column_info in columns.items():
        processed_column = process_column(column_name, column_info)
        column_definitions.append(f"{processed_column['normalized_name']} {processed_column['pgsql_data_type']}")
    
    normalized_table_name = re.sub(r'\W+', '_', table_name.lower())
    normalized_table_name = re.sub(r'_+', '_', normalized_table_name)  # Remove consecutive underscores
    create_table_query = f"CREATE TABLE {schema}.{normalized_table_name} (\n    {',\n    '.join(column_definitions)}\n);"
    
    return create_table_query

def copy_table_data(table_name, schema):
    source_cursor = sql_server_conn.cursor()
    dest_connection = postgresql_conn
    dest_cursor = dest_connection.cursor()
    
    columns = get_table_columns(table_name)
    source_columns = ', '.join([f"[{col}]" for col in columns.keys()])  # Ensure column names are correctly quoted
    dest_columns = ', '.join([process_column(col, info)['normalized_name'] for col, info in columns.items()])
    
    select_query = f"SELECT {source_columns} FROM {table_name}"
    insert_query = f"INSERT INTO {schema}.{re.sub(r'\W+', '_', table_name.lower())} ({dest_columns}) VALUES %s"
    
    chunk_size = 5000
    offset = 0
    
    while True:
        source_cursor.execute(f"{select_query} ORDER BY 1 OFFSET {offset} ROWS FETCH NEXT {chunk_size} ROWS ONLY")
        rows = source_cursor.fetchall()
        if not rows:
            break
        
        cleaned_rows = []
        for row in rows:
            cleaned_row = [col.replace('\x00', '') if isinstance(col, str) else col for col in row]
            cleaned_rows.append(cleaned_row)
        
        psycopg2.extras.execute_values(dest_cursor, insert_query, cleaned_rows)
        dest_connection.commit()
        
        offset += chunk_size
        tqdm.write(f"Processed {offset} rows for {table_name}")
    
    dest_cursor.close()
    source_cursor.close()

def create_pgsql_tables(table_names, schema):
    connection = postgresql_conn
    
    with connection.cursor() as cursor:
        # Create schema if it does not exist
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        connection.commit()
        
        for table_name in tqdm(table_names, desc="Creating tables", unit="table"):
            drop_table_query = f"DROP TABLE IF EXISTS {schema}.{table_name.lower()};"
            cursor.execute(drop_table_query)
            create_table_query = generate_pgsql_table_ddl(table_name, schema)
            print(f"DDL Query for {table_name}:\n{create_table_query}")  # Print the DDL query
            try:
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table {table_name} created successfully.")
                copy_table_data(table_name, schema)  # Copy data after creating the table
            except psycopg2.errors.DuplicateTable as e:
                print(f"Error creating table {table_name}: {e}")
                print(f"DDL Query: {create_table_query}")
                raise e
    
    ##connection.close()

def get_short_tables():
    cursor = sql_server_conn.cursor()
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE' 
    AND table_schema = 'dbo' 
    AND LEN(table_name) <= 7 AND (table_name LIKE 'sc%' OR table_name LIKE 'sf%')
    """
    cursor.execute(query)
    tables = cursor.fetchall()
    cursor.close()
    return [table[0] for table in tables]