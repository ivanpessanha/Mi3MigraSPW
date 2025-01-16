import re
import psycopg2
from tqdm import tqdm
import psycopg2.extras
from psycopg2 import sql

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

def normalize_name(name):
    """
    Normalize table or column names to be PostgreSQL-compliant.
    - Replace spaces with underscores.
    - Remove invalid characters.
    - Add an underscore if the name starts with a number.
    """
    # Substituir espaços por underscores e remover caracteres inválidos
    name = re.sub(r'\s+', '_', name)  # Replace spaces with underscores
    name = re.sub(r'[^\w]', '', name)  # Remove non-alphanumeric characters
    name = name.lower()  # Convert to lowercase

    # Garantir que não comece com um número
    if name[0].isdigit():
        name = f"_{name}"

    return name


def process_column(column_name, column_info):
    """Process column details and generate PostgreSQL-compatible type."""
    column_type = column_info.get('type', 'varchar').lower()
    column_length = column_info.get('length', 255)
    
    if column_length == -1 and column_type in {'varchar', 'nvarchar'}:
        column_type = 'text'
        column_length = None
    
    if column_type in SQL_SERVER_TO_PGSQL_TYPE_MAP:
        pgsql_data_type = SQL_SERVER_TO_PGSQL_TYPE_MAP[column_type]
        if column_type in {'nvarchar', 'nchar', 'varchar', 'char'} and column_length:
            pgsql_data_type = f"{pgsql_data_type}({column_length})"
    else:
        if column_type not in VALID_PGSQL_TYPES_WITH_LENGTH and column_type not in VALID_PGSQL_TYPES_WITHOUT_LENGTH:
            raise ValueError(f"Unknown data type: '{column_type}'")
        
        pgsql_data_type = column_type

    # Normalize column name and wrap in double quotes
    normalized_name = normalize_name(column_name)
    safe_name = f'"{normalized_name}"'
    return {
        'normalized_name': safe_name,
        'pgsql_data_type': pgsql_data_type
    }

def get_table_columns(sql_server_conn, table_name):
    """Get column details from SQL Server."""
    cursor = sql_server_conn.cursor()
    query = f"""
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    """
    cursor.execute(query)
    columns = cursor.fetchall()
    return {
        column_name: {
            'type': data_type,
            'length': character_maximum_length
        }
        for column_name, data_type, character_maximum_length in columns
    }

def generate_pgsql_table_ddl(sql_server_conn, table_name, schema):
    """Generate PostgreSQL table creation DDL."""
    columns = get_table_columns(sql_server_conn, table_name)
    column_definitions = []
    
    for column_name, column_info in columns.items():
        processed_column = process_column(column_name, column_info)
        column_definitions.append(f"{processed_column['normalized_name']} {processed_column['pgsql_data_type']}")
    
    normalized_table_name = normalize_name(table_name)
    
    return f"CREATE TABLE {schema}.{normalized_table_name} (\n    {',    '.join(column_definitions)}\n);"

def drop_table_if_exists(postgresql_conn, schema, table_name):
    """Drop table if it exists in PostgreSQL."""
    normalized_table_name = normalize_name(table_name)
    with postgresql_conn.cursor() as cursor:
        query = sql.SQL("DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;").format(
            schema=sql.Identifier(schema),
            table_name=sql.Identifier(normalized_table_name)
        )
        cursor.execute(query)
        postgresql_conn.commit()
        print(f"Dropped table {schema}.{normalized_table_name}")

def copy_table_data(sql_server_conn, postgresql_conn, table_name, schema, batch_size=1000):
    # Lista de tabelas a serem excluídas da cópia de dados
    excluded_tables = ['SFNH135',
                       'SCDH001','SCDH002','SCDH003','SCDH004','SCDH005',
                       'SCRH001','SCRH002','SCRH003','SCRH004','SCRH005']  # Adicione aqui as tabelas que você não quer copiar os dados
    
    if table_name in excluded_tables:
        print(f"Skipping data copy for table {table_name}")
        return
    
    """Copy table data in batches from SQL Server to PostgreSQL."""
    source_cursor = sql_server_conn.cursor()
    dest_cursor = postgresql_conn.cursor()
    
    columns = get_table_columns(sql_server_conn, table_name)
    source_columns = ', '.join([f"[{col}]" for col in columns.keys()])
    dest_columns = ', '.join([process_column(col, info)['normalized_name'] for col, info in columns.items()])
    
    select_query = f"SELECT {source_columns} FROM {table_name}"
    insert_query = f"INSERT INTO {schema}.{normalize_name(table_name)} ({dest_columns}) VALUES %s"
    
    offset = 0
    while True:
        source_cursor.execute(f"{select_query} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY")
        rows = source_cursor.fetchall()
        if not rows:
            break
        
        cleaned_rows = []
        for row in rows:
            cleaned_row = [col.replace('\x00', '') if isinstance(col, str) else col for col in row]
            cleaned_rows.append(cleaned_row)
        
        psycopg2.extras.execute_values(dest_cursor, insert_query, cleaned_rows)
        postgresql_conn.commit()
        offset += batch_size
        tqdm.write(f"Processed {offset} rows for {table_name}")
    
    dest_cursor.close()
    source_cursor.close()

def create_pgsql_tables(sql_server_conn, postgresql_conn, table_names, schema):
    """Create tables and copy data from SQL Server to PostgreSQL."""
    with postgresql_conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        postgresql_conn.commit()
        
        for table_name in tqdm(table_names, desc="Creating tables", unit="table"):
            drop_table_if_exists(postgresql_conn, schema, table_name)
            create_table_query = generate_pgsql_table_ddl(sql_server_conn, table_name, schema)
            print(create_table_query)
            cursor.execute(create_table_query)
            postgresql_conn.commit()
            print(f"Table {table_name} created successfully.")
            copy_table_data(sql_server_conn, postgresql_conn, table_name, schema, batch_size=1000)

def get_short_tables(sql_server_conn):
    """Get tables with short names from SQL Server."""
    cursor = sql_server_conn.cursor()
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE' 
    AND table_schema = 'dbo' 
    AND LEN(table_name) <= 7 
    AND (table_name LIKE 'SF%')
    ORDER BY table_name
    """
    #AND (table_name LIKE 'SC%' OR table_name LIKE 'SF%')
    cursor.execute(query)
    tables = cursor.fetchall()
    cursor.close()
    return [table[0] for table in tables]
