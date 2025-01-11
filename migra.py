import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))

from utils.functions_pgsql import get_short_tables, create_pgsql_tables
from database import get_sql_server_connection, get_postgresql_connection

# Example usage
if __name__ == "__main__":
    sql_server_database = "CONFEF_SOP"
    postgresql_database = "efcontrol_contratos"
    
    sql_server_conn = get_sql_server_connection(sql_server_database)
    postgresql_conn = get_postgresql_connection(postgresql_database)
    
    schema = "br"
    short_tables = get_short_tables(sql_server_conn)
    create_pgsql_tables(sql_server_conn, postgresql_conn, short_tables, schema)
    
    sql_server_conn.close()
    postgresql_conn.close()