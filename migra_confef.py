import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))

from utils.functions_pgsql import get_short_tables, create_pgsql_tables
from dbs import get_sql_server_connection, get_postgresql_connection, get_freetds_connection

def migrar(base_origem, base_destino, schema, instancia_origem, copy_data=True):
    """
    Migra tabelas de uma base de dados do SQL Server para o PostgreSQL.

    Args:
        base_origem (str): Nome da base de dados no SQL Server.
        base_destino (str): Nome da base de dados no PostgreSQL.
        schema (str): Esquema no PostgreSQL onde as tabelas serão criadas.
    """
    # Conectar ao SQL Server
    sql_server_conn = get_sql_server_connection(base_origem, instancia_origem)
    
    # Conectar ao PostgreSQL
    postgresql_conn = get_postgresql_connection(base_destino)

    try:
        # Obter as tabelas curtas da base de origem
        short_tables = get_short_tables(sql_server_conn)

        # Criar tabelas e migrar dados
        create_pgsql_tables(sql_server_conn, postgresql_conn, short_tables, schema, copy_data)

    finally:
        # Fechar conexões
        sql_server_conn.close()
        postgresql_conn.close()

# Exemplo de uso
if __name__ == "__main__":
    bases_para_migrar = [
        #("CONFEF_SDP", "efcontrol_eventos", "confef", "BD02_CONFEF"),
        #("CONFEF_SOP", "efcontrol_pagamentos", "confef", "BD02_CONFEF"),
        ("CONFEF_SEQ", "efcontrol_migracao", "public", "BD02_CONFEF", False),        
    ]

    for base_origem, base_destino, schema, instancia, copy_data in bases_para_migrar:
        print(f"Migrando dados de {base_origem} para {base_destino}...")
        migrar(base_origem, base_destino, schema, instancia, copy_data)
        print(f"Migração de {base_origem} para {base_destino} concluída.")