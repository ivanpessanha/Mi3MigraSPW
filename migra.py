import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))

from utils.functions_pgsql import get_short_tables, create_pgsql_tables
from database import get_sql_server_connection, get_postgresql_connection, get_freetds_connection

def migrar(base_origem, base_destino, schema, instancia_origem):
    """
    Migra tabelas de uma base de dados do SQL Server para o PostgreSQL.

    Args:
        base_origem (str): Nome da base de dados no SQL Server.
        base_destino (str): Nome da base de dados no PostgreSQL.
        schema (str): Esquema no PostgreSQL onde as tabelas serão criadas.
    """
    # Conectar ao SQL Server
    sql_server_conn = get_freetds_connection(base_origem, instancia_origem)
    
    # Conectar ao PostgreSQL
    postgresql_conn = get_postgresql_connection(base_destino)

    try:
        # Obter as tabelas curtas da base de origem
        short_tables = get_short_tables(sql_server_conn)

        # Criar tabelas e migrar dados
        create_pgsql_tables(sql_server_conn, postgresql_conn, short_tables, schema)

    finally:
        # Fechar conexões
        sql_server_conn.close()
        postgresql_conn.close()

# Exemplo de uso
if __name__ == "__main__":
    bases_para_migrar = [
        #("CONFEF_SDP", "efcontrol_eventos", "confef", "BD02_CONFEF"),
        #("CONFEF_SOP", "efcontrol_pagamentos", "confef", "BD02_CONFEF"),
        #("CONFEF_SEQ", "efcontrol_estoque", "confef", "BD02_CONFEF"),
        
        #MIGRAÇÃO DE CADASTRO DOS CREFS
        ("CREF_RJ_SCF", "efcontrol_cadastro", "rj", "BD01_CREFs"),
        ("CREF_RS_SCF", "efcontrol_cadastro", "rs", "BD01_CREFs"),
        ("CREF_SC_SCF", "efcontrol_cadastro", "sc", "BD01_CREFs"),
        ("CREF_SP_SCF", "efcontrol_cadastro", "sp", "BD01_CREFs"),
        ("CREF_CE_SCF", "efcontrol_cadastro", "ce", "BD01_CREFs"),
        ("CREF_MG_SCF", "efcontrol_cadastro", "mg", "BD01_CREFs"),
        ("CREF_DF_SCF", "efcontrol_cadastro", "df", "BD01_CREFs"),
        ("CREF_AM_SCF", "efcontrol_cadastro", "am", "BD01_CREFs"),
        ("CREF_PR_SCF", "efcontrol_cadastro", "pr", "BD01_CREFs"),
        ("CREF_PB_SCF", "efcontrol_cadastro", "pb", "BD01_CREFs"),
        ("CREF_MS_SCF", "efcontrol_cadastro", "ms", "BD01_CREFs"),
        ("CREF_PE_SCF", "efcontrol_cadastro", "pe", "BD01_CREFs"),
        ("CREF_BA_SCF", "efcontrol_cadastro", "ba", "BD01_CREFs"),
        ("CREF_GO_SCF", "efcontrol_cadastro", "go", "BD01_CREFs"),
        ("CREF_PI_SCF", "efcontrol_cadastro", "pi", "BD01_CREFs"),
        ("CREF_RN_SCF", "efcontrol_cadastro", "rn", "BD01_CREFs"),
        ("CREF_MT_SCF", "efcontrol_cadastro", "mt", "BD01_CREFs"),
        ("CREF_PA_SCF", "efcontrol_cadastro", "pa", "BD01_CREFs"),
        ("CREF_AL_SCF", "efcontrol_cadastro", "al", "BD01_CREFs"),
        ("CREF_SE_SCF", "efcontrol_cadastro", "se", "BD01_CREFs"),
        ("CREF_MA_SCF", "efcontrol_cadastro", "ma", "BD01_CREFs"),
        ("CREF_ES_SCF", "efcontrol_cadastro", "es", "BD01_CREFs"),
    ]

    for base_origem, base_destino, schema, instancia in bases_para_migrar:
        print(f"Migrando dados de {base_origem} para {base_destino}...")
        migrar(base_origem, base_destino, schema, instancia)
        print(f"Migração de {base_origem} para {base_destino} concluída.")