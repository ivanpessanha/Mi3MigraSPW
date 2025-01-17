import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'utils')))

from utils.functions_pgsql import get_short_tables, create_pgsql_tables
from dbs import get_sql_server_connection, get_postgresql_connection, get_freetds_connection

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
        ("CREF_RJ_SCF", "efcontrol_arrecadacao", "rj", "BD01_CREFs"),
        ("CREF_RS_SCF", "efcontrol_arrecadacao", "rs", "BD01_CREFs"),
        ("CREF_SC_SCF", "efcontrol_arrecadacao", "sc", "BD01_CREFs"),
        ("CREF_SP_SCF", "efcontrol_arrecadacao", "sp", "BD01_CREFs"),
        ("CREF_CE_SCF", "efcontrol_arrecadacao", "ce", "BD01_CREFs"),
        ("CREF_MG_SCF", "efcontrol_arrecadacao", "mg", "BD01_CREFs"),
        ("CREF_DF_SCF", "efcontrol_arrecadacao", "df", "BD01_CREFs"),
        ("CREF_AM_SCF", "efcontrol_arrecadacao", "am", "BD01_CREFs"),
        ("CREF_PR_SCF", "efcontrol_arrecadacao", "pr", "BD01_CREFs"),
        ("CREF_PB_SCF", "efcontrol_arrecadacao", "pb", "BD01_CREFs"),
        ("CREF_MS_SCF", "efcontrol_arrecadacao", "ms", "BD01_CREFs"),
        ("CREF_PE_SCF", "efcontrol_arrecadacao", "pe", "BD01_CREFs"),
        ("CREF_BA_SCF", "efcontrol_arrecadacao", "ba", "BD01_CREFs"),
        ("CREF_GO_SCF", "efcontrol_arrecadacao", "go", "BD01_CREFs"),
        ("CREF_PI_SCF", "efcontrol_arrecadacao", "pi", "BD01_CREFs"),
        ("CREF_RN_SCF", "efcontrol_arrecadacao", "rn", "BD01_CREFs"),
        ("CREF_MT_SCF", "efcontrol_arrecadacao", "mt", "BD01_CREFs"),
        ("CREF_PA_SCF", "efcontrol_arrecadacao", "pa", "BD01_CREFs"),
        ("CREF_AL_SCF", "efcontrol_arrecadacao", "al", "BD01_CREFs"),
        ("CREF_SE_SCF", "efcontrol_arrecadacao", "se", "BD01_CREFs"),
        ("CREF_MA_SCF", "efcontrol_arrecadacao", "ma", "BD01_CREFs"),
        ("CREF_ES_SCF", "efcontrol_arrecadacao", "es", "BD01_CREFs"),
    ]

    for base_origem, base_destino, schema, instancia in bases_para_migrar:
        print(f"Migrando dados de {base_origem} para {base_destino}...")
        migrar(base_origem, base_destino, schema, instancia)
        print(f"Migração de {base_origem} para {base_destino} concluída.")