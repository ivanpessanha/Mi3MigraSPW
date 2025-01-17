from fastapi import FastAPI

import sys
import os

# Adicionar o diretório raiz ao sys.path
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from dbs import get_freetds_connection, get_postgresql_connection, get_sql_server_connection
import uvicorn


app = FastAPI()

base_origem = "CONFEF_SDP"
base_destino = "efcontrol_eventos"
instancia_origem = "BD02_CONFEF"

# Conectar ao SQL Server
sql_server_conn = get_sql_server_connection(base_origem, instancia_origem)
    
# Conectar ao PostgreSQL
postgresql_conn = get_postgresql_connection(base_destino)

@app.get("/tables")
async def list_tables():
    try:
        with sql_server_conn as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND LEN(TABLE_NAME) <= 7")
            tables = [row.TABLE_NAME for row in cursor.fetchall()]
        return {"tables": tables}
    except Exception as e:
        return {"error": str(e)}

uvicorn.run(app, host="127.0.0.1", port=8000)