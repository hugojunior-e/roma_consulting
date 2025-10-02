import snowflake.connector
import json
from typing import Union
from fastapi import FastAPI, Query

private_key_path = "/algar/rsa_key_algaretl.der"

with open(private_key_path, "rb") as key_file:
    private_key = key_file.read()

conn = snowflake.connector.connect(
    user='ALGARETL@ALGAR.COM.BR',
    account='a6979572936171-ALGARPROD1',
    private_key=private_key,
    warehouse='DEV_WH',
    database='DWDEV',
    schema='DWADM',
    role='SNOWFLAKE_DEV_ALGAR_ETL'
)


app = FastAPI()

##----------------------------------------------------------------
## fastapi dev api.py --port 9191
##----------------------------------------------------------------

def get_data(tablename,page,page_size):
    offset = (page - 1) * page_size
    cursor = conn.cursor()

    sql = f"SELECT * FROM {tablename} ORDER BY id   LIMIT %s OFFSET %s"
    cursor.execute(sql, (page_size, offset))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    cursor.execute( f"SELECT COUNT(*) FROM {tablename}")
    total = cursor.fetchone()[0]

    return {
        "page": page,
        "page_size": page_size,
        "total": total,
        "data": [dict(zip(columns, row)) for row in rows]
    }



##----------------------------------------------------------------
##
##----------------------------------------------------------------


@app.get("/RetiradaMaterialQuantidadeMaxima")
def Retirada_Material_Quantidade_Maxima(
    page: int = Query(1, ge=1),             # página atual (default=1)
    page_size: int = Query(10, ge=1, le=100)  # qtd registros por página
):
    return get_data("hugoa.gar_api_01",page,page_size)


##----------------------------------------------------------------
##
##----------------------------------------------------------------


@app.get("/ProdutosQuantidadeMaximaNaoCadastrados")
def Produtos_Quantidade_Maxima_Nao_Cadastrados(
    page: int = Query(1, ge=1),             # página atual (default=1)
    page_size: int = Query(10, ge=1, le=100)  # qtd registros por página
):
    return get_data("hugoa.gar_api_02",page,page_size)
