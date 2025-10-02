import snowflake.connector
import json


def get_connection():
    private_key_path = "/algar/rsa_key_algaretl.der"

    with open(private_key_path, "rb") as key_file:
        private_key = key_file.read()

    return snowflake.connector.connect(
        user='ALGARETL@ALGAR.COM.BR',
        account='a6979572936171-ALGARPROD1',
        private_key=private_key,
        warehouse='DEV_WH',
        database='DWDEV',
        schema='DWADM',
        role='SNOWFLAKE_DEV_ALGAR_ETL'
    )



def apply_rules(conn, id, in_vars, data, fields ):
    c1   = conn.cursor()
    c1.execute( f"select id, name, code, table_out, table_fields, nvl(table_data,table_fields)  from hugoa.gar_risco_controle where id_gar_risco = { id } " )  
    rules       = c1.fetchall()
    G           = {}
    if in_vars:
        exec( in_vars, G )

    for id_gar_risco_controle, name, code, table_out, table_fields,table_data in rules:
        print("Executing rule:", name )

        sql_bind    = ",".join([ "%s" for _ in table_fields.split(",") ])
        sql_insert  = f"insert into { table_out }({ table_fields }) values ( { sql_bind })"
        data_insert = []

        for x in data:
            I                          = dict( zip( fields, x ) )
            I['ID_GAR_RISCO_CONTROLE'] = id_gar_risco_controle
            G["I"]                     = I

            exec( code, G )
            if G['ret']:
                data_bind = eval( f"({ ",".join([ f"I['{ x.upper() }']" for x in table_data.split(",")]) })" )              
                data_insert.append( data_bind )

                if len(data_insert) >= 5000:
                    c1.executemany( sql_insert, data_insert )
                    data_insert.clear()

        if len(data_insert) > 0:    
            c1.executemany( sql_insert, data_insert )

def run():
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("select id, in_vars, in_query, in_query_after from hugoa.gar_risco where status = 1") 


    for id, in_vars, in_query, in_query_after in cursor.fetchall():
        cursor.execute(in_query)
        fields = [  cursor.description[i][0] for i in range(0, len(cursor.description))  ]
        data   = cursor.fetchall()
        apply_rules( conn, id, in_vars, data, fields )
        if in_query_after:
            cursor.execute(in_query_after)


if __name__ == "__main__":
    run()