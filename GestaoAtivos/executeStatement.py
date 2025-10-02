import snowflake.connector
import json
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

sql = """
SELECT tecnico,
       cpf_tecnico,
       nome_empresa_tecnico,
       supervisor_tecnico,
       cordenador_tecnico,
       grupo_material,
       (SELECT to_char(max(IGNORE_MATERIAL_KIT_EXPIRATION),'DD/MM/YYYY')  FROM DWADM.AS_SOM_TECHNICIAN_ODS where registration = cpf_tecnico) dt_max_autorizacao,
       ( SELECT nvl(
           MAX(qtd_max_bxa), - 1
         )
           FROM hugoa.AUX_QTD_MATERIAIS
          WHERE dsc_grp_mat_bxa = grupo_material
       ) AS qtd_max_bxa,
       SUM(quantidade) quantidade
  FROM ( SELECT ( SELECT MAX(NX.NAME)
                         FROM DWADM.AS_SOM_TECHNICIAN_ODS NX
                        WHERE REGISTRATION = ET.NUM_CPF_TEC
                ) tecnico,
                ET.NUM_CPF_TEC cpf_tecnico,
                ET.NOM_EMP_TEC nome_empresa_tecnico,
                TD.SUPERVISOR supervisor_tecnico,
                TD.COORDINATOR cordenador_tecnico,
                ET.QTD quantidade,
                ET.DSC_GRP_MAT grupo_material
         FROM DWADM.D_TEC_SERV_ORD_ESTOQUE_TECNICO ET
        INNER JOIN DWADM.D_TEC_SERVICE_ORDER TEC ON ( ET.SRK_TEC_SRV_ORD = TEC.TEC_SERVICE_ORDER_KEY
          AND TEC.RECORD_STATUS = 1 )
         LEFT JOIN DWADM.AS_SOM_TECHNICIAN_DEPARTMENT_ODS TD ON ( TD.CPF = ET.NUM_CPF_TEC )
        WHERE 1 = 1
          AND ET.IDT_RGT_ATU = 1
       ) GROUP BY ALL

"""

sql2 = """delete from hugoa.gar_api_02 where id not in 
(
  select min(id) from hugoa.gar_api_02 group by grupo_material
)
"""
cursor = conn.cursor()
cursor.execute("""update hugoa.gar_risco
                    set in_query = %s
                  where id = 1  """, (sql,) )   
