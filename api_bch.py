import bcchapi #API del Banco Central de Chile
import pandas as pd
from datetime import datetime
import psycopg2

# Credenciales de la API del Banco Central de Chile
siete = bcchapi.Siete(file="credenciales_bch.txt")

# Obtener la fecha de hoy
fecha_hoy = datetime.now().strftime('%Y-%m-%d')

# [Dataframe con paridad USD de 10 monedas de LATAM con
#  info del banco central de Chile desde el año 2004 a la fecha]

#[ABREVIATURA DE MONEDAS]
# CLP: Peso Chileno
# ARS: Peso Argentino
# BRL: Real Brasileño
# COP: Peso Colombiano
# PYG: Guaraní Paraguayo
# PEN: Sol Peruano
# VEB: Bolivar Venezolano
# MXN: Peso Mexicano
# CRC: Colon Costarricense

monedas_latam = siete.cuadro(
  series=["F073.TCO.PRE.Z.D","F072.ARS.USD.N.O.D","F072.BOL.USD.N.O.D","F072.BRL.USD.N.O.D","F072.COP.USD.N.O.D",
          "F072.PYG.USD.N.O.D","F072.PEN.USD.N.O.D","F072.VEB.USD.N.O.D","F072.MXN.USD.N.O.D","F072.CRC.USD.N.O.D"],
  nombres = ["CLP_USD","ARS_USD","BOL_USD","BRL_USD","COP_USD","PYG_USD","PEN_USD","VEB_USD","MXN_USD",
             "CRC_USD"],
  desde="2004-01-01",
  hasta=fecha_hoy,
  frecuencia="D",
  observado="last"
  )

#Transformación a DataFrame + formateo del index + borra na

df_monedas = pd.DataFrame(monedas_latam)
df_monedas = df_monedas.dropna(subset=['CLP_USD', 'ARS_USD', 'BOL_USD', 'BRL_USD', 'COP_USD', 'PYG_USD', 'PEN_USD', 'VEB_USD', 'MXN_USD', 'CRC_USD'])
df_monedas.reset_index(inplace=True)


# Conexión a Amazon Redshift

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base="data-engineer-database"
user="gabrielsnd92_coderhouse"
with open("C:/Users/gabri/OneDrive/Escritorio/Proyectos Python/Proyecto Data Engineering/pwd.txt",'r') as f:
    pwd= f.read()
try:
    conn = psycopg2.connect(
        host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
        dbname=data_base,
        user=user,
        password=pwd,
        port='5439'
    )
    print("conexión exitosa")
    
except Exception as e:
    print("Conexión fallida")
    print(e)

# Creación de la tabla en caso de no existir

with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gabrielsnd92_coderhouse.FX_LATAM_v5
        (
            fecha DATE PRIMARY KEY   
      , CLP_USD FLOAT
      , ARS_USD FLOAT
      , BOL_USD FLOAT
      , BRL_USD FLOAT
      , COP_USD FLOAT
      , PYG_USD FLOAT
      , PEN_USD FLOAT
      , VEB_USD FLOAT
      , MXN_USD FLOAT
      , CRC_USD FLOAT
      )
    """)
    conn.commit()


# Carga de datos en Redshift
from psycopg2.extras import execute_values
with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO FX_LATAM_v5 (fecha,
        CLP_USD,
        ARS_USD,
        BOL_USD,
        BRL_USD,
        COP_USD,
        PYG_USD,
        PEN_USD,
        VEB_USD,
        MXN_USD,
        CRC_USD)
        VALUES %s''',
        [tuple(row) for row in df_monedas.values],
        page_size=len(df_monedas)
    )
    conn.commit()


cur.close()
conn.close()
