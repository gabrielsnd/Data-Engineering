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

#Transformación a DataFrame + formateo del indice a fecha

df_monedas = pd.DataFrame(monedas_latam)
df_monedas['fecha'] = df_monedas.index
df_monedas['fecha'] = pd.to_datetime(df_monedas['fecha'], format='%Y%m%d')


# Generar una columna de id automático
df_monedas['Id'] = pd.Series(range(len(df_monedas)))

#print(df_monedas)


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
        CREATE TABLE IF NOT EXISTS gabrielsnd92_coderhouse.FX_LATAM
        (
	    id VARCHAR(50) primary key  
	    , fecha DATE   
      , CLP_USD DECIMAL(10,4)
      , ARS_USD DECIMAL(10,4)
      , BOL_USD DECIMAL(10,4)
      , BRL_USD DECIMAL(10,4)
      , COP_USD DECIMAL(10,4)
      , PYG_USD DECIMAL(10,4)
      , PEN_USD DECIMAL(10,4)
      , VEB_USD DECIMAL(10,4)
      , MXN_USD DECIMAL(10,4)
      , CRC_USD DECIMAL(10,4)
      )
    """)
    conn.commit()


# Carga de datos en Redshift
from psycopg2.extras import execute_values
with conn.cursor() as cur:
    execute_values(
        cur,
        '''
        INSERT INTO FX_LATAM (Id, fecha, 
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
