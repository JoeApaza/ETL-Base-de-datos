# Logger
import logging
import requests
import oracledb
import polars as pl
import os
from dotenv import load_dotenv



def get_connection(user_db,password_db,dsn_db):
    logging.info(f'Iniciando proceso de conexion a la base de datos {dsn_db}')
    try:
        conexion= oracledb.connect(
            user=user_db,
            password=password_db,
            dsn=dsn_db
        )
        logging.info(f'Conexion exitosa a la base de datos {dsn_db}')
        return conexion
    except Exception as ex:
        logging.error(ex)


def close_connection_db(conexion):
    logging.info('Iniciando proceso para cerrar conexion a base de datos')
    try:
        cierre_conexion= conexion.close()
        logging.info('Se cerro conexion de manera exitosa')
        return cierre_conexion
    except Exception as ex:
        logging.error(ex)


def read_database_db(sql_query,source_connection):
        logging.info('Iniciando proceso para guardar la Deuda_Origen en un dataframe')
        df_polars = pl.read_database(sql_query, source_connection,batch_size=1000,schema_overrides=({
        "CUENTA":pl.Utf8,
        "CUST_ACCOUNT_ID":pl.Utf8,
        "CUSTOMER_ID":pl.Utf8,
        "CUENTA_LARGA":pl.Utf8,
        "DOC_IDENTIDAD":pl.Utf8,
        "PARTY_NAME":pl.Utf8,
        "CICLO":pl.Utf8,
        "CICLO2":pl.Utf8,
        "PARTY_TYPE":pl.Utf8,
        "GRUPO_FAC":pl.Utf8,
        "ORIGEN":pl.Utf8,
        "CUSTOMER_TRX_ID":pl.Utf8,
        "NRO_DOC":pl.Utf8,
        "MONEDA":pl.Utf8,
        "MONTO_FAC":pl.Float64,
        "MONTO_FAC_SOLES":pl.Float64,
        "CAMBIO":pl.Float64,
        "SALDO":pl.Float64,
        "SALDO_SOLES":pl.Float64,
        "RECLAMO":pl.Float64,
        "RECLAMO_SOLES":pl.Float64,
        "OBS":pl.Utf8,
        "DIAS":pl.Utf8,
        "TRAMO_EMISION":pl.Utf8,
        "TRAMO_VENCIMIENTO":pl.Utf8,
        "DIA":pl.Utf8,
        "MES":pl.Utf8,
        "ANO":pl.Utf8,
        "ESTADO_DOCUMENTO":pl.Utf8,
        "ESTADO_CONTABLE":pl.Utf8,
        "ESTADO_CUENTA":pl.Utf8,
        "TIPO_EMPRESA":pl.Utf8,
        "COMMENTS":pl.Utf8,
        "START_DATE":pl.Utf8,
        "PAYMENT_METHOD":pl.Utf8,
        "CLASS":pl.Utf8,
        "NAME":pl.Utf8,
        "DESCRIPTION":pl.Utf8,
        "TYPE":pl.Utf8,
        "CREATION_DATE":pl.Utf8,
        "IDFAC":pl.Utf8,
        "INTERFAZ":pl.Utf8,
        "GLOBAL_ATTRIBUTE13":pl.Utf8,
        "IMEI":pl.Utf8,
        "FLAG_ONE":pl.Utf8
        }))
        logging.info('Se guardo la deuda OAC en un dataframe')
        return df_polars
    
    

#Insertar_Query = './scripts/Consulta_Insertar_Datos.sql'


def truncate_table_db(target_connection,tabla):
    logging.info('Se inicia el proceso de limpiar la tabla(truncate)')
    cursor = target_connection.cursor()
    logging.info(f'Se creo un cursor para ejecutar comandos SQL')
    cursor.execute(f"TRUNCATE TABLE {tabla}")
    logging.info(f'Se ejecuto el comando truncate a la  {tabla}')
    target_connection.commit()
    logging.info(f'Se confirma dichos cambios a la {tabla}')
    cursor.close
    logging.info('Se cierra el cursor')
   

def Insert_dataframe_db(target_connection,df_polars,Insertar_Query):
    logging.info('Se inicia el proceso de ingresar dataframe a la tabla DEUDA_CORPORATIVO')
    target_cursor = target_connection.cursor()
    logging.info('Se crea el cursor')
    datos_insertar = [tuple(row) for row in df_polars.to_numpy()]
    logging.info('Se convierte en tuplas el dataframe')
    start_pos = 0
    batch_size = 15000
    all_data = datos_insertar
    while start_pos < len(all_data):
        data = all_data[start_pos:start_pos + batch_size]
        start_pos += batch_size
        target_cursor.executemany(Insertar_Query, data)
    logging.info('Se ingreso dataframe en tabla DEUDA_CORPORATIVO')
    target_connection.commit()
    logging.info('Se confirma dichos cambios a la tabla DEUDA_CORPORATIVO')
    target_cursor.close
    logging.info('Se cierra el cursor')



def ejecutar_consultas(archivo_sql, conexion):
    logging.info('Se inicia la funcion de ejecutar consultas largas')
    try:
        
        with open(archivo_sql, 'r') as archivo:
            consultas_sql = archivo.read().split(';')
        logging.info('Se abrio y se ha leido archivo sql')
        cursor = conexion.cursor()
        logging.info('Se crea cursor')
        for consulta in consultas_sql:
            if consulta.strip():  # Para evitar consultas vacías al final del archivo
                cursor.execute(consulta)
        logging.info('Se ejecuto toda la consulta del archivo sql')
        conexion.commit()
        logging.info('Se confirma dichos cambios a la tabla')
        cursor.close()
        logging.info('Se cierra el cursor')
 
    except Exception as e:
        logging.error(e)
 

  
def leer_sql(archivo_sql):
    logging.info('Se inicia la funcion de leer contenido de archivo sql')
    with open(archivo_sql, 'r') as archivo:
        x=archivo.read()
        logging.info('Se ha leido todo el contenido del archivo sql')
        return x
    
  