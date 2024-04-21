import logging
import polars as pl
import numpy as np
import os
from dotenv import load_dotenv
from src.database.db_oracle import Insert_dataframe_db,close_connection_db,read_database_db,ejecutar_consultas,leer_sql,get_connection
from src.routes.Rutas import Creacion_tabla,Deuda_Origen,Insertar_Tabla,Tabla_Deuda_Destino,Nuevos_Campos

logging.basicConfig(format="%(asctime)s::%(levelname)s::%(message)s",   
                    datefmt="%d-%m-%Y %H:%M:%S",    
                    level=10,   
                    filename='./src/utils/log/app.log',filemode='w')

load_dotenv()


source_connection=get_connection(os.getenv('USER_ORIGEN'),os.getenv('PASSWORD_ORIGEN'),os.getenv('DNS_ORIGEN'))
target_connection=get_connection(os.getenv('USER_DESTINO'),os.getenv('PASSWORD_DESTINO'),os.getenv('DNS_DESTINO'))

ejecutar_consultas(Creacion_tabla, target_connection)
df_polars=read_database_db(leer_sql(Deuda_Origen),source_connection)
Insert_dataframe_db(target_connection,df_polars,leer_sql(Insertar_Tabla))

ejecutar_consultas(Nuevos_Campos, target_connection)
close_connection_db(source_connection)
close_connection_db(target_connection) 

