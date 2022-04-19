# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import pymysql
from getpass import getpass

# Configuración de acceso a base de datos

print('Usuario')
user = input('... ')

password = getpass()

# Selección de Negocio y FY. El FY pueden colocarse otros años siempre y cuando se cuide que están creadas las tablas en base de datos

print('GID (minúsculas)')
gid = input('... ')

print('Elige Negocio [LP] [CP] [BP]')
negocio = input('... ')

print('Elige FY [18] [19] [20]')
fy = input('... ')

# Creación de variables para acceso a base de datos
try:
    db_connection_str = 'mysql+pymysql://{user}:{password}@127.0.0.1:3306/si_{negocio}'.format(user = user, password = password, negocio = negocio.lower())
    db_connection = create_engine(db_connection_str)
except Exception as e:
    print("Falla en conexión de base de datos")
    print(e)
    print(type(e).__name__)

periodos = ['P1', 'P2', 'P3', 'P4', 'P5', 'P6',
            'P7', 'P8', 'P9', 'P10', 'P11', 'P12']

# Reiniciar valores de la tabla de acuerdo al negocio y fy elegido

sql = "DELETE FROM copa_{negocio}_{fy};".format(negocio = negocio.lower(), fy = fy)
result = db_connection.execute(sql)
sql = "ALTER TABLE copa_{negocio}_{fy} AUTO_INCREMENT = 1;".format(negocio = negocio.lower(), fy = fy)
result = db_connection.execute(sql)

# Recorre los documentos guardados en la dirección definida. Deben tener el formato de nombre: .../[negocio] [fy]/[negocio] [periodo].XLSX

for periodo in periodos:
    documento = "C:/UserData/{gid}/CP-LP-BP (RODOLFO CELAYA AVILA)/{negocio} {fy}/{negocio} {periodo}.XLSX".format(negocio = negocio, periodo = periodo, fy = fy, gid = gid)
    try:
        df = pd.read_excel(documento)
    except:
        break
    
    print("Cargando {negocio}{fy} {periodo} ...".format(negocio = negocio, fy = fy, periodo = periodo))
    df.rename(columns = {
            'Sdoc Nr' : 'Sdoc_Nr',
            'SoldtoPty Nm' : 'SoldtoPty_Nm',
            'GBK/GCK' : 'GCK',
            'Prtnr PC' : 'Prtnr_PC',
            'Matnr Sitm' : 'Matnr_Sitm',
            'Matnr Desc' : 'Matnr_Desc',
            'Tr Partnr' : 'Tr_Partnr',
            'Cust PO No Sitm' : 'Cust_PO_No_Sitm',
            'Partner Depth' : 'Partner_Depth',
            'Sold to Party' : 'Sold_to_Party',
            'Total New Ord/GG1101' : 'Total_New_Ord',
            'End Orders on hand' : 'End_Orders_on_hand',
            'Revenue/GG1201' : 'Revenue',
            'Sales Margin/GG1401' : 'Sales_Margin',
            'GM in Ord On Hand' : 'GM_in_Ord_On_Hand',
            'REV Qty' : 'REV_Qty',
            'NO Qty' : 'NO_Qty',
            'PCK /FGK' : 'PCK'
        }, inplace=True)
        
    df = df.drop(df.index[-1])  # Borra última fila
    try:
        df.drop('Prctr', axis = 1, inplace=True)    # Borra columnas que no se utiliza
    except:
        pass
    
    df.to_sql('copa_{negocio}_{fy}'.format(negocio = negocio.lower(), fy = fy), con=db_connection, if_exists='append', index=False)

# Se llama el procedimiento cargado en base de datos que se encarga de generar la tabla resumida para tableau

print("Actualizando base de datos para Tableau")

connection = db_connection.raw_connection()
try:
    cursor = connection.cursor()
    cursor.callproc("si_{}.createTable{}{}".format(negocio.lower(), negocio, fy), [])
    results = list(cursor.fetchall())
    cursor.close()
    connection.commit()
finally:
    connection.close()

finish = input("Proceso Terminado. Pulsa Enter para salir")

