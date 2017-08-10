import pandas as pd


import pyodbc


conn_str = (
    r'DRIVER={CData ODBC Driver for Access};'
    r'Data Source=ClientDatabaseStructure.mdb;'
    r'CHARSET=UTF8')
cnxn = pyodbc.connect(conn_str, ansi=True)

crsr = cnxn.cursor()
for row in crsr.tables():
    print(row.table_name)

crsr.execute('select * from PackItem')
for row in crsr.fetchall():
    print(row)