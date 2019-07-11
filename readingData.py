import pandas as pd
import csv
import matplotlib.pyplot as plt
import os
import numpy as np
from mpl_toolkits.mplot3d import Axes3D
import warnings
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.types import VARCHAR
import json
import mysql.connector
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.sql import select

def load_database(year):
    dbname = 'College_Info'
    tableName = f'college_cost{year}'
    rds_connection_string = "root:leyla2009@127.0.0.1:3306"
    engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{rds_connection_string}/{dbname}', echo=False)
    engine.execute(f'CREATE DATABASE IF NOT EXISTS {dbname}')
    engine.execute(f'USE {dbname}') # select new db
    conn = engine.connect()
    engine.execute('SET SQL_SAFE_UPDATES = 0') 
    engine.execute(f'DROP TABLE IF EXISTS {tableName}')
    collegeCost.to_sql(name=tableName, con=conn,if_exists='replace')
    engine.execute(f'drop index ix_{tableName}_level_0 on {tableName}')
    engine.execute(f'ALTER TABLE {tableName} ADD PRIMARY KEY(name(50))') 
    engine.execute(f'ALTER TABLE {dbname}.{tableName} DROP `level_0`, DROP `index`')
    engine.execute('SET SQL_SAFE_UPDATES = 1')

year1 = 2018
collegeCost = pd.read_excel(f'Data/college{year1}.xls',na_values=[],keep_default_na = False, skiprows=5, skipfooter=3)
collegeCost.replace(r'^\s*$', np.nan, regex=True,inplace=True)
collegeCost = collegeCost.dropna(how='all',inplace=False)
print(collegeCost.columns)
collegeCost.drop(['2','3','5','6','7',9,10,12,13,14,15,17], axis=1,inplace=True)
collegeCost.rename(columns={'1':'name',
                          '4':'public_4Y_inState',
                          8:'public_4Y_outState',
                          11:'private_4Y',
                          16: 'public_2Y'}, 
                 inplace=True)
print(collegeCost.columns)

collegeCost['name'] = collegeCost['name'].str.replace('.','')
collegeCost['name'] = collegeCost['name'].str.strip()
collegeCost.reset_index(inplace=True)
print(collegeCost.head(15))
costjson = collegeCost.to_json(r'state.json',orient='records')
load_database(year1)

dbname = 'College_Info'
tableName = 'geo_states'
tableCost = f'college_cost{year1}'

rds_connection_string = "root:leyla2009@127.0.0.1:3306"
engine = sqlalchemy.create_engine(f'mysql+mysqlconnector://{rds_connection_string}/{dbname}', echo=True)
engine.execute(f'USE {dbname}') # select new db
conn = engine.connect()

Base = automap_base()
Base.prepare(engine, reflect=True)
GeoState = Base.classes.geo_states
CollegeCost = Base.classes.college_cost2018
session = Session(engine)

meta = MetaData(engine)
states = Table(tableName, meta, autoload=True)
statesCost = Table(tableCost, meta, autoload=True)

#json.dumps([dict(r) for r in res])

#res = {k: [float(x) for x in v] for k, v in state1.items()}
a1 = session.query(GeoState).first()
print (a1.name)
a2 = select([states]).limit(1)
state1 = conn.execute(a2)
for row in state1:
    #stateDict = {}
    for key, val in row.items():
        print('key is: ' + key + ' value is: ' + str(val))


