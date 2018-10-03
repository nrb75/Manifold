#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 18 12:39:04 2018

@author: natalie
"""

import numpy as np
import pandas as pd
#import odo
import dask.dataframe as dd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
#data=pd.read_csv('/home/natalie/Documents/Manifold/data/CIDDS-002/traffic/week1.csv', low_memory=False, usecols=[0,1,2,3,4,5,6,7,8])

#['Date', 'Duration', 'Proto', 'Src_IP', 'Src_Pt', 'Dst_IP' ,'Dst_Pt', 'Packets', 'Bytes']

myfile='/home/natalie/Documents/Manifold/data/CIDDS-002/traffic/week1.csv'
#odo(myfile, 'postgresql://hostname::tablename')
df=dd.read_csv(myfile, dtype={'Dst Pt': 'float64', 'Packets' :'float64'}, low_memory=False)
df=df.iloc[:, 0:9]
#we only want to keep certain variables, remove last

#change column names
df.columns=['Date', 'Duration', 'Proto', 'Src_IP', 'Src_Pt', 'Dst_IP', 'Dst_Pt', 'Packets', 'Bytes']
print(df.head())

divisor=int(len(df)/9)
df1=df[:divisor]
df2=df[divisor:divisor*2]
df3=df[divisor*2:divisor*3]
df4=df[divisor*3:divisor*4]
df5=df[divisor*4:]
#convert dask back to pandas df
df_pd =df1.compute()







#now setup SQL database

# Define a database name (we're using a dataset on births, so we'll call it birth_db)
# Set your postgres username/password, and connection specifics
username = 'postgres'
password = 'Natbou42'     # change this
host     = 'localhost'
port     = '5432'            # default port that postgres listens on
db_name  = 'manifold'

## 'engine' is a connection to a database
## Here, we're using postgres, but sqlalchemy can connect to other things too.
engine = create_engine( 'postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, db_name) )
print(engine.url)

## create a database (if it doesn't exist)
if not database_exists(engine.url):
    create_database(engine.url)
print(database_exists(engine.url))


## insert data into database from Python (proof of concept - this won't be useful for big data, of course)
#df.to_sql('manifold_table', engine, if_exists='replace')