#!/usr/bin/env python

import csv
import json
import sqlalchemy
import pandas as pd


def connect_to_sql_db_url(sql_config):
    """Connect to a SQL server instance with SQLAlchemy & return engine
    URL config: https://docs.sqlalchemy.org/en/20/core/engines.html#creating-urls-programmatically
    Args:
        sql_config (sqlachemy.engine.url.URL): URL constructed with SQLAchemy
    """
    engine = sqlalchemy.create_engine(sql_config)
    try:
        connection = engine.connect()
        print('----------- SQL Connection successful ! -----------')
    except Exception as e:
        print('\n----------- SQL Connection failed ! ERROR : ', e)
        raise
        
    return engine

def call_sql_stored_procedure(sql_engine, sp_name):
    """ Use a SqlAlchemy engine to trigger a stored procedure.

    Args:
        sql_engine (sqlalchemy.engine.base.Engine): connection to a SQL DB
        sp_name (string): name of the stored procedure
    """
    try:
        connection = sql_engine.raw_connection()
        cursor = connection.cursor()
        cursor.callproc(sp_name)
        cursor.close()
        connection.commit()
        
    except Exception as e:
        print('\n----------- Stored Procedure: ', sp_name, ' ! ERROR : ', e)
        raise

# Configuration
sql_config = sqlalchemy.engine.URL.create(
    "mysql",
    username="codetest",
    password="swordfish",  # plain (unescaped) text
    host="database",
    database="codetest",
)

data_folder = "/data/"

# Connect to the database
mysql_engine = connect_to_sql_db_url(sql_config)

### Upsert places.csv to dwh_d_city
df_place = pd.read_csv(filepath_or_buffer = data_folder + 'places.csv', delimiter = ',', header='infer')
df_place = df_place.drop_duplicates(['city', 'county', 'country'])
df_place.to_sql(name = 'stg_d_city', con = mysql_engine, if_exists = 'replace') 
call_sql_stored_procedure(mysql_engine, 'spUpsert_dwh_d_city')