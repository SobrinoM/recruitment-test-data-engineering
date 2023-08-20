#!/usr/bin/env python

import csv
import json
import sqlalchemy
import pandas as pd

### Definitions

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

### Configuration

sql_config = sqlalchemy.engine.URL.create(
    'mysql',
    username = 'codetest',
    password = 'swordfish',  # plain (unescaped) text
    host = 'database',
    database = 'codetest',
)

data_folder = '/data/'

### Load Data

# Connect to the database
mysql_engine = connect_to_sql_db_url(sql_config)

# Upsert places.csv to dwh_d_city
df_place = pd.read_csv(filepath_or_buffer = data_folder + 'places.csv', delimiter = ',', header = 'infer')
df_place = df_place.drop_duplicates(['city', 'county', 'country'])
df_place.to_sql(name = 'stg_d_city', con = mysql_engine, if_exists = 'replace') 
call_sql_stored_procedure(mysql_engine, 'spUpsert_dwh_d_city')

# Upsert names from people.csv to dwh_d_full_name
df_name = pd.read_csv(filepath_or_buffer = data_folder + 'people.csv', delimiter = ',', header = 'infer')
df_name = df_name[['given_name', 'family_name']].drop_duplicates(['given_name', 'family_name'])
df_name.to_sql(name = 'stg_d_full_name', con = mysql_engine, if_exists = 'replace') 
call_sql_stored_procedure(mysql_engine, 'spUpsert_dwh_d_full_name')


# Load natility numbers from people.csv to dwh_f_l1_natality
df_natality = pd.read_csv(filepath_or_buffer = data_folder + 'people.csv', delimiter = ',', header = 'infer')
# Generate FKs
df_natality['fk_date_of_birth'] = df_natality['date_of_birth'].str.replace('-','').astype('int')
# !!! people.csv only reference the city of birth. This can cause duplicates
# Todo: Add check ensure no duplicates are added.
df_place_map = pd.read_sql('select pk_city, city from dwh_d_city', mysql_engine) 
df_name_map = pd.read_sql('select pk_full_name, given_name, family_name from dwh_d_full_name', mysql_engine)
df_natality = df_natality.merge(df_place_map, left_on='place_of_birth', right_on='city')
df_natality = df_natality.merge(df_name_map, left_on=['given_name', 'family_name'], right_on=['given_name', 'family_name'])
df_natality = df_natality.rename(columns={'pk_city': 'fk_city', 'pk_full_name': 'fk_full_name'})
df_natality = df_natality[['fk_date_of_birth', 'fk_full_name', 'fk_city']]
# Write to SQL
df_natality.to_sql(name = 'dwh_f_l1_natality', con = mysql_engine, if_exists = 'append', index = False) 

