#!/usr/bin/env python

import pandas as pd
from utilities import (
    connect_to_sql_db_url, 
    call_sql_stored_procedure, 
    sql_records_to_json_file
    )
import config as cfg


mysql_engine = connect_to_sql_db_url(cfg.sql_url)

### Upsert places.csv to dwh_d_city
df_place = pd.read_csv(cfg.data_folder + "places.csv")
df_place = df_place.drop_duplicates(["city", "county", "country"])
df_place.to_sql("stg_d_city", mysql_engine, if_exists="replace")
call_sql_stored_procedure(mysql_engine, "spUpsert_dwh_d_city")

### Upsert names from people.csv to dwh_d_full_name
df_name = pd.read_csv(cfg.data_folder + "people.csv")
df_name = df_name[["given_name", "family_name"]].drop_duplicates(
    ["given_name", "family_name"]
)

df_name.to_sql("stg_d_full_name", mysql_engine, if_exists="replace")
call_sql_stored_procedure(mysql_engine, "spUpsert_dwh_d_full_name")

### Load natility numbers from people.csv to dwh_f_l1_natality
df_natality = pd.read_csv(filepath_or_buffer=cfg.data_folder + "people.csv")

# Generate FKs
df_natality["fk_date_of_birth"] = (
    df_natality["date_of_birth"].str.replace("-", "").astype("int")
)

q_place_map = """
    SELECT  pk_city AS fk_city
            ,city
    FROM dwh_d_city
"""
df_place_map = pd.read_sql(q_place_map, mysql_engine)
df_natality = df_natality.merge(df_place_map, left_on="place_of_birth", right_on="city")

q_name_map = """
    SELECT  pk_full_name AS fk_full_name
            ,given_name
            ,family_name
    FROM dwh_d_full_name
"""
df_name_map = pd.read_sql(q_name_map, mysql_engine)
df_natality = df_natality.merge(df_name_map, on=["given_name", "family_name"])

# Write to SQL
df_natality = df_natality[["fk_date_of_birth", "fk_full_name", "fk_city"]]
df_natality.to_sql("dwh_f_l1_natality", mysql_engine, if_exists="append", index=False)

### Output Query to JSON File
json_file_natality = cfg.data_folder + "summary_output.json"
select_query_natality = """
SELECT country
	,count(*) AS births
FROM dwh_d_city d
LEFT JOIN dwh_f_l1_natality f ON d.pk_city = f.fk_city
GROUP BY country
"""   
sql_records_to_json_file(json_file_natality, select_query_natality, mysql_engine )