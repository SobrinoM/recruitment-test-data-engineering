#!/usr/bin/env python

import sqlalchemy
import sys

### Definitions


def connect_to_sql_db_url(sql_url):
    """Connect to a SQL server instance with SQLAlchemy & return engine
    URL config: https://docs.sqlalchemy.org/en/20/core/engines.html#creating-urls-programmatically
    Args:
        sql_url: database URL
    """
    engine = sqlalchemy.create_engine(sql_url)
    try:
        connection = engine.connect()
    except Exception as e:
        print("\n----------- SQL Connection failed ! ERROR : ", e)
        sys.exit(1)
    return engine


def call_sql_stored_procedure(sql_engine, sp_name):
    """Use a SqlAlchemy engine to trigger a stored procedure.

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
        print("\n----------- Stored Procedure: ", sp_name, " ! ERROR : ", e)
        sys.exit(1)
