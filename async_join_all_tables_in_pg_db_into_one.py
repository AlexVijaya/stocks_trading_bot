import traceback
import itertools
import pandas as pd
from yahoo_fin import stock_info as si
import fetch_list_of_stock_names
import time
import datetime
import db_config
import yfinance as yf
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists
from sqlalchemy import inspect
from databases import Database
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine

def connect_to_postres_db(database:str):
    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                             isolation_level = 'AUTOCOMMIT' , echo = True )
    print ( f"{engine} created successfully" )

    # Create database if it does not exist.
    if not database_exists ( engine.url ):
        create_database ( engine.url )
        print ( f'new database created for {engine}' )
        connection=engine.connect ()
        print ( f'Connection to {engine} established after creating new database' )

    connection = engine.connect ()

    print ( f'Connection to {engine} established. Database already existed.'
            f' So no new db was created' )
    return engine , connection


def connect_to_postres_db_delete_and_recreate_it(database):
    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                             isolation_level = 'AUTOCOMMIT' ,
                             echo = False,
                             pool_pre_ping = True,
                             pool_size = 20 , max_overflow = 0,
                             connect_args={'connect_timeout': 10} )
    print ( f"{engine} created successfully" )

    # Create database if it does not exist.
    if not database_exists ( engine.url ):
        create_database ( engine.url )
        print ( f'new database created for {engine}' )
        connection=engine.connect ()
        print ( f'Connection to {engine} established after creating new database' )

    if database_exists ( engine.url ):
        print("database exists ok")

        engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{dummy_database}" ,
                                 isolation_level = 'AUTOCOMMIT' , echo = False )
        engine.execute(f'''REVOKE CONNECT ON DATABASE {database} FROM public;''')
        engine.execute ( f'''
                            ALTER DATABASE {database} allow_connections = off;
                            SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{database}';

                        ''' )
        engine.execute ( f'''DROP DATABASE {database};''' )

        engine = create_engine ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}" ,
                                 isolation_level = 'AUTOCOMMIT' , echo = False )
        create_database ( engine.url )
        print ( f'new database created for {engine}' )

    connection = engine.connect ()

    print ( f'Connection to {engine} established. Database already existed.'
            f' So no new db was created' )
    return engine , connection



def get_list_of_all_table_names_in_pg_db(database_name:str):
    engine , connection_to_stock_info = \
        connect_to_postres_db ( database_name )
    insp=inspect(engine)
    list_of_all_table_names=insp.get_table_names()

    return list_of_all_table_names


def join_all_tables_in_postgres_db_into_one(database_name:str):
    list_of_all_table_names=get_list_of_all_table_names_in_pg_db ( database_name)
    print ( "list_of_all_table_names" )
    print ( list_of_all_table_names )

    engine , connection_to_stock_info = \
        connect_to_postres_db ( database_name )


    final_df=pd.DataFrame()
    for counter,stock_table in enumerate(list_of_all_table_names):
        print ( f'{stock_table} is number {counter} '
                f'out of {len ( list_of_all_table_names )}\n' )



        stock_info_data_df = \
            pd.read_sql_query ( f'''select * from "{stock_table}"''' ,
                                connection_to_stock_info )
        list_of_dataframes = [final_df ,stock_info_data_df ]
        final_df=pd.concat(list_of_dataframes)
        print ( "final_df\n" , final_df )
    
    # engine.execute(f'''CREATE TABLE
    # "{joint_table_of_all_stock_info}"
    # AS
    # (SELECT * FROM "{list_of_all_table_names[0]}"
    # UNION
    # SELECT * FROM "{list_of_all_table_names[1]}");''')
    #
    # print(f"table {joint_table_of_all_stock_info} is created")
    joint_table_of_all_stock_info = "joint_table_of_all_stock_info"
    joint_table_of_all_stock_info_db_name = "joint_table_of_all_stock_info_db"
    engine_for_stock_info_joint_table , connection_to_stock_info_joint_table = \
        connect_to_postres_db ( joint_table_of_all_stock_info_db_name )

    final_df.to_sql ( f"{joint_table_of_all_stock_info}" ,
                     engine_for_stock_info_joint_table ,
                     if_exists = 'replace' )

    connection_to_stock_info.close()
    connection_to_stock_info_joint_table.close()
    pass
if __name__=="__main__":
    database_name="stock_info"
    join_all_tables_in_postgres_db_into_one(database_name = database_name )