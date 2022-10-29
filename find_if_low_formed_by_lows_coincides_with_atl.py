
import pandas as pd
import os
import time
import datetime
import traceback
import datetime as dt
import tzlocal
import numpy as np
from collections import Counter
from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base


def find_if_level_is_round(level_formed_by_high_or_low):
    level_formed_by_high_or_low = str ( level_formed_by_high_or_low )
    level_formed_by_high_or_low_is_round=False

    if "." in level_formed_by_high_or_low:  # quick check if it is decimal
        decimal_part = level_formed_by_high_or_low.split ( "." )[1]
        # print(f"decimal part of {mirror_level} is {decimal_part}")
        if decimal_part=="0":
            print(f"level_formed_by_high_or_low is round")
            print ( f"decimal part of {level_formed_by_high_or_low} is {decimal_part}" )
            level_formed_by_high_or_low_is_round = True
            return level_formed_by_high_or_low_is_round
        elif decimal_part=="25":
            print(f"level_formed_by_high_or_low is round")
            print ( f"decimal part of {level_formed_by_high_or_low} is {decimal_part}" )
            level_formed_by_high_or_low_is_round = True
            return level_formed_by_high_or_low_is_round
        elif decimal_part == "5":
            print ( f"level_formed_by_high_or_low is round" )
            print ( f"decimal part of {level_formed_by_high_or_low} is {decimal_part}" )
            level_formed_by_high_or_low_is_round = True
            return level_formed_by_high_or_low_is_round
        elif decimal_part == "75":
            print ( f"level_formed_by_high_or_low is round" )
            print ( f"decimal part of {level_formed_by_high_or_low} is {decimal_part}" )
            level_formed_by_high_or_low_is_round = True
            return level_formed_by_high_or_low_is_round
        else:
            print ( f"level_formed_by_high_or_low is not round" )
            print ( f"decimal part of {level_formed_by_high_or_low} is {decimal_part}" )
            return level_formed_by_high_or_low_is_round



def connect_to_postres_db_without_deleting_it_first(database):
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

def get_list_of_tables_in_db(engine_for_ohlcv_data_for_stocks):
    '''get list of all tables in db which is given as parameter'''
    inspector=inspect(engine_for_ohlcv_data_for_stocks)
    list_of_tables_in_db=inspector.get_table_names()

    return list_of_tables_in_db

def get_all_time_low_from_ohlcv_table(engine_for_ohlcv_data_for_stocks,
                                      table_with_ohlcv_table):
    table_with_ohlcv_data_df = \
        pd.read_sql_query ( f'''select * from "{table_with_ohlcv_table}"''' ,
                            engine_for_ohlcv_data_for_stocks )
    print("table_with_ohlcv_data_df")
    print ( table_with_ohlcv_data_df )

    all_time_low_in_stock=table_with_ohlcv_data_df["low"].min()
    print ( "all_time_low_in_stock" )
    print ( all_time_low_in_stock )

    return all_time_low_in_stock


def drop_table(table_name,engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )



def find_if_level_formed_by_lows_coincides_with_atl(db_where_levels_formed_by_lows_are_stored,
                                                    table_where_levels_formed_by_lows_are_stored,
                                                    db_where_ohlcv_data_for_stocks_is_stored,
                                                    table_where_levels_formed_by_lows_which_coincided_with_atl_will_be_stored):
    """this function goes over each low level in table with low levels
    then fetches ohlcv data from db for stocks where the level formed by lows
    occurred then it checks if level formed by lows coincides with atl"""

    engine_for_ohlcv_data_for_stocks,\
    connection_to_ohlcv_data_for_stocks=\
        connect_to_postres_db_without_deleting_it_first(db_where_ohlcv_data_for_stocks_is_stored)

    engine_for_table_where_level_formed_by_lows_are_stored,\
    connection_to_table_where_level_formed_by_lows_are_stored = \
        connect_to_postres_db_without_deleting_it_first ( db_where_levels_formed_by_lows_are_stored )

    #drop table with levels formed by lows and which coincide with atl
    drop_table(table_where_levels_formed_by_lows_which_coincided_with_atl_will_be_stored,
               engine_for_table_where_level_formed_by_lows_are_stored)


    # list_of_tables_in_ohlcv_db=get_list_of_tables_in_db ( engine_for_ohlcv_data_for_stocks )
    # print ( "list_of_tables_in_ohlcv_db" )
    # print ( list_of_tables_in_ohlcv_db )

    table_with_all_low_levels_df = \
        pd.read_sql_query ( f'''select * from "{table_where_levels_formed_by_lows_are_stored}"''' ,
                            connection_to_table_where_level_formed_by_lows_are_stored )
    counter=0
    for row_of_lows in range ( 0 , len ( table_with_all_low_levels_df ) ):
        counter = counter + 1


        try:
            # print ( table_with_all_low_levels_df.loc[[row_of_lows]].to_string () )
            one_row_df = table_with_all_low_levels_df.loc[[row_of_lows]]
            stock_ticker = table_with_all_low_levels_df.loc[row_of_lows , 'ticker']
            exchange = table_with_all_low_levels_df.loc[row_of_lows , 'exchange']


            #get level formed by low from table where levels formed by lows are stored
            level_formed_by_low = table_with_all_low_levels_df.loc[row_of_lows , 'level_formed_by_low']
            # level_formed_by_low=int(level_formed_by_low)
            print ( "stock_ticker=" , stock_ticker )
            print ( "exchange=" , exchange )
            print ("level_formed_by_low=" , )

            print ( f'{stock_ticker} with low formed by low = {level_formed_by_low}is'
                    f' number {counter} out of {len ( table_with_all_low_levels_df )}\n' )

            #get table with ohlcv data from db
            table_with_ohlcv_table=stock_ticker
            all_time_low_in_stock=get_all_time_low_from_ohlcv_table ( engine_for_ohlcv_data_for_stocks ,
                                                table_with_ohlcv_table )

            if level_formed_by_low==all_time_low_in_stock:
                print (f"low formed by low = {level_formed_by_low} is "
                       f"equal to atl for stock = {stock_ticker} on exchange = {exchange}")
                one_row_df.to_sql ( table_where_levels_formed_by_lows_which_coincided_with_atl_will_be_stored ,
                                                  connection_to_table_where_level_formed_by_lows_are_stored ,
                                                  if_exists = 'append' , index = False )
                #time.sleep(1000000)





        except:
            traceback.print_exc()




if __name__=="__main__":
    start_time=time.monotonic()
    db_where_levels_formed_by_lows_are_stored="round_levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_lows_are_stored="levels_formed_by_lows"
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    table_where_levels_formed_by_lows_which_coincided_with_atl_will_be_stored="levels_formed_by_lows_which_coincide_with_atl"



    find_if_level_formed_by_lows_coincides_with_atl(db_where_levels_formed_by_lows_are_stored,
                                                    table_where_levels_formed_by_lows_are_stored,
                                                    db_where_ohlcv_data_for_stocks_is_stored,
                                                    table_where_levels_formed_by_lows_which_coincided_with_atl_will_be_stored)
    end_time = time.monotonic ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )