from statistics import mean
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
from databases import Database
import asyncio



def find_if_level_is_round(level):
    level = str ( level )
    level_is_round=False

    if "." in level:  # quick check if it is decimal
        decimal_part = level.split ( "." )[1]
        # print(f"decimal part of {mirror_level} is {decimal_part}")
        if decimal_part=="0":
            print(f"level is round")
            print ( f"decimal part of {level} is {decimal_part}" )
            level_is_round = True
            return level_is_round
        elif decimal_part=="25":
            print(f"level is round")
            print ( f"decimal part of {level} is {decimal_part}" )
            level_is_round = True
            return level_is_round
        elif decimal_part == "5":
            print ( f"level is round" )
            print ( f"decimal part of {level} is {decimal_part}" )
            level_is_round = True
            return level_is_round
        elif decimal_part == "75":
            print ( f"level is round" )
            print ( f"decimal part of {level} is {decimal_part}" )
            level_is_round = True
            return level_is_round
        else:
            print ( f"level is not round" )
            print ( f"decimal part of {level} is {decimal_part}" )
            return level_is_round


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

def get_all_time_high_from_ohlcv_table(engine_for_ohlcv_data_for_stocks,
                                      table_with_ohlcv_table):
    table_with_ohlcv_data_df = \
        pd.read_sql_query ( f'''select * from "{table_with_ohlcv_table}"''' ,
                            engine_for_ohlcv_data_for_stocks )
    print("table_with_ohlcv_data_df")
    print ( table_with_ohlcv_data_df )

    all_time_high_in_stock=table_with_ohlcv_data_df["high"].max()
    print ( "all_time_high_in_stock" )
    print ( all_time_high_in_stock )

    return all_time_high_in_stock, table_with_ohlcv_data_df

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

    return all_time_low_in_stock, table_with_ohlcv_data_df

def drop_table(table_name,engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )

def get_last_close_price_of_asset(ohlcv_table_df):
    last_close_price=ohlcv_table_df["close"].iat[-1]
    return last_close_price

def get_date_with_and_without_time_from_timestamp(timestamp):

    try:
        open_time = \
            dt.datetime.fromtimestamp ( timestamp  )
        # last_timestamp = historical_data_for_stock_ticker_df["Timestamp"].iloc[-1]
        # last_date_with_time = historical_data_for_stock_ticker_df["open_time"].iloc[-1]
        # print ( "type(last_date_with_time)\n" , type ( last_date_with_time ) )
        # print ( "last_date_with_time\n" , last_date_with_time )
        date_with_time = open_time.strftime ( "%Y/%m/%d %H:%M:%S" )
        date_without_time = date_with_time.split ( " " )
        print ( "date_with_time\n" , date_without_time[0] )
        date_without_time = date_without_time[0]
        print ( "date_without_time\n" , date_without_time )
        # date_without_time = date_without_time.replace ( "/" , "_" )
        # date_with_time = date_with_time.replace ( "/" , "_" )
        # date_with_time = date_with_time.replace ( " " , "__" )
        # date_with_time = date_with_time.replace ( ":" , "_" )
        return date_with_time,date_without_time
    except:
        return timestamp,timestamp


def get_high_of_bpu2(truncated_high_and_low_table_with_ohlcv_data_df,row_number_of_bpu1):
    # get high of bpu2
    high_of_bpu2=np.NaN
    try:
        if len ( truncated_high_and_low_table_with_ohlcv_data_df ) - 1 == row_number_of_bpu1:
            print ( "there is no bpu2" )
        else:
            high_of_bpu2 = truncated_high_and_low_table_with_ohlcv_data_df.loc[row_number_of_bpu1 + 1 , "high"]
            # print ( "high_of_bpu2" )
            # print ( high_of_bpu2 )
    except:
        traceback.print_exc ()
    return high_of_bpu2

def get_low_of_bpu2(truncated_high_and_low_table_with_ohlcv_data_df,row_number_of_bpu1):
    # get high of bpu2
    low_of_bpu2=np.NaN
    try:
        if len ( truncated_high_and_low_table_with_ohlcv_data_df ) - 1 == row_number_of_bpu1:
            print ( "there is no bpu2" )
        else:
            low_of_bpu2 = truncated_high_and_low_table_with_ohlcv_data_df.loc[row_number_of_bpu1 + 1 , "low"]
            # print ( "high_of_bpu2" )
            # print ( high_of_bpu2 )
    except:
        traceback.print_exc ()
    return low_of_bpu2

def get_timestamp_of_bpu2(truncated_high_and_low_table_with_ohlcv_data_df,row_number_of_bpu1):
    # get high of bpu2
    timestamp_bpu2=np.NaN
    try:
        if len ( truncated_high_and_low_table_with_ohlcv_data_df ) - 1 == row_number_of_bpu1:
            print ( "there is no bpu2" )
        else:
            timestamp_bpu2 = truncated_high_and_low_table_with_ohlcv_data_df.loc[row_number_of_bpu1 + 1 , "Timestamp"]
            # print ( "high_of_bpu2" )
            # print ( high_of_bpu2 )
    except:
        traceback.print_exc ()
    return timestamp_bpu2

def calculate_atr(atr_over_this_period,truncated_high_and_low_table_with_ohlcv_data_df,row_number_of_bpu1):
    # calcualte atr over 5 days before bpu2. bpu2 is not included

    atr_list = []
    for row_number_for_atr_calculation_backwards in range ( 0 , atr_over_this_period ):
        try:
            high_for_atr_calculation = truncated_high_and_low_table_with_ohlcv_data_df.loc[
                row_number_of_bpu1 - row_number_for_atr_calculation_backwards , "high"]
            low_for_atr_calculation = truncated_high_and_low_table_with_ohlcv_data_df.loc[
                row_number_of_bpu1 - row_number_for_atr_calculation_backwards , "low"]
            true_range = abs ( high_for_atr_calculation - low_for_atr_calculation )
            print("true_range")
            print(true_range)

            atr_list.append ( true_range )

        except:
            traceback.print_exc ()
    atr = mean ( atr_list )
    print ( "atr" )
    print ( atr )
    return atr

def trunc(num, digits):
    if num!=np.NaN:
        try:
            l = str(float(num)).split('.')
            digits = min(len(l[1]), digits)
            return float(l[0] + '.' + l[1][:digits])
        except:
            traceback.print_exc()
    else:
        return np.NaN
async def async_connect_to_postgres_db_without_deleting_it_first(database_name,
                                                                 list_of_tables_in_ohlcv_db):


    dialect = db_config.dialect
    driver = db_config.driver
    password = db_config.password
    user = db_config.user
    host = db_config.host
    port = db_config.port

    dummy_database = db_config.dummy_database

    database = Database ( f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database_name}")
    await database.connect()
    print (f"connection to {dummy_database} established")
    # Select all records from table.
    query = 'SELECT * FROM "TSLA"'
    ohlcv_data=await database.fetch_all(query=query)
    print("ohlcv_data for tesla")
    print ( ohlcv_data )
    await database.disconnect ()
    print ( f"disconnected from {dummy_database}" )

async def read_sql_async(stmt, con):
    loop = asyncio.get_event_loop()
    df=await loop.run_in_executor ( None , pd.read_sql , stmt , con )
    print(df)
    return df

def search_for_tickers_with_rebound_situations(db_where_ohlcv_data_for_stocks_is_stored,
                                          db_where_levels_formed_by_rebound_level_will_be,
                                               table_where_ticker_which_had_rebound_situations_from_ath_will_be ,
                                               table_where_ticker_which_had_rebound_situations_from_atl_will_be,
                                               acceptable_backlash ,
                                               atr_over_this_period
                                               ):
    engine_for_ohlcv_data_for_stocks , \
    connection_to_ohlcv_data_for_stocks = \
        connect_to_postres_db_without_deleting_it_first ( db_where_ohlcv_data_for_stocks_is_stored )

    list_of_tables_in_ohlcv_db = \
        get_list_of_tables_in_db ( engine_for_ohlcv_data_for_stocks )
    # loop=asyncio.get_event_loop()
    # tasks=[]
    # counter=0
    # for stock_name in list_of_tables_in_ohlcv_db:
    #     counter=counter+1
    #     print ( f'{stock_name} is'
    #             f' number {counter} out of {len ( list_of_tables_in_ohlcv_db )}\n' )
    #
    #     query=f'''select * from "{stock_name}"'''
    #
    #     task=loop.create_task(read_sql_async(query, connection_to_ohlcv_data_for_stocks))
    #     tasks.append(task)
    #     print("tasks_appended")
    #     # print(f"stock={stock_name}")
    #     #df1 = await read_sql_async(query, con=connection_to_ohlcv_data_for_stocks)
    # loop.run_until_complete ( asyncio.wait ( tasks ) )
    # loop.close ()
    asyncio.run(async_connect_to_postgres_db_without_deleting_it_first ( db_where_ohlcv_data_for_stocks_is_stored,list_of_tables_in_ohlcv_db ))


if __name__=="__main__":
    start_time=time.time ()
    db_where_ohlcv_data_for_stocks_is_stored="stocks_ohlcv_daily"
    count_only_round_rebound_level=False
    db_where_levels_formed_by_rebound_level_will_be="levels_formed_by_highs_and_lows_for_stocks"
    table_where_ticker_which_had_rebound_situations_from_ath_will_be = "rebound_situations_from_ath"
    table_where_ticker_which_had_rebound_situations_from_atl_will_be = "rebound_situations_from_atl"

    if count_only_round_rebound_level:
        db_where_levels_formed_by_rebound_level_will_be="round_levels_formed_by_highs_and_lows_for_stocks"
    #0.05 means 5%
    acceptable_backlash=0.05
    atr_over_this_period=5
    search_for_tickers_with_rebound_situations(
                                              db_where_ohlcv_data_for_stocks_is_stored,
                                              db_where_levels_formed_by_rebound_level_will_be,
                                              table_where_ticker_which_had_rebound_situations_from_ath_will_be,
                                                table_where_ticker_which_had_rebound_situations_from_atl_will_be,
                                                acceptable_backlash,
                                                atr_over_this_period)

    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )