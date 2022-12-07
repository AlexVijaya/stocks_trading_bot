import pandas as pd
import os
import time
import datetime
import traceback
import datetime as dt
import tzlocal
import numpy as np
from collections import Counter
from sqlalchemy_utils import create_database , database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from pandas import DataFrame

def find_if_level_is_round(level):
    level = str ( level )
    level_is_round = False

    if "." in level:  # quick check if it is decimal
        decimal_part = level.split ( "." )[1]
        # print(f"decimal part of {mirror_level} is {decimal_part}")
        if decimal_part == "0":
            print ( f"level is round" )
            print ( f"decimal part of {level} is {decimal_part}" )
            level_is_round = True
            return level_is_round
        elif decimal_part == "25":
            print ( f"level is round" )
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
        connection = engine.connect ()
        print ( f'Connection to {engine} established after creating new database' )

    connection = engine.connect ()

    print ( f'Connection to {engine} established. Database already existed.'
            f' So no new db was created' )
    return engine , connection


def drop_table(table_name , engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )



def find_false_breakouts_of_ath_and_insert_into_db(db_with_daily_ohlcv_stock_data,
                                                   db_where_ath_had_fb,
                                                   db_where_atl_had_fb,
                                                   table_where_stocks_with_fd_of_ath_will_be,
                                                   table_where_stocks_with_fd_of_atl_will_be):

    counter_for_tables = 0

    engine_for_stocks_ohlcv , connection_to_stocks_ohlcv = \
        connect_to_postres_db_without_deleting_it_first ( db_with_daily_ohlcv_stock_data )
    engine_for_db_where_ath_had_fb , connection_to_db_where_ath_had_fb = \
        connect_to_postres_db_without_deleting_it_first ( db_where_ath_had_fb )
    engine_for_db_where_atl_had_fb , connection_to_db_where_atl_had_fb = \
        connect_to_postres_db_without_deleting_it_first ( db_where_atl_had_fb )

    inspector = inspect ( engine_for_stocks_ohlcv )
    # print(metadata.reflect(engine_for_stocks_ohlcv))
    # print(inspector.get_table_names())
    list_of_tables_from_sql_query = inspector.get_table_names ()
    print ( "list_of_tables_from_sql_query\n" )
    print ( list_of_tables_from_sql_query )


    try:
        drop_table ( table_where_stocks_with_fd_of_ath_will_be ,
                     engine_for_db_where_ath_had_fb )
        print ( "\ntable dropped\n" )
    except Exception as e:
        print ( "cant drop table from db\n" , e )


    try:
        drop_table ( table_where_stocks_with_fd_of_atl_will_be ,
                     engine_for_db_where_atl_had_fb )
        print ( "\ntable dropped\n" )
    except Exception as e:
        print ( "cant drop table from db\n" , e )






    ####################################################################
    ##################################################################
    list_of_tables = list_of_tables_from_sql_query
    list_of_tickers_with_fb_of_ath=[]
    list_of_tickers_with_fb_of_atl = []

    print ( list_of_tables )


    for table_in_db in list_of_tables:
        counter_for_tables = counter_for_tables + 1
        try:
            list_of_numbers_of_gaps_before_high = []
            data_df = \
                pd.read_sql_query ( f'''select * from "{table_in_db}"''' ,
                                    connection_to_stocks_ohlcv )

            list_of_timestamp_plus_numbers = [f"timestamp_{number_of_timestamp}" for
                                              number_of_timestamp in
                                              range ( 1 , 51 )]
            first_four_columns_list_for_fb_of_ath_df = ['ticker' , 'exchange' ,
                                       'ath' ,
                                       'short_name']
            column_list_for_fb_of_ath_df = [*first_four_columns_list_for_fb_of_ath_df ,
                                            *list_of_timestamp_plus_numbers]


            first_four_columns_list_for_fb_of_atl_df = ['ticker' , 'exchange' ,
                                       'atl' ,
                                       'short_name']
            column_list_for_fb_of_atl_df = [*first_four_columns_list_for_fb_of_atl_df ,
                                            *list_of_timestamp_plus_numbers]


            fb_of_ath_df = pd.DataFrame (
                columns = column_list_for_fb_of_ath_df )
            fb_of_atl_df = pd.DataFrame (
                columns = column_list_for_fb_of_atl_df )



            # print ( "data_df\n" , data_df )
            print ( "---------------------------" )
            print ( f'{table_in_db} is number {counter_for_tables} out of {len ( list_of_tables )}\n' )
            # print("usdt_ohlcv_df\n",data_df )
            stock_ticker = data_df.loc[0 , 'ticker']
            exchange = data_df.loc[0 , "exchange"]
            short_name = data_df.loc[0 , "short_name"]
            data_df.reset_index()
            data_df.set_index ( "Timestamp" , inplace = True )
            data_df_without_last_day=data_df.head(len(data_df)-1)
            print ( "data_df_without_last_day\n",
                    data_df_without_last_day.tail(5).to_string() )

            all_time_high_of_df_without_last_day=\
                data_df_without_last_day["high"].max()
            all_time_low_of_df_without_last_day = \
                data_df_without_last_day["low"].min ()

            last_high=data_df["high"].iat[-1]
            print("last_high=",last_high)
            last_low = data_df["low"].iat[-1]
            print ( "last_high=" , last_high )
            last_close=data_df["close"].iat[-1]
            print ( "last_close=" , last_close )

            if last_high>all_time_high_of_df_without_last_day and \
                    last_close<all_time_high_of_df_without_last_day:
                print (f"stock ticker = {stock_ticker} had a "
                       f"false break out of ath={all_time_high_of_df_without_last_day}")
                fb_of_ath_df.loc[0,"ticker"]=stock_ticker
                fb_of_ath_df.loc[0 , "exchange"] = exchange
                fb_of_ath_df.loc[0 , "short_name"] = short_name
                fb_of_ath_df.loc[0 , "ath"] =\
                    all_time_high_of_df_without_last_day

                data_df_slice=data_df.loc[data_df["high"]==all_time_high_of_df_without_last_day]
                data_df_slice.reset_index(inplace=True)
                list_of_ath_timestamps=data_df_slice["Timestamp"].values.tolist()
                print ( "list_of_ath_timestamps" )
                print ( list_of_ath_timestamps )
                for number,timestamp_of_ath in enumerate(list_of_ath_timestamps):
                    fb_of_ath_df.loc[0 , f"timestamp_{number+1}"] = int(timestamp_of_ath)




                fb_of_ath_df.to_sql(table_where_stocks_with_fd_of_ath_will_be,
                                    engine_for_db_where_ath_had_fb,if_exists = "append")
                list_of_tickers_with_fb_of_ath.append(stock_ticker)

            if last_low<all_time_low_of_df_without_last_day and \
                    last_close>all_time_low_of_df_without_last_day:
                print (f"stock ticker = {stock_ticker} had a "
                       f"false break out of atl={all_time_low_of_df_without_last_day}")
                fb_of_atl_df.loc[0 , "ticker"] = stock_ticker
                fb_of_atl_df.loc[0 , "exchange"] = exchange
                fb_of_atl_df.loc[0 , "short_name"] = short_name
                fb_of_atl_df.loc[0 , "atl"] = \
                    all_time_low_of_df_without_last_day

                data_df_slice = data_df.loc[data_df["low"] == all_time_low_of_df_without_last_day]
                data_df_slice.reset_index ( inplace = True )
                list_of_atl_timestamps = data_df_slice["Timestamp"].values.tolist ()
                print ( "list_of_atl_timestamps" )
                print ( list_of_atl_timestamps )
                for number , timestamp_of_atl in enumerate ( list_of_atl_timestamps ):
                    fb_of_atl_df.loc[0 , f"timestamp_{number + 1}"] = int(timestamp_of_atl)

                fb_of_atl_df.to_sql ( table_where_stocks_with_fd_of_atl_will_be ,
                                      engine_for_db_where_atl_had_fb,if_exists = "append" )

                list_of_tickers_with_fb_of_atl.append ( stock_ticker )





            print ( "data_df\n",data_df.tail(5).to_string() )

        except:
            traceback.print_exc()

    print ("list_of_tickers_with_fb_of_ath")
    print ( list_of_tickers_with_fb_of_ath )
    print ( "list_of_tickers_with_fb_of_atl" )
    print ( list_of_tickers_with_fb_of_atl )

    connection_to_stocks_ohlcv.close ()
    connection_to_db_where_ath_had_fb.close()
    connection_to_db_where_atl_had_fb.close ()





if __name__ == "__main__":
    start_time = time.time ()
    db_with_daily_ohlcv_stock_data="stocks_ohlcv_daily"
    db_where_ath_had_fb ="levels_formed_by_highs_and_lows_for_stocks"
    db_where_atl_had_fb="levels_formed_by_highs_and_lows_for_stocks"
    table_where_stocks_with_fd_of_ath_will_be ="stocks_with_fb_of_ath"
    table_where_stocks_with_fd_of_atl_will_be="stocks_with_fb_of_atl"
    find_false_breakouts_of_ath_and_insert_into_db ( db_with_daily_ohlcv_stock_data,
                                                   db_where_ath_had_fb,
                                                   db_where_atl_had_fb,
                                                   table_where_stocks_with_fd_of_ath_will_be,
                                                   table_where_stocks_with_fd_of_atl_will_be )
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )