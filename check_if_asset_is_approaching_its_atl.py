
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



def check_if_asset_is_approaching_its_atl(percentage_between_atl_and_closing_price,
                                          db_where_ohlcv_data_for_stocks_is_stored,
                                          count_only_round_atl,
                                          db_where_levels_formed_by_atl_will_be,
                                          table_where_levels_formed_by_atl_will_be):

    levels_formed_by_atl_df=pd.DataFrame(columns = ["ticker",
                                                    "atl",
                                                    "exchange",
                                                    "short_name",
                                                    "timestamp_1",
                                                    "timestamp_2",
                                                    "timestamp_3"])
    list_of_assets_with_last_close_close_to_atl=[]


    engine_for_ohlcv_data_for_stocks , \
    connection_to_ohlcv_data_for_stocks = \
        connect_to_postres_db_without_deleting_it_first ( db_where_ohlcv_data_for_stocks_is_stored )

    engine_for_db_where_levels_formed_by_atl_will_be , \
    connection_to_db_where_levels_formed_by_atl_will_be = \
        connect_to_postres_db_without_deleting_it_first ( db_where_levels_formed_by_atl_will_be )

    drop_table ( table_where_levels_formed_by_atl_will_be ,
                 engine_for_db_where_levels_formed_by_atl_will_be )

    list_of_tables_in_ohlcv_db=\
        get_list_of_tables_in_db ( engine_for_ohlcv_data_for_stocks )
    counter=0
    for stock_name in list_of_tables_in_ohlcv_db:
        counter=counter+1
        print ( f'{stock_name} is'
                f' number {counter} out of {len ( list_of_tables_in_ohlcv_db )}\n' )
        all_time_low_in_stock, table_with_ohlcv_data_df=\
            get_all_time_low_from_ohlcv_table ( engine_for_ohlcv_data_for_stocks ,
                                            stock_name )

        if count_only_round_atl==True:
            level_is_round_bool=find_if_level_is_round ( all_time_low_in_stock )
            if not level_is_round_bool:
                print(f"in {stock_name} level={all_time_low_in_stock} is not round and is ATL")
                continue

        last_close_price=get_last_close_price_of_asset ( table_with_ohlcv_data_df )
        print("last_close_price")
        print ( last_close_price)
        distance_in_percent_to_atl_from_close_price=\
            (last_close_price-all_time_low_in_stock)/all_time_low_in_stock
        if distance_in_percent_to_atl_from_close_price <= percentage_between_atl_and_closing_price/100.0:
            print(f"last closing price={last_close_price} is"
                  f" within {percentage_between_atl_and_closing_price}% range to atl={all_time_low_in_stock}")
            list_of_assets_with_last_close_close_to_atl.append(stock_name)
            print("list_of_assets_with_last_close_close_to_atl")
            print ( list_of_assets_with_last_close_close_to_atl )
            df_where_low_equals_atl=\
                table_with_ohlcv_data_df[table_with_ohlcv_data_df["low"]==all_time_low_in_stock]
            print ( "df_where_low_equals_atl" )
            print ( df_where_low_equals_atl )
            exchange=table_with_ohlcv_data_df["exchange"].iat[0]
            short_name = table_with_ohlcv_data_df["short_name"].iat[0]


            levels_formed_by_atl_df.at[counter - 1 , "ticker"] = stock_name
            levels_formed_by_atl_df.at[counter - 1 , "exchange"] = exchange
            levels_formed_by_atl_df.at[counter - 1 , "short_name"] = short_name
            levels_formed_by_atl_df.at[counter - 1 , "atl"] = all_time_low_in_stock
            for number_of_timestamp,timestamp_of_atl in enumerate(df_where_low_equals_atl.loc[:,"Timestamp"]):
                print("number_of_timestamp")
                print ( number_of_timestamp )
                print ( "timestamp_of_atl" )
                print ( timestamp_of_atl )
                levels_formed_by_atl_df.at[counter - 1 , f"timestamp_{number_of_timestamp+1}"]=\
                    timestamp_of_atl





            print("levels_formed_by_atl_df")
            print ( levels_formed_by_atl_df )


    levels_formed_by_atl_df.reset_index(inplace = True)
    levels_formed_by_atl_df.to_sql(table_where_levels_formed_by_atl_will_be,
                                   engine_for_db_where_levels_formed_by_atl_will_be,
                                   if_exists = 'replace')
    print ( "levels_formed_by_atl_df" )
    print ( levels_formed_by_atl_df )




    pass
if __name__=="__main__":
    db_where_ohlcv_data_for_stocks_is_stored="stocks_ohlcv_daily"
    count_only_round_atl=False
    db_where_levels_formed_by_atl_will_be="levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_atl_will_be = "levels_formed_by_atl"

    if count_only_round_atl:
        db_where_levels_formed_by_atl_will_be="round_levels_formed_by_highs_and_lows_for_stocks"
    percentage_between_atl_and_closing_price=10
    check_if_asset_is_approaching_its_atl(percentage_between_atl_and_closing_price,
                                              db_where_ohlcv_data_for_stocks_is_stored,
                                                count_only_round_atl,
                                              db_where_levels_formed_by_atl_will_be,
                                              table_where_levels_formed_by_atl_will_be)