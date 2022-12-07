
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

def get_date_with_and_without_time_from_timestamp(timestamp):
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
    date_without_time = date_without_time.replace ( "/" , "_" )
    date_with_time = date_with_time.replace ( "/" , "_" )
    date_with_time = date_with_time.replace ( " " , "__" )
    date_with_time = date_with_time.replace ( ":" , "_" )
    return date_with_time,date_without_time



def find_levels_formed_by_non_whipsawed_gaps(percentage_between_gap_level_and_closing_price,
                                          db_where_ohlcv_data_for_stocks_is_stored,
                                          count_only_round_gap_level,
                                          db_where_levels_formed_by_gap_level_will_be,
                                          table_where_levels_formed_by_gap_level_will_be):

    level_formed_by_gaps_which_were_not_whipsawed_df=pd.DataFrame(columns = ["ticker",
                                                    "gap_level_lower_border",
                                                    "gap_level_higher_border",
                                                    "exchange",
                                                    "short_name",
                                                    "timestamp_lower_border",
                                                    "timestamp_higher_border",
                                                    "gap_level_lower_border_was_whipsawed",
                                                    "gap_level_higher_border_was_whipsawed"])
    list_of_assets_with_last_close_close_to_gap_level=[]


    engine_for_ohlcv_data_for_stocks , \
    connection_to_ohlcv_data_for_stocks = \
        connect_to_postres_db_without_deleting_it_first ( db_where_ohlcv_data_for_stocks_is_stored )

    engine_for_db_where_levels_formed_by_gap_level_will_be , \
    connection_to_db_where_levels_formed_by_gap_level_will_be = \
        connect_to_postres_db_without_deleting_it_first ( db_where_levels_formed_by_gap_level_will_be )

    drop_table ( table_where_levels_formed_by_gap_level_will_be ,
                 engine_for_db_where_levels_formed_by_gap_level_will_be )

    list_of_tables_in_ohlcv_db=\
        get_list_of_tables_in_db ( engine_for_ohlcv_data_for_stocks )
    counter=0
    for stock_name in list_of_tables_in_ohlcv_db:

        # if stock_name!="EXTR":
        #     continue

        counter=counter+1
        print ( f'{stock_name} is'
                f' number {counter} out of {len ( list_of_tables_in_ohlcv_db )}\n' )



        table_with_ohlcv_data_df = \
            pd.read_sql_query ( f'''select * from "{stock_name}"''' ,
                                engine_for_ohlcv_data_for_stocks )

        exchange = table_with_ohlcv_data_df.loc[0 , "exchange"]
        short_name = table_with_ohlcv_data_df.loc[0 , 'short_name']

        # print("table_with_ohlcv_data_df")
        # print ( table_with_ohlcv_data_df )
        table_with_ohlcv_data_df_slice=table_with_ohlcv_data_df[["Timestamp","high","low"]]
        # print ( "table_with_ohlcv_data_df_slice" )
        # print ( table_with_ohlcv_data_df_slice )
        table_with_ohlcv_data_df_slice_numpy=table_with_ohlcv_data_df_slice.to_numpy()
        # print ( "table_with_ohlcv_data_df_slice_numpy" )
        # print ( table_with_ohlcv_data_df_slice_numpy )
        # for x in np.nditer(table_with_ohlcv_data_df_slice_numpy):
        #     print("x")
        #     print ( x )


        for row_number,row_in_numpy_array in enumerate(table_with_ohlcv_data_df_slice_numpy):
            # find gaps up
            try:
                prev_high=table_with_ohlcv_data_df_slice_numpy[row_number-1][1]
                #print("row_in_numpy_array=",row_in_numpy_array)
                current_low=row_in_numpy_array[2]
                if (current_low-prev_high)>0:
                    print (f"{stock_name} had a gap up")
                    max_high_before_gap_up=np.NaN
                    min_low_after_gap_up=np.NaN
                    try:
                        numpy_array_of_highs_before_gap_up=\
                            table_with_ohlcv_data_df_slice_numpy[0:row_number-1,1]
                        print ( "numpy_array_of_highs_before_gap_up=" , numpy_array_of_highs_before_gap_up )

                        #deal with situation if level formed by high is first bar in all data
                        if len(numpy_array_of_highs_before_gap_up)==0:
                            max_high_before_gap_up=prev_high
                        else:
                            max_high_before_gap_up=np.amax(numpy_array_of_highs_before_gap_up)
                            print("max_high_before_gap_up")
                            print ( max_high_before_gap_up )
                    except:
                        traceback.print_exc()
                    try:
                        numpy_array_of_lows_after_gap_up =\
                            table_with_ohlcv_data_df_slice_numpy[row_number + 1:len( table_with_ohlcv_data_df_slice_numpy )-1 ,2]
                        # print ( "numpy_array_of_lows_after_gap_up=" , numpy_array_of_lows_after_gap_up )

                        #if gap took place today
                        if len(numpy_array_of_lows_after_gap_up)==0:
                            min_low_after_gap_up=current_low
                        else:
                            min_low_after_gap_up = np.amin ( numpy_array_of_lows_after_gap_up )
                    except:
                        traceback.print_exc()

                    #Get quality of level. B+ was never whipsawed. B was whipsawed

                    #Both levels are good
                    if ((current_low<=min_low_after_gap_up) and (prev_high>=max_high_before_gap_up)):
                        timestamp_for_prev_high=table_with_ohlcv_data_df_slice_numpy[row_number-1][0]
                        timestamp_for_current_low = table_with_ohlcv_data_df_slice_numpy[row_number][0]
                        prev_high_open_date_with_time,prev_high_open_date_without_time=\
                            get_date_with_and_without_time_from_timestamp ( timestamp_for_prev_high )
                        current_low_open_time_with_time, current_low_open_date_without_time=\
                            get_date_with_and_without_time_from_timestamp ( timestamp_for_current_low )
                        print(f"{stock_name} had a gap up with high={prev_high} on {prev_high_open_date_without_time} "
                              f"and low={current_low} on {current_low_open_date_without_time}."
                              f"Gap levels formed by high and low are B+ quality. Both levels were never whipsawed")

                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , 'ticker'] = stock_name
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , 'exchange'] = exchange
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_lower_border"] = prev_high
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_higher_border"] = current_low
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "short_name"] = short_name
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "timestamp_lower_border"] = prev_high_open_date_without_time
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "timestamp_higher_border"] = current_low_open_date_without_time
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_lower_border_was_whipsawed"] = False
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_higher_border_was_whipsawed"] = False

                        # level_formed_by_gaps_which_were_not_whipsawed_df.reset_index ( inplace = True )
                        level_formed_by_gaps_which_were_not_whipsawed_df.to_sql ( table_where_levels_formed_by_gap_level_will_be ,
                                                         engine_for_db_where_levels_formed_by_gap_level_will_be ,
                                                         if_exists = 'append' )


                    #High level is bad . Low level is good
                    elif ((current_low<=min_low_after_gap_up) and (prev_high<max_high_before_gap_up)):
                        if current_low>max_high_before_gap_up:
                            timestamp_for_prev_high = table_with_ohlcv_data_df_slice_numpy[row_number - 1][0]
                            timestamp_for_current_low = table_with_ohlcv_data_df_slice_numpy[row_number][0]
                            prev_high_open_date_with_time , prev_high_open_date_without_time = \
                                get_date_with_and_without_time_from_timestamp ( timestamp_for_prev_high )
                            current_low_open_time_with_time , current_low_open_date_without_time = \
                                get_date_with_and_without_time_from_timestamp ( timestamp_for_current_low )
                            print (
                                f"{stock_name} had a gap up with high={prev_high} and low={current_low}."
                                f"Level formed by the first low after gap is B+ quality but "
                                f"level formed by high is B because it was whipsawed" )

                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , 'ticker'] = stock_name
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , 'exchange'] = exchange
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_lower_border"] = prev_high
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_higher_border"] = current_low
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "short_name"] = short_name
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "timestamp_lower_border"] = prev_high_open_date_without_time
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "timestamp_higher_border"] = current_low_open_date_without_time
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_lower_border_was_whipsawed"] = True
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_higher_border_was_whipsawed"] = False

                            # level_formed_by_gaps_which_were_not_whipsawed_df.reset_index ( inplace = True )
                            level_formed_by_gaps_which_were_not_whipsawed_df.to_sql (
                                table_where_levels_formed_by_gap_level_will_be ,
                                engine_for_db_where_levels_formed_by_gap_level_will_be ,
                                if_exists = 'append' )

                    # Low level is bad . High level is good
                    elif ((prev_high>=max_high_before_gap_up) and (current_low>min_low_after_gap_up)):
                        if prev_high<min_low_after_gap_up:
                            timestamp_for_prev_high = table_with_ohlcv_data_df_slice_numpy[row_number - 1][0]
                            timestamp_for_current_low = table_with_ohlcv_data_df_slice_numpy[row_number][0]
                            prev_high_open_date_with_time , prev_high_open_date_without_time = \
                                get_date_with_and_without_time_from_timestamp ( timestamp_for_prev_high )
                            current_low_open_time_with_time , current_low_open_date_without_time = \
                                get_date_with_and_without_time_from_timestamp ( timestamp_for_current_low )
                            print (
                                f"{stock_name} had a gap up with high={prev_high} and low={current_low}."
                                f"Level formed by the first low after gap is B quality.  "
                                f"Level formed by high is B+ because it was never whipsawed" )

                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , 'ticker'] = stock_name
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , 'exchange'] = exchange
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_lower_border"] = prev_high
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_higher_border"] = current_low
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "short_name"] = short_name
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "timestamp_lower_border"] = prev_high_open_date_without_time
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "timestamp_higher_border"] = current_low_open_date_without_time
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_lower_border_was_whipsawed"] = False
                            level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                                0 , "gap_level_higher_border_was_whipsawed"] = True

                            # level_formed_by_gaps_which_were_not_whipsawed_df.reset_index ( inplace = True )
                            level_formed_by_gaps_which_were_not_whipsawed_df.to_sql (
                                table_where_levels_formed_by_gap_level_will_be ,
                                engine_for_db_where_levels_formed_by_gap_level_will_be ,
                                if_exists = 'append' )

                    #both_levels are bad
                    else:
                        timestamp_for_prev_high = table_with_ohlcv_data_df_slice_numpy[row_number - 1][0]
                        timestamp_for_current_low = table_with_ohlcv_data_df_slice_numpy[row_number][0]
                        prev_high_open_date_with_time , prev_high_open_date_without_time = \
                            get_date_with_and_without_time_from_timestamp ( timestamp_for_prev_high )
                        current_low_open_time_with_time , current_low_open_date_without_time = \
                            get_date_with_and_without_time_from_timestamp ( timestamp_for_current_low )
                        print (
                            f"{stock_name} had a gap up with high={prev_high} and low={current_low}."
                            f"Both levels are B quality because they were whipsawed")
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , 'ticker'] = stock_name
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , 'exchange'] = exchange
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_lower_border"] = prev_high
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_higher_border"] = current_low
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "short_name"] = short_name
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "timestamp_lower_border"] = prev_high_open_date_without_time
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "timestamp_higher_border"] = current_low_open_date_without_time
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_lower_border_was_whipsawed"] = True
                        level_formed_by_gaps_which_were_not_whipsawed_df.loc[
                            0 , "gap_level_higher_border_was_whipsawed"] = True

                        # level_formed_by_gaps_which_were_not_whipsawed_df.reset_index ( inplace = True )
                        level_formed_by_gaps_which_were_not_whipsawed_df.to_sql (
                            table_where_levels_formed_by_gap_level_will_be ,
                            engine_for_db_where_levels_formed_by_gap_level_will_be ,
                            if_exists = 'append' )

            except:
                traceback.print_exc()


if __name__=="__main__":
    start_time=time.time ()
    db_where_ohlcv_data_for_stocks_is_stored="stocks_ohlcv_daily"
    count_only_round_gap_level=False
    db_where_levels_formed_by_gap_level_will_be="levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_gap_level_will_be = "levels_formed_by_gap_up"

    if count_only_round_gap_level:
        db_where_levels_formed_by_gap_level_will_be="round_levels_formed_by_highs_and_lows_for_stocks"
    percentage_between_gap_level_and_closing_price=10
    find_levels_formed_by_non_whipsawed_gaps(percentage_between_gap_level_and_closing_price,
                                              db_where_ohlcv_data_for_stocks_is_stored,
                                                count_only_round_gap_level,
                                              db_where_levels_formed_by_gap_level_will_be,
                                              table_where_levels_formed_by_gap_level_will_be)

    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )