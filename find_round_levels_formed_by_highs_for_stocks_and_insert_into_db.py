
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


def connect_to_postres_db_without_deleting_it_first(database ):
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




def drop_table(table_name,engine):
    engine.execute (
        f"DROP TABLE IF EXISTS {table_name};" )


    # base = declarative_base()
    # metadata = MetaData(engine)
    #
    # table = metadata.tables.get(table_name)
    # if table is not None:
    #    logging.info(f'Deleting {table_name} table')
    #    base.metadata.drop_all(engine, [table], checkfirst=True)

def check_if_a_few_days_before_high_were_gaps(current_index_of_high,
                                             data_df):
    current_low = data_df["low"].iat[current_index_of_high]
    # current_high=data_df["high"].iat[current_index_of_high]
    prev_low=data_df["low"].iat[current_index_of_high-1]
    prev_high = data_df["high"].iat[current_index_of_high - 1]
    current_high = data_df["high"].iat[current_index_of_high]
    low_before_prev_low = data_df["low"].iat[current_index_of_high - 2]
    high_before_prev_high = data_df["high"].iat[current_index_of_high - 2]
    high_before_low_which_was_before_prev_low=data_df["low"].iat[current_index_of_high - 3]
    high_before_high_which_was_before_prev_high = data_df["high"].iat[current_index_of_high - 3]

    bool_list_to_check_if_stock_had_one_gap_before_high =[(current_low-prev_high)>0]

    bool_list_to_check_if_stock_had_two_gaps_before_high=[(current_low-prev_high)>0,
                                                           (prev_low-high_before_prev_high)>0]
    bool_list_to_check_if_stock_had_three_gaps_before_high = [(current_low-prev_high) > 0 ,
                                                           (prev_low-high_before_prev_high) > 0,
                                                             (low_before_prev_low-high_before_high_which_was_before_prev_high)>0]

    if (current_low-prev_high)>0:
        print("ticker has a gap")

        # time.sleep ( 100000 )

    if all(bool_list_to_check_if_stock_had_one_gap_before_high) and not all(bool_list_to_check_if_stock_had_two_gaps_before_high) :
        print(f"{data_df['ticker'].iat[0]} had high with one gap before {current_high}")

        return 1
    elif all(bool_list_to_check_if_stock_had_two_gaps_before_high) and not all(bool_list_to_check_if_stock_had_three_gaps_before_high):
        print(f"{data_df['ticker'].iat[0]} had high with two gaps before {current_high}")


        return 2
    elif all(bool_list_to_check_if_stock_had_three_gaps_before_high):
        print(f"{data_df['ticker'].iat[0]} had high with three gaps before {current_high}")
        # time.sleep ( 100000 )
        # print(f"high_before_high_which_was_before_prev_high={high_before_high_which_was_before_prev_high}"
        #       f"\n and high_before_prev_high={high_before_prev_high}\n"
        #       f"(high_before_high_which_was_before_prev_high-high_before_prev_high)="
        #       f"{high_before_high_which_was_before_prev_high-high_before_prev_high},\n"
        #       f"high_before_low_which_was_before_prev_low-high_before_prev_high="
        #       f"{low_before_low_which_was_before_prev_low-high_before_prev_high}")
        return 3
        # print ( f"current high at {current_high} and prev low at {prev_low}" )

    else:
        print ( f"{data_df['ticker'].iat[0]} had no gaps before {current_high}" )
        return 0



def find_round_levels_formed_by_highs_for_stocks_and_insert_into_db(so_many_number_of_touches_of_level_by_highs,
                                        so_many_number_of_touches_of_level_by_lows,
                                         db_with_daily_ohlcv_stock_data,
                                         db_where_levels_formed_by_high_or_low_will_be,
                                         table_where_levels_formed_by_highs_will_be,
                                         so_many_last_days_for_level_calculation = 30):
    start_time = time.time ()
    counter_for_tables = 0


    engine_for_stocks_ohlcv, connection_to_stocks_ohlcv =\
        connect_to_postres_db_without_deleting_it_first(db_with_daily_ohlcv_stock_data)

    inspector = inspect ( engine_for_stocks_ohlcv )
    # print(metadata.reflect(engine_for_stocks_ohlcv))
    # print(inspector.get_table_names())
    list_of_tables_from_sql_query = inspector.get_table_names ()
    print ( "list_of_tables_from_sql_query\n" )
    print ( list_of_tables_from_sql_query )


    # open_connection to database where levels formed by highs will be stored
    engine_for_levels_formed_by_highs_and_lows_in_stocks , connection_to_levels_formed_by_highs_and_lows_in_stocks = \
        connect_to_postres_db_without_deleting_it_first ( db_where_levels_formed_by_high_or_low_will_be )



    counter_for_stock_ticker_with_low = 0
    counter_for_stock_ticker_with_high = 0

    try:
        drop_table ( table_where_levels_formed_by_highs_will_be ,
                     engine_for_levels_formed_by_highs_and_lows_in_stocks )
        print ( "\ntable dropped\n" )
        # time.sleep ( 1000 )
    except Exception as e:
        print ( "cant drop table from db\n" , e )
        # time.sleep(1000)


    for number_of_gaps_before_high in range(0,4):
        try:
            drop_table ( f"{table_where_levels_formed_by_highs_will_be}_with_{number_of_gaps_before_high}_number_of_gaps" ,
                         engine_for_levels_formed_by_highs_and_lows_in_stocks )
            print ( "\ntable dropped\n" )
            # time.sleep ( 1000 )
        except Exception as e:
            print ( "cant drop table from db\n" , e )
            # time.sleep(1000)


    # try:
    #     drop_table ( table_where_levels_formed_by_highs_will_be ,
    #                  engine_for_levels_formed_by_highs_and_lows_in_stocks )
    #     print ( "\ntable dropped\n" )
    #     # time.sleep ( 1000 )
    # except Exception as e:
    #     print ( "cant drop table from db\n" , e )
    #     # time.sleep(1000)

    ####################################################################
    ##################################################################
    list_of_tables = list_of_tables_from_sql_query


    print(list_of_tables)

    discard_pair_by_volume_counter = 0
    this_many_last_days_for_data_df_slice=90
    calculate_average_volume_for_this_many_days=30
    volume_limit=300000
    list_of_pairs_and_exchanges_with_touches_of_level_by_low=[]
    list_of_pairs_and_exchanges_with_touches_of_level_by_high=[]
    not_recent_pair_counter=0
    counter_for_low_level_in_final_df = 0
    counter_for_high_level_in_final_df = 0


    levels_formed_by_highs_df = pd.DataFrame ( columns = ['ticker' ,
                                                         'exchange' ,
                                                         'level_formed_by_high' ,
                                                         'average_volume','timestamp_1','timestamp_2','timestamp_3'] )

    for table_in_db in list_of_tables:
        counter_for_tables = counter_for_tables + 1
        try:
            list_of_numbers_of_gaps_before_high=[]
            data_df=\
                pd.read_sql_query(f'''select * from "{table_in_db}"''' ,
                                  connection_to_stocks_ohlcv)
            #print ( "data_df\n",data_df )

            #data_df.set_index("Timestamp")
            #print ( "data_df\n" , data_df )
            print("---------------------------")
            print(f'{table_in_db} is number {counter_for_tables} out of {len(list_of_tables)}\n' )
            #print("usdt_ohlcv_df\n",data_df )
            stock_ticker=data_df.loc[0,'ticker']
            exchange=data_df.loc[0,"exchange"]
            #data_df.reset_index()
            data_df.set_index("Timestamp", inplace = True)
            data_df["volume_by_close"] = data_df["volume"] * data_df["close"]

            # current_timestamp = time.time ()
            # last_timestamp_in_df = data_df.tail ( 1 ).index.item () / 1000.0
            # if (current_timestamp-last_timestamp_in_df)>(86400*3):
            #     not_recent_pair_counter=not_recent_pair_counter+1
            #     print("not_recent_pair_counter=",not_recent_pair_counter)
            #     continue





            last_several_days_slice_of_df_for_average_volume_calculation_and_removal_of_duplicates =\
                data_df.tail ( this_many_last_days_for_data_df_slice )
            average_volume=last_several_days_slice_of_df_for_average_volume_calculation_and_removal_of_duplicates[
                "volume_by_close"].tail(calculate_average_volume_for_this_many_days).mean()
            print("average volume=", average_volume)

            #discard pairs with low volume
            # if average_volume<volume_limit:
            #     print(f"average volume is less than {volume_limit}"
            #           f" for {stock_ticker} on {exchange}  ")
            #     discard_pair_by_volume_counter=discard_pair_by_volume_counter+1
            #     print("discard_pair_by_volume_counter=",
            #           discard_pair_by_volume_counter)
            #     continue
            

            if last_several_days_slice_of_df_for_average_volume_calculation_and_removal_of_duplicates.duplicated ( subset = 'high' ,
                                                       keep = False ).sum () == len ( last_several_days_slice_of_df_for_average_volume_calculation_and_removal_of_duplicates ):
                print ( f"all duplicated highs are found in {stock_ticker} on {exchange}" )
                continue

            data_df.reset_index(inplace=True)

            last_n_days_from_data_df=data_df.tail(so_many_last_days_for_level_calculation)
            # print("last_n_days_from_data_df\n")
            # print ( last_n_days_from_data_df )


            count_equal_highs_dict=Counter(last_n_days_from_data_df["high"].to_list())
            count_equal_highs_df=pd.DataFrame(count_equal_highs_dict.items(),
                                              columns=["high","number_of_equal_highs"])

            count_equal_lows_dict = Counter ( last_n_days_from_data_df["low"].to_list () )
            count_equal_lows_df = pd.DataFrame ( count_equal_lows_dict.items () ,
                                                  columns = ["low" , "number_of_equal_lows"] )

            data_df_plus_count_high_values_merged_full_df = pd.merge ( last_n_days_from_data_df ,count_equal_highs_df,
                                                          on = 'high' , how = 'left' )
            data_df_plus_count_low_values_merged_full_df = pd.merge ( last_n_days_from_data_df , count_equal_lows_df ,
                                                          on = 'low' , how = 'left' )

            data_df_slice_plus_number_of_equal_lows_and_highs = pd.merge ( data_df_plus_count_high_values_merged_full_df ,
                                                                  count_equal_lows_df,
                                                          on = 'low' , how = 'left' )
            #print ( f'{table_in_db} is number {counter} out of {len ( list_of_tables )}\n' )
            data_df_slice_plus_number_of_equal_lows_and_highs["low_level"] = np.nan
            data_df_slice_plus_number_of_equal_lows_and_highs["high_level"] = np.nan

            # lower_closes_for_that_many_last_days=5
            # higher_closes_for_that_many_last_days = 5
            for number_of_touches_by_low in range(2,so_many_number_of_touches_of_level_by_lows+1):
                try:
                    if number_of_touches_by_low in \
                            data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].values:
                        counter_for_stock_ticker_with_low = counter_for_stock_ticker_with_low + 1
                        print ( "counter_for_stock_ticker_with_low=",
                                counter_for_stock_ticker_with_low)
                        # list_of_pairs_and_exchanges_with_touches_of_level_by_low.append (
                        #     f"{stock_ticker} on {exchange}" )
                        #print ( data_df_slice_plus_number_of_equal_lows_and_highs )
                        print (f"in stock ticker {stock_ticker} on {exchange} the number"
                               f" of touches by lows is equal to {number_of_touches_by_low}")
                        print ( "data_df_slice_plus_number_of_equal_lows_and_highs\n" )
                        print ( data_df_slice_plus_number_of_equal_lows_and_highs )

                        for row in range(0,len(data_df_slice_plus_number_of_equal_lows_and_highs)) :
                            if data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].iloc[row]>1:
                                # print (
                                #     'data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].iloc[row]\n' )
                                # print ( data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_lows"].iloc[
                                #             row] )
                                data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].iat[row]=\
                                    data_df_slice_plus_number_of_equal_lows_and_highs["low"].iat[row]
                                # print(data_df_slice_plus_number_of_equal_lows_and_highs)

                            else:
                                data_df_slice_plus_number_of_equal_lows_and_highs["low_level"].iat[row] = np.nan



                        pass
                except Exception as e:
                    print(f"error with {stock_ticker} on {exchange}",e)

            data_df_slice_plus_number_of_equal_lows_without_NaNs=pd.DataFrame()
            data_df_slice_plus_number_of_equal_highs_without_NaNs = pd.DataFrame ()


            for number_of_touches_by_high in range ( 2 , so_many_number_of_touches_of_level_by_highs + 1 ):
                try:
                    if number_of_touches_by_high in \
                            data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].values:
                        counter_for_stock_ticker_with_high = counter_for_stock_ticker_with_high + 1
                        print ( "counter_for_stock_ticker_with_high=" ,
                                counter_for_stock_ticker_with_high )
                        print ( f"in stock ticker {stock_ticker} on {exchange} the number"
                                f" of touches by highs is equal to {number_of_touches_by_high}" )
                        # list_of_pairs_and_exchanges_with_touches_of_level_by_high.append(f"{stock_ticker} on {exchange}")
                        # print ( "data_df_slice_plus_number_of_equal_lows_and_highs\n" )
                        # print ( data_df_slice_plus_number_of_equal_lows_and_highs )
                        #counter_for_stock_ticker_with_high=counter_for_stock_ticker_with_high+1
                        #data_df_slice_plus_number_of_equal_lows_and_highs["high_level"] =  np.nan

                        for row in range ( 0 , len ( data_df_slice_plus_number_of_equal_lows_and_highs ) ):
                            if data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].iloc[row] > 1:
                                print('data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].iloc[row]\n')
                                print(data_df_slice_plus_number_of_equal_lows_and_highs["number_of_equal_highs"].iloc[row])
                                data_df_slice_plus_number_of_equal_lows_and_highs["high_level"].iat[row] = \
                                    data_df_slice_plus_number_of_equal_lows_and_highs["high"].iat[row]
                                # print ( data_df_slice_plus_number_of_equal_lows_and_highs )

                                # print ( "data_df_slice_plus_number_of_equal_lows_and_highs_outside\n" )
                                print ( "data_df_slice_plus_number_of_equal_lows_and_highs" )
                                print ( data_df_slice_plus_number_of_equal_lows_and_highs )
                                if row==len ( data_df_slice_plus_number_of_equal_lows_and_highs )-1:

                                    print ( "data_df \n" , data_df )
                                    print ( "data_df_slice_plus_number_of_equal_lows_and_highs_outside\n" )
                                    print ( data_df_slice_plus_number_of_equal_lows_and_highs )

                                    # data_df_slice_plus_number_of_equal_lows_without_NaNs=\
                                    #     data_df_slice_plus_number_of_equal_lows_and_highs.dropna(subset=
                                    #                                                          ["low_level"],
                                    #                                                          how="all")

                                    data_df_slice_plus_number_of_equal_highs_without_NaNs = \
                                        data_df_slice_plus_number_of_equal_lows_and_highs.dropna ( subset =
                                                                                                   ["high_level"] ,
                                                                                                   how = "all" )
                except Exception as e:
                    traceback.print_exc()
                                    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++#

            dict_of_highs_with_timestamps={}
            dict_of_highs_legit_or_not_legit = {}
            for row_of_high_levels in range(len(data_df_slice_plus_number_of_equal_highs_without_NaNs)):
                high_level=\
                    data_df_slice_plus_number_of_equal_highs_without_NaNs["high_level"].iat[row_of_high_levels]

                if not find_if_level_is_round ( high_level ):
                    continue

                timestamp_for_high_level=\
                    data_df_slice_plus_number_of_equal_highs_without_NaNs["Timestamp"].iat[row_of_high_levels]
                row_from_data_df_with_high=data_df[data_df["Timestamp"]==timestamp_for_high_level]
                try:
                    current_index_of_high=row_from_data_df_with_high.index.item()
                    print ( "row_from_data_df_with_high\n" ,
                            row_from_data_df_with_high )
                    print ( "current_index_of_high\n" , current_index_of_high )

                    prev_index_of_high=current_index_of_high-1
                    next_index_of_high = current_index_of_high +1
                    last_index_of_data_df=data_df.tail(1).index.item()
                    first_index_of_data_df = data_df.head ( 1 ).index.item ()

                    number_of_gaps_before_current_high=check_if_a_few_days_before_high_were_gaps(current_index_of_high,data_df)
                    list_of_numbers_of_gaps_before_high.append(number_of_gaps_before_current_high)





                    if not (next_index_of_high>last_index_of_data_df or\
                            prev_index_of_high<first_index_of_data_df):
                        prev_high = data_df["high"].iat[prev_index_of_high]
                        current_high = data_df["high"].iat[current_index_of_high]


                        next_high = data_df["high"].iat[next_index_of_high]
                        print(f"found non boundary high {current_high} "
                              f"for {stock_ticker} on {exchange}")
                        print ( "first_index_of_data_df=",first_index_of_data_df)
                        print ( "last_index_of_data_df=" , last_index_of_data_df )
                        print ( "prev_index_of_high=" , prev_index_of_high )
                        print ( "next_index_of_high=" , next_index_of_high )

                        ################################
                        #################################
                        #################################
                        if current_high not in dict_of_highs_with_timestamps:
                            dict_of_highs_with_timestamps[current_high]=[timestamp_for_high_level]
                        else:
                            dict_of_highs_with_timestamps[current_high].append(timestamp_for_high_level)




                except Exception as e:
                    print ("error=",e)

            print ( "dict_of_highs_with_timestamps\n" ,
                    dict_of_highs_with_timestamps )

            timestamp_for_high_level=None
            timestamp_for_low_level = None

            dict_of_highs_legit_or_not_legit_with_only_legit_level = {}
            dict_of_legit_high_levels_with_counter = {}
            for level in dict_of_highs_with_timestamps.keys ():
                counter = 0


                if len ( dict_of_highs_with_timestamps ) > 0:

                    counter_for_timestamp = 0
                    counter_for_high_level_in_final_df = counter_for_high_level_in_final_df + 1

                    list_of_timestamp_plus_numbers = [f"timestamp_{number_of_timestamp}" for
                                                      number_of_timestamp in
                                                      range ( 1 , 101 )]
                    print ( "list_of_timestamp_plus_numbers" )
                    print ( list_of_timestamp_plus_numbers )

                    first_four_columns_list = ['ticker' , 'exchange' ,
                                               'level_formed_by_high' ,
                                               'average_volume']
                    column_list = [*first_four_columns_list ,
                                   *list_of_timestamp_plus_numbers]
                    levels_formed_by_highs_df_with_1_gap = pd.DataFrame (
                        columns = column_list )
                    levels_formed_by_highs_df_with_2_gaps = pd.DataFrame (
                        columns = column_list )
                    levels_formed_by_highs_df_with_3_gaps = pd.DataFrame (
                        columns = column_list )

                    for timestamp in \
                    dict_of_highs_with_timestamps[level]:
                        counter_for_timestamp = counter_for_timestamp + 1

                        # string_with_timestamp = string_with_timestamp.replace (
                        #     "legit_at_" , "" )
                        #string_with_timestamp=string_with_timestamp[9:]
                        timestamp = int (  float(timestamp) )
                        # print ( "string_with_timestamp" )
                        print ( f"{stock_ticker}_on_{exchange}_at_high_level_{level}" )
                        print ( timestamp )

                        print ( f"list_of_numbers_of_gaps_before_high for {table_in_db} is {list_of_numbers_of_gaps_before_high}" )


                            #time.sleep(1000000)
                        levels_formed_by_highs_df.loc[counter_for_high_level_in_final_df - 1,'ticker'] = stock_ticker
                        levels_formed_by_highs_df.loc[counter_for_high_level_in_final_df - 1,'exchange'] = exchange
                        # levels_formed_by_highs_df.loc[counter_for_high_level_in_final_df - 1,'average_volume'] = average_volume
                        levels_formed_by_highs_df.loc[counter_for_high_level_in_final_df - 1,'level_formed_by_high'] = level
                        # if f'timestamp_{counter_for_timestamp}' not in levels_formed_by_highs_df.columns:
                        #     levels_formed_by_highs_df[f'timestamp_{counter_for_timestamp}']=""
                        levels_formed_by_highs_df.loc[counter_for_high_level_in_final_df - 1,f'timestamp_{counter_for_timestamp}'] = timestamp

                        print ( "levels_formed_by_highs_df" )
                        print ( levels_formed_by_highs_df )

                        if counter_for_high_level_in_final_df%10==0:
                            print("df with levels formed by highs is moved to database")

                            levels_formed_by_highs_df.to_sql ( table_where_levels_formed_by_highs_will_be ,
                                                              connection_to_levels_formed_by_highs_and_lows_in_stocks ,
                                                              if_exists = 'replace' ,
                                                              index = False )

                        list_of_min_numbers_of_gaps_before_any_high_for_stock_not_to_be_discarded=[1,2,3]
                        min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded = 1
                        if min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded in list_of_numbers_of_gaps_before_high:
                            # print ("found stock ticker with 3 gaps before high")


                            levels_formed_by_highs_df_with_1_gap.loc[
                                0, 'ticker'] = stock_ticker
                            levels_formed_by_highs_df_with_1_gap.loc[
                                0 , 'exchange'] = exchange
                            # levels_formed_by_highs_df_with_1_gap.loc[
                            #     0 , 'average_volume'] = average_volume
                            levels_formed_by_highs_df_with_1_gap.loc[
                                0, 'level_formed_by_high'] = level
                            # if f'timestamp_{counter_for_timestamp}' not in levels_formed_by_highs_df.columns:
                            #     levels_formed_by_highs_df[f'timestamp_{counter_for_timestamp}']=""
                            levels_formed_by_highs_df_with_1_gap.loc[
                                0 , f'timestamp_{counter_for_timestamp}'] = timestamp

                        min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded = 2
                        if min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded in list_of_numbers_of_gaps_before_high:
                            # print ("found stock ticker with 3 gaps before high")

                            levels_formed_by_highs_df_with_2_gaps.loc[
                                0 , 'ticker'] = stock_ticker
                            levels_formed_by_highs_df_with_2_gaps.loc[
                                0 , 'exchange'] = exchange
                            # levels_formed_by_highs_df_with_2_gaps.loc[
                            #     0 , 'average_volume'] = average_volume
                            levels_formed_by_highs_df_with_2_gaps.loc[
                                0 , 'level_formed_by_high'] = level
                            # if f'timestamp_{counter_for_timestamp}' not in levels_formed_by_highs_df.columns:
                            #     levels_formed_by_highs_df[f'timestamp_{counter_for_timestamp}']=""
                            levels_formed_by_highs_df_with_2_gaps.loc[
                                0 , f'timestamp_{counter_for_timestamp}'] = timestamp

                        min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded = 3
                        if min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded in list_of_numbers_of_gaps_before_high:
                            # print ("found stock ticker with 3 gaps before high")

                            levels_formed_by_highs_df_with_3_gaps.loc[
                                0 , 'ticker'] = stock_ticker
                            levels_formed_by_highs_df_with_3_gaps.loc[
                                0 , 'exchange'] = exchange
                            # levels_formed_by_highs_df_with_3_gaps.loc[
                            #     0 , 'average_volume'] = average_volume
                            levels_formed_by_highs_df_with_3_gaps.loc[
                                0 , 'level_formed_by_high'] = level
                            # if f'timestamp_{counter_for_timestamp}' not in levels_formed_by_highs_df.columns:
                            #     levels_formed_by_highs_df[f'timestamp_{counter_for_timestamp}']=""
                            levels_formed_by_highs_df_with_3_gaps.loc[
                                0 , f'timestamp_{counter_for_timestamp}'] = timestamp

                    min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded = 1
                    if min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded in list_of_numbers_of_gaps_before_high:
                        levels_formed_by_highs_df_with_1_gap.to_sql (
                            f"{table_where_levels_formed_by_highs_will_be}_with_{min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded}_number_of_gaps" ,
                            connection_to_levels_formed_by_highs_and_lows_in_stocks ,
                            if_exists = 'append' ,
                            index = False )

                    min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded = 2
                    if min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded in list_of_numbers_of_gaps_before_high:
                        levels_formed_by_highs_df_with_2_gaps.to_sql (
                            f"{table_where_levels_formed_by_highs_will_be}_with_{min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded}_number_of_gaps" ,
                            connection_to_levels_formed_by_highs_and_lows_in_stocks ,
                            if_exists = 'append' ,
                            index = False )
                    min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded = 3
                    if min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded in list_of_numbers_of_gaps_before_high:
                        levels_formed_by_highs_df_with_3_gaps.to_sql (
                            f"{table_where_levels_formed_by_highs_will_be}_with_{min_number_of_gaps_before_any_high_for_stock_not_to_be_discarded}_number_of_gaps" ,
                            connection_to_levels_formed_by_highs_and_lows_in_stocks ,
                            if_exists = 'append' ,
                            index = False )
                    # time.sleep(100000)

        except Exception as e:
            print(f"problem with {table_in_db}", e)
            traceback.print_exc ()


    print ( "not_recent_pair_counter=" , not_recent_pair_counter )
    levels_formed_by_highs_df.drop_duplicates(subset=
                                             ["ticker","exchange","level_formed_by_high"],
                                             ignore_index = True,inplace = True,keep="last")
    # levels_formed_by_highs_df.drop_duplicates ( subset=
    #                                             ["ticker","exchange","level_formed_by_high"],
    #                                             ignore_index = True , inplace = True,keep="last" )



    print ( "levels_formed_by_highs_df" )
    print(levels_formed_by_highs_df)
    # print ( "levels_formed_by_highs_df" )
    # print ( levels_formed_by_highs_df )

    levels_formed_by_highs_df.to_sql ( table_where_levels_formed_by_highs_will_be ,
                             connection_to_levels_formed_by_highs_and_lows_in_stocks ,
                             if_exists = 'replace' , index = False )
    # levels_formed_by_highs_df.to_sql ( table_where_levels_formed_by_highs_will_be ,
    #                                   connection_to_levels_formed_by_highs_and_lows_in_stocks ,
    #                                   if_exists = 'replace' , index = False )

    connection_to_stocks_ohlcv.close()
    connection_to_levels_formed_by_highs_and_lows_in_stocks.close()

    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )
if __name__=="__main__":
    so_many_last_days_for_level_calculation = 252
    db_with_daily_ohlcv_stock_data="stocks_ohlcv_daily"
    db_where_levels_formed_by_high_or_low_will_be=\
        "round_levels_formed_by_highs_and_lows_for_stocks"

    # table_where_levels_formed_by_highs_will_be="levels_formed_by_highs"
    table_where_levels_formed_by_highs_will_be="levels_formed_by_highs"
    so_many_number_of_touches_of_level_by_highs = 2
    so_many_number_of_touches_of_level_by_lows = 2
    find_round_levels_formed_by_highs_for_stocks_and_insert_into_db(so_many_number_of_touches_of_level_by_highs,
                                                                    so_many_number_of_touches_of_level_by_lows,
                                         db_with_daily_ohlcv_stock_data,
                                         db_where_levels_formed_by_high_or_low_will_be,

                                         table_where_levels_formed_by_highs_will_be,
                                         so_many_last_days_for_level_calculation)
