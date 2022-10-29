
import pandas as pd
import os
import time
import datetime
import traceback
import datetime as dt
import tzlocal

from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

def check_if_mirror_level_coincides_with_last_low(last_several_days_lows_slice_Series,
                                                    row_number_in_lows,
                                                    daily_low):
    if row_number_in_lows == last_several_days_lows_slice_Series.size - 1:
        print ( f"low_is_the_last_in_ohlcv_table with daily low = {daily_low}" )
        return True
    else:
        return False

def check_if_mirror_level_coincides_with_last_high(last_several_days_highs_slice_Series,
                                                    row_number_in_highs,
                                                    daily_high):
    if row_number_in_highs == last_several_days_highs_slice_Series.size - 1:
        print ( f"high_is_the_last_in_ohlcv_table with daily high = {daily_high}" )
        return True
    else:
        return False



def connect_to_postres_db_without_first_deleting_it(database = "mirror_level_for_stocks_db"):
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

def find_if_mirror_level_is_round(mirror_level):
    mirror_level = str ( mirror_level )
    mirror_level_is_round=False

    if "." in mirror_level:  # quick check if it is decimal
        decimal_part = mirror_level.split ( "." )[1]
        # print(f"decimal part of {mirror_level} is {decimal_part}")
        if decimal_part=="0":
            print(f"mirror level is round")
            print ( f"decimal part of {mirror_level} is {decimal_part}" )
            mirror_level_is_round = True
            return mirror_level_is_round
        elif decimal_part=="25":
            print(f"mirror level is round")
            print ( f"decimal part of {mirror_level} is {decimal_part}" )
            mirror_level_is_round = True
            return mirror_level_is_round
        elif decimal_part == "5":
            print ( f"mirror level is round" )
            print ( f"decimal part of {mirror_level} is {decimal_part}" )
            mirror_level_is_round = True
            return mirror_level_is_round
        elif decimal_part == "75":
            print ( f"mirror level is round" )
            print ( f"decimal part of {mirror_level} is {decimal_part}" )
            mirror_level_is_round = True
            return mirror_level_is_round
        else:
            print ( f"mirror level is not round" )
            print ( f"decimal part of {mirror_level} is {decimal_part}" )
            return mirror_level_is_round



def discard_stock_if_volume_is_low(data_df,
                                   this_many_last_days_to_keep_in_df_after_slicing,
                                   calculate_average_volume_for_this_many_days,
                                   volume_limit,
                                   stock_ticker ,
                                   exchange ,
                                   last_several_days_slice_df):
    # stock_ticker = data_df.loc[0 , "ticker"]
    # exchange = data_df.loc[0 , "exchange"]
    # data_df.reset_index()
    # data_df.set_index ( "Timestamp" , inplace = True )
    data_df["volume_by_close"] = data_df["volume"] * data_df["close"]

    # calculate mirror levels and insert them into db
    # last_several_days_slice_df = data_df.tail ( this_many_last_days_to_keep_in_df_after_slicing )
    average_volume = last_several_days_slice_df[
        "volume_by_close"].tail ( calculate_average_volume_for_this_many_days ).mean ()
    print ( "average volume=" , average_volume )
    stock_has_enough_volume=True

    # discard pairs with low volume
    if average_volume < volume_limit:
        print ( f"average volume is less than {volume_limit}"
                f" for {stock_ticker} on {exchange}  " )
        
        stock_has_enough_volume=False
        return stock_has_enough_volume
    else:
        return stock_has_enough_volume

def find_mirror_levels_in_database():
    start_time = time.time ()



    engine_for_stocks_ohlcv_db, connection_to_stocks_ohlcv =\
        connect_to_postres_db_without_first_deleting_it("stocks_ohlcv_daily")
    ###############################################33
    ###############################################

    lower_timeframe_for_mirror_level_rebound_trading='1h'

    #############################################
    #############################################

    list_of_tables_from_sql_query=engine_for_stocks_ohlcv_db.execute ( '''SELECT table_name
                                                        FROM information_schema.tables
                                                        WHERE table_schema='public'
                                                        AND table_type='BASE TABLE';''' )

    # print("list_of_tables_from_sql_query\n")
    # print(list_of_tables_from_sql_query)

    # print("metadata.reflect(engine)\n")
    inspector = inspect ( engine_for_stocks_ohlcv_db )
    # print(metadata.reflect(engine_for_stocks_ohlcv_db))
    # print(inspector.get_table_names())
    list_of_tables_from_sql_query=inspector.get_table_names()
    # print ( "list_of_tables_from_sql_query\n" )
    # print ( list_of_tables_from_sql_query )


    list_of_tables=[]
    #open_connection to database with mirror levels
    engine_mirror_level_for_stocks_db , connection_to_mirror_level_for_stocks = \
        connect_to_postres_db_without_first_deleting_it ( "mirror_level_for_stocks_db" )


    mirror_level_df = pd.DataFrame ( columns = ['stock_ticker' , 'exchange' , 'mirror_level' ,
                                                'timestamp_for_low' , 'timestamp_for_high' ,
                                                'low' , 'high' , 'open_time_of_candle_with_legit_low' ,
                                                'open_time_of_candle_with_legit_high','average_volume'] )
    try:
        drop_table ( "mirror_levels_calculated_separately" ,
                                   engine_mirror_level_for_stocks_db )
        print ("\ntable dropped\n")
        #time.sleep ( 1000 )
    except Exception as e:
        print ( "cant drop table from db\n",e )
        #time.sleep(1000)

####################################################################
    ##################################################################
    list_of_tables=list_of_tables_from_sql_query
    # for table_in_db in list_of_tables_from_sql_query:
    #     #print(table_in_db[0])
    #     list_of_tables.append(table_in_db[0])

    print(list_of_tables)
    counter=0
    discard_pair_by_volume_counter = 0
    this_many_last_days_to_keep_in_df_after_slicing=90
    calculate_average_volume_for_this_many_days=30
    volume_limit=300000
    for table_in_db in list_of_tables:
        try:
            counter=counter+1
            data_df=\
                pd.read_sql_query(f'''select * from "{table_in_db}"''' ,
                                  connection_to_stocks_ohlcv)
            print ( "data_df\n",data_df )

            #data_df.set_index("Timestamp")
            #print ( "data_df\n" , data_df )
            print("---------------------------")
            print(f'{table_in_db} is number {counter} out of {len(list_of_tables)}\n' )
            #print("usdt_ohlcv_df\n",data_df )
            stock_ticker=data_df.loc[0,"ticker"]
            exchange=data_df.loc[0,"exchange"]
            data_df.reset_index()
            data_df.set_index("Timestamp", inplace = True)
            # data_df["volume_by_close"] = data_df["volume"] * data_df["close"]
            #
            # #calculate mirror levels and insert them into db
            data_df["volume_by_close"] = data_df["volume"] * data_df["close"]
            last_several_days_slice_df = data_df.tail ( this_many_last_days_to_keep_in_df_after_slicing )



            average_volume=last_several_days_slice_df[
                "volume_by_close"].tail(calculate_average_volume_for_this_many_days).mean()
            # print("average volume=", average_volume)
            #
            # #discard pairs with low volume
            # if average_volume<volume_limit:
            #     print(f"average volume is less than {volume_limit}"
            #           f" for {stock_ticker} on {exchange}  ")
            #     discard_pair_by_volume_counter=discard_pair_by_volume_counter+1
            #     print("discard_pair_by_volume_counter=",
            #           discard_pair_by_volume_counter)
            #     continue



            # discard pairs with low volume
            stock_has_enough_volume_bool=discard_stock_if_volume_is_low ( data_df ,
                                             this_many_last_days_to_keep_in_df_after_slicing ,
                                             calculate_average_volume_for_this_many_days ,
                                             volume_limit ,
                                             stock_ticker ,
                                             exchange ,
                                             last_several_days_slice_df )
            if not stock_has_enough_volume_bool:
                discard_pair_by_volume_counter = discard_pair_by_volume_counter + 1
                print ( "discard_pair_by_volume_counter=" ,
                        discard_pair_by_volume_counter )
                continue
            

           

            #last_several_days_slice_df.set_index('open_time')

            if last_several_days_slice_df.duplicated ( subset = 'high' ,
                                                       keep = False ).sum () == len ( last_several_days_slice_df ):
                print ( f"all duplicated highs are found in {stock_ticker} on {exchange}" )
                continue
            # print ( "last_several_days_slice_df=\n" , last_several_days_slice_df.to_string () )
            #
            #print ( "last_several_days_slice_df=\n" , last_several_days_slice_df.to_string () )
            last_several_days_highs_slice_Series = \
                last_several_days_slice_df['high'].squeeze ()
            last_several_days_lows_slice_Series = \
                last_several_days_slice_df['low'].squeeze ()
            # print (type(last_several_days_lows_slice_Series))
            # print ( "last_several_days_lows_slice_Series=\n" ,
            #         last_several_days_lows_slice_Series.to_string () )
            # print ( "last_several_days_highs_slice_Series=\n" ,
            #         last_several_days_highs_slice_Series.to_string () )


            for row_number_in_highs in range ( last_several_days_highs_slice_Series.size ):
                for row_number_in_lows in range ( last_several_days_lows_slice_Series.size ):
                    daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs ]
                    daily_low = last_several_days_lows_slice_Series.iloc[row_number_in_lows ]
                    if daily_high == daily_low:
                        print ( "daily_low\n" ,
                                daily_low )
                        print ( "daily_high\n" ,
                                daily_high )
                        print(f"found mirror level of {stock_ticker} with mirror level of {daily_low} ")
                        # if row_number_in_lows > 1 and row_number_in_lows < last_several_days_lows_slice_Series.size:
                        #     print ( "found not boundary low" )
                        #
                        # if row_number_in_highs > 1 and row_number_in_highs < last_several_days_highs_slice_Series.size:
                        #     print ( "found not boundary high" )
                        #     if row_number_in_lows > 1 and row_number_in_lows < last_several_days_lows_slice_Series.size:
                        #         prev_daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs - 2]
                        #         next_daily_high = last_several_days_highs_slice_Series.iloc[row_number_in_highs]
                        #         prev_daily_low = last_several_days_lows_slice_Series.iloc[
                        #             row_number_in_lows - 2]
                        #         next_daily_low = last_several_days_lows_slice_Series.iloc[row_number_in_lows]

                                #print("last_several_days_lows_slice_Series.iloc[row_number_in_lows]=\n",
                                #      last_several_days_lows_slice_Series.iloc[row_number_in_lows])

                                # print ( "prev_daily_high\n" , prev_daily_high )
                                # print ( "next_daily_high\n" , next_daily_high )
                                # print ( "prev_daily_low\n" , prev_daily_low )
                                # print ( "next_daily_low\n" , next_daily_low )
                                # if prev_daily_low > daily_low and next_daily_low > daily_low:
                                #     print ( "found legit low" )
                                #
                                # if prev_daily_high < daily_high and next_daily_high < daily_high:
                                #     print ( "found legit high" )
                                #     if prev_daily_low > daily_low and next_daily_low > daily_low:
                                #         print ( "level is legit\n" )

                                        #print ( "last_several_days_lows_slice_Series\n" ,
                                        #        last_several_days_lows_slice_Series )

                        list_of_tuples_of_lows = list ( last_several_days_lows_slice_Series.items () )
                        #print ( "list_of_tuples_of_lows\n",list_of_tuples_of_lows )
                        tuple_of_legit_low_level = list_of_tuples_of_lows[row_number_in_lows]
                        #print ( "tuple_of_legit_low_level\n" ,
                        #        tuple_of_legit_low_level )

                        list_of_tuples_of_highs = list (
                            last_several_days_highs_slice_Series.items () )
                        #print ( "list_of_tuples_of_highs\n" , list_of_tuples_of_highs )
                        tuple_of_legit_high_level = list_of_tuples_of_highs[
                            row_number_in_highs]
                        #print ("tuple_of_legit_high_level\n", tuple_of_legit_high_level)

                        # print ( "dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] )\n" ,
                        #         dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0] / 1000.0 ) )
                        #
                        # print ( "dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] )" ,
                        #         dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0] / 1000.0 ) )

                        timestamp_for_high=tuple_of_legit_high_level[0]
                        timestamp_for_low = tuple_of_legit_low_level[0]
                        mirror_level=daily_low

                        mirror_level_is_round_bool=\
                            find_if_mirror_level_is_round(mirror_level)

                        if not mirror_level_is_round_bool:
                            continue

                        # last_low_equal_to_mirror_level_bool=\
                        #     check_if_mirror_level_coincides_with_last_low(last_several_days_lows_slice_Series,
                        #                             row_number_in_lows,
                        #                             daily_low)
                        # if not last_low_equal_to_mirror_level_bool:
                        #     print("last low is not equal to mirror_level")
                        #     continue
                        # else:
                        #     mirror_level_df.loc[0 , 'last_low_is_equal_to_mirror_level'] = True
                        #
                        #
                        # last_high_equal_to_mirror_level_bool =\
                        #     check_if_mirror_level_coincides_with_last_high (last_several_days_highs_slice_Series,
                        #                             row_number_in_highs,
                        #                             daily_high)
                        #
                        # if not last_high_equal_to_mirror_level_bool:
                        #     print("last high is not equal to mirror_level")
                        #     continue
                        # else:
                        #     mirror_level_df.loc[0 , 'last_high_is_equal_to_mirror_level'] = True
                        #






                        if timestamp_for_high!=timestamp_for_low:

                            mirror_level_df.loc[0 , 'stock_ticker'] = stock_ticker
                            mirror_level_df.loc[0 , 'exchange'] = exchange
                            mirror_level_df.loc[0 , 'mirror_level'] = daily_low
                            mirror_level_df.loc[0 , 'timestamp_for_low'] = \
                                tuple_of_legit_low_level[0]
                            mirror_level_df.loc[0 , 'timestamp_for_high'] = \
                                tuple_of_legit_high_level[0]
                            mirror_level_df.loc[0 , 'low'] = daily_low
                            mirror_level_df.loc[0 , 'high'] = daily_high
                            mirror_level_df.loc[0 , 'open_time_of_candle_with_legit_low'] = \
                                dt.datetime.fromtimestamp ( tuple_of_legit_low_level[0]  )
                            mirror_level_df.loc[0 , 'open_time_of_candle_with_legit_high'] = \
                                dt.datetime.fromtimestamp ( tuple_of_legit_high_level[0]  )
                            mirror_level_df.loc[0 , 'average_volume'] = average_volume

                            print ( 'mirror_level_df\n' , mirror_level_df )





                            mirror_level_df.to_sql ( "mirror_levels_calculated_separately" ,
                                                     engine_mirror_level_for_stocks_db ,
                                                     if_exists = 'append' , index = False )

                        # timestamp_for_low=tuple_of_legit_low_level[0]
                        # timestamp_for_high = tuple_of_legit_high_level[0]
                        # mirror_level=daily_low




                        pass
        except Exception as e:
            print(f"problem with {table_in_db}", e)
            traceback.print_exc ()
            continue
    connection_to_stocks_ohlcv.close()
    connection_to_mirror_level_for_stocks.close()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )
if __name__=="__main__":

    find_mirror_levels_in_database()
