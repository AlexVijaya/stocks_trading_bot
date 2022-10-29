import sqlite3
import pandas as pd
import os
import tzlocal
import datetime as dt
import time
import re

from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists
import db_config
# from sqlalchemy import MetaData
from sqlalchemy import inspect
import logging
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base



def connect_to_postres_db(database ):
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



def drop_duplicates_in_db(database_with_levels_formed_by_lows_duplicates,
                          table_with_duplicate_levels_formed_by_lows,
                          output_table_without_duplicates):
    start_time = time.time ()

    engine_for_stock_levels_formed_by_lows_db , connection_to_stock_levels_formed_by_lows_db = \
        connect_to_postres_db ( database_with_levels_formed_by_lows_duplicates )


    levels_formed_by_lows_df=pd.read_sql_query(f"select * from {table_with_duplicate_levels_formed_by_lows}",
                                       connection_to_stock_levels_formed_by_lows_db)
    # levels_formed_by_lows_df["numerical_index"]=list(range(len(levels_formed_by_lows_df)))
    # levels_formed_by_lows_df.set_index("numerical_index",inplace = True)
    print("number of rows in table with levels formed by lows where there might be duplicates=",
          len(levels_formed_by_lows_df))
    print("levels_formed_by_lows_df")
    print ( levels_formed_by_lows_df.head(10).to_string() )

    levels_formed_by_lows_df.drop_duplicates(subset = ["ticker","exchange","level_formed_by_low"],
                                     keep = "first",
                                     inplace = True)
    # create additional boolian coulumn
    # which says if there are multiple mirror levels in this pair
    levels_formed_by_lows_df["pair_with_multiple_levels_formed_by_lows_on_one_exchange"]=\
        levels_formed_by_lows_df.duplicated(subset=["ticker","exchange"],keep=False)

    levels_formed_by_lows_df.reset_index(inplace = True,drop = True)
    print("len(levels_formed_by_lows_df.index)=",len(levels_formed_by_lows_df.index))

    #drop pairs which begin with usd
    # for row in range(0,len(levels_formed_by_lows_df.index)):
    #     print(row)
    #     trading_pair_string=levels_formed_by_lows_df.loc[row,"stock_ticker"]
    #     print(levels_formed_by_lows_df.loc[row,"stock_ticker"])
    #     # print(re.search('^USD.*',trading_pair_string))
    #     # if re.search('^US[D,T].*',trading_pair_string):
    #     #     levels_formed_by_lows_df.loc[row , "not_begins_with_USD_or_UST"]=False
    #     # else:
    #     #     levels_formed_by_lows_df.loc[row , "not_begins_with_USD_or_UST"] = True

    # levels_formed_by_lows_df=levels_formed_by_lows_df[levels_formed_by_lows_df["not_begins_with_USD_or_UST"]==True]
    # levels_formed_by_lows_df.reset_index ( inplace = True , drop = True )

    levels_formed_by_lows_df.to_sql(f"{output_table_without_duplicates}",
                            connection_to_stock_levels_formed_by_lows_db,
                            if_exists ='replace')
    print ( "number of trading pairs without duplicates=" ,
            len ( levels_formed_by_lows_df ) )
    connection_to_stock_levels_formed_by_lows_db.close()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( dt.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time=' , start_time )
    print ( 'end_time=' , end_time )
    unix_timestamp_start = float ( start_time )
    unix_timestamp_end = float ( end_time )
    local_timezone = tzlocal.get_localzone ()  # get pytz timezone
    local_time_start = dt.datetime.fromtimestamp ( unix_timestamp_start , local_timezone )
    local_time_end = dt.datetime.fromtimestamp ( unix_timestamp_end , local_timezone )
    print ( 'local_time_start=' , local_time_start )
    print ( 'local_time_end=' , local_time_end )
    pass
if __name__=="__main__":
    database_with_levels_formed_by_lows_duplicates="round_levels_formed_by_highs_and_lows_for_stocks"
    table_with_duplicate_levels_formed_by_lows="levels_formed_by_lows"
    output_table_without_duplicates="levels_formed_by_lows_without_duplicates"
    drop_duplicates_in_db(database_with_levels_formed_by_lows_duplicates,
                          table_with_duplicate_levels_formed_by_lows,
                          output_table_without_duplicates)