import traceback
from yahoo_finance_async import OHLC, Interval, History
import pandas as pd
from yahoo_fin import stock_info as si
import fetch_list_of_stock_names
import time
import datetime
import db_config
import asyncio
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists
import fetch_stock_names_from_finviz_with_given_filters


new_counter=1
def connect_to_postres_db(database):
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

async def async_fetch_ohlcv_for_stocks(ohlcv_data_engine,stock_name):
    ohlcv_data = pd.DataFrame ()
    global new_counter

    try:


        #ohlcv_data = si.get_data ( stock_name )
        ohlcv_data_dict=await OHLC.fetch ( symbol = stock_name,interval=Interval.DAY, history=History.MAX )
        new_counter = new_counter + 1
        print(type(ohlcv_data))
        #print(ohlcv_data)
        ohlcv_data_df=pd.DataFrame(ohlcv_data_dict['candles'])
        ohlcv_data_df["ticker"]=stock_name
        #ohlcv_data=result['candles']
        print("new_counter=",new_counter)
        print("-"*80)
        print (stock_name)
        print ( "-" * 80 )
        print (ohlcv_data_df)
        ohlcv_data_df.to_sql ( f"{stock_name}" ,
                            ohlcv_data_engine ,
                            if_exists = 'replace' )
    except:
        traceback.print_exc ()


def fetch_ohlcv_data_for_stocks(list_of_stock_names,database_where_ohlcv_for_stocks_will_be):
    engine , connection_to_ohlcv_for_usdt_pairs = \
        connect_to_postres_db ( database_where_ohlcv_for_stocks_will_be )

    loop = asyncio.get_event_loop ()
    tasks = [loop.create_task ( async_fetch_ohlcv_for_stocks ( engine , stock_name )) for
             stock_name in list_of_stock_names]

    loop.run_until_complete ( asyncio.wait ( tasks ) )
    loop.close ()


if __name__=="__main__":
    start_time = time.time ()

    # list_of_stock_names=fetch_list_of_stock_names.fetch_list_of_stock_names()
    list_of_stock_names=\
        fetch_stock_names_from_finviz_with_given_filters.\
            fetch_stock_info_df_from_finviz_which_satisfy_certain_options()
    database_where_ohlcv_for_stocks_will_be="async_stocks_ohlcv_daily"
    fetch_ohlcv_data_for_stocks(list_of_stock_names,
                                database_where_ohlcv_for_stocks_will_be)
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
