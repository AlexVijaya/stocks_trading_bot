import traceback
from pandas import DataFrame
import pandas as pd
from yahoo_fin import stock_info as si
import fetch_list_of_stock_names
import time
import datetime
import db_config
import yfinance as yf
import fetch_stock_names_from_finviz_with_given_filters
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists

def connect_to_postres_db_without_deleting_it_first(database:str):
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




def connect_to_postres_db_with_deleting_it_first(database):
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




def fetch_ohlcv_data_for_stocks(list_of_stock_names,
                                database_where_ohlcv_for_stocks_will_be):
    engine_for_ohlcv_tables , connection_to_ohlcv_for_usdt_pairs = \
        connect_to_postres_db_with_deleting_it_first ( database_where_ohlcv_for_stocks_will_be)

    engine_for_stock_info , connection_to_stock_info = \
        connect_to_postres_db_without_deleting_it_first ( "joint_table_of_all_stock_info_db" )
    stock_info_df = pd.read_sql_table ( "joint_table_of_all_stock_info" ,
                                        engine_for_stock_info )
    for number_of_stock,stock_name in enumerate(list_of_stock_names):
        ohlcv_data_df=pd.DataFrame()
        # print("stock_info_df")
        # print(stock_info_df)
        exchange = "not_known"
        short_name=country=long_name=sector=long_business_summary=\
        website=quote_type=city=exchange_timezone_name=industry=market_cap=""
        try:


            exchange=stock_info_df['exchange'][stock_info_df['symbol'] == stock_name].values[0]
            short_name = stock_info_df['shortName'][stock_info_df['symbol'] == stock_name].values[0]
            country = stock_info_df['country'][stock_info_df['symbol'] == stock_name].values[0]
            long_name = stock_info_df['longName'][stock_info_df['symbol'] == stock_name].values[0]
            sector = stock_info_df['sector'][stock_info_df['symbol'] == stock_name].values[0]
            # long_business_summary = stock_info_df['longBusinessSummary'][stock_info_df['symbol'] == stock_name].values[0]
            website = stock_info_df['website'][stock_info_df['symbol'] == stock_name].values[
                0]
            quote_type = stock_info_df['quoteType'][stock_info_df['symbol'] == stock_name].values[0]
            city = stock_info_df['city'][stock_info_df['symbol'] == stock_name].values[0]
            exchange_timezone_name = stock_info_df['exchangeTimezoneName'][stock_info_df['symbol'] == stock_name].values[0]
            industry = \
            stock_info_df['industry'][stock_info_df['symbol'] == stock_name].values[0]
            market_cap = stock_info_df['marketCap'][stock_info_df['symbol'] == stock_name].values[0]

            # print("exchange")
            # print(exchange)
        except:
            traceback.print_exc()

        try:
            ohlcv_data_df=si.get_data(stock_name,start_date = "01/01/2010")

            #ticker=yf.Ticker(stock_name)
            #exchange=ticker['exchange']
            #print("ticker.info.exchange=",ticker.info["exchange"])
            #ohlcv_data_df["exchange"]=ticker.info["exchange"]
            # ohlcv_data_df['trading_pair'] = ohlcv_data_df["ticker"]
            ohlcv_data_df['Timestamp'] = \
                [datetime.datetime.timestamp ( x ) for x in ohlcv_data_df.index]
            ohlcv_data_df["open_time"] = ohlcv_data_df.index
            ohlcv_data_df.index=range(0,len(ohlcv_data_df))
            # ohlcv_data_df = populate_dataframe_with_td_indicator ( ohlcv_data_df )

            ohlcv_data_df["exchange"] = exchange
            ohlcv_data_df["short_name"] = short_name
            ohlcv_data_df["country"] = country
            ohlcv_data_df["long_name"] = long_name
            ohlcv_data_df["sector"] = sector
            # ohlcv_data_df["long_business_summary"] = long_business_summary
            ohlcv_data_df["website"] = website
            ohlcv_data_df["quote_type"] = quote_type
            ohlcv_data_df["city"] = city
            ohlcv_data_df["exchange_timezone_name"] = exchange_timezone_name
            ohlcv_data_df["industry"] = industry
            ohlcv_data_df["market_cap"] = market_cap


            ohlcv_data_df.set_index("open_time")

            ohlcv_data_df.to_sql ( f"{stock_name}" ,
                             engine_for_ohlcv_tables ,
                             if_exists = 'replace' )
        except:
            traceback.print_exc()
        print(f"{stock_name} is number {number_of_stock} out of {len(list_of_stock_names)}")
        print(type(ohlcv_data_df))
        print ( "ohlcv_data_df" )
        print ( ohlcv_data_df.tail(5).to_string() )

    connection_to_ohlcv_for_usdt_pairs.close()

def populate_dataframe_with_td_indicator(dataframe) -> DataFrame:
    """
    Adds several different TA indicators to the given DataFrame
    Performance Note: For the best performance be frugal on the number of indicators
    you are using. Let uncomment only the indicator you are using in your strategies
    or your hyperopt configuration, otherwise you will waste your memory and CPU usage.
    :param dataframe: Raw data from the exchange and parsed by parse_ticker_dataframe()
    :param metadata: Additional information, like the currently traded pair
    :return: a Dataframe with all mandatory indicators for the strategies
    """

    dataframe['exceed_high'] = False
    dataframe['exceed_low'] = False

    # count consecutive closes “lower” than the close 4 bars prior.
    dataframe['seq_buy'] = dataframe['close'] < dataframe['close'].shift(4)
    dataframe['seq_buy'] = dataframe['seq_buy'] * (dataframe['seq_buy'].groupby(
        (dataframe['seq_buy'] != dataframe['seq_buy'].shift()).cumsum()).cumcount() + 1)

    # count consecutive closes “higher” than the close 4 bars prior.
    dataframe['seq_sell'] = dataframe['close'] > dataframe['close'].shift(4)
    dataframe['seq_sell'] = dataframe['seq_sell'] * (dataframe['seq_sell'].groupby(
        (dataframe['seq_sell'] != dataframe['seq_sell'].shift()).cumsum()).cumcount() + 1)

    for index, row in dataframe.iterrows():
        # check if the low of bars 6 and 7 in the count are exceeded by the low of bars 8 or 9.
        seq_b = row['seq_buy']
        if seq_b == 8:
            dataframe.loc[index, 'exceed_low'] = (row['low'] < dataframe.loc[index - 2, 'low']) | \
                                (row['low'] < dataframe.loc[index - 1, 'low'])
        if seq_b > 8:
            dataframe.loc[index, 'exceed_low'] = (row['low'] < dataframe.loc[index - 3 - (seq_b - 9), 'low']) | \
                                (row['low'] < dataframe.loc[index - 2 - (seq_b - 9), 'low'])
            if seq_b == 9:
                dataframe.loc[index, 'exceed_low'] = row['exceed_low'] | dataframe.loc[index-1, 'exceed_low']

        # check if the high of bars 6 and 7 in the count are exceeded by the high of bars 8 or 9.
        seq_s = row['seq_sell']
        if seq_s == 8:
            dataframe.loc[index, 'exceed_high'] = (row['high'] > dataframe.loc[index - 2, 'high']) | \
                                (row['high'] > dataframe.loc[index - 1, 'high'])
        if seq_s > 8:
            dataframe.loc[index, 'exceed_high'] = (row['high'] > dataframe.loc[index - 3 - (seq_s - 9), 'high']) | \
                                (row['high'] > dataframe.loc[index - 2 - (seq_s - 9), 'high'])
            if seq_s == 9:
                dataframe.loc[index, 'exceed_high'] = row['exceed_high'] | dataframe.loc[index-1, 'exceed_high']

    return dataframe


if __name__=="__main__":
    start_time = time.time ()
    # list_of_stock_names=fetch_list_of_stock_names.fetch_list_of_stock_names()
    database_where_ohlcv_for_stocks_will_be="stocks_ohlcv_daily"
    list_of_stock_names = \
        fetch_stock_names_from_finviz_with_given_filters. \
            fetch_stock_info_df_from_finviz_which_satisfy_certain_options ()
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
