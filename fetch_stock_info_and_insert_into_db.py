import traceback
import itertools
import pandas as pd
from yahoo_fin import stock_info as si
import fetch_list_of_stock_names
import time
import datetime
import db_config
import yfinance as yf
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy_utils import create_database,database_exists

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




def fetch_ohlcv_data_for_stocks(list_of_stock_names):
    engine , connection_to_ohlcv_for_usdt_pairs = \
        connect_to_postres_db_with_deleting_it_first ( "stocks_info" )
    overall_list_of_column_names=[]

    for number_of_stock,stock_name in enumerate(list_of_stock_names):
        stock_info_df=pd.DataFrame()


        try:
            #ohlcv_data_df=si.get_data(stock_name)
            ticker=yf.Ticker(stock_name)
            #exchange=ticker['exchange']
            #print("ticker.info.exchange=",ticker.info["exchange"])
            column_name_list_for_df_creation=['toCurrency', 'beta3Year', 'regularMarketOpen', 'morningStarOverallRating', 'revenuePerShare', 'sharesShort', 'trailingAnnualDividendRate', 'lastFiscalYearEnd', 'grossProfits', 'bid', 'operatingCashflow', 'pegRatio', 'trailingPE', 'shortRatio', 'isEsgPopulated', 'currentPrice', 'currentRatio', 'bondPosition', 'shortName', 'enterpriseValue', 'volume24Hr', 'operatingMargins', 'legalType', 'messageBoardId', 'averageDailyVolume10Day', 'floatShares', 'sharesShortPriorMonth', 'category', 'address1', 'ebitda', 'netIncomeToCommon', 'trailingAnnualDividendYield', 'fiftyDayAverage', 'country', 'tradeable', 'dividendYield', 'regularMarketDayLow', 'longName', 'averageVolume10days', 'numberOfAnalystOpinions', 'open', 'targetMeanPrice', 'maxAge', 'earningsGrowth', 'startDate', 'debtToEquity', 'dayHigh', 'sharesOutstanding', 'state', 'targetHighPrice', 'twoHundredDayAverage', 'gmtOffSetMilliseconds', 'regularMarketDayHigh', 'ask', 'lastMarket', 'convertiblePosition', 'exDividendDate', 'heldPercentInstitutions', 'totalCash', 'sector', 'fullTimeEmployees', 'equityHoldings', 'totalDebt', 'returnOnAssets', 'fromCurrency', 'lastSplitDate', 'payoutRatio', 'mostRecentQuarter', 'regularMarketPreviousClose', 'lastSplitFactor', 'strikePrice', 'dateShortInterest', 'sharesShortPreviousMonthDate', 'threeYearAverageReturn', 'grossMargins', 'revenueGrowth', 'shortPercentOfFloat', 'sharesPercentSharesOut', 'currency', 'market', 'fundFamily', 'openInterest', 'longBusinessSummary', 'bookValue', 'website', 'expireDate', 'holdings', 'dividendRate', 'preferredPosition', 'fiveYearAvgDividendYield', 'algorithm', 'fundInceptionDate', 'priceHint', 'impliedSharesOutstanding', 'profitMargins', 'symbol', 'maxSupply', 'forwardEps', 'companyOfficers', 'averageVolume', 'totalRevenue', 'otherPosition', 'circulatingSupply', 'regularMarketPrice', 'quoteType', 'financialCurrency', 'annualReportExpenseRatio', 'priceToSalesTrailing12Months', 'dayLow', 'city', 'bidSize', 'revenueQuarterlyGrowth', 'trailingEps', 'lastDividendDate', 'nextFiscalYearEnd', 'quickRatio', 'totalAssets', 'askSize', 'cashPosition', 'previousClose', '52WeekChange', 'volume', 'exchangeTimezoneShortName', 'ebitdaMargins', 'freeCashflow', 'targetMedianPrice', 'annualHoldingsTurnover', 'lastDividendValue', 'morningStarRiskRating', 'navPrice', 'exchange', 'fiftyTwoWeekLow', 'stockPosition', 'coinMarketCapLink', 'SandP52WeekChange', 'enterpriseToRevenue', 'lastCapGain', 'recommendationMean', 'earningsQuarterlyGrowth', 'address2', 'ytdReturn', 'phone', 'forwardPE', 'regularMarketVolume', 'sectorWeightings', 'exchangeTimezoneName', 'returnOnEquity', 'industry', 'fiveYearAverageReturn', 'recommendationKey', 'fax', 'yield', 'beta', 'totalCashPerShare', 'heldPercentInsiders', 'logo_url', 'volumeAllCurrencies', 'marketCap', 'bondRatings', 'fiftyTwoWeekHigh', 'enterpriseToEbitda', 'zip', 'targetLowPrice', 'bondHoldings', 'preMarketPrice', 'priceToBook']

            stock_info_df=pd.DataFrame([ticker.info],index=[number_of_stock],columns = column_name_list_for_df_creation)
            # print('type(stock_info_df["exchange"].iat[0])')
            # print ( type ( stock_info_df["exchange"].iat[0] ) )
            # print (  "stock_info_df.columns"  )
            # print ( stock_info_df.columns.items )
            number_of_columns_in_df=len(stock_info_df.columns)
            # print ( "number_of_columns_in_df" )
            # print ( number_of_columns_in_df )
            # print ( "list ( range ( 0 , number_of_columns_in_df ) )" )
            # print(list(range(0,number_of_columns_in_df)))
            # stock_info_df_with_dropped_list_and_dict=pd.DataFrame()
            for column_number in range(0,number_of_columns_in_df):

                # print(f"type(stock_info_df.iloc[0][column_number]) for {stock_name} and column name {stock_info_df.columns[column_number]}")
                # print(type(stock_info_df.iloc[0][column_number]))

                # print("stock_info_df[0][column_number]")
                # print ( stock_info_df.iloc[0][column_number] )
                # print(type(stock_info_df.iloc[0][column_number]))

                if isinstance ( stock_info_df.iloc[0][column_number] , dict ):
                    try:
                        # print ( "stock_info_df from if with dict" )
                        # print ( stock_info_df.to_string () )
                        # print ( "type(stock_info_df.iloc[0][column_number])" )
                        # print ( type ( stock_info_df.iloc[0][column_number] ) )
                        #stock_info_df.iloc[: , column_number] = np.NaN
                        stock_info_df[stock_info_df.columns[column_number]] = np.NaN
                        # stock_info_df.iloc[: , column_number] = np.NaN

                        # stock_info_df.drop(stock_info_df.columns[column_number],axis = 1,inplace=True)
                        # print ( "stock_info_df_with_dropped_list_and_dict from try" )
                        # print ( stock_info_df_with_dropped_list_and_dict.to_string() )
                    except Exception as e:
                        print ( f"inside problem with {stock_name}: {e}" )
                        traceback.print_exc ()



                elif type(stock_info_df.iloc[0][column_number])==list:
                    try:
                        # print ( "stock_info_df from if with list" )
                        # print ( stock_info_df.to_string () )
                        # print("type(stock_info_df.iloc[0][column_number])")
                        # print ( type(stock_info_df.iloc[0][column_number]) )
                        # stock_info_df.iloc[: , column_number] = np.NaN
                        stock_info_df[stock_info_df.columns[column_number]] = np.NaN
                        # stock_info_df.iloc[: , column_number] = np.NaN

                        #stock_info_df.drop(stock_info_df.columns[column_number],axis = 1,inplace=True)
                        # print ( "stock_info_df_with_dropped_list_and_dict from try" )
                        # print ( stock_info_df_with_dropped_list_and_dict.to_string() )
                    except Exception as e:
                        print ( f"inside problem with {stock_name}: {e}" )
                        traceback.print_exc()



                    # print("df2")
                    # print(df2.to_string())

            # for column_value in stock_info_df.columns.values:
            #     if type(column_value)==dict:
            #         print("column_value=dict")
            # ohlcv_data_df['trading_pair'] = ohlcv_data_df["ticker"]
            # ohlcv_data_df['Timestamp'] = \
            #     [datetime.datetime.timestamp ( x ) for x in ohlcv_data_df.index]
            # ohlcv_data_df["open_time"]=ohlcv_data_df.index
            # ohlcv_data_df.set_index("open_time")

            #print ( "stock_info_df" , stock_info_df )
            # if len(stock_info_df_with_dropped_list_and_dict.index)==0:
            #     print("len(stock_info_df_with_dropped_list_and_dict)")
            #     print ( len ( stock_info_df_with_dropped_list_and_dict ) )
            #     print ("stock_info_df_with_dropped_list_and_dict from if")
            #     print ( stock_info_df_with_dropped_list_and_dict.to_string() )
            #     stock_info_df.to_sql ( "stock_info" ,
            #                      engine ,
            #                      if_exists = 'replace' )
            # else:
            #     print("len(stock_info_df_with_dropped_list_and_dict)")
            #     print ( len ( stock_info_df_with_dropped_list_and_dict ) )
            #     print ( "stock_info_df_with_dropped_list_and_dict from else" )
            #     print ( stock_info_df_with_dropped_list_and_dict.to_string() )
            #     stock_info_df_with_dropped_list_and_dict.to_sql ( "stock_info" ,
            #                            engine ,
            #                            if_exists = 'replace' )
            list_of_column_names=list ( stock_info_df.columns )

            # overall_list_of_column_names = [sublist for sublist in overall_list_of_column_names]
            #overall_list_of_column_names=list ( itertools.chain.from_iterable ( overall_list_of_column_names ) )
            for column_name in list_of_column_names:
                overall_list_of_column_names.append ( column_name )
            overall_list_of_column_names=list(set(overall_list_of_column_names))

            print("overall_list_of_column_names")
            print ( overall_list_of_column_names )
            stock_info_df.to_sql ( f"{stock_name}_stock_info" ,
                                   engine ,
                                   if_exists = 'replace' )



        except Exception as e:
            print (f"problem with {stock_name}: {e}")
            traceback.print_exc()

        print ( f"stock {stock_name} is {number_of_stock} out of {len(list_of_stock_names)}" )


if __name__=="__main__":
    start_time = time.time ()
    list_of_stock_names=fetch_list_of_stock_names.fetch_list_of_stock_names()
    fetch_ohlcv_data_for_stocks(list_of_stock_names)
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
