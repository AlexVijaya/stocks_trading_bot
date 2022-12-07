import time
import traceback

import datetime
from pyfinviz.screener import Screener
def fetch_stock_info_df_from_finviz_which_satisfy_certain_options():
    print ( f"fetching stock tickers from finviz...")
    options = [Screener.IndustryOption.STOCKS_ONLY_EX_FUNDS ,
               Screener.AverageVolumeOption.OVER_300K,
               Screener.OptionShortOption.SHORTABLE]
    last_page = 160
    screener = Screener ( filter_options = options ,
                          view_option = Screener.ViewOption.OVERVIEW ,
                          pages = [x for x in range ( 1 , last_page+1 )] )

    print (" screener.main_url ")  # scraped URL
    print ( screener.main_url )
    # print ( screener.soups )  # beautiful soup object per page {1: soup, 2: soup, ...}
    # print (
    #     screener.data_frames )  # table information in a pd.DataFrame object per page {1: table_df, 2, table_df, ...}
    list_of_tickers=[]
    for number_of_df in range(1,last_page):
        stock_info_df=screener.data_frames[number_of_df]
        # soup_per_page_object=screener.soups[number_of_df]
        # print (f"fetching stock tickers from finviz"
        #        f" which satisfy certain options prom page {number_of_df}")

        try:
            # print ( stock_info_df.loc[:,"Ticker"] )
            # list_of_tickers.append(stock_info_df["Ticker"])
            list_of_tickers_from_one_page=stock_info_df.loc[:,"Ticker"].tolist()
            # print("soup_per_page_object")
            # print ( soup_per_page_object.prettify() )
            list_of_tickers.extend(list_of_tickers_from_one_page)
        except Exception as e:
            print (f"page number of error is {number_of_df}")
            traceback.print_exc()
    print("list_of_tickers")
    print(list_of_tickers)
    print ( "len(list_of_tickers)" )
    print ( len(list_of_tickers) )

    return list_of_tickers


if __name__=="__main__":
    start_time = time.monotonic ()
    fetch_stock_info_df_from_finviz_which_satisfy_certain_options ()
    end_time = time.monotonic ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    print ( 'overall time in hours=' , overall_time / 3600.0 )
    print ( 'overall time=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
