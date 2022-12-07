import traceback
import time
import datetime

import check_if_asset_is_approaching_its_ath
import fetch_stock_names_from_finviz_with_given_filters
import fetch_ohlcv_data_for_stocks
import find_limit_levels_formed_by_highs_for_stocks_and_insert_into_db
import find_round_levels_formed_by_highs_for_stocks_and_insert_into_db
import plot_levels_formed_by_highs_which_had_certain_numbers_of_false_breakouts
import plot_round_levels_formed_by_highs_which_had_certain_numbers_of_false_breakouts
import check_if_asset_is_approaching_its_ath
import plot_stock_charts_with_levels_formed_by_ath
import main_plot_levels_formed_by_highs_with_false_breakouts_and_gaps
import main_plot_levels_formed_by_lows_with_false_breakouts_and_gaps
import main_plot_stock_charts_with_fb_of_ath_and_atl
import main_plot_stock_charts_with_rebound_situations
from multiprocessing import Process

def main_fetch_daily_ohlcv_data_for_stocks_and_insert_into_db():
    database_where_ohlcv_for_stocks_will_be = "stocks_ohlcv_daily"
    list_of_stock_names = \
        fetch_stock_names_from_finviz_with_given_filters. \
            fetch_stock_info_df_from_finviz_which_satisfy_certain_options ()
    fetch_ohlcv_data_for_stocks.fetch_ohlcv_data_for_stocks ( list_of_stock_names ,
                                  database_where_ohlcv_for_stocks_will_be )
    proc1=Process(target = main_plot_levels_formed_by_highs_with_false_breakouts_and_gaps.
                  main_plot_levels_formed_by_highs_with_false_breakouts_and_gaps)

    proc2 = Process ( target = main_plot_levels_formed_by_lows_with_false_breakouts_and_gaps.
                      main_plot_levels_formed_by_lows_with_false_breakouts_and_gaps )

    proc3 = Process ( target = main_plot_stock_charts_with_fb_of_ath_and_atl.
                      main_plot_stock_charts_with_fb_of_ath_and_atl )
    proc4 = Process ( target = main_plot_stock_charts_with_rebound_situations.
                      main_plot_stock_charts_with_rebound_situations )


    proc1.start ()
    proc2.start()
    proc3.start()
    proc4.start ()
    proc1.join ()
    proc2.join ()
    proc3.join()
    proc4.join ()

if __name__ == '__main__':
    start_time = time.time ()
    main_fetch_daily_ohlcv_data_for_stocks_and_insert_into_db()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )


