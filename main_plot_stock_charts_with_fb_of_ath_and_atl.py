import traceback
import time
import datetime
import find_false_breakouts_of_ath_or_atl_insert_into_db
import plot_stock_charts_with_levels_formed_by_ath_and_broken_by_fb
import plot_stock_charts_with_levels_formed_by_atl_and_broken_by_fb

def main_plot_stock_charts_with_fb_of_ath_and_atl():
    db_with_daily_ohlcv_stock_data = "stocks_ohlcv_daily"
    db_where_ath_had_fb = "levels_formed_by_highs_and_lows_for_stocks"
    db_where_atl_had_fb = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_stocks_with_fd_of_ath_will_be = "stocks_with_fb_of_ath"
    table_where_stocks_with_fd_of_atl_will_be = "stocks_with_fb_of_atl"
    find_false_breakouts_of_ath_or_atl_insert_into_db.\
        find_false_breakouts_of_ath_and_insert_into_db ( db_with_daily_ohlcv_stock_data ,
                                                     db_where_ath_had_fb ,
                                                     db_where_atl_had_fb ,
                                                     table_where_stocks_with_fd_of_ath_will_be ,
                                                     table_where_stocks_with_fd_of_atl_will_be )

    name_of_folder_where_plots_will_be = 'levels_formed_by_ath_and_broken_by_fb'
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    db_where_levels_formed_by_ath_are_stored = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_ath_are_stored = "stocks_with_fb_of_ath"
    try:
        plot_stock_charts_with_levels_formed_by_ath_and_broken_by_fb.\
            plot_ohlcv_chart_with_levels_formed_by_ath ( name_of_folder_where_plots_will_be ,
                                                     db_where_ohlcv_data_for_stocks_is_stored ,
                                                     db_where_levels_formed_by_ath_are_stored ,
                                                     table_where_levels_formed_by_ath_are_stored )
    except:
        traceback.print_exc ()

    name_of_folder_where_plots_will_be = 'levels_formed_by_atl_and_broken_by_fb'
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    db_where_levels_formed_by_atl_are_stored = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_atl_are_stored = "stocks_with_fb_of_atl"
    try:
        plot_stock_charts_with_levels_formed_by_atl_and_broken_by_fb.\
            plot_ohlcv_chart_with_levels_formed_by_atl ( name_of_folder_where_plots_will_be ,
                                                     db_where_ohlcv_data_for_stocks_is_stored ,
                                                     db_where_levels_formed_by_atl_are_stored ,
                                                     table_where_levels_formed_by_atl_are_stored )
    except:
        traceback.print_exc ()


if __name__ == '__main__':
    start_time = time.time ()
    main_plot_stock_charts_with_fb_of_ath_and_atl()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )


