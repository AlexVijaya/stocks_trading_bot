import traceback
import time
import datetime
import search_for_tickers_with_rebound_situations
import plot_stock_charts_of_assets_with_rebound_situations_off_the_ath_in_history
import plot_stock_charts_of_assets_with_rebound_situations_off_the_atl_in_history

def main_plot_stock_charts_with_rebound_situations():
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    count_only_round_rebound_level = False
    db_where_levels_formed_by_rebound_level_will_be = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_ticker_which_had_rebound_situations_from_ath_will_be = "rebound_situations_from_ath"
    table_where_ticker_which_had_rebound_situations_from_atl_will_be = "rebound_situations_from_atl"

    if count_only_round_rebound_level:
        db_where_levels_formed_by_rebound_level_will_be = "round_levels_formed_by_highs_and_lows_for_stocks"
    # 0.05 means 5%
    acceptable_backlash = 0.05
    atr_over_this_period = 5
    advanced_atr_over_this_period = 30
    search_for_tickers_with_rebound_situations.search_for_tickers_with_rebound_situations (
        db_where_ohlcv_data_for_stocks_is_stored ,
        db_where_levels_formed_by_rebound_level_will_be ,
        table_where_ticker_which_had_rebound_situations_from_ath_will_be ,
        table_where_ticker_which_had_rebound_situations_from_atl_will_be ,
        acceptable_backlash ,
        atr_over_this_period,advanced_atr_over_this_period )

    name_of_folder_where_plots_will_be = 'levels_formed_by_rebound_off_atl'
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    db_where_levels_formed_by_rebound_off_atl_are_stored = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_rebound_off_atl_are_stored = "rebound_situations_from_atl"
    try:
        plot_stock_charts_of_assets_with_rebound_situations_off_the_atl_in_history.\
            plot_ohlcv_chart_with_levels_formed_by_rebound_off_atl ( name_of_folder_where_plots_will_be ,
                                                                 db_where_ohlcv_data_for_stocks_is_stored ,
                                                                 db_where_levels_formed_by_rebound_off_atl_are_stored ,
                                                                 table_where_levels_formed_by_rebound_off_atl_are_stored )
    except:
        traceback.print_exc ()

    name_of_folder_where_plots_will_be = 'levels_formed_by_rebound_off_ath'
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    db_where_levels_formed_by_rebound_off_ath_are_stored = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_rebound_off_ath_are_stored = "rebound_situations_from_ath"
    try:
        plot_stock_charts_of_assets_with_rebound_situations_off_the_ath_in_history.\
            plot_ohlcv_chart_with_levels_formed_by_rebound_off_ath ( name_of_folder_where_plots_will_be ,
                                                                 db_where_ohlcv_data_for_stocks_is_stored ,
                                                                 db_where_levels_formed_by_rebound_off_ath_are_stored ,
                                                                 table_where_levels_formed_by_rebound_off_ath_are_stored )
    except:
        traceback.print_exc ()



if __name__ == '__main__':
    start_time = time.time ()
    main_plot_stock_charts_with_rebound_situations()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )


