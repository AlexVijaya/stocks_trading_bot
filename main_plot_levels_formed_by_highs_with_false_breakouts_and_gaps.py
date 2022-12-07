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

def main_plot_levels_formed_by_highs_with_false_breakouts_and_gaps():
    # database_where_ohlcv_for_stocks_will_be = "stocks_ohlcv_daily"
    # list_of_stock_names = \
    #     fetch_stock_names_from_finviz_with_given_filters. \
    #         fetch_stock_info_df_from_finviz_which_satisfy_certain_options ()
    # fetch_ohlcv_data_for_stocks.fetch_ohlcv_data_for_stocks ( list_of_stock_names ,
    #                               database_where_ohlcv_for_stocks_will_be )
    # print ( "proc1 is in progress" )
    #
    # so_many_last_days_for_level_calculation = 252
    # db_with_daily_ohlcv_stock_data = "stocks_ohlcv_daily"
    # db_where_levels_formed_by_high_or_low_will_be = \
    #     "levels_formed_by_highs_and_lows_for_stocks"
    #
    #
    # table_where_levels_formed_by_highs_will_be = "levels_formed_by_highs"
    #
    # so_many_number_of_touches_of_level_by_lows = 2
    # so_many_number_of_touches_of_level_by_highs=2
    # find_limit_levels_formed_by_highs_for_stocks_and_insert_into_db.\
    #     find_limit_levels_formed_by_highs_for_stocks ( so_many_number_of_touches_of_level_by_highs,
    #                                                   so_many_number_of_touches_of_level_by_lows ,
    #                                                   db_with_daily_ohlcv_stock_data ,
    #                                                   db_where_levels_formed_by_high_or_low_will_be ,
    #
    #                                                   table_where_levels_formed_by_highs_will_be ,
    #                                                   so_many_last_days_for_level_calculation )

    # so_many_last_days_for_level_calculation = 252
    # db_with_daily_ohlcv_stock_data = "stocks_ohlcv_daily"
    # db_where_levels_formed_by_high_or_low_will_be = \
    #     "round_levels_formed_by_highs_and_lows_for_stocks"
    # table_where_levels_formed_by_highs_will_be = "levels_formed_by_highs"
    # so_many_number_of_touches_of_level_by_highs=2
    # so_many_number_of_touches_of_level_by_lows = 2
    # find_round_levels_formed_by_highs_for_stocks_and_insert_into_db.\
    #     find_round_levels_formed_by_highs_for_stocks_and_insert_into_db ( so_many_number_of_touches_of_level_by_highs,
    #                                                                      so_many_number_of_touches_of_level_by_lows ,
    #                                                                      db_with_daily_ohlcv_stock_data ,
    #                                                                      db_where_levels_formed_by_high_or_low_will_be ,
    #
    #                                                                      table_where_levels_formed_by_highs_will_be ,
    #                                                                      so_many_last_days_for_level_calculation )


    # db_where_levels_formed_by_highs_are_stored = "levels_formed_by_highs_and_lows_for_stocks"
    # db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    # for number_of_false_breakouts in range ( 0 , 6 ):
    #     table_where_levels_formed_by_highs_are_stored = f"levels_with_{number_of_false_breakouts}_number_of_highs_higher_than_level_formed_by_highs"
    #     name_of_folder_where_plots_will_be = f'levels_formed_by_highs_which_had_{number_of_false_breakouts}_number_of_highs_higher_than_the_level'
    #     try:
    #         plot_levels_formed_by_highs_which_had_certain_numbers_of_false_breakouts.\
    #             plot_ohlcv_chart_with_levels_formed_by_highs ( name_of_folder_where_plots_will_be ,
    #                                                       db_where_levels_formed_by_highs_are_stored ,
    #                                                       db_where_ohlcv_data_for_stocks_is_stored ,
    #                                                       table_where_levels_formed_by_highs_are_stored )
    #     except:
    #         traceback.print_exc ()
    #
    #
    # db_where_levels_formed_by_highs_are_stored = "round_levels_formed_by_highs_and_lows_for_stocks"
    # db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    # for number_of_false_breakouts in range ( 0 , 6 ):
    #     table_where_levels_formed_by_highs_are_stored = f"levels_with_{number_of_false_breakouts}_number_of_highs_higher_than_level_formed_by_highs"
    #     name_of_folder_where_plots_will_be = f'round_levels_formed_by_highs_which_had_{number_of_false_breakouts}_number_of_highs_higher_than_the_level'
    #     try:
    #         plot_round_levels_formed_by_highs_which_had_certain_numbers_of_false_breakouts.\
    #             plot_ohlcv_chart_with_levels_formed_by_highs ( name_of_folder_where_plots_will_be ,
    #                                                       db_where_levels_formed_by_highs_are_stored ,
    #                                                       db_where_ohlcv_data_for_stocks_is_stored ,
    #                                                       table_where_levels_formed_by_highs_are_stored )
    #     except:
    #         traceback.print_exc ()

    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    count_only_round_ath = False
    db_where_levels_formed_by_ath_will_be = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_ath_will_be = "levels_formed_by_ath"
    if count_only_round_ath:
        db_where_levels_formed_by_ath_will_be = "round_levels_formed_by_highs_and_lows_for_stocks"
    percentage_between_ath_and_closing_price = 5
    check_if_asset_is_approaching_its_ath.check_if_asset_is_approaching_its_ath ( percentage_between_ath_and_closing_price ,
                                            db_where_ohlcv_data_for_stocks_is_stored ,
                                            count_only_round_ath ,
                                            db_where_levels_formed_by_ath_will_be ,
                                            table_where_levels_formed_by_ath_will_be )

    name_of_folder_where_plots_will_be = 'levels_formed_by_ath'
    db_where_ohlcv_data_for_stocks_is_stored = "stocks_ohlcv_daily"
    db_where_levels_formed_by_ath_are_stored = "levels_formed_by_highs_and_lows_for_stocks"
    table_where_levels_formed_by_ath_are_stored = "levels_formed_by_ath"
    try:
        plot_stock_charts_with_levels_formed_by_ath.plot_ohlcv_chart_with_levels_formed_by_ath ( name_of_folder_where_plots_will_be ,
                                                      db_where_ohlcv_data_for_stocks_is_stored ,
                                                      db_where_levels_formed_by_ath_are_stored ,
                                                      table_where_levels_formed_by_ath_are_stored )
    except:
        traceback.print_exc ()

if __name__ == '__main__':
    start_time = time.time ()
    main_plot_levels_formed_by_highs_with_false_breakouts_and_gaps()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )


