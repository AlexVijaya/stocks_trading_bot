import fetch_list_of_stock_names
import fetch_ohlcv_data_for_stocks
import find_mirror_levels_if_last_low_or_high_is_equal_to_mirror_level
import time
import datetime
import drop_duplicate_mirror_levels_for_stocks
import plot_mirror_levels_for_stocks
import fetch_stock_names_from_finviz_with_given_filters

def main_plot_mirror_levels_if_they_were_equal_to_last_high_or_low():

    # list_of_stock_names = fetch_list_of_stock_names.fetch_list_of_stock_names ()
    list_of_stock_names = \
        fetch_stock_names_from_finviz_with_given_filters. \
            fetch_stock_info_df_from_finviz_which_satisfy_certain_options ()
    fetch_ohlcv_data_for_stocks.fetch_ohlcv_data_for_stocks ( list_of_stock_names )

    database_where_stock_ohlcv_data_for_daily_df_is_contained = "stocks_ohlcv_daily"
    db_where_mirror_levels_will_be_contained = "mirror_level_for_stocks_db"
    table_where_mirror_levels_equal_to_last_high_or_low_will_be_contained = \
        "mirror_level_db_if_last_high_or_low_equal_to_it"
    find_mirror_levels_if_last_low_or_high_is_equal_to_mirror_level.find_mirror_levels_in_database(database_where_stock_ohlcv_data_for_daily_df_is_contained ,
                                     db_where_mirror_levels_will_be_contained ,
                                     table_where_mirror_levels_equal_to_last_high_or_low_will_be_contained )

    database_with_mirror_level_duplicates = "mirror_level_for_stocks_db"
    table_with_duplicate_mirror_levels = "mirror_level_db_if_last_high_or_low_equal_to_it"
    output_table_without_duplicates = "mirror_level_if_last_h_or_l_equal_to_it_no_duplicates"
    drop_duplicate_mirror_levels_for_stocks.drop_duplicates_in_db ( database_with_mirror_level_duplicates ,
                            table_with_duplicate_mirror_levels ,
                            output_table_without_duplicates )

    name_of_folder_where_plots_will_be = 'all_mirror_levels_in_stocks'
    db_where_mirror_levels_are_stored = "mirror_level_for_stocks_db"
    table_where_mirror_levels_are_stored = "mirror_level_if_last_h_or_l_equal_to_it_no_duplicates"
    plot_mirror_levels_for_stocks.plot_ohlcv_chart_with_mirror_levels_from_given_exchange ( name_of_folder_where_plots_will_be ,
                                                              db_where_mirror_levels_are_stored ,
                                                              table_where_mirror_levels_are_stored )



if __name__ == '__main__':
    start_time = time.time ()
    main_plot_mirror_levels_if_they_were_equal_to_last_high_or_low()
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time of the main program in minutes=' , overall_time / 60.0 )
    print ( 'overall time of the main program in hours=' , overall_time / 3600.0 )
    print ( 'overall time of the main program=' , str ( datetime.timedelta ( seconds = overall_time ) ) )
    print ( 'start_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( start_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )
    print ( 'end_time of the main program=' ,
            datetime.datetime.utcfromtimestamp ( end_time ).strftime ( '%Y-%m-%dT%H:%M:%SZ' ) )


