import pandas as pd
from yahoo_fin import stock_info as si
def fetch_list_of_stock_names():


    # gather stock symbols from major US exchanges
    df1 = pd.DataFrame ( si.tickers_sp500 () )
    print("tickers_sp500 names fetched")
    df2 = pd.DataFrame ( si.tickers_nasdaq () )
    print ( "tickers_nasdaq names fetched" )
    df3 = pd.DataFrame ( si.tickers_dow () )
    print ( "tickers_dow names fetched" )
    df4 = pd.DataFrame ( si.tickers_other () )
    print ( "tickers_other names fetched" )

    # convert DataFrame to list, then to sets
    sym1 = set ( symbol for symbol in df1[0].values.tolist () )
    sym2 = set ( symbol for symbol in df2[0].values.tolist () )
    sym3 = set ( symbol for symbol in df3[0].values.tolist () )
    sym4 = set ( symbol for symbol in df4[0].values.tolist () )

    # join the 4 sets into one. Because it's a set, there will be no duplicate symbols
    symbols = set.union ( sym1 , sym2 , sym3 , sym4 )

    # Some stocks are 5 characters. Those stocks with the suffixes listed below are not of interest.
    my_list = ['W' , 'R' , 'P' , 'Q']
    delete_set = set ()
    save_set = set ()

    for symbol in symbols:
        if len ( symbol ) > 4 and symbol[-1] in my_list:
            delete_set.add ( symbol )
        else:
            save_set.add ( symbol )

    print ( f'Removed {len ( delete_set )} unqualified stock symbols...' )
    print ( f'There are {len ( save_set )} qualified stock symbols...' )
    print(save_set)
    return save_set
if __name__=="__main__":
    fetch_list_of_stock_names()