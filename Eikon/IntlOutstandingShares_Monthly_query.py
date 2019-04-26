# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 19:16:43 2019
Adapted from OutstandingShares_Monthly_query.py
@author: Grégoire
"""

if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    import datetime as dt
    import eikon
    import configparser as cp
    cfg = cp.ConfigParser()
    cfg.read('eikon.cfg')
    eikon.set_app_key(cfg['eikon']['app_id'])
    from IntlStockFundOwnership_monthly import Concat_Stocks
    
def Get_Data(eikon_iter_ric_list, date):
    OutShares, err = eikon.get_data(eikon_iter_ric_list, ['TR.BasicShrsOutAvg.calcdate', 'TR.BasicShrsOutAvg'], {'SDate':date})
    return OutShares

def Loop_Stocks(ric_list, date):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 2000
    width = ideal_width
    while initial_value < N_stocks:
        if width == 0:
            initial_value += 1
            width = ideal_width
        if initial_value + width - 1 >= N_stocks:
            end_value = None 
        else:
            end_value = initial_value + width
        eikon_iter_ric_list = ','.join(ric_list[initial_value:end_value])
        #print(eikon_iter_ric_list)
        try:
            #FundOwners = p.apply_async(Get_Data, args = (eikon_iter_ric_list, date))
            OutShares = Get_Data(eikon_iter_ric_list, date)
            #print(OutShares)
            OutShares.to_csv("Monthly/IntlOutstandingShares_Stocks_Monthly_db.csv", mode = 'a', header = False)
#            OutShares.to_hdf("Monthly/IntlOutstandingShares_Stocks_Monthly_db.hdf", key = 'out_shares', complevel = 6, complib = 'zlib')
            #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 4

def main():
    StocksRICs = Concat_Stocks('./NonUS_StocksLists/')
    StocksRICs = StocksRICs.RIC.tolist()
    #StocksRICs = np.load("Monthly/Additional41_RIC_list.npz")
    #StocksRICs = list(StocksRICs['arr_0'])
    
    mo = range(1, 13)
    dd = list([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    years = range(1999, 2019, 1)
    ref_dates = list()
    dates_list = list()
    for y in years:
        for m, month in enumerate(mo):
            dates_list.append(str(y) + "-" + str(month).zfill(2) + "-" + str(dd[m]))
            ref_dates.append(dt.datetime(y, month, dd[m], 0, 0))
    for i, date in enumerate(dates_list):
        print("Processing date : ", date)
        print(ref_dates[i])
        Loop_Stocks(StocksRICs, date)
        print("Processed date : ", date)

if __name__ == "__main__":
    main()
