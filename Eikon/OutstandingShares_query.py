# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 19:16:43 2019

@author: Grégoire
"""

if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    import eikon
    import configparser as cp
    cfg = cp.ConfigParser()
    cfg.read('eikon.cfg')
    eikon.set_app_key(cfg['eikon']['app_id'])
    
def Get_Data(eikon_iter_ric_list, date):
    OutShares, err = eikon.get_data(eikon_iter_ric_list, ['TR.SharesOutstanding'], {'SDate':date})
    return OutShares

def Loop_Stocks(ric_list, date):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 100
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
        print(eikon_iter_ric_list)
        try:
            #FundOwners = p.apply_async(Get_Data, args = (eikon_iter_ric_list, date))
            OutShares = Get_Data(eikon_iter_ric_list, date)
            print(OutShares)
            OutShares.to_csv("OutstandingShares_Stocks_db.csv", mode = 'a', header = False)
            OutShares.to_hdf("OutstandingShares_Stocks_db.hdf", key = 'out_shares', complevel = 6, complib = 'zlib')
            #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 4

def main:
    StocksRICs = np.load("FundOwners_ETFHeld_RIC_list.npz")
    StocksRICs = list(StocksRICs['RIC'])
    
    mo = 12
    dd = 31
    years = range(1999, 2019, 1)
    ref_dates = list()
    dates_list = list()
    for y in years:
        dates_list.append(str(y) + "-" + str(mo) + "-" + str(dd))
        ref_dates.append(dt.datetime(y, mo, dd, 0, 0))
    for i, date in enumerate(dates_list):
        Loop_Stocks(StocksRICs, date)

    
main()
