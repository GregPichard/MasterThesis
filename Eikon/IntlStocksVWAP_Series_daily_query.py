#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 19:16:43 2019
Adapted from StocksVWAP_Series_daily_query.py
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
    
def Get_Data(iter_ric_list):
    TS, err = eikon.get_data(iter_ric_list, ['TR.TSVWAP.calcdate', 'TR.TSVWAP'], {'SDate':'1999-08-02', 'EDate':'2018-12-31', 'Frq':'D'})
    return TS

def Loop_Stocks(ric_list):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 25
    width = ideal_width
    while initial_value < N_stocks:
        if width == 0:
            initial_value += 1
            width = ideal_width
        if initial_value + width - 1 >= N_stocks:
            end_value = None
        else:
            end_value = initial_value + width
        iter_ric_list = ric_list[initial_value:end_value]
        #print(eikon_iter_ric_list)
        try:
            TS = Get_Data(iter_ric_list)
            #print(TS)
            TS.to_csv("Daily/IntlStockVWAP_Series_daily_db.csv", mode = 'a', header = False)
            #TS.to_csv("Daily/CopyStockVWAP_Series_daily_db.csv", mode = 'a', header = False)

            #TS.to_hdf("Monthly/AdditionalOutstandingShares_Stocks_Monthly_db.hdf", key = 'out_shares', complevel = 6, complib = 'zlib')
            #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 2

def main():
    StocksRICs = Concat_Stocks('./NonUS_StocksLists/')
    StocksRICs = StocksRICs.RIC.tolist()
    Loop_Stocks(StocksRICs)

if __name__ == "__main__":
    main()
