# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 19:16:43 2019
Adapted from OutstandingShares_Monthly_query.py
@author: Grégoire
"""

if __name__ == "__main__":
    import os
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
    # The sample has to be cut into two bits : from August 1999 to late January 2007 and from February 2007 to end 2018.
    #TS = eikon.get_timeseries(iter_ric_list, fields = ['TIMESTAMP','VOLUME', 'HIGH', 'LOW', 'OPEN', 'CLOSE'], start_date='1999-08-01', end_date='2007-01-31', interval='daily', normalize=True)
    TS = eikon.get_timeseries(iter_ric_list, fields = ['TIMESTAMP','VOLUME', 'HIGH', 'LOW', 'OPEN', 'CLOSE'], start_date='2007-02-01', end_date='2018-12-31', interval='daily', normalize=True)
    return TS

def Loop_Stocks(ric_list):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 1
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
            TS.to_csv("D:/ETF_GP/Daily/IntlStockPrice-volume_Series_daily_db.csv", mode = 'a', header = False)
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
    RemainingRICs = list(set(StocksRICs).difference(set(pd.read_csv('D:/ETF_GP/Daily/IntlStockPrice-volume_Series_daily_db.csv', header = None, usecols = [2], squeeze= True).unique())))
    Loop_Stocks(RemainingRICs)
if __name__ == "__main__":
    main()
