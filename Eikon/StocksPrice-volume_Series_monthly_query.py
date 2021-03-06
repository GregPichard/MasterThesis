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
    
def Get_Data(iter_ric_list):
    TS = eikon.get_timeseries(iter_ric_list, fields = ['TIMESTAMP','VOLUME', 'HIGH', 'LOW', 'OPEN', 'CLOSE'], start_date='1999-09-30', end_date='2018-12-31', interval='monthly', normalize=True)
    return TS

def Loop_Stocks(ric_list):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 10
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
            TS.to_csv("Monthly/StockPrice-volume_Series_monthly_db.csv", mode = 'a', header = False)
            #TS.to_hdf("Monthly/AdditionalOutstandingShares_Stocks_Monthly_db.hdf", key = 'out_shares', complevel = 6, complib = 'zlib')
            #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 2

def main():
    StocksRICs = np.load("FundOwners_ETFHeld_RIC_list.npz") # Original query for the whole list of stocks
    StocksRICs = list(StocksRICs['RIC']) 
#    StocksRICs = np.load("Monthly/Additional41_RIC_list.npz")
#    StocksRICs = list(StocksRICs['arr_0'])
 
    Loop_Stocks(StocksRICs)

 
main()
