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
    TS, err = eikon.get_data(iter_ric_list, ['TR.TSVWAP.calcdate', 'TR.TSVWAP'], {'SDate':'1999-08-02', 'EDate':'2018-12-31', 'Frq':'D'})
    return TS

def Loop_Stocks(ric_list):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 50
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
            TS.to_csv("D:/ETF_GP/Daily/StockVWAP_Series_daily_db.csv", mode = 'a', header = False)
            #TS.to_csv("Daily/CopyStockVWAP_Series_daily_db.csv", mode = 'a', header = False)

            #TS.to_hdf("Monthly/AdditionalOutstandingShares_Stocks_Monthly_db.hdf", key = 'out_shares', complevel = 6, complib = 'zlib')
            #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 2

def main():
    StocksRICs = pd.read_excel("ReportEikon_Stocks_US_static_20190304.xlsx", header = 0)
    StocksRICs = StocksRICs.RIC.tolist()
 #    # Initial error of not including the RICs of companies that never held ETF. Here they are (early comparison just after noticing the mistake) :
    RemainingRICs = list(set(StocksRICs).difference(set(pd.read_csv('D:/ETF_GP/Daily/StockVWAP_Series_daily_db.csv', header = None, usecols = [1], squeeze= True).unique())))
#    # Manually processed the list to a CSV file :
#    pd.Series(RemainingRICs).to_csv('RemainingRICs_StocksPrice-volume_Series.csv')
    # Unfortunately, the script has to be run twice because of subperiods. Read the record of missing RICs :
    #RemainingRICs = list(pd.read_csv('D:/ETF_GP/', header = None, usecols= [1], squeeze = True))
    Loop_Stocks(RemainingRICs)
#    Loop_Stocks(StocksRICs)

if __name__ == "__main__":
    main()
