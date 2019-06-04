# -*- coding: utf-8 -*-
"""
Created on Tue Jun 4
Adapted from OutstandingShares_Monthly_query.py
@author: Grégoire
"""

if __name__ == "__main__":
    #import xlrd
    import pandas as pd
    import numpy as np
    import datetime as dt
    import eikon
    import configparser as cp
    cfg = cp.ConfigParser()
    cfg.read('eikon.cfg')
    eikon.set_app_key(cfg['eikon']['app_id'])
    
def Get_Data(eikon_iter_ric_list):
    Output, err = eikon.get_data(eikon_iter_ric_list, ['TR.FirstTradeDate'])
    return Output

def Loop_Stocks(ric_list):
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
        try:
            FirstTrade = Get_Data(eikon_iter_ric_list)
            FirstTrade.to_csv("D:/ETF_GP/Monthly/FirstTradeDate_db.csv", mode = 'a', header = False)
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 4

def main():
    StocksRICs = pd.read_excel("ReportEikon_Stocks_US_static_20190304.xlsx", header = 0)
    StocksRICs = StocksRICs.RIC.tolist()
    #RemainingRICs = list(set(StocksRICs).difference(set(pd.read_csv('D:/ETF_GP/Monthly/BasicOutstandingShares_Stocks_Monthly_db.csv', header = None, usecols = [1], squeeze= True).unique())))
#    # Manually processed the list to a CSV file :
#    pd.Series(RemainingRICs).to_csv('RemainingRICs_StocksPrice-volume_Series.csv')
    # Unfortunately, the script has to be run twice because of subperiods. Read the record of missing RICs :
    #RemainingRICs = list(pd.read_csv('D:/ETF_GP/', header = None, usecols= [1], squeeze = True))
    Loop_Stocks(StocksRICs)


if __name__ == "__main__":    
    main()
