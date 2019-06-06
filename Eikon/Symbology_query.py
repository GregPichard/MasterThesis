# -*- coding: utf-8 -*-
"""
Created on Tue Jun 4
Adapted from FirstTradeDate_query.py
@author: Grégoire
"""

if __name__ == "__main__":
    #import xlrd
    import pandas as pd
    import numpy as np
    import datetime as dt
    import eikon
    import configparser as cp
    import time
    cfg = cp.ConfigParser()
    cfg.read('eikon.cfg')
    eikon.set_app_key(cfg['eikon']['app_id'])
    from IntlStockFundOwnership_monthly import Concat_Stocks
    
def Get_Data(eikon_iter_ric_list):
    Output = eikon.get_symbology(eikon_iter_ric_list, from_symbol_type = "RIC")
    return Output

def Loop_Stocks(ric_list):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 5000
    width = ideal_width
    while initial_value < N_stocks:
        if width == 0:
            initial_value += 1
            width = ideal_width
        if initial_value + width - 1 >= N_stocks:
            end_value = None 
        else:
            end_value = initial_value + width
        eikon_iter_ric_list = ric_list[initial_value:end_value]
        try:
            time.sleep(1)
            Symbols = Get_Data(eikon_iter_ric_list)
            Symbols.to_csv("D:/ETF_GP/Symbology_db.csv", mode = 'a', header = False)
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 4

def main():
    # First aggregate US and International stocks datasets
    USStocksRICs = pd.read_excel("ReportEikon_Stocks_US_static_20190304.xlsx", header = 0)
    USStocksRICs.drop(columns = 'Company_Name.1', inplace = True)
    IntlStocksRICs = Concat_Stocks('./NonUS_StocksLists/')
    MergedRICs = pd.concat([USStocksRICs, IntlStocksRICs], keys = ['US', 'International'])
    MergedRICs.to_csv('MergedStocksLists.csv', header = True, index = True)
    Loop_Stocks(list(MergedRICs.RIC))


if __name__ == "__main__":    
    main()
