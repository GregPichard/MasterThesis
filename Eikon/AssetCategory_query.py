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
    Output, err = eikon.get_data(eikon_iter_ric_list, "TR.AssetCategory")
    return Output

def Loop_Stocks(ric_list):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 5000
    width = ideal_width
    AssetCategory = pd.DataFrame()
    while initial_value < N_stocks:
        if width == 0:
            initial_value += 1
            width = ideal_width
        if initial_value + width - 1 >= N_stocks:
            end_value = None 
        else:
            end_value = initial_value + width
        eikon_iter_ric_list = ', '.join(ric_list[initial_value:end_value])
        try:
            time.sleep(1)
            AssetCategory = pd.concat([AssetCategory, Get_Data(eikon_iter_ric_list)], ignore_index = True)
            #Symbols.to_csv("D:/ETF_GP/Symbology_db.csv", mode = 'a', header = False)
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 4
    return AssetCategory

def Export_Summary(MergedRICs):
    Summary = pd.crosstab(index=MergedRICs['Asset Category Description'].astype('category'), columns='count')
    Summary.reset_index(drop = False, inplace = True)
    Summary.sort_values(by = ['count', 'Asset Category Description'], ascending=[False, True], inplace= True)
    del Summary.columns.name
    Summary.set_index('Asset Category Description', drop = True, inplace = True)
    Summary.rename({'count':'# of entities'}, axis = 1, inplace = True)
    print(Summary)
    Summary.to_latex('../SummaryStats/StocksAssetCategories.tex')

def main():
    MergedRICs = pd.read_csv('MergedStocksLists.csv', header = 0, index_col = list([0, 1]))
    AssetCategory = Loop_Stocks(list(MergedRICs.RIC))
    print(AssetCategory)
    MergedRICs = MergedRICs.merge(AssetCategory, how = 'left', left_on = 'RIC', right_on = 'Instrument')
    MergedRICs.to_csv('MergedStocksLists.csv', header = True, index = True)
    Export_Summary(MergedRICs)


if __name__ == "__main__":    
    main()
