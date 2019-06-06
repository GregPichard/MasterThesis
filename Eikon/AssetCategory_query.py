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
    Summary = pd.crosstab(columns = MergedRICs['level_0'] , index= MergedRICs['Asset Category Description'], margins = True)
    Summary_resorted = Summary.reset_index().iloc[1:(-1), :].sort_values(by = ['All', 'Asset Category Description'], ascending=[False, True], na_position='last').set_index('Asset Category Description')
    Summary_resorted = pd.concat([Summary_resorted, Summary.loc[['', 'All']]])
    Column_Titles = ['US', 'International', 'All']
    Summary_resorted.reindex(columns = Column_Titles)
    del Summary_resorted.columns.name
    print(Summary_resorted)
    Summary_resorted.to_latex('../SummaryStats/StocksAssetCategories.tex')

def main():
    MergedRICs = pd.read_csv('MergedStocksLists.csv', header = 0, index_col = list([0, 1]))
    AssetCategory = Loop_Stocks(list(MergedRICs.RIC))
    print(AssetCategory)
    MergedRICs = MergedRICs.reset_index(drop = False).merge(AssetCategory, how = 'left', left_on = 'RIC', right_on = 'Instrument')
    MergedRICs.to_csv('MergedStocksLists.csv', header = True, index = True)
    Export_Summary(MergedRICs)


if __name__ == "__main__":    
    main()
