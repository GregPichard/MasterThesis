#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:06:33 2019
Adapted from 'StocksAdditionalVariables_monthly.py'
@author: gpichard
"""

import os
file_path = os.path.realpath(__file__)
dir_path = os.path.dirname(file_path)
if os.getcwd() != dir_path:
    print("Need to relocate current working directory")
    os.chdir(dir_path)

import pandas as pd
import numpy as np
import datetime

Stocks_AdditionalVariables_db = pd.read_csv('Monthly/AdditionalVariables_IntlStocks_Monthly_db.csv', header = None, index_col = 0)
Stocks_AdditionalVariables_db.info()
Stocks_AdditionalVariables_db.rename({1:'RIC', 2:'Date', 3:'CompanyMarketCap', 4:'Bid', 5:'Ask', 6:'Close', 7:'Volume', 8:'TotalReturn52Wk', 9:'PriceToBVPerShare', 10:'GrossProfit', 11:'TotalAssetsReported'}, axis = 'columns', inplace = True)
Stocks_AdditionalVariables_db.Date = pd.to_datetime(Stocks_AdditionalVariables_db.Date, infer_datetime_format = True)
Stocks_ControlVariables_db = Stocks_AdditionalVariables_db.filter(['Date', 'RIC', 'Close', 'CompanyMarketCap', 'PriceToBVPerShare']).dropna()
Stocks_ControlVariables_db.set_index(['Date', 'RIC'], drop = True, inplace=True)
Stocks_ControlVariables_db.to_csv('Monthly/IntlStockControlVariables_monthly_db.csv', index = True, header = True)
# Loading time series downloaded through script 'StocksPrice-volume_Series_monthly_query.py'
Stocks_PriceVol_db = pd.read_csv('Monthly/IntlStockPrice-volume_Series_monthly_db.csv', header = None, index_col = 0)
Stocks_PriceVol_db.info()
Stocks_PriceVol_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
Stocks_PriceVol_db.Date = pd.to_datetime(Stocks_PriceVol_db.Date, infer_datetime_format = True)
for col in list(Stocks_PriceVol_db[['RIC', 'Variable']].columns):
    Stocks_PriceVol_db[col] = Stocks_PriceVol_db[col].astype('category')
Stocks_PriceVol_db['Year'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).year
Stocks_PriceVol_db['Month'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).month
Stocks_PriceVol_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(Stocks_PriceVol_db['Year'], Stocks_PriceVol_db['Month'])]
Stocks_PriceVolPanel_db = Stocks_PriceVol_db.pivot_table('Value', index = ['YearMonth', 'Variable'], columns='RIC', fill_value = 0)
Stocks_PriceVolPanel_db.info()
Stocks_PriceVolPanel_db.shape

# How to slice the panel :
# All price series for stock with RIC 'A.N': Stocks_PriceVolPanel_db['A.N'].unstack()
# All data across stocks for Year-Month end '2018-12' : Stocks_PriceVolPanel_db.xs('2018-12')
# All Close price series : Stocks_PriceVolPanel_db.xs('CLOSE', level=1)
Stocks_ClosePricePanel_db = Stocks_PriceVolPanel_db.xs('CLOSE', level=1)
Stocks_VolumePanel_db = Stocks_PriceVolPanel_db.xs('VOLUME', level=1)
# The amount of non-null values (resp. 595120 and 59127) is higher than in the common ('Additional variables') query.

# "Melting" the datasets from wide to long - preparing them for the panel regression analysis
YearMonth_Close = list(Stocks_ClosePricePanel_db.index)
Stocks_ClosePricePanel_db = Stocks_ClosePricePanel_db.reset_index(drop=True)
Stocks_ClosePricePanel_db.columns = Stocks_ClosePricePanel_db.columns.add_categories('YearMonth')
Stocks_ClosePricePanel_db = Stocks_ClosePricePanel_db.assign(YearMonth = YearMonth_Close)
Stocks_ClosePrice_agg_db = pd.melt(Stocks_ClosePricePanel_db, id_vars = ['YearMonth'], var_name = 'RIC', value_name = 'CLOSE')
Stocks_ClosePrice_agg_db.set_index(['YearMonth', 'RIC'], drop = True, inplace=True)
Stocks_ClosePrice_agg_db.to_csv('Monthly/Stocks_ClosePrice_LongMerged_db.csv', index = True, header = True)
# Index set back on original db
Stocks_ClosePricePanel_db.set_index(['YearMonth'], drop = True, inplace=True)

YearMonth_Volume = list(Stocks_VolumePanel_db.index)
Stocks_VolumePanel_db = Stocks_VolumePanel_db.reset_index(drop=True)
Stocks_VolumePanel_db.columns = Stocks_VolumePanel_db.columns.add_categories('YearMonth')
Stocks_VolumePanel_db['YearMonth'] = YearMonth_Volume
Stocks_Volume_agg_db = pd.melt(Stocks_VolumePanel_db, id_vars = ['YearMonth'], var_name = 'RIC', value_name = 'VOLUME')
Stocks_Volume_agg_db.set_index(['YearMonth', 'RIC'], drop = True, inplace=True)
Stocks_Volume_agg_db.to_csv('Monthly/Stocks_Volume_LongMerged_db.csv', index = True, header = True)

# % Bid-Ask spread relative to midpoint
Stocks_BidAsk_db = Stocks_AdditionalVariables_db.filter(['RIC', 'Date', 'Bid', 'Ask']).dropna()
for col in list(Stocks_BidAsk_db[['Bid', 'Ask']].columns):
    Stocks_BidAsk_db[col] = Stocks_BidAsk_db[col].astype('float64')
Stocks_BidAsk_db = Stocks_BidAsk_db.assign(PctBidAskSpread = (Stocks_BidAsk_db.Ask - Stocks_BidAsk_db.Bid)/(0.5*(Stocks_BidAsk_db.Ask + Stocks_BidAsk_db.Bid)))
Stocks_BidAsk_db.set_index(['Date', 'RIC'], drop = True, inplace=True)
Stocks_BidAsk_db.to_csv('Monthly/IntlStocks_BidAskSpread_db.csv', index = True, header = True)
Stocks_BidAskPanel_db = Stocks_BidAsk_db.reset_index(drop = False).pivot_table('PctBidAskSpread', index = ['Date'], columns='RIC', fill_value = None)
# Gross profitability
Stocks_GrossProfit_db = Stocks_AdditionalVariables_db.filter(['RIC', 'Date', 'GrossProfit', 'TotalAssetsReported']).dropna()
Stocks_GrossProfit_db =  Stocks_GrossProfit_db.assign(GrossProfitability = Stocks_GrossProfit_db.GrossProfit/Stocks_GrossProfit_db.TotalAssetsReported)
Stocks_GrossProfit_db.set_index(['Date', 'RIC'], drop = True, inplace=True)
Stocks_GrossProfit_db.to_csv('Monthly/IntlStocks_GrossProfit_db.csv', index = True, header = True)
Stocks_GrossProfitPanel_db = Stocks_GrossProfit_db.reset_index(drop = False).pivot_table('GrossProfitability', index = ['Date'], columns='RIC', fill_value = None)
# Previous 12 to 1 months' cumulative return (for momentum factor) - prices are already adjusted for corporate actions such as stock splits
Stocks_RetPast12to1MonthsPanel_db = (Stocks_ClosePricePanel_db.shift(1) - Stocks_ClosePricePanel_db.shift(12))/Stocks_ClosePricePanel_db.shift(12)
Stocks_RetPast12to1MonthsPanel_db.to_csv('Monthly/IntlStocks_RetPast12to1Months.csv', index = True, header = True)

# Previous 12 to 7 months' cumulative return (for momentum factor) - prices are already adjusted for corporate actions such as stock splits
Stocks_RetPast12to7MonthsPanel_db = (Stocks_ClosePricePanel_db.shift(7) - Stocks_ClosePricePanel_db.shift(12))/Stocks_ClosePricePanel_db.shift(7)
Stocks_RetPast12to7MonthsPanel_db.to_csv('Monthly/IntlStocks_RetPast12to7Months.csv', index = True, header = True)
