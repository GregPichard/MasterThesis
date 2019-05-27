#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 22 17:25:42 2019
Adapted from script 'Stocks_DailyVolatility.py'

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
import xarray
import datetime
print("Loading Price and volume data (large table > 9.5 GB)")
#Stocks_PriceVol_Daily_db = pd.read_csv('Daily/IntlStockPrice-volume_Series_daily_db.csv', header = None, index_col = 0)
#Stocks_PriceVol_Daily_db.info()
#Stocks_PriceVol_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
#Stocks_PriceVol_Daily_db.Date = pd.to_datetime(Stocks_PriceVol_Daily_db.Date, infer_datetime_format = True)
#for col in list(Stocks_PriceVol_Daily_db[['RIC', 'Variable']].columns):
#    Stocks_PriceVol_Daily_db[col] = Stocks_PriceVol_Daily_db[col].astype('category')
#Stocks_PriceVol_Daily_db.set_index(['Date', 'RIC', 'Variable'], inplace=True)
#print("Generating Close Prices panel")
# Close Price
#Stocks_Close_Daily_db = Stocks_PriceVol_Daily_db.xs('CLOSE', level=2)
#Stocks_Close_Daily_db.to_csv('Daily/IntlStocks_Close_Daily_db.csv')
# Volume
#print("Generating (daily) Volume panel")
#Stocks_Volume_Daily_db = Stocks_PriceVol_Daily_db.xs('VOLUME', level=2)
#del Stocks_PriceVol_Daily_db
# Close Price
Stocks_Close_Daily_db = pd.read_csv('Daily/IntlStocks_Close_Daily_db.csv', header = None)
Stocks_Close_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
Stocks_Close_Daily_db.drop(columns=[0, 'Variable'], inplace=True)
Stocks_Close_Daily_db.Date = pd.to_datetime(Stocks_Close_Daily_db.Date, infer_datetime_format = True)
print("Generating Close Prices panel")
Stocks_Close_DailyPanel_db = pd.pivot_table(Stocks_Close_Daily_db.reset_index(), values = 'Value', index = 'Date', columns = 'RIC', fill_value = 0)
del Stocks_Close_Daily_db

# Volume
Stocks_Volume_Daily_db = pd.read_csv('Daily/IntlStocks_Volume_Daily_db.csv', header = None)
Stocks_Volume_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
Stocks_Volume_Daily_db.drop(columns=[0, 'Variable'], inplace=True)
Stocks_Volume_Daily_db.Date = pd.to_datetime(Stocks_Volume_Daily_db.Date, infer_datetime_format = True)
Stocks_Volume_DailyPanel_db = pd.pivot_table(Stocks_Volume_Daily_db.reset_index(), values = 'Value', index = 'Date', columns = 'RIC', fill_value = 0)
del Stocks_Volume_Daily_db
# Aggregation at the monthly frequency
Stocks_Volume_MonthlyPanel_db = Stocks_Volume_DailyPanel_db.resample('M', axis = 0).sum()
Stocks_Volume_MonthlyPanel_db.to_csv('Monthly/IntlStocks_Volume_LongMerged_db.csv')
del Stocks_Volume_MonthlyPanel_db

# Back to Close price : returns and volatility
print("Generating Percentage daily return panel")
Stocks_ReturnClose_DailyPanel = Stocks_Close_DailyPanel_db.pct_change().drop(pd.to_datetime('1999-08-02'), axis=0)
print("Generating Monthly volatility panel")
Stocks_ReturnCloseVolatility_MonthlyPanel = Stocks_ReturnClose_DailyPanel.resample('M', axis=0).std()
Stocks_ReturnCloseVolatility_MonthlyPanel.to_csv('Monthly/IntlStocks_DVolatility_MFreq_WidePanel.csv', index = True, header = True)
# Loading the VWAP data set
print("Loading VWAP data (large table > 2.3 GB)")
Stocks_VWAP_Daily_db = pd.read_csv('Daily/IntlStockVWAP_Series_daily_db.csv', header = None, index_col = 0)
Stocks_VWAP_Daily_db.info()
Stocks_VWAP_Daily_db.rename({1:'RIC', 2:'Date', 3:'VWAP'}, axis = 'columns', inplace = True)
Stocks_VWAP_Daily_db.Date = pd.to_datetime(Stocks_VWAP_Daily_db.Date, infer_datetime_format = True)
print("Generating VWAP panel")
Stocks_VWAP_DailyPanel_db = pd.pivot_table(Stocks_VWAP_Daily_db, values = 'VWAP', index = 'Date', columns = 'RIC', fill_value = 0)
del Stocks_VWAP_Daily_db
# As there are less stocks in the VWAP data set than elsewhere (precisely 16377 vs 16539 (Price) and 16544 (Volume)), those that are only present in the latter tables are excluded from further analysis
# Intersection
CommonIntlStocks = set(Stocks_VWAP_DailyPanel_db.columns).intersection(set(Stocks_Volume_DailyPanel_db.columns))
CommonIntlStocks = CommonIntlStocks.intersection(set(Stocks_ReturnCloseVolatility_MonthlyPanel.columns))
Stocks_VWAP_DailyPanel_db = Stocks_VWAP_DailyPanel_db.filter(list(CommonIntlStocks), axis = 1)
Stocks_Volume_DailyPanel_db = Stocks_Volume_DailyPanel_db.filter(list(CommonIntlStocks), axis = 1)
Stocks_ReturnClose_DailyPanel = Stocks_ReturnClose_DailyPanel.filter(list(CommonIntlStocks), axis = 1)
# Length (time dimension) of the three samples do not match : intersection order to divide by same-sized arrays
# Intersection
CommonDates = set(Stocks_VWAP_DailyPanel_db.index).intersection(set(Stocks_Volume_DailyPanel_db.index))
CommonDates = CommonDates.intersection(set(Stocks_Volume_DailyPanel_db.index))
Stocks_VWAP_DailyPanel_db = Stocks_VWAP_DailyPanel_db.filter(list(CommonDates), axis = 0)
Stocks_Volume_DailyPanel_db = Stocks_Volume_DailyPanel_db.filter(list(CommonDates), axis = 0)
Stocks_ReturnClose_DailyPanel = Stocks_ReturnClose_DailyPanel.filter(list(CommonDates), axis = 0)


# Computing the Amihud illiquidity ratio over calendar months
Stocks_AbsReturnOverVolume_DailyPanel = abs(Stocks_ReturnClose_DailyPanel)/(Stocks_Volume_DailyPanel_db * Stocks_VWAP_DailyPanel_db)
Stocks_AmihudRatio_MonthlyPanel = Stocks_AbsReturnOverVolume_DailyPanel.resample('M', axis=0).sum()/Stocks_ReturnClose_DailyPanel.resample('M', axis=0).count()
Stocks_AmihudRatio_MonthlyPanel.to_csv('Monthly/IntlStocks_AmihudRatio_WidePanel.csv', index = True, header = True)
#Stocks_PriceVol_db['Year'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).year
#Stocks_PriceVol_db['Month'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).month
#Stocks_PriceVol_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(Stocks_PriceVol_db['Year'], Stocks_PriceVol_db['Month'])]
