#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr  7 16:32:37 2019

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
print("Loading Price and volume data (large table > 2.5 GB)")
Stocks_PriceVol_Daily_db = pd.read_csv('Daily/StockPrice-volume_Series_daily_db.csv', header = None, index_col = 0)
Stocks_PriceVol_Daily_db.info()
Stocks_PriceVol_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
Stocks_PriceVol_Daily_db.Date = pd.to_datetime(Stocks_PriceVol_Daily_db.Date, infer_datetime_format = True)
for col in list(Stocks_PriceVol_Daily_db[['RIC', 'Variable']].columns):
    Stocks_PriceVol_Daily_db[col] = Stocks_PriceVol_Daily_db[col].astype('category')
Stocks_PriceVol_Daily_db.set_index(['Date', 'RIC', 'Variable'], inplace=True)
print("Generating Close Prices panel")
Stocks_Close_Daily_db = Stocks_PriceVol_Daily_db.xs('CLOSE', level=2)
Stocks_Close_DailyPanel_db = pd.pivot_table(Stocks_Close_Daily_db.reset_index(), values = 'Value', index = 'Date', columns = 'RIC', fill_value = 0)
del Stocks_Close_Daily_db
print("Generating (daily) Volume panel")
Stocks_Volume_Daily_db = Stocks_PriceVol_Daily_db.xs('VOLUME', level=2)
del Stocks_PriceVol_Daily_db
Stocks_Volume_DailyPanel_db = pd.pivot_table(Stocks_Volume_Daily_db.reset_index(), values = 'Value', index = 'Date', columns = 'RIC', fill_value = 0)
# Aggregation at the monthly frequency
Stocks_Volume_MonthlyPanel_db = Stocks_Volume_DailyPanel_db.resample('M', axis = 0).sum()
Stocks_Volume_MonthlyPanel_db.to_csv('Monthly/Stocks_Volume_LongMerged_db.csv')
del Stocks_Volume_Daily_db
print("Generating Percentage daily return panel")
Stocks_ReturnClose_DailyPanel = Stocks_Close_DailyPanel_db.pct_change().drop(pd.to_datetime('1999-08-02'), axis=0)
print("Generating Monthly volatility panel")
# There is an extremely large number of missing values, yielding infinite yield. Replace np.inf with np.nan before dropping all np.nan
# Replace infinite values with NaN in order to drop them afterwards
Stocks_ReturnClose_DailyPanel.replace([np.inf, -np.inf], np.nan, inplace = True)
Stocks_ReturnClose_DailyPanel.dropna(how = 'all', inplace = True)

Stocks_ReturnCloseVolatility_MonthlyPanel = Stocks_ReturnClose_DailyPanel.resample('M', axis=0).std()
Stocks_ReturnCloseVolatility_MonthlyPanel.to_csv('Monthly/Stocks_DVolatility_MFreq_WidePanel.csv', index = True, header = True)
# Loading the VWAP data set
print("Loading VWAP data (large table > 550 MB)")
Stocks_VWAP_Daily_db = pd.read_csv('Daily/StockVWAP_Series_daily_db.csv', header = None, index_col = 0)
Stocks_VWAP_Daily_db.info()
Stocks_VWAP_Daily_db.rename({1:'RIC', 2:'Date', 3:'VWAP'}, axis = 'columns', inplace = True)
Stocks_VWAP_Daily_db.Date = pd.to_datetime(Stocks_VWAP_Daily_db.Date, infer_datetime_format = True)
Stocks_VWAP_Daily_db.RIC = Stocks_VWAP_Daily_db.RIC.astype('category')
print("Generating VWAP panel")
Stocks_VWAP_DailyPanel_db = pd.pivot_table(Stocks_VWAP_Daily_db, values = 'VWAP', index = 'Date', columns = 'RIC', fill_value = 0)
del Stocks_VWAP_Daily_db
# As there are more stocks in the VWAP data set than elswhere (precisely 4119 vs 4085), those that are only present in there are excluded from further analysis
Stocks_VWAP_DailyPanel_db = Stocks_VWAP_DailyPanel_db.filter(Stocks_Volume_DailyPanel_db.columns, axis = 1)
# Dropping the first row of Volume and VWAP series in order to divide by same-sized arrays
Stocks_Volume_DailyPanel_db.drop(pd.to_datetime('1999-08-02'), axis=0, inplace=True)
Stocks_VWAP_DailyPanel_db.drop(pd.to_datetime('1999-08-02'), axis=0, inplace=True)
# Dropping one date (Christmas...) existing in returns, and whose values are worth nothing
Stocks_ReturnClose_DailyPanel.drop(pd.to_datetime('2004-12-24'), axis=0, inplace=True)
# Filtering out one stock from the return on Close price history in order to match with those in VWAP and Volume series (the company, BSB Bancorp Inc., has filed out from NASDAQ on 2019-04-01)
Stocks_ReturnClose_DailyPanel = Stocks_ReturnClose_DailyPanel.filter(Stocks_Volume_DailyPanel_db.columns, axis = 1)

# Computing the Amihud illiquidity ratio over calendar months
# For model 2 (liquidity), both the numerator and denominator will be needed
Stocks_AbsReturnOverVolume_DailyPanel = abs(Stocks_ReturnClose_DailyPanel)/(Stocks_Volume_DailyPanel_db * Stocks_VWAP_DailyPanel_db)
Stocks_AmihudNumerator_MonthlyPanel = Stocks_AbsReturnOverVolume_DailyPanel.resample('M', axis=0).sum()
Stocks_AmihudNumerator_MonthlyPanel.to_csv('Monthly/Stocks_AmihudNumerator_WidePanel.csv', index = True, header = True)
Stocks_AmihudDenominator_MonthlyPanel = Stocks_ReturnClose_DailyPanel.resample('M', axis=0).count()
Stocks_AmihudDenominator_MonthlyPanel.to_csv('Monthly/Stocks_AmihudDenominator_WidePanel.csv', index = True, header = True)
Stocks_AmihudRatio_MonthlyPanel = Stocks_AmihudNumerator_MonthlyPanel/Stocks_AmihudDenominator_MonthlyPanel
Stocks_AmihudRatio_MonthlyPanel.to_csv('Monthly/Stocks_AmihudRatio_WidePanel.csv', index = True, header = True)
#Stocks_PriceVol_db['Year'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).year
#Stocks_PriceVol_db['Month'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).month
#Stocks_PriceVol_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(Stocks_PriceVol_db['Year'], Stocks_PriceVol_db['Month'])]
