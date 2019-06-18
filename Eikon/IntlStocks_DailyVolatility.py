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
# =============================================================================
# # Long handling (outdated). Still needed for monthly volatility of daily returns
# =============================================================================
#print("Loading Price and volume data (large table > 9.5 GB)")
#Stocks_PriceVol_Daily_db = pd.concat([chunk for chunk in pd.read_csv('Daily/IntlStockPrice-volume_Series_daily_db.csv', header = None, index_col = 0, chunksize = 10e5)])
#Stocks_PriceVol_Daily_db.info()
#Stocks_PriceVol_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
#Stocks_PriceVol_Daily_db.Date = pd.to_datetime(Stocks_PriceVol_Daily_db.Date, infer_datetime_format = True)
#for col in list(Stocks_PriceVol_Daily_db[['RIC', 'Variable']].columns):
#    Stocks_PriceVol_Daily_db[col] = Stocks_PriceVol_Daily_db[col].astype('category')
#Stocks_PriceVol_Daily_db.set_index(['Date', 'RIC', 'Variable'], inplace=True)
#print("Generating Close Prices panel")
#Stocks_Close_Daily_db = Stocks_PriceVol_Daily_db.xs('CLOSE', level=2)
#Stocks_Close_Daily_db = Stocks_Close_Daily_db.reset_index().drop_duplicates()
#Stocks_Close_Daily_db.to_csv('Daily/IntlStocks_Close_LongMerged_db.csv')
#
#Stocks_Close_DailyPanel_db = pd.pivot_table(Stocks_Close_Daily_db.reset_index(), values = 'Value', index = 'Date', columns = 'RIC', fill_value = 0)
#del Stocks_Close_Daily_db
#print("Generating (daily) Volume panel")
#Stocks_Volume_Daily_db = Stocks_PriceVol_Daily_db.xs('VOLUME', level=2)
#del Stocks_PriceVol_Daily_db
#Stocks_Volume_DailyPanel_db = pd.pivot_table(Stocks_Volume_Daily_db.reset_index(), values = 'Value', index = 'Date', columns = 'RIC', fill_value = 0)
## Aggregation at the monthly frequency
#Stocks_Volume_MonthlyPanel_db = Stocks_Volume_DailyPanel_db.resample('M', axis = 0).sum()
#Stocks_Volume_MonthlyPanel_db.to_csv('Monthly/IntlStocks_Volume_LongMerged_db.csv')
#del Stocks_Volume_Daily_db
#print("Generating Percentage daily return panel")
#Stocks_ReturnClose_DailyPanel = Stocks_Close_DailyPanel_db.pct_change().drop(pd.to_datetime('1999-08-02'), axis=0)
#print("Generating Monthly volatility panel")
## There is an extremely large number of missing values, yielding infinite yield. Replace np.inf with np.nan before dropping all np.nan
## Replace infinite values with NaN in order to drop them afterwards
#Stocks_ReturnClose_DailyPanel.replace([np.inf, -np.inf], np.nan, inplace = True)
#Stocks_ReturnClose_DailyPanel.dropna(how = 'all', inplace = True)
#
#Stocks_ReturnCloseVolatility_MonthlyPanel = Stocks_ReturnClose_DailyPanel.resample('M', axis=0).std()
#Stocks_ReturnCloseVolatility_MonthlyPanel.to_csv('Monthly/IntlStocks_DVolatility_MFreq_WidePanel.csv', index = True, header = True)
## Loading the VWAP data set
#print("Loading VWAP data (large table > 2.3 GB)")
#Stocks_VWAP_Daily_db = pd.read_csv('Daily/IntlStockVWAP_Series_daily_db.csv', header = None, index_col = 0)
#Stocks_VWAP_Daily_db.info()
#Stocks_VWAP_Daily_db.rename({1:'RIC', 2:'Date', 3:'VWAP'}, axis = 'columns', inplace = True)
#Stocks_VWAP_Daily_db.Date = pd.to_datetime(Stocks_VWAP_Daily_db.Date, infer_datetime_format = True)
#Stocks_VWAP_Daily_db.RIC = Stocks_VWAP_Daily_db.RIC.astype('category')
#print("Generating VWAP panel")
#Stocks_VWAP_DailyPanel_db = pd.pivot_table(Stocks_VWAP_Daily_db, values = 'VWAP', index = 'Date', columns = 'RIC', fill_value = 0)
#del Stocks_VWAP_Daily_db
# As there are more stocks in the VWAP data set than elswhere (precisely 4119 vs 4085), those that are only present in there are excluded from further analysis

# =============================================================================
# Quick operations (equivalent to those above after the dataframes loaded have been initially written)
# =============================================================================
# Volume
Stocks_Volume_Daily_db = pd.read_csv('Daily/IntlStocks_Volume_Daily_db.csv', header = None)
Stocks_Volume_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
Stocks_Volume_Daily_db.drop(columns = [0, 'Variable'], inplace = True)
Stocks_Volume_Daily_db.Date = pd.to_datetime(Stocks_Volume_Daily_db.Date, infer_datetime_format = True)
Stocks_Volume_Daily_db.drop_duplicates(inplace = True)
Stocks_Volume_Daily_db.set_index(['Date', 'RIC'], inplace=True)

Stocks_Volume_DailyPanel_db = Stocks_Volume_Daily_db.unstack()
Stocks_Volume_DailyPanel_db.replace([np.inf, -np.inf], np.nan, inplace = True)
Stocks_Volume_DailyPanel_db.dropna(how = 'all', inplace = True)

del Stocks_Volume_Daily_db

# VWAP
Stocks_VWAP_Daily_db = pd.read_csv('Daily/IntlStockVWAP_Series_daily_db.csv', header = None, index_col = 0)
Stocks_VWAP_Daily_db.info()
Stocks_VWAP_Daily_db.rename({1:'RIC', 2:'Date', 3:'Value'}, axis = 'columns', inplace = True)
Stocks_VWAP_Daily_db.Date = pd.to_datetime(Stocks_VWAP_Daily_db.Date, infer_datetime_format = True)
Stocks_VWAP_Daily_db.set_index(['Date', 'RIC'], inplace = True)

Stocks_VWAP_DailyPanel_db = Stocks_VWAP_Daily_db.unstack()
Stocks_VWAP_DailyPanel_db.replace([np.inf, -np.inf], np.nan, inplace = True)
Stocks_VWAP_DailyPanel_db.dropna(how = 'all', inplace = True)
del Stocks_VWAP_Daily_db

# Return
Stocks_Close_Daily_db = pd.read_csv('Daily/IntlStocks_Close_LongMerged_db.csv')
Stocks_Close_Daily_db.drop(columns = 'Unnamed: 0', inplace = True)
Stocks_Close_Daily_db.Date = pd.to_datetime(Stocks_Close_Daily_db.Date, infer_datetime_format = True)
Stocks_Close_Daily_db.drop_duplicates(inplace = True)
Stocks_Close_Daily_db.set_index(['Date', 'RIC'], inplace = True)

Stocks_Close_DailyPanel_db = Stocks_Close_Daily_db.unstack()
del Stocks_Close_Daily_db

Stocks_ReturnClose_DailyPanel = Stocks_Close_DailyPanel_db.pct_change(fill_method='pad').mask(Stocks_Close_DailyPanel_db.isnull()) 
Stocks_ReturnClose_DailyPanel.replace([np.inf, -np.inf], np.nan, inplace = True)
Stocks_ReturnClose_DailyPanel.dropna(how = 'all', inplace = True)


# =============================================================================
# Computation of variables in regression
# =============================================================================

Stocks_VWAP_DailyPanel_db = Stocks_VWAP_DailyPanel_db.filter(Stocks_Volume_DailyPanel_db.columns, axis = 1)
# Dropping the first row of Volume and VWAP series in order to divide by same-sized arrays
Stocks_Volume_DailyPanel_db.drop(pd.to_datetime('1999-08-02'), axis=0, inplace=True)
Stocks_VWAP_DailyPanel_db.drop(pd.to_datetime('1999-08-02'), axis=0, inplace=True)
# Dropping one date (Christmas...) existing in returns, and whose values are worth nothing
Stocks_ReturnClose_DailyPanel.drop(pd.to_datetime('2004-12-24'), axis=0, inplace=True)
# Filtering out one stock from the return on Close price history in order to match with those in VWAP and Volume series (the company, BSB Bancorp Inc., has filed out from NASDAQ on 2019-04-01)
Stocks_ReturnClose_DailyPanel = Stocks_ReturnClose_DailyPanel.filter(Stocks_Volume_DailyPanel_db.columns, axis = 1)

# Computing the Amihud illiquidity ratio over calendar months
# For model 2 (liquidity), both the numerator and denominator will be needed, following the indication in Israeli (2017)
Stocks_AmihudNumerator_MonthlyPanel = abs(Stocks_ReturnClose_DailyPanel).resample('M', axis=0).mean()
Stocks_AmihudNumerator_MonthlyPanel.to_csv('Monthly/IntlStocks_AmihudNumerator_WidePanel.csv', index = True, header = True)
Stocks_AmihudDenominator_MonthlyPanel = (Stocks_Volume_DailyPanel_db * Stocks_VWAP_DailyPanel_db).resample('M', axis=0).mean()
Stocks_AmihudDenominator_MonthlyPanel.to_csv('Monthly/IntlStocks_AmihudDenominator_WidePanel.csv', index = True, header = True)
# Original definition of the ILLIQ ratio in Amihud (2002)
Stocks_AbsReturnOverVolume_DailyPanel = abs(Stocks_ReturnClose_DailyPanel)/(Stocks_Volume_DailyPanel_db * Stocks_VWAP_DailyPanel_db)
Stocks_AmihudRatio_MonthlyPanel = Stocks_AbsReturnOverVolume_DailyPanel.resample('M', axis=0).sum()/Stocks_ReturnClose_DailyPanel.resample('M', axis=0).count()
Stocks_AmihudRatio_MonthlyPanel.replace([np.inf, -np.inf], np.nan, inplace = True)
Stocks_AmihudRatio_MonthlyPanel.to_csv('Monthly/IntlStocks_AmihudRatio_WidePanel.csv', index = True, header = True)
#Stocks_PriceVol_db['Year'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).year
#Stocks_PriceVol_db['Month'] = pd.DatetimeIndex(Stocks_PriceVol_db.Date).month
#Stocks_PriceVol_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(Stocks_PriceVol_db['Year'], Stocks_PriceVol_db['Month'])]

# Variance ratio: the variable in order to analyze mean reversion
# Quarterly 1-day versus 5-day returns variance comparison
Stocks_1dVariance = Stocks_ReturnClose_DailyPanel.resample('Q', axis=0).var()
Stocks_5dReturns = Stocks_Close_DailyPanel_db.diff(periods = 5)/Stocks_Close_DailyPanel_db.shift(5)
Stocks_5dReturns.replace([np.inf, -np.inf], np.nan, inplace = True)
Stocks_5dReturns.dropna(how = 'all', inplace = True)
# Non-overlapping 5-day periods : keeping the first (valid, i.e numeric, even 0 and -1) 5-day return value, at the weekly frequency. The label accounts for the 5-day period end date
Stocks_5dReturns = Stocks_5dReturns.resample('W', axis = 0, convention='start', label = 'left').first()
Stocks_5dVariance = Stocks_5dReturns.resample('Q', axis = 0).var()

Stocks_5to1dVarianceRatio = Stocks_5dVariance/(5 * Stocks_1dVariance)
Stocks_5to1dVarianceRatio.to_csv('Quarterly/IntlStocks_5to1dVarianceRatio.csv', header = True, index = True)