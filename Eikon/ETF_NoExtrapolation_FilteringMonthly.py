#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 16:32:31 2019

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
import multiprocessing

### Shares held by ETFs
# Read data extracted from ETFs' ID data set
ETF_ID_db = pd.read_excel("ReportEikon_ETF_live&nonswap_20190303.xlsx", header = 0)
ETF_ID_db.info()

# Read fund holdings of US stocks (monthly appended data) - HDF5
# Since file is large, it is read in successive chunks
print("Extraction of raw ETF holdings in progress. Please wait...")
StocksETF_db = pd.concat([x.query('Fund_RIC in ' + str(list(ETF_ID_db.Lipper_RIC))) for x in pd.read_csv('Monthly/FundOwners_Monthly_Full_db.csv', chunksize = 10e4)], ignore_index = True)
print("Extracted raw ETF holdings")
StocksETF_db.info()
StocksETF_db.Date = pd.to_datetime(StocksETF_db.Date ,infer_datetime_format = True)
for col in list(StocksETF_db.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns):
    StocksETF_db[col] = StocksETF_db[col].astype('category')
StocksETF_db.head(10)
StocksETF_db.shape

## Delete all exact duplicate rows except the last one occurring (descending order)
StocksETF_db = StocksETF_db.drop_duplicates()
StocksETF_db.info()
# The dataset is incomplete : some dates near the period start, in 1999-2001 aren't populated at all.
print(sorted(StocksETF_db.Date.unique()))

# In order to reassign duplicates to the query date
StocksETF_db['Year'] = pd.DatetimeIndex(StocksETF_db.Date).year
StocksETF_db['Month'] = pd.DatetimeIndex(StocksETF_db.Date).month
StocksETF_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(StocksETF_db['Year'], StocksETF_db['Month'])]
StocksETF_db.set_index(['YearMonth', 'RIC'], inplace = True)

StocksETF_Aggregate_db = StocksETF_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()

# Panel form (not necessary for next operations)
# StocksPanel_db = pd.pivot_table(StocksETF_db, values = 'AdjNbSharesHeld', index = 'YearMonth', columns = 'RIC', aggfunc = np.sum, fill_value = 0)
# Year 1999 is incomplete in the shares quantities held by ETF (StocksETF_db) : January and August totally missing. Thus dropping from January until September (kept).
# StocksPanel_db = StocksPanel_db[StocksPanel_db.index >= '1999-09']
#YearMonth_final = list(StocksPanel_db.index)

### Total shares outstanding
## Warning : from here on, script "OutstandingShares_query.py" is expected to have been run
Stocks_SharesOutstanding_db = pd.read_csv("Monthly/BasicOutstandingShares_Stocks_Monthly_db.csv", header = None, index_col = 0)
AdditionalStocks_SharesOutstanding_db = pd.read_csv("Monthly/AdditionalOutstandingShares_Stocks_Monthly_db.csv", header = None, index_col = 0)
Stocks_SharesOutstanding_db = pd.concat([Stocks_SharesOutstanding_db, AdditionalStocks_SharesOutstanding_db])
Stocks_SharesOutstanding_db = Stocks_SharesOutstanding_db.rename(index = str, columns = {1:"RIC", 2:"Date", 3:"NbSharesOutstanding"})
Stocks_SharesOutstanding_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(np.int64(pd.DatetimeIndex(Stocks_SharesOutstanding_db['Date']).year), np.int64(pd.DatetimeIndex(Stocks_SharesOutstanding_db['Date']).month))]
Stocks_SharesOutstanding_db = Stocks_SharesOutstanding_db.dropna()
Stocks_SharesOutstanding_db.set_index(['YearMonth', 'RIC'], inplace = True)

# Panel form (not necessary for next operations)
# Stocks_SharesOutstandingPanel_db = pd.pivot_table(Stocks_SharesOutstanding_db, values = 'NbSharesOutstanding', index = 'YearMonth', columns = 'RIC', fill_value = 0)
#Stocks_SharesOutstandingPanel_db = Stocks_SharesOutstandingPanel_db[Stocks_SharesOutstandingPanel_db.index >= '1999-09']
#Extracting the time index to be used in two dataframes

# (Outer) joining both data sets


# Both variables, the ETF ownership shares and the total number of shares outstanding, do not fully overlap :
# Missing (=0) ownership
print(len(list(set(Stocks_SharesOutstanding_db.index).difference(set(StocksETF_Aggregate_db.index)))))
# More dangerous : missing nb of shares outstanding -> NaN (actual missing value for ownership share)
print(len(list(set(StocksETF_Aggregate_db.index).difference(set(Stocks_SharesOutstanding_db.index)))))

StocksETF_Holdings_db = pd.concat([Stocks_SharesOutstanding_db, StocksETF_Aggregate_db], axis = 1, verify_integrity = True)
# As expected, no row has simultaneously missing nb of shares outstanding and nb of shares held by ETF
#StocksETF_Holdings_db.dropna(how = 'all', subset = ['NbSharesOutstanding', 'AdjNbSharesHeld']).info()

StocksETF_Holdings_db = StocksETF_Holdings_db.assign(PctSharesHeldETF = pd.Series(StocksETF_Holdings_db.AdjNbSharesHeld/StocksETF_Holdings_db.NbSharesOutstanding))
StocksETF_Holdings_db.loc[StocksETF_Holdings_db.AdjNbSharesHeld.isna(), 'PctSharesHeldETF'] = 0

# Panel form (not necessary for next operations)
#StocksETF_HoldingsPanel_db = pd.pivot_table(StocksETF_Holdings_db.reset_index(), values = 'PctSharesHeldETF', index = 'YearMonth', columns = 'RIC', fill_value = None)

# Writing DataFrame to file
StocksETF_Holdings_db.to_csv('Monthly/ETF_Holdings_LongMerged_db.csv', index = True, header = True)