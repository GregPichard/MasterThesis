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

# Read data extracted from ETFs' ID data set
ETF_ID_db = pd.read_excel("ReportEikon_ETF_live&nonswap_20190303.xlsx", header = 0)
ETF_ID_db.info()
#
## Read fund holdings of US stocks (monthly appended data)
### Assessing column categories (dtypes) to read CSV file more efficiently
#StocksFunds_head = pd.read_csv('Monthly/FundOwners_Monthly_Full_db.csv', header = 0, nrows = 10)
#converted_head = pd.DataFrame()
#for col in StocksFunds_head.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns:
#    converted_head.loc[:, col] = StocksFunds_head[col].astype('category') 
#dtypes = converted_head.dtypes
#dtypes_col = dtypes.index
#dtypes_type = [i.name for i in dtypes.values]
#column_types = dict(zip(dtypes_col, dtypes_type))
#column_types['AdjNbSharesHeld']='float64'
#print(column_types)
## Reading the whole db file
#StocksFunds_opt_db = pd.read_csv('Monthly/FundOwners_Monthly_Full_db.csv', dtype = column_types, parse_dates = ['Date'], infer_datetime_format = True)
#StocksFunds_opt_db.info()
## Exporting dataframe to compressed HDF5 file
#StocksFunds_opt_db.to_hdf('Monthly/FundOwners_Monthly_Full_db_comp.h5', 'StocksFunds_Monthly_db', format='table', data_columns = True, complib = 'zlib', complevel = 6)

# Read fund holdings of US stocks (monthly appended data) - HDF5
print("Extraction of raw ETF holdings in progess. Please wait...")
StocksETF_db = pd.concat([x.query('Fund_RIC in ' + str(list(ETF_ID_db.Lipper_RIC))) for x in pd.read_csv('Monthly/FundOwners_Monthly_Full_db.csv', chunksize = 10e4)], ignore_index = True)
print("Extracted raw ETF holdings")
StocksETF_db.info()
StocksETF_db.Date = pd.to_datetime(StocksETF_db.Date ,infer_datetime_format = True)
for col in list(StocksETF_db.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns):
    StocksETF_db[col] = StocksETF_db[col].astype('category')
StocksETF_db.head(10)
StocksETF_db.shape

## Delete all exact duplicate rows except the last one occurring (descending order)
#StocksETF_db = StocksETF_db.drop_duplicates(keep = 'last')
#StocksETF_db.info()
#
#print(sorted(StocksETF_db.Date.unique()))

# In order to reassign duplicates to the query date
StocksETF_db['Year'] = pd.DatetimeIndex(StocksETF_db.Date).year
StocksETF_db['Month'] = pd.DatetimeIndex(StocksETF_db.Date).month

global Duplicates_db
Duplicates_db = StocksETF_db[StocksETF_db.duplicated(keep = False)]
Duplicates_db.info()
Duplicates_db.index
Duplicates_db = Duplicates_db.sort_values(['RIC', 'Date', 'AdjNbSharesHeld'])
Duplicates_db_backup = Duplicates_db
# Another backup, necessary for the next "Shares outstanding" query
np.savez_compressed("FundOwners_ETFHeld_RIC_list", RIC = StocksETF_db.RIC.unique())


def IncrementDate(i):
    #index_current = Duplicates_db.index[i]
    #index_prev = Duplicates_db.index[i - 1]
    #if i > 0:
        # First case : exact match (first duplicate)
        # Second case : values and original dates match but not (next duplicates)
        #if np.sum(Duplicates_db.drop(['Year', 'Month'], axis = 1).loc[index_current] == Duplicates_db.drop(['Year', 'Month'], axis = 1).loc[index_prev]) == (Duplicates_db.shape[1] - 2):
            #Duplicates_db[index_current, 'Month'] = Duplicates_db.Month.loc[index_prev] % 12 + 1
            #Duplicates_db[index_current, 'Year'] = Duplicates_db.Year.loc[index_prev] + (Duplicates_db.Month.loc[index_prev]) // 12
        if Duplicates_db.RIC.iat[i] == Duplicates_db.RIC.iat[i - 1]:
            if Duplicates_db.Fund_RIC.iat[i] == Duplicates_db.Fund_RIC.iat[i - 1]:
                if Duplicates_db.AdjNbSharesHeld.iat[i] == Duplicates_db.AdjNbSharesHeld.iat[i - 1]:
                    Duplicates_db.Month.iat[i] = Duplicates_db.Month.iat[i - 1] % 12 + 1
                    Duplicates_db.Year.iat[i] = Duplicates_db.Year.iat[i - 1] + (Duplicates_db.Month.iat[i - 1]) // 12
def IncrementDate_Loop():
    for i,x in enumerate(Duplicates_db.index):
        print(i, x)
        IncrementDate(i)
IncrementDate_Loop()
#p = multiprocessing.Process(target = IncrementDate_Loop)
#p.join()
#p.close()
Duplicates_db.head(100)

# Correcting values in the initial StocksETF_db
for i, x in enumerate(Duplicates_db.index):
    StocksETF_db.Month.at[x] = Duplicates_db.Month.at[x]
    StocksETF_db.Year.at[x] = Duplicates_db.Year.at[x]
StocksETF_db['YearMonth'] = list(zip(StocksETF_db.Year, StocksETF_db.Month))
StocksPanel_db = pd.pivot_table(StocksETF_db, values = 'AdjNbSharesHeld', index = 'YearMonth', columns = 'RIC', aggfunc = np.sum, fill_value = 0)

## Warning : from here on, script "OutstandingShares_query.py" is expected to have been run
Stocks_MarketCap_db = pd.read_csv("Monthly/BasicOutstandingShares_Stocks_Monthly_db.csv", header = None, index_col = 0)
Stocks_MarketCap_db = Stocks_MarketCap_db.rename(index = str, columns = {1:"RIC", 2:"Date", 3:"NbSharesOutstanding"})
Stocks_MarketCap_db['Year'] =pd.DatetimeIndex(Stocks_MarketCap_db['Date']).year
Stocks_MarketCap_db['Month'] =pd.DatetimeIndex(Stocks_MarketCap_db['Date']).month
Stocks_MarketCap_db['YearMonth'] = list(zip(Stocks_MarketCap_db.Year, Stocks_MarketCap_db.Month))
Stocks_MarketCap_db = Stocks_MarketCap_db.dropna()

# Issue : with the variable 'TR.SharesOutstanding'. some securities have shares outstanding data before 1999. Here is their subsample.
Outliers_MarketCap_db = Stocks_MarketCap_db[Stocks_MarketCap_db.RIC.isin(list(Stocks_MarketCap_db.loc[Stocks_MarketCap_db.Year < 1999, 'RIC'].unique()))]
OutliersMarketCapPanel_db = pd.pivot_table(Outliers_MarketCap_db, values = 'NbSharesOutstanding', index = 'Year', columns = 'RIC', fill_value = None)
OutliersMarketCapPanel_db.to_csv("Monthly/OutliersMarketCapPanel_Monthly.csv", header = True)

StocksMarketCapPanel_db = pd.pivot_table(Stocks_MarketCap_db, values = 'NbSharesOutstanding', index = 'YearMonth', columns = 'RIC', fill_value = 0)

# Issue : there are 7 (seven) (?) companies which never report the number of shares outstanding over the whole period : 
NonreportingStocks_MarketCap_list = list(set(Stocks_MarketCap_db.RIC.unique()).difference(set(StocksMarketCapPanel_db.columns)))
# Dropping non-reporting companies from the ETF holdings panel
StocksPanel_db = StocksPanel_db.drop(columns = NonreportingStocks_MarketCap_list)
ETF_agg_db = pd.melt(StocksPanel_db.reset_index(), id_vars = ['YearMonth'], var_name = 'RIC', value_name = 'TotalAdjNbSharesHeld')

StocksETF_Holdings_Panel_db = StocksPanel_db/StocksMarketCapPanel_db
StocksETF_Holdings_Long_db = pd.melt(StocksETF_Holdings_Panel_db.reset_index(), id_vars = ['YearMonth'], var_name = 'RIC', value_name = 'ETFHoldings')

