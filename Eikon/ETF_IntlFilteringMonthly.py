#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 16:32:31 2019
Adapted from ETF_FilteringMonthly.py
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

# Read fund holdings of International stocks (monthly appended data) - CSV
print("Extraction of raw ETF holdings in progress. Please wait...")
StocksETF_db = pd.concat([x.query('Fund_RIC in ' + str(list(ETF_ID_db.Lipper_RIC))) for x in pd.read_csv('Monthly/IntlFundOwners_Monthly_Full_db.csv', chunksize = 10e4)], ignore_index = True)
print("Extracted raw ETF holdings")
StocksETF_db.info()
StocksETF_db.Date = pd.to_datetime(StocksETF_db.Date ,infer_datetime_format = True)
for col in list(StocksETF_db.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns):
    StocksETF_db[col] = StocksETF_db[col].astype('category')
del col
StocksETF_db.head(10)
StocksETF_db.shape

## Delete all exact duplicate rows except the last one occurring (descending order)
#StocksETF_db = StocksETF_db.drop_duplicates(keep = 'last')
#StocksETF_ETF_ID_db = pd.read_excel("ReportEikon_ETF_live&nonswap_20190303.xlsx", header = 0)

# In order to reassign duplicates to the query date
StocksETF_db['Year'] = pd.DatetimeIndex(StocksETF_db.Date).year
StocksETF_db['Month'] = pd.DatetimeIndex(StocksETF_db.Date).month

# This section assimilates delayed values to current ones : e.g. the shares held by an ETF queried on 01-01-2000 are not available, thus the database only provides the holdings as of 31-12-1999.
global Duplicates_db
Duplicates_db = StocksETF_db[StocksETF_db.duplicated(keep = False)]
Duplicates_db.info()
Duplicates_db.index
# Sorting holdings by company, then by date,then by number of shares held : duplicated value are grouped together
Duplicates_db = Duplicates_db.sort_values(['RIC', 'Date', 'AdjNbSharesHeld'])


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
#        print(i, x)
        IncrementDate(i)
IncrementDate_Loop()
#p = multiprocessing.Process(target = IncrementDate_Loop)
#p.join()
#p.close()
Duplicates_db.head(100)

# Correcting values in the initial StocksETF_db : replacing delayed dates with queried dates (i.e. "not the actual ones")
for i, x in enumerate(Duplicates_db.index):
    StocksETF_db.Month.at[x] = Duplicates_db.Month.at[x]
    StocksETF_db.Year.at[x] = Duplicates_db.Year.at[x]
StocksETF_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(StocksETF_db['Year'], StocksETF_db['Month'])]
StocksETF_db.set_index(['YearMonth', 'RIC'], inplace = True)
del i, x
StocksETF_Aggregate_db = StocksETF_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()

StocksPanel_db = pd.pivot_table(StocksETF_db, values = 'AdjNbSharesHeld', index = 'YearMonth', columns = 'RIC', aggfunc = np.sum, fill_value = 0)

### Total shares outstanding
## Warning : from here on, script "OutstandingShares_query.py" is expected to have been run
Stocks_SharesOutstanding_db = pd.read_csv("Monthly/IntlOutstandingShares_Stocks_Monthly_db.csv", header = None, index_col = 0)
Stocks_SharesOutstanding_db = Stocks_SharesOutstanding_db.rename(index = str, columns = {1:"RIC", 2:"Date", 3:"NbSharesOutstanding"})
Stocks_SharesOutstanding_db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(np.int64(pd.DatetimeIndex(Stocks_SharesOutstanding_db['Date']).year), np.int64(pd.DatetimeIndex(Stocks_SharesOutstanding_db['Date']).month))]
Stocks_SharesOutstanding_db.drop_duplicates(inplace = True)
Stocks_SharesOutstanding_db = Stocks_SharesOutstanding_db.dropna()
Stocks_SharesOutstanding_db.set_index(['YearMonth', 'RIC'], inplace = True)

## Monthly frequency : those stocks do not appear because another variable, 'TR.BasicShrsOutAvg' is used. See script 'OutstandingShares_Monthly_query.py'
## Issue : with the variable 'TR.SharesOutstanding'. some securities have shares outstanding data before 1999. Here is their subsample.
#Outliers_MarketCap_db = Stocks_SharesOutstanding_db[Stocks_SharesOutstanding_db.RIC.isin(list(Stocks_SharesOutstanding_db.loc[Stocks_SharesOutstanding_db.Year < 1999, 'RIC'].unique()))]
#OutliersMarketCapPanel_db = pd.pivot_table(Outliers_MarketCap_db, values = 'NbSharesOutstanding', index = 'Year', columns = 'RIC', fill_value = None)
#OutliersMarketCapPanel_db.to_csv("Monthly/OutliersMarketCapPanel_Monthly.csv", header = True)

#StocksMarketCapPanel_db = pd.pivot_table(Stocks_SharesOutstanding_db, values = 'NbSharesOutstanding', index = 'YearMonth', columns = 'RIC', fill_value = 0)



StocksETF_Holdings_db = pd.concat([Stocks_SharesOutstanding_db, StocksETF_Aggregate_db], axis = 1, verify_integrity = True)
# As expected, no row has simultaneously missing nb of shares outstanding and nb of shares held by ETF
#StocksETF_Holdings_db.dropna(how = 'all', subset = ['NbSharesOutstanding', 'AdjNbSharesHeld']).info()
StocksETF_Holdings_db = StocksETF_Holdings_db.assign(PctSharesHeldETF = pd.Series(StocksETF_Holdings_db.AdjNbSharesHeld/StocksETF_Holdings_db.NbSharesOutstanding))
StocksETF_Holdings_db.loc[StocksETF_Holdings_db.AdjNbSharesHeld.isna(), 'PctSharesHeldETF'] = 0

# Panel form (not necessary for next operations)
#StocksETF_HoldingsPanel_db = pd.pivot_table(StocksETF_Holdings_db.reset_index(), values = 'PctSharesHeldETF', index = 'YearMonth', columns = 'RIC', fill_value = None)

# Writing DataFrame to file
StocksETF_Holdings_db.to_csv('Monthly/IntlETF_Holdings_Extrapolation_LongMerged_db.csv', index = True, header = True)
del StocksETF_Aggregate_db, StocksETF_db, StocksETF_Holdings_db
del Duplicates_db
## Extension : Aggregating other categories of institutional ownership, taking duplicates into account


def FindDuplicates(db):
    Duplicates_db = db[db.duplicated(keep = False)]
    Duplicates_db = Duplicates_db.sort_values(['RIC', 'Date', 'AdjNbSharesHeld'])
    return Duplicates_db

def IncrementDateDuplicates(Duplicates_db):
    for i,x in enumerate(Duplicates_db.index):
        if Duplicates_db.RIC.iat[i] == Duplicates_db.RIC.iat[i - 1]:
            if Duplicates_db.Fund_RIC.iat[i] == Duplicates_db.Fund_RIC.iat[i - 1]:
                if Duplicates_db.AdjNbSharesHeld.iat[i] == Duplicates_db.AdjNbSharesHeld.iat[i - 1]:
                    Duplicates_db.Month.iat[i] = Duplicates_db.Month.iat[i - 1] % 12 + 1
                    Duplicates_db.Year.iat[i] = Duplicates_db.Year.iat[i - 1] + (Duplicates_db.Month.iat[i - 1]) // 12
    return Duplicates_db

def CorrectDuplicates(db):
    Duplicates_db = FindDuplicates(db)
    Duplicates_db = IncrementDateDuplicates(Duplicates_db)
    for i, x in enumerate(Duplicates_db.index):
        db.Month.at[x] = Duplicates_db.Month.at[x]
        db.Year.at[x] = Duplicates_db.Year.at[x]
    del Duplicates_db
    db['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(db['Year'], db['Month'])]
    db.set_index(['YearMonth', 'RIC'], inplace = True)
    return db

 # Other, non-ETF mutual funds   
OtherMutual_db = pd.concat([x.query('Owner_type == "Mutual Fund" and Fund_RIC not in ' + str(list(ETF_ID_db.Lipper_RIC)))  for x in pd.read_csv('Monthly/IntlFundOwners_Monthly_Full_db.csv', chunksize = 10e5)], ignore_index = True)

#OtherMutual_db.to_csv('Monthly/OtherMutualHoldings_db.csv', header = True, index = True)

for col in list(OtherMutual_db.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns):
    OtherMutual_db[col] = OtherMutual_db[col].astype('category')
OtherMutual_db.Date = pd.to_datetime(OtherMutual_db.Date, infer_datetime_format = True)
OtherMutual_db = OtherMutual_db.assign(Year = lambda x: pd.DatetimeIndex(x['Date']).year, Month = lambda x: pd.DatetimeIndex(x['Date']).month)
OtherMutual_db.info()
# If the memory can handle this large dataframe
#OtherMutual_db = CorrectDuplicates(OtherMutual_db)
#OtherMutual_Aggregate_db = OtherMutual_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()
#OtherMutual_Aggregate_db.to_csv('Monthly/IntlOtherMutual_AggregateExtrapolation_db.csv', index = True, header = True)

# Process in chunks
OtherMutual_early_db = OtherMutual_db[OtherMutual_db.Date <= '2007-12-31']
OtherMutual_mid_db = OtherMutual_db[(OtherMutual_db.Date > '2007-12-31') & (OtherMutual_db.Date <= '2013-12-31')]
OtherMutual_late_db = OtherMutual_db[OtherMutual_db.Date > '2013-12-31']
del OtherMutual_db

OtherMutual_early_db = CorrectDuplicates(OtherMutual_early_db)
OtherMutual_early_Aggregate_db = OtherMutual_early_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()
OtherMutual_early_Aggregate_db.to_csv('Monthly/IntlOtherMutual_AggregateExtrapolation_db.csv', index = True, header = True, mode = 'a')
del OtherMutual_early_db, OtherMutual_early_Aggregate_db

OtherMutual_mid_db = CorrectDuplicates(OtherMutual_mid_db)
OtherMutual_mid_Aggregate_db = OtherMutual_mid_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()
OtherMutual_mid_Aggregate_db.to_csv('Monthly/IntlOtherMutual_AggregateExtrapolation_db.csv', index = True, header = False, mode = 'a')
del OtherMutual_mid_db, OtherMutual_mid_Aggregate_db

OtherMutual_late_db = CorrectDuplicates(OtherMutual_late_db)
OtherMutual_late_Aggregate_db = OtherMutual_late_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()
OtherMutual_late_Aggregate_db.to_csv('Monthly/IntlOtherMutual_AggregateExtrapolation_db.csv', index = True, header = False, mode = 'a')
del OtherMutual_late_db, OtherMutual_late_Aggregate_db

OtherMutual_Aggregate_db = pd.read_csv('Monthly/IntlOtherMutual_AggregateExtrapolation_db.csv', header = 0)
OtherMutual_Aggregate_db.drop_duplicates(subset = ['RIC', 'YearMonth'],keep='first', inplace = True)

OtherMutual_Aggregate_db.set_index(['YearMonth', 'RIC'], inplace = True)
# Dropping a row containing a duplicate of the column labels
OtherMutual_Aggregate_db.drop(['YearMonth'], inplace = True)
OtherMutual_Aggregate_db.AdjNbSharesHeld = [float(x) for x in OtherMutual_Aggregate_db.AdjNbSharesHeld]
# Beware of duplicates remaining in this dataframe; they are rows with similar multiindex but not the same values. We keep the first ones to appear

# Pension funds
Pension_db = pd.concat([x.query('Owner_type == "Pension Fund Portfolio"')  for x in pd.read_csv('Monthly/IntlFundOwners_Monthly_Full_db.csv', chunksize = 10e5)], ignore_index = True)
for col in list(Pension_db.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns):
    Pension_db[col] = Pension_db[col].astype('category')
Pension_db.Date = pd.to_datetime(Pension_db.Date, infer_datetime_format = True)
Pension_db = Pension_db.assign(Year = lambda x: pd.DatetimeIndex(x['Date']).year, Month = lambda x: pd.DatetimeIndex(x['Date']).month)
Pension_db.info()
Pension_db = CorrectDuplicates(Pension_db)

Pension_Aggregate_db = Pension_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()
Pension_Aggregate_db.to_csv('Monthly/IntlPension_AggregateExtrapolation_db.csv', index = True, header = True)


# Hedge funds
Hedge_db = pd.concat([x.query('Owner_type == "Hedge Fund Portfolio"')  for x in pd.read_csv('Monthly/FundOwners_Monthly_Full_db.csv', chunksize = 10e5)], ignore_index = True)
for col in list(Hedge_db.drop(['Date', 'AdjNbSharesHeld'], axis = 1).columns):
    Hedge_db[col] = Hedge_db[col].astype('category')
Hedge_db.Date = pd.to_datetime(Hedge_db.Date, infer_datetime_format = True)
Hedge_db = Hedge_db.assign(Year = lambda x: pd.DatetimeIndex(x['Date']).year, Month = lambda x: pd.DatetimeIndex(x['Date']).month)
Hedge_db.info()
Hedge_db = CorrectDuplicates(Hedge_db)

Hedge_Aggregate_db = Hedge_db.reset_index().groupby(by= ['YearMonth', 'RIC'], as_index=True)[['AdjNbSharesHeld']].sum().dropna()
Hedge_Aggregate_db.to_csv('Monthly/Hedge_AggregateExtrapolation_db.csv', index = True, header = True)

# Combination into one table, merging based on multi-index
OtherMutual_Aggregate_db.rename(index = str, columns = {"AdjNbSharesHeld":"OtherMutual_AdjNbSharesHeld"}, inplace = True)
Pension_Aggregate_db.rename(index = str, columns = {"AdjNbSharesHeld":"Pension_AdjNbSharesHeld"}, inplace = True)
Hedge_Aggregate_db.rename(index = str, columns = {"AdjNbSharesHeld":"Hedge_AdjNbSharesHeld"}, inplace = True)

NonETF_Holdings_db = pd.concat([Stocks_SharesOutstanding_db, OtherMutual_Aggregate_db, Pension_Aggregate_db, Hedge_Aggregate_db], axis = 1, verify_integrity = True)
NonETF_Holdings_db.info()

NonETF_Holdings_db = NonETF_Holdings_db.assign(PctSharesHeldOtherMutual = pd.Series(NonETF_Holdings_db.OtherMutual_AdjNbSharesHeld/NonETF_Holdings_db.NbSharesOutstanding))
NonETF_Holdings_db = NonETF_Holdings_db.assign(PctSharesHeldPension = pd.Series(NonETF_Holdings_db.Pension_AdjNbSharesHeld/NonETF_Holdings_db.NbSharesOutstanding))
NonETF_Holdings_db = NonETF_Holdings_db.assign(PctSharesHeldHedge = pd.Series(NonETF_Holdings_db.Hedge_AdjNbSharesHeld/NonETF_Holdings_db.NbSharesOutstanding))

# If the number of shares held by a category of funds is unknown, we assume the percentage (relative to total shares outstanding) is 0.
NonETF_Holdings_db.loc[NonETF_Holdings_db.OtherMutual_AdjNbSharesHeld.isna(), 'PctSharesHeldOtherMutual'] = 0
NonETF_Holdings_db.loc[NonETF_Holdings_db.Pension_AdjNbSharesHeld.isna(), 'PctSharesHeldPension'] = 0
NonETF_Holdings_db.loc[NonETF_Holdings_db.Hedge_AdjNbSharesHeld.isna(), 'PctSharesHeldHedge'] = 0

# Issue with several inf (0/0) values in Non-ETF mutual funds
NonETF_Holdings_db.PctSharesHeldOtherMutual.replace([np.inf, -np.inf], np.nan, inplace = True)
NonETF_Holdings_db.PctSharesHeldHedge.replace([np.inf, -np.inf], np.nan, inplace = True)

# Dropping a column that can already be found in the ETF holdings db 
NonETF_Holdings_db.drop(columns = 'NbSharesOutstanding', inplace = True)
# Exporting the "Extrapolation" version of other fund holdings
NonETF_Holdings_db.to_csv('Monthly/IntlNonETF_HoldingsExtrapolation_LongMerged_db.csv', index = True, header = True)
