# -*- coding: utf-8 -*-
"""
Created on Mon Mar 18 10:51:30 2019

@author: Grégoire
"""

import pandas as pd
import numpy as np
import datetime

# Read data extracted from ETFs' ID data set
ETF_ID_db = pd.read_excel("ReportEikon_ETF_live&nonswap_20190303.xlsx", header = 0)
ETF_ID_db.info()

# Read fund holdings of US stocks  
StocksFunds_db = pd.read_csv("FundOwners_Full_db.csv", header = 0)

StocksETF_db = StocksFunds_db.loc[StocksFunds_db.Fund_RIC.isin(list(ETF_ID_db.Lipper_RIC)), :]
StocksETF_db['Year'] = pd.DatetimeIndex(StocksETF_db['Date']).year
np.savez_compressed("FundOwners_ETFHeld_RIC_list", RIC = StocksETF_db.RIC.unique())
StocksPanel_db = pd.pivot_table(StocksETF_db, values = 'AdjNbSharesHeld', index = 'Year', columns = 'RIC', aggfunc = np.sum, fill_value = 0)

## Warning : from here on, script "OutstandingShares_query.py" is expected to have been run
Stocks_MarketCap_db = pd.read_csv("../../../../../../switchdrive/ETF/BasicOutstandingShares_Stocks_db.csv", header = None, index_col = 0)
Stocks_MarketCap_db = Stocks_MarketCap_db.rename(index = str, columns = {1:"RIC", 2:"Date", 3:"NbSharesOutstanding"})
Stocks_MarketCap_db['Year'] =pd.DatetimeIndex(Stocks_MarketCap_db['Date']).year
Stocks_MarketCap_db = Stocks_MarketCap_db.dropna()

# Issue : with the variable 'TR.SharesOutstanding'. some securities have shares outstanding data before 1999. Here is their subsample.
Outliers_MarketCap_db = Stocks_MarketCap_db[Stocks_MarketCap_db.RIC.isin(list(Stocks_MarketCap_db.loc[Stocks_MarketCap_db.Year < 1999, 'RIC'].unique()))]
OutliersMarketCapPanel_db = pd.pivot_table(Outliers_MarketCap_db, values = 'NbSharesOutstanding', index = 'Year', columns = 'RIC', fill_value = None)
OutliersMarketCapPanel_db.to_csv("OutliersMarketCapPanel.csv", header = True)

StocksMarketCapPanel_db = pd.pivot_table(Stocks_MarketCap_db, values = 'NbSharesOutstanding', index = 'Year', columns = 'RIC', fill_value = 0)

# Issue : there are 7 (seven) companies which never report the number of shares outstanding over the whole period : 
NonreportingStocks_MarketCap_list = list(set(Stocks_MarketCap_db.RIC.unique()).difference(set(StocksMarketCapPanel_db.columns)))
# Dropping non-reporting companies from the ETF holdings panel
StocksPanel_db = StocksPanel_db.drop(columns = NonreportingStocks_MarketCap_list)
ETF_agg_db = pd.melt(StocksPanel_db.reset_index(), id_vars = ['Year'], var_name = 'RIC', value_name = 'TotalAdjNbSharesHeld')

StocksETF_Holdings_Panel_db = StocksPanel_db/StocksMarketCapPanel_db
StocksETF_Holdings_Long_db = pd.melt(StocksETF_Holdings_Panel_db.reset_index(), id_vars = ['Year'], var_name = 'RIC', value_name = 'ETFHoldings')
