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
np.savez_compressed("FundOwners_ETFHeld_RIC_list", StocksETF_db.RIC.unique())
StocksPanel_db = pd.pivot_table(StocksETF_db, values = 'AdjNbSharesHeld', index = 'Year', columns = 'RIC', aggfunc = np.sum, fill_value = 0)
ETF_agg_db = pd.melt(StocksPanel_db.reset_index(), id_vars = ['Year'], var_name = 'RIC', value_name = 'TotalAdjNbSharesHeld')

