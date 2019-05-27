#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 22 17:25:42 2019
Adapted from script 'MergingVariables_monthly.py'
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

# Loading the dependent variable : Monthly-averaged daily returns volatility (panel form)
ReturnCloseVolatility_MonthlyPanel = pd.read_csv('Monthly/Stocks_DVolatility_MFreq_WidePanel.csv', header = 0)
ReturnCloseVolatility_db = pd.melt(ReturnCloseVolatility_MonthlyPanel, id_vars = ['Date'], var_name = 'RIC', value_name = 'Volatility')
# Loading the main independent variable : ETF ownership shares (already long)
ETF_Holdings_LongMerged_db = pd.read_csv('Monthly/ETF_Holdings_LongMerged_db.csv', header=0)
# Loading all the remaining control variables
# Amihud ratio (panel form)
AmihudRatio_MonthlyPanel = pd.read_csv('Monthly/Stocks_AmihudRatio_WidePanel.csv', header = 0)
AmihudRatio_db = pd.melt(AmihudRatio_MonthlyPanel, id_vars = ['Date'], var_name = 'RIC', value_name = 'AmihudRatio')
# Pct Bid-Ask Spread (long)
BidAsk_db = pd.read_csv('Monthly/Stocks_BidAskSpread_db.csv', header = 0)
BidAsk_db.drop_duplicates(keep = 'last', inplace = True)
for i, d in enumerate(pd.DatetimeIndex(BidAsk_db.Date)):
    if d.day == 1:
        BidAsk_db.Date.iat[i] = d - datetime.timedelta(weeks = 1)
    else:
        BidAsk_db.Date.iat[i] = d
# Past 12-to-1 months return (momentum) (panel form)
RetPast12to1M_MonthlyPanel = pd.read_csv('Monthly/Stocks_RetPast12to1Months.csv', header = 0)
RetPast12to1M_MonthlyPanel.drop(np.arange(12), axis = 0, inplace = True)
RetPast12to1M_MonthlyPanel.replace([np.inf, -np.inf], np.nan, inplace = True)
RetPast12to1M_db = pd.melt(RetPast12to1M_MonthlyPanel, id_vars = ['YearMonth'], var_name = 'RIC', value_name = 'RetPast12to1M')
# Gross Profitability (Novy-Marx) (already long)

# Past 12-to-7 months return (momentum) (panel form)
RetPast12to7M_MonthlyPanel = pd.read_csv('Monthly/Stocks_RetPast12to7Months.csv', header = 0)
RetPast12to7M_MonthlyPanel.drop(np.arange(12), axis = 0, inplace = True)
RetPast12to7M_MonthlyPanel.replace([np.inf, -np.inf], np.nan, inplace = True)
RetPast12to7M_db = pd.melt(RetPast12to7M_MonthlyPanel, id_vars = ['YearMonth'], var_name = 'RIC', value_name = 'RetPast12to7M')
# Gross Profitability (Novy-Marx) (already long)

GrossProfit_db = pd.read_csv('Monthly/Stocks_GrossProfit_db.csv', header = 0)
GrossProfit_db.drop_duplicates(keep = 'last', inplace = True)
for i, d in enumerate(pd.DatetimeIndex(GrossProfit_db.Date)):
    if d.day == 1:
        GrossProfit_db.Date.iat[i] = d - datetime.timedelta(weeks = 1)
    else:
        GrossProfit_db.Date.iat[i] = d
# Stacked in one (long) dataset : 
# Market Cap (which log is used)
# Close price (which inverse is used)
# Price to BV per share ratio (which inverse is used)
ControlVariables_db = pd.read_csv('Monthly/StockControlVariables_monthly_db.csv', header = 0)
ControlVariables_db = ControlVariables_db.assign(CompanyMarketCap_millions = pd.Series(ControlVariables_db.CompanyMarketCap/10e6))
ControlVariables_db.drop_duplicates(keep = 'last', inplace = True)
for i, d in enumerate(pd.DatetimeIndex(ControlVariables_db.Date)):
    if d.day == 1:
        ControlVariables_db.Date.iat[i] = d - datetime.timedelta(weeks = 1)
    else:
        ControlVariables_db.Date.iat[i] = d
# 1/2 : for wide datasets, melt them and generate the index similarly
# Done individually
# 2/2  : for long datasets, define the multiple index ['YearMonth', 'RIC']
# The ETF Holdings already have the YearMonth variable
ETF_Holdings_LongMerged_db.set_index(['YearMonth', 'RIC'], inplace = True)
RetPast12to1M_db.set_index(['YearMonth', 'RIC'], inplace = True)
RetPast12to7M_db.set_index(['YearMonth', 'RIC'], inplace = True)

for df in list([GrossProfit_db, ControlVariables_db, ReturnCloseVolatility_db, AmihudRatio_db, BidAsk_db]):
    year = pd.DatetimeIndex(df.Date).year
    month = pd.DatetimeIndex(df.Date).month - (pd.DatetimeIndex(df.Date).day==1).astype('int64')
    df['YearMonth'] = [str(y) + '-' + str(m).zfill(2) for y, m in zip(year, month)]
    df.set_index(['YearMonth', 'RIC'], inplace = True)

BidAsk_db = BidAsk_db[~BidAsk_db.index.duplicated(keep = 'last')]
GrossProfit_db = GrossProfit_db[~GrossProfit_db.index.duplicated(keep = 'last')]
del ReturnCloseVolatility_MonthlyPanel, AmihudRatio_MonthlyPanel, RetPast12to1M_MonthlyPanel, RetPast12to7M_MonthlyPanel

MonthlyVariables_db = pd.concat([ETF_Holdings_LongMerged_db,ReturnCloseVolatility_db, AmihudRatio_db, BidAsk_db, RetPast12to1M_db, RetPast12to7M_db, GrossProfit_db, ControlVariables_db], axis = 1)
MonthlyVariables_db.replace([np.inf, -np.inf], np.nan, inplace = True)
MonthlyVariables_db.to_csv('Monthly/MonthlyVariables_db.csv', index = True, header = True)