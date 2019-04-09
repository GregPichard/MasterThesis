#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 14:52:43 2019

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
# Loading the main independent variable : ETF ownership shares (already long)
ETF_Holdings_LongMerged_db = pd.read_csv('Monthly/ETF_Holdings_LongMerged_db.csv', header=0)
# Loading all the remaining control variables
# Amihud ratio (panel form)
AmihudRatio_MonthlyPanel = pd.read_csv('Monthly/Stocks_AmihudRatio_WidePanel.csv', header = 0)
# Pct Bid-Ask Spread
BidAsk_MonthlyPanel = pd.read_csv('Monthly/Stocks_BidAskSpread_db.csv', header = 0)
# Past 12-to-1 months return (momentum) (panel form)
RetPast12to1M_MonthlyPanel = pd.read_csv('Monthly/Stocks_RetPast12to1Months.csv', header = 0)
# Gross Profitability (Novy-Marx) (already long)
GrossProfit_db = pd.read_csv('Monthly/Stocks_GrossProfit_db.csv', header = 0)
# Stacked in one (long) dataset : 
# Market Cap (which log is used)
# Close price (which inverse is used)
# Price to BV per share ratio (which inverse is used)
ControlVariables_db = pd.read_csv('Monthly/StockControlVariables_monthly_db.csv', header = 0)