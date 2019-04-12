#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 10 09:19:05 2019

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
import linearmodels

MonthlyVariables_db = pd.read_csv('Monthly/MonthlyVariables_db.csv', index_col = [0, 1], header = 0, parse_dates = True)
MonthlyVariables_db.info()
for col in list(['Date.1', 'Date.2', 'Date.3', 'Date.4']):
    MonthlyVariables_db[col] = pd.to_datetime(MonthlyVariables_db[col], infer_datetime_format = True)
del col
MonthlyVariables_db.reset_index(inplace = True)
MonthlyVariables_db.set_index(['RIC','YearMonth'], inplace = True)
MonthlyAvailable_db = MonthlyVariables_db.dropna()
# Transformations and lags needed for regressions
MonthlyAvailable_db = MonthlyAvailable_db.assign(InvClose = pd.Series(1/MonthlyAvailable_db['Close']))
MonthlyAvailable_db = MonthlyAvailable_db.assign(BookToMarketRatio = pd.Series(1/MonthlyAvailable_db['PriceToBVPerShare']))
# Shift variables in time : independent variables and volatility lags
mod1_All_Volatility = linearmodels.PanelOLS.from_formula('Volatility ~ 0 + ETFHoldings + np.log(CompanyMarketCap) +  InvClose + AmihudRatio + PctBidAskSpread + BookToMarketRatio + RetPast12to1M +  GrossProfitability +  EntityEffects + TimeEffects', MonthlyAvailable_db)

print(mod1_All_Volatility.fit())

#from statsmodels.datasets import grunfeld
#data = grunfeld.load_pandas().data
#data.firm = data.firm.astype('category')
#data.year = pd.to_datetime(data.year)
#data = data.set_index(['firm', 'year'])
#print(data.head())
#mod = linearmodels.PanelOLS.from_formula('invest ~ 0 + value + capital + EntityEffects', data = data)
#print(mod.fit())