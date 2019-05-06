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

#MonthlyAvailable_db = MonthlyVariables_db.dropna()
MonthlyAvailable_db = MonthlyVariables_db
# Whenever, at a given time and for a given company, the number of shares detained by ETFs is equal to or larger than the number of shares outstanding, the observation is dropped
MonthlyAvailable_db.loc[MonthlyAvailable_db.PctSharesHeldETF >= 1, 'PctSharesHeldETF'] = None
# Dropping observations with stock price less than USD 1
MonthlyAvailable_db.loc[MonthlyAvailable_db.Close < 1, 'Close'] = None


# Transformations and lags needed for regressions
MonthlyAvailable_db = MonthlyAvailable_db.assign(InvClose = pd.Series(1/MonthlyAvailable_db['Close']))
MonthlyAvailable_db = MonthlyAvailable_db.assign(BookToMarketRatio = pd.Series(1/MonthlyAvailable_db['PriceToBVPerShare']))
# Shift variables in time : independent variables and volatility lags
MonthlyAvailable_1lag_db = MonthlyAvailable_db.groupby(level = 0)[['CompanyMarketCap', 'InvClose', 'AmihudRatio', 'PctBidAskSpread', 'BookToMarketRatio', 'RetPast12to1M', 'GrossProfitability', 'Volatility']].shift(1)
MonthlyAvailable_1lag_db.columns = [s + "_1lag" for s in list(MonthlyAvailable_1lag_db.columns)]
MonthlyAvailable_2lag_db = MonthlyAvailable_db.groupby(level = 0)['Volatility'].shift(2)
MonthlyAvailable_2lag_db.name = MonthlyAvailable_2lag_db.name + "_2lag"
MonthlyAvailable_3lag_db = MonthlyAvailable_db.groupby(level = 0)['Volatility'].shift(3)
MonthlyAvailable_3lag_db.name = MonthlyAvailable_3lag_db.name + "_3lag"
MonthlyAvailable_4lag_db = MonthlyAvailable_db.groupby(level = 0)['Volatility'].shift(4)
MonthlyAvailable_4lag_db.name = MonthlyAvailable_4lag_db.name + "_4lag"
MonthlyAvailable_db = pd.concat([MonthlyAvailable_db, MonthlyAvailable_1lag_db, MonthlyAvailable_2lag_db, MonthlyAvailable_3lag_db, MonthlyAvailable_4lag_db], axis = 1)
MonthlyAvailable_db = MonthlyAvailable_db.dropna()
MonthlyAvailable_db.info()

mod1_All_Volatility = linearmodels.PanelOLS.from_formula('Volatility ~ 0 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) +  InvClose_1lag  + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to1M_1lag  +  EntityEffects + TimeEffects', MonthlyAvailable_db)
print(mod1_All_Volatility.fit(cov_type = "kernel"))

mod1_All_Volatility_withLags = linearmodels.PanelOLS.from_formula('Volatility ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) +  InvClose_1lag + AmihudRatio_1lag + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to1M_1lag +  GrossProfitability_1lag +  EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag', MonthlyAvailable_db)
print(mod1_All_Volatility_withLags.fit(cov_type = "kernel"))
MonthlyAvailable_db.describe().to_csv('../SummaryStats/All_Volatlity_withLags.csv', header = True, index = True)
# Summary statistics : there are a few extreme values that may bias the sample.
MonthlyAvailable_db.describe().to_csv('../SummaryStats/All_Volatlity_withLags.csv', header = True, index = True)

mod1_All_Volatility_noLag = linearmodels.PanelOLS.from_formula('Volatility ~ 0 + PctSharesHeldETF + np.log(CompanyMarketCap) +  InvClose + AmihudRatio + PctBidAskSpread + BookToMarketRatio + RetPast12to1M +  GrossProfitability +  EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag', MonthlyAvailable_db)
print(mod1_All_Volatility_noLag.fit(cov_type = "clustered"))

#from statsmodels.datasets import grunfeld
#data = grunfeld.load_pandas().data
#data.firm = data.firm.astype('category')
#data.year = pd.to_datetime(data.year)
#data = data.set_index(['firm', 'year'])
#print(data.head())
#mod = linearmodels.PanelOLS.from_formula('invest ~ 0 + value + capital + EntityEffects', data = data)
#print(mod.fit())