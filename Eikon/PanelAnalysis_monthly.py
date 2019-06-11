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
pd.options.display.float_format = '{:.3f}'.format
import numpy as np
import datetime
import multiprocessing
import linearmodels

# =============================================================================
# # Loading the monthly panel, transforming some variables and filtering some outliers
# =============================================================================
MonthlyVariables_db = pd.read_csv('Monthly/MonthlyVariables_db.csv', index_col = [0, 1], header = 0, parse_dates = True)
MonthlyVariables_db.info()
for col in list(['Date.' + str(i) for i in np.arange(1, 9)]):
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

# Replace infinite values with NaN in order to drop them afterwards
MonthlyAvailable_db.replace([np.inf, -np.inf], np.nan, inplace = True)


## Summary statistics
KeptVariables_summary = list(['Volatility', 'PctSharesHeldETF', 'BookToMarketRatio', 'CompanyMarketCap_millions', 'InvClose', 'PctBidAskSpread', 'AmihudRatio', 'RetPast12to1M', 'RetPast12to7M', 'GrossProfitability'])
KeptVariables_headers = list(['Volatility', 'ETF Ownership', 'Book-to-market', 'Market cap. ($ Mln.)', '1/Price', 'Rel. Bid-Ask spread', 'Amihud ratio', 'Past 12-to-1-month return', 'Past 12-to-7-month return', 'Gross profitability'])
SumStats = MonthlyAvailable_db[KeptVariables_summary].dropna().describe().transpose()
SumStats['count'] = SumStats['count'].astype(int)
SumStats.set_index(pd.Series(KeptVariables_headers), drop = True, inplace = True)
# Export
#SumStats.to_latex('../SummaryStats/SummaryTable.tex', header = ['N (obs.)', 'Mean', 'St. dev.', 'Min.', '25%', 'Median', '75%', 'Max.'])
#SumStats.to_latex()
# Correlation matrix
CorrMat = MonthlyAvailable_db[KeptVariables_summary].corr()
mask = np.tril(np.ones_like(CorrMat, dtype=np.bool), k=0)
CorrMat.where(mask, other = '', inplace = True)
NumberedHeader = ["({})".format(x) for x in np.arange(1, len(KeptVariables_summary) + 1)]
CorrMat.set_index([pd.Series(KeptVariables_headers), pd.Series(NumberedHeader)], inplace = True, drop = True)

# Export
#CorrMat.to_latex('../SummaryStats/CorrTable.tex', index_names = True, header = NumberedHeader[:(len(NumberedHeader))])

## Dynamic panel
# Shift variables in time : independent variables and volatility lags
MonthlyAvailable_1lag_db = MonthlyAvailable_db.groupby(level = 0)[['PctSharesHeldETF', 'CompanyMarketCap', 'InvClose', 'AmihudRatio', 'PctBidAskSpread', 'BookToMarketRatio', 'RetPast12to1M', 'RetPast12to7M','GrossProfitability', 'Volatility']].shift(1)
MonthlyAvailable_1lag_db.columns = [s + "_1lag" for s in list(MonthlyAvailable_1lag_db.columns)]
MonthlyAvailable_2lag_db = MonthlyAvailable_db.groupby(level = 0)['Volatility'].shift(2)
MonthlyAvailable_2lag_db.name = MonthlyAvailable_2lag_db.name + "_2lag"
MonthlyAvailable_3lag_db = MonthlyAvailable_db.groupby(level = 0)['Volatility'].shift(3)
MonthlyAvailable_3lag_db.name = MonthlyAvailable_3lag_db.name + "_3lag"
MonthlyAvailable_4lag_db = MonthlyAvailable_db.groupby(level = 0)['Volatility'].shift(4)
MonthlyAvailable_4lag_db.name = MonthlyAvailable_4lag_db.name + "_4lag"
MonthlyAvailable_db = pd.concat([MonthlyAvailable_db, MonthlyAvailable_1lag_db, MonthlyAvailable_2lag_db, MonthlyAvailable_3lag_db, MonthlyAvailable_4lag_db], axis = 1)
#MonthlyAvailable_db = MonthlyAvailable_db.dropna()
MonthlyAvailable_db.info()

## Standardizing dependent and ownership variables
## Censoring the sample at 99.99th percentile for statistics
MonthlyAvailable_std_db = MonthlyAvailable_db[['Volatility', 'PctSharesHeldETF', 'PctSharesHeldOtherMutual', 'PctSharesHeldPension', 'PctSharesHeldHedge']]
colInit = list(MonthlyAvailable_std_db.columns)
for col in colInit:
    mean = MonthlyAvailable_std_db[col].dropna().loc[MonthlyAvailable_std_db[col].dropna() < np.percentile(MonthlyAvailable_std_db[col].dropna(), 99.99)].mean()
    std = MonthlyAvailable_std_db[col].dropna().loc[MonthlyAvailable_std_db[col].dropna() < np.percentile(MonthlyAvailable_std_db[col].dropna(), 99.99)].std()
    # replacing the top 0.01% of values with NaN
    MonthlyAvailable_std_db[col].loc[MonthlyAvailable_std_db[col] >= np.percentile(MonthlyAvailable_std_db[col].dropna(), 99.99)] = None
    MonthlyAvailable_std_db[col] = (MonthlyAvailable_std_db[col] - mean)/std
    MonthlyAvailable_db[col + '_std'] = MonthlyAvailable_std_db[col]

del colInit, col, mean, std

## First differences
MonthlyAvailable_diff_db = MonthlyAvailable_db.groupby(level=0).diff()
MonthlyAvailable_diff_db.to_csv('Monthly/MonthlyAvailable_diff.csv', header = True, index = True)

# =============================================================================
# ## Regression estimates
# =============================================================================
# Model 1 : Volatility
# Full US sample with a constant term, lagged controls
mod1_All_Volatility = linearmodels.PanelOLS.from_formula('Volatility ~ 0 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) +  InvClose_1lag  + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to1M_1lag + EntityEffects + TimeEffects', MonthlyAvailable_db)
print(mod1_All_Volatility.fit(cov_type = "kernel"))

# Full US sample with a constant term, lagged controls and 4 volatility lags
mod1_All_Volatility_withLags = linearmodels.PanelOLS.from_formula('Volatility ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) +  InvClose_1lag + AmihudRatio_1lag + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to7M_1lag + GrossProfitability_1lag +  EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag + Volatility_4lag', MonthlyAvailable_db)
mod1_fit = mod1_All_Volatility_withLags.fit(cov_type = "kernel")
print(mod1_fit)
f = open('../Regression_Results/mod1_All_Volatility_withLags.tex', 'w')
f.write(mod1_fit.summary.as_latex())
f.close()

# Controls are contemporaneous of the dependent
mod1_All_Volatility_contemporaneousControls = linearmodels.PanelOLS.from_formula('Volatility ~ 1 + PctSharesHeldETF + PctSharesHeldETF* + np.log(CompanyMarketCap_1lag) +  InvClose_1lag + AmihudRatio_1lag + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to1M_1lag +  GrossProfitability_1lag + EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag + Volatility_4lag', MonthlyAvailable_db)
mod1_contemporaneousControls_fit = mod1_All_Volatility_contemporaneousControls.fit(cov_type = "kernel")
print(mod1_contemporaneousControls_fit)
## Attempt to do a dynamic panel estimation through GMM. Needs further research.
#mod1_GMM_All_Volatility_withlags = linearmodels.LinearFactorModelGMM()
MonthlyAvailable_db.describe().to_csv('../SummaryStats/All_Volatlity_withLags.csv', header = True, index = True)
# Summary statistics : there are a few extreme values that may bias the sample.
MonthlyAvailable_db.describe().to_csv('../SummaryStats/All_Volatlity_withLags.csv', header = True, index = True)

# No constant term and controls are contemporaneous of the dependent
mod1_All_Volatility_noLag = linearmodels.PanelOLS.from_formula('Volatility ~ 0 + PctSharesHeldETF + np.log(CompanyMarketCap) +  InvClose + AmihudRatio + PctBidAskSpread + BookToMarketRatio + RetPast12to1M +  GrossProfitability +  EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag', MonthlyAvailable_db)
print(mod1_All_Volatility_noLag.fit(cov_type = "clustered"))

mod1_All_Volatility_withLags_withFundcontrols = linearmodels.PanelOLS.from_formula('Volatility ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) +  InvClose_1lag + AmihudRatio_1lag + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to7M_1lag + GrossProfitability_1lag + PctSharesHeldOtherMutual + PctSharesHeldPension + PctSharesHeldHedge + EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag + Volatility_4lag', MonthlyAvailable_db)
mod1_fit = mod1_All_Volatility_withLags_withFundcontrols.fit(cov_type = "kernel")
print(mod1_fit)


# Standardization could (and should?) be performed over the relevant sample of values only
MonthlyAvailable_stdMod1_db = MonthlyAvailable_db[['Volatility', 'PctSharesHeldETF_std', 'CompanyMarketCap_1lag', 'InvClose_1lag', 'AmihudRatio_1lag', 'PctBidAskSpread_1lag', 'BookToMarketRatio_1lag', 'RetPast12to7M_1lag', 'GrossProfitability_1lag', 'PctSharesHeldOtherMutual_std', 'PctSharesHeldPension_std', 'PctSharesHeldHedge_std', 'Volatility_1lag', 'Volatility_2lag', 'Volatility_3lag', 'Volatility_4lag']].dropna()
colInit = list(MonthlyAvailable_stdMod1_db.columns)
for col in colInit:
    mean = MonthlyAvailable_stdMod1_db[col].loc[MonthlyAvailable_stdMod1_db[col] < np.percentile(MonthlyAvailable_stdMod1_db[col].dropna(), 99.99)].mean()
    std = MonthlyAvailable_stdMod1_db[col].loc[MonthlyAvailable_stdMod1_db[col].dropna() < np.percentile(MonthlyAvailable_stdMod1_db[col].dropna(), 99.99)].std()
    # replacing the top 0.01% of values with NaN
    MonthlyAvailable_stdMod1_db[col].loc[MonthlyAvailable_stdMod1_db[col] >= np.percentile(MonthlyAvailable_stdMod1_db[col].dropna(), 99.99)] = None
    MonthlyAvailable_stdMod1_db[col] = (MonthlyAvailable_stdMod1_db[col] - mean)/std

del colInit, col, mean, std

# Estimation on standardized, winsorized variables
mod1_All_Volatility_std_withLags_withFundcontrols = linearmodels.PanelOLS.from_formula('Volatility ~ 0 + PctSharesHeldETF_std + CompanyMarketCap_1lag +  InvClose_1lag + AmihudRatio_1lag + PctBidAskSpread_1lag + BookToMarketRatio_1lag + RetPast12to7M_1lag + GrossProfitability_1lag + PctSharesHeldOtherMutual_std + PctSharesHeldPension_std + PctSharesHeldHedge_std + EntityEffects + TimeEffects + Volatility_1lag + Volatility_2lag + Volatility_3lag + Volatility_4lag', MonthlyAvailable_stdMod1_db)
mod1_fit = mod1_All_Volatility_std_withLags_withFundcontrols.fit(cov_type = "kernel")
print(mod1_fit)

# First differences, standardized ownership and volatility, no controls except other fund holdings and volatility lags
# The full regressors matrix is not full rank, thus we do not include all usual controls 
mod1_All_diffVolatility_std_withLags_withFundcontrols = linearmodels.PanelOLS.from_formula('Volatility_std ~ 0 + PctSharesHeldETF_std + PctSharesHeldOtherMutual_std + PctSharesHeldPension_std + PctSharesHeldHedge_std + AmihudRatio_1lag + BookToMarketRatio_1lag + RetPast12to7M_1lag + GrossProfitability_1lag + EntityEffects + TimeEffects + Volatility_1lag', MonthlyAvailable_diff_db)
mod1_fit = mod1_All_diffVolatility_std_withLags_withFundcontrols.fit(cov_type = "kernel")
print(mod1_fit)

# Model 2 : Liquidity
# Bid-ask spread on close prices (not equal to the High-Low spread metric from Israeli 2017) - static model
mod2_All_Liquidity_BidAsk = linearmodels.PanelOLS.from_formula('PctBidAskSpread ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) + BookToMarketRatio_1lag + Volatility_1lag + EntityEffects + TimeEffects', MonthlyAvailable_db)
mod2_fit = mod2_All_Liquidity_BidAsk.fit(cov_type = "kernel")
print(mod2_fit)

mod2_All_Liquidity_BidAsk_withFundcontrols = linearmodels.PanelOLS.from_formula('PctBidAskSpread ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) + BookToMarketRatio_1lag + Volatility_1lag + EntityEffects + TimeEffects + PctSharesHeldOtherMutual + PctSharesHeldPension + PctSharesHeldHedge', MonthlyAvailable_db)
mod2_fit = mod2_All_Liquidity_BidAsk_withFundcontrols.fit(cov_type = "kernel")
print(mod2_fit)
# Amihud ratio decomposition
mod2_All_Liquidity_Amihud = linearmodels.PanelOLS.from_formula('AmihudNumerator ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) + BookToMarketRatio_1lag + AmihudDenominator + EntityEffects + TimeEffects', MonthlyAvailable_db)
mod2_fit = mod2_All_Liquidity_Amihud.fit(cov_type = "kernel")
print(mod2_fit)

mod2_All_Liquidity_Amihud_withFundcontrols = linearmodels.PanelOLS.from_formula('AmihudNumerator ~ 1 + PctSharesHeldETF + np.log(CompanyMarketCap_1lag) + BookToMarketRatio_1lag + AmihudDenominator + EntityEffects + TimeEffects + PctSharesHeldOtherMutual ', MonthlyAvailable_db)
mod2_fit = mod2_All_Liquidity_Amihud_withFundcontrols.fit(cov_type = "kernel")
print(mod2_fit)
# Only the other mutual fund holdings are included to control for institutional ownership, otherwise the matrix of regressors does not have full column rank. Other mutual funds are the most available values and economically the more sizeable of the three categories at hand.

# Model 3 : Efficiency