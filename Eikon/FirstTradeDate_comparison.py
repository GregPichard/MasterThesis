#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  4 12:43:20 2019
Comparing IPO dates and earliest values included in sample. 
Some securities miss part of their history in our sample - and they were live at that time. Striking example : AAPL.OQ !
@author: gpichard
"""

# Loading price/volume series and extracting Close prices panel to find the earliest
Stocks_PriceVol_Daily_db = pd.read_csv('Daily/StockPrice-volume_Series_daily_db.csv', header = None, index_col = 0)
Stocks_PriceVol_Daily_db.info()
Stocks_PriceVol_Daily_db.rename({1:'Date', 2:'RIC', 3:'Variable', 4:'Value'}, axis = 'columns', inplace = True)
Stocks_PriceVol_Daily_db.Date = pd.to_datetime(Stocks_PriceVol_Daily_db.Date, infer_datetime_format = True)
for col in list(Stocks_PriceVol_Daily_db[['RIC', 'Variable']].columns):
    Stocks_PriceVol_Daily_db[col] = Stocks_PriceVol_Daily_db[col].astype('category')

Stocks_PriceVol_Daily_db.set_index(['Date', 'RIC', 'Variable'], inplace=True)
print("Generating Close Prices panel")
Stocks_Close_Daily_db = Stocks_PriceVol_Daily_db.xs('CLOSE', level=2)

Stocks_Close_Daily_db.reset_index(drop=False, inplace=True)
Stocks_Close_Daily_db.groupby(by = 'RIC')['Date']
Stocks_Close_Daily_db.groupby(by = 'RIC')['Date'].min()
sorted(Stocks_Close_Daily_db.groupby(by = 'RIC')['Date'].min())
pd.Series.hist(Stocks_Close_Daily_db.groupby(by = 'RIC')['Date'].min())

EarliestDate = Stocks_Close_Daily_db.groupby(by = 'RIC')['Date'].min()

# Results obtained through the script 'FirstTradeDate_query.py'
ListingDate = pd.read_csv('Monthly/FirstTradeDate_db.csv', header = None)
ListingDate.set_index(1, inplace=True)
ListingDate.drop(columns=0, inplace = True)
ListingDate[2] = pd.to_datetime(ListingDate[2], infer_datetime_format=True)

# Comparison
CompTable = pd.concat([ListingDate, EarliestDate], axis=1, verify_integrity=True)
CompTable.rename({'Date':'Earliest'}, axis='columns', inplace = True)
CompTable.columns

print('Matching : Introduction date and Earliest date identical in sample')
np.sum(CompTable.Earliest == CompTable.Listing)
print('Surprising : Earliest date is before Introduction date')
np.sum(CompTable.Earliest < CompTable.Listing)
print('Searched for : Earliest date is after 1999-08-02, and after the Introduction date, hence there a part of the time series is missing')
MissingHistory = CompTable[(CompTable.Earliest > CompTable.Listing) & (CompTable.Earliest > pd.to_datetime('1999-08-02'))]
pd.Series.hist(MissingHistory['Earliest'],bins=20)
MissingHistory.to_csv('Issues/MissingDailyPriceVol_timetable.csv', header = True, index = True)
## There are indeed about 40 securities missing the first half of the sample, while they were already listed. Some incimplete have only are few days missing at the beginning.