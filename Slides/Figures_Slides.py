#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 26 20:15:41 2019

@author: gpichard
"""

import pandas as pd
import numpy as np
import matplotlib as mpl

ETF_db = pd.read_excel('../Eikon/ReportEikon_ETF_20180224.xlsx', header = 0)

Inceptions = pd.DataFrame(ETF_db.loc[:,'SEC Inception Date'].dropna())
Inceptions.set_index('SEC Inception Date', inplace = True)
Inceptions = Inceptions.assign(Listed = 1)
Listings = Inceptions.resample('M').sum()

Closed = pd.DataFrame(ETF_db.loc[:, 'Closed Date'].dropna())
Closed.set_index('Closed Date', inplace = True)
Closed = Closed.assign(Delisted = 1)
Delistings = Closed.resample('M').sum()

Lifespan = pd.concat([Listings, Delistings], axis=1, verify_integrity=True)
Lifespan.loc[Lifespan.Delisted.isna(), 'Delisted'] = 0
Lifespan = Lifespan.assign(NetListed = Lifespan.Listed - Lifespan.Delisted)
Lifespan = Lifespan.assign(NetCumListed = np.cumsum(Lifespan.NetListed))
fig, ax = mpl.pyplot.subplots()
ax.plot(Lifespan.NetCumListed)
ax.set(ylabel = "Live ETFs", title = "Evolution of live ETFs registered at the SEC, 1993-2018")
minor_ticks = pd.to_datetime(np.arange(0, 28,1), unit = 'Y', origin = pd.Timestamp('1993-01-01'))
ax.set_xticks(minor_ticks, minor = True)
ax.grid(which = "both")
mpl.pyplot.show()
fig.savefig('LiveETF_Eikon.eps')


# Mutual fund flows vs ETF flows
Flows_db = pd.read_csv('USFlows_ETFvsMutual.csv', header = 0, infer_datetime_format=True)
Flows_db.DATE = pd.to_datetime(Flows_db.DATE)
Flows_db.columns = ["Date", "ETF","Mutual funds"]

fig, ax = mpl.pyplot.subplots()
#ax.plot(Flows_db)
ax.bar(x = Flows_db.Date, height = Flows_db.ETF, width = 100, alpha = 0.75)
ax.bar(x = Flows_db.Date, height = Flows_db['Mutual funds'], width = 100, alpha = 0.75)
ax.set(ylabel = "USD millions", title = "Quarterly ETF vs. Mutual fund flows into U.S. domestic equities")
#ax.set_xticks(Flows_db.Date, minor = True)
ax.grid(which = "both")
ax.legend(list(Flows_db.columns[1:]), loc = 0)
mpl.pyplot.figtext(0.99, 0.01, 'Data from FRED, Federal Reserve Bank of St.Louis', horizontalalignment='right') 
#ax.legend(labels = ("ETF Flows", "Mutual funds Flows"))
mpl.pyplot.show()
fig.savefig('USFlows_ETFvsMutual.pdf') # Exporting to pdf preserves transparency and resolution


