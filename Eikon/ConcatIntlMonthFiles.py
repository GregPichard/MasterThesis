# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:37:01 2019
Adapted from ConcatMonthFiles.py
@author: gpichard
"""

import pandas as pd
import numpy as np

years = range(1999, 2019, 1)
mo = range(1,13)
dd = list([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
dates_list = list()
filenames_list = list()
appended_data = list()
for y in years:
    for m, month in enumerate(mo):
        print("Processing month/year " + str(month) + "/" + str(y))
        dates_list.append(str(y) + "-" + str(month).zfill(2) + "-" + str(dd[m]))
        #filenames_list.append("Monthly/FundOwners_" + dates_list[-1] + "_db.csv") # Greg's local path on debian2 machine
        filenames_list.append("./Monthly/IntlFundOwners_" + dates_list[-1] + "_db.csv") # Greg's local path on Süleyman's virtual machine
        data = pd.read_csv(filenames_list[-1], header = None)
        data.info()
        #data.head()
        appended_data.append(data)
        print("Processed month/year " + str(month) + "/" + str(y))

appended_data = pd.concat(appended_data, sort = False)
appended_data.info()
print(appended_data.head())
appended_data = appended_data.drop(0, axis = 1)
appended_data.info()
appended_data.columns = ['RIC', 'Owner_type', 'Date', 'Fund_name', 'Fund_RIC', 'AdjNbSharesHeld']
print(appended_data.head())
appended_data.to_csv("Monthly/IntlFundOwners_Monthly_Full_db.csv", header=True, index = False, mode = 'a')
