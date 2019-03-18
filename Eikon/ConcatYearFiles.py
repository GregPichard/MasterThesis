import pandas as pd
import numpy as np

years = range(1997, 2019, 1)
mo = 12
dd = 31
dates_list = list()
filenames_list = list()
appended_data = list()
for y in years:
    print("Processing year ", y)
    dates_list.append(str(y) + "-" + str(mo) + "-" + str(dd))
    # filenames_list.append("FundOwners_" + dates_list[-1] + "_db.csv") # On the VM
    filenames_list.append("../../../../../../switchdrive/ETF/FundOwners_" + dates_list[-1] + "_db.csv") # Greg's local path
    data = pd.read_csv(filenames_list[-1], header = None)
    data.info()
    data.head()
    appended_data.append(data)
    print("Processed year ", y)

appended_data = pd.concat(appended_data, sort = False)
appended_data.info()
print(appended_data.head())
appended_data = appended_data.drop(0, axis = 1)
appended_data.info()
appended_data.columns = ['RIC', 'Owner_type', 'Date', 'Fund_name', 'Fund_RIC', 'AdjNbSharesHeld']
print(appended_data.head())
appended_data.to_csv("FundOwners_Full_db.csv", header=True, index = False)
