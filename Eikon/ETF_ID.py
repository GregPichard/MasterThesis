import numpy as np
import pandas as pd
import eikon
eikon.set_app_key('638fa3bbb90349d5a97dc60c1c0cc4b0b5646846')


ETF_db = pd.read_excel("ReportEikon_ETF_20180224.xlsx", header = 0)
print(ETF_db.head())
print(ETF_db[['Lipper RIC']].nunique()) #All different
print(ETF_db[['Lipper RIC']].dropna().nunique())# Same length, thus no NA.

for ric in ETF_db[['Lipper RIC']].loc([1:3]):
    #Constituents, err = eikon.get_data(ric, ['TR.IndexConstituentRIC', {'TR.IndexConstituentWeightPercent':{'sort_dir':'desc'}}], {'SDate':'2019-02-01'})
    #print(Constituents)
    print(ric)
