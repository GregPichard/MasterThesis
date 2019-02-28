import numpy as np
import pandas as pd
import eikon
eikon.set_app_key('638fa3bbb90349d5a97dc60c1c0cc4b0b5646846')


ETF_db = pd.read_excel("ReportEikon_ETF_live_20190228.xlsx", header = 0)
#print(ETF_db.head())
#print(ETF_db[['Lipper RIC']].nunique()) #All different
#print(ETF_db[['Lipper RIC']].dropna().nunique())# Same length, thus no NA.
ric_list = np.array((ETF_db[['Lipper RIC']]))
#print(str(ric_list[0]))
for ric in ric_list[0:3]:
    print(ric[0])
    Constituents, err = eikon.get_data(ric[0], ['TR.IndexConstituentRIC', {'TR.IndexConstituentWeightPercent':{'sort_dir':'desc'}}, 'TR.PctOfSharesOutHeld'], {'SDate':'2019-01-31'})
    print(Constituents)

