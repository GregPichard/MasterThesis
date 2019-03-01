import numpy as np
import pandas as pd
import eikon
import configparser as cp
cfg = cp.ConfigParser()
cfg.read('eikon.cfg')
eikon.set_app_key(cfg['eikon']['app_id'])


ETF_db = pd.read_excel("ReportEikon_ETF_live_20190228.xlsx", header = 0)
#print(ETF_db.head())
#print(ETF_db[['Lipper RIC']].nunique()) #All different
#print(ETF_db[['Lipper RIC']].dropna().nunique())# Same length, thus no NA.
ric_list = ETF_db.Lipper_RIC.tolist()
#print(ric_list)
for ric in ric_list[0:1]:
    #print(ric)
    Constituents, err = eikon.get_data(ric, ['TR.IndexConstituentRIC', 'TR.PctOfSharesOutHeld'], {'SDate':['2019-01-31', '2018-12-31']})
    print(Constituents)
#Constituents, err = eikon.get_data(ric_list[0], 'TR.IndexConstituentRIC', {'SDate':'2019-01-31'})
#print(Constituents)
