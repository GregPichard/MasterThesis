import numpy as np
import pandas as pd
import eikon
import configparser as cp
cfg = cp.ConfigParser()
cfg.read('eikon.cfg')
eikon.set_app_key(cfg['eikon']['app_id'])


#ETF_db = pd.read_excel("ReportEikon_ETF_live_20190228.xlsx", header = 0)
#print(ETF_db.head())
#print(ETF_db[['Lipper RIC']].nunique()) #All different
#print(ETF_db[['Lipper RIC']].dropna().nunique())# Same length, thus no NA.
#ETF_db = ETF_db.loc[ETF_db.Index_Replication_Method != 'Swap']
#ETF_db.to_excel("ReportEikon_ETF_live&nonswap_20190303.xlsx")
ETF_db = pd.read_excel("ReportEikon_ETF_live&nonswap_20190303.xlsx", header = 0)
ric_list = ETF_db.Lipper_RIC.tolist()
#print(ric_list)
for ric in ric_list[814:]:
    print(ric)
    Constituents, err = eikon.get_data(ric, ['TR.ETPConstituentName', 'TR.PctOfSharesOutHeld'], {'SDate':'2019-01-31'})
    print(Constituents)
    #ValidRIC_rowsnb = Constituents.loc[Constituents['Constituent RIC'].notnull()].shape[0]
    #print(ValidRIC_rowsnb)
    #if ValidRIC_rowsnb > 0:
     #   Constituents = Constituents.loc[(Constituents['Holdings Pct Of Shares Outstanding Held'].notnull()) & (Constituents['Holdings Pct Of Shares Outstanding Held'] > 0) & (Constituents['Constituent RIC'] != '')]
        #print(Constituents)
      #  Constituents.to_csv("Ex1.csv", mode = 'a', header = False)
#Constituents, err = eikon.get_data('0#.FTSE', 'TR.PriceClose')
#print(Constituents)
