import numpy as np
import pandas as pd
import eikon
import configparser as cp
cfg = cp.ConfigParser()
cfg.read('eikon.cfg')
eikon.set_app_key(cfg['eikon']['app_id'])

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres@localhost:5432/FundOwners_db')
Stocks_db = pd.read_excel("ReportEikon_Stocks_US_static_20190304.xlsx", header = 0)
ric_list = Stocks_db.RIC.tolist()
N_stocks = len(ric_list)
#eikon_ric_list = ','.join(ric_list[1000:1001])
#print(ric_list)
     #   Constituents = Constituents.loc[(Constituents['Holdings Pct Of Shares Outstanding Held'].notnull()) & (Constituents['Holdings Pct Of Shares Outstanding Held'] > 0) & (Constituents['Constituent RIC'] != '')]
        #print(Constituents)
      #  Constituents.to_csv("Ex1.csv", mode = 'a', header = False)

initial_value = 4975
width = 5
while initial_value < N_stocks:
    if initial_value + width - 1 >= N_stocks:
        end_value = None 
    else:
        end_value = initial_value + width
    eikon_iter_ric_list = ','.join(ric_list[initial_value:end_value])
    FundOwners, err = eikon.get_data(eikon_iter_ric_list, ['TR.FundInvestorType', 'TR.FundHoldingsDate', 'TR.FundPortfolioName', 'TR.FundClassID', 'TR.FundAdjShrsHeld'], {'SDate':'2019-01-31'})
    FundOwners = FundOwners.loc[FundOwners['Lipper Primary Fund Class ID'].notnull() & (FundOwners['Fund Shares Held (Adjusted)'] > 0)]
    #print(FundOwners.loc[:,"Fund Holdings Filing Date"].unique())
    FundOwners.to_csv("FundOwners_db.csv", mode = 'a', header = False)
    FundOwners.to_hdf('FundOwners_db.hdf', key = 'owners_shares', complevel = 6, complib = 'zlib')
    #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
    print("init", initial_value, "end", end_value)
    initial_value += width
