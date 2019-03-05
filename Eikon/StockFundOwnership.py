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

#eikon_ric_list = ','.join(ric_list[1000:1001])
#print(ric_list)
     #   Constituents = Constituents.loc[(Constituents['Holdings Pct Of Shares Outstanding Held'].notnull()) & (Constituents['Holdings Pct Of Shares Outstanding Held'] > 0) & (Constituents['Constituent RIC'] != '')]
        #print(Constituents)
      #  Constituents.to_csv("Ex1.csv", mode = 'a', header = False)
dates_list = ['2015-12-31', '2014-12-31', '2013-12-31', '2012-12-31', '2011-12-31', '2010-12-31', '2009-12-31', '2008-12-31', '2007-12-31', '2006-12-31', '2005-12-31', '2004-12-31', '2003-12-31', '2002-12-31', '2001-12-31', '2000-12-31', '1999-12-31']
def Get_Data(eikon_iter_ric_list, date):
    FundOwners, err = eikon.get_data(eikon_iter_ric_list, ['TR.FundInvestorType', 'TR.FundHoldingsDate', 'TR.FundPortfolioName', 'TR.FundClassID', 'TR.FundAdjShrsHeld'], {'SDate':date})
    return FundOwners
def Loop_Stocks(ric_list, date):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 100
    width = ideal_width
    while initial_value < N_stocks:
        if width == 0:
            initial_value += 1
            width = ideal_width
        if initial_value + width - 1 >= N_stocks:
            end_value = None 
        else:
            end_value = initial_value + width
        eikon_iter_ric_list = ','.join(ric_list[initial_value:end_value])
        attempt_count = 0
        get_data_successful = True
        try:
            FundOwners = Get_Data(eikon_iter_ric_list)
        except:
            #attempt_count += 1
            #print("Additional attempt(s): ", attempt_count)
            #try :
                #FundOwners = Get_Data(eikon_iter_ric_list)
            #except:
                #attempt_count += 1
                #print("Additional attempt(s): ", attempt_count)
                #try:
                    #FundOwners = Get_Data(eikon_iter_ric_list)
                #except:
                # print("Could not succeed after ", attempt_count, " attempts. Init value: ", initial_value, ". Width . ", width)
            width //= 2
            get_data_successful = False
        if get_data_successful:
            FundOwners = FundOwners.loc[FundOwners['Lipper Primary Fund Class ID'].notnull() & (FundOwners['Fund Shares Held (Adjusted)'] > 0)]
            #print(FundOwners)
            FundOwners.to_csv("FundOwners_"+date+"_db.csv", mode = 'a', header = False)
            FundOwners.to_hdf("FundOwners_"+date+"_db.hdf", key = 'owners_shares', complevel = 6, complib = 'zlib')
            #FundOwners.to_sql('FundOwners_db', engine, if_exists = 'append', index = True, index_label = "Instrument")
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width

    return 0

for date in dates_list:
    print("Processing date : ", date)
    Loop_Stocks(ric_list, date)
    print("Processed date : ", date)
