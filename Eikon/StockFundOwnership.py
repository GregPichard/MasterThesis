import numpy as np
import pandas as pd
import datetime as dt
import eikon
import configparser as cp
cfg = cp.ConfigParser()
cfg.read('eikon.cfg')
eikon.set_app_key(cfg['eikon']['app_id'])

from itertools import compress
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:postgres@localhost:5432/FundOwners_db')
Stocks_db = pd.read_excel("ReportEikon_Stocks_US_static_20190304.xlsx", header = 0)
ric_list = Stocks_db.RIC.tolist()

#eikon_ric_list = ','.join(ric_list[1000:1001])
#print(ric_list)
     #   Constituents = Constituents.loc[(Constituents['Holdings Pct Of Shares Outstanding Held'].notnull()) & (Constituents['Holdings Pct Of Shares Outstanding Held'] > 0) & (Constituents['Constituent RIC'] != '')]
        #print(Constituents)
      #  Constituents.to_csv("Ex1.csv", mode = 'a', header = False)
mo = 12
dd = 31
years = range(2014, 1998, -1)
ref_dates = list()
dates_list = list()
for y in years:
    dates_list.append(str(y) + "-" + str(mo) + "-" + str(dd))
    ref_dates.append(dt.datetime(y, mo, dd, 0, 0))

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
        print(eikon_iter_ric_list)
        attempt_count = 0
        get_data_successful = True
        try:
            FundOwners = Get_Data(eikon_iter_ric_list, date)
        except:
            #attempt_count += 1
            #print("Additional attempt(s): ", attempt_count)
            #try :
                #FundOwners = Get_Data(eikon_iter_ric_list, date)
            #except:
                #attempt_count += 1
                #print("Additional attempt(s): ", attempt_count)
                #try:
                    #FundOwners = Get_Data(eikon_iter_ric_list, date)
                #except:
                # print("Could not succeed after ", attempt_count, " attempts. Init value: ", initial_value, ". Width . ", width)
            width //= 4
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

for i, date in enumerate(dates_list):
    print("Processing date : ", date)
    print(ref_dates[i])
    stocks_kept = [None for x in range(len(ric_list))]
    for j in range(0, len(ric_list)):
        if isinstance(Stocks_db.Date_Became_Public[j], dt.datetime):
            stocks_kept[j] = Stocks_db.Date_Became_Public[j] < ref_dates[i]
        else:
            stocks_kept[j] = True
    #print(list(compress(ric_list, stocks_kept)))
    print(date)
    Loop_Stocks(list(compress(ric_list,stocks_kept)), date)
    print("Processed date : ", date)
