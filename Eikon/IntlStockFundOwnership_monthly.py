# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 12:31:08 2019
Adapted from StockFundOwnership_monthly.py for international (i.e. non-US stocks)

@author: gpichard
"""

import os
import numpy as np
import pandas as pd
import datetime as dt
import eikon
import configparser as cp
from itertools import compress


if __name__ == "__main__":
    cfg = cp.ConfigParser()
    cfg.read('eikon.cfg')
    eikon.set_app_key(cfg['eikon']['app_id'])

    from itertools import compress
    import multiprocessing

def Concat_Stocks(path):
    region_files_list = os.listdir(path)
    Stocks_db = list()
    for f in region_files_list:
        region_file_db = pd.read_excel(path + f, header = 0)
        Stocks_db.append(region_file_db)
    Stocks_db = pd.concat(Stocks_db, sort = False)
    return Stocks_db

def Get_Data(eikon_iter_ric_list, date):
    FundOwners, err = eikon.get_data(eikon_iter_ric_list, ['TR.FundInvestorType', 'TR.FundHoldingsDate', 'TR.FundPortfolioName', 'TR.FundClassID', 'TR.FundAdjShrsHeld'], {'SDate':date})
    return FundOwners

def Loop_Stocks(ric_list, date):
    N_stocks = len(ric_list)
    print("Number of stocks : ", N_stocks)
    initial_value = 0
    ideal_width = 50
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
        #print(eikon_iter_ric_list)
        try:
            FundOwners = Get_Data(eikon_iter_ric_list, date)
            #print(FundOwners)
            FundOwners = FundOwners.loc[FundOwners['Lipper Primary Fund Class ID'].notnull() & (FundOwners['Fund Shares Held (Adjusted)'] > 0)]
            #print(FundOwners)
            FundOwners.to_csv("Monthly/IntlFundOwners_"+date+"_db.csv", mode = 'a', header = False)
            print("init", initial_value, "end", end_value, "-> OK !")
            initial_value += width
            width = ideal_width
        except:
            width //= 4
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


def main():
    Stocks_db = Concat_Stocks('./NonUS_StocksLists/')
    ric_list = Stocks_db.RIC.tolist()

    #eikon_ric_list = ','.join(ric_list[1000:1001])
    #print(ric_list)
    mo = range(1, 13)
    dd = list([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    years = range(1999, 2019, 1)
    ref_dates = list()
    dates_list = list()
    for y in years:
        for m, month in enumerate(mo):
            dates_list.append(str(y) + "-" + str(month).zfill(2) + "-" + str(dd[m]))
            ref_dates.append(dt.datetime(y, month, dd[m], 0, 0))
    for i, date in enumerate(dates_list):
        #p = multiprocessing.Pool(processes = multiprocessing.cpu_count()) # - 1 if more than one CPU
        #print("Number of CPU cores used : ", multiprocessing.cpu_count()) # - 1
        print("Processing date : ", date)
        print(ref_dates[i])
        stocks_kept = [None for x in range(len(ric_list))]
        for j in range(0, len(ric_list)):
            if isinstance(Stocks_db.Date_Became_Public.iat[j], dt.datetime):
                stocks_kept[j] = Stocks_db.Date_Became_Public.iat[j] < ref_dates[i]
            else:
                stocks_kept[j] = True
                #print(list(compress(ric_list, stocks_kept)))
        print(date)
        Loop_Stocks(list(compress(ric_list,stocks_kept)), date)
        print("Processed date : ", date)
        #p.close()
        #p.join()

if __name__ == "__main__":
    main()
