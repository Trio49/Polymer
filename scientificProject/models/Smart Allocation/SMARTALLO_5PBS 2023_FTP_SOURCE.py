# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 14:19:24 2020

@author: zzhang

SMART ALLOCATION MODEL
"""

import numpy as np
import pandas as pd
import pytz
# import jsonSMART
# import pprint
# import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from os import listdir

pd.set_option('mode.chained_assignment', None)

today = datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially
today = today.date()
# today=today.date()-timedelta(days=1)
# today=today.date()
today = today.strftime('%Y%m%d')

# all_PM_expiryblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_expiryblotter_file_path = r"\\paghk.local\Polymer\HK\Department\All\Enfusion\Polymer_reports\Tora"
# all_PM_expiryblotter_file_path=r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\"+today
firm_positions_list_dir = listdir(all_PM_expiryblotter_file_path)
firm_positions_list_dir1 = firm_positions_list_dir[::-1]
# add a input box to choose the file name for 14:15 or 16:15 file
file = input("please choose the file: 1 for 14:15 file, 2 for 16:15 file")
if file == "1":
    for file_name in firm_positions_list_dir1:

        if "POLYMER_BORROW_1415" in file_name:
            # print(file_name)
            borrow_file :str= all_PM_expiryblotter_file_path + "\\" + file_name
            break
            print(borrow_file)
    for file_name in firm_positions_list_dir1:
        if "POLYMER_EOD_1415" in file_name:
            # print(file_name)
            trade_file: str = all_PM_expiryblotter_file_path + "\\" + file_name
            break
            print(trade_file)

    print(borrow_file)
    print(trade_file)
    filecheck = input('are files correct? y/n')
    if filecheck == 'y':
        borrows = pd.read_csv(borrow_file)
        raw_trades = pd.read_csv(trade_file,low_memory=False)
        raw_trades = raw_trades[raw_trades["Side"] == "SHORT"]
        raw_trades = raw_trades[raw_trades["CCY"].isin(["JPY", 'AUD'])]
    # borrows = borrows[borrows["CCY"].isin(["JPY",'AUD'])]
    # rename InternalAccount to Internal Account

else :
    for file_name in firm_positions_list_dir1:

        if "POLYMER_BORROW_1615" in file_name:
            # print(file_name)
            borrow_file:str=all_PM_expiryblotter_file_path + "\\" + file_name
            break
            print(borrow_file)

    for file_name in firm_positions_list_dir1:
        if "POLYMER_EOD_1615" in file_name:
            # print(file_name)
            trade_file: str =all_PM_expiryblotter_file_path + "\\" + file_name
            break
            print(trade_file)



    print(borrow_file)
    print(trade_file)
    filecheck=input('are files correct? y/n')
    if filecheck=='y':
        borrows = pd.read_csv(borrow_file)
        raw_trades = pd.read_csv(trade_file,low_memory=False)
        raw_trades = raw_trades[raw_trades["Side"] == "SHORT"]
        raw_trades = raw_trades[raw_trades["CCY"].isin(["HKD"])]

    # borrows = borrows[borrows["CCY"].isin(["HKD"])]

# T:\Daily trades\Daily Re\SmartAllo
today = datetime.now(pytz.timezone('Asia/Shanghai'))
# raw_trades= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Smart Allocation\SmartAllo\SmartAllocation.xlsx",sheet_name="trades")

# synthflag is empty
raw_trades = raw_trades[raw_trades["Syn Flag"] != 'swp']
raw_trades_gc_copy=raw_trades.copy()
#create a ticker and ccy dataframe called ccy_ticker
ccy_ticker= raw_trades[["BBG Code", "CCY"]]
raw_trades['Code']=raw_trades['BBG Code'].str.split(' ').str[0]
raw_trades["new_ticker_id"] = raw_trades["Code"].apply(lambda x:str(x)) + raw_trades["CCY"]
raw_trades = raw_trades[["Internal Account", "BBG Code", "new_ticker_id", "Execution Quantity"]]
raw_trades = raw_trades[raw_trades["Execution Quantity"] != 0]
#group the trades by account, ticker, and BBG code
raw_trades = raw_trades.groupby(["Internal Account", "new_ticker_id", "BBG Code"]).agg({"Execution Quantity":np.sum})

raw_trades.reset_index(inplace=True)

# borrows= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Smart Allocation\SmartAllo\SmartAllocation.xlsx",sheet_name="borrows")
#define Code as the 1st string before the 1st space
borrows['Code'] = borrows['BloombergCompositeCode'].str.split(' ').str[0]
borrows['Code'].fillna(0, inplace=True)
# borrows['Code']= borrows['Code'].astype(int)
borrows['Code'] = borrows['Code']
borrows = pd.merge(borrows, ccy_ticker, left_on='BloombergCode', right_on='BBG Code', how='left')
borrows["new_ticker_id"] = borrows["Code"].apply(lambda x:str(x)) + borrows["CCY"]
#Vlookup borrows with ccy_ticker to map on BBG code and BloombergCode

# rename Rate as Rate(bps)
borrows.rename(columns={"InternalAccount":"Internal Account"}, inplace=True)
borrows.rename(columns={"Rate":"Rate (bps)"}, inplace=True)
borrows.rename(columns={"ApprovedQty":"Filled"}, inplace=True)
borrows = borrows[["Internal Account", "new_ticker_id", "Broker", "Filled", "Rate (bps)",'CCY']]
# only filter out cash trades mkt: SGD JPY HKD


borrows = borrows.groupby(["Internal Account", "new_ticker_id", "Broker"]).agg({"Filled":np.sum, "Rate (bps)":np.mean})
# 6952JPY
borrows.reset_index(inplace=True)

GS_borrow = borrows[borrows["Broker"] == "gs"]
MS_borrow = borrows[borrows["Broker"] == "msdw"]
UBS_borrow = borrows[borrows["Broker"] == "ubs"]
BAML_borrow = borrows[borrows["Broker"] == "baml"]
JPM_borrow = borrows[borrows["Broker"] == "jpm"]
NOMU_borrow = borrows[borrows["Broker"] == "nomu"]

PBs = {"GS":GS_borrow, "MS":MS_borrow, "UBS":UBS_borrow, "BAML":BAML_borrow, "JPM":JPM_borrow, "NOMU":NOMU_borrow}

for pb in PBs:
    # pb="GS"
    raw_trades = raw_trades.merge(PBs[pb], left_on=["Internal Account", "new_ticker_id"],
                                  right_on=["Internal Account", "new_ticker_id"],
                                  how="left")
    raw_trades = raw_trades.drop(columns=["Broker"])
    raw_trades.fillna(0, inplace=True)
    raw_trades.rename(columns={"Filled":pb + " Qty", "Rate (bps)":pb + " Rate bps"}, inplace=True)

raw_trades["isGC"] = True

for i in range(len(raw_trades)):
    x1 = (raw_trades.loc[i, "GS Rate bps"] <= 51) & (raw_trades.loc[i, "GS Rate bps"] > 0)
    x2 = (raw_trades.loc[i, "MS Rate bps"] <= 51) & (raw_trades.loc[i, "MS Rate bps"] > 0)
    x3 = (raw_trades.loc[i, "UBS Rate bps"] <= 51) & (raw_trades.loc[i, "UBS Rate bps"] > 0)
    x4 = (raw_trades.loc[i, "BAML Rate bps"] <= 51) & (raw_trades.loc[i, "BAML Rate bps"] > 0)
    x5 = (raw_trades.loc[i, "JPM Rate bps"] <= 51) & (raw_trades.loc[i, "JPM Rate bps"] > 0)
    x6 = (raw_trades.loc[i, "NOMU Rate bps"] <= 51) & (raw_trades.loc[i, "NOMU Rate bps"] > 0)
    if (x1 & x2 & x3 & x4 & x5 & x6):
        raw_trades.loc[i, "isGC"] = True
    else:
        raw_trades.loc[i, "isGC"] = False

raw_trades_HTB = raw_trades[raw_trades["isGC"] == False]
raw_trades_HTB["SmartAllo"] = ""
raw_trades_HTB["Cmmts"] = ""

raw_trades_HTB.reset_index(inplace=True)
raw_trades_HTB.drop(columns=["index"], inplace=True)

# for i in range(len(raw_trades_HTB)):
#     print(i)

# raw_trades_HTB['largest locate'] = raw_trades_HTB[
#     # ['GS Qty', 'MS Qty', 'UBS Qty', 'UBS Qty', 'BAML Qty', 'JPM Qty', 'NOMU Qty']].max(axis=1)
raw_trades_HTB['largest locate'] = raw_trades_HTB[
    ['GS Qty', 'MS Qty', 'UBS Qty', 'UBS Qty', 'BAML Qty', 'JPM Qty']].max(axis=1)
#rename Execution Quantity as TotalFilled
raw_trades_HTB.rename(columns={"Execution Quantity":"TotalFilled"}, inplace=True)
for i in range(len(raw_trades_HTB)):
    if raw_trades_HTB.loc[i, 'TotalFilled'] - raw_trades_HTB.loc[i, 'largest locate'] >= 0:
        raw_trades_HTB.loc[i, 'Needmorelocate'] = raw_trades_HTB.loc[i, 'TotalFilled'] - raw_trades_HTB.loc[
            i, 'largest locate']
    else:
        raw_trades_HTB.loc[i, 'Needmorelocate'] = 0
for i in range(len(raw_trades_HTB)):

    # i=0
    print(i)
    optimal_allo = {}
    miss_allo = {}

    ss_qty = raw_trades_HTB.loc[i, "TotalFilled"]
    for pb in PBs:
        pb_coverqty = raw_trades_HTB.loc[i, pb + " Qty"]
        if pb_coverqty < ss_qty:
            # optimal_allo[pb]=0.0001
            miss_allo[pb] = raw_trades_HTB.loc[i, pb + " Qty"]
        elif pb_coverqty >= ss_qty:
            try:
                optimal_allo[pb] = raw_trades_HTB.loc[i, pb + ' Rate bps']
            except:
                continue
            # optimal_allo[pb]=raw_trades_HTB.loc[i,pb+" Qty"]

            # needmoreborrow[pb]=raw_trades_HTB.loc[i,"BBG Code"]
    # optimal allo not empty
    if bool(optimal_allo):
        length = len(optimal_allo)
        x = []
        for ii in optimal_allo:
            #if <51 and >0, ignore NOMU Rate bps
            xi=(optimal_allo[ii]<51 and optimal_allo[ii]>0)
            x.append(xi)
        if (([True] * length) == x):
            raw_trades_HTB.loc[i, "Cmmts"] = "Liquid_Less_Important"

        try:
            minval = min(filter(None, optimal_allo.values()))

        except:
            continue

    raw_trades_HTB.loc[i, "SmartAllo"] = str(dict((k, v) for k, v in optimal_allo.items() if v == minval))
    raw_trades_HTB.loc[i, "Missallo"] = str(dict((k, v) for k, v in miss_allo.items()))
    # raw_trades_HTB.loc[i,"needmoreborrow"]=str(dict((k,v) for k,v in needmoreborrow.items()))
    #for rows that defined as Liquid_Less_Important, if Missallo has a non NOMU value, then it is important， if it only has NOMU, then it is less important
    if raw_trades_HTB.loc[i, "Cmmts"] == "Liquid_Less_Important":
        #if Missallo has 1 value and it is NOMU, then it is less important
        if len(miss_allo) == 1 and "NOMU" in miss_allo:
            raw_trades_HTB.loc[i, "Cmmts"] = "Liquid_Less_Important"
        else:
            raw_trades_HTB.loc[i, "Cmmts"] = "Liquid_Important"
# PTH= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx",index=False)


raw_trades_HTB_IMPORTANT = raw_trades_HTB[
    (raw_trades_HTB["Cmmts"] != "Liquid_Less_Important") | (raw_trades_HTB["Needmorelocate"] > 0)]

# raw_trades_HTB_IMPORTANT= raw_trades_HTB_IMPORTANT[['Internal Account','BBG Code','TotalFilled','SmartAllo']]

# raw_trades_HTB_IMPORTANT.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\Smart_Allo_Outputv2.xlsx",index=False)
# search the ticker
raw_trades_HTB_IMPORTANT = raw_trades_HTB_IMPORTANT[['Internal Account', 'BBG Code', 'TotalFilled',
                                                     'GS Qty', 'GS Rate bps', 'MS Qty', 'MS Rate bps', 'UBS Qty',
                                                     'UBS Rate bps', 'BAML Qty', 'BAML Rate bps', 'JPM Qty',
                                                     'JPM Rate bps',
                                                     'NOMU Qty', 'NOMU Rate bps', 'isGC', 'SmartAllo', 'Cmmts',
                                                     'largest locate', 'Needmorelocate', 'Missallo']]
# Tora ticker massage
raw_trades_HTB_IMPORTANT['BBG Code'] = raw_trades_HTB_IMPORTANT['BBG Code'].str.replace('AT Equity', 'AU Equity')
raw_trades_HTB_IMPORTANT['BBG Code'] = raw_trades_HTB_IMPORTANT['BBG Code'].str.replace('JT Equity', 'JP Equity')

# PTHtoday= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx",index=False)
PTHtoday = pd.read_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx")
PTH = PTHtoday[['Underlying BB Yellow Key', 'PM', 'PB', 'Position']]
# ticker massage

PTH['Existing_PTH_PB'] = PTH['PB'] + ' ' + PTH['Position']
raw_trades_HTB_IMPORTANT['short_scenario'] = raw_trades_HTB_IMPORTANT['Internal Account'] + ' ' + \
                                             raw_trades_HTB_IMPORTANT['BBG Code']
PTH['PM'] = PTH['PM'].str.replace('ECMIPO', 'ECMIPO_PO')
PTH['pth_scenario'] = PTH['PM'] + ' ' + PTH['Underlying BB Yellow Key']
PTH = PTH[['pth_scenario', 'Existing_PTH_PB']]
PTH.to_excel(r"P:\Operations\Polymer - Middle Office\Smart Allocation\SmartAllo/PTHraw.xlsx", index=False)
raw_trades_HTB_IMPORTANT = raw_trades_HTB_IMPORTANT.merge(PTH, left_on='short_scenario', right_on='pth_scenario',
                                                          copy=False, how='left')
raw_trades_HTB_IMPORTANT.to_excel(r"P:\Operations\Polymer - Middle Office\Smart Allocation\SmartAllo/HTBraw.xlsx",
                                  index=False)
raw_trades_HTB_IMPORTANT1 = raw_trades_HTB_IMPORTANT[
    ['Internal Account', 'BBG Code', 'TotalFilled', 'GS Qty', 'GS Rate bps',
     'MS Qty', 'MS Rate bps', 'UBS Qty', 'UBS Rate bps', 'BAML Qty',
     'BAML Rate bps', 'JPM Qty', 'JPM Rate bps', 'NOMU Qty', 'NOMU Rate bps',
     'isGC', 'SmartAllo', 'Existing_PTH_PB', 'largest locate', 'Needmorelocate',
     'Missallo']]
raw_trades_HTB_IMPORTANT1.to_excel(
    r"P:\Operations\Polymer - Middle Office\Smart Allocation\SmartAllo\Smart_Allo_Output_tora.xlsx", index=False)

# def gc_short_pthcheck():
# locate the PTH report
from datetime import datetime
import pytz
from os import listdir
import pandas as pd

today = datetime.now(pytz.timezone('Asia/Shanghai'))
today = today.date()
today = today.strftime('%Y%m%d')

all_PM_PTH_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"
firm_positions_list_dir = listdir(all_PM_PTH_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:

    if "PTH REPORT_PTH position report" in file_name:
        print(file_name)

        all_PM_expiryblotter_file_path = all_PM_PTH_file_path + "\\" + file_name
        break

PTHtoday = pd.read_csv(all_PM_expiryblotter_file_path, engine='python', thousands=',')
# create a new df called PTH_GC which only includes coupon rate < 0.5
PTH_GC = PTHtoday[PTHtoday['Active Coupon Rate'] < 0.5]
# only includes Underying BB Yellow Key ends with JP Equity,AU Equity and HK Equity
PTH_GC = PTH_GC[PTH_GC['Underlying BB Yellow Key'].str.endswith(('JP Equity', 'AU Equity', 'HK Equity'))]
# first 5 characters of sub business unit as 'pm'
PTH_GC['pm'] = PTH_GC['Sub Business Unit'].str[:5]
# create id column as concatenation of pm and Underlying BB Yellow Key
PTH_GC['id'] = PTH_GC['pm'] + ' ' + PTH_GC['Underlying BB Yellow Key']
# load raw trades see any Ticker matches

raw_trades_gc_copy = raw_trades_gc_copy[raw_trades_gc_copy["Side"] == "SHORT"]
raw_trades_gc_copy['pm'] = raw_trades_gc_copy['Internal Account'].str[:5]
raw_trades_gc_copy['id'] = raw_trades_gc_copy['pm'] + ' ' + raw_trades_gc_copy['BBG Code']
# create a list of ticker from PTH_GC['Underlying BB Yellow Key'],
# cross check against all the raw_trades, add a new dataframe to include Underlying BB yellow key, custodian account display name and short sell quantity
# create a new list ot store the result
gc_short = []
for i in PTH_GC[['id']].values.tolist():
    for j in raw_trades_gc_copy[['id']].values.tolist():
        if i == j:
            gc_short.append(j)
            # print a gc_short list
# if gc_short is not empty, create a dataframe to store the result
if gc_short:
    gc_short_pth = pd.DataFrame(gc_short, columns=['id'])
    # for i in gc_short, cross check against raw trades, add 'TotalFilled' column to dataframe the
    # for i in gc_short, cross check against PTH_GC, add 'Position' column to dataframe
    gc_short_pth = gc_short_pth.merge(raw_trades_gc_copy[['id', 'TotalFilled']], left_on='id', right_on='id', copy=False,
                                      how='left')
    gc_short_pth = gc_short_pth.merge(PTH_GC[['id', 'Position']], left_on='id', right_on='id', copy=False, how='left')
    # rename columns
    gc_short_pth.rename(columns={'id':'Ticker', 'TotalFilled':'Short Qty', 'Position':'PTH Qty'}, inplace=True)
    print("the following GC names has been shorted today:"
          ""
          ""
          ""
          "")
    print(gc_short_pth)
