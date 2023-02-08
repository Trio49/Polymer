
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 14:19:24 2020

@author: tyin,zzhang

SMART ALLOCATION MODEL
"""



import numpy as np
import pandas as pd
import pytz
#import jsonSMART
#import pprint
#import requests
from datetime import datetime, timedelta
import matplotlib.pyplot as plt 
from os import listdir
pd.set_option('mode.chained_assignment', None)


#T:\Daily trades\Daily Re\SmartAllo
today=datetime.now(pytz.timezone('Asia/Shanghai'))
raw_trades= pd.read_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocation.xlsx",sheet_name="trades")
raw_trades=raw_trades[raw_trades["Side"]=="SHORT"]
raw_trades=raw_trades[raw_trades["CCY"].isin(["SGD","JPY","HKD","USD",'CAD','AUD'])]
raw_trades=raw_trades[raw_trades["SynthType"]!="swp"]
raw_trades["new_ticker_id"]= raw_trades["Code"].apply(lambda x:str(x))+raw_trades["CCY"]
raw_trades=raw_trades[["Internal Account","BBG Code","new_ticker_id","TotalFilled","Notional 2"]]
raw_trades=raw_trades[raw_trades["TotalFilled"]!=0]

raw_trades=raw_trades.groupby(["Internal Account","BBG Code","new_ticker_id"])["TotalFilled","Notional 2"].sum()
raw_trades.reset_index(inplace=True)



borrows= pd.read_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocation.xlsx",sheet_name="borrows")
borrows['Code'].fillna(0,inplace=True)
# borrows['Code']= borrows['Code'].astype(int)
borrows['Code']= borrows['Code']
borrows["new_ticker_id"] =  borrows["Code"].apply(lambda x:str(x))+borrows["CCY"]

#only filter out cash trades mkt: SGD JPY HKD
borrows=borrows[borrows["CCY"].isin(["SGD","JPY","HKD",'USD','CAD','AUD'])]
borrows=borrows[["Internal Account","new_ticker_id","Broker","Filled","Rate (bps)"]]

borrows=borrows.groupby(["Internal Account","new_ticker_id","Broker"]).agg({"Filled":np.sum,"Rate (bps)":np.mean})
#6952JPY
borrows.reset_index(inplace=True)

GS_borrow =borrows[borrows["Broker"]=="gs"]
MS_borrow =borrows[borrows["Broker"]=="msdw"]
UBS_borrow =borrows[borrows["Broker"]=="ubs"]
BAML_borrow =borrows[borrows["Broker"]=="baml"]
JPM_borrow =borrows[borrows["Broker"]=="jpm"]
NOMU_borrow =borrows[borrows["Broker"]=="nomu"]

PBs={"GS":GS_borrow,"MS":MS_borrow,"UBS":UBS_borrow,"BAML":BAML_borrow,"JPM":JPM_borrow,"NOMU":NOMU_borrow}




for pb in PBs:
    #pb="GS"
    raw_trades = raw_trades.merge(PBs[pb],left_on= ["Internal Account","new_ticker_id"],
                    right_on=["Internal Account","new_ticker_id"],
                    how="left")
    raw_trades = raw_trades.drop(columns=["Broker"])
    raw_trades.fillna(0,inplace=True)
    raw_trades.rename(columns={"Filled":pb+" Qty","Rate (bps)":pb+" Rate bps"},inplace=True)




raw_trades["isGC"]=True
    
for i in range(len(raw_trades)):
    x1=(raw_trades.loc[i,"GS Rate bps"]<=51) & (raw_trades.loc[i,"GS Rate bps"]>0)
    x2=(raw_trades.loc[i,"MS Rate bps"]<=51) & (raw_trades.loc[i,"MS Rate bps"]>0)
    x3=(raw_trades.loc[i,"UBS Rate bps"]<=51) & (raw_trades.loc[i,"UBS Rate bps"]>0)
    x4=(raw_trades.loc[i,"BAML Rate bps"]<=51) & (raw_trades.loc[i,"BAML Rate bps"]>0)
    x5=(raw_trades.loc[i,"JPM Rate bps"]<=51) & (raw_trades.loc[i,"JPM Rate bps"]>0)
    x6=(raw_trades.loc[i,"NOMU Rate bps"]<=51) & (raw_trades.loc[i,"NOMU Rate bps"]>0)
    if (x1 & x2 & x3 & x4 & x5& x6):
        raw_trades.loc[i,"isGC"]=True
    else:
        raw_trades.loc[i,"isGC"]=False
        

    
    

raw_trades_HTB=raw_trades[raw_trades["isGC"]==False]
raw_trades_HTB["SmartAllo"]=""
raw_trades_HTB["Cmmts"]=""



raw_trades_HTB.reset_index(inplace=True)
raw_trades_HTB.drop(columns=["index"],inplace=True)

# for i in range(len(raw_trades_HTB)):
#     print(i)

raw_trades_HTB['largest locate']=raw_trades_HTB[['GS Qty','MS Qty','UBS Qty','UBS Qty','BAML Qty','JPM Qty','NOMU Qty']].max(axis=1)
for i in range(len(raw_trades_HTB)):
    if raw_trades_HTB.loc[i,'TotalFilled']-raw_trades_HTB.loc[i,'largest locate']>=0:
     raw_trades_HTB.loc[i,'Needmorelocate']=raw_trades_HTB.loc[i,'TotalFilled']-raw_trades_HTB.loc[i,'largest locate']
    else:
     raw_trades_HTB.loc[i,'Needmorelocate']=0
for i in range(len(raw_trades_HTB)):
    
    #i=0
    print(i)
    optimal_allo={}
    miss_allo={}
    
    ss_qty=raw_trades_HTB.loc[i,"TotalFilled"]
    for pb in PBs:
        pb_coverqty= raw_trades_HTB.loc[i, pb+" Qty"]
        if pb_coverqty<ss_qty:
            # optimal_allo[pb]=0.0001
            miss_allo[pb]=raw_trades_HTB.loc[i,pb+" Qty"]
        elif pb_coverqty>= ss_qty:
            try:optimal_allo[pb]=raw_trades_HTB.loc[i,pb+' Rate bps']
            except:
                continue
            # optimal_allo[pb]=raw_trades_HTB.loc[i,pb+" Qty"]
            
            # needmoreborrow[pb]=raw_trades_HTB.loc[i,"BBG Code"]
    # optimal allo not empty
    if bool(optimal_allo):
        length = len(optimal_allo)
        x= []
        for ii in optimal_allo:
            xi=(optimal_allo[ii]<51)
            x.append(xi)
        if (([True] * length) == x):
            raw_trades_HTB.loc[i,"Cmmts"]="Liquid_Less_Important"
            

        try:minval = min(filter(None,optimal_allo.values()))
           
        except:
            continue
    
    raw_trades_HTB.loc[i,"SmartAllo"]=str(dict((k,v) for k,v in optimal_allo.items() if v==minval))
    raw_trades_HTB.loc[i,"Missallo"]=str(dict((k,v) for k,v in miss_allo.items()))
    # raw_trades_HTB.loc[i,"needmoreborrow"]=str(dict((k,v) for k,v in needmoreborrow.items()))
    
# PTH= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx",index=False)


raw_trades_HTB_IMPORTANT= raw_trades_HTB[(raw_trades_HTB["Cmmts"]!="Liquid_Less_Important")|(raw_trades_HTB["Needmorelocate"]>0)]

#raw_trades_HTB_IMPORTANT= raw_trades_HTB_IMPORTANT[['Internal Account','BBG Code','TotalFilled','SmartAllo']]

# raw_trades_HTB_IMPORTANT.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\Smart_Allo_Outputv2.xlsx",index=False)
#search the ticker
raw_trades_HTB_IMPORTANT=raw_trades_HTB_IMPORTANT[['Internal Account', 'BBG Code', 'TotalFilled',
       'GS Qty', 'GS Rate bps', 'MS Qty', 'MS Rate bps', 'UBS Qty',
       'UBS Rate bps', 'BAML Qty', 'BAML Rate bps', 'JPM Qty', 'JPM Rate bps',
       'NOMU Qty', 'NOMU Rate bps', 'isGC', 'SmartAllo', 'Cmmts',
       'largest locate', 'Needmorelocate', 'Missallo']]
#Tora ticker massage
raw_trades_HTB_IMPORTANT['BBG Code'] = raw_trades_HTB_IMPORTANT['BBG Code'].str.replace('AT Equity','AU Equity')
raw_trades_HTB_IMPORTANT['BBG Code'] = raw_trades_HTB_IMPORTANT['BBG Code'].str.replace('JT Equity','JP Equity')

# PTHtoday= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx",index=False)
PTHtoday= pd.read_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx")
PTH=PTHtoday[['Underlying BB Yellow Key', 'PM', 'PB','Position']]
#ticker massage

PTH['Existing_PTH_PB']=PTH['PB']+' '+PTH['Position']
raw_trades_HTB_IMPORTANT['short_scenario']=raw_trades_HTB_IMPORTANT['Internal Account']+' '+raw_trades_HTB_IMPORTANT['BBG Code']
PTH['PM'] = PTH['PM'].str.replace('ECMIPO','ECMIPO_PO')
PTH['pth_scenario']=PTH['PM']+' '+PTH['Underlying BB Yellow Key']
PTH=PTH[['pth_scenario','Existing_PTH_PB']]
PTH.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\PTHraw.xlsx",index=False)
raw_trades_HTB_IMPORTANT=raw_trades_HTB_IMPORTANT.merge(PTH, left_on='short_scenario',right_on='pth_scenario', copy=False,how='left')
raw_trades_HTB_IMPORTANT.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\HTBraw.xlsx",index=False)
raw_trades_HTB_IMPORTANT1=raw_trades_HTB_IMPORTANT[['Internal Account', 'BBG Code', 'TotalFilled', 'GS Qty', 'GS Rate bps',
        'MS Qty', 'MS Rate bps', 'UBS Qty', 'UBS Rate bps', 'BAML Qty',
        'BAML Rate bps', 'JPM Qty', 'JPM Rate bps', 'NOMU Qty', 'NOMU Rate bps',
        'isGC', 'SmartAllo','Existing_PTH_PB','largest locate', 'Needmorelocate',
        'Missallo']]
raw_trades_HTB_IMPORTANT1.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\Smart_Allo_Outputv3.xlsx",index=False)