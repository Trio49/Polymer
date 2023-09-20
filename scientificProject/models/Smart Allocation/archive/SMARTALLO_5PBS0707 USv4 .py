
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  3 14:19:24 2020

@author: zzhang

SMART ALLOCATION MODEL
"""



import numpy as np
import pandas as pd
import pytz
#import jsonSMART
#import pprint
#import requests
from datetime import datetime, timedelta
from os import listdir
pd.set_option('mode.chained_assignment', None)

#define d to find right file 
d=2
#Define IDK to filter out t-4's overnight order
idk= input('Today is Monday? yes or no ')
if idk=='yes':
    idk=4
else:
     idk=2


#T:\Daily trades\Daily Re\SmartAllo
today=datetime.now(pytz.timezone('Asia/Shanghai'))
#locate the trade and borrow file
today=today.date()
today=today.strftime('%yesY%m%d')
pbd = datetime.today() - timedelta(days=d)
pbd1 =datetime.today() - timedelta(days=idk)
#convert pbd into integer
pbd = int(pbd.strftime('%Y%m%d'))
pbd1 = int(pbd1.strftime('%Y%m%d'))
#tora file path
Tora_file_path=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\Polymer_reports\Tora/"
Tora_file_path_list=listdir(Tora_file_path)
Tora_file_path_list1 = Tora_file_path_list[::-1]

#trade file 1 is 930am batch, file 2 is 1815pm batch
combine = 'yes'
if combine =='yes':
    for file_name in Tora_file_path_list1:   
        if "POLYMER_EOD_930" in file_name:
            print(file_name)
            tradefile1=Tora_file_path+file_name
            break
    for file_name in Tora_file_path_list1:   
        if "POLYMER_EOD_1815" in file_name:
            print(file_name)
            tradefile2=Tora_file_path+file_name
            break
    #borrow file 1 is 930am batch, file 2 is 1815pm batch
    for file_name in Tora_file_path_list1:   
        if "POLYMER_BORROW_930" in file_name:
            print(file_name)
            borrowfile1=Tora_file_path+file_name
            break
    for file_name in Tora_file_path_list1:   
        if "POLYMER_BORROW_1815" in file_name:
            print(file_name)
            borrowfile2=Tora_file_path+file_name
            break
    #combine tradefile and 
    a = pd.read_csv(tradefile1)
    b = pd.read_csv(tradefile2)
    c = pd.read_csv(borrowfile1)
    d = pd.read_csv(borrowfile2)
    # merged = a.append(b,ignore_index=True)
    # concatenate a and b
    merged = pd.concat([a,b],ignore_index=True)
    # merged2 = c.append(d,ignore_index=True)
    # concatenate c and d
    merged2 = pd.concat([c,d],ignore_index=True)
    #save file
    # print(a,b,c,d)
    #ask if the file is correct
    merge_file=input('check if the file is correct, press 1 to continue')
    if merge_file=='1':
        merged.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocationUStrade.xlsx", sheet_name="trades")
        merged2.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocationUSborrow.xlsx", sheet_name="borrows")

#loading
raw_trades= pd.read_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocationUStrade.xlsx",sheet_name="trades")
raw_trades=raw_trades[raw_trades["Side"]=="SHORT"]
raw_trades=raw_trades[raw_trades["CCY"].isin(["CHF","EUR","USD",'CAD'])]
# raw_trades=raw_trades[raw_trades["SynthType"]!="swp"]
raw_trades["new_ticker_id"]= raw_trades["Code"].apply(lambda x:str(x))+raw_trades["CCY"]
raw_trades["new_ticker_id"]=raw_trades["new_ticker_id"].str[4:]
#filter out t-2 trade
raw_trades=raw_trades[raw_trades["Business Trade Date"]!=pbd1]
raw_trades=raw_trades[["Internal Account","BBG Code","new_ticker_id","Execution Quantity","Notional - Local"]]
raw_trades=raw_trades[raw_trades["Execution Quantity"]!=0]

raw_trades=raw_trades.groupby(["Internal Account","BBG Code","new_ticker_id"])["Execution Quantity","Notional - Local"].sum()
raw_trades.reset_index(inplace=True)
#align US ticker names
raw_trades['BBG Code'] = raw_trades['BBG Code'].str.replace('UF Equity','US Equity')
raw_trades['BBG Code'] = raw_trades['BBG Code'].str.replace('UN Equity','US Equity')
raw_trades['BBG Code'] = raw_trades['BBG Code'].str.replace('UP Equity','US Equity')
raw_trades['BBG Code'] = raw_trades['BBG Code'].str.replace('UQ Equity','US Equity')
raw_trades['BBG Code'] = raw_trades['BBG Code'].str.replace('UV Equity','US Equity')
raw_trades['BBG Code'] = raw_trades['BBG Code'].str.replace('UW Equity','US Equity') 
#remove SP trade
raw_trades = raw_trades[~raw_trades['BBG Code'].str.contains('SP Equity') ]
# Trade_Blotter_Data_allPM=Trade_Blotter_Data_allPM[~Trade_Blotter_Data_allPM["Strategy"].str.contains("PTH")]

borrows= pd.read_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocationUSborrow.xlsx",sheet_name="borrows")
# borrows['Code'].fillna(0,inplace=True)
# # borrows['Code']= borrows['Code'].astype(int)
# borrows['Code']= borrows['Code']
# borrows["new_ticker_id"] =  borrows["Code"].apply(lambda x:str(x))+borrows["CCY"]

#only filter out cash trades mkt: SGD JPY HKD
# borrows=borrows[borrows["CCY"].isin(["EUR","USD",'CAD'])]
borrows['Internal Account']=borrows['InternalAccount']
borrows['BBG Code']=borrows['BloombergCode']
borrows=borrows[["Internal Account","BBG Code","Broker","ApprovedQty","Rate"]]
#massage borrow ticker
borrows['BBG Code'] = borrows['BBG Code'].str.replace('UF Equity','US Equity')
borrows['BBG Code'] = borrows['BBG Code'].str.replace('UN Equity','US Equity')
borrows['BBG Code'] = borrows['BBG Code'].str.replace('UP Equity','US Equity')
borrows['BBG Code'] = borrows['BBG Code'].str.replace('UQ Equity','US Equity')
borrows['BBG Code'] = borrows['BBG Code'].str.replace('UV Equity','US Equity')
borrows['BBG Code'] = borrows['BBG Code'].str.replace('UW Equity','US Equity') 
borrows=borrows.groupby(["Internal Account","BBG Code","Broker"]).agg({"ApprovedQty":np.sum,"Rate":np.mean})
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
    raw_trades = raw_trades.merge(PBs[pb],left_on= ["Internal Account","BBG Code"],
                    right_on=["Internal Account","BBG Code"],
                    how="left")
    raw_trades = raw_trades.drop(columns=["Broker"])
    raw_trades.fillna(0,inplace=True)
    raw_trades.rename(columns={"ApprovedQty":pb+" Qty","Rate":pb+" Rate bps"},inplace=True)




raw_trades["isGC"]=True
    
for i in range(len(raw_trades)):
    x1=(raw_trades.loc[i,"GS Rate bps"]<=50) & (raw_trades.loc[i,"GS Rate bps"]>0)
    x2=(raw_trades.loc[i,"MS Rate bps"]<=50) & (raw_trades.loc[i,"MS Rate bps"]>0)
    x3=(raw_trades.loc[i,"UBS Rate bps"]<=50) & (raw_trades.loc[i,"UBS Rate bps"]>0)
    x4=(raw_trades.loc[i,"BAML Rate bps"]<=50) & (raw_trades.loc[i,"BAML Rate bps"]>0)
    x5=(raw_trades.loc[i,"JPM Rate bps"]<=50) & (raw_trades.loc[i,"JPM Rate bps"]>0)
    x6=(raw_trades.loc[i,"NOMU Rate bps"]<=50) & (raw_trades.loc[i,"NOMU Rate bps"]>0)
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
    if raw_trades_HTB.loc[i,'Execution Quantity']-raw_trades_HTB.loc[i,'largest locate']>=0:
     raw_trades_HTB.loc[i,'Needmorelocate']=raw_trades_HTB.loc[i,'Execution Quantity']-raw_trades_HTB.loc[i,'largest locate']
    else:
     raw_trades_HTB.loc[i,'Needmorelocate']=0
for i in range(len(raw_trades_HTB)):
    
    #i=0
    print(i)
    optimal_allo={}
    miss_allo={}
    
    ss_qty=raw_trades_HTB.loc[i,"Execution Quantity"]
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


raw_trades_HTB_IMPORTANT= raw_trades_HTB[(raw_trades_HTB["Cmmts"]!="Liquid_Less_Important")|(raw_trades_HTB["Needmorelocate"]>0)]

#raw_trades_HTB_IMPORTANT= raw_trades_HTB_IMPORTANT[['Internal Account','BBG Code','TotalFilled','SmartAllo']]

raw_trades_HTB_IMPORTANT.to_excel(r"T:\Daily trades\Daily Re\SmartAllo\Smart_Allo_OutputUS.xlsx",index=False)
print("done")