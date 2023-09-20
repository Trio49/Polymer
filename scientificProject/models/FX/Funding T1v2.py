
import pytz
import pandas as pd
import numpy as np
import os
import datetime as dt
#import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from os import listdir
from openpyxl import load_workbook
from pandas.tseries.offsets import BDay

# today=datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 
today = datetime.today()
#define today1 as t-1 define today 2 as t-2
#a is the backdate delta
a=2
#t0
today1=today-BDay(a-1)
#t-1
today2=today-BDay(a)
today1=today1.strftime('%Y%m%d')
today2=today2.strftime('%Y%m%d')
t_1 = (datetime.today() - BDay(a-1)).strftime('%Y-%m-%d')
t_2 = (datetime.today() - BDay(a)).strftime('%Y-%m-%d')
#today=today.date()
month=dt.datetime.today().strftime('%b')
#load t-1 and t_2 blotter
# all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today1
all_PM_tradeblotter_file_path=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"+today1
firm_positions_list_dir=listdir(all_PM_tradeblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:
    
    
    if "PM ALL (New)" in file_name:
        print(file_name)

        all_PM_tradeblotter_file_path=all_PM_tradeblotter_file_path+"\\"+file_name
        break
print(all_PM_tradeblotter_file_path)
import sys
input_checking = input('is the file latest? yes/no:')
if input_checking=='yes':
    t1trade=pd.read_csv(all_PM_tradeblotter_file_path,engine='python')
elif input_checking=='no':
    print ('please run systemjob to generate all PM blotter')
    sys.exit()
#Prompt to skip KRW reorg trades
bypass = input('skip KRW reorganization trades? yes/no')
if bypass=='yes':
    t1trade=t1trade.loc[t1trade['Trade Type']!='Reorganization']
# all_PM_tradeblotter_file_path2=r"\\paghk.local\shares\Enfusion\Polymer\\"+today2
all_PM_tradeblotter_file_path2=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"+today2
firm_positions_list_dir2=listdir(all_PM_tradeblotter_file_path2)

firm_positions_list_dir2 = firm_positions_list_dir2[::-1]

for file_name in firm_positions_list_dir2:
    
    
    if "Trade Blotter - PM ALL (New)" in file_name:
        print(file_name)

        all_PM_tradeblotter_file_path2=all_PM_tradeblotter_file_path2+"\\"+file_name
        break


t2trade=pd.read_csv(all_PM_tradeblotter_file_path2,engine='python')

#combine
trades=[t1trade,t2trade]
tradesx=pd.concat(trades)
#filter out reorg trades for TWD 
bypass = input('skip TWD reorganization trades? yes/no')
if bypass=='yes':
    tradesx=tradesx.loc[tradesx['Trade Type']!='Reorganization']
#filter out non market trade
tradesx=tradesx.loc[tradesx['Counterparty Short Name']!='INT']
tradesx=tradesx.loc[tradesx['Counterparty Short Name']!='REORG']
tradesx=tradesx.loc[tradesx['Counterparty Short Name']!='CROSS']
#get mapping table

Bunitmapping=tradesx[['Sub Business Unit','Business Unit']]
Bunitmapping = Bunitmapping.drop_duplicates(subset=['Sub Business Unit'], keep='last')
Bunitmapping.to_csv(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv',index=False,line_terminator='\n')
#filter out the cash trades 
tradesfx = tradesx[['Business Unit','Sub Business Unit','Transaction Type','CustodianShortName','Trade Date','Settle Date','Trading Net Proceeds','Trade Currency','Settle Currency','Instrument Type',]]
#for KRW only need to use t-1 trade, name it t1 trade v2
t1tradev2 = t1trade[['Business Unit','Sub Business Unit','Transaction Type','CustodianShortName','Trade Date','Settle Date','Trading Net Proceeds','Trade Currency','Settle Currency','Instrument Type',]]

#filter  out cash trades by currency and settlement currency TWD KRW
tradesfx=tradesfx.loc[tradesfx['Trade Currency'].isin(['TWD','KRW'])]
tradesfx=tradesfx.loc[tradesfx['Settle Currency'].isin(['TWD','KRW'])]
tradesfx['Trading Net Proceeds']=tradesfx['Trading Net Proceeds'].astype(int)
#include only equity trades
tradesfx=tradesfx.loc[tradesfx['Instrument Type'].isin(['Equity'])]
t1tradev2=t1tradev2.loc[t1tradev2['Instrument Type'].isin(['Equity'])]
#pivot twd trades 

tradesfxtwd=tradesfx.loc[tradesfx['Trade Currency']=='TWD']

isempty2 = tradesfxtwd.empty
print('No TWD trades :', isempty2)

if isempty2==False:
 tradesfxtwd = tradesfxtwd.copy()
 tradesfxtwd['Trade Date'] = pd.to_datetime(tradesfxtwd['Trade Date']).dt.strftime('%Y-%m-%d')
 tradesfxtwd['Settle Date'] = pd.to_datetime(tradesfxtwd['Trade Date']).dt.strftime('%Y-%m-%d')
#define t_1 and t_2 


#t-1 twd buy
 tradesfxtwd1=tradesfxtwd[(tradesfxtwd['Trade Date']== t_1)& (tradesfxtwd['Transaction Type']=='Buy')]
#t-2 twd Sell
 tradesfxtwd2=tradesfxtwd[(tradesfxtwd['Trade Date']== t_2)& (tradesfxtwd['Transaction Type']=='Sell')]
#t-1 twd sell
 tradesfxtwd3=tradesfxtwd[(tradesfxtwd['Trade Date']== t_1)& (tradesfxtwd['Transaction Type']=='Sell')]

#generating the funding instruction
 tradesfxtwdx=[tradesfxtwd1,tradesfxtwd2]
 tradesfxtwd=pd.concat(tradesfxtwdx)
#generating the TWD delta change
 tradesfxtwdx2=[tradesfxtwd1,tradesfxtwd2,tradesfxtwd3]
 TWD_exposurechange=pd.concat(tradesfxtwdx2)
#twd funding pivot
 fundingtwd=(tradesfxtwd.pivot_table(values=['Trading Net Proceeds'],
                              index=['Business Unit'],
                              columns=['CustodianShortName'],
                              aggfunc=np.sum,
                              margins=True,
                              margins_name='Total',
                              fill_value='0',
                              dropna=True))
 funding_sub_twd=(tradesfxtwd.pivot_table(values=['Trading Net Proceeds'],
                              index=['Sub Business Unit'],
                              columns=['CustodianShortName'],
                              aggfunc=np.sum,
                              margins=True,
                              margins_name='Total',
                              fill_value='0',
                              dropna=True))
#twd exposure pivot
 TWDExposurechangepivot=(TWD_exposurechange.pivot_table(values=['Trading Net Proceeds'],
                              index=['Business Unit'],
                              columns=['CustodianShortName'],
                              aggfunc=np.sum,
                              margins=True,
                              margins_name='Total',
                              fill_value='0',
                              dropna=True))
#pivot KRW traades
tradesfxkrw=t1tradev2.loc[t1tradev2['Trade Currency']=='KRW']
isempty = tradesfxkrw.empty
print('No KRW trades :', isempty)
#only include Equity trades

if isempty==False:
#KRW equity trades
 fundingkrw=(tradesfxkrw.pivot_table(values=['Trading Net Proceeds'],
                              index=['Business Unit'],
                              columns=['CustodianShortName'],
                              aggfunc=np.sum,
                              margins=True,
                              margins_name='Total',
                              fill_value='0',
                              dropna=True))
 funding_sub_krw=(tradesfxkrw.pivot_table(values=['Trading Net Proceeds'],
                              index=['Sub Business Unit'],
                              columns=['CustodianShortName'],
                              aggfunc=np.sum,
                              margins=True,
                              margins_name='Total',
                              fill_value='0',
                              dropna=True))
#KRW Exposure change

#excel path
# write=pd.ExcelWriter('T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\TWD.xlsx')
# fundingtwd.to_excel(write,sheet_name='Funding',index=True)
 with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw.xlsx')) as writer1:
  fundingkrw.to_excel(writer1,sheet_name='Funding')
 with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw_sub.xlsx')) as writer2:
  funding_sub_krw.to_excel(writer2,sheet_name='Funding')

# pathtwd=r'fundingtwd.xlsx'
# book=load_workbook(path)
# writer= pd.ExcelWriter(pathtwd,engine='openpyxl')
# writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd.xlsx')) as writer:
  fundingtwd.to_excel(writer,sheet_name='Funding')
  TWDExposurechangepivot.to_excel(writer,sheet_name='TWDexposurechange')
  TWD_exposurechange.to_excel(writer,sheet_name='TWDtrades')
with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd_sub.xlsx')) as writer3:
  funding_sub_twd.to_excel(writer3,sheet_name='Funding')
