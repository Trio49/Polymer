import openpyxl
import pandas as pd
import numpy as np
import pytz
import datetime as dt
import os
from os import listdir
from os.path import isfile,join
from datetime import datetime, timedelta
from pandas.tseries.offsets import BDay
from openpyxl import load_workbook

import pandas.io.common


#define time

today=datetime.now(pytz.timezone('Asia/Shanghai'))
today=today.date()
# today=today.date()-timedelta(days=2)
date=dt.datetime.today().strftime('%b%m%yyyy')
# day = dt.datetime.today() - dt.timedelta(days=5)
# day = day.strftime('%d')

#define path
path=r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Tradingdata\dailystats2.xlsx'
book=load_workbook(path)
writer= pd.ExcelWriter(path,engine='openpyxl')
writer.book=book
#reading CSV
tradedata=pd.read_excel(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Tradingdata\Data Loader1.xlsx')

#remove Nan trader name
# =============================================================================
# tradedata=tradedata[tradedata['OutSourceTraderID'].notna()]
# =============================================================================
# =============================================================================
tradedata=tradedata[tradedata['Notional Quantity'].notna()]
# =============================================================================
tradedata['OutSourceTraderID']=tradedata['OutSourceTraderID'].replace(['fixgwuser','jho2','bpjepsen','jzonenshine','mmazor','mzonenshine','dsamuell1','atong','jevans','gmclaughlin'],'Tora')
tradedata['OutSourceTraderID']=tradedata['OutSourceTraderID'].fillna('Manual & Quant')
# if tradedata.loc['FundShortName']==['Polymer - AW_ALPHA'|'Polymer - xTRADE']:

#  tradedata['OutSourceTraderID']=np.where(tradedata['FundShortName']=='Polymer - AW_ALPHA','Quant trade','')
#  tradedata['OutSourceTraderID']=np.where(tradedata['FundShortName']=='Polymer - xTRADE','Quant trade','')
# tradedata['OutSourceTraderID']=tradedata['OutSourceTraderID'].add(tradedata['OutSourceTraderID1'])
#Adding Custom Column test if value greater than 1mil
tradedata['1M Test'] = np.where(abs(tradedata['PAG Delta adjusted Option Notional '])>1000000,'>1m',"<1m")
#copy the original transaction type for GMV calculation, replacing Buytocover to Buy for notional
tradedata['Transaction Type2']=tradedata['Transaction Type']
tradedata['Transaction Type']=tradedata['Transaction Type'].replace(['BuyToCover'],'Buy')
#Name list of Traders
Traders=tradedata['OutSourceTraderID'].unique()
#Name list of trade types
Tradetypes=tradedata['Transaction Type'].unique()
#Name list of PB
custodians=tradedata['CustodianShortName'].unique()
#Name list of Exchange Country Code
Country=tradedata['Exchange Country Code'].unique()
#differentiate negative notional for short and long sell
# =============================================================================
tradedata['PAG Delta adjusted Option Notional 2']=np.where(tradedata['Transaction Type']=='Buy',
                                                            tradedata['PAG Delta adjusted Option Notional ']*1,
                                                            tradedata['PAG Delta adjusted Option Notional ']*-1)
#round the notional to the nearest mil
# =============================================================================
tradedata['PAG Delta adjusted Option Notional ']=abs(tradedata['PAG Delta adjusted Option Notional ']/1000000)
tradedata['PAG Delta adjusted Option Notional 2']=tradedata['PAG Delta adjusted Option Notional 2']/1000000
tradedata['ADV USD']=tradedata['ADV USD']/1000000


# =============================================================================
# =============================================================================
#drop listed option
tradedata=tradedata.loc[tradedata['Instrument Type']!='Listed Option']
#drop internal trades
tradedata=tradedata.loc[tradedata['Counterparty Short Name']!='Internal']
tradedata=tradedata.loc[tradedata['Counterparty Short Name']!='INT']
tradedata=tradedata.loc[tradedata['Counterparty Short Name']!='REORG']
#drop reorg
#beta exposure
# =============================================================================
# tradedata['Beta adjusted Nootional']=tradedata['PAG Delta adjusted Option Notional 2']*tradedata['EQY_BETA_6M']
# =============================================================================
#mapp the trade to LT/HT,creating duplicate lines
htlt = pd.read_csv(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Tradingdata\HTLT.csv')
htlt.columns=['Counterparty Short Name','HT or LT']
tradedatahtlt=tradedata.merge(htlt,left_on='Counterparty Short Name',right_on='Counterparty Short Name',how='left')
# =============================================================================
# tradedatahtlt=tradedatahtlt.drop_duplicates(keep='first')
# =============================================================================

#cutting
def advcutting(row):
 if row['PAG Delta adjusted Option Notional ']>row['ADV USD']*0.2:
  return '>20% ADV'  
 elif row['PAG Delta adjusted Option Notional ']>row['ADV USD']*0.1:
  return '>10% ADV'
 elif row['PAG Delta adjusted Option Notional ']>row['ADV USD']*0.05:
  return '>5% ADV'
 else:
  return '<5% ADV'
tradedata['ADV Cut']=tradedata.apply(advcutting, axis=1)

# =============================================================================
#counting HTLT
# =============================================================================
# TradeTickets2=tradedata.groupby(['OutSourceTraderID','HT or LT']).size()
# =============================================================================
#Combining 2 table
# =============================================================================
# print(TradeTickets)
# =============================================================================
#save a copy for messaged data
#tradedatahtlt.to_csv(r'C:\Users\zzhang\Desktop\PYTHON\tradedata.csv',index=False,line_terminator='\n')
#tradedata.to_csv(r'C:\Users\zzhang\Desktop\PYTHON\tradedata2.csv',index=False,line_terminator='\n')
tradedata.to_csv(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\dailystats'+date+'.xlsx',index=False,line_terminator='\n')
#PIVOT table

#Trader buy,short, sell
Table1=(tradedata.pivot_table(values=['PAG Delta adjusted Option Notional '],
                             index=['OutSourceTraderID'],
                             columns=['Transaction Type'],
                             aggfunc=np.size,
                             margins=True,
                             margins_name='Total',
                             fill_value=0,
                             dropna=True)).astype(int)

# Table1=Table1.transpose()
# Table1.reset_index(drop=True,inplace=True)
Table1.columns=['Buy','Sell','SellShort','# of Trades']

# Table1=Table1.transpose()

Table1v2=(tradedata.pivot_table(values=['PAG Delta adjusted Option Notional 2'],
                             index=['OutSourceTraderID'],
                             columns=['Transaction Type'],
                             aggfunc=np.sum,
                             margins=True,
                             margins_name='Total',
                             fill_value='None',
                            ))

Table1v2b=(tradedata.pivot_table(values=['PAG Delta adjusted Option Notional '],
                             index=['OutSourceTraderID'],
                             columns=['Transaction Type'],
                             aggfunc=np.sum,
                             margins=True,
                             margins_name='Total',
                             fill_value='None',
                            ))
#by PM
Table1v3=(tradedata.pivot_table(values=['PAG Delta adjusted Option Notional 2'],
                             index=['FundShortName'],
                             columns=['Transaction Type'],
                             aggfunc=np.sum,
                             margins=True,
                             margins_name='Total',
                             fill_value='0',
                             dropna=True))
#by PM2
#by PM
Table1v4=(tradedata.pivot_table(values=['PAG Delta adjusted Option Notional '],
                             index=['FundShortName'],
                             columns=['Transaction Type2'],
                             aggfunc=np.sum,
                             margins=True,
                             fill_value='0',
                             dropna=True))
Table1v5=Table1v4.copy(deep=True)
Table1v5.columns=Table1v4.columns.droplevel(0)
Table1v5['GMV Long increase']=Table1v5['Buy'].astype(float)-Table1v5['Sell'].astype(float)
Table1v5['GMV Short increase']=Table1v5['SellShort'].astype(float)-Table1v5['BuyToCover'].astype(float)
#by custodian
Tablecustodian=(tradedata.pivot_table(values=['PAG Delta adjusted Option Notional '],
                             index=['CustodianShortName'],
                             columns=['Transaction Type2'],
                             aggfunc=np.sum,
                             margins=True,
                             fill_value='0',
                             dropna=True))
# Table1v5=Table1v4.copy(deep=True)
Tablecustodian.columns=Tablecustodian.columns.droplevel(0)
Tablecustodian['GMV Long increase']=Tablecustodian['Buy'].astype(float)-Tablecustodian['Sell'].astype(float)
Tablecustodian['GMV Short increase']=Tablecustodian['SellShort'].astype(float)-Tablecustodian['BuyToCover'].astype(float)
Tablecustodian['GMV Net Change']=Tablecustodian['GMV Long increase']+Tablecustodian['GMV Short increase']
#Trader by country
Table2=(tradedata.pivot_table(values='PAG Delta adjusted Option Notional ',
                              index=['OutSourceTraderID'],
                              columns=['Exchange Country Code'],
                              aggfunc=np.sum,
                              fill_value='n/a', 
                              margins=True,
                              margins_name='Total'
                              ))
#Trader by HT LT, notional and size and percentage
Table3=(tradedatahtlt.pivot_table(values='PAG Delta adjusted Option Notional ',
                              index=['OutSourceTraderID'],
                              columns=['HT or LT'],
                              aggfunc=np.sum,
                              fill_value='0',
                              margins=True,
                              margins_name='Total',
                              dropna=True))
Table3['HT%']=Table3['HT'].astype(float)/Table3['Total'].astype(float)
Table3['LT%']=Table3['LT'].astype(float)/Table3['Total'].astype(float)
Table3=Table3[['HT%','LT%']]
#Trade by 1mil bench mark
Table5=(tradedata.pivot_table(values='PAG Delta adjusted Option Notional ',
                              index=['OutSourceTraderID'],
                              columns=['1M Test'],
                              aggfunc=np.sum,
                              fill_value='0',
                              margins=True,
                              margins_name='Total',
                              dropna=True))
Table5['>1m Percentage']=Table5['>1m'].astype(float)/Table5['Total']
#to only include the percentage
Table5=Table5['>1m Percentage']
#Table by ADV
Table6=(tradedata.pivot_table(values='PAG Delta adjusted Option Notional ',
                              index=['OutSourceTraderID'],
                              columns=['ADV Cut'],
                              aggfunc=np.sum,
                              fill_value='0',
                              margins=True,
                              margins_name='Total',
                              dropna=True))

Table6['>5% ADV%']=Table6['>5% ADV'].astype(float)/Table6['Total'].astype(float)
Table6['>10% ADV%']=Table6['>10% ADV'].astype(float)/Table6['Total']
Table6['>20% ADV%']=Table6['>20% ADV'].astype(float)/Table6['Total']
#only include percentage
Table6=Table6[['>5% ADV%','>10% ADV%','>20% ADV%']]
#Consolidated Tables
TableX=Table1.merge(Table1v2,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
TableX=TableX.merge(Table1v2b,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
# =============================================================================
# TableX=TableX.merge(Table2,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
# =============================================================================
TableX=TableX.merge(Table2,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
TableX=TableX.merge(Table3,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
TableX=TableX.merge(Table5,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
TableX=TableX.merge(Table6,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
print (TableX.shape)
#temperary fix
# TableX.columns=['Buy','Sell','SellShort','Total','Notional Buy','Notional Sell','Notional SellShort','Notional SellShort',
#                                                                 'AU',
#                                                                 'CN',
#                                                                 'HK',
#                                                                 'ID',
#                                                                 'IN',
#                                                                 'JP',
#                                                                 'KR',
#                                                                 'PH',
#                                                                 'SG',
#                                                                 'TH',
#                                                                 'TW',
#                                                                 'US',
#                                                             'Total',
#                                                               'HT%',
#                                                               'LT%',
#                                                     '>1m Percentage',
#                                                           '>5% ADV%',
#                                                         '>10% ADV%',
#                                                         '>20% ADV%']
TableX=TableX.transpose()




# =============================================================================

# =============================================================================
# TradeTickets.to_excel(writer,sheet_name='Ticketscount')
# =============================================================================
# =============================================================================
#save the tables to Excel
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
TableX.to_excel(writer,sheet_name='Summary')
Table1v3.to_excel(writer,sheet_name='PMTradervolumnNet')
Table1v5.to_excel(writer,sheet_name='PM GMV change')
Table1.to_excel(writer,sheet_name='Tradervolumn',header=True)
Table1v2.to_excel(writer,sheet_name='TraderNotionalNet')
Table1v2b.to_excel(writer,sheet_name='TraderGrossNotionalNet')
Table2.to_excel(writer,sheet_name='Bycountryvolumn')
Table3.to_excel(writer,sheet_name='HTLTvolumn')
Table5.to_excel(writer,sheet_name='1m threshhold')
Table6.to_excel(writer,sheet_name='ADV Cut')
Tablecustodian.to_excel(writer,sheet_name='Custodian GMV change')
# =============================================================================



writer.save()
writer.close()

# import win32com.client as win32
#         outlook = win32.Dispatch('outlook.application')
#         mail = outlook.CreateItem(0)
#         mail.To = 'zzhang@pag.com'
#         # mail.To = PM_enail_mapping[PM]
#         # mail.CC = PM_enail_mapping_CC_list
#         sub = "Daily Trade Stats " + today
        
        
#         mail.Subject = sub
#         mail.Body = 'Message body testing'
        
#         #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
#         HtmlFile = open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\test file1.htm', 'r')
#         source_code = HtmlFile.read() 
#         HtmlFile.close()
#         #html=wb.to_html(classes='table table-striped')
#         #html=firm_positions.to_html(classes='table table-striped')
#         mail.HTMLBody= source_code
#         mail.Send()
