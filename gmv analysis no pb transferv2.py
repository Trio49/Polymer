# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 15:02:02 2023

@author: zzhang
"""


import pytz
import pandas as pd
pd.options.display.float_format = '{:,}'.format
import numpy as np
import openpyxl
import matplotlib.pyplot as plt
#import matplotlib.pyplot as plt 
from datetime import datetime, timedelta,date
from os import listdir
from openpyxl import load_workbook
import win32com.client as win32
from IPython.display import HTML

YTD_trades=pd.read_csv(r'P:\Operations\Polymer - Middle Office\GMV analysis\trades-fundlevel.csv',engine='python',thousands=',')
YTD_trades=YTD_trades.loc[YTD_trades['counterpartyShortName']!='Internal']
YTD_trades=YTD_trades.loc[YTD_trades['counterpartyShortName']!='INT']
YTD_trades=YTD_trades.loc[YTD_trades['counterpartyShortName']!='REORG']
YTD_trades=YTD_trades.loc[YTD_trades['counterpartyShortName']!='Adjustment']
YTD_trades=YTD_trades.loc[YTD_trades['counterpartyShortName']!='CROSS']
YTD_trades=YTD_trades.loc[YTD_trades['counterpartyShortName']!='REORG-VCA']
YTD_trades=YTD_trades.loc[YTD_trades['custodian']!='(INTERNAL) FX Treasury']
check = input('Including PB transfer? yes/no: ')
if check=='no':
 YTD_trades=YTD_trades[YTD_trades['counterpartyShortName'].str.contains("CROSS") == False]
YTD_trades_PB=YTD_trades[['counterpartyShortName','tradeDate','transactionType','custodian', 'deltaUsd']]
YTD_trades_PB['tradeDate']=pd.to_datetime(YTD_trades_PB['tradeDate'])
YTD_trades_PB['deltaUsd']=YTD_trades_PB['deltaUsd'].abs()/1000000
#SAMPLE
# YTD_trades_PB_100=YTD_trades.head(100)
YTD_trades_PB['GMV_Change'] = np.where(YTD_trades_PB['transactionType'].isin(['Buy',"Sell Short"]) ,'Upsize',"Downsize")
GMV_change_ytd=(YTD_trades_PB.pivot_table(values=['deltaUsd'],
                             index=['tradeDate','custodian'],
                             columns=['GMV_Change'],
                             aggfunc=np.sum,
                             margins=False,
                             fill_value='0',
                             dropna=True))
# Table1v5=Table1v4.copy(deep=True)
GMV_change_ytd.columns=GMV_change_ytd.columns.droplevel(0)
GMV_change_ytd=GMV_change_ytd.reset_index()
GMV_change_ytd['tradeDate'] = GMV_change_ytd['tradeDate'].dt.strftime('%m/%d/%Y')
GMV_change_ytd=GMV_change_ytd.sort_values(by=['tradeDate','custodian'])
GMV_change_ytd['Downsize']=GMV_change_ytd['Downsize'].astype(float)*-1
GMV_change_ytd['Upsize']=GMV_change_ytd['Upsize'].astype(float)
GMV_change_ytd['Net change']=GMV_change_ytd['Upsize']+GMV_change_ytd['Downsize']
GMV_change_ytd1=GMV_change_ytd[['custodian', 'Downsize', 'Upsize','Net change']]
GMV_change_ytd_net=GMV_change_ytd[['tradeDate','custodian', 'Net change']]
# GMV_change_ytd_net['Cumsum']=GMV_change_ytd['Net change'].cumsum()
GMV_change_ytd_max=GMV_change_ytd1.groupby('custodian').agg('max')
GMV_change_ytd_min=GMV_change_ytd1.groupby('custodian').agg('min')
GMV_change_ytd_avg=GMV_change_ytd1.groupby('custodian').agg('mean')
GMV_change_ytd_med=GMV_change_ytd1.groupby('custodian').agg('median')


path=r'P:\Operations\Polymer - Middle Office\GMV analysis\GMVmove2022_nopbtransferv2.xlsx'
book=load_workbook(path)
writer= pd.ExcelWriter(path,engine='openpyxl')
writer.book=book

writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
GMV_change_ytd.to_excel(writer,sheet_name='YTD')
GMV_change_ytd_max.to_excel(writer,sheet_name='MAX')
GMV_change_ytd_min.to_excel(writer,sheet_name='MIN')
GMV_change_ytd_avg.to_excel(writer,sheet_name='MEAN')
GMV_change_ytd_med.to_excel(writer,sheet_name='MEDIAN')



writer.save()
writer.close()

check = input('Plot? yes/no: ')
pblist=GMV_change_ytd_net['custodian'].unique()
if check=='yes':
  from matplotlib.backends.backend_pdf import PdfPages
  for x in pblist:
    # with PdfPages(r'P:\Operations\Polymer - Middle Office\GMV analysis\PBgmvchanges.pdf') as pdf:
        GMV_change_ytd_BAML=GMV_change_ytd_net.loc[GMV_change_ytd_net['custodian']==x].drop('custodian', axis=1).set_index('tradeDate')
        GMV_change_ytd_BAML.sort_index(axis=0, inplace=True)
        GMV_change_ytd_BAML['Cumsum']=GMV_change_ytd_BAML['Net change'].cumsum()
        # GMV_change_ytd_BAML.cumsum(axis=1).plot(kind='line',title=x)
        GMV_change_ytd_BAML.plot(kind='line',title=x)
        plt.savefig(r"P:\Operations\Polymer - Middle Office\GMV analysis\GMV"+x+".pdf",format='pdf')
        plt.close()
    