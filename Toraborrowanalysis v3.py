# -*- coding: utf-8 -*-
"""
Created on Tue Mar 22 10:49:14 2022

@author: zzhang
"""


import os
import glob
import pandas as pd
combine = input('combine?yes/no: ')
if combine=="yes":
    os.chdir(r"T:\Daily trades\Daily Re\SmartAllo\Inputdata")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    #combine all files in the list
    all_filenames=[a for a in all_filenames if 'POLYMER_BORROW_1815' in a]
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
    #export to csv
    # combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')
    combined_csv.to_csv( "combined_csv.csv", index=False)


import pytz

import numpy as np
#import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from openpyxl import load_workbook


today=datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 
# today=today.date()-timedelta(days=1)

today=today.date()
today=today.strftime('%Y%m%d')

# df=pd.read_csv(r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\combined_csv.csv",engine='python')
df=pd.read_csv(r"T:\Daily trades\Daily Re\SmartAllo\Inputdata\combined_csv.csv",engine='python')
# df=pd.read_csv(r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\POLYMER_BORROW_1615_20220204.csv",engine='python')
Locates=df[['BloombergCode','State','RequestOty','ApprovedQty','Rate','Broker','RecDate','ExecTimestamp','InternalAccount']]  

Locates['BloombergCode2']=Locates['BloombergCode']
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('AT Equity','AU EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('C1 Equity','CH EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('C2 Equity','CH EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('CG Equity','CH EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('CS Equity','CH EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('KP Equity','KS EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('KQ Equity','KS EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('UF Equity','US EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('UN Equity','US EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('UP Equity','US EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('UQ Equity','US EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('UV Equity','US EQUITY')
Locates['BloombergCode2'] = Locates['BloombergCode2'].str.replace('UW Equity','US EQUITY') 

#massage date format
DATA_FORMAT = '%Y%m%d'
Locates['RecDate'] = pd.to_datetime(
            Locates['RecDate'],
            errors='coerce'
        ).apply(lambda x: x.strftime(DATA_FORMAT))     
Locates.to_csv(r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\locatesraw.csv",index=False,line_terminator='\n')
import pdblp
from xbbg import blp
con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()


TICKER=Locates["BloombergCode2"]
Dates=Locates["RecDate"]
Ticker_list=pd.Series.tolist(TICKER)
Ticker_list2=pd.Series.tolist(Dates)
#enriching BBG country code

Ticker_list= list(set(Ticker_list))

px_list=blp.bdp(Ticker_list, 'EXCH_CODE')
px_list2=blp.bdp(Ticker_list, 'CRNCY')
#load all historical dates
px_list3=blp.bdh(Ticker_list, 'PX_LAST', Dates.min(), Dates.max(),Currency='USD')

px_list3.columns = [i[0] for i in list(px_list3.columns)]



px_list3 = px_list3.unstack(level=0).reset_index()

px_list3.columns = ['BloombergCode', 'RecDate', 'HistPrice']
px_list3['RecDate'] = pd.to_datetime(
            px_list3['RecDate'],
            errors='coerce'
        ).apply(lambda x: x.strftime(DATA_FORMAT))


# px_list=px_list

px_list=px_list.reset_index()
px_list2=px_list2.reset_index()
# px_list3=px_list3.reset_index()
px_list.rename(columns={px_list.columns[1]:"EXCH_CODE"},inplace=True)  
px_list2.rename(columns={px_list2.columns[1]:"Currency"},inplace=True)  

Locates2=Locates.merge(px_list, left_on='BloombergCode2', right_on='index',how='left')  
Locates3=Locates2.merge(px_list2, left_on='BloombergCode2', right_on='index',how='left')  
Locates4=Locates3.merge(px_list3, left_on=['BloombergCode2', 'RecDate'], right_on=['BloombergCode','RecDate'], how='left')
#data massage
# df[df['A'].isin([3, 6])
Locates4=Locates4[Locates4['Broker'].isin(['gs','baml','jpm','msdw','ubs','nomu','cicc'])]
Locates4['MV']=Locates4['ApprovedQty']*Locates4['HistPrice']
Locates4['BloombergCode']=Locates4['BloombergCode_x']
Locatesdata=Locates4[['BloombergCode','BloombergCode2','State','RequestOty','ApprovedQty','Rate','Broker','RecDate','ExecTimestamp','InternalAccount','EXCH_CODE','Currency','HistPrice','MV']] 
#create a new column to unifiy the ticker

Country=Locates4['EXCH_CODE'].unique()
print(Country)
#datetime conversion
Locatesdata['RecDate'] = pd.to_datetime(
    Locates['RecDate'].str[:10],
    format='%Y-%m-%d',
)
# Locatesdata['ExecTimestamp'] = pd.to_datetime(
#     Locatesdata['ExecTimestamp'].str[:10],
#     format='%Y-%m-%d',
# )
                            
#filter out the trade with rejection or partial fill
Cantfill=Locates4.loc[Locates2['State']=='Rejected']
Cantfill=Cantfill.sort_values(by=['RecDate','ExecTimestamp','BloombergCode2','EXCH_CODE'])
# Cantfillgroup=Cantfill.groupby(['RecDate','ExecTimestamp','BloombergCode','Country']).count()

Cantfillcount=Cantfill.groupby(['RecDate','EXCH_CODE','Broker'])['BloombergCode2'].count()
Cantfillcount=Cantfillcount.reset_index()

#saving
#define path
# path=r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\toraborrowanalyze.xlsx"
# book=load_workbook(path)
# writer= pd.ExcelWriter(path,engine='openpyxl')
# writer.book=book

Locatesdata.to_csv(r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\Toradata.csv",index=False,line_terminator='\n')
Cantfill.to_csv(r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\nofill.csv",index=False,line_terminator='\n')
Cantfillcount.to_csv(r"T:\Daily trades\Daily Re\SmartAllo\Borrowinput\nofillcount.csv",index=False,line_terminator='\n')