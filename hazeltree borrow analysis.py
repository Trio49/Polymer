import openpyxl
import pandas as pd
import numpy as np
import pytz
import datetime as dt
import os
from os import listdir
from os.path import isfile,join
from datetime import datetime, timedelta


a=2



today=datetime.now(pytz.timezone('Asia/Shanghai'))
#today=today.date()
today1=today.date()-timedelta(days=a)
td = pd.datetime.today()
print (today1)

# =============================================================================
path = r'P://All\Enfusion\Polymer_reports\Hazeltree'
month=dt.datetime.today().strftime('%m')
# =============================================================================
# month=dt.datetime.today().strftime('%b')
# =============================================================================
day = dt.datetime.today() - dt.timedelta(days=a)
day = day.strftime('%d')

# path3=path+"/"+"2023"+month+"/"+month+day+"/"
# path3=path+"/"+"2023"+'1'+"/"+month+day+"/"
path3=path+"/"+"2023"+"01"+"/"+"0130"+"/"

# =============================================================================
files = listdir(path3)
files = listdir(path3)
# =============================================================================
print(files)
files = [x for x in files if x.startswith('PAG_POLY FINANCING REPORT')]
file = sorted(files)[-1]



# poly_fin=pd.read_csv(r"T:\Daily trades\Daily Re\Oct20\Oct05\!Margin Summary\PAG_POLY FINANCING REPORT_20201005.csv")

poly_fin = pd.read_csv(path3+file,skip_blank_lines=True)
pb_map = pd.read_excel('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\PB mappping.xlsx')
pb_map.columns = ['Custodian Code', 'Custodian ID']


# #filter _FX account
poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
poly_fin=poly_fin[['Custodian Code',
                   'Custodian Account',
                  'Bberg',
                  'Quantity - Internal T/D',
                  'Financing Value - Broker (Base)',
                  'ENSO Fee',
                  'Broker Fee',                 
                  ]]
poly_fin['BB Ticker']=poly_fin['Bberg']+' Equity'

#Get usd notional
TICKER=poly_fin['BB Ticker']

Ticker_list=pd.Series.tolist(TICKER)

#enriching BBG country code

Ticker_list= list(set(Ticker_list))
import pdblp
from xbbg import blp
con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()

# px_list2=blp.bdp(Ticker_list, 'CRNCY')
pricelist=blp.bdp(Ticker_list,'CRNCY_ADJ_PX_LAST',EQY_FUND_CRNCY='USD')
pricelist=pricelist.reset_index()
poly_fin=poly_fin.merge(pricelist, left_on='BB Ticker', right_on='index',how='left') 
poly_fin['Financing Value - Broker (USD)']=poly_fin['Quantity - Internal T/D']*poly_fin['crncy_adj_px_last']

poly_fin['ENSO Fee']=poly_fin['ENSO Fee'].fillna(0)
poly_fin['Higher than benchmark']=poly_fin['ENSO Fee']-poly_fin['Broker Fee']
poly_fin['PM']=poly_fin['Custodian Account'].str[:10]
poly_fin['Market']=poly_fin['Bberg'].str[-2:]
poly_fin['Daily Accrual']=poly_fin['Financing Value - Broker (USD)']*poly_fin['Broker Fee']/365


# poly_fin['Custodians']=poly_fin.groupby('Bberg')['Custodian Code'].transform(lambda x:'|'.join(set(x)))

# df['newCol'] = df.groupby('BB Ticker')['Custodian Code'].transform(lambda x: '|'.join(set(x))).reset_index()
#consolidate custodians
df2=poly_fin.groupby(['BB Ticker','Market']).agg({'Custodian Code':lambda x : ' | '.join(set(x))}).reset_index()
df2['HoldingCustodians']=df2['Custodian Code']
df2=df2[['BB Ticker',
        'HoldingCustodians',
        'Market']]

hazeltreedata=poly_fin.merge(df2, left_on='BB Ticker',right_on='BB Ticker', copy=False,how='left')  
hazeltreedata['Q*Fee']=hazeltreedata['Quantity - Internal T/D']*hazeltreedata['Broker Fee']
hazeltreedata1=hazeltreedata.groupby(['BB Ticker','Bberg','Custodian Code']).agg(lambda x : x.sum() if x.dtype=='float64' else ' '.join(x)).reset_index()
hazeltreedata1['Weighted Broker Fee']=hazeltreedata1['Q*Fee']/hazeltreedata1['Quantity - Internal T/D']*-1
hazeltreedata1=hazeltreedata1.drop(columns=['HoldingCustodians'])
hazeltreedata1=hazeltreedata1.drop(columns=['Market_x'])
hazeltreedata1=hazeltreedata1.drop(columns=['Market_y'])
hazeltreedata1=hazeltreedata1.merge(df2, left_on='BB Ticker',right_on='BB Ticker', copy=False,how='left')  
# hazeltreedata=hazeltreedata[['Custodian Code',
#                     'Custodian Account',
#                   'Bberg',
#                   'Quantity - Internal T/D',
#                   'Financing Value - Broker (Base)',
#                   'ENSO Fee',
#                   'Broker Fee',
#                   'PM',
#                   'Market',
#                   'Monthly Accrual',
#                   'HoldingCustodians'
#                   ]]
# Notional=poly_fin.groupby(['BB Ticker']).agg('sum')
# Table=(hazeltreedata.pivot_table(values='Financing Value - Broker (Base)',
#                               index=['Bberg'],
#                               columns=['Market'],
#                               aggfunc=np.sum,
#                               fill_value='n/a', 
#                               margins=True,
#                               margins_name='Total'
#                               ))
hazeltreedata1.to_csv(r"P:\Operations\Polymer - Middle Office\Borrow analysis\hazeltreedata.csv",index=False,line_terminator='\n')
hazeltreedata.to_excel(r'T:\Daily trades\Daily Re\PTH Adjustment/Daily rates/'+today1.strftime('%Y%m%d')+'rates.xlsx',sheet_name='Rates',index=False)