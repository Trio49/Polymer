import openpyxl
import pandas as pd
import numpy as np
import pytz
import datetime as dt
import os
from os import listdir
from os.path import isfile, join
from datetime import datetime, timedelta

a = 2

today = datetime.now(pytz.timezone('Asia/Shanghai'))
# today=today.date()
today1 = today.date() - timedelta(days=a)
td = pd.datetime.today()
print(today1)

# =============================================================================
path = r'P://All\Enfusion\Polymer_reports\Hazeltree'

# =============================================================================
# month=dt.datetime.today().strftime('%b')
# =============================================================================
day = dt.datetime.today() - dt.timedelta(days=a)
# month =day.strftime("%m")
month = today1.strftime("%m")
day = day.strftime('%d')

path3 = path + "/" + "2023" + month + "/" + month + day + "/"
# path3=path+"/"+"2023/"+'3'+"/"+"3"+day+"/"
# path3=path+"/"+"2023"+"01"+"/"+"0130"+"/"

# =============================================================================
files = listdir(path3)
files = listdir(path3)
# =============================================================================
print(files)
files = [x for x in files if x.startswith('PAG_POLY FINANCING REPORT')]
file = sorted(files)[-1]

# poly_fin=pd.read_csv(r"T:\Daily trades\Daily Re\Oct20\Oct05\!Margin Summary\PAG_POLY FINANCING REPORT_20201005.csv")

poly_fin = pd.read_csv(path3 + file, skip_blank_lines=True)
pb_map = pd.read_excel('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\PB mappping.xlsx')
pb_map.columns = ['Custodian Code', 'Custodian ID']

# #filter _FX account
poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
poly_fin = poly_fin[['Custodian Code',
                     'Custodian Account',
                     'Bberg',
                     'Quantity - Internal T/D',
                     'Financing Value - Broker (Base)',
                     'ENSO Fee',
                     'Broker Fee',
                     ]]
poly_fin['BB Ticker'] = poly_fin['Bberg'] + ' Equity'

# Get usd notional
TICKER = poly_fin['BB Ticker']

Ticker_list = pd.Series.tolist(TICKER)

# enriching BBG country code

Ticker_list = list(set(Ticker_list))
import pdblp
from xbbg import blp

con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()

# px_list2=blp.bdp(Ticker_list, 'CRNCY')
pricelist = blp.bdp(Ticker_list, 'CRNCY_ADJ_PX_LAST', EQY_FUND_CRNCY='USD')
pricelist = pricelist.reset_index()
poly_fin = poly_fin.merge(pricelist, left_on='BB Ticker', right_on='index', how='left')
poly_fin['Financing Value - Broker (USD)'] = poly_fin['Quantity - Internal T/D'] * poly_fin['crncy_adj_px_last']

poly_fin['ENSO Fee'] = poly_fin['ENSO Fee'].fillna(0)
poly_fin['Higher than benchmark'] = poly_fin['ENSO Fee'] - poly_fin['Broker Fee']
poly_fin['PM'] = poly_fin['Custodian Account'].str[:10]
poly_fin['Market'] = poly_fin['Bberg'].str[-2:]
poly_fin['Daily Accrual'] = poly_fin['Financing Value - Broker (USD)'] * poly_fin['Broker Fee'] / 365

# poly_fin['Custodians']=poly_fin.groupby('Bberg')['Custodian Code'].transform(lambda x:'|'.join(set(x)))

# df['newCol'] = df.groupby('BB Ticker')['Custodian Code'].transform(lambda x: '|'.join(set(x))).reset_index()
# consolidate custodians
df2 = poly_fin.groupby(['BB Ticker', 'Market']).agg({'Custodian Code':lambda x:' | '.join(set(x))}).reset_index()
df2['HoldingCustodians'] = df2['Custodian Code']
df2 = df2[['BB Ticker',
           'HoldingCustodians',
           'Market']]

hazeltreedata = poly_fin.merge(df2, left_on='BB Ticker', right_on='BB Ticker', copy=False, how='left')
hazeltreedata['Q*Fee'] = hazeltreedata['Quantity - Internal T/D'] * hazeltreedata['Broker Fee']
hazeltreedata1 = hazeltreedata.groupby(['BB Ticker', 'Bberg', 'Custodian Code']).agg(
    lambda x:x.sum() if x.dtype == 'float64' else ' '.join(x)).reset_index()
hazeltreedata1['Weighted Broker Fee'] = hazeltreedata1['Q*Fee'] / hazeltreedata1['Quantity - Internal T/D'] * -1
hazeltreedata1 = hazeltreedata1.drop(columns=['HoldingCustodians'])
hazeltreedata1 = hazeltreedata1.drop(columns=['Market_x'])
hazeltreedata1 = hazeltreedata1.drop(columns=['Market_y'])
hazeltreedata1 = hazeltreedata1.merge(df2, left_on='BB Ticker', right_on='BB Ticker', copy=False, how='left')

#                               margins_name='Total'
#                               ))
# hazeltreedata1.to_excel(r"P:\Operations\Polymer - Middle Office\Borrow analysis\hazeltreedata.xlsx", index=False,
#                       line_terminator='\n')
hazeltreedata.to_excel(
    r'P:\Operations\Polymer - Middle Office\Borrow analysis\Dailyrates/' + today1.strftime('%Y%m%d') + 'rates.xlsx',
    sheet_name='Rates', index=False)

# generate quantity weighted broker fee for each ticker based on haelzeltreedata1

htdata_firm = hazeltreedata1
# remove the columns that are not needed
htdata_firm = htdata_firm[['BB Ticker', 'Custodian Code', 'Quantity - Internal T/D', 'Broker Fee', 'Financing Value - Broker (USD)','HoldingCustodians']]
# flip the broker fee to absolute value
htdata_firm['Broker Fee'] = htdata_firm['Broker Fee'].abs()
htdata_firm['Q*Fee'] = hazeltreedata1['Quantity - Internal T/D'] * hazeltreedata1['Broker Fee']
# group the data by BB Ticker
htdata_firm = htdata_firm.groupby(['BB Ticker']).agg(
    lambda x:x.sum() if x.dtype == 'float64' else ' '.join(x)).reset_index()
htdata_firm['Weighted Broker Fee'] = htdata_firm['Q*Fee'] / htdata_firm['Quantity - Internal T/D'] * -1
# extract the top 30 highest weighted broker fee ticker
htdata_firm_top30borrow = htdata_firm.sort_values(by=['Weighted Broker Fee'], ascending=False).head(30)
# save htdata_firm_top30borrow and hazeltreedata1 to an excel under different tabs
writer = pd.ExcelWriter(r'P:\Operations\Polymer - Middle Office\Borrow analysis/hazeltree_dataanalysis_apr.xlsx',
                        engine='xlsxwriter')
htdata_firm_top30borrow.to_excel(writer, sheet_name='Top30Borrow', index=False)
hazeltreedata1.to_excel(writer, sheet_name='AllBorrow', index=False)

writer.save()
writer.close()
