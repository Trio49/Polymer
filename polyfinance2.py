import openpyxl
import pandas as pd
import numpy as np
import pytz
import datetime as dt
import os
from os import listdir
from os.path import isfile,join
from datetime import datetime, timedelta


today=datetime.now(pytz.timezone('Asia/Shanghai'))
#today=today.date()
#define backdate

a=5
today1=today.date()-timedelta(days=a)

#define the directory
path = r'P://All\Enfusion\Polymer_reports\Hazeltree'
month=dt.datetime.today().strftime('%m')
# =============================================================================
# month=dt.datetime.today().strftime('%b')
# =============================================================================
day = dt.datetime.today() - dt.timedelta(days=a)
day = day.strftime('%d')
# path3 = path+"Dec"+"20/"+"Dec"+day+"/"+"!Margin Summary/"
# path3 = path+month+"22/"+month+day+"/"+"!Margin Summary/"
path3=path+"/"+"2023"+month+"/"+month+day+"/"
# =============================================================================
files = listdir(path3)
# =============================================================================
print(files)
files = [x for x in files if x.startswith('PAG_POLY FINANCING REPORT')]
file = sorted(files)[-1]



#poly_fin=pd.read_csv(r"T:\Daily trades\Daily Re\Oct20\Oct05\!Margin Summary\PAG_POLY FINANCING REPORT_20201005.csv")

poly_fin = pd.read_csv(path3+file,skip_blank_lines=True)
pb_map = pd.read_excel(r'T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\PB mappping.xlsx')
pb_map.columns = ['Custodian Code', 'Custodian ID']


#filter _FX account
poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
#mapp Custodian code
poly_fin = poly_fin.merge(pb_map, how='outer', left_on='Custodian Code', right_on='Custodian Code')
poly_fin['Date']=today1
#Exacting ISIN
poly_fin['type']='ISIN'
#amend Bberg
poly_fin['Bberg1']=poly_fin['Bberg']+' Equity'
poly_fin['AssetMeasure']='MarketPrice'
poly_fin['Quoteset']='PAG Default(SSnC)'
poly_fin['Quotesource']='Internal'
#massage the data, 1.fill 0 to blank 2, remove %, 3 adjust the type of fee to float
poly_fin['Broker Fee'] = poly_fin['Broker Fee'].fillna(0)
poly_fin['Broker Fee'] = poly_fin['Broker Fee'].replace('%', '')
poly_fin['Broker Fee'] = poly_fin['Broker Fee'].astype(np.float64) 
#filterout the broker to 0.4%
poly_fin = poly_fin.loc[poly_fin['Broker Fee']<-0.004]
pbs = poly_fin['Custodian Code'].unique()
#Summary for Swap upload

sub_summary = poly_fin[['Bberg','type','Bberg1','Custodian Account','Position Type Code','ISIN','Broker Fee','Custodian ID','Date','AssetMeasure','Quoteset','Quotesource']]
sub_summary_equity=' Equity'
# sub_summary[['Bberg']]=poly_fin['Bberg']+

# =============================================================================
# sub_summary_isda=sub_summary.loc[poly_fin['Custodian Account'].str.endswith('_ISDA')].copy()
sub_summary_isda=sub_summary.loc[poly_fin['Custodian Account'].str.endswith('_ISDA')]
sub_summary_isda=sub_summary_isda.sort_values(by=['ISIN','Custodian ID','Broker Fee']).drop_duplicates(['ISIN','Custodian ID']).sort_index()
sub_summary_isda = sub_summary_isda[['ISIN','Bberg1','type','Custodian ID','Broker Fee','Broker Fee','Broker Fee','AssetMeasure','Date','Quoteset','Quotesource']]
sub_summary_isda['Broker Fee'] = sub_summary_isda['Broker Fee']*10000
# =============================================================================
sub_summary_isda.to_csv('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\Output/'+'UploadSummary'+month+day+'_ISDA.csv', index=False,line_terminator='\n')
#Summary for Cash upload
# sub_summary_cash=sub_summary.loc[poly_fin['Custodian Account'].str.endswith('_PB')].copy()
sub_summary_cash=sub_summary.loc[poly_fin['Custodian Account'].str.endswith('_PB')]
sub_summary_cash = sub_summary_cash.loc[~sub_summary_cash['Position Type Code'].str.endswith('PRE')]
#sort by ISIN then Custdodian ID, remove the duplicates keeping the highest value
sub_summary_cash=sub_summary_cash.sort_values(by=['ISIN','Custodian ID','Broker Fee']).drop_duplicates(['ISIN','Custodian ID']).sort_index()
sub_summary_cash = sub_summary_cash[['ISIN','type','Custodian ID','Broker Fee','Broker Fee','Broker Fee','AssetMeasure','Date','Quoteset','Quotesource']]
sub_summary_cash['Broker Fee']=sub_summary_cash['Broker Fee']*-1
# =============================================================================
# sub_summary_cash['Broker Fee'] = sub_summary_cash['Broker Fee']
# =============================================================================
sub_summary_cash.to_csv('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\Output/'+'UploadSummary'+month+day+'_Cash.csv', index=False,line_terminator='\n')



# #for each PB
# for pb in pbs:
#     sub_fin = poly_fin.loc[poly_fin['Custodian Code'] == pb].copy()
#     sub_fin_isda = sub_fin.loc[sub_fin['Custodian Account'].str.endswith('_ISDA')].copy()
#     sub_fin_cash = sub_fin.loc[~sub_fin['Custodian Account'].str.endswith('_ISDA')].copy()
#
#
#     # sub_fin_cash['Account ID'] = pb_map.loc[pb_map.PB == pb, 'ID'].iloc[0]
#     sub_fin_isda.to_csv('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Python\Output/Per PB and PM/'+pb+'_ISDA.csv', index=False,line_terminator='\n')
#     sub_fin_cash.to_csv('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Python\Output/Per PB and PM/'+pb+'_CASH.csv', index=False,line_terminator='\n')


