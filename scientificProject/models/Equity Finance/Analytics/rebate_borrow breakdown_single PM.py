# import os
import glob
# import pytz
# import datetime as dt
import os

import numpy as np
import pandas as pd

# from os import listdir
# from os.path import isfile,join
# from datetime import datetime, timedelta

# import pandas as pd

# os.chdir(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon"),combine the CSV file if i need to run YTD analysis
combine = input('ytd combine?yes/no: ')
if combine == "yes":
    # ytd folder
    os.chdir(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    # combine all files in the list
    all_filenames = [a for a in all_filenames if 'POLY_FINANCING_II_2023' in a]
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
    # export to csv
    # combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')
    combined_csv.to_csv("combineddata.csv", index=False)
    poly_fin = combined_csv
else:
    poly_fin = pd.read_csv('P:\All\FinanceTradingMO\Rebate_Borrow breakdown\POLY_FINANCING_II_202308.csv',
                           skip_blank_lines=True)

# smaller sample
# poly_fin=poly_fin.head(10000)
PM_filter = input('PM filter? yes/no: ')
if PM_filter == 'yes':
    PM_name = input('PM name: ')
    # filter poly_fin to include data where custodian account includes the PM_name
    poly_fin = poly_fin.loc[poly_fin['Custodian Account'].str.contains(PM_name)]
else:
    #break if no input

    pass

# PM filter

print(poly_fin['Date'].unique())
pb_map = pd.read_excel(r'P:\All\FinanceTradingMO\Rebate_Borrow breakdown\PB mappping.xlsx')
# pb_map.columns = ['Custodian Code', 'Custodian ID']


# #filter _FX account
# poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
poly_fin['BB Ticker'] = poly_fin['Bberg'] + ' Equity'
poly_fin['Type'] = np.where(poly_fin['Custodian Account'].str.endswith('_PB'), 'Cash', 'Swap')
# poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_PB')]
poly_fin = poly_fin[['Custodian Code',
                     'Custodian Account',
                     'BB Ticker',
                     'Quantity - Internal T/D',
                     'Broker Fee',
                     'Base Rate',
                     'Broker Rebate Rate',
                     'Date',
                     'Type'
                     ]]
# poly_fin['BB Ticker']=poly_fin['Bberg']+' Equity'

# Get BB yellow keylist
TICKER = poly_fin['BB Ticker']
# massage date-format
DATA_FORMAT = '%Y%m%d'
poly_fin['Date'] = pd.to_datetime(
    poly_fin['Date'],
    errors='coerce'
).apply(lambda x:x.strftime(DATA_FORMAT))
# poly_fin['Date'] = pd.to_datetime(poly_fin['Date'], format='%Y%m%d')
Ticker_list = pd.Series.tolist(TICKER)
# get the columns needed
poly_fin = poly_fin[['Custodian Code',
                     'Custodian Account',
                     'BB Ticker',
                     'Quantity - Internal T/D',
                     'Broker Fee',
                     'Base Rate',
                     'Broker Rebate Rate',
                     'Date',
                     'Type'
                     ]]
# enriching BBG country code


import pdblp
from xbbg import blp

con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()
# load historical price
TICKER = poly_fin["BB Ticker"]
Dates = poly_fin["Date"]
# Ticker_list=pd.Series.tolist(TICKER)
Ticker_list2 = pd.Series.tolist(Dates)
# enriching BBG country code

Ticker_list = list(set(Ticker_list))

px_list = blp.bdp(Ticker_list, 'EXCH_CODE')
px_list2 = blp.bdp(Ticker_list, 'CRNCY')
# load all historical dates
px_list3 = blp.bdh(Ticker_list, 'PX_LAST', '20230701', Dates.max(), Currency='USD')

px_list3.columns = [i[0] for i in list(px_list3.columns)]

px_list3 = px_list3.unstack(level=0).reset_index()

px_list3.columns = ['BloombergCode', 'RecDate', 'HistPrice']
px_list3['RecDate'] = pd.to_datetime(
    px_list3['RecDate'],
    errors='coerce'
).apply(lambda x:x.strftime(DATA_FORMAT))

px_list3["HistPrice"] = px_list3["HistPrice"].fillna(method="ffill")
# px_list=px_list

px_list = px_list.reset_index()
px_list2 = px_list2.reset_index()
# px_list3=px_list3.reset_index()
px_list.rename(columns={px_list.columns[1]:"EXCH_CODE"}, inplace=True)
# px_list2.rename(columns={px_list2.columns[1]:"Currency"},inplace=True)  

poly_fin = poly_fin.merge(px_list3, left_on=['BB Ticker', 'Date'], right_on=['BloombergCode', 'RecDate'], how='left')
poly_fin = poly_fin.merge(px_list, left_on='BB Ticker', right_on='index', how='left')
poly_fin = poly_fin.merge(px_list2, left_on='BB Ticker', right_on='index', how='left')

poly_fin['Custodian Account2'] = poly_fin['Custodian Account'].str.replace('AW_ALPHA_IAC', 'AWIAC', regex=True)
poly_fin['Custodian Account2'] = poly_fin['Custodian Account2'].str.replace('AW_ALPHA', 'SCALA', regex=True)
poly_fin['Custodian Account2'] = poly_fin['Custodian Account2'].str.replace('(FPI)', '', regex=True)
poly_fin['Custodian Account2'] = poly_fin['Custodian Account2'].str.replace(r"\(.*\)", '', regex=True)
poly_fin['PM'] = poly_fin['Custodian Account2'].str[5:10]
poly_fin = poly_fin[['Custodian Code',
                     'PM',
                     'Custodian Account',
                     'BB Ticker',
                     'Date',
                     'Quantity - Internal T/D',
                     'HistPrice',
                     'Broker Fee',
                     'Base Rate',
                     'Broker Rebate Rate',
                     'crncy',
                     'Type'
                     ]]
poly_fin['Q*Fee'] = poly_fin['Quantity - Internal T/D'] * poly_fin['Broker Fee']
# taking AKUNG as example
# if PMlist:
# poly_fin_AGINE=poly_fin[poly_fin['PM'].isin([('AGINE')])]
# remove empty ticker and empty broker fee row
poly_fin = poly_fin.dropna(subset=['BB Ticker', 'Quantity - Internal T/D', 'Broker Fee'])
# calculate notional
poly_fin['Notional$'] = poly_fin['Quantity - Internal T/D'].abs() * poly_fin['HistPrice']
# daily short fee
poly_fin['Short Fee Daily Accrual'] = poly_fin['Notional$'] * poly_fin['Broker Fee'] / 365
# daily short proceeds rebate
poly_fin['Short Proceeds Rebate Daily Accrual'] = poly_fin['Notional$'] * poly_fin['Base Rate'] / 365
poly_fin['Date2'] = pd.to_datetime(poly_fin['Date'], format='%Y-%m-%d')
# mark day of the week based on date
poly_fin['day_of_week'] = poly_fin['Date2'].dt.day_name()
poly_fin['Month'] = poly_fin['Date2'].dt.month_name()
# triple daily accrual for friday's number to account for the weekend
poly_fin.loc[poly_fin['day_of_week'] == 'Friday', 'Short Fee Daily Accrual'] = poly_fin['Short Fee Daily Accrual'] * 3
poly_fin.loc[poly_fin['day_of_week'] == 'Friday', 'Short Proceeds Rebate Daily Accrual'] = poly_fin[
                                                                                               'Short Proceeds Rebate Daily Accrual'] * 3
# only include account with ISDA in custodian account
# poly_fin = poly_fin[poly_fin['Custodian Account'].str.contains('ISDA')]
# save reports
# poly_fin.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\outputdata/"+PM_name+'.csv',index=False)
poly_fin = poly_fin.apply(lambda x:'{:,.0f}'.format(x) if isinstance(x, (int, float)) else x)
#check if need to filter only swap
swap_filter = input('swap filter? yes/no: ')
if swap_filter == 'yes':
    raw_swap = poly_fin[poly_fin['Type'] == 'Swap']
else:
    raw_swap = poly_fin

raw_swap = raw_swap[['Custodian Code',
                     'BB Ticker',
                     'Short Fee Daily Accrual',
                     'Short Proceeds Rebate Daily Accrual',
                     'Month',
                     'Quantity - Internal T/D',
                     'Q*Fee',
                     ]]
# group by BB Ticker
raw_swap_ticker = raw_swap.groupby(['BB Ticker']).sum()
raw_swap_ticker = raw_swap_ticker.reset_index()
raw_swap_ticker['Weighted Short Fee']=raw_swap_ticker['Q*Fee']/raw_swap_ticker['Quantity - Internal T/D']
#keep the columns needed
raw_swap_ticker = raw_swap_ticker[['BB Ticker',"Short Fee Daily Accrual","Short Proceeds Rebate Daily Accrual","Weighted Short Fee"]]
#change column names
raw_swap_ticker.rename(columns={"Short Fee Daily Accrual":"Short Fee Daily Accrual Total","Short Proceeds Rebate Daily Accrual":"Short Proceeds Rebate Daily Accrual Total"},inplace=True)
#sort values by short fee daily accrual total
raw_swap_ticker=raw_swap_ticker.sort_values(by="Short Fee Daily Accrual Total",ascending=True)
#add comma to the short fee daily accrual total and short proceeds rebate daily accrual total
#add % to the weighted short fee
raw_swap_ticker["Short Fee Daily Accrual Total"]=raw_swap_ticker["Short Fee Daily Accrual Total"].apply(lambda x:'{:,.0f}'.format(x))
raw_swap_ticker["Short Proceeds Rebate Daily Accrual Total"]=raw_swap_ticker["Short Proceeds Rebate Daily Accrual Total"].apply(lambda x:'{:,.0f}'.format(x))
raw_swap_ticker["Weighted Short Fee"]=raw_swap_ticker["Weighted Short Fee"].apply(lambda x:'{:,.2%}'.format(x))
#to save the file as csv
month=input("Please enter the month you want to save the file for:")
raw_swap_ticker=raw_swap_ticker.head(20)
raw_swap_ticker.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown/"+PM_name+month+"_swap_summary.csv",index=False)
print("The file has been saved to P:\All\FinanceTradingMO\Rebate_Borrow breakdown/"+PM_name+month+"_swap_summary.csv")






