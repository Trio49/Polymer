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

ytd = input('load ytd file?else check the directory : yes/no:  ')
if ytd == 'yes':
    poly_fin = combined_csv
else:
    poly_fin = pd.read_csv('P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\POLY_FINANCING_II_202304.csv',
                           skip_blank_lines=True)
# smaller sample
# poly_fin=poly_fin.head(10000)
# PM filter
# PMlist=('AGINE')
print(poly_fin['Date'].unique())
pb_map = pd.read_excel('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\PB mappping.xlsx')
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
px_list3 = blp.bdh(Ticker_list, 'PX_LAST', '20220101', Dates.max(), Currency='USD')

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
# grouped per PB and ticker
check = input('Analyze Variance? yes/no')
if check == 'yes':
    poly_fin_grouped_PB = poly_fin.groupby(['BB Ticker', 'Custodian Code', 'Date2']).agg(
        lambda x:x.sum() if x.dtype == 'float64' else ' '.join(x)).reset_index()
    poly_fin_grouped_PB['Weighted Broker Fee'] = poly_fin_grouped_PB['Q*Fee'] / poly_fin_grouped_PB[
        'Quantity - Internal T/D'] * -1
    poly_fin_grouped_PB = poly_fin_grouped_PB[['BB Ticker', 'Custodian Code', 'Date2', 'Weighted Broker Fee']]
    poly_fin_grouped_PB['Holding Scenario'] = poly_fin_grouped_PB['BB Ticker'].astype(str) + '__' + poly_fin_grouped_PB[
        'Custodian Code'].astype(str)
    poly_fin_grouped_PB = poly_fin_grouped_PB[['Holding Scenario', 'Date2', 'Weighted Broker Fee']]
    poly_fin_save = poly_fin_grouped_PB
    # export report?
    # calculate standard deviation of each name's rate across all dates
    poly_fin_grouped_PB['Std Dev'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Weighted Broker Fee'].transform(
        np.std)
    # calculate max value of each name's rate and the corresponding date
    poly_fin_grouped_PB['Max Value'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Weighted Broker Fee'].transform(
        max)
    # poly_fin_grouped_PB['Max Date'] = poly_fin_grouped_PB.groupby(['Holding Scenario', 'Weighted Broker Fee'])['Date2'].transform(max)
    poly_fin_grouped_PB['Max Date'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Date2'].transform(
        lambda x:x.loc[x.index == x.index.max()])

    # calculate min value of each name's rate and the corresponding date
    poly_fin_grouped_PB['Min Value'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Weighted Broker Fee'].transform(
        min)
    # poly_fin_grouped_PB['Min Date'] = poly_fin_grouped_PB.groupby(['Holding Scenario', 'Weighted Broker Fee'])['Date2'].transform(min)
    poly_fin_grouped_PB['Min Date'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Date2'].transform(
        lambda x:x.loc[x.index == x.index.min()])
    # poly_fin_grouped_PB.drop_duplicates(subset=['Std Dev'], inplace=True)
    poly_fin_grouped_PB = poly_fin_grouped_PB[poly_fin_grouped_PB['Std Dev'] != 0]
    # save max and min dates
    Max_ticker_dates = poly_fin_grouped_PB[['Holding Scenario', 'Max Date']]
    Max_ticker_dates = Max_ticker_dates.dropna(subset=['Max Date'])
    Min_ticker_dates = poly_fin_grouped_PB[['Holding Scenario', 'Min Date']]
    Min_ticker_dates = Min_ticker_dates.dropna(subset=['Min Date'])
    poly_fin_grouped_PB.drop_duplicates(subset=['Holding Scenario'], inplace=True)
    poly_fin_stddev = poly_fin_grouped_PB[['Holding Scenario', 'Max Value',
                                           'Min Value', 'Std Dev']]
    poly_fin_stddev = poly_fin_stddev.merge(Max_ticker_dates, left_on='Holding Scenario', right_on='Holding Scenario',
                                            how='left')
    poly_fin_stddev = poly_fin_stddev.merge(Min_ticker_dates, left_on='Holding Scenario', right_on='Holding Scenario',
                                            how='left')
    poly_fin_stddev.drop_duplicates(subset=['Holding Scenario'], inplace=True)
    poly_fin_top20variance = poly_fin_stddev.sort_values(by='Std Dev', ascending=False).head(30)
    poly_fin_top20variance['Max Date'] = poly_fin_top20variance['Max Date'].dt.strftime('%Y%m%d')
    poly_fin_top20variance['Min Date'] = poly_fin_top20variance['Min Date'].dt.strftime('%Y%m%d')
    # save
    poly_fin_top20variance = poly_fin_top20variance[
        ['Holding Scenario', 'Std Dev', 'Max Value', 'Max Date', 'Min Value',
         'Min Date']]
    # if ytd=='yes':
    #  poly_fin_top20variance.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\YTD_ticker_ratevariance.csv",index=False,line_terminator='\n')
    # else:
    poly_fin_top20variance.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\MTD_ticker_ratevariance.csv",
                                  index=False, line_terminator='\n')

check = input('save rates reports? ')
if check == 'yes':
    # poly_fin.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\outputdata.csv",index=False,line_terminator='\n')
    poly_fin = poly_fin.apply(lambda x:'{:,.0f}'.format(x) if isinstance(x, (int, float)) else x)
    raw_swap = poly_fin[poly_fin['Type'] == 'Swap']
    raw_cash = poly_fin[poly_fin['Type'] == 'Cash']
    raw_country_currency = poly_fin[poly_fin['Type'] == 'Swap']
    raw_country_currency = raw_country_currency[['crncy',
                                                 'Month',
                                                 'Short Fee Daily Accrual',
                                                 'Short Proceeds Rebate Daily Accrual']]
    raw_swap = raw_swap[['Custodian Code',
                         'PM',
                         'Short Fee Daily Accrual',
                         'Short Proceeds Rebate Daily Accrual',
                         'Month'
                         ]]
    raw_cash = raw_cash[['Custodian Code',
                         'PM',
                         'Month',
                         'Short Fee Daily Accrual',
                         ]]

    MTD_PB_Swap = raw_swap.groupby(['Custodian Code', raw_swap['Month']]).sum()
    MTD_PM_Swap = raw_swap.groupby(['PM', raw_swap['Month']]).sum()
    MTD_PB_Swap = MTD_PB_Swap.reset_index()
    MTD_PM_Swap = MTD_PM_Swap.reset_index()
    MTD_PB_Swap.columns = ['PB', 'Month', 'MTD Short Fee Accrual', 'MTD Short Proceeds Rebate']
    MTD_PM_Swap.columns = ['PM', 'Month', 'MTD Short Fee Accrual', 'MTD Short Proceeds Rebate']
    MTD_PB_Cash = raw_cash.groupby(['Custodian Code', raw_cash['Month']]).sum()
    MTD_PM_Cash = raw_cash.groupby(['PM', raw_cash['Month']]).sum()
    MTD_Currency_Swap = raw_country_currency.groupby(['crncy', raw_country_currency['Month']]).sum()
    MTD_PB_Cash = MTD_PB_Cash.reset_index()
    MTD_PM_Cash = MTD_PM_Cash.reset_index()
    MTD_Currency_Swap = MTD_Currency_Swap.reset_index()
    MTD_PB_Cash.columns = ['PB', 'Month', 'MTD Short Fee Accrual']
    MTD_PM_Cash.columns = ['PM', 'Month', 'MTD Short Fee Accrual']
    month = input('which month please:  ')
    MTD_PB_Swap.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\MTD_PB_Swap" + month + ".csv", index=False,
                       lineterminator='\n')
    MTD_PM_Swap.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\MTD_PM_Swap" + month + ".csv", index=False,
                       lineterminator='\n')
    MTD_PB_Cash.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\MTD_PB_Cash" + month + ".csv", index=False,
                       lineterminator='\n')
    MTD_PM_Cash.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\MTD_PM_Cash" + month + ".csv", index=False,
                       lineterminator='\n')
    print('Saved')
    # to mark overwrite file

    # with pd.ExcelWriter(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\202304_Financing.xlsx") as writer:
    #     MTD_PB_Swap.to_excel(writer,sheet_name='MTD_PB_SWAP')
    #     MTD_PM_Swap.to_excel(writer,sheet_name='MTD_PM_SWAP')
    #     MTD_PB_Cash.to_excel(writer,sheet_name='MTD_PB_Cash')
    #     MTD_PM_Cash.to_excel(writer,sheet_name='MTD_PM_Cash')
    with pd.ExcelWriter(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\2023YTD(Refreshing).xlsx") as writer:
        MTD_PB_Swap.to_excel(writer, sheet_name='YTD_PB_SWAP')
        MTD_PM_Swap.to_excel(writer, sheet_name='YTD_PM_SWAP')
        MTD_PB_Cash.to_excel(writer, sheet_name='YTD_PB_Cash')
        MTD_PM_Cash.to_excel(writer, sheet_name='YTD_PM_Cash')
    #     TableX.to_excel(writer,sheet_name='Summary')
    #     Table1v3.to_excel(writer,sheet_name='Notional_PM')
    #     Table1v5.to_excel(writer,sheet_name='Notional_ABS_PM_GMV')
    #     Tablecustodian.to_excel(writer,sheet_name='Custodian_GMV')
    writer.save()
    writer.close()

    # plot
# import plotly.express as px
# fig_currency_swap = px.bar(MTD_Currency_Swap, x=(['crncy','Month']), y=(['MTD Short Fee Accrual', 'MTD Short Proceeds Rebate']), color="City", barmode="group")
