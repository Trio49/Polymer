# import os
import glob
# import pytz
# import datetime as dt
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from pandas import DataFrame, Series, read_csv

# from os import listdir
# from os.path import isfile,join
# from datetime import datetime, timedelta

# import pandas as pd

# os.chdir(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon"),combine the CSV file if i need to run YTD analysis
combine = input('ytd combine?yes/no: ')
if combine == "yes":
    # ytd folder
    os.chdir(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown")
    extension = 'csv'
    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    # combine all files in the list
    all_filenames = [a for a in all_filenames if 'POLY_FINANCING_II_2023' in a]
    combined_csv: DataFrame | Series = pd.concat([pd.read_csv(f) for f in all_filenames])
    # export to csv
    combined_csv.to_csv("combineddata.csv", index=False)

ytd = input('load ytd file?else check the directory : yes/no:  ')
if ytd == 'yes':
    poly_fin = combined_csv
else:
    poly_fin = pd.read_csv(r'P:\All\FinanceTradingMO\Rebate_Borrow breakdown\POLY_FINANCING_II_202307.csv',
                           skip_blank_lines=True)
# smaller sample size for testing
# poly_fin=poly_fin.head(10000)
# PM filter list
# PMlist = ('AKUNG')
print(poly_fin['Date'].unique())
# read pb mapping
pb_map = pd.read_excel('P:\Operations\Polymer - Middle Office\PTH and FX booking\Borrow, OP booking/PB mappping.xlsx')
pb_map.columns = ['Custodian Code', 'Custodian ID']

# #filter _FX account
poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
# Enrich BB Ticker as bloomberg equity
poly_fin['BB Ticker'] = poly_fin['Bberg'] + ' Equity'
# segregate cash and swap
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

# Get BB yellow keylist
TICKER = poly_fin['BB Ticker']
# remove the empty value
TICKER = TICKER[~pd.isnull(TICKER)]
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
#add a input to chek if need to use blpbdp function
loading_bdp = input('load bdp?yes/no: ')
if loading_bdp != 'yes':

    px_list=pd.read_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data\px_list.csv')
    px_list2=pd.read_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data\px_list2.csv')
    px_list3=pd.read_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data\px_list3.csv')
else:
    px_list = blp.bdp(Ticker_list, 'EXCH_CODE')
    px_list2 = blp.bdp(Ticker_list, 'CRNCY')
    # load all historical dates
    px_list3 = blp.bdh(Ticker_list, 'PX_LAST', '20230101', Dates.max(), Currency='USD')
    #px_list ,px_list2,px_list3 to dataframe
    px_list = pd.DataFrame(px_list)
    px_list2 = pd.DataFrame(px_list2)
    px_list3 = pd.DataFrame(px_list3)
    #save the csvs to P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data
    px_list.to_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data\px_list.csv')
    px_list2.to_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data\px_list2.csv')
    px_list3.to_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\Bloomberg data\px_list3.csv')
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
# enrich the df with bloomberg columns
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

# Group by ticker and date
poly_fin_grouped = poly_fin.groupby(['BB Ticker', 'Custodian Code', 'Date2']).agg(
    lambda x:x.sum() if x.dtype == 'float64' else ' '.join(x)).reset_index()
# keep columns to calculate weighted broker fee
poly_fin_grouped = poly_fin_grouped[['BB Ticker', 'Custodian Code', 'Date2', 'Q*Fee', 'Quantity - Internal T/D']]
# calculate weighted broker fee
poly_fin_grouped['Weighted Broker Fee'] = poly_fin_grouped['Q*Fee'] / poly_fin_grouped['Quantity - Internal T/D'] * -1
poly_fin_grouped = poly_fin_grouped[['BB Ticker', 'Custodian Code', 'Date2', 'Weighted Broker Fee']]

# grouped per PB and ticker
check = input('Analyze Variance? yes/no')
if check == 'yes':
    poly_fin_grouped_PB = poly_fin.groupby(['BB Ticker', 'Custodian Code', 'Date2']).agg(
        lambda x:x.sum() if x.dtype == 'float64' else ' '.join(x)).reset_index()
    poly_fin_grouped_PB['Weighted Broker Fee'] = poly_fin_grouped_PB['Q*Fee'] / poly_fin_grouped_PB[
        'Quantity - Internal T/D'] * -1
    poly_fin_grouped_PB = poly_fin_grouped_PB[['BB Ticker', 'Custodian Code', 'Date2', 'Weighted Broker Fee']]
    poly_fin_gpb = poly_fin_grouped_PB
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
    # calculate min value of each name's rate
    poly_fin_grouped_PB['Min Value'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Weighted Broker Fee'].transform(
        min)
    # add a max date column to the dataframe to show the first Date2 when Max Value equals the Weighted Broker Fee
    # for each Holding Scenario
    poly_fin_grouped_PB['Max Date'] = np.where(poly_fin_grouped_PB['Weighted Broker Fee'] == poly_fin_grouped_PB[
        'Max Value'], poly_fin_grouped_PB['Date2'].astype(str), np.nan)
    # keep the first Max date for each holding scenario
    poly_fin_grouped_PB['Max Date'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Max Date'].transform('first')
    # add a min date column to the dataframe to show the first Date2 when Min Value equals the Weighted Broker Fee
    # for each Holding Scenario
    poly_fin_grouped_PB['Min Date'] = np.where(poly_fin_grouped_PB['Weighted Broker Fee'] == poly_fin_grouped_PB[
        'Min Value'], poly_fin_grouped_PB['Date2'].astype(str), np.nan)
    # keep the first Min date for each holding scenario
    poly_fin_grouped_PB['Min Date'] = poly_fin_grouped_PB.groupby('Holding Scenario')['Min Date'].transform('first')
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
    # poly_fin_top20variance['Max Date'] = poly_fin_top20variance['Max Date'].dt.strftime('%Y%m%d')
    # poly_fin_top20variance['Min Date'] = poly_fin_top20variance['Min Date'].dt.strftime('%Y%m%d')
    # save
    poly_fin_top20variance = poly_fin_top20variance[
        ['Holding Scenario', 'Std Dev', 'Max Value', 'Max Date', 'Min Value',
         'Min Date']]
    #add a column to compare the Max Date and Min Date, if Max Date is later than min, put "trend up" in the column, else put "trend down"
    poly_fin_top20variance['Trend'] = np.where(poly_fin_top20variance['Max Date'] > poly_fin_top20variance['Min Date'], 'Trend Up', 'Trend Down')
    #breakdown holding Scenario as ticker and PB
    poly_fin_top20variance['Ticker'] = poly_fin_top20variance['Holding Scenario'].str.split('__').str[0]
    poly_fin_top20variance['PB'] = poly_fin_top20variance['Holding Scenario'].str.split('__').str[1]
    #rearrange columns
    poly_fin_top20variance_PB = poly_fin_top20variance[
        ['Ticker','PB', 'Std Dev', 'Max Value', 'Max Date', 'Min Value',
         'Min Date']]

    # if ytd=='yes':
    #  poly_fin_top20variance.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdwon\YTD_ticker_ratevariance.csv",index=False,line_terminator='\n')
    # else:
    date_input = input(
        "Please enter the date you want to run the report for in the format MMDD: (e.g. 0101 for Jan 1st)")
    poly_fin_top20variance.to_csv(
        r"P:\Operations\Polymer - Middle Office\Borrow analysis\Hazeltree_dailydata\top20_MTD_ticker_rate_variance" + date_input + ".csv",
        index=False, line_terminator='\n')
    poly_fin_top20variance_PB.to_csv(
        r"P:\Operations\Polymer - Middle Office\Borrow analysis\Hazeltree_dailydata\top20_MTD_ticker_rate_variance" +"_pb and ticker"+ date_input + ".csv",
        index=False, line_terminator='\n')
    poly_fin_top20variance.to_csv(
        r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\plots/top20_MTD_ticker_rate_variance.csv")
#
#
# #grab the top 5 tikcers from the variance report and filter poly_fin_grouped_PB to only include those tickers
top5_volatile_names = read_csv(
    r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\plots/top20_MTD_ticker_rate_variance.csv")
top5_volatile_names = top5_volatile_names.head(20)
# add holding scenartio to poly_fin_grouped too
poly_fin_grouped['Holding Scenario'] = poly_fin_grouped['BB Ticker'].astype(str) + '__' + poly_fin_grouped[
    'Custodian Code'].astype(str)
# filter poly_fin_grouped to only include the top 5 tickers
poly_fin_grouped_top5 = poly_fin_grouped[
    poly_fin_grouped['Holding Scenario'].isin(top5_volatile_names['Holding Scenario'])]
# remove columns that are not needed
poly_fin_grouped_top5 = poly_fin_grouped_top5[['Holding Scenario', 'Date2', 'Weighted Broker Fee']]
# save poly_fin_grouped_top5 to csv
poly_fin_grouped_top5.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\plots/plots_raw_data.csv")
# for each holding scenario, scatter the weighted broker fee
for i in poly_fin_grouped_top5['Holding Scenario'].unique():
    poly_fin_grouped_top5[poly_fin_grouped_top5['Holding Scenario'] == i].plot(x='Date2', y='Weighted Broker Fee',
                                                                               title=i, kind='line')
    plt.show()
# save the plots to folder with holding scenario name plus the month as the file name
for i in poly_fin_grouped_top5['Holding Scenario'].unique():
    poly_fin_grouped_top5[poly_fin_grouped_top5['Holding Scenario'] == i].plot(x='Date2', y='Weighted Broker Fee',
                                                                               title=i, kind='line')
    plt.savefig(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\plots\\" + i + ".png")
    plt.close()
