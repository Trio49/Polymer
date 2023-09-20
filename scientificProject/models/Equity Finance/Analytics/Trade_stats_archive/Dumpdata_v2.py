# Description: This script is used to dump the data from the trade blotter and save it to a csv file

# import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from os import listdir
import pytz
import pandas as pd

today = datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 
# today=today.date()
today = today.date() - timedelta(days=0)

today = today.strftime('%Y%m%d')

# all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_tradeblotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today
firm_positions_list_dir = listdir(all_PM_tradeblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:

    if "Trade Blotter - PM ALL (New)" in file_name:
        print(file_name)

        all_PM_tradeblotter_file_path = all_PM_tradeblotter_file_path + "\\" + file_name
        break

# Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path)
# load EOD blotter
Alltrade = pd.read_csv(all_PM_tradeblotter_file_path)
Alltrade = Alltrade[['FundShortName', 'Strategy', 'Counterparty Short Name', 'BB Yellow Key',
                     'Transaction Type', 'Notional Quantity', 'CustodianShortName',
                     'Trade Price', 'Market Price', 'Trade Date', 'Settle Date',
                     'Trade Currency', 'Business Unit',
                     'Sub Business Unit', 'Contract Multiplier',
                     'Trade/Book FX Rate', 'Settle Currency', 'Description', 'Asset Class',
                     'Volume',
                     'Trading Net Proceeds', '$ Trading Net Proceeds',
                     'Order Total Quantity', 'Order Today Executed Quantity',
                     'Trading Notional Proceeds', 'InstIDPlusBuySell',
                     'WeightAverageTradePrice', 'Exchange Country Code',
                     'PAG Delta adjusted Option Notional ', 'Delta Adjusted MV',
                     'OutSourceTraderID',
                     '$ Notional Quantity * Trade Price', 'Instrument Type',
                     '$ Trading Notional Net Proceeds']]

import blpapi

# Define the Bloomberg API session
session = blpapi.Session()
session.start()
session.openService("//blp/refdata")
refDataService = session.getService("//blp/refdata")

# Load the ticker list from the CSV file
# ticker_list = pd.read_csv('ticker_list.csv')

# Define the fields you want to retrieve
from xbbg import blp
import pdblp

con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()

TICKER = Alltrade['BB Yellow Key']
Ticker_list = pd.Series.tolist(TICKER)

# enriching BBG country code

Ticker_list = list(set(Ticker_list))
fields = ['EQY_BETA_6M', 'VOLUME_AVG_30D', 'CRNCY_ADJ_PX_LAST']
# CRY_list=blp.bdp(Ticker_list, 'EQY_BETA_6M', 'VOLUME_AVG_30D', 'CRNCY_ADJ_PX_LAST')
CRY_list = blp.bdp(Ticker_list, flds=['EQY_BETA_6M', 'VOLUME_AVG_30D', 'CRNCY_ADJ_PX_LAST'])
CRY_list = CRY_list.reset_index()
# Save the updated ticker list to a new CSV file
# tradedatahtlt=tradedata.merge(htlt,left_on='Counterparty Short Name',right_on='Counterparty Short Name',how='left')
Alltrade_1 = Alltrade.merge(CRY_list, how='inner', left_on='BB Yellow Key', right_on='index')
# define average daily volume in USD
Alltrade_1['ADV USD'] = Alltrade_1['volume_avg_30d'] * Alltrade_1['crncy_adj_px_last'] / Alltrade_1[
    'Trade/Book FX Rate']
# save the massaged data to csv
Alltrade_1.to_csv(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Tradingdata\updated_all_trade.csv', index=False)
