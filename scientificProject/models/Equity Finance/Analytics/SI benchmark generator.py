#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
import pprint
import json
import requests
from time import perf_counter

readfile=input('Read file for the SI ticker list? yes/no: ')
if readfile=='yes':
    top5_volatile_names = pd.read_csv(
        r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\plots/top20_MTD_ticker_rate_variance.csv")
    SI_Ticker=top5_volatile_names['Ticker'].unique()
    #Save SI_Ticker to a list
    SI_Ticker=SI_Ticker.tolist()
elif readfile=='no':
    print('Here is the list of tickers: ')
    SI_Ticker=[
    "MPNGY US Equity",
    "4071 JP Equity",
    "GOTO IJ Equity",
    "SDRL US Equity",
    "EVEX US Equity",
    "6526 JP Equity",
    "GRND US Equity",
    "601689 CH Equity",
    "2590 JP Equity",
    "002216 CH Equity",
    "2007 HK Equity",
    "001570 KS Equity",
    "1605 TT Equity",
    "085370 KS Equity",
    "600557 CH Equity",
    "9866 HK Equity",
    "603127 CH Equity",
    "600754 CH Equity",
    "2002 TT Equity",
    "1797 HK Equity",
    "8255 TT Equity",
    "688169 CH Equity",
    "2007 HK Equity",
    "6526 JP Equity",
    "5285 TT Equity",
    "002129 CH Equity",
    "1797 HK Equity",
    "2603 TT Equity",
    "002507 CH Equity",
    "688521 C1 Equity"
    ]

# Markit data risk API

def load_markit_data():
    API_ROOT = 'https://risk-api.polymerrisk.com/api'
    AUTH_ROOT = 'https://auth-api.polymerrisk.com/api'
    PRINTER = pprint.PrettyPrinter()


    def getAuthToken(userName, password):
        url = AUTH_ROOT + '/auth/risk/live'
        reqBody = json.dumps({
            'userName': userName,
            'password': password
        })
        headers = {
            'accept': 'text/plain',
            'Content-Type': 'application/json',
        }
        response = requests.post(
            url = url,
            data = reqBody,
            headers=headers,
        )

        token = None
        if response.status_code == 200:
            res = json.loads(response.text)
            token = res['token']

        return token


    def getShortInterest(token, reqBody):
        url = API_ROOT + '/ShortInterest/DxOpen'
        data = json.dumps(reqBody)
        headers = {
            'accept': 'text/plain',
            'Content-Type': 'application/json-patch+json',
            'Authorization': 'Bearer ' + token
        }
        response = requests.post(
            url = url,
            data = data,
            headers = headers,
        )
        short_interest = None
        if response.status_code == 200:
            res = json.loads(response.text)
            short_interest = res

        return short_interest

    token = getAuthToken('zzhang', 'Zach22222')
    if not token:
        print('Fail to pass authentication, plz check network connection or credential details')

    short_interest_ReqBody = {
        "from": '2023-08-01',
        "to": '2023-08-31',
        "tickers": SI_Ticker,
    }

    t1_start = perf_counter()
    si = getShortInterest(token, short_interest_ReqBody)
    t1_stop = perf_counter()
    print("Elapsed time during the whole program in seconds:", t1_stop-t1_start)
    si = pd.DataFrame(si)
    return(si)

run_markit_load = input('Run Markit load? yes/no: ')
if run_markit_load=='yes':
    si = load_markit_data()
else:
    si = pd.read_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\MTDshortinterest/Markit_si.csv')
#rename si as Markit_si


Markit_si=si
#get ticker, date, indicativefee7
Markit_si_benchmark=Markit_si[['bbgYellowKey','date','indicativeFee7']]
#read ENSO fee
poly_fin = pd.read_csv('P:\All\FinanceTradingMO\Rebate_Borrow breakdown\POLY_FINANCING_II_202308.csv',
                       skip_blank_lines=True)

datelist=poly_fin['Date'].unique()
print('datelist is: ',datelist)
poly_fin['BB Ticker'] = poly_fin['Bberg'] + ' Equity'

poly_fin = poly_fin[[
                     'BB Ticker','Date',
                     'ENSO Fee',
                     ]]
poly_fin = poly_fin.loc[poly_fin['BB Ticker'].isin(SI_Ticker)]
poly_fin['ENSO Fee']=poly_fin['ENSO Fee'].abs()
#keep the Unique BB Ticker and Date
poly_fin=poly_fin.drop_duplicates(subset=['BB Ticker','Date'],keep='first')
#print shape of poly_fin and Markit_si_benchmark
print('shape of poly_fin is: ',poly_fin.shape)
print('shape of Markit_si_benchmark is: ',Markit_si_benchmark.shape)
#fromat the dateformat of both dataframes to be the same as "2021-07-01"
poly_fin['Date']=pd.to_datetime(poly_fin['Date'])
poly_fin['Date']=poly_fin['Date'].dt.strftime('%Y-%m-%d')
Markit_si_benchmark['date']=pd.to_datetime(Markit_si_benchmark['date'])
Markit_si_benchmark['date']=Markit_si_benchmark['date'].dt.strftime('%Y-%m-%d')
#create a new dataframe call Benchmark, take Markit_si_benchmark as the base, and add ENSO fee to it, if there's no Markit_si_benchmark, then add ENSO fee to it
Benchmark=Markit_si_benchmark.merge(poly_fin,how='left',left_on=['bbgYellowKey','date'],right_on=['BB Ticker','Date'])
Benchmark['Benchmark']=np.where(Benchmark['indicativeFee7'].isnull(),Benchmark['ENSO Fee'],Benchmark['indicativeFee7'])
Benchmark=Benchmark[['bbgYellowKey','date','Benchmark']]
#save the Benchmark to a csv file under P:\Operations\Polymer - Middle Office\Borrow analysis\MTDshortinterest
Benchmark.to_csv(r'P:\Operations\Polymer - Middle Office\Borrow analysis\MTDshortinterest/Benchmark.csv',index=False)
Benchmark.to_csv(r'P:\All\FinanceTradingMO\Rebate_Borrow breakdown\plots/Benchmark.csv',index=False)
print('done, please rerun the plots generation')