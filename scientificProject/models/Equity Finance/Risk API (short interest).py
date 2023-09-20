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


# In[2]:


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


    # In[4]:


    # get the short interest data from Markit API
    #define a list of Tickes
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

    token = getAuthToken('zzhang', 'Zach22222')
    if not token:
        print('Fail to pass authentication, plz check network connection or credential details')

    short_interest_ReqBody = {
        "from": '2023-07-01',
        "to": '2023-07-31',
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
import pandas as pd

Markit_si=si


# In[ ]:




