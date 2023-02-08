
import pytz
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from time import perf_counter

import json
import requests

import pprint

pd.set_option('display.max_columns', 30)


# Markit data risk API


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




token = getAuthToken('zzhang','Zach54321')
if not token:
    print('Fail to pass authentication, plz check network connection or credential details')

short_interest_ReqBody = {
    "from": "2022-11-29",
    "to": "2022-11-29",
    "tickers": ['LKE AU Equity',
                '3653 TT Equity',
                '7816 JP Equity',
                '2481 TT Equity',
                '5269 TT Equity',
                '5274 TT Equity',
                '2360 TT Equity',
                '3707 TT Equity',
                '5032 JP Equity',
                '6488 TT Equity',
                '3680 TT Equity',
                '6619 JP Equity',
                '6443 TT Equity',
                'VUL AU Equity',
                '2353 TT Equity']
}


t1_start = perf_counter()
SI = getShortInterest(token, short_interest_ReqBody)
SI = pd.DataFrame(SI)
t1_stop = perf_counter()
print("Elapsed time during the whole program in seconds:", t1_stop-t1_start)
SI.to_excel(r"P:\Operations\Polymer - Middle Office\Borrow analysis\Markit_rates.xlsx",index=False)





