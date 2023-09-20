# read the API data to fetch the correct same date range
#AUTH_ROOT = 'https://risk-api-internal.polymerrisk.com/api'
import os, pandas as pd, glob
import pandas.io.common

###################manufacturing the trade blotter
import pytz
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from os import listdir
from time import perf_counter
import pdblp

import json
import requests

import pprint
from os import listdir

import pickle

pd.set_option('display.max_columns', 137)

API_ROOT = 'https://risk-api.polymerrisk.com/api'
AUTH_ROOT = 'https://auth-api.polymerrisk.com/api'
PRINTER = pprint.PrettyPrinter()


def getAuthToken(userName, password):
    url = AUTH_ROOT + '/auth/risk/live'
    reqBody = json.dumps({
        'userName': 'zzhang@pag.com',
        'password': 'Zach12345'
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


def getPositions(token, reqBody):
    url = API_ROOT + '/positions/daily'
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
    positions = None
    if response.status_code == 200:
        print('success API extract')
        res = json.loads(response.text)
        positions = res
    else:
        print('fail API extract, status code:'+ str(response.status_code))
        print(response)



    return positions

def getTrades(token, reqBody):
    url = API_ROOT + '/Trades/filter'
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
    trades = None
    if response.status_code == 200:
        print('success API extract')
        res = json.loads(response.text)
        trades = res
    else:
        print('fail API extract, status code:'+ str(response.status_code))
        print(response)



    return trades
