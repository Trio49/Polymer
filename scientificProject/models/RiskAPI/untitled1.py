# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:43:49 2022

@author: zzhang
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:33:28 2022

@author: zzhang
"""

import json

import requests

url = "https://auth-api.polymerrisk.com/api/auth/risk/live"

payload = json.dumps({
    "userName":"zzhang",
    "password":"Zach33333"
})
headers = {
    'Content-Type':'application/json',
    'Accept':'text/plain',
}

response = requests.request("POST", url, headers=headers, data=payload)
token = json.loads(response.text)['token']

# print(response.text)
url = "https://risk-api.polymerrisk.com/api/Positions/daily"

payload = json.dumps({
    "filter":{
        "book":"AGINE",
        "startDate":"2022-09-18",
        "endDate":"2022-09-18",
        "isActive":True,
        "onlyEod":True,
        "onlyLatestIntraday":False,
        "columns":[
            "DeltaUsd",
            "BbgYellowKey",
            "UnderlyingBbgYellowKey",
            "Ric",
            "UnderlyingRic"
        ]
    }
})
headers = {
    'Content-Type':'application/json',
    'Accept':'text/plain',
    'Authorization':'Bearer ' + token
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
