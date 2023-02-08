# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:43:49 2022

@author: zzhang
"""

import requests

import json
# import pandas as pd
# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:33:28 2022

@author: zzhang
"""

import requests
import json



url = "https://auth-api.polymerrisk.com/api/auth/risk/live"



payload = json.dumps({
  "userName": "zzhang",
  "password": "Zach55555"
})
headers = {
  'Content-Type': 'application/json',
  'Accept': 'text/plain',
}



response = requests.request("POST", url, headers=headers, data=payload)
token = json.loads(response.text)['token']


# print(response.text)
url = "https://risk-api.polymerrisk.com/api/Positions/daily"



payload = json.dumps({
  "filter": {
    "book": "AAGAO",
    "startDate": "2022-09-07",
    "endDate": "2022-09-07",
    "isActive": True,
    "onlyEod": True,
    "onlyLatestIntraday": False,
    "columns": [
      "DeltaUsd",
      "BbgYellowKey",
      "UnderlyingBbgYellowKey",
      "Ric",
      "UnderlyingRic"
    ]
  }
})
headers = {
  'Content-Type': 'application/json',
  'Accept': 'text/plain',
  'Authorization': 'Bearer '+token
}



response = requests.request("POST", url, headers=headers, data=payload)
data=json.loads(response.text)


print(response.text)

