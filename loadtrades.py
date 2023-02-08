# -*- coding: utf-8 -*-
"""
Created on Tue Feb  7 18:13:56 2023

@author: zzhang
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 11:43:49 2022

@author: zzhang
"""

import requests

import json
import pandas as pd
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
url = "https://risk-api.polymerrisk.com/api/Trades/filter"

payload = json.dumps({
  # "book": "string",
  "pm": "PANHU",
  # "subPm": "string",
  "from": "2022-12-31",
  "to": "2023-02-01",
  # "date": "string",
  # "port": "string",
  # "instrumentIds": [
  #   "string"
  # ],
  # "instrumentTypes": [
  #   "string"
  # ],
  # "instrumentSubTypes": [
  #   "string"
  # ],
  "columns": [
   "bbgYellowKey","quantity","tradeDate"
   
  ],
 
})

headers = {
  'Content-Type': 'application/json',
  'Accept': 'text/plain',
  'Authorization': 'Bearer '+token
}



response = requests.request("POST", url, headers=headers, data=payload)
data=json.loads(response.text)
data=pd.DataFrame(data)
