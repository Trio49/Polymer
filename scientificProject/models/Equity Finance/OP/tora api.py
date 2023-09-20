import json
import requests
import pandas as pd

base_url = "https://tora-api.polymerquant.com"
login_url = f"{base_url}/auth/login"
#enter my credentials
payload = {'username':'mo', 'password':'UZGTvmyjxPJcaQgAoAKXcWY9D4iq9NgkhRZoCyAV5BNspaH2'}

response = requests.request("POST", url=login_url, data=payload).json()
token = response['access_token']

borrows_url = f"{base_url}/borrows/filter"
orders_url = f"{base_url}/orders/filter"

payload = json.dumps({
    "internalAccount":"AGINE",
    "creationTimeRange":{
        "startTime": "2023-05-23T06:07:58+08:00",
        "endTime": "2023-05-25T13:07:58+08:00"
    },
    "toraGroup":"POLYMER"
})
headers = {
    'Content-Type':'application/json',
    'Accept':'application/json',
    'Authorization':f'Bearer {token}'
}

response = requests.request("POST", url=borrows_url, headers=headers, data=payload)
response_order= requests.request("POST", url=orders_url, headers=headers, data=payload)
data = response_order.json()

print(data)
#convert data to dataframe
df = pd.DataFrame(data)
