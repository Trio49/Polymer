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
    # "internalAccount":"HSASA",
    "timeRange":{
        "start_time":"2023-08-14T02:07:09.028",
        "end_time":"2023-08-15T02:07:09.028"
    }
    # "toraGroup":"POLYMER"
})
headers = {
    'Content-Type':'application/json',
    'Accept':'application/json',
    'Authorization':f'Bearer {token}'
}

response = requests.request("POST", url=borrows_url, headers=headers, data=payload)
response_order= requests.request("POST", url=orders_url, headers=headers, data=payload)
borrow_data = response.json()
order_data = response_order.json()


print(borrow_data)
#convert data to dataframe
df_borrow = pd.DataFrame(borrow_data)
df_order= pd.DataFrame(order_data)
