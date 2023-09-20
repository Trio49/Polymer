import pandas as pd
from collections import defaultdict
import requests
import json

import requests
import json

def getAuthToken(userName, password):
    url = AUTH_ROOT + '/auth/risk/live'
    reqBody = json.dumps({
        'userName': 'zzhang',
        'password': 'Zach22222'
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

a = input('call trades API? y/n')
if a == 'y':
    #----call trades API to get all trades from 2023-01-01 to 2023-06-30
    url = "https://auth-api.polymerrisk.com/api/auth/risk/live"

    payload = json.dumps({
        "userName":"zzhang",
        "password":"Zach22222"
    })
    headers = {
        'Content-Type':'application/json',
        'Accept':'text/plain',
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    token = json.loads(response.text)['token']

    # print(response.text)
    url = "https://risk-api.polymerrisk.com/api/Trades/filter"

    payload = json.dumps({
        # "book": "string",
        # "pm": "PANHU",
        # "subPm": "string",
        "from":"2023-06-01",
        "to":"2023-06-30",
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
        "columns":[
            "bbgYellowKey", "quantity", "tradeDate",'pm'

        ],

    })

    headers = {
        'Content-Type':'application/json',
        'Accept':'text/plain',
        'Authorization':'Bearer ' + token
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    data = json.loads(response.text)
    #save data to dataframe
    data = pd.DataFrame(data)
    data.to_csv('P:\All\FinanceTradingMO\OP and netting Output/trades.csv', index=False)
else:
    data = pd.read_csv('P:\All\FinanceTradingMO\OP and netting Output/trades.csv')

#----read trades.csv
#conduct opposite direction check
b=input('press enter to conduct opposite direction check')
if b=='':

        trades = pd.read_csv('P:\All\FinanceTradingMO\OP and netting Output/trades.csv')
        #remove isEOD and isCancelled columns
        trades=trades[['pm','tradeDate','bbgYellowKey','quantity']]
        #remove quant pm from pm
        trades=trades[trades['pm']!=['KSUZU', 'HSASA', 'SHIGN', 'KUCHI', 'YANGX', 'YSUGI',
        'DPARK',
       'ECMIPO', 'AGINE', 'HGAWA', 'KAZUW', 'VIHAN', 'KNAKA',
       'ILIAW', 'SGUPT', 'LCHOW', 'AZHAN', 'MTANG', 'DAVWU', 'MWONG',
       'MELOU', 'SLEUN', 'TNARU', 'MKITA', 'YOKISH', 'YINOU', 'TSHOM',
       'SSUGI', 'MSUZU', 'AFUKU', 'YNAKA', 'HFUJI', 'KOKUM', 'JZHUO',
       'KABUR', 'MAMORI', 'ALEXC', 'KMOHR', 'KMIYA', 'AKUNG', 'PANHU',
       'ROSUH', 'EDMLO', 'DCHIN', 'KZHAN', 'BOYAN', 'NICYU', 'AAGAO']
                      ]

        #rename tradeDate to trade_date, rename bbgYellowKey to bbg_yellow_key
        trades.rename(columns={'tradeDate':'trade_date','bbgYellowKey':'bbg_yellow_key'},inplace=True)
        trades = trades[['pm', 'trade_date', 'bbg_yellow_key', 'quantity']]
        trades.dropna(inplace=True, subset=['bbg_yellow_key'])
        # include only JP equity trades
        trades = trades[trades['bbg_yellow_key'].str.contains('JP Equity')]

        opp_trades_num = defaultdict(int)

        # if pm_subset:
        #     trades = trades[trades['pm'].isin(pm_subset)]

        group_by = trades.groupby(['trade_date', 'bbg_yellow_key', 'pm'])

        df_grouped = pd.DataFrame()
        df_grouped['quantity'] = group_by['quantity'].sum().reset_index()['quantity']
        df_grouped['pm'] = group_by['quantity'].first().reset_index()['pm']
        df_grouped['bbg_yellow_key'] = group_by['quantity'].first().reset_index()['bbg_yellow_key']
        df_grouped['trade_date'] = group_by['quantity'].first().reset_index()['trade_date']

        # keep only duplicated records
        df_grouped = df_grouped[df_grouped.duplicated(subset=['trade_date', 'bbg_yellow_key'], keep=False)]

        unique_ticker_by_date = df_grouped.drop_duplicates(subset=['trade_date', 'bbg_yellow_key'])

        for index, row in unique_ticker_by_date.iterrows():
            tk = row['bbg_yellow_key']
            dt = row['trade_date']

            sub_group = df_grouped[(df_grouped['bbg_yellow_key'] == tk) & (df_grouped['trade_date'] == dt)]

            pm_list = sorted(list(sub_group['pm']))

            for pm1 in pm_list:
                for pm2 in pm_list:
                    if pm1 != pm2:
                        q1 = sub_group[(sub_group['pm'] == pm1)]['quantity'].sum()
                        q2 = sub_group[(sub_group['pm'] == pm2)]['quantity'].sum()
                        if q1 * q2 < 0:
                            opp_trades_num[pm1 + pm2] += 1


        opp_trades_num = pd.DataFrame({'pair': list(opp_trades_num.keys()), 'value': list(opp_trades_num.values())})
opp_trades_num.to_csv('P:\All\FinanceTradingMO\OP and netting Output/opp_trades_num.csv', index=False)
# result.to_csv('P:\All\FinanceTradingMO\OP and netting Output/opp_trades_num.csv', index=False)



