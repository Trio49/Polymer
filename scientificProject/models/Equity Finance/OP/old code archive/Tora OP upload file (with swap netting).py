#!/usr/bin/env python
# coding: utf-8

# In[28]:


import pandas as pd
import numpy as np
import pdblp
from datetime import datetime, timedelta
from os import listdir
import shutil


import pytz
from time import perf_counter
import json
import requests
import pprint
import pickle


pd.set_option('display.max_columns', 132)
pd.set_option('display.max_rows', 200)



def get_yest(date):
    if date.strftime('%w') == '1':
        return date - timedelta(days=3)
    else:
        return date - timedelta(days=1)
    
# convert UBS spread column
def UBS_convert_spread(spread):
    try:
        if '(' in spread:
            return float(spread[1:-1])*100
    except:
        return -float(spread)*100
    
# change CG CS to CH in tickers
def CH_convert(ticker):
    try:
        if ('CG' in ticker) | ('CS' in ticker):
            return ticker[0:6] + ' CH'
        else:
            return ticker[0:9]
    except:
        return ''


# In[30]:


#read the brokers' outperformance list

today = datetime.today()
#today = today - timedelta(days=1)  #use when need try backdate
#today = datetime.strptime('2023-03-27', '%Y-%m-%d') #only use to backdate specific date
today_text = today.strftime('%Y%m%d')
yest = get_yest(today)
yest_text = yest.strftime('%Y%m%d')

print(today_text)
print(yest_text)

# get all broker OP file from share drive
def get_broker_OP_file(today_text, yest_text):
    # get BAML file path and file name
    BAML_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/BAML/'
    BAML_filename = 'OP_List_' + today_text + '.csv'
    print(BAML_filepath + BAML_filename)

    # get GS file path and file name
    GS_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/GS/' + yest_text + '/'
    GS_filename = 'SRPB_220285_1200596676_DATA_Daily_Optim_302811_APE_794730_' + yest_text + '.xls'
    print(GS_filepath + GS_filename)

    # get UBS file path and file name
    UBS_filepath= '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/UBS/'+ yest_text + '/'
    UBS_files_list_dir=listdir(UBS_filepath)
    for file_name in UBS_files_list_dir:
        if yest_text + '.AxesOpportunitiesAPACCN.GRPPOLYM.' in file_name:
            UBS_filename = file_name
            break
    #UBS_filename = '20230327.AxesOpportunitiesAPACCN.GRPPOLYM.668065323_original.csv' # remove later after testing
    print(UBS_filepath + UBS_filename)
    
    # get JPM file path and file name
    JPM_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/JPM/' + today_text[0:6] + '/' + today_text + '/'
    JPM_filename = 'APAC_OP_' + today_text + '.csv'
    print(JPM_filepath + JPM_filename)
    
    # read each broker's file
    try:
        BAML_OP = pd.read_csv(BAML_filepath + BAML_filename)
        BAML_OP = BAML_OP.loc[0:len(BAML_OP)-5,]
#         target_path = 'C:/Users/douglasl/Documents/SBL/Trading/python/outperformance china/broker_OP_file/BAML/'
#         shutil.copyfile(BAML_filepath + BAML_filename, target_path + BAML_filename)
    except:
        print('fail to read BAML OP file')
        BAML_OP = pd.DataFrame()

    try:
        UBS_OP = pd.read_csv(UBS_filepath + UBS_filename)
#         target_path = 'C:/Users/douglasl/Documents/SBL/Trading/python/outperformance china/broker_OP_file/UBS/'
#         shutil.copyfile(UBS_filepath + UBS_filename, target_path + UBS_filename)
    except:
        print('fail to read UBS OP file')
        UBS_OP = pd.DataFrame()
        
    try:
        GS_OP = pd.read_excel(GS_filepath + GS_filename, header=7)
#         target_path = 'C:/Users/douglasl/Documents/SBL/Trading/python/outperformance china/broker_OP_file/GS/'
#         shutil.copyfile(GS_filepath + GS_filename, target_path + GS_filename)
        #GS_OP = GS_OP.loc[0:len(GS_OP)-2,]
    except:
        print('fail to read GS OP file')
        GS_OP = pd.DataFrame()
    
    try:
        JPM_OP = pd.read_csv(JPM_filepath + JPM_filename)
#         target_path = 'C:/Users/douglasl/Documents/SBL/Trading/python/outperformance china/broker_OP_file/JPM/'
#         shutil.copyfile(JPM_filepath + JPM_filename, target_path + JPM_filename)
    except:
        print('fail to read JPM OP file')
        JPM_OP = pd.DataFrame()
    
    #return (BAML_OP, UBS_OP, GS_OP) #old version
    return (BAML_OP, UBS_OP, GS_OP, JPM_OP) #new version with JPM
    
#BAML_OP, UBS_OP, GS_OP = get_broker_OP_file(today_text = today_text, yest_text = yest_text) #old version
BAML_OP, UBS_OP, GS_OP, JPM_OP = get_broker_OP_file(today_text = today_text, yest_text = yest_text) #new version with JPM


# In[31]:


# gather brokers' OP file into one standard format table
#def get_summary_op(BAML_OP, UBS_OP, GS_OP): #old version
def get_summary_op(BAML_OP, UBS_OP, GS_OP, JPM_OP): # new version with JPM   
    
    if len(BAML_OP) > 0:
        filt = (BAML_OP['Mkt'] == 'CC') | (BAML_OP['Mkt'] == 'CN')
        BAML_OP = BAML_OP[filt]
        BAML_OP = BAML_OP[['Ticker','Shares', 'Rate']]
        BAML_OP['Broker'] = 'baml'
        BAML_OP['Rate'] = BAML_OP['Rate']*-1

    if len(UBS_OP) > 0:
        UBS_OP = UBS_OP[['BB Ticker', 'Synthetic UBS Qty', 'Synthetic Spread']]
        UBS_OP['Broker'] = 'ubs'
        UBS_OP['BB Ticker'] = UBS_OP['BB Ticker'].apply(CH_convert)
        filt = -(UBS_OP['Synthetic Spread'].apply(lambda x: pd.isna(x)) | (UBS_OP['Synthetic Spread'] == '0.50') | (UBS_OP['Synthetic Spread'] == 0.5))
        UBS_OP = UBS_OP[filt]
        try:
            UBS_OP['Synthetic Spread'] = UBS_OP['Synthetic Spread'].apply(UBS_convert_spread)
        except:
            pass
        UBS_OP.rename(columns={'BB Ticker': 'Ticker', 'Synthetic UBS Qty':'Shares', 'Synthetic Spread':'Rate'}, inplace=True)

    if len(GS_OP) > 0:
        filt = (GS_OP['Issue Market'] == 'China') & (GS_OP['Preference'] == 'Transfer In') & (GS_OP['Long / Short'] == 'Long')
        GS_OP = GS_OP.loc[filt, ['Bloomberg', 'Synthetic Quantity', 'Synthetic Rate']]
        GS_OP['Broker'] = 'gs'
        GS_OP['Bloomberg'] = GS_OP['Bloomberg'].apply(CH_convert)
        GS_OP['Synthetic Rate'] = GS_OP['Synthetic Rate']*-100*100
        GS_OP.rename(columns={'Bloomberg': 'Ticker', 'Synthetic Quantity':'Shares', 'Synthetic Rate':'Rate'}, inplace=True)
    
    if len(JPM_OP) > 0:
        JPM_OP['BBTicker'] = JPM_OP['BBTicker'].apply(CH_convert)
        JPM_OP = JPM_OP[['BBTicker','Quantity', 'Rate']]
        JPM_OP['Broker'] = 'jpm'
        JPM_OP['Rate'] = JPM_OP['Rate']*-1
        JPM_OP.rename(columns={'BBTicker':'Ticker', 'Quantity':'Shares' }, inplace=True) #JPM_OP[['Ticker','Shares', 'Rate']]

    summary_op = pd.DataFrame()
    #summary_op = pd.concat([summary_op, BAML_OP, UBS_OP, GS_OP]) #old version
    summary_op = pd.concat([summary_op, BAML_OP, UBS_OP, GS_OP, JPM_OP]) #new version with JPM
    summary_op.reset_index(inplace=True)
    summary_op.drop(columns='index', inplace=True)
    return summary_op

#summary_op = get_summary_op(BAML_OP=BAML_OP, UBS_OP=UBS_OP, GS_OP=GS_OP) # old version
summary_op = get_summary_op(BAML_OP=BAML_OP, UBS_OP=UBS_OP, GS_OP=GS_OP, JPM_OP=JPM_OP) # new version with JPM
# summary_op.to_csv('C:/Users/douglasl/Documents/SBL/Trading/python/outperformance china/output/OP tracker/summary_op_avail/'+today_text+'_summary_op_avail.csv', index=False)

#-----------------------------------------------


# In[32]:


# rearrange the brokers OP file for Tora upload
tora_upload_op_summary = summary_op.loc[summary_op['Broker']=='baml','Ticker':'Rate'].rename(columns={'Shares':'baml_shs', 'Rate':'baml_rate'})
tora_upload_op_summary = tora_upload_op_summary.merge(summary_op.loc[summary_op['Broker']=='ubs','Ticker':'Rate'].rename(columns={'Shares':'ubs_shs', 'Rate':'ubs_rate'}), how='outer', on='Ticker')
tora_upload_op_summary = tora_upload_op_summary.merge(summary_op.loc[summary_op['Broker']=='gs','Ticker':'Rate'].rename(columns={'Shares':'gs_shs', 'Rate':'gs_rate'}), how='outer', on='Ticker')
tora_upload_op_summary = tora_upload_op_summary.merge(summary_op.loc[summary_op['Broker']=='jpm','Ticker':'Rate'].rename(columns={'Shares':'jpm_shs', 'Rate':'jpm_rate'}), how='outer', on='Ticker')
filt = -(tora_upload_op_summary['Ticker'] == '')
tora_upload_op_summary = tora_upload_op_summary[filt]
tora_upload_op_summary.reset_index(inplace=True)
tora_upload_op_summary.drop(columns='index', inplace=True)

# second approach for aggregate op_rate, sort from highest to lowest, prefer way
for index in tora_upload_op_summary.index:
    filt = summary_op['Ticker'] == tora_upload_op_summary.loc[index,'Ticker']
    temp_table = summary_op[filt].sort_values('Rate', ascending=False)
    temp_table.reset_index(inplace=True)
    temp_table.drop(columns='index', inplace=True)
    rate_str = ''
    for temp_table_index in range(len(temp_table)):
        rate_str = rate_str + temp_table.loc[temp_table_index, 'Broker'] + '_' + str(int(temp_table.loc[temp_table_index, 'Rate'])) + '/'
    tora_upload_op_summary.loc[index,'op_rate_str'] = rate_str[0:-1]
final_table = tora_upload_op_summary[['Ticker', 'op_rate_str']]
final_table['Ticker'] = final_table['Ticker'] + ' Equity'

# save tora upload file
#output_filepath = 'P:/Trading/OP Tora upload/'
# output_filepath = 'C:/Users/douglasl/Documents/SBL/Trading/python/outperformance china/output/' #testing
output_filename = today_text + '_tora_upload.csv'

final_table.rename(columns={'Ticker':'Instrument'}, inplace=True)
# final_table.to_csv( + output_filename, index=False)


# In[33]:


# read the API data to fetch the correct same date range
#AUTH_ROOT = 'https://risk-api-internal.polymerrisk.com/api'


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


# In[35]:


# get extra week risk API position data and save to sharedrive

# TODO: Set your PC login credential here
token = getAuthToken('zzhang', 'Zach11111')
if not token:
    print('Fail to pass authentication, plz check network connection or credential details')

date_range_original = []
positions_new = pd.DataFrame()


yest_text = yest.strftime('%Y-%m-%d')
DATE_RANGE = [[yest_text, yest_text]]


for dates_range in DATE_RANGE:
    previous = dates_range[0]
    latter = dates_range[1]
    startDate=previous
    endDate=latter
    
    print(previous)

    #for book_name in book_list:

    positionReqBody = {
        'filter': {
            # Change filter content to fit your need
            #'book': 'EDMLO',
            'startDate': startDate,
            'endDate': endDate,
            'bbgYellowKey': None,
            'isActive': True,
            'onlyEod': True,
            'onlyLatestIntraday': False,
            'instrumentTypes':['Equity'],
            # Add the required cols here, explicit declare required
            'columns': [
                'underlyingBbgYellowKey',
                'description',
                'quantity',
                'marketValueUsd',
                'notionalUsd',
                'dailyPnlUsd',
                'instrumentType',
                'countryOfExchange',
                'avgVol30D',
                'isLong',
                'betaAdjDelta',
                'betaMkt',
                'deltaUsd',
                ### try other cols
                'mktCapCat',
                'avgVol20D',
                'custodian',
                'custodianAccount',
                'TrsId',
                'pm',
                'strategy',
                'avgCostUsd',
                'ytdPnlUsd',
                'mtdPnlUsd'


            ]
        }
    }

    t1_start = perf_counter()
    positions = getPositions(token, positionReqBody)
    t1_stop = perf_counter()
    print("Elapsed time during the whole program in seconds:", t1_stop-t1_start)
    if positions:
        print('Position Count: ', len(positions))
        PRINTER.pprint(positions[0])


    positions= pd.DataFrame(positions)
    #print(positions)
    print('position data has ' + str(len(positions)) + ' rows')
    
    if len(positions) > 0:
        #positions=positions[positions["instrumentType"].isin(["Equity"])]
        positions["Standadized_Date"]=positions["positionDate"].apply(lambda x:x[0:10])
        positions["dtdates"]=positions["Standadized_Date"].apply(lambda x:datetime.strptime(x,"%Y-%m-%d"))

        positions_new =  positions_new.append(positions)


        date_range=list(set(positions["dtdates"]))

        date_range.sort()
        date_range_original.append(date_range)

#         #Save the past_days_list to local server as record
    
#         file_name = startDate.replace('-','') + '_week_polymer_position.obj'
#         path_name = "P:/Trading/Risk API data/Position/"

#         with open(path_name+file_name, 'wb',) as fp:
#             pickle.dump(positions, fp)

print("read API positions done!")


# In[36]:


# broker mapping table

broker_mappings={
    'Morgan Stanley' : 'msdw',
    'Goldman Sachs' : 'gs',
    'Goldman Sachs (FPI)' : 'gs',
    'BAML' : 'baml',
    'UBS' : 'ubs',
    'JP Morgan' : 'jpm',
    'Nomura' : 'nmr',
    'CICC' : 'cicc',
    'CLSA' : 'clsa',
    'GUOTAI JUNAN' : 'gtja',
    'HUATAI' : 'huatai',
    'Mizuho' : 'miz'
}

def convert_broker(custodian):
    try:
        return broker_mappings[custodian]
    except:
        return 
    
def convert_shs_quantity(quantity):
    if abs(quantity) >= 1000000:
        temp_str = str(abs(quantity)//1000/1000)+'m'
    elif abs(quantity) >= 1000:
        temp_str = str(abs(quantity)//100/10)+'k'
    else:
        temp_str = str(abs(quantity))
    
    if quantity >= 0:
        return temp_str
    else:
        return '-' + temp_str


# In[37]:


# filter swap and equity only, then ignore blank bbgYellowKey

filt = (positions_new['custodianAccount'].apply(lambda x: 'Swap' in x)) & (positions_new['instrumentType'] == 'Equity') & (positions_new['bbgYellowKey'] != '')
swap_positions = positions_new.loc[filt]
swap_positions['broker_code'] = swap_positions['custodian'].apply(convert_broker)

len(positions_new.loc[filt])
#swap_positions.to_csv(r'C:\Users\douglasl\Documents\SBL\Trading\python\swap netting\output\test_filter2.csv')

# only look at position with brokers offer swap netting
swap_net_brokers = ['jpm']
swap_net_positions_raw = swap_positions.loc[swap_positions['broker_code'].apply(lambda x: x in swap_net_brokers)]

# group by swap_net_brokers, then by ticker, get the net position across PMs
swap_net_positions_agg = swap_net_positions_raw.groupby(by=['broker_code','bbgYellowKey'])['quantity'].sum()
swap_net_positions_agg = pd.DataFrame(swap_net_positions_agg).reset_index()

# add 1 column for net long, 1 column for net short
swap_net_positions_agg['net_long'] = swap_net_positions_agg['quantity'].apply(lambda x: x if x>0 else 0)
swap_net_positions_agg['net_short'] = swap_net_positions_agg['quantity'].apply(lambda x: x if x<0 else 0)

# add the converted shs quantity str column
swap_net_positions_agg['net_long_convert'] = swap_net_positions_agg['net_long'].apply(convert_shs_quantity)
swap_net_positions_agg['net_short_convert'] = swap_net_positions_agg['net_short'].apply(convert_shs_quantity)

# final net long and net short table and merge them into final swap netting upload file
net_long = pd.DataFrame()
for ticker in swap_net_positions_agg.loc[swap_net_positions_agg['quantity']>0, 'bbgYellowKey'].unique():
    filt = (swap_net_positions_agg['quantity']>0) & (swap_net_positions_agg['bbgYellowKey'] == ticker)
    temp_table =  swap_net_positions_agg[filt].sort_values(by='quantity', ascending=False).reset_index(drop=True)
    temp_str = ''
    for index in temp_table.index:
        temp_str = temp_str + temp_table.loc[index,'broker_code'] + '_' + temp_table.loc[index,'net_long_convert'] + '/'
    net_long.loc[ticker, 'net_long_str'] = temp_str[0:-1]
    

net_short = pd.DataFrame()
for ticker in swap_net_positions_agg.loc[swap_net_positions_agg['quantity']<0, 'bbgYellowKey'].unique():
    filt = (swap_net_positions_agg['quantity']<0) & (swap_net_positions_agg['bbgYellowKey'] == ticker)
    temp_table =  swap_net_positions_agg[filt].sort_values(by='quantity', ascending=True).reset_index(drop=True)
    temp_str = ''
    for index in temp_table.index:
        temp_str = temp_str + temp_table.loc[index,'broker_code'] + '_' + temp_table.loc[index,'net_short_convert'] + '/'
    net_short.loc[ticker, 'net_short_str'] = temp_str[0:-1]
    
swap_net_positions_final = net_long.merge(net_short, 'outer', left_index=True, right_index=True)
swap_net_positions_final = swap_net_positions_final.reset_index()
swap_net_positions_final.rename(columns={'index':'Instrument'}, inplace=True)
# swap_net_positions_final.to_csv('C:/Users/douglasl/Documents/SBL/Trading/python/swap netting/output/' + today_text + '_swap_netting_upload.csv', index=False)

op_swap_net_table = final_table.merge(swap_net_positions_final,'outer', left_on='Instrument', right_on='Instrument')

filepath = r'P:\All\FinanceTradingMO/'
filename = today_text + '_op_swap_netting_tora_upload.csv'
op_swap_net_table.to_csv(filepath + filename, index=False)


# In[ ]:




