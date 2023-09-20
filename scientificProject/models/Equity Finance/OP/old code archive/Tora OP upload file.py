#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import pdblp
from datetime import datetime, timedelta
from os import listdir
import shutil


pd.set_option('display.max_columns', 132)
pd.set_option('display.max_rows', 200)


# In[2]:


# get the T-1 date (weekday)

def get_yest(date):
    if date.strftime('%w') == '1':
        return date - timedelta(days=3)
    else:
        return date - timedelta(days=1)
    
# # convert UBS spread column
# def UBS_convert_spread(spread):
#     try:
#         if '(' in spread:
#             return float(spread[1:-1])*100     
#     except:
#         return -float(spread)*100
    
# change CG CS to CH in tickers
def CH_convert(ticker):
    try:
        if ('CG' in ticker) | ('CS' in ticker):
            return ticker[0:6] + ' CH'
        else:
            return ticker[0:9]
    except:
        return ''

# convert UBS spread column
def UBS_convert_spread(spread):
    try:
        if '(' in spread:
            return float(spread[1:-1])*100
    except:
        return -float(spread)*100

# In[3]:


#read the brokers' outperformance list

today = datetime.today()
#today = today - timedelta(days=1)  #use when need try backdate
#today = datetime.strptime('2023-02-20', '%Y-%m-%d') #only use to backdate specific date
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


# In[4]:


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
summary_op.to_csv(r'//paghk.local/Polymer/Department/All/FinanceTradingMO/OPnamesummary.csv', index=False)

#-----------------------------------------------


# In[5]:


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
output_filepath = 'P:/Trading/OP Tora upload/'
output_filename = today_text + '_tora_upload.csv'

final_table.rename(columns={'Ticker':'Instrument'}, inplace=True)
# final_table.to_csv(output_filepath + output_filename, index=False)
final_table.to_csv(r'//paghk.local/Polymer/Department/All/FinanceTradingMO/opupload.csv', index=False)
print('done')

# In[ ]:




