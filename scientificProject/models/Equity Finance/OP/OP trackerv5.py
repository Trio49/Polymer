#!/usr/bin/env python
# coding: utf-8

# In[41]:


import shutil
from datetime import datetime, timedelta
from os import listdir
from typing import Any

import numpy as np
import pandas as pd
import win32com.client as win32
from pandas import Series, DataFrame

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
            return float(spread[1:-1]) * 100
    except:
        return -float(spread) * 100


# change CG CS to CH in tickers
def CH_convert(ticker):
    try:
        if ('CG' in ticker) | ('CS' in ticker):
            return ticker[0:6] + ' CH'
        else:
            return ticker[0:9]
    except:
        return ''


# read the GC spread table
GC_spread = pd.read_csv('P:\All\FinanceTradingMO\outperformance swap/GC_table.csv')
# GS is all 0 as the GC spread is included in the OP spread
# Name the first column as PB
GC_spread.rename(columns={GC_spread.columns[0]:'PB'}, inplace=True)


def attach_GC_spread(row):
    # go thru Broker column to check which PB it is in GC_spread dataframe and check BBG code column to see if it contains CH or C1 OR c2 to determine which spread to add
    # add a new column in OP_booking called GC_spread
    # if the broker is not in GC_spread, then return 0
    if row['Broker'] in GC_spread['PB'].values:
        if 'CH' in row['BBG Code']:
            return GC_spread.loc[GC_spread['PB'] == row['Broker'], 'CH'].values[0]
        elif 'C1' in row['BBG Code'] or 'C2' in row['BBG Code']:
            return GC_spread.loc[GC_spread['PB'] == row['Broker'], 'C1'].values[0]
        else:
            return 0


# In[18]:
a = input('time delta:')
a = int(a)

# read the brokers' outperformance list

today = datetime.today()
today = today - timedelta(days=a)  # use when need try backdate
# today = datetime.strptime('2023-02-10', '%Y-%m-%d') #only use to backdate specific date
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
    UBS_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/UBS/' + yest_text + '/'
    UBS_files_list_dir = listdir(UBS_filepath)
    for file_name in UBS_files_list_dir:
        if yest_text + '.AxesOpportunitiesAPACCN.GRPPOLYM.' in file_name:
            UBS_filename = file_name
            break
    print(UBS_filepath + UBS_filename)

    # get JPM file path and file name
    JPM_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/JPM/' + today_text[
                                                                                             0:6] + '/' + today_text + '/'
    JPM_filename = 'APAC_OP_' + today_text + '.csv'
    print(JPM_filepath + JPM_filename)

    # read each broker's file
    try:
        BAML_OP = pd.read_csv(BAML_filepath + BAML_filename)
        BAML_OP = BAML_OP.loc[0:len(BAML_OP) - 5, ]
        target_path = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/broker_OP_file/BAML/'
        shutil.copyfile(BAML_filepath + BAML_filename, target_path + BAML_filename)
    except:
        print('fail to read BAML OP file')
        BAML_OP = pd.DataFrame()

    try:
        UBS_OP = pd.read_csv(UBS_filepath + UBS_filename)
        target_path = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/broker_OP_file/UBS/'
        shutil.copyfile(UBS_filepath + UBS_filename, target_path + UBS_filename)
    except:
        print('fail to read UBS OP file')
        UBS_OP = pd.DataFrame()

    try:
        GS_OP = pd.read_excel(GS_filepath + GS_filename, header=7)
        target_path = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/broker_OP_file/GS/'
        shutil.copyfile(GS_filepath + GS_filename, target_path + GS_filename)
        # GS_OP = GS_OP.loc[0:len(GS_OP)-2,]
    except:
        print('fail to read GS OP file')
        GS_OP = pd.DataFrame()

    try:
        JPM_OP = pd.read_csv(JPM_filepath + JPM_filename)
        target_path = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/broker_OP_file/JPM/'
        shutil.copyfile(JPM_filepath + JPM_filename, target_path + JPM_filename)
    except:
        print('fail to read JPM OP file')
        JPM_OP = pd.DataFrame()

    # return (BAML_OP, UBS_OP, GS_OP) #old version
    return (BAML_OP, UBS_OP, GS_OP, JPM_OP)  # new version with JPM


# BAML_OP, UBS_OP, GS_OP = get_broker_OP_file(today_text = today_text, yest_text = yest_text) #old version
BAML_OP, UBS_OP, GS_OP, JPM_OP = get_broker_OP_file(today_text=today_text, yest_text=yest_text)  # new version with JPM


# In[19]:


# gather brokers' OP file into one standard format table
# def get_summary_op(BAML_OP, UBS_OP, GS_OP): #old version
def get_summary_op(BAML_OP, UBS_OP, GS_OP, JPM_OP):  # new version with JPM

    if len(BAML_OP) > 0:
        filt = (BAML_OP['Mkt'] == 'CC') | (BAML_OP['Mkt'] == 'CN')
        BAML_OP = BAML_OP[filt]
        BAML_OP = BAML_OP[['Ticker', 'Shares', 'Rate']]
        BAML_OP['Broker'] = 'baml'
        BAML_OP['Rate'] = BAML_OP['Rate'] * -1

    if len(UBS_OP) > 0:
        UBS_OP = UBS_OP[['BB Ticker', 'Synthetic UBS Qty', 'Synthetic Spread']]
        UBS_OP['Broker'] = 'ubs'
        UBS_OP['BB Ticker'] = UBS_OP['BB Ticker'].apply(CH_convert)
        filt = -(UBS_OP['Synthetic Spread'].apply(lambda x:pd.isna(x)) | (UBS_OP['Synthetic Spread'] == '0.50') | (
                UBS_OP['Synthetic Spread'] == 0.5))
        UBS_OP = UBS_OP[filt]
        try:
            UBS_OP['Synthetic Spread'] = UBS_OP['Synthetic Spread'].apply(UBS_convert_spread)
        except:
            pass
        UBS_OP.rename(columns={'BB Ticker':'Ticker', 'Synthetic UBS Qty':'Shares', 'Synthetic Spread':'Rate'},
                      inplace=True)

    if len(GS_OP) > 0:
        filt = (GS_OP['Issue Market'] == 'China') & (GS_OP['Preference'] == 'Transfer In') & (
                GS_OP['Long / Short'] == 'Long')
        GS_OP = GS_OP.loc[filt, ['Bloomberg', 'Synthetic Quantity', 'Synthetic Rate']]
        GS_OP['Broker'] = 'gs'
        GS_OP['Bloomberg'] = GS_OP['Bloomberg'].apply(CH_convert)
        GS_OP['Synthetic Rate'] = GS_OP['Synthetic Rate'] * -100 * 100
        GS_OP.rename(columns={'Bloomberg':'Ticker', 'Synthetic Quantity':'Shares', 'Synthetic Rate':'Rate'},
                     inplace=True)

    if len(JPM_OP) > 0:
        JPM_OP['BBTicker'] = JPM_OP['BBTicker'].apply(CH_convert)
        JPM_OP = JPM_OP[['BBTicker', 'Quantity', 'Rate']]
        JPM_OP['Broker'] = 'jpm'
        JPM_OP['Rate'] = JPM_OP['Rate'] * -1
        JPM_OP.rename(columns={'BBTicker':'Ticker', 'Quantity':'Shares'},
                      inplace=True)  # JPM_OP[['Ticker','Shares', 'Rate']]

    summary_op = pd.DataFrame()
    # summary_op = pd.concat([summary_op, BAML_OP, UBS_OP, GS_OP]) #old version
    summary_op = pd.concat([summary_op, BAML_OP, UBS_OP, GS_OP, JPM_OP])  # new version with JPM
    summary_op.reset_index(inplace=True)
    summary_op.drop(columns='index', inplace=True)
    return summary_op


# summary_op = get_summary_op(BAML_OP=BAML_OP, UBS_OP=UBS_OP, GS_OP=GS_OP) # old version
summary_op = get_summary_op(BAML_OP=BAML_OP, UBS_OP=UBS_OP, GS_OP=GS_OP, JPM_OP=JPM_OP)  # new version with JPM

# flip the value of GC_Spread and save it as a new dataframe called GC_spread_flip
GC_spread_flip = GC_spread.copy()
# flip all the values in GC_spread_flip to negative
GC_spread_flip['C1'] = GC_spread_flip['C1'] * -1
GC_spread_flip['C2'] = GC_spread_flip['C2'] * -1
GC_spread_flip['CH'] = GC_spread_flip['CH'] * -1


# save the final_OP_saving as a csv file
summary_op.to_csv(
    '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/summary_op_avail/' + today_text + '_summary_op_avail.csv',
    index=False)

# In[40]:


# get the Tora daily China names BUY execution (run this after 16:20)

Tora_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/Tora/'
Tora_filename = 'POLYMER_EOD_1615_' + today_text + '.csv'

target_path = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/Tora trade/'

Tora_execution = pd.read_csv(Tora_filepath + Tora_filename)
# remove the row with no trade book
Tora_execution = Tora_execution[~Tora_execution['Trade Book'].isna()]
Tora_execution['Trade Book'] = Tora_execution['Trade Book'].apply(lambda x:'baml' if x == 'ML' else x.lower())
filt = Tora_execution['Broker'] == 'tpairs'
Tora_execution.loc[filt, 'Broker'] = Tora_execution.loc[filt, 'Trade Book']
#     Tora_exeuction = Tora_execution[['Internal Account', 'BBG Code', 'Side', 'Execution Quantity', 'Broker','CCY', 'Syn Flag', 'Notional - TORAFX']]
shutil.copyfile(Tora_filepath + Tora_filename, target_path + Tora_filename)
Tora_exeuction = Tora_execution[
    ['Internal Account', 'BBG Code', 'Side', 'Execution Quantity', 'Broker', 'CCY', 'Syn Flag', 'Notional - TORAFX']]

filt = (Tora_exeuction['CCY'] == 'CNY') | (Tora_exeuction['CCY'] == 'CNH')
Tora_China = Tora_exeuction[filt]
filt = Tora_China['Side'] == 'BUY'
Tora_China = Tora_China[filt]
Tora_China['BBG Code'] = Tora_China['BBG Code'].apply(CH_convert)
Tora_China.reset_index(inplace=True)
Tora_China.drop(columns='index', inplace=True)

# check if today's China buy execution is on the broker outperformance file
Tora_China['OP_applicable'] = False
Tora_China['OP_offer_quantity'] = ''
Tora_China['exceed_offer_size'] = ''
for index in range(len(Tora_China)):
    filt = (summary_op['Ticker'] == Tora_China.loc[index, 'BBG Code']) & (
            summary_op['Broker'] == Tora_China.loc[index, 'Broker'])
    if filt.sum() == 1:
        Tora_China.loc[index, 'OP_applicable'] = True
        Tora_China.loc[index, 'Rate'] = summary_op.loc[filt, 'Rate'].iloc[0]
        Tora_China.loc[index, 'OP_offer_quantity'] = summary_op.loc[filt, 'Shares'].iloc[0]
        if Tora_China.loc[index, 'Execution Quantity'] >= Tora_China.loc[index, 'OP_offer_quantity']:
            Tora_China.loc[index, 'exceed_offer_size'] = True
        else:
            Tora_China.loc[index, 'exceed_offer_size'] = False

if Tora_China['OP_applicable'].sum() > 0:
    Tora_China['1m accrual rev$'] = Tora_China['Notional - TORAFX'] * Tora_China['Rate'] / 10000 / 12
else:
    Tora_China['1m accrual rev$'] = np.NaN

# save daily summary file as csv for dashboard purpose
dashboard_filepath = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/'
dashboard_filename = '_outperformance_trade_summary.csv'

filt = Tora_China['OP_applicable'] == True
final_table = Tora_China[filt]
final_table['TD'] = today.date()
# outperformance summary
final_table[['OP_offer_quantity']] = final_table[['OP_offer_quantity']].applymap(
    lambda x:'{:,}'.format(int(round(float(x)))) if x != 'nan' else x)
# add the 1m accrual revenue total at the bottom, bold the font
# save a copy of final_table to be sent in email
final_table_sent = final_table.copy()
#remove PCM_BORROW from the final_table_sent
filt = final_table_sent['Internal Account'] == 'PCM_BORROW'
final_table_sent = final_table_sent[~filt]
# if final table is not empty
if len(final_table) > 0:
    final_table_sent.loc[final_table_sent.index[-1] + 1, '1m accrual rev$'] = final_table_sent['1m accrual rev$'].sum()
    final_table_sent.loc[final_table_sent.index[-1], '1m accrual rev$'] = final_table_sent.loc[
        final_table_sent.index[-1], '1m accrual rev$'].astype(str)
    final_table_sent.loc[final_table_sent.index[-1], 'BBG Code'] = 'Total'
    final_table_sent.loc[final_table_sent.index[-1], 'BBG Code'] = 'Total'
    final_table_sent.loc[final_table_sent.index[-1], 'OP_offer_quantity'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'exceed_offer_size'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'OP_applicable'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'Rate'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'TD'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'Broker'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'Side'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'Syn Flag'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'Execution Quantity'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'Notional - TORAFX'] = ''
    final_table_sent.loc[final_table_sent.index[-1], 'CCY'] = ''

final_table.to_csv(dashboard_filepath + today_text + dashboard_filename, index=False)
final_table.to_csv(r'P:\All\FinanceTradingMO\outperformance swap\Daily booking/booking_data' + today_text + '.csv',
                   index=False)

#add a summary to analyze the final_table trades vs the final_op_saving to see how much we have saved
#run op_saving first
def op_saving():

    # create a summary_op_matrix which displays each broker's OP for each ticker
    summary_op_matrix = summary_op.pivot_table(index='Ticker', columns='Broker', values='Rate')
    # for summary_op_matrix, replace na to 0
    summary_op_matrix.fillna(0, inplace=True)
    # remove row with index = 'nan'
    summary_op_matrix = summary_op_matrix.reset_index()
    # remove row with empty Ticker
    summary_op_matrix = summary_op_matrix[~summary_op_matrix['Ticker'].isna()]
    final_OP_saving: Series | DataFrame | Any = summary_op_matrix.copy()
    # add a new column to show the max OP rate for each ticker, comparing the values from columns 'baml', 'ubs', 'gs', 'jpm'
    final_OP_saving['max_OP'] = final_OP_saving[['baml', 'ubs', 'gs', 'jpm']].max(axis=1)
    #add a column to show the max_op broker
    final_OP_saving['max_OP_broker'] = final_OP_saving[['baml', 'ubs', 'gs', 'jpm']].idxmax(axis=1)
    # add a column to calculate the average of the OP rate for each ticker but excluding the max_OP rate
    final_OP_saving['avg_OP'] = (final_OP_saving[['baml', 'ubs', 'gs', 'jpm']].sum(axis=1) - final_OP_saving['max_OP']) / 3
    # add a column called Best execution saving spread to show the difference between the max_OP and avg_OP
    final_OP_saving['Best execution saving spread'] = final_OP_saving['max_OP'] - final_OP_saving['avg_OP']
    #save columns of best execution saving spread and max_op broker
    final_OP_saving = final_OP_saving[['Ticker', 'Best execution saving spread', 'max_OP_broker']]

    return final_OP_saving
#run op_saving function and save final_op_saving to dataframe
final_OP_saving=op_saving()
#save final_op_saving to dataframe

#merge the final_table and final_op_saving, left on BBG Code, right on Ticker
final_table_execution=pd.merge(final_table,final_OP_saving,how='left',left_on='BBG Code',right_on='Ticker')
#check if Boker column is the same as max—op_broker column
final_table_execution['Best Execution?']=final_table_execution['Broker']==final_table_execution['max_OP_broker']
#Add a column called best execution saving , calculated by notional times saving spread divided by 10000*365/30
final_table_execution['Best execution saving']=final_table_execution['Notional - TORAFX']*final_table_execution['Best execution saving spread']/(10000*365/30)
#rearrange the columns of final_table_execution
final_table_execution=final_table_execution[['BBG Code',
                                             'Internal Account',
                                             'Execution Quantity',
                                                'Notional - TORAFX',
                                             'Best Execution?',
                                             'Broker',
                                             'max_OP_broker','Rate', 'Best execution saving spread',
                                             '1m accrual rev$','Best execution saving','TD']]
#rename Notional - TORAFX to Notional, rename Internal Account as PM, Rename Best Execution saving to 1m Best execution saving $
final_table_execution.rename(columns={'Notional - TORAFX':'Notional'},inplace=True)
final_table_execution.rename(columns={'Internal Account':'PM'},inplace=True)
final_table_execution.rename(columns={'Best execution saving':'1m Best execution saving $'},inplace=True)
#chnge the best execution saving spread to Best Execution saving spread(bps)
final_table_execution.rename(columns={'Best execution saving spread':'Best execution saving spread(bps)'},inplace=True)
#rename Rate to OP rate
final_table_execution.rename(columns={'Rate':'OP rate(bps)'},inplace=True)
#round 1m accrual rev$ and 1m best execution saving $ to 0 decimal places
final_table_execution['1m accrual rev$']=round(final_table_execution['1m accrual rev$'],0)
final_table_execution['1m Best execution saving $']=round(final_table_execution['1m Best execution saving $'],0)
final_table_execution['Best execution saving spread(bps)']=round(final_table_execution['Best execution saving spread(bps)'],0)

#change the Best execution? column to yes or no as string
final_table_execution['Best Execution?']=np.where(final_table_execution['Best Execution?']==True,'Yes','No')
final_table_execution_sent=final_table_execution.copy()
#add a empty row at the bottom
final_table_execution_sent.loc[final_table_execution_sent.index[-1] + 1] = np.nan

#add a row at the bottom to sum the 1m accrual rev$ and 1m best execution saving $ and Notional
final_table_execution_sent.loc[final_table_execution_sent.index[-1], 'BBG Code'] = 'Total'
final_table_execution_sent.loc[final_table_execution_sent.index[-1], '1m accrual rev$'] = final_table_execution['1m accrual rev$'].sum()
final_table_execution_sent.loc[final_table_execution_sent.index[-1], '1m Best execution saving $'] = final_table_execution['1m Best execution saving $'].sum()
final_table_execution_sent.loc[final_table_execution_sent.index[-1], 'Notional'] = final_table_execution['Notional'].sum()

#remove PCM_BORROW from final table execution

final_table_execution.to_csv(r'P:\All\FinanceTradingMO\outperformance swap\OP tracker\OP execution report/OP_Execution_data' + today_text + '.csv',)

#save final_table_exectuion to csv
final_table_execution_sent.to_csv(r'P:\All\FinanceTradingMO\outperformance swap\OP tracker\OP execution report/OP_Execution_Report' + today_text + '.csv',
                   index=False)

check = input('Send the OP summary? yes/no: ')
if check == 'yes':
    with pd.ExcelWriter(r'P:\All\FinanceTradingMO\outperformance swap\Outperformance.xlsx',
                        engine='xlsxwriter') as writer:
        # for n in pmlist():
        final_table_sent.to_excel(writer, sheet_name="sheet1", index=None)

    html = final_table_sent.to_html(index=False)
    text_file = open("T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\expiry.htm", 'w')
    text_file.write(html)
    text_file.close()

    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)

    mail.to = 'polymerops@polymercapital.com'
    # mail.to = 'zzhang@polymercapital.com'
    mail.cc = 'fundfinance@polymercapital.com;polymertrading@polymercapital.com'

    sub = "Outperformance summary " + today_text
    # sub='PTH report testing'    

    mail.Subject = sub
    mail.Body = 'Message body testing'

    # HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
    HtmlFile = open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\Expiry.htm', 'r')
    source_code = HtmlFile.read()
    # mail.Attachments.Add(r'P:\All\FinanceTradingMO\China deal PTH\China_deal_pth_summary.xlsx')
    HtmlFile.close()
    # html=wb.to_html(classes='table table-striped')
    # html=firm_positions.to_html(classes='table table-striped')
    mail.HTMLBody = source_code
    mail.Send()
# generate booking file
OP_booking = final_table[
    ['Internal Account', 'Broker', 'BBG Code', 'Execution Quantity', 'exceed_offer_size', 'OP_offer_quantity', 'Rate',
     'TD']]
OP_booking['real_op'] = OP_booking['Execution Quantity']
filt = OP_booking['exceed_offer_size'] == True
OP_booking.loc[filt, 'real_op'] = OP_booking.loc[filt, 'OP_offer_quantity']
OP_booking['Rate'] = OP_booking['Rate'] / 100
OP_booking['Broker'] = OP_booking['Broker'].apply(lambda x:x.upper())
OP_booking['TD_str'] = OP_booking['TD'].apply(lambda x:'TD ' + x.strftime('%Y/%m/%d'))
OP_booking = OP_booking[['Internal Account', 'Broker', 'BBG Code', 'real_op', 'Rate', 'TD_str']]
# add a new procedure to add extra GC spread back to the OP_booking file based on the broker and if it contains CH or C1/C2

# call attach_GC_spread function to add GC_spread column
OP_booking['GC_spread'] = OP_booking.apply(attach_GC_spread, axis=1)
# Add the GC_spread column to the Rate column to generate a new column called final_rate
OP_booking['final_rate'] = OP_booking['Rate'] + OP_booking['GC_spread'] * 0.01
# rearrange the columns
OP_booking = OP_booking[['Internal Account', 'Broker', 'BBG Code', 'real_op', 'final_rate', 'TD_str']]
#
OP_booking.to_csv(
    '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/Daily booking/' + today_text + '_OP_booking_file.csv',
    index=False)
OP_booking.to_csv(
    '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/Daily booking/' + today_text + '_OP_booking_file.csv',
    index=False)

# # In[46]:


# build outperformance internal tracker
# first in first out

# get date range
import datetime

# only today
start = today
end = today

# # # if want to rerun back date
# start = datetime.datetime.strptime("19-09-2022", "%d-%m-%Y")
# end = datetime.datetime.strptime("10-02-2023", "%d-%m-%Y")

# get date range
date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1)]

# get old position from yesterday active all position file
active_op_pos = pd.read_csv(
    '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/' + yest_text + '_active_all_position.csv')
# active_op_pos = pd.read_csv('//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/'+'20220916'+'_active_all_position.csv') #for backdate
active_op_pos['dtdates'] = active_op_pos['TD'].apply(lambda x: datetime.datetime.strptime(x, '%Y/%m/%d'))
# active_op_pos['dtdates'] = active_op_pos['TD'].apply(lambda x:datetime.datetime.strptime(x, '%m/%d/%yy'))
active_op_pos['TD'] = active_op_pos['dtdates'].apply(lambda x:x.strftime('%Y/%m/%d'))

# define the column list from Tora execution
column_list_1 = ['Internal Account', 'BBG Code', 'Execution Quantity', 'Broker', 'Notional - TORAFX', 'OP_applicable',
                 'OP_offer_quantity', 'exceed_offer_size', 'Rate', 'real_op_quantity', 'active_quantity', 'TD',
                 'dtdates',
                 'active_op']

column_list_2 = ['Internal Account', 'BBG Code', 'Execution Quantity', 'Broker', 'Notional - TORAFX', 'OP_applicable',
                 'OP_offer_quantity', 'exceed_offer_size', 'active_quantity', 'TD', 'dtdates']

# loop through the date range
for date in date_generated:

    if date.strftime('%w') in ['0', '6']:
        continue

    # get the correct file date
    today = date
    today_text = today.strftime('%Y%m%d')
    yest = get_yest(today)
    yest_text = yest.strftime('%Y%m%d')

    print(today_text)
    print(yest_text)

    try:
        summary_op = pd.read_csv(
            '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/summary_op_avail/' + today_text + '_summary_op_avail.csv')
    except:
        continue

    # get Tora daily China trade execution history (BUY/SELL)
    Tora_filepath = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/Tora trade/'
    Tora_filename = 'POLYMER_EOD_1615_' + today_text + '.csv'

    Tora_execution = pd.read_csv(Tora_filepath + Tora_filename)
    Tora_exeuction = Tora_execution[
        ['Internal Account', 'BBG Code', 'Side', 'Execution Quantity', 'Broker', 'CCY', 'Syn Flag',
         'Notional - TORAFX']]

    op_broker = ['baml', 'gs', 'ubs', 'jpm']
    filt = (Tora_exeuction['CCY'] == 'CNY') | (Tora_exeuction['CCY'] == 'CNH')
    filt = filt & (Tora_exeuction['Broker'].apply(lambda x:x in op_broker))
    Tora_China = Tora_exeuction[filt]
    Tora_China['BBG Code'] = Tora_China['BBG Code'].apply(CH_convert)
    filt = Tora_China['Side'] == 'BUY'
    Tora_China_buy = Tora_China[filt]
    filt = Tora_China['Side'] == 'SELL'
    Tora_China_sell = Tora_China[filt]
    Tora_China_buy.reset_index(drop=True, inplace=True)
    Tora_China_sell.reset_index(drop=True, inplace=True)

    # check if today's China buy execution is on the broker outperformance file
    Tora_China_buy['OP_applicable'] = False
    Tora_China_buy['OP_offer_quantity'] = ''
    Tora_China_buy['exceed_offer_size'] = ''
    # print('Tora_China_buy table len is', len(Tora_China_buy))
    for index in range(len(Tora_China_buy)):
        filt = (summary_op['Ticker'] == Tora_China_buy.loc[index, 'BBG Code']) & (
                    summary_op['Broker'] == Tora_China_buy.loc[index, 'Broker'])
        if filt.sum() == 1:
            Tora_China_buy.loc[index, 'OP_applicable'] = True
            Tora_China_buy.loc[index, 'Rate'] = summary_op.loc[filt, 'Rate'].iloc[0]
            Tora_China_buy.loc[index, 'OP_offer_quantity'] = summary_op.loc[filt, 'Shares'].iloc[0]
            if Tora_China_buy.loc[index, 'Execution Quantity'] >= Tora_China_buy.loc[index, 'OP_offer_quantity']:
                Tora_China_buy.loc[index, 'exceed_offer_size'] = True
                Tora_China_buy.loc[index, 'real_op_quantity'] = Tora_China_buy.loc[index, 'OP_offer_quantity']
            else:
                Tora_China_buy.loc[index, 'exceed_offer_size'] = False
                Tora_China_buy.loc[index, 'real_op_quantity'] = Tora_China_buy.loc[index, 'Execution Quantity']
            Tora_China_buy.loc[index, 'active_op'] = Tora_China_buy.loc[index, 'real_op_quantity']

        Tora_China_buy.loc[index, 'active_quantity'] = Tora_China_buy.loc[index, 'Execution Quantity']

    if Tora_China_buy['OP_applicable'].sum() > 0:
        Tora_China_buy['1m accrual rev$'] = Tora_China_buy['Notional - TORAFX'] * Tora_China_buy['Rate'] / 10000 / 12
    else:
        Tora_China_buy['1m accrual rev$'] = np.NaN
    # filt = Tora_China_buy['OP_applicable'] == True
    # Tora_China_buy = Tora_China_buy[filt]
    Tora_China_buy['TD'] = today.strftime('%Y/%m/%d')
    Tora_China_buy['dtdates'] = Tora_China_buy['TD'].apply(lambda x:datetime.datetime.strptime(x, '%Y/%m/%d'))

    # add new OP eligible trades to the active position dataframe
    # active_op_pos = active_op_pos.append(Tora_China_buy, ignore_index=True)
    if len(Tora_China_buy) > 0:
        if 'active_op' in Tora_China_buy.columns:
            active_op_pos = active_op_pos.append(Tora_China_buy[column_list_1], ignore_index=True)
        else:
            active_op_pos = active_op_pos.append(Tora_China_buy[column_list_2], ignore_index=True)

    # find sell trades that have active OP position
    unwind_trades = pd.DataFrame()
    for index in range(len(Tora_China_sell)):
        filt = (active_op_pos['BBG Code'] == Tora_China_sell.loc[index, 'BBG Code']) & (
                    active_op_pos['Broker'] == Tora_China_sell.loc[index, 'Broker']) & (
                           active_op_pos['Internal Account'] == Tora_China_sell.loc[index, 'Internal Account'])
        if filt.sum() == 0:
            continue
        else:
            ticker_active_pos = active_op_pos[filt].sort_values(by='dtdates', ignore_index=True)
            active_op_pos = active_op_pos.drop(index=active_op_pos.index[filt])
            active_op_pos.reset_index(drop=True, inplace=True)
            print(ticker_active_pos)
            for pos_index in ticker_active_pos.index:
                if ticker_active_pos.loc[pos_index, 'active_quantity'] >= Tora_China_sell.loc[
                    index, 'Execution Quantity']:
                    temp_unwind = pd.DataFrame(ticker_active_pos.loc[pos_index]).T
                    temp_unwind['unwind_quantity'] = Tora_China_sell.loc[index, 'Execution Quantity']
                    unwind_trades = unwind_trades.append(temp_unwind, ignore_index=True)
                    ticker_active_pos.loc[pos_index, 'active_quantity'] -= Tora_China_sell.loc[
                        index, 'Execution Quantity']
                    filt = ticker_active_pos['active_quantity'] != 0
                    active_op_pos = active_op_pos.append(ticker_active_pos[filt], ignore_index=True)
                    break
                else:
                    temp_unwind = pd.DataFrame(ticker_active_pos.loc[pos_index]).T
                    temp_unwind['unwind_quantity'] = ticker_active_pos.loc[pos_index, 'active_quantity']
                    unwind_trades = unwind_trades.append(temp_unwind, ignore_index=True)
                    Tora_China_sell.loc[index, 'Execution Quantity'] -= ticker_active_pos.loc[
                        pos_index, 'active_quantity']
                    ticker_active_pos.loc[pos_index, 'active_quantity'] = 0

                if pos_index == ticker_active_pos.index[-1]:
                    filt = ticker_active_pos['active_quantity'] != 0
                    active_op_pos = active_op_pos.append(ticker_active_pos[filt], ignore_index=True)

    # save unwind trade

    if len(unwind_trades) > 0:
        for index in unwind_trades.index:
            new_active_quantity = unwind_trades.loc[index, 'active_quantity'] - unwind_trades.loc[
                index, 'unwind_quantity']
            new_active_op = min(new_active_quantity, unwind_trades.loc[index, 'real_op_quantity'])
            unwind_trades.loc[index, 'unwind_op_quantity'] = unwind_trades.loc[index, 'active_op'] - new_active_op

        unwind_trades['unwind_TD'] = today.strftime('%Y/%m/%d')
        filt = unwind_trades['OP_applicable'] == True

        # unwind_trades['unwind_op_quantity'] = [min(unwind_trades.loc[index,'unwind_quantity'], unwind_trades.loc[index,'active_op']) for index in unwind_trades.index]
        # unwind_trades['unwind_TD'] = today.strftime('%Y/%m/%d')
        # filt = unwind_trades['OP_applicable'] == True
        # unwind_trades=unwind_trades[['Internal Account', 'BBG Code',  'Broker',
        #    'Notional - TORAFX', 'OP_applicable', 'OP_offer_quantity',
        #    'unwind_op_quantity']]
        unwind_trades[filt].to_csv(
            '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/Daily booking/' + today_text + '_unwind_OP_trades.csv',
            index=False)
    else:
        # save a blank csv file even no unwind trades
        unwind_trades.to_csv(
            '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/Daily booking/' + today_text + '_unwind_OP_trades.csv',
            index=False)
        unwind_trades = unwind_trades[['Internal Account', 'BBG Code', 'Broker',
                                       'Notional - TORAFX', 'OP_applicable', 'OP_offer_quantity',
                                       'unwind_op_quantity']]
    unwind_trades.to_csv(
        '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/' + today_text + '_unwind_all_trades.csv',
        index=False)

    # save active position
    # active_op_pos['active_op'] = active_op_pos['real_op_quantity']-(active_op_pos['Execution Quantity']-active_op_pos['active_quantity']) #conversative approach (deduct OP first)
    # active_op_pos['active_op'] = active_op_pos['active_op'].apply(lambda x: max(x,0)) #conversative approach (deduct OP first)
    active_op_pos['active_op'] = active_op_pos[['real_op_quantity', 'active_quantity']].min(
        axis=1)  # aggressive approach (deduct OP the latest)
    active_op_pos['active_op_daily_pnl'] = active_op_pos['Notional - TORAFX'] / active_op_pos['Execution Quantity'] * \
                                           active_op_pos['active_op'] * active_op_pos['Rate'] / 10000 / 360
    daily_active_op_pos = active_op_pos.copy()
    daily_active_op_pos['business_date'] = today.strftime('%Y/%m/%d')
    daily_active_op_pos.to_csv(
        '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/' + today_text + '_active_all_position.csv',
        index=False)
    filt = daily_active_op_pos['OP_applicable'] == True
    daily_active_op_pos[filt].to_csv(
        '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/OP tracker/' + today_text + '_active_OP_position.csv',
        index=False)

# In[ ]:


#
