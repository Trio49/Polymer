#!/usr/bin/env python
# coding: utf-8

# In[41]:


import pandas as pd
import numpy as np
import pdblp
from datetime import datetime, timedelta
from os import listdir
import shutil
import win32com.client as win32

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
#send OP summary to team
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


