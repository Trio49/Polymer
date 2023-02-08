#!/usr/bin/env python
# coding: utf-8

# In[463]:


import pandas as pd
import numpy as np
import pdblp
from datetime import datetime, timedelta
from os import listdir
import win32com.client as win32
from IPython.display import HTML

pd.set_option('display.max_columns', 132)



# get the T-1 date (weekday)

def get_yest(date):
    if date.strftime('%w') == '1':
        return date - timedelta(days=3)
    else:
        return date - timedelta(days=1)



#read the brokers' outperformance list

today = datetime.today()
# today = datetime.today() - timedelta(days=2)  #use when need try backdate
today_text = today.strftime('%Y%m%d')
yest = get_yest(today)
yest_text = yest.strftime('%Y%m%d')

print(today_text)
print(yest_text)

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

# read each broker's file
BAML_OP = pd.read_csv(BAML_filepath + BAML_filename)
BAML_OP = BAML_OP.loc[0:len(BAML_OP)-5,]

UBS_OP = pd.read_csv(UBS_filepath + UBS_filename)

GS_OP = pd.read_excel(GS_filepath + GS_filename, header=7)
#GS_OP = GS_OP.loc[0:len(GS_OP)-2,]



# convert UBS spread column
def UBS_convert_spread(spread):
    if '(' in spread:
        return float(spread[1:-1])*100
    else:
        return float(spread)*100

# change CG CS to CH in tickers
def CH_convert(ticker):
    try:
        if ('CG' in ticker) | ('CS' in ticker):
            return ticker[0:6] + ' CH'
        else:
            return ticker[0:9]
    except:
        return ''
# gather brokers' OP file into one standard format table




# BAML_OP = BAML_OP[['Ticker','Shares', 'Rate']]
# BAML_OP['Broker'] = 'baml'
# BAML_OP['Rate'] = BAML_OP['Rate']*-1

# UBS_OP = UBS_OP[['BB Ticker', 'Synthetic UBS Qty', 'Synthetic Spread']]
# UBS_OP['Broker'] = 'ubs'
# UBS_OP['BB Ticker'] = UBS_OP['BB Ticker'].apply(CH_convert)
# filt = -(UBS_OP['Synthetic Spread'] == '0.50')
# UBS_OP = UBS_OP[filt]
# UBS_OP['Synthetic Spread'] = UBS_OP['Synthetic Spread'].apply(UBS_convert_spread)
# UBS_OP.rename(columns={'BB Ticker': 'Ticker', 'Synthetic UBS Qty':'Shares', 'Synthetic Spread':'Rate'}, inplace=True)

# filt = (GS_OP['Issue Market'] == 'China') & (GS_OP['Preference'] == 'Transfer In') & (GS_OP['Long / Short'] == 'Long')
# GS_OP = GS_OP.loc[filt, ['Bloomberg', 'Synthetic Quantity', 'Synthetic Rate']]
# GS_OP['Broker'] = 'gs'
# GS_OP['Bloomberg'] = GS_OP['Bloomberg'].apply(CH_convert)
# GS_OP['Synthetic Rate'] = GS_OP['Synthetic Rate']*-100*100
# GS_OP.rename(columns={'Bloomberg': 'Ticker', 'Synthetic Quantity':'Shares', 'Synthetic Rate':'Rate'}, inplace=True)

# summary_op = pd.DataFrame()
# summary_op = pd.concat([summary_op, BAML_OP, UBS_OP, GS_OP])
# summary_op.reset_index(inplace=True)
# summary_op.drop(columns='index', inplace=True)

# gather brokers' OP file into one standard format table



filt = (BAML_OP['Mkt'] == 'CC') | (BAML_OP['Mkt'] == 'CN')
BAML_OP = BAML_OP[filt]
BAML_OP = BAML_OP[['Ticker','Shares', 'Rate']]
BAML_OP['Broker'] = 'baml'
BAML_OP['Rate'] = BAML_OP['Rate']*-1



UBS_OP = UBS_OP[['BB Ticker', 'Synthetic UBS Qty', 'Synthetic Spread']]
UBS_OP['Broker'] = 'ubs'
UBS_OP['BB Ticker'] = UBS_OP['BB Ticker'].apply(CH_convert)
filt = -((UBS_OP['Synthetic Spread'] == '0.50') | (UBS_OP['Synthetic Spread'] == 0.5))
UBS_OP = UBS_OP[filt]
try:
    UBS_OP['Synthetic Spread'] = UBS_OP['Synthetic Spread'].apply(UBS_convert_spread)
except:
    pass
UBS_OP.rename(columns={'BB Ticker': 'Ticker', 'Synthetic UBS Qty':'Shares', 'Synthetic Spread':'Rate'}, inplace=True)



filt = (GS_OP['Issue Market'] == 'China') & (GS_OP['Preference'] == 'Transfer In') & (GS_OP['Long / Short'] == 'Long')
GS_OP = GS_OP.loc[filt, ['Bloomberg', 'Synthetic Quantity', 'Synthetic Rate']]
GS_OP['Broker'] = 'gs'
GS_OP['Bloomberg'] = GS_OP['Bloomberg'].apply(CH_convert)
GS_OP['Synthetic Rate'] = GS_OP['Synthetic Rate']*-100*100
GS_OP.rename(columns={'Bloomberg': 'Ticker', 'Synthetic Quantity':'Shares', 'Synthetic Rate':'Rate'}, inplace=True)



summary_op = pd.DataFrame()
summary_op = pd.concat([summary_op, BAML_OP, UBS_OP, GS_OP])
summary_op.reset_index(inplace=True)
summary_op.drop(columns='index', inplace=True)


# get the Tora daily China names BUY execution (run this after 16:20)

Tora_filepath = '//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer_reports/Tora/'
Tora_filename = 'POLYMER_EOD_1615_' + today_text + '.csv'

Tora_execution = pd.read_csv(Tora_filepath + Tora_filename)
Tora_exeuction = Tora_execution[['Internal Account', 'BBG Code', 'Side', 'Execution Quantity', 'Broker','CCY', 'Syn Flag', 'Notional - TORAFX']]

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
    filt = (summary_op['Ticker'] == Tora_China.loc[index,'BBG Code']) & (summary_op['Broker'] == Tora_China.loc[index,'Broker'])
    if filt.sum() == 1:
        Tora_China.loc[index, 'OP_applicable'] = True
        Tora_China.loc[index, 'Rate'] = summary_op.loc[filt, 'Rate'].iloc[0]
        Tora_China.loc[index, 'OP_offer_quantity'] = summary_op.loc[filt, 'Shares'].iloc[0]
        if Tora_China.loc[index,'Execution Quantity'] >= Tora_China.loc[index, 'OP_offer_quantity']:
            Tora_China.loc[index, 'exceed_offer_size'] = True
        else:
            Tora_China.loc[index, 'exceed_offer_size'] = False

 

if Tora_China['OP_applicable'].sum() > 0:
    Tora_China['1m accrual rev$'] = Tora_China['Notional - TORAFX']* Tora_China['Rate']/10000/12
else:
    Tora_China['1m accrual rev$'] = np.NaN
# get today's trade that applicable for outperformance
output_filepath = 'P:\Operations\Polymer - Middle Office\Borrow analysis\outperformance/'
output_filename = today_text + '_outperformance_swap_trade_summary.xlsx'


#save file with raw table
writer = pd.ExcelWriter(output_filepath + output_filename, engine = 'xlsxwriter')
Tora_China.to_excel(writer, sheet_name = 'all_china_buy')
summary_op.to_excel(writer, sheet_name = 'op_file')
filt = Tora_China['OP_applicable'] == True
Tora_China[filt].to_excel(writer, sheet_name = 'op_eligible_trades')

writer.save()    

# Save and release handle
writer.close()
writer.handles = None


#save daily summary file as csv for dashboard purpose
dashboard_filepath = '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/'
dashboard_filename = '_outperformance_trade_summary.csv'



filt = Tora_China['OP_applicable'] == True
final_table = Tora_China[filt]
final_table['TD'] = today.date()
final_table.to_csv(dashboard_filepath + today_text + dashboard_filename, index=False)

final_table.to_excel(r"P:\All\FinanceTradingMO\outperformance swap\Outperformance.xlsx",index=False)
with pd.ExcelWriter(r"P:\All\FinanceTradingMO\outperformance swap\Outperformance.xlsx",engine='xlsxwriter') as writer:

    html = final_table.to_html(index=False)
    text_file = open("T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\CA.htm",'w')
    text_file.write(html)
    text_file.close()

    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
    mail.to = 'zzhang@polymercapital.com'
    sub = "Outperformance " + today.strftime('%Y%m%d')
    
    
        
        
    mail.Subject = sub
    mail.Body ='Message body testing'
        
        #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
    HtmlFile=open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\CA.htm', 'r')
    source_code = HtmlFile.read() 
    HtmlFile.close()
        #html=wb.to_html(classes='table table-striped')
        #html=firm_positions.to_html(classes='table table-striped')
    mail.HTMLBody= source_code
    mail.Send() 
