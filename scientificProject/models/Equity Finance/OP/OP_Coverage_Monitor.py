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
summary_op = get_summary_op(BAML_OP=BAML_OP, UBS_OP=UBS_OP, GS_OP=GS_OP, JPM_OP=JPM_OP)
#add " Equity" to ticker
summary_op['Ticker'] = summary_op['Ticker'] + ' Equity'

#reading the firm level position file
# import pytz
# pd.set_option('display.max_columns', 20)
# today = datetime.now(pytz.timezone('Asia/Shanghai'))
# # =============================================================================
# # =============================================================================
# # today=today.date()
# a=input("time delta:")
# a=int(a)
# today = today.date() - timedelta(days=a)
# today = today.strftime('%Y%m%d')

def firm_positions():
    # read the FIRM FLAT positions from some place!
    # firm_positions_file_path="//paghk.local/shares/Enfusion/Polymer/"+today
    firm_positions_file_path = r"P:\All\Enfusion\Polymer/"+today_text+"/"
    firm_positions_list_dir = listdir(firm_positions_file_path)

    firm_positions_list_dir = firm_positions_list_dir[::-1]
    for file_name in firm_positions_list_dir:
        if "REPORT_Polymer_Firm_Flat_All_Pos" in file_name:
            firm_positions_file_path = firm_positions_file_path + file_name
            print(firm_positions_file_path)
            break

    ####tyin
    firm_positions = pd.read_csv(firm_positions_file_path)
    firm_positions_Aswap = firm_positions[firm_positions['Currency'].isin(['CNY', 'CNH'])]
    firm_positions_Aswap = firm_positions_Aswap[firm_positions_Aswap['Position'] > 0]
    # choose the columns
    firm_positions_Aswap = firm_positions_Aswap[['BB Yellow Key', 'Position', 'CustodianShortName']]
    total_firm = firm_positions_Aswap.groupby('BB Yellow Key').aggregate({'Position':np.sum})
    # rename Position to total holdings
    total_firm.rename(columns={'Position':'total holdings'}, inplace=True)
    total_firm.reset_index(inplace=True)

    return(firm_positions, firm_positions_Aswap, total_firm)


total_firm: Series | DataFrame | Any
firm_positions,firm_positions_Aswap,total_firm=firm_positions()
    #only keep the rows with currency as CNY or CNH

def OP_positions():
    # read the FIRM FLAT positions from some place!
    # firm_positions_file_path="//paghk.local/shares/Enfusion/Polymer/"+today
    import pandas as pd
    firm_positions_file_path = r"P:\All\Enfusion\Polymer/"+today_text+"/"
    firm_positions_list_dir = listdir(firm_positions_file_path)

    firm_positions_list_dir = firm_positions_list_dir[::-1]
    for file_name in firm_positions_list_dir:
        if "Polymer_OP position" in file_name:
            firm_positions_file_path = firm_positions_file_path + file_name
            print(firm_positions_file_path)
            break

    ####tyin
    OP_positions = pd.read_csv(firm_positions_file_path)

    return(OP_positions)
OP_positions=OP_positions()
OP_positions=OP_positions[['Underlying BB Yellow Key','Active Coupon Rate','Position','Underlying $ Delta','CustodianShortName']]
OP_no=OP_positions[['Underlying BB Yellow Key','CustodianShortName','Position']]
OP_no=OP_no.groupby(['Underlying BB Yellow Key','CustodianShortName']).agg({'Position':np.sum})
OP_no=OP_no.reset_index()
#cross check OP_positions and total firm, check for the same ticker, how many shares are there in OP_positions and how many shares are there in total firm,
# create a new column called Total PB shares in OP_positions, to show the total shares
OP_positions['Total PB shares']=''
for i in range(len(OP_positions)):
    for j in range(len(total_firm)):
        if OP_positions['Underlying BB Yellow Key'][i]==total_firm['BB Yellow Key'][j]:
            OP_positions['Total PB shares'][i]=total_firm['total holdings'][j]
            break
        else:
            pass
#remove Total PB shares empty
#save the OP % of total PB shares >1 to a new dataframe called "OP_positions_over_1"
#save the OP % of total PB shares <0 to a new dataframe called "OP_positions_under_0"


OP_positions=OP_positions[OP_positions['Total PB shares']!='']
#calculate the percentage of OP positions in total firm
OP_positions['OP % of total PB shares']=OP_positions['Position']/OP_positions['Total PB shares']
#check if there is any OP % of total PB shares >1 or <0
def incorrect_op_check():

    OP_positions_over_1=OP_positions[OP_positions['OP % of total PB shares']>1]
    OP_positions_under_0=OP_positions[OP_positions['OP % of total PB shares']<0]
    #if the size of over_1 and under_0 >0, then print out the ticker and the size
    if len(OP_positions_over_1)>0:
        print('OP positions over 100% of total PB shares:')
        print(OP_positions_over_1[['Underlying BB Yellow Key','OP % of total PB shares']])
    else:
        print('no OP positions overbooked')
    if len(OP_positions_under_0)>0:
        print('OP positions under 0% of total PB shares:')
        print(OP_positions_under_0[['Underlying BB Yellow Key','OP % of total PB shares']])
    else:
        print('no existing OP name is actually short positions')
incorrect_op_check()

#keep the data with OP % of total PB shares between 0 and 1
OP_positions=OP_positions[(OP_positions['OP % of total PB shares']<=1)&(OP_positions['OP % of total PB shares']>=0)]
OP_positions['OP % of total PB shares']=OP_positions['OP % of total PB shares'].apply(lambda x: round(x,2))
#sort by OP % of total PB shares
ExistingOP_coverage_report=OP_positions.sort_values(by='OP % of total PB shares',ascending=False)
ExistingOP_coverage_report.to_excel(r'P:\All\FinanceTradingMO\outperformance swap\ExistingOP_coverage_report.xlsx',index=False)
#add % to Active Coupon Rate and OP % of total PB shares
#Active Coupon Rate times 100 then apply the % sign
OP_positions['Active Coupon Rate']=OP_positions['Active Coupon Rate'].apply(lambda x: x*100)
OP_positions['Active Coupon Rate']=OP_positions['Active Coupon Rate'].apply(lambda x: round(x,2))

# def firm_op_utilization:
#groupby the firm_positions_Aswap by BB Yellow Key and CustodianShortName
firm_positions_Aswap_group=firm_positions_Aswap.groupby(['BB Yellow Key','CustodianShortName']).agg({'Position':np.sum})
firm_positions_Aswap_group=firm_positions_Aswap_group.reset_index()
#rename OP_no CustodianShortName as OP_broker
OP_no.rename(columns={'CustodianShortName':'OP_broker'},inplace=True)
#merge the firm_positions_Aswap_group with OP_positions if the BB Yellow Key and CustodianShortName are the same,
firm_positions_Aswap_group_op=firm_positions_Aswap_group.merge(OP_no,how='left',left_on=['BB Yellow Key','CustodianShortName'],right_on=['Underlying BB Yellow Key','OP_broker'])
#rename Position_x as firm_position, Position_y as OPed_position
firm_positions_Aswap_group_op.rename(columns={'Position_x':'firm_position','Position_y':'OPed_position'},inplace=True)
firm_positions_Aswap_group_op['Not yet OP quantity']=firm_positions_Aswap_group_op['firm_position']-firm_positions_Aswap_group_op['OPed_position']
#for columns of Not yet OP quantity, if the value is empty, then fill it with the value of firm_position
firm_positions_Aswap_group_op['Not yet OP quantity']=firm_positions_Aswap_group_op['Not yet OP quantity'].fillna(firm_positions_Aswap_group_op['firm_position'])
Aswap_group_unOP=firm_positions_Aswap_group_op[['BB Yellow Key','CustodianShortName','OPed_position','Not yet OP quantity']]
#save summary_op as a dictionary, per ticker
summary_op_dict = summary_op.groupby('Ticker').apply(lambda x: x.to_dict(orient='records')).to_dict()
#only keep the highest rate for each ticker and rename the dataframe as summary_op_best
summary_op_best=summary_op.groupby('Ticker').apply(lambda x: x.sort_values(by='Rate',ascending=False)).reset_index(drop=True)
summary_op_best=summary_op_best.groupby('Ticker').head(1)

#find out for each Ticker in summary_op, which broker has the highest rate
summary_op['Best Offer']=''
summary_op['Best Offer Quantity']=''
summary_op['Best Offer Rate']=''
for i in range(len(summary_op)):
    summary_op['Best Offer'][i]=summary_op_dict[summary_op['Ticker'][i]][0]['Broker']
    summary_op['Best Offer Quantity'][i]=summary_op_dict[summary_op['Ticker'][i]][0]['Shares']
    summary_op['Best Offer Rate'][i]=summary_op_dict[summary_op['Ticker'][i]][0]['Rate']

#check firm_positions_Aswap_group_op against summary_OP, if the BB Yellow key matches, add a new column to print OP summary per summary_op_dict
Aswap_group_unOP['Potential OP summary']=''
Aswap_group_unOP['Best Offer']=''
for i in range(len(Aswap_group_unOP)):
    for j in range(len(summary_op)):
        #check if the ticker is OP available
        # if available , go thru the summary_op,
        # check 1. If the Shares in summary_op is greater than Not yet OP quantity, if yes, then
        # check 2. for the same ticker in summary_op, check which broker has the highest rate
        if Aswap_group_unOP['BB Yellow Key'][i]==summary_op['Ticker'][j]:
            Aswap_group_unOP










