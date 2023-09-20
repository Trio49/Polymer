import datetime as dt
from datetime import datetime, timedelta
from os import listdir

import numpy as np
import pandas as pd
import pytz
import pdblp
from xbbg import blp

today = datetime.now(pytz.timezone('Asia/Shanghai'))
# today=today.date()
# define backdate

a = 3
today1 = today.date() - timedelta(days=a)

# define the directory
path = r'P://All\Enfusion\Polymer_reports\Hazeltree'
# month=dt.datetime.today().strftime('%m')
month = today1.strftime("%m")
# =============================================================================
# month=dt.datetime.today().strftime('%b')
# =============================================================================
day = dt.datetime.today() - dt.timedelta(days=a)
day = day.strftime('%d')
# path3 = path+"Dec"+"20/"+"Dec"+day+"/"+"!Margin Summary/"
# path3 = path+month+"22/"+month+day+"/"+"!Margin Summary/"
path3 = path + "/" + "2023" + month + "/" + month + day + "/"
# path3=path+"/"+"2023"+"03"+"/"+"03"+day+"/"
# =============================================================================
files = listdir(path3)
# define a program to load the currency based on ISIN

# =============================================================================
print(files)
files = [x for x in files if x.startswith('PAG_POLY FINANCING REPORT')]
file = sorted(files)[-1]
# define a blacklist of BB tickers cant be consumed by enfusion
cash_black_list = ['AU000000YAL0',
                   'JP3122550001',
                   'CA16890P1036',
                   'US98850P1093',
                   'GB0005405286',
                   'BMG423131256',
                   'BMG507361001',
                   'DE0007010803',
                   'DE0005545503',
                   'AT0000743059',
                   'DE000A0JBPG2',
                   'FR0000120271',
                   'SG1EA8000000',
                   'FI0009003230',
                   'SE0003366871',
                   'ZAE000058723',
                   'KYG9884T1013',
                   'DE0008232125',
                   'ZAE000058723',
                   'FR0004024222',
                   'FI0009003230',
                   'PLPKN0000018',
                   'SE0003366871',
                   'NO0003078107',
                   'KYG521321268',
                   'NO0005806802',
                   'DK0060952240',
                   'BMG2624N1535',
                   'DE0005937007',
                   'CA29446Y5020',
                   'SE0008294953',
                   'DE0005937007',
                   'CA29446Y5020',
                   'JP3659350007',
                   'KYG9390R1020',
                   '0',
                   'KYG981491007',
                   'GB0004082847',
                   'KYG7956A1177',
                   'BMG8800F1876',
                   'TW0006187008',
                   'CNR100000140',
                   'NZAIRE0010S3',
                   'GB0007099541',
                   'SGXC50067435',
                   'HK0000876562',
                   'NZAIRE0001S2',
                   'US2825792003',
                   'HK0000916640',
                   'IT0005383291',
                   'CA44955L1067',
                   'BMG7997W1029'
                   ]
swap_black_list = ['MEL NZ Equity',
                   '253450 KQ Equity',
                   'PLLDA AU Equity',
                   '002013 CH Equity',
                   'PTRCY US Equity',
                   '688008 CH Equity',
                   'CT SP Equity',
                   '091990 KS Equity',
                   'MAKRO-R TB Equity',
                   '091990 KQ Equity',
                   'AIRA MK Equity'


                   ]
Tony_black_list = ['601326 C1 Equity',
'000630 CH Equity',
'600236 C1 Equity',
'600027 C1 Equity',
'600535 CH Equity',
'600717 C1 Equity',
'000807 C2 Equity',
'601615 CH Equity',
'603328 CH Equity',
'000630 CH Equity',
'000630 CH Equity',
'600027 C1 Equity',
'600535 CH Equity',
'300319 C2 Equity',
'301039 C2 Equity',
'002407 C2 Equity',
'002124 C2 Equity',
'601615 CH Equity',
'603067 CH Equity',
'300627 CH Equity',
'002439 CH Equity',
'600882 C1 Equity',
'300068 C2 Equity',
'603612 C1 Equity',
'002407 C2 Equity',
'688526 CH Equity',
'002636 CH Equity',
'600340 C1 Equity',
'688700 CH Equity',
'300068 C2 Equity',
'002541 C2 Equity',
'000513 CH Equity',
'688388 C1 Equity',
'600258 C1 Equity',
'300298 CH Equity',
'688188 CH Equity',
'688700 CH Equity',
  '600909 c1 Equity',
               '688389 ch equity'    ]
#combine swap blacklist and tony blacklist
swap_black_list=swap_black_list+Tony_black_list
#Create a dictionary to replace Bberg ticker to a readable name
bb_ticker_replace_list = {
    '166090 KQ Equity': '166090 KS Equity',}
# poly_fin=pd.read_csv(r"T:\Daily trades\Daily Re\Oct20\Oct05\!Margin Summary\PAG_POLY FINANCING REPORT_20201005.csv")

poly_fin = pd.read_csv(path3 + file, skip_blank_lines=True)
pb_map = pd.read_excel(r'P:\Operations\Polymer - Middle Office\PTH and FX booking\Borrow, OP booking\PB mapping.xlsx')
pb_map.columns = ['Custodian Code', 'Custodian ID']

# filter _FX account
poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
# mapp Custodian code
poly_fin = poly_fin.merge(pb_map, how='outer', left_on='Custodian Code', right_on='Custodian Code')
poly_fin['Date'] = today1
# Exacting ISIN
poly_fin['type'] = 'ISIN'
# amend Bberg
poly_fin['Bberg1'] = poly_fin['Bberg'] + ' Equity'
poly_fin['AssetMeasure'] = 'MarketPrice'
poly_fin['Quoteset'] = 'POLYMER QUOTE SET#1'
poly_fin['Quotesource'] = 'Internal'
# massage the data, 1.fill 0 to blank 2, remove %, 3 adjust the type of fee to float
poly_fin['Broker Fee'] = poly_fin['Broker Fee'].fillna(0)
poly_fin['Broker Fee'] = poly_fin['Broker Fee'].replace('%', '')
poly_fin['Broker Fee'] = poly_fin['Broker Fee'].astype(np.float64)
# filterout the broker to 0.4%
poly_fin = poly_fin.loc[poly_fin['Broker Fee'] < -0.004]
pbs = poly_fin['Custodian Code'].unique()
# Summary for Swap upload

sub_summary = poly_fin[
    ['Bberg', 'type', 'Bberg1', 'Custodian Account', 'Position Type Code', 'ISIN', 'Broker Fee', 'Custodian ID', 'Date',
     'AssetMeasure', 'Quoteset', 'Quotesource']]
sub_summary_equity = ' Equity'
sub_summary = sub_summary.loc[~sub_summary['Position Type Code'].str.endswith('PRE')]
# sub_summary[['Bberg']]=poly_fin['Bberg']+
#

con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()

TICKER = sub_summary["Bberg1"]
Ticker_list = pd.Series.tolist(TICKER)

# enriching BBG country code

Ticker_list = list(set(Ticker_list))
CRY_list = blp.bdp(Ticker_list, 'CRNCY')
CRY_list = CRY_list.reset_index()
sub_summary = sub_summary.merge(CRY_list, left_on='Bberg1', right_on='index', how='left')
sub_summary = sub_summary[
    ['Bberg', 'type', 'Bberg1', 'Custodian Account', 'Position Type Code', 'ISIN', 'Broker Fee', 'Custodian ID', 'Date',
     'crncy', 'AssetMeasure', 'Quoteset', 'Quotesource']]
# =============================================================================
# sub_summary_isda=sub_summary.loc[poly_fin['Custodian Account'].str.endswith('_ISDA')].copy()
sub_summary_isda = sub_summary.loc[sub_summary['Custodian Account'].str.endswith('_ISDA')]
sub_summary_isda = sub_summary_isda.sort_values(by=['ISIN', 'Custodian ID', 'Broker Fee']).drop_duplicates(
    ['ISIN', 'Custodian ID']).sort_index()
# load TRS grid
# def TRSid(trs_id):
trs_id = pd.read_csv(r'P:\Operations\Polymer - Middle Office\PTH and FX booking\Borrow, OP booking\Spread/TRS_ID_grid_short.csv').fillna(0)
# change the 1st column to index
trs_id = trs_id.set_index(trs_id.iloc[:, 0]).drop(trs_id.columns[0], axis=1)
trs_id = trs_id.stack().reset_index()
trs_id['index'] = trs_id['broker'].astype(str) + '_' + trs_id['level_1'].astype(str)
trs_id = trs_id[['index', 0]]
headers_trsid = ['TRS', 'TRS_ID']
trs_id.columns = headers_trsid
sub_summary_isda['TRS_ID2'] = sub_summary_isda['Custodian ID'].astype(str).str.slice(stop=6) + "_" + sub_summary_isda[
    'crncy'].astype(str)
# vlookup TRS ID
sub_summary_isda = sub_summary_isda.merge(trs_id, left_on='TRS_ID2', right_on='TRS', how='left')
# select the columns needed
sub_summary_isda['Broker Fee'] = sub_summary_isda['Broker Fee'] * 10000
sub_summary_isda = sub_summary_isda[['Bberg1', 'type', 'Custodian ID', 'TRS_ID', 'Broker Fee', 'Broker Fee',
                                     'Broker Fee', 'crncy', 'AssetMeasure', 'Date', 'Quoteset', 'Quotesource',
                                     ]]
# chagne type to bbyellow
sub_summary_isda['type'] = 'Bloomberg Yellow'
# exclude te black list names
sub_summary_isda = sub_summary_isda[~sub_summary_isda['Bberg1'].isin(swap_black_list)]
#replace the Underlying Identifier for names fall within bb_ticker_replace_list
sub_summary_isda['Bberg1'] = sub_summary_isda['Bberg1'].replace(bb_ticker_replace_list)
# return trs_id
swap_headers = ['Underlying Identifier', 'Underlying Identifier Type', 'Legal Entity Id', 'TRS Id', 'Bid', 'Ask',
                'Last', 'CCY', 'Asset Measure', 'Date', 'Quote Set', 'Quote Source']
sub_summary_isda.columns = swap_headers
# trs_id = trs_id.set_index(trs_id.iloc[:, 0]).drop(trs_id.columns[0], axis=1)
# check the excluded cells

isda_dropped = sub_summary_isda.dropna()
isda_dropped1 = sub_summary_isda.drop(isda_dropped.index)
sub_summary_isda.dropna(inplace=True)

print(isda_dropped1)
# sub_summary_isda = sub_summary_isda[['Bberg1','type','Custodian ID','Broker Fee','Broker Fee','Broker Fee','AssetMeasure','Date','Quoteset','Quotesource','crncy']]
dayinput = input('Namly date of the file: i.e 05')
# remove the swap black list name
# sub_summary_isda=sub_summary_isda.dropna
# =============================================================================
# sub_summary_isda.to_csv('P:\Operations\Polymer - Middle Office\Borrow, OP booking\Spread\Output/'+'Polymer_swap_spread_2023'+month+dayinput+'.csv', index=False,line_terminator='\n')
sub_summary_isda.to_excel(
    'P:\Operations\Polymer - Middle Office\PTH and FX booking\Borrow, OP booking\Spread\Output/' + 'Polymer_swap_spread_2023' + month + dayinput + '.xlsx',
    index=False)
# Summary for Cash upload
# sub_summary_cash=sub_summary.loc[poly_fin['Custodian Account'].str.endswith('_PB')].copy()

# ========================================================
sub_summary_cash = sub_summary.loc[sub_summary['Custodian Account'].str.endswith('_PB')]

# sort by ISIN then Custdodian ID, remove the duplicates keeping the highest value
sub_summary_cash = sub_summary_cash.sort_values(by=['ISIN', 'Custodian ID', 'Broker Fee']).drop_duplicates(
    ['ISIN', 'Custodian ID']).sort_index()
sub_summary_cash['Broker Fee'] = sub_summary_cash['Broker Fee'] * -1
# remove blacklist name
sub_summary_cash = sub_summary_cash[~sub_summary_cash['ISIN'].isin(cash_black_list)]
sub_summary_cash = sub_summary_cash[
    ['ISIN', 'type', 'Custodian ID', 'Broker Fee', 'Broker Fee', 'Broker Fee', 'crncy', 'AssetMeasure', 'Date',
     'Quoteset', 'Quotesource']]
# define a new list of header names
borrow_headers = ['Underlying Identifier', 'Underlying Identifier Type', 'Legal Entity Id', 'Bid', 'Ask', 'Last', 'CCY',
                  'Asset Measure', 'Date', 'Quote Set', 'Quote Source']
sub_summary_cash.columns = borrow_headers
# dropped_cash=sub_summary_cash.dropna()
# check the excluded cells

cash_dropped = sub_summary_cash.dropna()
cash_dropped1 = sub_summary_cash.drop(cash_dropped.index)
sub_summary_cash.dropna(inplace=True)

print(cash_dropped1)
#

# sub_summary_cash.to_csv('P:\Operations\Polymer - Middle Office\Borrow, OP booking\Spread\Output/'+'Polymer_cash_rate_'+'2023'+month+dayinput+'.csv', index=False,line_terminator='\n')
sub_summary_cash.to_excel(
    'P:\Operations\Polymer - Middle Office\PTH and FX booking\Borrow, OP booking\Spread\Output/' + 'Polymer_cash_rate_' + '2023' + month + dayinput + '.xlsx',
    index=False)
