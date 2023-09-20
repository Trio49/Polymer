import numpy as np
import pandas as pd

poly_fin = pd.read_csv('P:\All\FinanceTradingMO\Rebate_Borrow breakdown\POLY_FINANCING_II_202307.csv',
                       skip_blank_lines=True)
# smaller sample
# poly_fin=poly_fin.head(5000)
# PM filter
# PMlist=('AGINE')
print(poly_fin['Date'].unique())
pb_map = pd.read_excel('T:\Daily trades\Daily Re\Middle Office\Hazel Tree\Spread\PB mappping.xlsx')
# pb_map.columns = ['Custodian Code', 'Custodian ID']


# #filter _FX account
# poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_FX')]
# amend ticker
poly_fin['BB Ticker'] = poly_fin['Bberg'] + ' Equity'
# differentitate swap and cash
poly_fin['Type'] = np.where(poly_fin['Custodian Account'].str.endswith('_PB'), 'Cash', 'Swap')
# poly_fin = poly_fin.loc[~poly_fin['Custodian Account'].str.endswith('_PB')]
poly_fin = poly_fin[['Position Type Code',
                     'Custodian Code',
                     'Custodian Account',
                     'BB Ticker',
                     'Quantity - Internal T/D',
                     'Broker Fee',
                     'Base Rate',
                     'Broker Rebate Rate',
                     'Date',
                     'Type'
                     ]]
# Save PTH positions
poly_fin = poly_fin[poly_fin['Position Type Code'] == 'PRE']
# Get BB yellow keylist
TICKER = poly_fin['BB Ticker']
# massage dateformat
DATA_FORMAT = '%Y%m%d'
poly_fin['Date'] = pd.to_datetime(
    poly_fin['Date'],
    errors='coerce'
).apply(lambda x:x.strftime(DATA_FORMAT))
Ticker_list = pd.Series.tolist(TICKER)

poly_fin = poly_fin[['Custodian Code',
                     'Custodian Account',
                     'BB Ticker',
                     'Quantity - Internal T/D',
                     'Broker Fee',
                     'Base Rate',
                     'Broker Rebate Rate',
                     'Date',
                     'Type'
                     ]]
# enriching BBG country code

Ticker_list = list(set(Ticker_list))
import pdblp
from xbbg import blp

con = pdblp.BCon(debug=True, port=8194, timeout=60000)
con.start()
# load historical price
TICKER = poly_fin["BB Ticker"]
Dates = poly_fin["Date"]
Ticker_list = pd.Series.tolist(TICKER)
Ticker_list2 = pd.Series.tolist(Dates)
# enriching BBG country code

Ticker_list = list(set(Ticker_list))

px_list = blp.bdp(Ticker_list, 'EXCH_CODE')
px_list2 = blp.bdp(Ticker_list, 'CRNCY')
# load all historical dates
px_list3 = blp.bdh(Ticker_list, 'PX_LAST', '20220131', Dates.max(), Currency='USD')

px_list3.columns = [i[0] for i in list(px_list3.columns)]

px_list3 = px_list3.unstack(level=0).reset_index()

px_list3.columns = ['BloombergCode', 'RecDate', 'HistPrice']
px_list3['RecDate'] = pd.to_datetime(
    px_list3['RecDate'],
    errors='coerce'
).apply(lambda x:x.strftime(DATA_FORMAT))

px_list3["HistPrice"] = px_list3["HistPrice"].fillna(method="ffill")
# px_list=px_list

px_list = px_list.reset_index()
px_list2 = px_list2.reset_index()
# px_list3=px_list3.reset_index()
px_list.rename(columns={px_list.columns[1]:"EXCH_CODE"}, inplace=True)
# px_list2.rename(columns={px_list2.columns[1]:"Currency"},inplace=True)  

poly_fin = poly_fin.merge(px_list3, left_on=['BB Ticker', 'Date'], right_on=['BloombergCode', 'RecDate'], how='left')
poly_fin = poly_fin.merge(px_list, left_on='BB Ticker', right_on='index', how='left')
poly_fin = poly_fin.merge(px_list2, left_on='BB Ticker', right_on='index', how='left')

poly_fin['Custodian Account2'] = poly_fin['Custodian Account'].str.replace('AW_ALPHA_IAC', 'AWIAC', regex=True)
poly_fin['Custodian Account2'] = poly_fin['Custodian Account2'].str.replace('AW_ALPHA', 'SCALA', regex=True)
poly_fin['Custodian Account2'] = poly_fin['Custodian Account2'].str.replace('(FPI)', '', regex=True)
poly_fin['Custodian Account2'] = poly_fin['Custodian Account2'].str.replace(r"\(.*\)", '', regex=True)
poly_fin['PM'] = poly_fin['Custodian Account2'].str[5:10]
poly_fin = poly_fin[['Custodian Code',
                     'PM',
                     'Custodian Account',
                     'BB Ticker',
                     'Date',
                     'Quantity - Internal T/D',
                     'HistPrice',
                     'Broker Fee',
                     'Base Rate',
                     'Broker Rebate Rate',
                     'crncy',
                     'Type'
                     ]]

# taking AKUNG as example
# if PMlist:
# poly_fin_AGINE=poly_fin[poly_fin['PM'].isin([('AGINE')])]
# remove empty ticker and empty broker fee row
poly_fin = poly_fin.dropna(subset=['BB Ticker', 'Quantity - Internal T/D', 'Broker Fee'])
# calculate notional
poly_fin['Notional$'] = poly_fin['Quantity - Internal T/D'].abs() * poly_fin['HistPrice']
# daily short fee
poly_fin['Short Fee Daily Accrual'] = poly_fin['Notional$'] * poly_fin['Broker Fee'] / 365
# daily short proceeds rebate
# poly_fin['Short Proceeds Rebate Daily Accrual']=poly_fin['Notional$']*poly_fin['Base Rate']/365
poly_fin['Date2'] = pd.to_datetime(poly_fin['Date'], format='%Y-%m-%d')
# mark day of the week based on date
poly_fin['day_of_week'] = poly_fin['Date2'].dt.day_name()
poly_fin['Month'] = poly_fin['Date2'].dt.month_name()
# triple daily accrual for friday's number to account for the weekend
poly_fin.loc[poly_fin['day_of_week'] == 'Friday', 'Short Fee Daily Accrual'] = poly_fin['Short Fee Daily Accrual'] * 3
# poly_fin.loc[poly_fin['day_of_week']=='Friday','Short Proceeds Rebate Daily Accrual']=poly_fin['Short Proceeds Rebate Daily Accrual']*3


# export report?
check = input('save reports? ')
if check == 'yes':
    poly_fin.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown/outputdata.csv", index=False,
                    line_terminator='\n')

    raw_swap = poly_fin[poly_fin['Type'] == 'Swap']
    raw_cash = poly_fin[poly_fin['Type'] == 'Cash']
    # raw_country_currency=poly_fin[poly_fin['Type']=='Swap']
    # raw_country_currency=raw_country_currency[['crncy',
    #                      'Month',  
    #                      'Short Fee Daily Accrual',
    #                   'Short Proceeds Rebate Daily Accrual']]
    raw_swap = raw_swap[['Custodian Code',
                         'PM',
                         'Month',
                         'Short Fee Daily Accrual'
                         # 'Short Proceeds Rebate Daily Accrual',

                         ]]
    raw_cash = raw_cash[['Custodian Code',
                         'PM',
                         'Month',
                         'Short Fee Daily Accrual',
                         ]]

    MTD_PB_Swap = raw_swap.groupby(['Custodian Code', raw_swap['Month']]).sum()
    MTD_PM_Swap = raw_swap.groupby(['PM', raw_swap['Month']]).sum()
    MTD_PB_Swap = MTD_PB_Swap.reset_index()
    MTD_PM_Swap = MTD_PM_Swap.reset_index()
    MTD_PB_Swap.columns = ['PB', 'Month', 'MTD Short Fee Accrual']
    MTD_PM_Swap.columns = ['PM', 'Month', 'MTD Short Fee Accrual']
    MTD_PB_Cash = raw_cash.groupby(['Custodian Code', raw_cash['Month']]).sum()
    MTD_PM_Cash = raw_cash.groupby(['PM', raw_cash['Month']]).sum()
    # MTD_Currency_Swap=raw_country_currency.groupby(['crncy',raw_country_currency['Month']]).sum()
    MTD_PB_Cash = MTD_PB_Cash.reset_index()
    MTD_PM_Cash = MTD_PM_Cash.reset_index()
    # MTD_Currency_Swap=MTD_Currency_Swap.reset_index()
    MTD_PB_Cash.columns = ['PB', 'Month', 'MTD Short Fee Accrual']
    MTD_PM_Cash.columns = ['PM', 'Month', 'MTD Short Fee Accrual']
    month = input('which month please:  ')
    MTD_PB_Swap.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\MTD_PB_Swap_PTH" + month + ".csv", index=False,
                       line_terminator='\n')
    MTD_PM_Swap.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\MTD_PM_Swap_PTH" + month + ".csv", index=False,
                       line_terminator='\n')
    MTD_PB_Cash.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\MTD_PB_Cash_PTH" + month + ".csv", index=False,
                       line_terminator='\n')
    MTD_PM_Cash.to_csv(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\MTD_PM_Cash_PTH" + month + ".csv", index=False,
                       line_terminator='\n')
    print('Saved')

    with pd.ExcelWriter(r"P:\All\FinanceTradingMO\Rebate_Borrow breakdown\202307_PTH_Financing.xlsx") as writer:
        MTD_PB_Swap.to_excel(writer, sheet_name='MTD_PB_SWAP_PTH')
        MTD_PM_Swap.to_excel(writer, sheet_name='MTD_PM_SWAP_PTH')
        MTD_PB_Cash.to_excel(writer, sheet_name='MTD_PB_Cash_PTH')
        MTD_PM_Cash.to_excel(writer, sheet_name='MTD_PM_Cash_PTH')

    writer.save()
    writer.close()

    # plot
# import plotly.express as px
# fig_currency_swap = px.bar(MTD_Currency_Swap, x=(['crncy','Month']), y=(['MTD Short Fee Accrual', 'MTD Short Proceeds Rebate']), color="City", barmode="group")
