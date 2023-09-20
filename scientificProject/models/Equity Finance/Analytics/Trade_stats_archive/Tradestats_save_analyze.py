# Description: This script is used to dump the data from the trade blotter and save it to a csv file
# Path: models\Equity Finance\Analytics\Dumpdata_v2.py

from datetime import datetime, timedelta
from os import listdir

import pandas as pd
import pytz


def data_dump():
    today = datetime.now(pytz.timezone('Asia/Shanghai'))
    #create a input box to enter the time delta for the date
    a = input("Enter the time delta for the date: ")
    a = int(a)
    ############remove when running officially
    # today=today.date()
    today = today.date() - timedelta(days=a)

    today = today.strftime('%Y%m%d')

    # all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
    all_PM_tradeblotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today
    firm_positions_list_dir = listdir(all_PM_tradeblotter_file_path)

    firm_positions_list_dir1 = firm_positions_list_dir[::-1]

    for file_name in firm_positions_list_dir1:

        if "Trade Blotter - PM ALL (New)" in file_name:
            print(file_name)

            all_PM_tradeblotter_file_path = all_PM_tradeblotter_file_path + "\\" + file_name
            break

    # Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path)
    # load EOD blotter
    Alltrade = pd.read_csv(all_PM_tradeblotter_file_path)
    Alltrade = Alltrade[['FundShortName', 'Strategy', 'Counterparty Short Name', 'BB Yellow Key',
                         'Transaction Type', 'Notional Quantity', 'CustodianShortName',
                         'Trade Price', 'Market Price', 'Trade Date', 'Settle Date',
                         'Trade Currency', 'Business Unit',
                         'Sub Business Unit', 'Contract Multiplier',
                         'Trade/Book FX Rate', 'Settle Currency', 'Description', 'Asset Class',
                         'Volume',
                         'Trading Net Proceeds', '$ Trading Net Proceeds',
                         'Order Total Quantity', 'Order Today Executed Quantity',
                         'Trading Notional Proceeds', 'InstIDPlusBuySell',
                         'WeightAverageTradePrice', 'Exchange Country Code',
                         'PAG Delta adjusted Option Notional ', 'Delta Adjusted MV',
                         'OutSourceTraderID',
                         '$ Notional Quantity * Trade Price', 'Instrument Type',
                         '$ Trading Notional Net Proceeds']]

    import blpapi

    # Define the Bloomberg API session
    session = blpapi.Session()
    session.start()
    session.openService("//blp/refdata")
    refDataService = session.getService("//blp/refdata")

    # Load the ticker list from the CSV file
    # ticker_list = pd.read_csv('ticker_list.csv')

    # Define the fields you want to retrieve
    from xbbg import blp
    import pdblp

    con = pdblp.BCon(debug=True, port=8194, timeout=60000)
    con.start()

    TICKER = Alltrade['BB Yellow Key']
    Ticker_list = pd.Series.tolist(TICKER)

    # enriching BBG country code

    Ticker_list = list(set(Ticker_list))
    fields = ['EQY_BETA_6M', 'VOLUME_AVG_30D', 'CRNCY_ADJ_PX_LAST']
    # CRY_list=blp.bdp(Ticker_list, 'EQY_BETA_6M', 'VOLUME_AVG_30D', 'CRNCY_ADJ_PX_LAST')
    CRY_list = blp.bdp(Ticker_list, flds=['EQY_BETA_6M', 'VOLUME_AVG_30D', 'CRNCY_ADJ_PX_LAST'])
    CRY_list = CRY_list.reset_index()
    # Save the updated ticker list to a new CSV file
    # tradedatahtlt=tradedata.merge(htlt,left_on='Counterparty Short Name',right_on='Counterparty Short Name',how='left')
    Alltrade_1 = Alltrade.merge(CRY_list, how='inner', left_on='BB Yellow Key', right_on='index')
    # define average daily volume in USD
    Alltrade_1['ADV USD'] = Alltrade_1['volume_avg_30d'] * Alltrade_1['crncy_adj_px_last'] / Alltrade_1[
        'Trade/Book FX Rate']
    # save the massaged data to csv
    Alltrade_1.to_csv(r'P:\Operations\Polymer - Middle Office\Tradingdata/updated_all_trade.csv', index=False)


# =============================================================================
# Description: This script is used to analyze the data from the trade blotter and save it to a csv file
# Path: models\Equity Finance\Analytics\Tradestats_save_analyze.py
def trade_analysis():
    import datetime as dt
    from datetime import datetime, timedelta

    import numpy as np
    import pandas as pd
    import pytz

    # define time

    today = datetime.now(pytz.timezone('Asia/Shanghai'))
    today = today.date()
    # today=today.date()-timedelta(days=2)
    date = dt.datetime.today().strftime('%b%m%yyyy')

    tradedata = pd.read_csv(r'P:\Operations\Polymer - Middle Office\Tradingdata/updated_all_trade.csv')

    # remove Nan trader name
    # =============================================================================
    # tradedata=tradedata[tradedata['OutSourceTraderID'].notna()]
    # =============================================================================
    # =============================================================================
    tradedata = tradedata[tradedata['Notional Quantity'].notna()]
    # =============================================================================
    tradedata['OutSourceTraderID'] = tradedata['OutSourceTraderID'].replace(
        ['fixgwuser', 'jho2', 'artelo', 'bpjepsen', 'jzonenshine', 'mmazor', 'mzonenshine', 'dsamuell1', 'atong',
         'jevans','wfaulkner',
         'gmclaughlin', 'jtan1'], 'Tora')
    tradedata['OutSourceTraderID'] = tradedata['OutSourceTraderID'].fillna('Manual & Quant')

    # Adding Custom Column test if value greater than 1mil
    tradedata['1M Test'] = np.where(abs(tradedata['PAG Delta adjusted Option Notional ']) > 1000000, '>1m', "<1m")
    # copy the original transaction type for GMV calculation, replacing Buytocover to Buy for notional
    tradedata['Transaction Type2'] = tradedata['Transaction Type']
    tradedata['Transaction Type'] = tradedata['Transaction Type'].replace(['BuyToCover'], 'Buy')
    # Name list of Traders
    Traders = tradedata['OutSourceTraderID'].unique()
    # Name list of trade types
    Tradetypes = tradedata['Transaction Type'].unique()
    # Name list of PB
    custodians = tradedata['CustodianShortName'].unique()
    # Name list of Exchange Country Code
    Country = tradedata['Exchange Country Code'].unique()
    # differentiate negative notional for short and long sell
    # =============================================================================
    tradedata['PAG Delta adjusted Option Notional 2'] = np.where(tradedata['Transaction Type'] == 'Buy',
                                                                 tradedata['PAG Delta adjusted Option Notional '] * 1,
                                                                 tradedata['PAG Delta adjusted Option Notional '] * -1)
    # round the notional to the nearest mil
    # =============================================================================
    tradedata['PAG Delta adjusted Option Notional '] = abs(tradedata['PAG Delta adjusted Option Notional '] / 1000000)
    tradedata['PAG Delta adjusted Option Notional 2'] = tradedata['PAG Delta adjusted Option Notional 2'] / 1000000
    tradedata['ADV USD'] = tradedata['ADV USD'] / 1000000

    # =============================================================================
    # =============================================================================
    # drop listed option
    tradedata = tradedata.loc[tradedata['Instrument Type'] != 'Listed Option']
    # drop internal trades
    tradedata = tradedata.loc[tradedata['Counterparty Short Name'] != 'Internal']
    tradedata = tradedata.loc[tradedata['Counterparty Short Name'] != 'INT']
    tradedata = tradedata.loc[tradedata['Counterparty Short Name'] != 'REORG']
    tradedata['Notional(Abs)'] = tradedata['PAG Delta adjusted Option Notional ']
    tradedata['Notional'] = tradedata['PAG Delta adjusted Option Notional 2']
    # drop reorg
    # beta exposure
    # =============================================================================
    # tradedata['Beta adjusted Nootional']=tradedata['PAG Delta adjusted Option Notional 2']*tradedata['EQY_BETA_6M']
    # =============================================================================
    # mapp the trade to LT/HT,creating duplicate lines
    htlt = pd.read_csv(r'P:\Operations\Polymer - Middle Office\Tradingdata/HTLT.csv')
    htlt.columns = ['Counterparty Short Name', 'HT or LT']
    tradedatahtlt = tradedata.merge(htlt, left_on='Counterparty Short Name', right_on='Counterparty Short Name',
                                    how='left')

    # =============================================================================
    # tradedatahtlt=tradedatahtlt.drop_duplicates(keep='first')
    # =============================================================================

    # cutting
    def advcutting(row):
        if row['PAG Delta adjusted Option Notional '] > row['ADV USD'] * 0.2:
            return '>20% ADV'
        elif row['PAG Delta adjusted Option Notional '] > row['ADV USD'] * 0.1:
            return '>10% ADV'
        elif row['PAG Delta adjusted Option Notional '] > row['ADV USD'] * 0.05:
            return '>5% ADV'
        else:
            return '<5% ADV'

    tradedata['ADV Cut'] = tradedata.apply(advcutting, axis=1)

    # =============================================================================
    # counting HTLT
    # =============================================================================
    # TradeTickets2=tradedata.groupby(['OutSourceTraderID','HT or LT']).size()
    # =============================================================================
    # Combining 2 table
    # =============================================================================
    # print(TradeTickets)
    # =============================================================================
    # save a copy for messaged data
    # tradedatahtlt.to_csv(r'C:\Users\zzhang\Desktop\PYTHON\tradedata.csv',index=False,line_terminator='\n')
    # tradedata.to_csv(r'C:\Users\zzhang\Desktop\PYTHON\tradedata2.csv',index=False,line_terminator='\n')
    tradedata.to_csv(r'P:\Operations\Polymer - Middle Office\Tradingdata/' + date + '.xlsx', index=False,
                     line_terminator='\n')
    # PIVOT table

    # Trader buy,short, sell
    Table1 = (tradedata.pivot_table(values=['Notional(Abs)'],
                                    index=['OutSourceTraderID'],
                                    columns=['Transaction Type'],
                                    aggfunc=np.size,
                                    margins=True,
                                    margins_name='Total',
                                    fill_value=0,
                                    dropna=True)).astype(int)

    # Table1=Table1.transpose()
    # Table1.reset_index(drop=True,inplace=True)
    Table1.columns = ['Buy', 'Sell', 'SellShort', '# of Trades']

    # Table1=Table1.transpose()

    Table_Trader_Notional = (tradedata.pivot_table(values=['Notional'],
                                                   index=['OutSourceTraderID'],
                                                   columns=['Transaction Type'],
                                                   aggfunc=np.sum,
                                                   margins=True,
                                                   margins_name='Total',
                                                   fill_value=0,
                                                   ))
    Table_Trader_Notional = Table_Trader_Notional.round(2)
    # Table_Trader_Notional_abs as absolute trading notional

    Table_Trader_Notional_abs = (tradedata.pivot_table(values=['Notional(Abs)'],
                                                       index=['OutSourceTraderID'],
                                                       columns=['Transaction Type'],
                                                       aggfunc=np.sum,
                                                       margins=True,
                                                       margins_name='Total',
                                                       fill_value=0,
                                                       ))
    Table_Trader_Notional_abs = Table_Trader_Notional_abs.round(2)
    # Table_Trader_Notional_abs as absolute trading notional
    Table1v3 = (tradedata.pivot_table(values=['Notional'],
                                      index=['FundShortName'],
                                      columns=['Transaction Type'],
                                      aggfunc=np.sum,
                                      margins=True,
                                      margins_name='Total',
                                      fill_value=0,
                                      dropna=True))
    Table1v3 = Table1v3.round(2)
    # by PM
    Table1v4 = (tradedata.pivot_table(values=['Notional(Abs)'],
                                      index=['FundShortName'],
                                      columns=['Transaction Type2'],
                                      aggfunc=np.sum,
                                      margins=True,
                                      fill_value=0,
                                      dropna=True))
    Table1v4 = Table1v4.round(2)
    Table1v5 = Table1v4.copy(deep=True)
    Table1v5.columns = Table1v4.columns.droplevel(0)
    Table1v5['GMV Long increase'] = Table1v5['Buy'].astype(float) - Table1v5['Sell'].astype(float)
    Table1v5['GMV Short increase'] = Table1v5['SellShort'].astype(float) - Table1v5['BuyToCover'].astype(float)
    # by custodian
    Tablecustodian = (tradedata.pivot_table(values=['Notional(Abs)'],
                                            index=['CustodianShortName'],
                                            columns=['Transaction Type2'],
                                            aggfunc=np.sum,
                                            margins=True,
                                            fill_value=0,
                                            dropna=True))
    # Table1v5=Table1v4.copy(deep=True)
    Tablecustodian.columns = Tablecustodian.columns.droplevel(0)
    Tablecustodian['GMV Long increase'] = Tablecustodian['Buy'].astype(float) - Tablecustodian['Sell'].astype(float)
    Tablecustodian['GMV Short increase'] = Tablecustodian['SellShort'].astype(float) - Tablecustodian[
        'BuyToCover'].astype(
        float)
    Tablecustodian['GMV Net Change'] = Tablecustodian['GMV Long increase'] + Tablecustodian['GMV Short increase']
    # Trader by country
    TableX_2 = (tradedata.pivot_table(values='Notional(Abs)',
                                      index=['OutSourceTraderID'],
                                      columns=['Exchange Country Code'],
                                      aggfunc=np.sum,
                                      fill_value=0,
                                      margins=True,
                                      margins_name='Total'
                                      ))
    # round TableX_2 to 2 decimal places
    TableX_2 = TableX_2.round(2)
    # Trader by HT LT, notional and size and percentage
    TableX_3 = (tradedatahtlt.pivot_table(values='Notional(Abs)',
                                          index=['OutSourceTraderID'],
                                          columns=['HT or LT'],
                                          aggfunc=np.sum,
                                          fill_value=0,
                                          margins=True,
                                          margins_name='Total',
                                          dropna=True))
    TableX_3['HT%'] = TableX_3['HT'].astype(float) / TableX_3['Total'].astype(float)
    TableX_3['LT%'] = TableX_3['LT'].astype(float) / TableX_3['Total'].astype(float)
    TableX_3 = TableX_3[['HT%', 'LT%']]
    # Format Table 3 to percentage
    TableX_3 = TableX_3.applymap(lambda x:"{0:.2f}%".format(x * 100))
    # Trade by 1mil bench mark
    TableX_5 = (tradedata.pivot_table(values='Notional(Abs)',
                                      index=['OutSourceTraderID'],
                                      columns=['1M Test'],
                                      aggfunc=np.sum,
                                      fill_value=0,
                                      margins=True,
                                      margins_name='Total',
                                      dropna=True))
    TableX_5['>1m Percentage'] = TableX_5['>1m'].astype(float) / TableX_5['Total']
    # Format TableX_5 to percentage
    TableX_5 = TableX_5.applymap(lambda x:"{0:.2f}%".format(x * 100))
    # to only include the percentage
    TableX_5 = TableX_5['>1m Percentage']
    # Table by ADV
    TableX_6 = (tradedata.pivot_table(values='Notional(Abs)',
                                      index=['OutSourceTraderID'],
                                      columns=['ADV Cut'],
                                      aggfunc=np.sum,
                                      fill_value=0,
                                      margins=True,
                                      margins_name='Total',
                                      dropna=True))

    TableX_6['>5% ADV%'] = TableX_6['>5% ADV'].astype(float) / TableX_6['Total'].astype(float)
    TableX_6['>10% ADV%'] = TableX_6['>10% ADV'].astype(float) / TableX_6['Total']
    TableX_6['>20% ADV%'] = TableX_6['>20% ADV'].astype(float) / TableX_6['Total']
    # only include percentage
    TableX_6 = TableX_6[['>5% ADV%', '>10% ADV%', '>20% ADV%']]
    # format percentage
    TableX_6 = TableX_6.applymap(lambda x:"{0:.2f}%".format(x * 100))
    # Consolidated Tables
    TableX = Table1.merge(Table_Trader_Notional, how='outer', left_on='OutSourceTraderID', right_on='OutSourceTraderID')
    TableX = TableX.merge(Table_Trader_Notional_abs, how='outer', left_on='OutSourceTraderID',
                          right_on='OutSourceTraderID')
    # =============================================================================
    # TableX=TableX.merge(TableX_2,how='outer',left_on='OutSourceTraderID',right_on='OutSourceTraderID')
    # =============================================================================
    TableX = TableX.merge(TableX_2, how='outer', left_on='OutSourceTraderID', right_on='OutSourceTraderID')
    TableX = TableX.merge(TableX_3, how='outer', left_on='OutSourceTraderID', right_on='OutSourceTraderID')
    TableX = TableX.merge(TableX_5, how='outer', left_on='OutSourceTraderID', right_on='OutSourceTraderID')
    TableX = TableX.merge(TableX_6, how='outer', left_on='OutSourceTraderID', right_on='OutSourceTraderID')
    print(TableX.shape)
    # temporary fix

    TableX = TableX.transpose()
    TableX = TableX.fillna(0)
    # TableX: object = TableX.astype(float)

    Table1v3 = Table1v3.astype(float)
    Table1v3 = Table1v3.fillna(0)
    Table1v5 = Table1v5.astype(float)
    Table1v5 = Table1v5.fillna(0)
    Table_custodian = Tablecustodian.astype(float).fillna(0)
    # Table_custodian = Tablecustodian.fillna(0)

    # =============================================================================
    # format TableX
    # TableX values from row index "Buy" to index "Total" round to 2 decimal places

    # TableX.iloc[4:26] = TableX.iloc[4:26].round(2)
    # TableX.iloc[26:34] = TableX.iloc[26:34].applymap(lambda x:'{:.2%}'.format(x))
    Table1v3 = Table1v3.round(2)
    Table1v5 = Table1v5.round(2)
    # convert all values of Tablecustodian to float and round to 2 decimal places
    Table_custodian = Tablecustodian.astype(float)
    Table_custodian = Table_custodian.round(2).astype(float)
    #save Table_custodian to csv file with today string
    Table_custodian.to_csv(r'P:\Operations\Polymer - Middle Office\Tradingdata/PB_GMV/{}.csv'.format(today))
    # =============================================================================
    # TradeTickets.to_excel(writer,sheet_name='Ticketscount')
    # =============================================================================
    # =============================================================================
    # save the tables to Excel
    with pd.ExcelWriter(r'P:\Operations\Polymer - Middle Office\Tradingdata/data.xlsx') as writer:
        # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        TableX.to_excel(writer, sheet_name='Summary')
        Table1v3.to_excel(writer, sheet_name='Notional_PM')
        Table1v5.to_excel(writer, sheet_name='Notional_ABS_PM_GMV')
        Table_custodian.to_excel(writer, sheet_name='Custodian_GMV')
    # =============================================================================
    with pd.ExcelWriter(r'P:\Operations\Polymer - Middle Office\Tradingdata/tradestats_new.xlsx') as writer:
        TableX.to_excel(writer, sheet_name='Summary')
        Table1v3.to_excel(writer, sheet_name='Notional_PM')
        Table1v5.to_excel(writer, sheet_name='Notional_ABS_PM_GMV')
        Table_custodian.to_excel(writer, sheet_name='Custodian_GMV')
        # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
        workbook = writer.book
        worksheet = writer.sheets['Notional_PM']
        worksheet2 = writer.sheets['Notional_ABS_PM_GMV']
        worksheet3 = writer.sheets['Custodian_GMV']
        worksheet4 = writer.sheets['Summary']

        # Apply conditional formatting to the last two columns
        worksheet.conditional_format(1, Table1v3.shape[1], Table1v3.shape[0], Table1v3.shape[1],
                                     {'type':'3_color_scale', 'min_color':"#F8696B", 'mid_color':"#FFEB84",
                                      'max_color':"#63BE7B"})
        worksheet2.conditional_format(1, Table1v5.shape[1], Table1v5.shape[0], Table1v5.shape[1],
                                      {'type':'3_color_scale', 'min_color':"#F8696B", 'mid_color':"#FFEB84",
                                       'max_color':"#63BE7B"})
        worksheet3.conditional_format(1, Table_custodian.shape[1], Table_custodian.shape[0],
                                      Table_custodian.shape[1],
                                      {'type':'3_color_scale', 'min_color':"#F8696B", 'mid_color':"#FFEB84",
                                       'max_color':"#63BE7B"})
        # Apply All borderlines to Table1v3 Table 1v5 and Tablecustodian
        border_format = workbook.add_format({'border':1})
        worksheet.conditional_format(1, 0, Table1v3.shape[0], Table1v3.shape[1],
                                     {'type':'no_blanks', 'format':border_format})
        # apply it to 1v5 as well
        worksheet2.conditional_format(1, 0, Table1v5.shape[0], Table1v5.shape[1],
                                      {'type':'no_blanks', 'format':border_format})
        # apply it to custodian as well
        worksheet3.conditional_format(1, 0, Table_custodian.shape[0], Table_custodian.shape[1],
                                      {'type':'no_blanks', 'format':border_format})
        # apply it to tableX as well
        worksheet4.conditional_format(1, 0, TableX.shape[0], TableX.shape[1],
                                      {'type':'no_blanks', 'format':border_format})
        # apply width to fit the columns to the content
        worksheet.set_column(0, Table1v3.shape[1], 20)
        worksheet2.set_column(0, Table1v5.shape[1], 20)
        worksheet3.set_column(0, Tablecustodian.shape[1], 20)
        worksheet4.set_column(0, TableX.shape[1], 20)

    writer.save()
    writer.close()
    return TableX, Table1v3, Table1v5, Table_custodian


# run datadump function
data_dump()
TableX, Table1v3, Table1v5, Table_custodian=trade_analysis()
#create 2 bar charts for Table1v5 and Table_custodian
print("done")
