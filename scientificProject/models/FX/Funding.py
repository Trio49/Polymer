# Funding T1v2.py
import sys


def funding_t1():
    # code for funding_t1

    import pandas as pd
    import numpy as np
    import datetime as dt
    # import matplotlib.pyplot as plt
    from datetime import datetime
    from os import listdir
    from pandas.tseries.offsets import BDay

    # today=datetime.now(pytz.timezone('Asia/Shanghai'))

    ############remove when running officially
    today = datetime.today()
    # define today1 as t-1 define today 2 as t-2
    # a is the backdate delta
    a = 2
    # t0
    today1 = today - BDay(a - 1)
    # t-1
    today2 = today - BDay(a)
    today1 = today1.strftime('%Y%m%d')
    today2 = today2.strftime('%Y%m%d')
    t_1 = (datetime.today() - BDay(a - 1)).strftime('%Y-%m-%d')
    t_2 = (datetime.today() - BDay(a)).strftime('%Y-%m-%d')
    # today=today.date()
    month = dt.datetime.today().strftime('%b')
    # load t-1 and t_2 blotter
    # all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today1
    all_PM_tradeblotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today1
    firm_positions_list_dir = listdir(all_PM_tradeblotter_file_path)

    firm_positions_list_dir1 = firm_positions_list_dir[::-1]

    for file_name in firm_positions_list_dir1:

        if "PM ALL (New)" in file_name:
            print(file_name)

            all_PM_tradeblotter_file_path = all_PM_tradeblotter_file_path + "\\" + file_name
            break
    print(all_PM_tradeblotter_file_path)
    import sys
    input_checking = input('is the file latest? yes/no:')
    if input_checking == 'yes':
        t1trade = pd.read_csv(all_PM_tradeblotter_file_path, engine='python')
    elif input_checking == 'no':
        print('please run systemjob to generate all PM blotter')
        sys.exit()
    # Prompt to skip KRW reorg trades
    bypass = input('skip KRW reorganization trades? yes/no')
    if bypass == 'yes':
        t1trade = t1trade.loc[t1trade['Trade Type'] != 'Reorganization']
    # all_PM_tradeblotter_file_path2=r"\\paghk.local\shares\Enfusion\Polymer\\"+today2
    all_PM_tradeblotter_file_path2 = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today2
    firm_positions_list_dir2 = listdir(all_PM_tradeblotter_file_path2)

    firm_positions_list_dir2 = firm_positions_list_dir2[::-1]

    for file_name in firm_positions_list_dir2:

        if "Trade Blotter - PM ALL (New)" in file_name:
            print(file_name)

            all_PM_tradeblotter_file_path2 = all_PM_tradeblotter_file_path2 + "\\" + file_name
            break

    t2trade = pd.read_csv(all_PM_tradeblotter_file_path2, engine='python')

    # combine
    trades = [t1trade, t2trade]
    tradesx = pd.concat(trades)
    # filter out reorg trades for TWD
    bypass = input('skip TWD reorganization trades? yes/no')
    if bypass == 'yes':
        tradesx = tradesx.loc[tradesx['Trade Type'] != 'Reorganization']
    # filter out non market trade
    tradesx = tradesx.loc[tradesx['Counterparty Short Name'] != 'INT']
    tradesx = tradesx.loc[tradesx['Counterparty Short Name'] != 'REORG']
    tradesx = tradesx.loc[tradesx['Counterparty Short Name'] != 'CROSS']
    # get mapping table

    Bunitmapping = tradesx[['Sub Business Unit', 'Business Unit']]
    Bunitmapping = Bunitmapping.drop_duplicates(subset=['Sub Business Unit'], keep='last')
    Bunitmapping.to_csv(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv', index=False,
                        line_terminator='\n')
    # filter out the cash trades
    tradesfx = tradesx[
        ['Business Unit', 'Sub Business Unit', 'Transaction Type', 'CustodianShortName', 'Trade Date', 'Settle Date',
         'Trading Net Proceeds', 'Trade Currency', 'Settle Currency', 'Instrument Type', ]]
    # for KRW only need to use t-1 trade, name it t1 trade v2
    t1tradev2 = t1trade[
        ['Business Unit', 'Sub Business Unit', 'Transaction Type', 'CustodianShortName', 'Trade Date', 'Settle Date',
         'Trading Net Proceeds', 'Trade Currency', 'Settle Currency', 'Instrument Type', ]]

    # filter  out cash trades by currency and settlement currency TWD KRW
    tradesfx = tradesfx.loc[tradesfx['Trade Currency'].isin(['TWD', 'KRW'])]
    tradesfx = tradesfx.loc[tradesfx['Settle Currency'].isin(['TWD', 'KRW'])]
    tradesfx['Trading Net Proceeds'] = tradesfx['Trading Net Proceeds'].astype(int)
    # include only equity trades
    tradesfx = tradesfx.loc[tradesfx['Instrument Type'].isin(['Equity'])]
    t1tradev2 = t1tradev2.loc[t1tradev2['Instrument Type'].isin(['Equity'])]
    # pivot twd trades

    tradesfxtwd = tradesfx.loc[tradesfx['Trade Currency'] == 'TWD']

    isempty2 = tradesfxtwd.empty
    print('No TWD trades :', isempty2)

    if isempty2 == False:
        tradesfxtwd = tradesfxtwd.copy()
        tradesfxtwd['Trade Date'] = pd.to_datetime(tradesfxtwd['Trade Date']).dt.strftime('%Y-%m-%d')
        tradesfxtwd['Settle Date'] = pd.to_datetime(tradesfxtwd['Trade Date']).dt.strftime('%Y-%m-%d')
        # define t_1 and t_2

        # t-1 twd buy
        tradesfxtwd1 = tradesfxtwd[(tradesfxtwd['Trade Date'] == t_1) & (tradesfxtwd['Transaction Type'] == 'Buy')]
        # t-2 twd Sell
        tradesfxtwd2 = tradesfxtwd[(tradesfxtwd['Trade Date'] == t_2) & (tradesfxtwd['Transaction Type'] == 'Sell')]
        # t-1 twd sell
        tradesfxtwd3 = tradesfxtwd[(tradesfxtwd['Trade Date'] == t_1) & (tradesfxtwd['Transaction Type'] == 'Sell')]

        # generating the funding instruction
        tradesfxtwdx = [tradesfxtwd1, tradesfxtwd2]
        tradesfxtwd = pd.concat(tradesfxtwdx)
        # generating the TWD delta change
        tradesfxtwdx2 = [tradesfxtwd1, tradesfxtwd2, tradesfxtwd3]
        TWD_exposurechange = pd.concat(tradesfxtwdx2)
        # twd funding pivot
        fundingtwd = (tradesfxtwd.pivot_table(values=['Trading Net Proceeds'],
                                              index=['Business Unit'],
                                              columns=['CustodianShortName'],
                                              aggfunc=np.sum,
                                              margins=True,
                                              margins_name='Total',
                                              fill_value='0',
                                              dropna=True))
        funding_sub_twd = (tradesfxtwd.pivot_table(values=['Trading Net Proceeds'],
                                                   index=['Sub Business Unit'],
                                                   columns=['CustodianShortName'],
                                                   aggfunc=np.sum,
                                                   margins=True,
                                                   margins_name='Total',
                                                   fill_value='0',
                                                   dropna=True))
        # twd exposure pivot
        TWDExposurechangepivot = (TWD_exposurechange.pivot_table(values=['Trading Net Proceeds'],
                                                                 index=['Business Unit'],
                                                                 columns=['CustodianShortName'],
                                                                 aggfunc=np.sum,
                                                                 margins=True,
                                                                 margins_name='Total',
                                                                 fill_value='0',
                                                                 dropna=True))
    # pivot KRW traades
    tradesfxkrw = t1tradev2.loc[t1tradev2['Trade Currency'] == 'KRW']
    isempty = tradesfxkrw.empty
    print('No KRW trades :', isempty)
    # only include Equity trades

    if isempty == False:
        # KRW equity trades
        fundingkrw = (tradesfxkrw.pivot_table(values=['Trading Net Proceeds'],
                                              index=['Business Unit'],
                                              columns=['CustodianShortName'],
                                              aggfunc=np.sum,
                                              margins=True,
                                              margins_name='Total',
                                              fill_value='0',
                                              dropna=True))
        funding_sub_krw = (tradesfxkrw.pivot_table(values=['Trading Net Proceeds'],
                                                   index=['Sub Business Unit'],
                                                   columns=['CustodianShortName'],
                                                   aggfunc=np.sum,
                                                   margins=True,
                                                   margins_name='Total',
                                                   fill_value='0',
                                                   dropna=True))
        # KRW Exposure change

        # excel path
        # write=pd.ExcelWriter('T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\TWD.xlsx')
        # fundingtwd.to_excel(write,sheet_name='Funding',index=True)
        with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw.xlsx')) as writer1:
            fundingkrw.to_excel(writer1, sheet_name='Funding')
        with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw_sub.xlsx')) as writer2:
            funding_sub_krw.to_excel(writer2, sheet_name='Funding')

    # pathtwd=r'fundingtwd.xlsx'
    # book=load_workbook(path)
    # writer= pd.ExcelWriter(pathtwd,engine='openpyxl')
    # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd.xlsx')) as writer:
        fundingtwd.to_excel(writer, sheet_name='Funding')
        TWDExposurechangepivot.to_excel(writer, sheet_name='TWDexposurechange')
        TWD_exposurechange.to_excel(writer, sheet_name='TWDtrades')
    with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd_sub.xlsx')) as writer3:
        funding_sub_twd.to_excel(writer3, sheet_name='Funding')


# Funding T0v2.py
def funding_t0():
    # code for funding t0

    import pandas as pd
    import numpy as np
    import datetime as dt
    # import matplotlib.pyplot as plt
    from datetime import datetime
    from os import listdir
    from pandas.tseries.offsets import BDay

    # today=datetime.now(pytz.timezone('Asia/Shanghai'))

    ############remove when running officially
    today = datetime.today()
    # define today1 as t-1 define today 2 as t-2
    # a is the backdate delta
    a = 1
    # t0
    today1 = today - BDay(a - 1)
    # t-1
    today2 = today - BDay(a)
    today1 = today1.strftime('%Y%m%d')
    today2 = today2.strftime('%Y%m%d')
    t_1 = (datetime.today() - BDay(0)).strftime('%Y-%m-%d')
    t_2 = (datetime.today() - BDay(a)).strftime('%Y-%m-%d')
    # today=today.date()
    month = dt.datetime.today().strftime('%b')
    # load t-1 and t_2 blotter
    # all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today1
    all_PM_tradeblotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today1
    firm_positions_list_dir = listdir(all_PM_tradeblotter_file_path)

    firm_positions_list_dir1 = firm_positions_list_dir[::-1]

    for file_name in firm_positions_list_dir1:

        if "PM ALL (New)" in file_name:
            print(file_name)

            all_PM_tradeblotter_file_path = all_PM_tradeblotter_file_path + "\\" + file_name
            break
    print(all_PM_tradeblotter_file_path)
    import sys
    input_checking = input('is the file latest? yes/no:')
    if input_checking == 'yes':
        t1trade = pd.read_csv(all_PM_tradeblotter_file_path, engine='python')
    elif input_checking == 'no':
        print('please run systemjob to generate all PM blotter')
        sys.exit()
    # Prompt to skip KRW reorg trades
    bypass = input('skip KRW reorganization trades? yes/no')
    if bypass == 'yes':
        t1trade = t1trade.loc[t1trade['Trade Type'] != 'Reorganization']
    # all_PM_tradeblotter_file_path2=r"\\paghk.local\shares\Enfusion\Polymer\\"+today2
    all_PM_tradeblotter_file_path2 = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today2
    firm_positions_list_dir2 = listdir(all_PM_tradeblotter_file_path2)

    firm_positions_list_dir2 = firm_positions_list_dir2[::-1]

    for file_name in firm_positions_list_dir2:

        if "Trade Blotter - PM ALL (New)" in file_name:
            print(file_name)

            all_PM_tradeblotter_file_path2 = all_PM_tradeblotter_file_path2 + "\\" + file_name
            break

    t2trade = pd.read_csv(all_PM_tradeblotter_file_path2, engine='python')

    # combine
    trades = [t1trade, t2trade]
    tradesx = pd.concat(trades)
    # filter out reorg trades for TWD
    bypass = input('skip TWD reorganization trades? yes/no')
    if bypass == 'yes':
        tradesx = tradesx.loc[tradesx['Trade Type'] != 'Reorganization']
    # filter out reorg trades for KRW IPO

    # tradesx=tradesx.loc[tradesx['Trade Type']!='Reorganization']
    # filter out non market trade
    tradesx = tradesx.loc[tradesx['Counterparty Short Name'] != 'INT']
    tradesx = tradesx.loc[tradesx['Counterparty Short Name'] != 'REORG']
    tradesx = tradesx.loc[tradesx['Counterparty Short Name'] != 'CROSS']
    # get mapping table

    Bunitmapping = tradesx[['Sub Business Unit', 'Business Unit']]
    Bunitmapping = Bunitmapping.drop_duplicates(subset=['Sub Business Unit'], keep='last')
    Bunitmapping.to_csv(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv', index=False,
                        line_terminator='\n')
    # filter out the cash trades
    tradesfx = tradesx[
        ['Business Unit', 'Sub Business Unit', 'Transaction Type', 'CustodianShortName', 'Trade Date', 'Settle Date',
         'Trading Net Proceeds', 'Trade Currency', 'Settle Currency', 'Instrument Type', ]]
    # for KRW only need to use t-1 trade, name it t1 trade v2
    t1tradev2 = t1trade[
        ['Business Unit', 'Sub Business Unit', 'Transaction Type', 'CustodianShortName', 'Trade Date', 'Settle Date',
         'Trading Net Proceeds', 'Trade Currency', 'Settle Currency', 'Instrument Type', ]]

    # filter  out cash trades by currency and settlement currency TWD KRW
    tradesfx = tradesfx.loc[tradesfx['Trade Currency'].isin(['TWD', 'KRW'])]
    tradesfx = tradesfx.loc[tradesfx['Settle Currency'].isin(['TWD', 'KRW'])]
    tradesfx['Trading Net Proceeds'] = tradesfx['Trading Net Proceeds'].astype(int)
    # include only equity trades
    tradesfx = tradesfx.loc[tradesfx['Instrument Type'].isin(['Equity'])]
    t1tradev2 = t1tradev2.loc[t1tradev2['Instrument Type'].isin(['Equity'])]
    # pivot twd trades

    tradesfxtwd = tradesfx.loc[tradesfx['Trade Currency'] == 'TWD']

    isempty2 = tradesfxtwd.empty
    print('No TWD trades :', isempty2)

    if isempty2 == False:
        tradesfxtwd = tradesfxtwd.copy()
        tradesfxtwd['Trade Date'] = pd.to_datetime(tradesfxtwd['Trade Date']).dt.strftime('%Y-%m-%d')
        tradesfxtwd['Settle Date'] = pd.to_datetime(tradesfxtwd['Trade Date']).dt.strftime('%Y-%m-%d')
        # define t_1 and t_2

        # t-1 twd buy
        tradesfxtwd1 = tradesfxtwd[(tradesfxtwd['Trade Date'] == t_1) & (tradesfxtwd['Transaction Type'] == 'Buy')]
        # t-2 twd Sell
        tradesfxtwd2 = tradesfxtwd[(tradesfxtwd['Trade Date'] == t_2) & (tradesfxtwd['Transaction Type'] == 'Sell')]
        # t-1 twd sell
        tradesfxtwd3 = tradesfxtwd[(tradesfxtwd['Trade Date'] == t_1) & (tradesfxtwd['Transaction Type'] == 'Sell')]

        # generating the funding instruction
        tradesfxtwdx = [tradesfxtwd1, tradesfxtwd2]
        tradesfxtwd = pd.concat(tradesfxtwdx)
        # generating the TWD delta change
        tradesfxtwdx2 = [tradesfxtwd1, tradesfxtwd2, tradesfxtwd3]
        TWD_exposurechange = pd.concat(tradesfxtwdx2)
        # twd funding pivot
        fundingtwd = (tradesfxtwd.pivot_table(values=['Trading Net Proceeds'],
                                              index=['Business Unit'],
                                              columns=['CustodianShortName'],
                                              aggfunc=np.sum,
                                              margins=True,
                                              margins_name='Total',
                                              fill_value='0',
                                              dropna=True))
        funding_sub_twd = (tradesfxtwd.pivot_table(values=['Trading Net Proceeds'],
                                                   index=['Sub Business Unit'],
                                                   columns=['CustodianShortName'],
                                                   aggfunc=np.sum,
                                                   margins=True,
                                                   margins_name='Total',
                                                   fill_value='0',
                                                   dropna=True))
        # twd exposure pivot
        TWDExposurechangepivot = (TWD_exposurechange.pivot_table(values=['Trading Net Proceeds'],
                                                                 index=['Business Unit'],
                                                                 columns=['CustodianShortName'],
                                                                 aggfunc=np.sum,
                                                                 margins=True,
                                                                 margins_name='Total',
                                                                 fill_value='0',
                                                                 dropna=True))
    # pivot KRW traades
    tradesfxkrw = t1tradev2.loc[t1tradev2['Trade Currency'] == 'KRW']
    isempty = tradesfxkrw.empty
    print('No KRW trades :', isempty)
    # only include Equity trades

    if isempty == False:
        # KRW equity trades
        fundingkrw = (tradesfxkrw.pivot_table(values=['Trading Net Proceeds'],
                                              index=['Business Unit'],
                                              columns=['CustodianShortName'],
                                              aggfunc=np.sum,
                                              margins=True,
                                              margins_name='Total',
                                              fill_value='0',
                                              dropna=True))
        funding_sub_krw = (tradesfxkrw.pivot_table(values=['Trading Net Proceeds'],
                                                   index=['Sub Business Unit'],
                                                   columns=['CustodianShortName'],
                                                   aggfunc=np.sum,
                                                   margins=True,
                                                   margins_name='Total',
                                                   fill_value='0',
                                                   dropna=True))
        # KRW Exposure change

        # excel path
        # write=pd.ExcelWriter('T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\TWD.xlsx')
        # fundingtwd.to_excel(write,sheet_name='Funding',index=True)
        with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingkrw.xlsx')) as writer1:
            fundingkrw.to_excel(writer1, sheet_name='Funding')
        with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingkrw_sub.xlsx')) as writer2:
            funding_sub_krw.to_excel(writer2, sheet_name='Funding')

    # pathtwd=r'fundingtwd.xlsx'
    # book=load_workbook(path)
    # writer= pd.ExcelWriter(pathtwd,engine='openpyxl')
    # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)

    with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingtwd.xlsx')) as writer:
        fundingtwd.to_excel(writer, sheet_name='Funding')
        TWDExposurechangepivot.to_excel(writer, sheet_name='TWDexposurechange')
        TWD_exposurechange.to_excel(writer, sheet_name='TWDtrades')
    with pd.ExcelWriter((r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingtwd_sub.xlsx')) as writer3:
        funding_sub_twd.to_excel(writer3, sheet_name='Funding')
    # writer.save()


# input the selection to run funding_t0 or funding_t1
election = input('Please select the funding date: t0 or t1')
if election == 't0':
    funding_t0()
elif election == 't1':
    funding_t1()
else:
    print('Please select t0 or t1')


def FX_generating_T1():
    # code for function 1
    import pandas as pd
    import numpy as np
    import datetime
    from pandas.tseries.offsets import BDay
    krw = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw.xlsx'
    twd = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd.xlsx'
    krw_sub = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw_sub.xlsx'
    twd_sub = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd_sub.xlsx'
    pbs = {'GSCO':"GS_PB", 'GSCO_FPI':"GS_FPI", 'JPM':"JPM_FX", 'UBS':"UBS_FX",
           'MLCO':"BAML_FX", 'MSCO':"MS_FX"}
    bunitmap = pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv",
                           engine='python')
    bookid = pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\Subbunit mapping.csv")
    acc = pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\account mapping.csv")

    # empty_series = pd.Series(dtype=float)
    def read_excel(krw, currency, f_date):
        dfk = pd.read_excel(krw, header=1)
        if dfk.empty == False:
            dfk = dfk.iloc[1:-1, :]
            dfk = dfk[dfk.columns[:-1]]
            # print(dfk)
            # dfk = dfk.unstack(['CustodianShortName', 'GSCO', 'MLCO', 'UBS'])
            dfk = dfk.set_index('CustodianShortName')
            dfk = dfk.stack()
            dfk = dfk.reset_index()

            dfk['FORWARD DATE'] = date + BDay(delta)
            if currency in ['TWD', 'twd']:
                dfk['FORWARD DATE'] = date + BDay(delta)
                dfk['FORWARD DATE'] = dfk['FORWARD DATE'].apply(lambda x:datetime.date(x.year, x.month, x.day))
            if currency in ['KRW', 'krw']:
                dfk['FORWARD DATE'] = date + BDay(delta + 1)
                dfk['FORWARD DATE'] = dfk['FORWARD DATE'].apply(lambda x:datetime.date(x.year, x.month, x.day))
            dfk['Currency'] = currency

            dfk['RATE'] = 0
            dfk['USD Notional'] = np.NaN
            dfk['LOCAL NOTIONAL'] = -dfk[0]
            dfk['PM'] = dfk['CustodianShortName']
            dfk = dfk.merge(bunitmap, left_on='PM', right_on='Sub Business Unit', how='left')
            dfk = dfk[dfk[0] != 0]
            dfk['PB'] = dfk['level_1'].apply(lambda x:pbs[x])
            dfk['SPOT'] = 'TRUE'
            dfk['Product'] = 'SPOT'
            dfk['CCY Pair'] = 'USD' + dfk['Currency']
            dfk = dfk[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM',
                       'PB', 'RATE', 'USD Notional', 'LOCAL NOTIONAL', 'Business Unit']]
            dfk_copy = dfk.copy()
            dfk_copy['SPOT'] = 'FALSE'
            dfk_copy['Product'] = 'NDF'
            dfk_copy['LOCAL NOTIONAL'] = -dfk_copy['LOCAL NOTIONAL']

            dfk_copy['FORWARD DATE'] = datetime.datetime.strptime(f_date, '%Y%m%d').date()

            dfk = pd.concat([dfk, dfk_copy], axis=0)
            # dfk = dfk.reset_index()
            dfk = dfk.sort_index()
            dfk['Column1'] = 'POLY_' + dfk['PM'] + '_' + dfk['PB']
            return dfk

    # dfk.to_excel('fx.xlsx',index=False)
    fdate = input('please enter krw forward date(example:20210731):')
    fdate2 = input('please enter twd forward date(example:20210731):')

    # fdate = fdate[:4]+'/' + fdate[4:6] + '/' + fdate[-2:]
    today = datetime.datetime.today()
    td = today - datetime.timedelta(days=0)
    date = datetime.date(td.year, td.month, td.day)
    delta = 0

    df_twd = read_excel(twd, 'TWD', fdate2)
    df_krw = read_excel(krw, 'KRW', fdate)
    df_twd_sub = read_excel(twd_sub, 'TWD', fdate2)
    df_krw_sub = read_excel(krw_sub, 'KRW', fdate)

    df = pd.concat([df_krw, df_twd], axis=0)
    df_sub = pd.concat([df_krw_sub, df_twd_sub], axis=0)

    def massage_fx_trades(df):
        df['PM'] = np.where(df['SPOT'] == 'TRUE', df['PM'], 'EXT_FX')
        df = df.sort_values(by=['PB', 'CCY Pair'], ascending=False)

        # Mappbookid, account
        df = df.merge(bookid, left_on='PM', right_on='Sub Business Unit', how='left')
        # df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
        #         'USD Notional', 'LOCAL NOTIONAL','Account','Business Unit','Book ID']]
        df['Account'] = np.where(
            df['PM'] == 'EXT_FX',
            'POLY_EXT_FX' + '_' + df['PB'],
            'POLY_' + df['Business Unit'] + '_' + df['PB']

        )
        df = df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
                 'USD Notional', 'LOCAL NOTIONAL', 'Account', 'Business Unit', 'Book ID']]

        # df=df.merge(bookid, left_on='PM', right_on='Sub Business Unit',how='inner')
        # map book ID and generate

        df['Product'] = np.where(df['SPOT'] == 'TRUE', 'SPOT', 'NDF')
        pm = df['PM']
        df = df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
                 'USD Notional', 'LOCAL NOTIONAL', 'Account', 'Book ID']]
        df = df.merge(acc, left_on='Account', right_on='ACCOUNT ', how='left')
        df['Account ID'] = df['ID']
        df['Rate'] = str('Paste from recap')
        FX_trades = df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
                        'USD Notional', 'LOCAL NOTIONAL', 'Rate', 'Book ID', 'Account', 'Account ID']]
        return FX_trades

    fxt0 = massage_fx_trades(df)
    fxt0_sub = massage_fx_trades(df_sub)

    # adding instruction matrix
    # idk= input('Today is Monday? yes or no ')
    instruction = input('Generate Instruction file? yes/no ')
    if instruction == 'yes':
        dfx = df
        dfx['Product'] = np.where(dfx['SPOT'] == 'TRUE', 'SPOT', 'NDF')
        dfx['Direction'] = np.where(dfx['LOCAL NOTIONAL'] > 0, 'Buy', 'Sell')
        dfx['VD'] = dfx['FORWARD DATE']
        dfx = dfx[['CCY Pair', 'Product', 'Direction', 'VD',
                   'PB', 'PM', 'LOCAL NOTIONAL']]
        dfx = dfx.sort_values(by=['PB', 'CCY Pair'], ascending=False)
        # filter out NDFs for individual PMs
        dfx3 = dfx.copy(deep=True)
        dfx3 = dfx3.loc[dfx3['Product'] == 'SPOT']
        # fx reasury
        dfx1 = dfx[['CCY Pair', 'Product', 'Direction', 'VD',
                    'PB', 'PM', 'LOCAL NOTIONAL']]

        # grouping
        dfx1 = dfx1.groupby(['CCY Pair', 'PB', 'Product']).sum().reset_index()
        dfx1['Direction'] = np.where(dfx1['LOCAL NOTIONAL'] > 0, 'Buy', 'Sell')

        dfx1['VD'] = np.where(dfx1['CCY Pair'] == 'USDKRW',
                              datetime.date(int(fdate[:4]), int(fdate[4:6].lstrip('0')), int(fdate[-2:].lstrip('0'))),
                              datetime.date(int(fdate2[:4]), int(fdate2[4:6].lstrip('0')),
                                            int(fdate2[-2:].lstrip('0'))))
        # diffrentiate top net level
        dfx1['PM'] = np.where(dfx1['Product'] == 'NDF', 'FX_Treasury', 'Net Fund level')
        # dfx1['Product']='NDF'
        dfx1 = dfx1[['CCY Pair', 'Product', 'Direction', 'VD',
                     'PB', 'PM', 'LOCAL NOTIONAL']]
        dfx2 = pd.concat([dfx3, dfx1], axis=0)
        dfx2 = dfx2.sort_values(by=['PB', 'CCY Pair'], ascending=False)
        # adding
        # save instruction per PB
        PB = dfx2['PB'].unique()

        # with pd.ExcelWriter(r"P:\Operations\Polymer-FX\fx_project\funding/Instruction.xlsx",engine='xlsxwriter') as writer:
        # for n in PB:
        #   dfx2[dfx2['PB']==n].to_excel(writer,sheet_name=n,index=None)
        dfx2['Product'] = dfx2['Product'].str.replace('SPOT', 'ONSHORE')
        dfx2 = dfx2.loc[~dfx2['PM'].str.endswith('Net Fund level')]
        dfx2['LOCAL NOTIONAL'] = dfx2['LOCAL NOTIONAL'].abs()

        def add_empty_row(group):
            return group.append(pd.Series(dtype='float64'), ignore_index=True)

        dfx2 = dfx2.groupby('PB').apply(add_empty_row)
        dfx2 = dfx2.reset_index(drop=True)

        def format_float(x):
            return "{:,.2f}".format(x)

        dfx2['LOCAL NOTIONAL'] = dfx2['LOCAL NOTIONAL'].apply(format_float)
        dfx2['LOCAL NOTIONAL'] = dfx2['LOCAL NOTIONAL'].str.replace('nan', '')
        dfx2['Traded Currency'] = dfx2['CCY Pair'].str.slice(-3)
        dfx2 = dfx2.rename(columns={'PM':'Account'})
        dfx2 = dfx2[['CCY Pair', 'Product', 'Traded Currency', 'Direction', 'VD', 'PB', 'Account',
                     'LOCAL NOTIONAL']]
        dfx2.to_excel(
            r'P:\Operations\Polymer-FX/fx_project\funding/' + td.strftime('%Y%m%d') + '_T1' + 'fxinstruction.xlsx',
            sheet_name='Instruction', index=False)

    # generate account

    # # #save file
    fxt0_sub.to_excel(r'P:\Operations\Polymer-FX\fx_project\funding/' + td.strftime('%Y%m%d') + '_T1' + 'fx.xlsx',
                      sheet_name='Uploader', index=False)
    # df.to_excel(r'P:\Operations\Polymer-FX\fx_project\funding/FX Uploader v2.xlsx',sheet_name='fundinginput',index=False)

    # =================


# script2.py
def FX_generating_T0():
    # code for function 2
    import pandas as pd
    import numpy as np
    import datetime
    from pandas.tseries.offsets import BDay
    krw = r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingkrw.xlsx'
    twd = r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingtwd.xlsx'
    krw_sub = r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingkrw_sub.xlsx'
    twd_sub = r'P:\Operations\Polymer-FX\fx_project\funding\t0fundingtwd_sub.xlsx'
    pbs = {'GSCO':"GS_PB", 'GSCO_FPI':"GS_FPI", 'JPM':"JPM_FX", 'UBS':"UBS_FX",
           'MLCO':"BAML_FX", 'MSCO':"MS_FX"}
    bunitmap = pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv",
                           engine='python')
    bookid = pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\Subbunit mapping.csv")
    acc = pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\account mapping.csv")

    # empty_series = pd.Series(dtype=float)
    def read_excel(krw, currency, f_date):
        dfk = pd.read_excel(krw, header=1)
        if dfk.empty == False:
            dfk = dfk.iloc[1:-1, :]
            dfk = dfk[dfk.columns[:-1]]
            # print(dfk)
            # dfk = dfk.unstack(['CustodianShortName', 'GSCO', 'MLCO', 'UBS'])
            dfk = dfk.set_index('CustodianShortName')
            dfk = dfk.stack()
            dfk = dfk.reset_index()

            dfk['FORWARD DATE'] = date + BDay(delta)
            if currency in ['TWD', 'twd']:
                dfk['FORWARD DATE'] = date + BDay(delta)
                dfk['FORWARD DATE'] = dfk['FORWARD DATE'].apply(lambda x:datetime.date(x.year, x.month, x.day))
            if currency in ['KRW', 'krw']:
                dfk['FORWARD DATE'] = date + BDay(delta + 1)
                dfk['FORWARD DATE'] = dfk['FORWARD DATE'].apply(lambda x:datetime.date(x.year, x.month, x.day))
            dfk['Currency'] = currency

            dfk['RATE'] = 0
            dfk['USD Notional'] = np.NaN
            dfk['LOCAL NOTIONAL'] = -dfk[0]
            dfk['PM'] = dfk['CustodianShortName']
            dfk = dfk.merge(bunitmap, left_on='PM', right_on='Sub Business Unit', how='left')
            dfk = dfk[dfk[0] != 0]
            dfk['PB'] = dfk['level_1'].apply(lambda x:pbs[x])
            dfk['SPOT'] = 'TRUE'
            dfk['Product'] = 'SPOT'
            dfk['CCY Pair'] = 'USD' + dfk['Currency']
            dfk = dfk[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM',
                       'PB', 'RATE', 'USD Notional', 'LOCAL NOTIONAL', 'Business Unit']]
            dfk_copy = dfk.copy()
            dfk_copy['SPOT'] = 'FALSE'
            dfk_copy['Product'] = 'NDF'
            dfk_copy['LOCAL NOTIONAL'] = -dfk_copy['LOCAL NOTIONAL']

            dfk_copy['FORWARD DATE'] = datetime.datetime.strptime(f_date, '%Y%m%d').date()

            dfk = pd.concat([dfk, dfk_copy], axis=0)
            # dfk = dfk.reset_index()
            dfk = dfk.sort_index()
            dfk['Column1'] = 'POLY_' + dfk['PM'] + '_' + dfk['PB']
            return dfk

    # dfk.to_excel('fx.xlsx',index=False)
    fdate = input('please enter krw forward date(example:20210731):')
    fdate2 = input('please enter twd forward date(example:20210731):')

    # fdate = fdate[:4]+'/' + fdate[4:6] + '/' + fdate[-2:]
    today = datetime.datetime.today()
    td = today - datetime.timedelta(days=0)
    date = datetime.date(td.year, td.month, td.day)
    # mark spot vd with day+delta, if t0, krw t+1 and twd t+0
    delta = 1

    df_twd = read_excel(twd, 'TWD', fdate2)
    df_krw = read_excel(krw, 'KRW', fdate)
    df_twd_sub = read_excel(twd_sub, 'TWD', fdate2)
    df_krw_sub = read_excel(krw_sub, 'KRW', fdate)

    df = pd.concat([df_krw, df_twd], axis=0)
    df_sub = pd.concat([df_krw_sub, df_twd_sub], axis=0)

    def massage_fx_trades(df):
        df['PM'] = np.where(df['SPOT'] == 'TRUE', df['PM'], 'EXT_FX')
        df = df.sort_values(by=['PB', 'CCY Pair'], ascending=False)

        # Mappbookid, account
        df = df.merge(bookid, left_on='PM', right_on='Sub Business Unit', how='left')
        # df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
        #         'USD Notional', 'LOCAL NOTIONAL','Account','Business Unit','Book ID']]
        df['Account'] = np.where(
            df['PM'] == 'EXT_FX',
            'POLY_EXT_FX' + '_' + df['PB'],
            'POLY_' + df['Business Unit'] + '_' + df['PB']

        )
        df = df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
                 'USD Notional', 'LOCAL NOTIONAL', 'Account', 'Business Unit', 'Book ID']]

        # df=df.merge(bookid, left_on='PM', right_on='Sub Business Unit',how='inner')
        # map book ID and generate

        df['Product'] = np.where(df['SPOT'] == 'TRUE', 'SPOT', 'NDF')
        pm = df['PM']
        df = df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
                 'USD Notional', 'LOCAL NOTIONAL', 'Account', 'Book ID']]
        df = df.merge(acc, left_on='Account', right_on='ACCOUNT ', how='left')
        df['Account ID'] = df['ID']
        df['Rate'] = str('Paste from recap')
        FX_trades = df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB',
                        'USD Notional', 'LOCAL NOTIONAL', 'Rate', 'Book ID', 'Account', 'Account ID']]
        return FX_trades

    fxt0 = massage_fx_trades(df)
    fxt0_sub = massage_fx_trades(df_sub)

    # adding instruction matrix
    # idk= input('Today is Monday? yes or no ')
    instruction = input('Generate Instruction file? yes/no ')
    if instruction == 'yes':
        dfx = df
        dfx['Product'] = np.where(dfx['SPOT'] == 'TRUE', 'SPOT', 'NDF')
        dfx['Direction'] = np.where(dfx['LOCAL NOTIONAL'] > 0, 'Buy', 'Sell')
        dfx['VD'] = dfx['FORWARD DATE']
        dfx = dfx[['CCY Pair', 'Product', 'Currency', 'Direction', 'VD',
                   'PB', 'PM', 'LOCAL NOTIONAL']]
        dfx = dfx.sort_values(by=['PB', 'CCY Pair'], ascending=False)
        # filter out NDFs for individual PMs
        dfx3 = dfx.copy(deep=True)
        dfx3 = dfx3.loc[dfx3['Product'] == 'SPOT']
        # fx reasury
        dfx1 = dfx[['CCY Pair', 'Product', 'Direction', 'VD',
                    'PB', 'PM', 'LOCAL NOTIONAL']]

        # grouping
        dfx1 = dfx1.groupby(['CCY Pair', 'PB', 'Product']).sum().reset_index()
        dfx1['Direction'] = np.where(dfx1['LOCAL NOTIONAL'] > 0, 'Buy', 'Sell')

        dfx1['VD'] = np.where(dfx1['CCY Pair'] == 'USDKRW',
                              datetime.date(int(fdate[:4]), int(fdate[4:6].lstrip('0')), int(fdate[-2:].lstrip('0'))),
                              datetime.date(int(fdate2[:4]), int(fdate2[4:6].lstrip('0')),
                                            int(fdate2[-2:].lstrip('0'))))
        # diffrentiate top net level
        dfx1['PM'] = np.where(dfx1['Product'] == 'NDF', 'FX_Treasury', 'Net Fund level')
        # dfx1['Product']='NDF'
        dfx1 = dfx1[['CCY Pair', 'Product', 'Direction', 'VD',
                     'PB', 'PM', 'LOCAL NOTIONAL']]
        dfx2 = pd.concat([dfx3, dfx1], axis=0)
        dfx2 = dfx2.sort_values(by=['PB', 'CCY Pair'], ascending=False)
        # adding
        # save instruction per PB
        PB = dfx2['PB'].unique()

        # with pd.ExcelWriter(r"P:\Operations\Polymer-FX\fx_project\funding/Instruction.xlsx",engine='xlsxwriter') as writer:
        # for n in PB:
        #   dfx2[dfx2['PB']==n].to_excel(writer,sheet_name=n,index=None)
        dfx2['Product'] = dfx2['Product'].str.replace('SPOT', 'ONSHORE')
        dfx2 = dfx2.loc[~dfx2['PM'].str.endswith('Net Fund level')]
        dfx2['LOCAL NOTIONAL'] = dfx2['LOCAL NOTIONAL'].abs()

        def add_empty_row(group):
            return group.append(pd.Series(dtype='float64'), ignore_index=True)

        dfx2 = dfx2.groupby('PB').apply(add_empty_row)
        dfx2 = dfx2.reset_index(drop=True)

        def format_float(x):
            return "{:,.2f}".format(x)

        dfx2['LOCAL NOTIONAL'] = dfx2['LOCAL NOTIONAL'].apply(format_float)
        dfx2['LOCAL NOTIONAL'] = dfx2['LOCAL NOTIONAL'].str.replace('nan', '')
        dfx2['Traded Currency'] = dfx2['CCY Pair'].str.slice(-3)
        dfx2 = dfx2.rename(columns={'PM':'Account'})
        dfx2 = dfx2[['CCY Pair', 'Product', 'Traded Currency', 'Direction', 'VD', 'PB', 'Account',
                     'LOCAL NOTIONAL']]
        dfx2.to_excel(
            r'P:\Operations\Polymer-FX/fx_project\funding/' + td.strftime('%Y%m%d') + '_T0' + 'fxinstruction.xlsx',
            sheet_name='Instruction', index=False)

    # generate account

    # # #save file
    fxt0_sub.to_excel(r'P:\Operations\Polymer-FX\fx_project\funding/' + td.strftime('%Y%m%d') + '_T0' + 'fx.xlsx',
                      sheet_name='Uploader', index=False)
    # df.to_excel(r'P:\Operations\Polymer-FX\fx_project\funding/FX Uploader v2.xlsx',sheet_name='fundinginput',index=False)

    # =================


# input election to run def FX_generating_T1() or def FX_generating_T0()
election2 = input('Generating instruction T0 or T1? ')
if election2 == 't0':
    FX_generating_T0()
elif election2 == 't1':
    FX_generating_T1()
else:
    print('wrong input')
    sys.exit()
