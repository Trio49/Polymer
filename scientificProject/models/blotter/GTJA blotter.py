from datetime import datetime, timedelta
from os import listdir

import pandas as pd
import pytz
import matplotlib.pyplot as plt
import pytz
import pandas as pd
import numpy as np
import openpyxl
#import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from os import listdir
from openpyxl import load_workbook
import win32com.client as win32
from IPython.display import HTML
#read all trades at EOD
def GTJA_data_dump():
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
                         '$ Notional Quantity * Trade Price', 'Instrument Type','TRADE Settle CCY FXRate',
                         '$ Trading Notional Net Proceeds']]
    Alltrade = Alltrade.loc[Alltrade['CustodianShortName'].str.contains('GTJA')]
    Alltrade.keys()
    # save the columns
    alltrade1 = Alltrade[
        ['BB Yellow Key', 'Trade Date', 'Transaction Type', 'Notional Quantity', 'Trade Price', 'TRADE Settle CCY FXRate',
         '$ Notional Quantity * Trade Price', '$ Trading Notional Net Proceeds']]
    # Rename 'Trade/Book FX Rate' as FX
    # Rename '$ Notional Quantity * Trade Price' as 'Notional'
    # calculate the 'Notional'-'Trading Notional Proceeds' as 'Sum of trading cost'
    alltrade1.rename(columns={'TRADE Settle CCY FXRate':'FX', '$ Notional Quantity * Trade Price':'Notional',
                              '$ Trading Notional Net Proceeds':'Net Notional'}, inplace=True)
    # take abs value of Notional as Notional
    alltrade1['Notional'] = abs(alltrade1['Notional'])
    alltrade1['Net Notional'] = abs(alltrade1['Net Notional'])
    alltrade1['Sum of trading cost'] = alltrade1['Notional'] - alltrade1['Net Notional']
    # abs of 'Sum of trading cost'
    alltrade1['Sum of trading cost'] = abs(alltrade1['Sum of trading cost'])
    return Alltrade,alltrade1

Alltrade,alltrade1=GTJA_data_dump()
#round 'Notional','Net Notional' and Sum of trading cost to 0 decimal places
#remove decimal places
alltrade1['Notional'] = alltrade1['Notional'].round(0)
alltrade1['Net Notional'] = alltrade1['Net Notional'].round(0)
alltrade1['Sum of trading cost'] = alltrade1['Sum of trading cost'].round(0)
#remove the decimal places
alltrade1['Notional'] = alltrade1['Notional'].astype(int)
alltrade1['Net Notional'] = alltrade1['Net Notional'].astype(int)
alltrade1['Sum of trading cost'] = alltrade1['Sum of trading cost'].astype(int)
alltrade1['FX']=1/alltrade1['FX']

# =============================================================================
#send an email including alltrade1 dataframe to the email list
#add a confirmation to send email
confirmation=input("Do you want to send the email? yes/no: ")
if confirmation=='yes':
    #send email
    with pd.ExcelWriter(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter/GTJA_blotter.xlsx",
                        engine='xlsxwriter') as writer:
        # for n in pmlist():
        alltrade1.to_excel(writer, sheet_name="sheet1", index=None)
    today=datetime.now(pytz.timezone('Asia/Shanghai'))
    today=today.strftime('%Y%m%d')
    html = alltrade1.to_html(index=False)
    text_file = open("P:\Operations\Polymer - Middle Office\Python_Trade_Blotter/expiry.htm", 'w')
    text_file.write(html)
    text_file.close()
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
    mail.to = 'isd.d1trading@gtjas.com.hk; isd.eqt@gtjas.com.hk;'
    mail.cc = 'polymerops@polymercapital.com;jade.yin@gtjas.com.hk'
    # mail.to='zzhang@polymercapital.com'
    sub = "POLYxGTJA Blotter  " + today
    mail.Subject = sub
    #add Thanks and Regards at the end of the email
    mail.HTMLBody = html + "<br><br>Thanks and Regards,<br>Zach"
    # mail.Send()
    # mail.Body = 'Message body testing'
    Htmlfile = open(r'P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\expiry.htm', 'r')
    source_code = Htmlfile.read()
    mail.Attachments.Add(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter/GTJA_blotter.xlsx")
    Htmlfile.close()
    # mail.HTMLBody = source_code
    mail.Send()
else:
    print("No email sent")

