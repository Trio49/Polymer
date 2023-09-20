# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 10:03:33 2021

@author: zzhang

load TORA FX
"""

import numpy as np
import pandas as pd
import pytz
# import jsonSMART
# import pprint
# import requests
from datetime import datetime, timedelta
import win32com.client as win32
import matplotlib.pyplot as plt
from os import listdir

from pandas import Series

pd.set_option('mode.chained_assignment', None)

today = datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially
today = today.date()
# today = today - timedelta(days=1)
# today=today.date()
today = today.strftime('%Y%m%d')

# all_PM_expiryblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_expiryblotter_file_path = r"\\paghk.local\Polymer\HK\Department\All\Enfusion\Polymer_reports\Tora"
# all_PM_expiryblotter_file_path=r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\"+today
firm_positions_list_dir = listdir(all_PM_expiryblotter_file_path)
firm_positions_list_dir1 = firm_positions_list_dir[::-1]

# create a dictionary to load POLYMER_EOD file at 1415,1515,1615,1715 and 1815
file_dict = {'1':'POLYMER_EOD_1415',
             '2':'POLYMER_EOD_1515',
             '3':'POLYMER_EOD_1615',
             '4':'POLYMER_EOD_1715',
             '5':'POLYMER_EOD_1815'}

print(file_dict)
'POLYMER_EOD_1815_20230919.csv'
file = input("please choose the file: ")
# select the string based on the input of file
file_name_elect = file_dict[file]
# define the trade_file based on input
for file_name in firm_positions_list_dir1:
    if file_name_elect in file_name:
        # print(file_name)
        trade_file: str = all_PM_expiryblotter_file_path + "\\" + file_name
        break
        print(trade_file)
trade_file = all_PM_expiryblotter_file_path + "\\" + file_dict[file] + "_" + today + ".csv"
# check if the file is in the folder
# if trade_file not in firm_positions_list_dir:
#     input('can not find file' + trade_file)
#     system.exit()
# else:
print("loading file"+"   "+trade_file)

filecheck = input('are files correct? y/n')
# pause the program if the files are not correct
if filecheck == 'y':
    trades = pd.read_csv(trade_file)
trades_swap = trades[['Creation', 'BBG Code', 'CCY', 'Side', 'Execution Quantity', 'Broker', 'Syn Flag', 'Product Type',
                      'Broker FXRate']]

# check if all the creation date is unique


trades_swap = trades_swap[trades_swap['Syn Flag'] == 'swp']
#nottoday is a dataframe where trades_swap['Creation']!=today
today_int = int(today)
nottoday=trades_swap[trades_swap['Creation']!=today_int]
nottoday_list=nottoday['BBG Code'].unique()
nottoday_list=nottoday_list.tolist()
nottoday_list=str(nottoday_list)
nottoday_list='These trades are not creation date today: '+nottoday_list

# filter out trades with Creation as today
print("today is_" + str(today))
# trades_swap=trades_swap[trades_swap['Creation']==today]
# set Broker FXRate as string
trades_swap['Broker FXRate'] = trades_swap['Broker FXRate'].astype(str)
# join the implied_fxrate unique value with a comma
trades_swap_brokerfx = trades_swap.groupby(['Broker', 'CCY']).agg({'Broker FXRate':lambda x:','.join(x)}).reset_index()
#change Broker to Upper case
trades_swap_brokerfx['Broker']=trades_swap_brokerfx['Broker'].str.upper()
# get the unique value of Implied_FXRate
trades_swap_brokerfx['Broker FXRate'] = trades_swap_brokerfx['Broker FXRate'].str.split(',').str[0]
trades_swap_brokerfx_valid = trades_swap_brokerfx[trades_swap_brokerfx['Broker FXRate'] != '0.0']
#replace CNH under CCY to CNH(Connect), CNY to CNY(QFII)
trades_swap_brokerfx_valid['CCY']=trades_swap_brokerfx_valid['CCY'].replace('CNH','CNH(Connect)')
trades_swap_brokerfx_valid['CCY']=trades_swap_brokerfx_valid['CCY'].replace('CNY','CNY(QFII)')
#change file_name column to the string between POLYMER_EOD_ and .csv
trades_swap_brokerfx_valid['file_name']=file_name_elect
#increase the font size of the dataframe
pd.set_option('display.max_colwidth', 100)

# drop an email to polymerops@polymercapital.com with trades_swap_brokerfx_valid in the email body
# save the trades_swap_brokerfx_valid to a csv file
trades_swap_brokerfx_valid.to_excel(
    r"P:\Operations\Polymer - Middle Office\TORA_swapFX_loader\trades_swap_brokerfx_valid.xlsx",
    index=False)

with pd.ExcelWriter(
        r"P:\Operations\Polymer - Middle Office\TORA_swapFX_loader\trades_swap_brokerfx_valid.xlsx",
        engine='xlsxwriter') as writer:
    trades_swap_brokerfx_valid.to_excel(writer, sheet_name="sheet1", index=None)
    # # add the file_name to the first row of trades_swap_brokerfx_valid
    # trades_swap_brokerfx_valid.insert(0, 'file_name', file_name)

html = trades_swap_brokerfx_valid.to_html(index=False)
text_file = open("P:\Operations\Polymer - Middle Office\TORA_swapFX_loader/mail.htm", 'w')

text_file.write(html)
text_file.close()

outlook = win32.Dispatch('Outlook.Application')
mail = outlook.CreateItem(0)

# mail.to = 'polymerops@polymercapital.com'
mail.to = 'zzhang@polymercapital.com'
# mail.to = 'polymertrading@polymercapital.com'
# mail.cc='polymerops@polymercapital.com'

sub = 'TORA_Broker_feed_FX  ' + today
# sub='PTH report testing'

mail.Subject = sub
mail.Body = 'Message body testing'

# HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
HtmlFile = open("P:\Operations\Polymer - Middle Office\TORA_swapFX_loader/mail.htm",'r')
source_code = HtmlFile.read()
#add the nottoday_list to the email body behind the source_code after 3 empty rows
source_code=source_code+'\n'+'\n'+'\n'+nottoday_list

HtmlFile.close()
# html=wb.to_html(classes='table table-striped')
# html=firm_positions.to_html(classes='table table-striped')
mail.HTMLBody = source_code
mail.Send()
#
