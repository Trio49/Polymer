# -*- coding: utf-8 -*-
"""
Created on Thu Mar  9 14:40:40 2023

@author: zzhang
"""
# import numpy as np
# import pdblp
from datetime import datetime, timedelta
from os import listdir

import pandas as pd
import pytz

today = datetime.now(pytz.timezone('Asia/Shanghai'))
today = today.date() - timedelta(days=0)
today_text = today.strftime('%Y%m%d')

############remove when running officially 
# today=today.strftime('%Y%m%d')
# today=today.date()-timedelta(days=1)
# today=today.date()
today = today.strftime('%Y%m%d')

# all_PM_expiryblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_expiryblotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today

firm_positions_list_dir = listdir(all_PM_expiryblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:

    if "Polymer_OP" in file_name:
        print(file_name)

        all_PM_expiryblotter_file_path = all_PM_expiryblotter_file_path + "\\" + file_name
        break
OP_pos = pd.read_csv(all_PM_expiryblotter_file_path, engine='python', thousands=',')
OP_pos['PM'] = OP_pos['Book Name'].str[-5:]
replace_dict = {'BAML':'baml', 'Goldman Sachs':'gs', 'JP Morgan':'jpm', 'UBS':'ubs'}
OP_pos['Prime'] = OP_pos['Prime'].replace(replace_dict, regex=True)
OP_pos['ID2'] = OP_pos['PM'] + OP_pos['Prime'] + OP_pos['Underlying BB Yellow Key']
OP_pos = OP_pos[
    ['ID2', 'Underlying BB Yellow Key', 'Position', 'Custodian Account Display Name', 'Deal Id', 'Instrument Id']]
Unwind_trades = pd.read_csv(
    '//paghk.local/Polymer/Department/All/FinanceTradingMO/outperformance swap/Daily booking/' + today_text + '_unwind_OP_trades.csv',
    engine='python', thousands=',')
Unwind_trades['BBcode'] = Unwind_trades['BBG Code'] + ' ' + 'Equity'
Unwind_trades['Internal Account'] = Unwind_trades['Internal Account'].str[-5:]
Unwind_trades['ID'] = Unwind_trades['Internal Account'] + Unwind_trades['Broker'] + Unwind_trades['BBcode']
# check = input('is the PTH file latest? yes/no: ')
Unwind_trades = Unwind_trades[['Internal Account', 'BBcode', 'Broker', 'unwind_op_quantity', 'ID']]
Unwind_file = pd.merge(Unwind_trades.reset_index(), OP_pos, left_on='ID', right_on='ID2', how='left').set_index('index')
Unwind_file = Unwind_file[['Internal Account', 'Broker', 'BBcode', 'unwind_op_quantity', 'Underlying BB Yellow Key',
                           'Custodian Account Display Name', 'Position', 'Deal Id',
                           'Instrument Id']]

Unwind_file.to_csv(r"P:\All\FinanceTradingMO\outperformance swap\Daily booking\unwind" + today + ".csv", index=False,
                   line_terminator='\n')
