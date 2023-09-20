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
# define the op position path and report name
for file_name in firm_positions_list_dir1:

    if "Polymer_OP" in file_name:
        print(file_name)

        all_PM_expiryblotter_file_path = all_PM_expiryblotter_file_path + "\\" + file_name
        break
# read OP position file
OP_pos = pd.read_csv(all_PM_expiryblotter_file_path, engine='python', thousands=',')
OP_pos['PM'] = OP_pos['Book Name'].str[-5:]
replace_dict = {'BAML':'baml', 'Goldman Sachs':'gs', 'JP Morgan':'jpm', 'UBS':'ubs'}
OP_pos['Prime'] = OP_pos['Prime'].replace(replace_dict, regex=True)
OP_pos['ID2'] = OP_pos['PM'] + OP_pos['Prime'] + OP_pos['Underlying BB Yellow Key']
#keep the columns we need
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
#remove trades with 0 unwind_op_quantity
Unwind_trades = Unwind_trades[Unwind_trades['unwind_op_quantity'] != 0]
# sum the unwind_op_quantity with the same ID, Internal Account, Broker, BBcode
Unwind_trades_X = Unwind_trades.groupby(['ID', 'Internal Account', 'Broker', 'BBcode']).sum()
Unwind_trades_X = Unwind_trades_X.reset_index()
# cross check Unwind_trades_X ID with OP_pos ID2,if there is any ID matched, for each matched ID, minus the unwind_op_quantity from the position and record each value deducted
Unwind_trades_X['Unwind_op_quantity_total'] = Unwind_trades_X['unwind_op_quantity']
#Create a new dataframe to store the deduction row with deduction quantity,internal account, broker, BBcode, deal id and instrument id
Unwind_trades_deduction = pd.DataFrame(columns=[ 'Internal Account', 'Broker', 'BBcode','deduction','Custodian Account Display Name', 'Deal Id',
                                                'Instrument Id'])
#create a deduction column under Unwind_trades_X
Unwind_trades_X['deduction'] = 0
#create a deal id column under Unwind_trades_X
Unwind_trades_X['Deal Id'] = ''
#create a instrument id column under Unwind_trades_X
Unwind_trades_X['Instrument Id'] = ''
#create a column to store Custodian Account Display Name
Unwind_trades_X['Custodian Account Display Name'] = ''
for i in range(len(Unwind_trades_X)):
    for j in range(len(OP_pos)):
        #if the ID in Unwind_trades_X matches the ID2 in OP_pos, and the unwind_op_quantity is greater than the position, then minus the position from the unwind_op_quantity and record the value deducted
        if Unwind_trades_X['ID'][i] == OP_pos['ID2'][j]:
            #if the unwind_op_quantity is greater than the position, then minus the position from the unwind_op_quantity and record the value deducted
            #append the deal id and instrument id to the Unwind_trades_X and save the row to a new dataframe

          if Unwind_trades_X['unwind_op_quantity'][i] >= OP_pos['Position'][j]:
            Unwind_trades_X['unwind_op_quantity'][i] = Unwind_trades_X['unwind_op_quantity'][i] - \
                                                              OP_pos['Position'][j]
            Unwind_trades_X['deduction'][i] = OP_pos['Position'][j]
            #inherit custodian account display name, deal id and instrument id from OP_pos
            Unwind_trades_X['Deal Id'][i] = OP_pos['Deal Id'][j]
            Unwind_trades_X['Instrument Id'][i] = OP_pos['Instrument Id'][j]
            Unwind_trades_X['Custodian Account Display Name'][i] = OP_pos['Custodian Account Display Name'][j]
            #append ['deduction', 'Internal Account', 'Broker', 'BBcode', 'Deal Id','Instrument Id'] to the Unwind_trades_deduction
            Unwind_trades_deduction = Unwind_trades_deduction.append(Unwind_trades_X.iloc[i])


          elif Unwind_trades_X['unwind_op_quantity'][i] < OP_pos['Position'][j]:
                #if the unwind_op_quantity is less than the position, then minus the unwind_op_quantity from the position and record the value deducted
            Unwind_trades_X['deduction'][i] = Unwind_trades_X['unwind_op_quantity'][i]
            Unwind_trades_X['Deal Id'][i] = OP_pos['Deal Id'][j]
            Unwind_trades_X['Instrument Id'][i] = OP_pos['Instrument Id'][j]
            Unwind_trades_X['Custodian Account Display Name'][i] = OP_pos['Custodian Account Display Name'][j]

                # append ['deduction', 'Internal Account', 'Broker', 'BBcode', 'Deal Id','Instrument Id'] to the Unwind_trades_deduction
            Unwind_trades_deduction = Unwind_trades_deduction.append(Unwind_trades_X.iloc[i])

            break


#remove the rows with 0 deduction from Unwind_trades_deduction
Unwind_trades_deduction = Unwind_trades_deduction[Unwind_trades_deduction['deduction'] != 0]
#save the file
Unwind_trades_deduction.to_csv(r"P:\All\FinanceTradingMO\outperformance swap\Daily booking\unwind_new" + today +'.csv')

#convert the unwind_trades_deduction into an Enfusion consumable format
#create a dataframe to store the data with extra columns
Unwind_trades_deduction_Enfusion = pd.DataFrame(columns=['Account', 'Broker', 'Symbol', 'Quantity', 'Deal Id', 'Instrument Id'])
#example
# example=pd.read_csv(r'P:\All\FinanceTradingMO\outperformance swap\Daily booking/)
