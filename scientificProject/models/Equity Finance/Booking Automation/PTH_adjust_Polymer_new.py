# -*- coding: utf-8 -*-
"""
Created on Oct 22 2020

@author: zzhang
"""

# Last edited by:
# Zach Zhang 06222021
# PTH update model  03222020 
import pytz
import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from os import listdir

pd.set_option('display.max_columns', 20)

today = datetime.now(pytz.timezone('Asia/Shanghai'))
# =============================================================================
# =============================================================================
# today=today.date()
a=input("time delta:")
a=int(a)
today = today.date() - timedelta(days=a)
# 

today = today.strftime('%Y%m%d')


def firm_positions():
    # read the FIRM FLAT positions from some place!
    # firm_positions_file_path="//paghk.local/shares/Enfusion/Polymer/"+today
    firm_positions_file_path = r"P:\All\Enfusion\Polymer/"
    firm_positions_list_dir = listdir(firm_positions_file_path)

    firm_positions_list_dir = firm_positions_list_dir[::-1]
    for file_name in firm_positions_list_dir:
        if "PTH REPORT_PTH" in file_name:
            firm_positions_file_path = firm_positions_file_path + "\\" + file_name
            print(firm_positions_file_path)
            break

    ####tyin
    firm_positions = pd.read_csv(firm_positions_file_path)
    # firm_positions = pd.read_csv("\\paghk.local\shares\Enfusion\Polymer\" + today)
    #
    # firm_positions_path=r"C:\Users\tyin\Documents\report_FIRM_FLAT_TYIN0322.csv"
    #    firm_positions=pd.read_csv(firm_positions_path)
    firm_positions = firm_positions.iloc[1:]
    firm_positions.describe()
    firm_positions = firm_positions[firm_positions["Book Name"].str.contains("PTH")]
    firm_positions = firm_positions[firm_positions["Position"] != 0]
    firm_positions = firm_positions[["Sub Business Unit",
                                     "Book Name",
                                     "Underlying BB Yellow Key",
                                     "Description",
                                     "Position",
                                     "Custodian Account Display Name",
                                     "Last Trade Date",
                                     "Deal Id",
                                     "Active Coupon Rate",
                                     "Instrument Id"]]

    # to the sub-PM level to match with trade blotter strategy column
    firm_positions["strats"] = firm_positions["Book Name"].apply(lambda x:x.split("(")[1].replace(")", ""))
    firm_positions["Product"] = np.where(firm_positions['Custodian Account Display Name'].str.contains('ISDA'), 'swap',
                                         'cash')

    # firm_positions

    firm_positions["Custodian_match"] = firm_positions["Custodian Account Display Name"].apply(
        lambda x:tuple(x.split("_")[1:3]))
    return (firm_positions)


firm_positions = firm_positions()
firm_positions["ticker_PM"] = firm_positions["Underlying BB Yellow Key"] + " " + firm_positions["strats"]
firm_positions["Product"] = np.where(firm_positions['Custodian Account Display Name'].str.contains('ISDA'),
                                     'swap', 'cash')
ticker_PM_list = firm_positions["ticker_PM"].tolist()
######################################################################## list all existing PM positions for those PTH names
whole = firm_positions


#

########################################################################

# read the REPORT_Polymer_Blotter_Tony_202
def Trade_Blotter():
    # Trade_Blotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
    Trade_Blotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today
    list_dir = listdir(Trade_Blotter_file_path)

    list_dir = list_dir[::-1]

    for file_name in list_dir:
        if "Polymer_Blotter_Tony" in file_name:
            Trade_Blotter_file_path = Trade_Blotter_file_path + "\\" + file_name
            break

    Trade_Blotter = pd.read_csv(Trade_Blotter_file_path)
    Trade_Blotter = Trade_Blotter.iloc[1:]

    Trade_Blotter_1 = Trade_Blotter[["Transaction Type",
                                     "Custodian Name",
                                     "BB Yellow Key",
                                     "Notional Quantity",
                                     "Sub Business Unit",
                                     "Trade Id",
                                     "Strategy"]]

    Trade_Blotter_1["Custodian_match_name"] = Trade_Blotter_1["Custodian Name"].apply(lambda x:tuple(x.split("_")[1:3]))
    return (Trade_Blotter_1)


Trade_Blotter = Trade_Blotter()

Trade_Blotter.head(10)

net = Trade_Blotter[Trade_Blotter["Transaction Type"] == "SellShort"]

# net["Custodian_match_name"]=net["Custodian Account Display Name"].apply(lambda x:tuple(x.split("_")[2:3]))
net["Rate"] = 0

# Add ID to combine custodian account display name and underlying bb yellow key
net["ID"] = net["Custodian Name"] + net["BB Yellow Key"]
# remove empty ID rows
net = net[net["ID"] != ""]
# change notional quantity to absolute value
net["Notional Quantity"] = net["Notional Quantity"].abs()

# filter PTH position from whole by containing string Loan/Borrow
PTHs = whole[whole["Description"].str.contains("Loan/Borrow")]
#
# combining Custodian Account Display Name and DW Underlying Ticker to create ID
PTHs["ID2"] = PTHs["Custodian Account Display Name"] + PTHs["Underlying BB Yellow Key"]
# Sort PTHs by Ticker_PM
PTHs.sort_values(by=["ticker_PM"], inplace=True)
# PTHs position changed to abs value
PTHs["Position"] = PTHs["Position"].abs()
# keep the PTHs columns needed
PTHs = PTHs[["Underlying BB Yellow Key", "Description", "Position", "Custodian Account Display Name", "Deal Id",
             "Instrument Id", "ID2"]]
# save a list of unique value of Underlying BB Yellow Key
PTH_ticker_list = PTHs["Underlying BB Yellow Key"].unique().tolist()
# Create a new dataframe call unwind_pth to copy net
unwind_pth = net.copy()
# filter unwind_pth by PTH_ticker_list
unwind_pth = unwind_pth[unwind_pth["BB Yellow Key"].isin(PTH_ticker_list)]
# remove empty bb yellow key
unwind_pth = unwind_pth[unwind_pth["BB Yellow Key"] != ""]
# remove empty custodian account display name
unwind_pth = unwind_pth[unwind_pth["Custodian Name"] != ""]
#unwind_pth quantity change to abs value
unwind_pth["Notional Quantity"] = unwind_pth["Notional Quantity"].abs()
# add new columns
unwind_pth['deduction'] = ''
unwind_pth['Deal Id'] = ''
unwind_pth['Instrument Id'] = ''
# create a new dataframe to record unwind_pth_booking
unwind_pth_booking = pd.DataFrame()
#keep a column to record the original notional quantity
unwind_pth['original_notional_quantity'] = unwind_pth['Notional Quantity']
#reset index for PTHs and unwind_pth
PTHs.reset_index(drop=True, inplace=True)
unwind_pth.reset_index(drop=True, inplace=True)
#sort PTHs by underlying bb yellow key then by position,desending
PTHs.sort_values(by=["Underlying BB Yellow Key", "Position"], ascending=False, inplace=True)
# loop through unwind_pth and PTHs
for i in range(len(unwind_pth)):
    for j in range(len(PTHs)):
        # if ID and ID2 match and PTHs unwind_pth position is greater than PTHs position, the
        if unwind_pth['ID'][i] == PTHs['ID2'][j]:
            #if notional quantity is greater than PTHs position, deduction is PTHs position and notional quantity is notional quantity minus PTHs position
            #if notional quantity is less than PTHs position, deduction is notional quantity
            if unwind_pth['Notional Quantity'][i] > PTHs['Position'][j]:
                unwind_pth['Notional Quantity'][i]=unwind_pth['Notional Quantity'][i]-PTHs['Position'][j]
                unwind_pth['deduction'][i] = PTHs['Position'][j]
                unwind_pth['Deal Id'][i] = PTHs['Deal Id'][j]
                unwind_pth['Instrument Id'][i] = PTHs['Instrument Id'][j]
                #add the deduction to a new dataframe called unwind_pth_booking
                unwind_pth_booking = unwind_pth_booking.append(unwind_pth.iloc[i])
            elif unwind_pth['Notional Quantity'][i]<=PTHs['Position'][j]:
                unwind_pth['deduction'][i] = unwind_pth['Notional Quantity'][i]
                unwind_pth['Deal Id'][i] = PTHs['Deal Id'][j]
                unwind_pth['Instrument Id'][i] = PTHs['Instrument Id'][j]
                # add the deduction to a new dataframe called unwind_pth_booking
                unwind_pth_booking = unwind_pth_booking.append(unwind_pth.iloc[i])
            elif unwind_pth['Notional Quantity'][i] == 0:
                break

                break

#get the string before "_" in Sub Business Unit
unwind_pth_booking['Strat'] = unwind_pth_booking['Sub Business Unit'].apply(lambda x: x.split("_")[0])
#if Strt has "PO" replaced it to ECMIPO
unwind_pth_booking['Strat'] = unwind_pth_booking['Strat'].apply(lambda x: x.replace("PO", "ECMIPO"))
#get the custodian name from the string between the two "_" in Custodian_match_name
unwind_pth_booking['Custodian'] = unwind_pth_booking['Custodian_match_name'].apply(lambda x: x[1])
#keep the columns needed
unwind_pth_booking = unwind_pth_booking[['Strat',
                                         'Custodian',
                                         'BB Yellow Key',
                                         'deduction',
                                         'Rate',
                                         'Deal Id',
                                         'Instrument Id']]

unwind_pth_booking.to_csv(r'P:\Operations\Polymer - Middle Office\PTH and FX booking\PTH unwind/PTH unwind'+today+'.csv', index=False)