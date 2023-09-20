 # -*- coding: utf-8 -*-
"""
Created on Fri Feb  4 08:17:36 2022

@author: dchau
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Aug 19 15:17:59 2020

@author: 
"""

#raw_pos_group_gmv_df

#import pdblp
#con = pdblp.BCon(debug=True, port=8194, timeout=60000)
#con.start()

#USDINR1= con.ref("USDINR REGN Curncy",'PX_LAST')
#USDINR = USDINR1["value"].iloc[0]
import os


USDINR = 79

assumed_ssf_notional ={"DSING":15000000,
                       "SLEUN":7000000,                
                }
import pytz
import numpy as np
import pandas as pd
#import jsonSMART
from datetime import datetime, timedelta
import matplotlib.pyplot as plt 
from os import listdir
pd.set_option('mode.chained_assignment', None)

from pandas.tseries.offsets import BDay

#T:\Daily trades\Daily Re\SmartAllo


today=datetime.now(pytz.timezone('Asia/Shanghai'))
today=today.date()
# today=today.strftime('%Y%m%d')
yesterday=today-timedelta(days=1)
yesterday=today-BDay(1)

yesterday_str=yesterday.strftime('%Y%m%d')
today=today.strftime('%Y%m%d')

# read MTM and CASH balance from enfuson data set 
# mtmpath=r\\paghk.local\shares\Enfusion\Polymer\\+today.strftime('%Y%m%d')
# mtmpath=os.path.join(r\\paghk.local\shares\Enfusion\Polymer\,x)
mtmpath=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\Polymer\\"+today
MTM_list_dir=listdir(mtmpath)
MTM_list_dir = MTM_list_dir[::-1]
for file_name in MTM_list_dir:
    if "FX Exposure FPI (Flat View)" in file_name:
        mtmpath=os.path.join(mtmpath,file_name)
        break
print(mtmpath)
MTM_File=pd.read_csv(mtmpath)
MTM_File = MTM_File[MTM_File["Custodian Account Display Name"].str.contains("GS_FO")]
MTM=MTM_File.groupby(["FundName"])["PAGNativeDirtyNMVSecurities","PAGNativeDirtyNMVCash"].sum()
MTM.rename(columns={"PAGNativeDirtyNMVSecurities":"SecMTM","PAGNativeDirtyNMVCash":"CashBalance"},inplace=True)

newnamelist = []
for name in list(MTM.index):
    newname = name[10:]  
    newnamelist.append(newname)

namemapping = {"AAGAO":"AAGAO","ECMIPO":"ECMIPO","AW_ALPHA":"ALPHA","AW_ALPHA_IAC":"ALPHA_IAC","MAIN":"MAIN_","AZHAN":"AZHAN","ASHAH":"ASHAH","ANDNG":"ANDNG","DSING":"DSING","EDMLO":"EDMLO","SLEUN":"SLEUN", "MGUPT":"MGUPT"}
MTM.index=[namemapping[k] for k in newnamelist]


MTM.drop("MAIN_",inplace=True)

total = pd.DataFrame(MTM.sum()).T
total.index =["ROLL-UP"]

MTM_TOTAL = MTM.append(total)

# read india futures positions
# pospath=r\\paghk.local\shares\Enfusion\Polymer\\+today.strftime('%Y%m%d')
pospath=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\Polymer\\"+today
firm_positions_list_dir=listdir(pospath)
firm_positions_list_dir = firm_positions_list_dir[::-1]
for file_name in firm_positions_list_dir:
    if "Polymer_Firm_Flat_All_Pos_" in file_name:
        pospath=pospath+"\\"+file_name
        break

raw_pos = pd.read_csv(pospath)
raw_pos.dropna(subset=["BB Yellow Key"],inplace=True)
raw_pos = raw_pos[raw_pos["BB Yellow Key"].str.contains("=")]
raw_pos = raw_pos[raw_pos["CustodianShortName"].str.contains("FPI")]
raw_pos=raw_pos[["BB Yellow Key","Custodian Account Display Name","$ Gross Exposure"]]

raw_pos_group_gmv = raw_pos.groupby("Custodian Account Display Name")["$ Gross Exposure"].sum()


new_index= []
for i in list(raw_pos_group_gmv.index):
    new_pm_name = i[10:].split('_GS_')[0]
    if new_pm_name == 'AW_ALPHA':
        new_pm_name = 'ALPHA'
    new_index.append(new_pm_name)

raw_pos_group_gmv.index=new_index
raw_pos_group_gmv_df= pd.DataFrame(raw_pos_group_gmv)
raw_pos_group_gmv_df.loc["ROLL-UP",:]= raw_pos_group_gmv_df.sum(axis= 0)


#gs margin reports
GS_path=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\polymer_reports\GS\\"+yesterday_str
GS_list_dir=listdir(GS_path)
GS_list_dir = GS_list_dir[::-1]

PM_path = {}
print(GS_list_dir)
for file_name in GS_list_dir:
    if "NSE_Margin_Summary_Report" in file_name:
        print('Ys')
        #pospath=pospath+"\\"+file_name
        PM_name = file_name[22:].split("_")[0]
        if PM_name == "":
            PM_name = "ROLL-UP"
        if PM_name == "MAIN":
            PM_name = "MAIN_"
        print(GS_path+"\\"+file_name)
        PM_path[PM_name]= pd.read_excel(GS_path+"\\"+file_name)

for i in assumed_ssf_notional.keys():
    assumed_ssf_notional[i]=raw_pos_group_gmv.loc[i]

import math
def roundup(x):
    return int(math.ceil(x / 100000.0)) * 100000

MARGINOUTPUTDATA = []
MARGINOUTPUTINDEX = []
print(PM_path)
for pmkey in PM_path.keys():
    #pmkey="DSING"
    pm_data= PM_path[pmkey]
    print(pm_data)
    pm_MR_INR = pm_data.iloc[12,8]
    pm_MR_USD = pm_MR_INR/USDINR
    if pmkey in raw_pos_group_gmv_df.index:
        pm_AVG_MR = -pm_MR_USD/(raw_pos_group_gmv_df.loc[pmkey,"$ Gross Exposure"])
    else:
        pm_AVG_MR = 0
    EOD_BAL_INR = pm_data.iloc[19,7]
    EOD_BAL_USD = EOD_BAL_INR/USDINR


    if pmkey in assumed_ssf_notional.keys():
        upperlimit80=assumed_ssf_notional[pmkey]*0.50*0.2
        lowerlimit50=assumed_ssf_notional[pmkey]*0.50*0.5
    else:
        lowerlimit50 = 0
        upperlimit80 = 0

    if EOD_BAL_USD<upperlimit80:
        alert = "TOP UP"
    if EOD_BAL_USD>lowerlimit50:
        alert = "WITHDRAW"
    if (EOD_BAL_USD>=upperlimit80) & (EOD_BAL_USD<=lowerlimit50):
        alert = "OK"


    if alert == "WITHDRAW":
        Amount =  EOD_BAL_USD - lowerlimit50
        Amount=roundup(Amount)
    elif alert== "TOP UP":
        Amount =  upperlimit80 - EOD_BAL_USD
        Amount=roundup(Amount)

    else:
        Amount = 0
    MARGINOUTPUTDATA.append([pm_MR_INR,pm_MR_USD,pm_AVG_MR,EOD_BAL_INR,EOD_BAL_USD,upperlimit80,lowerlimit50,alert,Amount])


    MARGINOUTPUTINDEX.append(pmkey)



PM_Margin_Output= pd.DataFrame(MARGINOUTPUTDATA,index=MARGINOUTPUTINDEX)
print(PM_Margin_Output)
PM_Margin_Output.columns = ["MR(INR)","MR(USD)","AVG MR%","EOD BALANCE (INR)","EOD BALANCE (USD)",
                            "Upper Limit - 80%","Lower Limit - 50%","Alert","Amount (USD)"]

output_step1 = PM_Margin_Output.merge(MTM_TOTAL,left_index=True, right_index=True,how= "left")
output_step1.fillna(0,inplace=True)
output_step2=output_step1.merge(raw_pos_group_gmv_df,left_index=True,right_index=True, how ="left")
output_step2.fillna(0,inplace=True)
output_step2.drop("MAIN_",inplace=True)

#rearrange column sequence
output_final = output_step2[["$ Gross Exposure","MR(USD)","AVG MR%",
                             "SecMTM","CashBalance",
                             "MR(INR)","EOD BALANCE (INR)","EOD BALANCE (USD)",
                             "Upper Limit - 80%","Lower Limit - 50%","Alert",
                             "Amount (USD)"
                             ]]

output_final.loc["ROLL-UP","Amount (USD)"]=0
output_final.sort_index(inplace=True)
right_squence = []

for i in output_final.index:
    if i != "ROLL-UP":
        right_squence.append(i)

right_squence.append("ROLL-UP")
output_final= output_final.reindex(right_squence)

with pd.ExcelWriter(r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\FPI_Margin.xlsx") as writer:
    output_final.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=True)


writer = pd.ExcelWriter(r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\FPI_Margin.xlsx")
output_final.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=True)

pandaswb = writer.book
pandaswb.filename = r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\FPI_Margin.xlsm"
pandaswb.add_vba_project(r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\vbaProject.bin")
writer.save()


from win32com.client import Dispatch
xl = Dispatch('Excel.Application')

wb=xl.Workbooks.Open(r'T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\FPI_Margin.xlsm')
xl.Application.Run("format_fpi")
xl.ActiveWorkbook.Save()
wb.Close(True)


###
import win32com.client as win32
outlook = win32.Dispatch('outlook.application')
mail = outlook.CreateItem(0)
#mail.To = 'tyin@pag.com'
mail.To = 'dchau@polymercapital.com;johnathanl@polymercapital.com'

#mail.CC = PM_enail_mapping_CC_list
sub = "India FPI F&O Margin Summary - POLYMER - COB " + yesterday_str

mail.Subject = sub
mail.Body = 'Message body testing'

#HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
HtmlFile = open(r'T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\test file1.htm', 'r')
source_code = HtmlFile.read()
HtmlFile.close()
#html=wb.to_html(classes='table table-striped')
#html=firm_positions.to_html(classes='table table-striped')
mail.HTMLBody= source_code
mail.Send()
