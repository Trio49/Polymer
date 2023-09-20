
#USDINR1= con.ref("USDINR REGN Curncy",'PX_LAST')
#USDINR = USDINR1["value"].iloc[0]
import os

# Printing out the USDINR using
USDINR = 82.253
print(str(USDINR),"is used as the USDINR rate")

import pytz
import pandas as pd
import math
from datetime import datetime, timedelta
from os import listdir
pd.set_option('mode.chained_assignment', None)
from pandas.tseries.offsets import BDay

#define datetime

today=datetime.now(pytz.timezone('Asia/Shanghai'))
today=today.date()
yesterday=today-timedelta(days=1)
yesterday=today-BDay(1)
yesterday_str=yesterday.strftime('%Y%m%d')
today=today.strftime('%Y%m%d')

# Part One - MTM_BOND_CASH_TOTAL (Source from Enfusion)
# read Bond(USD) & Cash (USD) for every PM from enfuson data set 

mtmpath=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\Polymer\\"+today

MTM_list_dir=listdir(mtmpath)
MTM_list_dir = MTM_list_dir[::-1]
for file_name in MTM_list_dir:
    if "FX Exposure FPI (Flat View)" in file_name:
        mtmpath=os.path.join(mtmpath,file_name)
        break
print(mtmpath)


# Bond(USD) 

MTM_BOND_CASH=pd.read_csv(mtmpath)

MTM_BOND = MTM_BOND_CASH[MTM_BOND_CASH["Instrument Type"].str.contains("Bond") & MTM_BOND_CASH["CCY"].str.contains("INR")]
MTM_BOND_1=MTM_BOND.groupby(["FundName"])["$ NMV"].sum()
MTM_BOND_df=pd.DataFrame(MTM_BOND_1)

# Cash (USD)

MTM_CASH = MTM_BOND_CASH[MTM_BOND_CASH["Instrument Type"].str.contains("Cash") & MTM_BOND_CASH["CCY"].str.contains("INR")]
MTM_CASH_1=MTM_CASH.groupby(["FundName"])["PAGBookDirtyNMVCash"].sum()
MTM_CASH_df=pd.DataFrame(MTM_CASH_1)

# Futures + Listed Option (USD) (SSF Expousre)
MTM_SSF = MTM_BOND_CASH[MTM_BOND_CASH["Instrument Type"].str.contains("Listed Option")|
                        MTM_BOND_CASH["Instrument Type"].str.contains("Future")]
                        
MTM_SSF = MTM_SSF[MTM_SSF["CCY"].str.contains("INR")] 
MTM_SSF = MTM_SSF[MTM_SSF["Custodian Account Display Name"].str.contains("GS_FO")]

MTM_SSF_1=MTM_SSF.groupby(["FundName"])["$ Gross Exposure"].sum()
MTM_SSF_df=pd.DataFrame(MTM_SSF_1)

# Merge the three parts (BOND + Cash + Futures)
MTM_BOND_CASH_SSF_FINAL = pd.concat([MTM_SSF_df, MTM_CASH_df,MTM_BOND_df])  

MTM_BOND_CASH_SSF_FINAL.rename(columns={"$ Gross Exposure":"SSF Exposure","$ NMV":"Bond (USD)","PAGBookDirtyNMVCash":"Cash (USD)"},inplace=True)
MTM_BOND_CASH_SSF_FINAL.fillna(0,inplace=True)

# Merge row with duplicated name 
duplicate_rows = MTM_BOND_CASH_SSF_FINAL[MTM_BOND_CASH_SSF_FINAL.index.duplicated(keep=False)]

# Group and aggregate the duplicate rows
combined_rows = duplicate_rows.groupby(duplicate_rows.index).sum()

# Update the original DataFrame with the combined rows
MTM_BOND_CASH_SSF_FINAL.update(combined_rows)

# Drop all duplicate rows, keeping the first occurrence
MTM_BOND_CASH_SSF_FINAL = MTM_BOND_CASH_SSF_FINAL.drop_duplicates(keep='first')

# Rename the PM
newnamelist_BC = []
for name in list(MTM_BOND_CASH_SSF_FINAL.index):
    newname = name[10:]  
    newnamelist_BC.append(newname)
    
namemapping = {"AAGAO":"AAGAO","ECMIPO":"ECMIPO","AW_ALPHA_IAC":"ALPHA_IAC","AW_ALPHA":"ALPHA","MAIN":"MAIN_","AZHAN":"AZHAN","ASHAH":"ASHAH","ANDNG":"ANDNG","DSING":"DSING","EDMLO":"EDMLO","SLEUN":"SLEUN", "MGUPT":"MGUPT","MELOU":"MELOU","LBURK":"LBURK","VIHAN":"VIHAN","HSASA":"HSASA","KUCHI":"KUCHI"}

MTM_BOND_CASH_SSF_FINAL.index=[namemapping[k] for k in newnamelist_BC]

# Add the Roll_UP row for Bond(USD) & Cash (USD) and merge
MTM_BOND_CASH_SSF_ROLL = pd.DataFrame(MTM_BOND_CASH_SSF_FINAL.sum()).T
MTM_BOND_CASH_SSF_ROLL.index =["ROLL-UP"]

MTM_BOND_CASH_TOTAL = MTM_BOND_CASH_SSF_FINAL.append(MTM_BOND_CASH_SSF_ROLL)


#Part Two - PM_Margin_Output_NEW (Source from GS margin reports)

GS_path=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\polymer_reports\GS\\"+yesterday_str
GS_list_dir=listdir(GS_path)
GS_list_dir = GS_list_dir[::-1]


#Read Report except MAIN    
PM_path = {}
print(GS_list_dir)


for file_name in GS_list_dir:
    if "NSE_Margin_Summary_Report" in file_name:
        print('Ys')
        
        #pospath=pospath+"\\"+file_name
        PM_name = file_name[22:].split("_")[0]
        if PM_name == "":
            PM_name = "ROLL-UP"
        # if PM_name == "MAIN":
        #     PM_name = "MAIN_"
        print(GS_path+"\\"+file_name)
        if PM_name != "MAIN" and PM_name != "ROLL-UP" and PM_name != "ANDNG":
            PM_path[PM_name]= pd.read_excel(GS_path+"\\"+file_name)
            
       
print(PM_path)

def roundup(x):
    return int(math.ceil(x / 100000.0)) * 100000

MARGINOUTPUTDATA_NEW = []
MARGINOUTPUTINDEX = []

#loop in different PM for "MR(USD)","AVG MR%","MR(INR)","CASH (INR)","EOD BALANCE (INR)","EOD BALANCE (USD)"

for pmkey in PM_path.keys():
    pm_data= PM_path[pmkey]
    print(pm_data)
    
    #reading the INR amount
    #Update the MR(INR) formula **[12,8] (EXCHANGE MARGIN AMOUNTS)-- > [14,4] (MARGIN UTILISED FOR THE DAY ON OPEN POSITION)
    
    pm_MR_INR_NEW = pm_data.iloc[14,4]
    pm_MR_USD_NEW = pm_MR_INR_NEW/USDINR  
    
    #read a list of pmkeys where the SSF Exposure (USD) is not 0

    if pmkey in list(MTM_BOND_CASH_SSF_FINAL[MTM_BOND_CASH_SSF_FINAL["SSF Exposure"]!=0].index):
        pm_AVG_MR_NEW = -pm_MR_USD_NEW/(MTM_BOND_CASH_SSF_FINAL.loc[pmkey,"SSF Exposure"])  
    else:
        pm_AVG_MR_NEW = 0
        
        #for rows where $ Gross Exposure is not 0, calculate the pm_AVG_MR
    # if pmkey in list(raw_pos_group_SSFE_USD_df[raw_pos_group_SSFE_USD_df["SSF Exposure"]==0].index):
    #     pm_AVG_MR_NEW = 0
   
    EOD_BAL_INR = pm_data.iloc[19,7]
    EOD_BAL_USD = EOD_BAL_INR/USDINR

    #Add new column - CASH (INR)
    CASH_INR = pm_data.iloc[14,3]   
    
    MARGINOUTPUTDATA_NEW.append([pm_MR_USD_NEW,pm_AVG_MR_NEW,pm_MR_INR_NEW,CASH_INR,EOD_BAL_INR,EOD_BAL_USD])

    MARGINOUTPUTINDEX.append(pmkey)


PM_Margin_Output_NEW= pd.DataFrame(MARGINOUTPUTDATA_NEW,index=MARGINOUTPUTINDEX)

PM_Margin_Output_NEW.columns = ["MR(USD)","AVG MR%","MR(INR)","CASH (INR)","EOD BALANCE (INR)",
                            "EOD BALANCE (USD)"]


# Merge All three parts (MTM_BOND_CASH_TOTAL,PM_Margin_Output_NEW,raw_pos_group_SSFE_USD_df) and combine into one (NEW_INDIA_FO_MID)

NEW_INDIA_FO_1 = PM_Margin_Output_NEW.merge(MTM_BOND_CASH_TOTAL,left_index=True, right_index=True,how= "outer")
NEW_INDIA_FO_1.fillna(0,inplace=True)

NEW_INDIA_FO_MID = NEW_INDIA_FO_1[["SSF Exposure","MR(USD)","Bond (USD)","Cash (USD)","AVG MR%",
                             "MR(INR)","CASH (INR)","EOD BALANCE (INR)","EOD BALANCE (USD)"]]
                            

# Sorting the index in right order
NEW_INDIA_FO_MID.sort_index(inplace=True)
right_squence_NEW = []

for j in NEW_INDIA_FO_MID.index:
    if  j != "MAIN_" and j != "ROLL-UP":
        right_squence_NEW.append(j)
    
right_squence_NEW.append("MAIN_")

right_squence_NEW.append("ROLL-UP")

NEW_INDIA_FO_MID= NEW_INDIA_FO_MID.reindex(right_squence_NEW)

# Find the no. of total PM for calculating AVG
NEW_INDIA_FO_COUNT=0
    
for i in NEW_INDIA_FO_MID.index:
    NEW_INDIA_FO_COUNT+=1

# Summarize the ROLL_UP Row (Enfusion Source)

# Get all rows except the last row (ROLL_UP) avoiding double count in the roll_UP row 
NIFO_rows_to_sum = NEW_INDIA_FO_MID.iloc[:-1]

#Roll_up(MR(USD),AVG MR%)
NEW_INDIA_FO_MID.loc["ROLL-UP","MR(USD)"]=NIFO_rows_to_sum['MR(USD)'].sum()
NEW_INDIA_FO_MID.loc["ROLL-UP","AVG MR%"]=NIFO_rows_to_sum['AVG MR%'].sum()/(NEW_INDIA_FO_COUNT-2)


#Reading the GS Margin file for MAIN and Overall
margin_path=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\polymer_reports\GS\\"+yesterday_str
MARGIN_PATH_MAIN=r"\\paghk.local\Polymer\HK\Department\All\Enfusion\polymer_reports\GS\\"+yesterday_str

margin_path_dir=listdir(margin_path)
margin_path_dir = margin_path_dir[::-1]
for m_summary_file in margin_path_dir:
    if "POLY_NSE_Margin_Summary_Report" in m_summary_file:
        margin_path=os.path.join(margin_path,m_summary_file)
        break
    
for m_summary_file in margin_path_dir:
    if "POLYMAIN_NSE_Margin_Summary_Report" in m_summary_file:
        MARGIN_PATH_MAIN=os.path.join(MARGIN_PATH_MAIN,m_summary_file)
        break

print(margin_path)
print(MARGIN_PATH_MAIN)


MAR_SUM_File=pd.read_excel(margin_path)
MAR_MAIN_File=pd.read_excel(MARGIN_PATH_MAIN)

NEW_INDIA_FO_FINAL = NEW_INDIA_FO_MID.copy()


# Check whether collecting the valuation value from the file
# Bond (INR) - MAIN 

Valuation_i=25
Valuation_j="Unnamed: 6"
print(MAR_SUM_File.loc[Valuation_i-1][Valuation_j])
Check_Valuation=MAR_SUM_File.loc[Valuation_i-1][Valuation_j]

for i in range(0,10):
    Check_Valuation=MAR_SUM_File.loc[Valuation_i-1][Valuation_j]
    if Check_Valuation != "Valuation":
        Valuation_i=25+i
        Valuation_j="Unnamed: 6"
        print(MAR_SUM_File.loc[Valuation_i-1][Valuation_j])
        
    else:
        break

if Check_Valuation == "Valuation":
    print("Correct")
else:
    print("Please Double Check!")
    
if Check_Valuation == "Valuation":
    NEW_INDIA_FO_FINAL.loc["MAIN_","Bond (INR)"]=MAR_SUM_File.loc[Valuation_i][Valuation_j]
    NEW_INDIA_FO_FINAL.fillna(0,inplace=True)


# Get all rows except the last row avoid double counting (NEW_INDIA_FO_FINAL)
NIFF_rows_to_sum = NEW_INDIA_FO_FINAL.iloc[:-1]

# Bond (INR) - Roll_Up
NEW_INDIA_FO_FINAL.loc["ROLL-UP", "Bond (INR)"] = NIFF_rows_to_sum["Bond (INR)"].sum()

# MR(INR) - MAIN, Roll_Up
NEW_INDIA_FO_FINAL.loc["MAIN_","MR(INR)"]=MAR_MAIN_File.loc[8]["Unnamed: 10"]
NEW_INDIA_FO_FINAL.loc["ROLL-UP","MR(INR)"]=MAR_SUM_File.loc[8]["Unnamed: 10"]


# CASH (INR)(Opening Balance - Cash + Cash Movement) - MAIN, Roll_Up
NEW_INDIA_FO_FINAL.loc["MAIN_","CASH (INR)"]=MAR_MAIN_File.loc[8]["NSE F&O"] + MAR_MAIN_File.loc[8]["Unnamed: 2"]
NEW_INDIA_FO_FINAL.loc["ROLL-UP","CASH (INR)"]=MAR_SUM_File.loc[8]["NSE F&O"] + MAR_SUM_File.loc[8]["Unnamed: 2"]


#EOD Balance (INR) - MAIN, Roll_Up
NEW_INDIA_FO_FINAL.loc["ROLL-UP","EOD BALANCE (INR)"]=MAR_SUM_File.loc[8]["Unnamed: 11"]
NEW_INDIA_FO_FINAL.loc["MAIN_","EOD BALANCE (INR)"]=MAR_MAIN_File.loc[8]["Unnamed: 11"]

#EOD Balance (USD)  - MAIN, Roll_Up
NEW_INDIA_FO_FINAL.loc["ROLL-UP","EOD BALANCE (USD)"]=MAR_SUM_File.loc[8]["Unnamed: 11"]/USDINR
NEW_INDIA_FO_FINAL.loc["MAIN_","EOD BALANCE (USD)"]=MAR_MAIN_File.loc[8]["Unnamed: 11"]/USDINR

# Rename & Re-order the column
NEW_INDIA_FO_FINAL.rename(columns={"EOD BALANCE (INR)":"Margin Excess/Deficit (INR)","EOD BALANCE (USD)":"Margin Excess/Deficit (USD)"},inplace=True)
NEW_INDIA_FO_FINAL = NEW_INDIA_FO_FINAL[["SSF Exposure","MR(USD)","Bond (USD)","Cash (USD)","AVG MR%",
                             "MR(INR)","Bond (INR)","CASH (INR)","Margin Excess/Deficit (INR)","Margin Excess/Deficit (USD)"]]

# Function to multiply non-zero values with -1
def multiply_non_zero_with_neg1(x):
    if x != 0:
        return x * -1
    return x

# Fliping the sign for MR(USD) & MR(INR)
NEW_INDIA_FO_FINAL["MR(USD)"] = NEW_INDIA_FO_FINAL["MR(USD)"] .apply(multiply_non_zero_with_neg1)
NEW_INDIA_FO_FINAL["MR(INR)"] = NEW_INDIA_FO_FINAL["MR(INR)"] .apply(multiply_non_zero_with_neg1)

# Convert all columns to integers except 'AVG MR%' column
int_columns = [col for col in NEW_INDIA_FO_FINAL.columns if col != 'AVG MR%']
NEW_INDIA_FO_FINAL[int_columns] = NEW_INDIA_FO_FINAL[int_columns].round()

# Convert 'AVG MR%' column to percentage format and add percentage sign
NEW_INDIA_FO_FINAL['AVG MR%'] = (NEW_INDIA_FO_FINAL['AVG MR%'] * 100).map('{:.2f}%'.format)

# For Checking Purpose
NEW_INDIA_FO_FINAL['Difference between Cash (USD) & CASH (INR) (in USD)'] = NEW_INDIA_FO_FINAL['Cash (USD)'] - NEW_INDIA_FO_FINAL['CASH (INR)']/82.5
NEW_INDIA_FO_FINAL['Difference between Cash (USD) & CASH (INR) (in USD)'] = NEW_INDIA_FO_FINAL['Difference between Cash (USD) & CASH (INR) (in USD)'].round()

#end

#Change path
Export_path= r"P:\Operations\Polymer - Middle Office\Python_India Margin\FPI_Margin_"+yesterday_str+".csv"
NEW_INDIA_FO_FINAL.to_csv(Export_path)




# with pd.ExcelWriter(r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\Christy\FPI_Margin_T.xlsx") as writer:
#     output_final_M.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=True)


# writer = pd.ExcelWriter(r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\Christy\FPI_Margin_T.xlsx")
# output_final_M.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=True)

# pandaswb = writer.book
# pandaswb.filename = r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\Christy\FPI_Margin_T.xlsm"
# pandaswb.add_vba_project(r"T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\Christy\vbaProject.bin")
# writer.save()


# from win32com.client import Dispatch
# xl = Dispatch('Excel.Application')

# wb=xl.Workbooks.Open(r'T:\Dailytrades\Daily Re\SmartAllo\FPI Margin Monitor\Christy\FPI_Margin_T.xlsx')
# xl.Application.Run("format_fpi")
# xl.ActiveWorkbook.Save()
# wb.Close(True)


# #sending the email
# import win32com.client as win32
# outlook = win32.Dispatch('outlook.application')
# mail = outlook.CreateItem(0)

# mail.To = 'cmang@polymercapital.com'

# #mail.CC = PM_enail_mapping_CC_list
# sub = "India FPI F&O Margin Summary - POLYMER - COB " + yesterday_str

# mail.Subject = sub
# mail.Body = 'Message body testing'

# #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
# HtmlFile = open(r'T:\Daily trades\Daily Re\SmartAllo\FPI Margin Monitor\test file1.htm', 'r')
# source_code = HtmlFile.read()
# HtmlFile.close()
# #html=wb.to_html(classes='table table-striped')
# #html=firm_positions.to_html(classes='table table-striped')
# mail.HTMLBody= source_code
# mail.Send()
