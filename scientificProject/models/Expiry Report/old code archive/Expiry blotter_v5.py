# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 10:03:33 2021

@author: zzhang
"""
# import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from os import listdir
from typing import Any

import pandas as pd
import pytz
import win32com.client as win32
from numpy import ndarray
from pandas import Series, DataFrame
from pandas.core.arrays import ExtensionArray
from pandas.core.generic import NDFrame

today = datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 
today = today.date()
# today = today.date() - timedelta(days=1)
# today=today.date()
today = today.strftime('%Y%m%d')

# all_PM_expiryblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_expiryblotter_file_path = r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/" + today
# all_PM_expiryblotter_file_path=r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\"+today
firm_positions_list_dir = listdir(all_PM_expiryblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:

#find the latest file with Expiry_Report in the name
    if "Expiry_Report" in file_name:
        print(file_name)

        expiry_report = all_PM_expiryblotter_file_path + "\\" + file_name
        #if can not find the file, stop the program and print “no expiry"

# Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path)
# load EOD blotter
Expiry_Blotter_Data_allPM = pd.read_csv(expiry_report, engine='python')
#if Expiry_Blotter_Data_allPM does not have value, stop the program

if Expiry_Blotter_Data_allPM.empty:
    print("No data in the file")
    exit()
# remove the space in Account Code
#if Expiry_Blotter_Data_allPM is not empty

Expiry_Blotter_Data_allPM['PM code'] = Expiry_Blotter_Data_allPM['Account Code'].str.split('-', expand=True)[1]
# remove the spaces in PM code
Expiry_Blotter_Data_allPM['PM code'] = Expiry_Blotter_Data_allPM['PM code'].str.strip()
Expiry_Blotter_Data_allPM.to_excel(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\Expiry.xlsx",
                                   index=False)
# open loader

# nwb.save(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\Expiry per pm.xlsx")
# define pmlist as the string after Polymer - in Account Code
Product_list: ExtensionArray | ndarray | Series | None | DataFrame | NDFrame | Any = Expiry_Blotter_Data_allPM['Description'
   ].unique

pd.options.display.float_format = '{:,.2f}'.format

# define PM email mapping
PM_enail_mapping = dict(AKUNG="akung@polymercapital.com", EDMLO="elo@polymercapital.com",
                        SLEUN="stevenl@polymercapital.com", ECMIPO="hillmanw@polymercapital.com",
                        MTANG="markt@polymercapital.com", HSASA="hsasao@pag.com;kmoriuchi@pag.com;kmori@pag.com",
                        KZHAN="kzhang@polymercapital.com", BOYAN="bhuang@polymercapital.com", AZHAN="azhang@pag.com",
                        ILIAW="iliaw@polymercapital.com", MGUPT="mgupta@polymercapital.com",
                        LCHOW="lchow@polymercapital.com", ACHEU="anthonyc@polymercapital.com",
                        YANGX="yxia@polymercapital.com", DAVWU="dwu@polymercapital.com",
                        DPARK="dpark@polymercapital.com", QIHAO="hqin@polymercapital.com",
                        EDENG="edeng@polymercapital.com", AAGAO="agao@polymercapital.com",
                        MWONG="michaelw@polymercapital.com", NICYU="nicholasy@polymercapital.com",
                        ROSUH="rsuh@polymercapital.com", KUCHI="kmoriuchi@polymercapital.com",
                        PANHU="phui@polymercapital.com;teamphui@polymercapital.com", DCHIN="dching@polymercapital.com",
                        MELOU="mengl@polymercapital.com", TLIAN="tliang@polymercapital.com",
                        LBURK="lburke@polymercapital.com", VIHAN="vhan@polymercapital.com",
                        JZHAN="johnnyz@polymercapital.com", xHEDGE="awai@polymercapital.com;hho@polymercapital.com",
                        AW_ALPHA="awai@polymercapital.com;hho@polymercapital.com;kklo@polymercapital.com;pchung@polymercapital.com;cfang@polymercapital.com",
                        PCM_CTA="awai@polymercapital.com;hho@polymercapital.com;kklo@polymercapital.com;pchung@polymercapital.com;cfang@polymercapital.com",
                        PCM_FACTOR="awai@polymercapital.com;hho@polymercapital.com;kklo@polymercapital.com;pchung@polymercapital.com;cfang@polymercapital.com",
                        PCM_SCALAR_JP="awai@polymercapital.com;hho@polymercapital.com;kklo@polymercapital.com;pchung@polymercapital.com;cfang@polymercapital.com",
                        PCM_STRAT="awai@polymercapital.com;hho@polymercapital.com;kklo@polymercapital.com;pchung@polymercapital.com;cfang@polymercapital.com",
                        PCM_XZHAN="hsasao@pag.com", xTRADE="awai@polymercapital.com;hho@polymercapital.com",
                        AGINE="agineitis@pag.com", KYAHA="kyahata@pag.com", AFUKU="afukushima@polymercapital.com",
                        KNAKA="knakamura@pag.com;tkasai@pag.com", KOKUM="kokumoto@polymercapital.com",
                        KMIYA="kmiyashita@polymercapital.com", KSUZU="ksuzuki@polymercapital.com",
                        YINOU="yinoue@polymercapital.com", PDRZE="pdrzewucki@polymercapital.com",
                        KAZUW="kwatanabe@pag.com", SHIGN="snaito@pag.com", SSUGI="ssugitomo@polymercapital.com",
                        KMOHR="kmohri@polymercapital.com", YSUGI="ysugiyama@polymercapital.com",
                        YNAKA="ynakashima@polymercapital.com", JZHUO="jack.zhuo@polymercapital.com",
                        TNARU="tnarui@polymercapital.com", MSUZU="mitsuharu.suzuki@polymercapital.com",
                        YOKISH="yokishio@polymercapital.com", MAMORI="masashi.mori@polymercapital.com",
                        HGAWA="hhasegawa@polymercapital.com", TSHOM="tshomura@polymercapital.com",
                        MKITA="mkitani@polymercapital.com", HFUJI="hfujinaga@polymercapital.com",
                        SGUPT="shiv.gupta@polymercapital.com",
                  )
# PM_enail_mapping = {
#     "AW_ALPHA":"zzhang@polymercapital.com",
#     "AAGAO":"zzhang@polymercapital.com",
#
# }

# print (PM_enail_mapping.keys())

All_PM = list(PM_enail_mapping.keys())
PM_enail_mapping_CC_list = "polymertrading@pag.com;polymerops@polymercapital.com;minfengy@pag.com;vyip@polymercapital.com;hho@polymercapital.com;polymercompliance@polymercapital.com"
print(Product_list)

# add an input box to confirm to send the email
send_email = input("Press 1 to send the email to all PMs:")
if send_email == "1":

 with pd.ExcelWriter(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\Expiry.xlsx",
                    engine='xlsxwriter') as writer:
    # split the expiry_blotter_data by PM
    # email each pm based on the PM email mapping
    for PM in All_PM:
        Expiry_Blotter_Data = Expiry_Blotter_Data_allPM[Expiry_Blotter_Data_allPM['PM code'] == PM]
        Expiry_Blotter_Data_allPM.to_excel(writer, sheet_name=PM, index=False)
        if len(Expiry_Blotter_Data) == 0:
            continue
        html = Expiry_Blotter_Data_allPM[Expiry_Blotter_Data_allPM['PM code'] == PM].to_html(index=False)
        text_file = open("T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\expiry.htm", 'w')
        text_file.write(html)
        text_file.close()

        outlook = win32.Dispatch('Outlook.Application')
        mail = outlook.CreateItem(0)

        mail.to = PM_enail_mapping[PM]
        mail.CC = PM_enail_mapping_CC_list

        sub = "Expiry" + today + " (" + PM + ")"
        # sub = 'testing'

        mail.Subject = sub
        mail.Body = 'Message body testing'

        # HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
        HtmlFile = open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\Expiry.htm', 'r')
        source_code = HtmlFile.read()
        HtmlFile.close()
        # html=wb.to_html(classes='table table-striped')
        # html=firm_positions.to_html(classes='table table-striped')
        mail.HTMLBody = source_code
        mail.Send()
