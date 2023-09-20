# -*- coding: utf-8 -*-
"""
Created on Wed Mar 10 10:03:33 2021

@author: zzhang
"""


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

today=datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 
today=today.date()
# today=today.date()-timedelta(days=1)
#today=today.date()
today=today.strftime('%Y%m%d')

# all_PM_expiryblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_expiryblotter_file_path=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"
# all_PM_expiryblotter_file_path=r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\"+today
firm_positions_list_dir=listdir(all_PM_expiryblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:
    
    
    if "PTH REPORT_PTH position report" in file_name:
        print(file_name)

        all_PM_expiryblotter_file_path=all_PM_expiryblotter_file_path+"\\"+file_name
        break

print(all_PM_expiryblotter_file_path)

# check = input('is the PTH file latest? yes/no: ')
check='yes'
if check=='yes':
#Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path)
#load EOD blotter
    def get_pm_code(book_name):
        start = book_name.find('(') + 1
        end = book_name.find(')')
        return book_name[start:end]
    
    # get broker from custodian account
    def get_broker(custodian):
        start = custodian.find('_') + 1
        start = custodian.find('_', start) + 1
        end = custodian.find('_', start)
        return custodian[start:end]

    PTHtoday=pd.read_csv(all_PM_expiryblotter_file_path,engine='python',thousands=',')
    PTHtoday['PM'] = PTHtoday['Book Name'].apply(get_pm_code)
    PTHtoday['Rate(bps)']=PTHtoday['Active Coupon Rate']*100
    PTHtoday['PB'] = PTHtoday['Custodian Account Display Name'].apply(get_broker)
    PTHtoday['Swap']=np.where(PTHtoday['Custodian Account Display Name'].str.endswith('ISDA'),'Swap','Cash')

    PTHtoday['Notional']=PTHtoday['Underlying $ Delta'].astype(float)
    PTHtoday['Position']=PTHtoday['Position'].astype(float)
    PTHtoday['Comments']=PTHtoday['Deal Id'].astype(str)
    PTHtoday=PTHtoday[['Underlying BB Yellow Key','PM','PB','Swap','Position','Rate(bps)','Notional','Comments']]
    PTHtoday['Q*R']=PTHtoday['Rate(bps)']*PTHtoday['Position']
    # Comments=PTHtoday.groupby(['Underlying BB Yellow Key','PM','PB'])['Comments'].apply(','.join).reset_index()
  
    
    # PTHtoday=PTHtoday.merge(Comments,how='left',left_on=['Underlying BB Yellow Key','PM','PB'],right_on=['Underlying BB Yellow Key','PM','PB'])
    PTHtoday=PTHtoday.groupby(['Underlying BB Yellow Key','PM','PB','Swap']).agg(lambda x : x.sum() if x.dtype=='float64' else ' '.join(x)).reset_index()
    PTHtoday['Weighted Rate(Bps)']=PTHtoday['Q*R']/PTHtoday['Position']
    PTHtoday['Position']=PTHtoday['Position'].apply(lambda x: '{:,.0f}'.format(x))
    PTHtoday['Notional']=PTHtoday['Notional'].apply(lambda x: '{:,.0f}'.format(x))
    PTHtoday=PTHtoday[['Underlying BB Yellow Key','PM','PB','Swap','Position','Weighted Rate(Bps)','Notional','Comments']]
 
    PTHtoday=PTHtoday.sort_values('Underlying BB Yellow Key')
    PTHtoday.to_excel(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx",index=False)


# send email  
    with pd.ExcelWriter(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx",engine='xlsxwriter') as writer:
      # for n in pmlist():
        PTHtoday.to_excel(writer,sheet_name="sheet1",index=None)
    
    html = PTHtoday.to_html(index=False)
    text_file = open("T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\expiry.htm",'w')
    text_file.write(html)
    text_file.close()
    
    outlook = win32.Dispatch('Outlook.Application')
    mail = outlook.CreateItem(0)
      
    # mail.to='polymerops@polymercapital.com'
    mail.to = 'polymertrading@polymercapital.com'
    mail.cc='polymerops@polymercapital.com'
        
    sub = "PTH Position " + today
    # sub='PTH report testing'    
        
    mail.Subject = sub
    mail.Body ='Message body testing'
        
        #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
    HtmlFile=open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\Expiry.htm', 'r')
    source_code = HtmlFile.read() 
    mail.Attachments.Add(r"P:\Operations\Polymer - Middle Office\Python_Trade_Blotter\PTH report\PTH.xlsx")
    HtmlFile.close()
        #html=wb.to_html(classes='table table-striped')
        #html=firm_positions.to_html(classes='table table-striped')
    mail.HTMLBody= source_code
    mail.Send()
#
# #run this code at 9am everyday
# schedule.every().day.at("09:00").do(main)
# schedule.every(1).minutes.do(main)
# while True:
#    schedule.run_pending()
#    time.sleep(1)
# else:
#     print('please check the file name')
#     sys.exit()