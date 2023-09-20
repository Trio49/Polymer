#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import pdblp
from datetime import datetime, timedelta
from os import listdir
import win32com.client as win32

pd.set_option('display.max_columns', 100)


today = datetime.today() #-timedelta(days=1)
today_str = today.strftime(format='%Y%m%d')
filename = today.strftime(format='%Y%m%d_%H%M%S') + '_china_deal_PTH_monitoring.xlsx'
filepath = r'P:\All\FinanceTradingMO\China deal PTH/'

# required columns in input PTH position report, 
#'Issuer','Book Name','Custodian Account Display Name','Underlying BB Yellow Key','Active Coupon Rate','Position'

# read the csv SOD pos from enfusion and drop the first blank row

# In[2]:


# get PM code from book name
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

# get stock tag from Aaron table
def get_tag(ticker):
    try:
        return china_deal_names.loc[(china_deal_names['num_code'] + ' CH' == ticker), 'Tag'].iloc[0]
    except:
        return
        
# get T-1 date
def t_minus_1(today):
    if (today.strftime('%u') == '1'):
        print(today)
        return (today - timedelta(days=3))
    else:
        print(today)
        return (today - timedelta(days=1))


# In[3]:
all_PM_expiryblotter_file_path=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"
# all_PM_expiryblotter_file_path=r"T:\Daily trades\Daily Re\Python_Trade_Blotter\Expiry report\"+today
firm_positions_list_dir=listdir(all_PM_expiryblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:
    
    
    if "PTH REPORT_PTH position report" in file_name:
        print(file_name)

        all_PM_expiryblotter_file_path=all_PM_expiryblotter_file_path+"\\"+file_name
        break
pth_sod_pos=pd.read_csv(all_PM_expiryblotter_file_path,engine='python',thousands=',')

# check = input('is the PTH file latest? yes/no: ')
# if check=='yes':
pth_sod_pos.drop(index=0, inplace=True)

# pth_sod_pos = pd.read_csv('sod_pth.csv')
 

# read the china deal names to check
china_deal_names = pd.read_csv(r'P:\All\FinanceTradingMO\China deal PTH\ECM_focus_name.csv')
china_deal_names['num_code'] = china_deal_names['Ticker'].apply(lambda x: x[0:6])

# get PM, broker and rate*quantity column
pth_sod_pos['pm'] = pth_sod_pos['Book Name'].apply(get_pm_code)
pth_sod_pos['broker'] = pth_sod_pos['Custodian Account Display Name'].apply(get_broker)
pth_sod_pos['num_code'] = pth_sod_pos['Underlying BB Yellow Key'].apply(lambda x: x[0:6])
pth_sod_pos['ticker'] = pth_sod_pos['num_code'] + ' CH'
pth_sod_pos['rate_quantity'] = pth_sod_pos['Active Coupon Rate'] * pth_sod_pos['Position']

# filter PTH to china deal names and only AAGAO/ECMIPO
pm_list = ['AAGAO','ECMIPO'] #,'ECMIPO'
china_suffix = ['CH Equity', 'C1 Equity', 'C2 Equity'] 
filt = pth_sod_pos['Underlying BB Yellow Key'].apply(lambda x: any(suffix in x for suffix in china_suffix))
china_deal_pth = pth_sod_pos[filt]
filt = china_deal_pth['pm'].apply(lambda x: any(pm in x for pm in pm_list))
china_deal_pth = china_deal_pth[filt]
# ['Underlying $ Delta']

# group by ticker
china_deal_pth_group = china_deal_pth.groupby(by=['ticker'])
china_deal_pth_summary = china_deal_pth_group[['Position', 'Underlying $ Delta','rate_quantity']].sum()
#china_deal_pth_summary['num_code'] = china_deal_pth_summary['Underlying BB Yellow Key'].apply(lambda x: x[0:6])
china_deal_pth_summary['Rate'] = china_deal_pth_summary['rate_quantity']/china_deal_pth_summary['Position']
china_deal_pth_summary['Day_PnL'] = china_deal_pth_summary['Rate']*china_deal_pth_summary['Underlying $ Delta']/36000
china_deal_pth_summary['Name'] = [(pth_sod_pos.loc[pth_sod_pos['ticker']==ticker,'Issuer']).iloc[0] for ticker in china_deal_pth_summary.index]
china_deal_pth_summary.loc['Total','Underlying $ Delta'] = china_deal_pth_summary['Underlying $ Delta'].sum().astype(float)
china_deal_pth_summary.loc['Total','Day_PnL'] = china_deal_pth_summary['Day_PnL'].sum()
china_deal_pth_summary = china_deal_pth_summary.drop(columns='rate_quantity')

# map stock tag
china_deal_pth_summary['Tag'] = [get_tag(ticker) for ticker in china_deal_pth_summary.index]
china_deal_pth_summary=china_deal_pth_summary.reset_index()
china_deal_pth_summary = china_deal_pth_summary[['ticker','Name','Position','Underlying $ Delta','Rate','Day_PnL','Tag']]
china_deal_pth_summary[['Position','Underlying $ Delta','Day_PnL']]=china_deal_pth_summary[['Position','Underlying $ Delta','Day_PnL']].astype(str)
china_deal_pth_summary[['Position','Underlying $ Delta','Day_PnL']]=china_deal_pth_summary[['Position','Underlying $ Delta','Day_PnL']].applymap(lambda x: '{:,}'.format(int(round(float(x)))) if x != 'nan' else x)
china_deal_pth_summary[['Rate']]=china_deal_pth_summary[['Rate']].round(2)
# In[4]:


#break it down to each ticker table and get the total

#save file with raw table
writer = pd.ExcelWriter(filepath+filename, engine = 'xlsxwriter')
china_deal_pth_summary.to_excel(r'P:\All\FinanceTradingMO\China deal PTH\China_deal_pth_summary.xlsx', sheet_name = 'PTH Summary')

# writer.save()    

# # Save and release handle
# writer.close()
# writer.handles = None


# In[ ]:

with pd.ExcelWriter(r'P:\All\FinanceTradingMO\China deal PTH\China_deal_pth_summary.xlsx',engine='xlsxwriter') as writer:
  # for n in pmlist():
    china_deal_pth_summary.to_excel(writer,sheet_name="sheet1",index=None)

html = china_deal_pth_summary.to_html(index=False)
text_file = open("P:\All\FinanceTradingMO\China deal PTH\expiry.htm",'w')
text_file.write(html)
text_file.close()

outlook = win32.Dispatch('Outlook.Application')
mail = outlook.CreateItem(0)
  
mail.to='agao@polymercapital.com;hillmanw@polymercapital.com'
# mail.to = 'zzhang@polymercapital.com;angelat@polymercapital.com'
mail.cc='polymerops@polymercapital.com;polymertrading@polymercapital.com'
    
sub = "China_deal_pth_summary " + today_str
# sub='PTH report testing'    
    
mail.Subject = sub
mail.Body ='Message body testing'
    
    #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
HtmlFile=open(r'P:\All\FinanceTradingMO\China deal PTH/expiry.htm', 'r')
source_code = HtmlFile.read() 
mail.Attachments.Add(r'P:\All\FinanceTradingMO\China deal PTH\China_deal_pth_summary.xlsx')
HtmlFile.close()
    #html=wb.to_html(classes='table table-striped')
    #html=firm_positions.to_html(classes='table table-striped')
mail.HTMLBody= source_code
mail.Send() 

print('done and sent')

