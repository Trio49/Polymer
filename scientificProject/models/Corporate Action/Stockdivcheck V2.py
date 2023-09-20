# -*- coding: utf-8 -*-
"""
Created on Mon Apr  4 11:11:40 2022

@author: zzhang
"""




import pytz
import pandas as pd
import numpy as np
import openpyxl
#import matplotlib.pyplot as plt 
from datetime import datetime, timedelta,date
from os import listdir
from openpyxl import load_workbook
import win32com.client as win32
from IPython.display import HTML
import tkinter as tk
from tkinter import messagebox

# today=datetime.now(pytz.timezone('Asia/Shanghai')) 
# today=today.date()
# # today=today-timedelta(days=1)

# today=today.strftime('%Y%m%d')
# # today2=today2.strftime('%Y%m%d')
#if today is monday a=3, if not a=1
today = datetime.now(pytz.timezone('Asia/Shanghai'))
if date.today().weekday() == 0:
    a=3
else:

    a=1

def run_div_check(check_date):
    print(check_date)    
    
    # check_date= "20221202"
    all_PM_expiryblotter_file_path=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"+check_date
    # firm_positions_file_path="//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"+today
    firm_positions_list_dir=listdir(all_PM_expiryblotter_file_path)
    
    firm_positions_list_dir1 = firm_positions_list_dir[::-1]
    
    for file_name in firm_positions_list_dir1:
        
        
        if "PAG_Corporate Actions Monitoring" in file_name:
            print(file_name)
    
            all_PM_expiryblotter_file_path=all_PM_expiryblotter_file_path+"\\"+file_name
            break

        
    Divt0=pd.read_csv(all_PM_expiryblotter_file_path,engine='python')
    #Divdata massage
    Divt0['Checkdate']=check_date
    Divt0=Divt0[['Underlying BB Yellow Key', 
                'Description',
                'Account',
                'Notional Quantity',
                'Last Stock Dividend Payment Date',
                'Next Stock Dividend Payment Date',
                'Last Stock Dividend Rate',
                'Last Stock Dividend Type',
                'Checkdate']]
    #clean up 
    Divt0 = Divt0[Divt0['Last Stock Dividend Payment Date'].notnull()]
    Divt0=Divt0.loc[Divt0['Last Stock Dividend Payment Date']!='TBD']
    #unification of dateformat
    DATA_FORMAT = '%Y%m%d'
    Divt0['Last Stock Dividend Payment Date'] = pd.to_datetime(
            Divt0['Last Stock Dividend Payment Date'],
            format='%m/%d/%Y',errors='coerce'
        ).apply(lambda x: x.strftime(DATA_FORMAT))
    
    Divt0['Next Stock Dividend Payment Date'] = pd.to_datetime(
            Divt0['Next Stock Dividend Payment Date'],
            format='%m/%d/%Y',errors='coerce'
        ).dt.strftime('%Y%m%d')
    Divt0['Last Stock Dividend Rate']=Divt0['Last Stock Dividend Rate'].fillna(' No data')
    # Divt0['Last Stock Dividend Payment Date']=Divt0['Last Stock Dividend Payment Date'].fillna(' No data')
    Divt0=Divt0.loc[Divt0['Last Stock Dividend Rate']!=0]
    Divt0=Divt0.loc[Divt0['Account'].str.contains('POLY')]
    Divt0 = Divt0[Divt0['Last Stock Dividend Payment Date'].notnull()]
    #add bbg figure
    import pdblp
    from xbbg import blp
    con = pdblp.BCon(debug=True, port=8194, timeout=60000)
    con.start()


    TICKER=Divt0["Underlying BB Yellow Key"]
    Ticker_list=pd.Series.tolist(TICKER)

    #enriching BBG country code

    Ticker_list= list(set(Ticker_list))
    px_list=blp.bdp(Ticker_list, 'EQY_DVD_STK_RECORD_DT_NEXT')
    px_list2=blp.bdp(Ticker_list, 'EQY_DVD_STK_EX_DT_NEXT')
    px_list3=blp.bdp(Ticker_list, 'EQY_DVD_STK_PAY_DT_NEXT')
    #bypass empty one
    if px_list.empty:
        px_list=pd.DataFrame(['1999-01-01'], index=['5 HK Equity'],
                  columns=['EQY_DVD_STK_PAY_DT_NEXT'])
    if px_list2.empty:
        px_list2=pd.DataFrame(['1999-01-01'], index=['5 HK Equity'],
                  columns=['EQY_DVD_STK_PAY_DT_NEXT'])
    if px_list3.empty:
        px_list3=pd.DataFrame(['1999-01-01'], index=['5 HK Equity'],
                  columns=['EQY_DVD_STK_PAY_DT_NEXT'])
    print(px_list3)
    # px_list=px_list

    px_list=px_list.reset_index()
    px_list2=px_list2.reset_index()
    px_list3=px_list3.reset_index()
    
    # px_list3['EQY_DVD_STK_PAY_DT_NEXT'] = px_list3['EQY_DVD_STK_PAY_DT_NEXT'].fillna(0)
    px_list.rename(columns={px_list.columns[1]:"EQY_DVD_STK_RECORD_DT_NEXT"},inplace=True)  
    px_list2.rename(columns={px_list2.columns[1]:"EQY_DVD_STK_EX_DT_NEXT"},inplace=True)  
    px_list3.rename(columns={px_list3.columns[1]:"EQY_DVD_STK_PAY_DT_NEXT"},inplace=True) 
    # px_list3 = px_list3.replace(r'^\s*$', np.nan, regex=True)
    Divt1=Divt0.merge(px_list, left_on='Underlying BB Yellow Key', right_on='index',how='left')  
    Divt2=Divt1.merge(px_list2, left_on='Underlying BB Yellow Key', right_on='index',how='left')  
    Divt3=Divt2.merge(px_list3, left_on='Underlying BB Yellow Key', right_on='index',how='left')  
    # Divt0['Last Stock Dividend Payment Date']=Divt0['Last Stock Dividend Payment Date'].to_datetime
    Divt3=Divt3.sort_values(by=['Last Stock Dividend Payment Date'],ascending=True)
    #massage the BBG date format
    DATA_FORMAT = '%Y%m%d'
    Divt3['EQY_DVD_STK_RECORD_DT_NEXT'] = pd.to_datetime(
            Divt3['EQY_DVD_STK_RECORD_DT_NEXT'],
            format='%Y-%m-%d',errors='ignore'
        ).dt.strftime('%Y%m%d')
    Divt3['EQY_DVD_STK_EX_DT_NEXT'] = pd.to_datetime(
            Divt3['EQY_DVD_STK_EX_DT_NEXT'],
            format='%Y-%m-%d',errors='ignore'
        ).dt.strftime('%Y%m%d')
    Divt3['EQY_DVD_STK_PAY_DT_NEXT'] = pd.to_datetime(
            Divt3['EQY_DVD_STK_PAY_DT_NEXT'],
            format='%Y-%m-%d',errors='ignore'
        ).dt.strftime('%Y%m%d')
    #filter out the closest div payment day from "last" and "next"
    Divt3['NextDivpaymentdate']=np.where(Divt3['Checkdate']>Divt3['Last Stock Dividend Payment Date'],
                                                                Divt3['Next Stock Dividend Payment Date'],
                                                                Divt3['Last Stock Dividend Payment Date'])
    Divt3=Divt3[['Underlying BB Yellow Key', 
                'Description',
                'Account',
                'Notional Quantity',
                'Last Stock Dividend Payment Date',
                'Next Stock Dividend Payment Date',
                'Last Stock Dividend Rate',
                'Last Stock Dividend Type',
                'Checkdate',
                'NextDivpaymentdate',
                "EQY_DVD_STK_RECORD_DT_NEXT",
                "EQY_DVD_STK_EX_DT_NEXT",
                "EQY_DVD_STK_PAY_DT_NEXT"]]
    Divt3['Long/Short']=np.where(Divt3['Notional Quantity'] >0 , 'Long', 'Short')
    Divt3 = Divt3[Divt3['NextDivpaymentdate'].notnull()]
    Divt3['Holding Scenario']=Divt3['Long/Short']+' '+Divt3['Underlying BB Yellow Key']+' '+Divt3['NextDivpaymentdate'].astype(str)+' '+Divt3['Last Stock Dividend Rate'].astype(str)+' '+Divt3['Last Stock Dividend Type']
    # Cantfill=Cantfill.sort_values(by=['RecDate','ExecTimestamp','BloombergCode','Country'])
    # firm_positions=firm_positions[firm_positions["Book Name"].str.contains("PTH")]
    divs=Divt3['Holding Scenario'].unique()
    Upcomingdiv=pd.DataFrame(divs,columns=['Dvds'+str(check_date)])
    Upcomingdiv['date']=check_date
    return Upcomingdiv,Divt3


if __name__== '__main__':
    today=datetime.now(pytz.timezone('Asia/Shanghai')) 
    #div date t=1 define delta to 
    pre_day=today-timedelta(days=a)
    
    today_str=today.strftime('%Y%m%d')
    # today_str=today-timedelta(days=a)
    pre_day_str=pre_day.strftime('%Y%m%d')
    
    data_today,Div0_today=run_div_check(today_str)
    data_pre_day,Div0_pre_day=run_div_check(pre_day_str)
    
    #data massage

    data_join=pd.concat([data_pre_day,data_today],axis=1)
    data_join=data_join.reset_index()
    data_join.rename(columns={data_join.columns[1]:"DvdT-1"},inplace=True)
    data_join.rename(columns={data_join.columns[3]:"DvdT-0"},inplace=True)
    divlist=data_join['DvdT-0'].values.tolist()
    divlist2=data_join['DvdT-1'].values.tolist()
    print(divlist)
    a=[x for x in divlist if x not in divlist2] #if a in t-0 not in t-1
    print (a)
    df=pd.DataFrame(a,columns=['Value'])

    Div0_today['BBG Paydate check']=np.where(Div0_today['NextDivpaymentdate'] == Div0_today['EQY_DVD_STK_PAY_DT_NEXT'], 'Match', 'Check')  
    Div0_Upcoming=Div0_today[['Holding Scenario','Underlying BB Yellow Key','Account','Long/Short','Checkdate','NextDivpaymentdate',"EQY_DVD_STK_PAY_DT_NEXT",'BBG Paydate check']]
    
    Div0_a=Div0_Upcoming.loc[Div0_Upcoming["EQY_DVD_STK_PAY_DT_NEXT"].notna()]
    # Divt0 = Divt0[Divt0['Last Stock Dividend Payment Date'].notnull()]
    Div0_a=Div0_a.loc[Div0_a['BBG Paydate check']=='Check']
    if not Div0_a.empty:
        add=Div0_a[['Holding Scenario',"EQY_DVD_STK_PAY_DT_NEXT"]]
        add=add.reset_index()
        # add=add.rename(columns={"Value":"Holding Scenario"})
        df=df.append(add,ignore_index=True)
        df['New/Updated']=df['Value']
        df['BBG Payment mismatch']=df['Holding Scenario']
        df=df[['New/Updated','BBG Payment mismatch',"EQY_DVD_STK_PAY_DT_NEXT"]]
        
    #define path and saving
    with pd.ExcelWriter(r"P:\Operations\Polymer - Middle Office\Stock Div Rec/"+today.strftime('%Y%m%d')+'StockDiv.xlsx',engine='xlsxwriter') as writer:
    # path=r'P:\Operations\Polymer - Middle Office\Stock Div Rec\Stock div.xlsx'
    # book=load_workbook(path)
    # writer= pd.ExcelWriter(path,engine='openpyxl')
    # writer.book=book
    # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
     df.to_excel(writer,sheet_name='ExceptionT0')
     data_join.to_excel(writer,sheet_name='upcomingcheck')
     Div0_today.to_excel(writer,sheet_name='T-0Div')
     Div0_pre_day.to_excel(writer,sheet_name='T-1Div')
     Div0_Upcoming.to_excel(writer,sheet_name='BBG paymentdate check')
     html = df.to_html(index=False)
     text_file = open(r"P:\Operations\Polymer - Middle Office\Stock Div Rec\divcheck.htm",'w')
     text_file.write(html)
     text_file.close()
    
     outlook = win32.Dispatch('Outlook.Application')
     mail = outlook.CreateItem(0)
        
     mail.to = 'polymerops@polymercapital.com'
          
     sub = "Div check" +"  "+ today.strftime('%Y%m%d')
          
          
     mail.Subject = sub
     mail.Body ='Message body testing'
          
          #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
     HtmlFile=open(r"P:\Operations\Polymer - Middle Office\Stock Div Rec\divcheck.htm", 'r')
     source_code = HtmlFile.read() 
     HtmlFile.close()
          #html=wb.to_html(classes='table table-striped')
          #html=firm_positions.to_html(classes='table table-striped')
     mail.HTMLBody= source_code
     mail.Send() 
print('done')

# pmlist=Expiry_Blotter_Data_allPM['Account Code'].unique
    # pd.options.display.float_format = '{:,.2f}'.format
    