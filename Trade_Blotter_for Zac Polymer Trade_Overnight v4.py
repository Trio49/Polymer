# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 11:12:14 2020

@author: tyin
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 18:38:43 2020

@author: tyin
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Mar 23 18:04:37 2020

@author: tyin
"""


from pandas.tseries.offsets import BDay

import os, pandas as pd, glob
import pandas.io.common

'''

path = "/home/username/data_folder"
files_list = glob.glob(os.path.join(path, "*.csv"))

for i in range(0,len(files_list)):
   try:
       raw_data = pd.read_csv(files_list[i])
   except pandas.io.common.EmptyDataError:
      print(files_list[i], " is empty and has been skipped.")

'''



PM_enail_mapping = {"EPAUL":"epaulsson@pag.com",
                      "FLIAN":"fliang@polymercapital.com",
                      "AAGAO":"agao@polymercapital.com",
                      "AKUNG":"akung@polymercapital.com",
                      "AWALL":"awallis@polymercapital.com",
                      "DEROH":"doh@polymercapital.com",
                      "EDMLO":"elo@polymercapital.com",
                      "BOYAN":"bhuang@polymercapital.com",
                    "GARYN":"gng@polymercapital.com",
                    "ANDNG":"andyn@pag.com",
                    "MTANG":"markt@polymercapital.com",
                    "ECMIPO":"hillmanw@polymercapital.com",
                    "KZHAN":"kzhang@polymercapital.com",
                    "BOYAN":"bhuang@polymercapital.com",
                    "CHRXU":"chrisx@polymercapital.com",
                    "DAVWU":"dwu@polymercapital.com",
                    "HSASA":"hsasao@pag.com;kmoriuchi@pag.com;kmori@pag.com",
                    "PDRZE":"pdrzewucki@polymercapital.com",
                    "GRHSU":"ghsu@polymercapital.com",
                    "EDENG":"edeng@polymercapital.com",
                    "AW_ALPHA":"awai@polymercapital.com;hho@polymercapital.com;kklo@polymercapital.com;pchung@polymercapital.com;cfang@polymercapital.com",
                    "SLEUN":"stevenl@polymercapital.com",
                    "xHEDGE":"awai@polymercapital.com;hho@polymercapital.com",

                    }




# PM_enail_mapping = {
                    # "GZHAO":"polymerops@polymercapital.com",
                    

# }
PM_enail_mapping_CC_list = "polymertrading@pag.com;hackmanc@polymercapital.com;polymerops@polymercapital.com;minfengy@pag.com;jsugita@polymercapital.com;teresac@polymercapital.com;vyip@polymercapital.com;hho@polymercapital.com"

# PM_enail_mapping_CC_list = "zzhang@pag.com;tyin@pag.com"



###################manufacturing the trade blotter email:

import pytz
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from os import listdir


pd.set_option('display.max_columns', 20)

today=datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 

#activate line 97 if you want to run t-2 data, activate 99 if you want to run t-1
# today=today.date()-timedelta(days=2)

today=today.date()
#today=today.date()
today=today.strftime('%Y%m%d')

#Trade_Blotter_Data=pd.read_csv(r'C:\Users\tyin\Documents\Trade_Blotter_GZHAO.csv')


# Find out the right file from FTP folder
# all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_tradeblotter_file_path=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"+today
firm_positions_list_dir=listdir(all_PM_tradeblotter_file_path)



firm_positions_list_dir = firm_positions_list_dir[::-1]




for file_name in firm_positions_list_dir:
    if "Trade Blotter - PM ALL Overnight (New)" in file_name:
        all_PM_tradeblotter_file_path=all_PM_tradeblotter_file_path+"\\"+file_name
        break

#Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path)



Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path,engine='python')

last_chars = all_PM_tradeblotter_file_path[-41:]
print ('the data file you are loading is '+last_chars)
dateconfirmation = input('please confirm above file(yes or no):')
if dateconfirmation!='yes':
    Trade_Blotter_Data_allPM=(1,1)
#Trade_Blotter_Data=Trade_Blotter_Data.iloc[1:]
Trade_Blotter_Data_allPM=Trade_Blotter_Data_allPM[["Business Unit",
                                      "Strategy",
                                      "BB Yellow Key",
                                      "Transaction Type",
                                      "Notional Quantity",
                                      "WeightAverageTradePrice",
                                      "Trade Date",
                                      "Trade Currency",
                                      "Trade/Book FX Rate",
                                      "Settle Currency",
                                      "Order Total Quantity"
                                      ]]


Trade_Blotter_Data_allPM['Order Total Quantity']=np.where(Trade_Blotter_Data_allPM["Order Total Quantity"].abs()<0.0000001,Trade_Blotter_Data_allPM['Notional Quantity'],Trade_Blotter_Data_allPM["Order Total Quantity"])
Trade_Blotter_Data_allPM['Order Total Quantity']=np.where(Trade_Blotter_Data_allPM["Order Total Quantity"].isna(),Trade_Blotter_Data_allPM['Notional Quantity'],Trade_Blotter_Data_allPM["Order Total Quantity"])
Trade_Blotter_Data_allPM=Trade_Blotter_Data_allPM[~Trade_Blotter_Data_allPM["Strategy"].str.contains("PTH")]
    
#All_PM_names = Trade_Blotter_Data_allPM["Business Unit"].unique().tolist()

All_PM_names= list(PM_enail_mapping.keys())


grouped1=Trade_Blotter_Data_allPM.groupby(["Strategy","BB Yellow Key","Transaction Type"])["Business Unit","Trade Date","Trade Currency","Trade/Book FX Rate","Settle Currency"].first()
grouped1=grouped1.reset_index()

grouped2=Trade_Blotter_Data_allPM.groupby(["Strategy","BB Yellow Key","Transaction Type"])["Notional Quantity","Order Total Quantity"].sum()
grouped2=grouped2.reset_index()


Trade_Blotter_Data_allPM["WeightAverageTradePrice"] = Trade_Blotter_Data_allPM["WeightAverageTradePrice"].astype(np.float)


wm = lambda x: np.average(x, weights=Trade_Blotter_Data_allPM.loc[x.index, "Notional Quantity"])


group_wgtpx= Trade_Blotter_Data_allPM.groupby(["Strategy","BB Yellow Key","Transaction Type"]).agg({"WeightAverageTradePrice":wm})

group_wgtpx=group_wgtpx.reset_index()





BIG_GROUP = pd.merge(grouped1,grouped2,left_on=["Strategy","BB Yellow Key","Transaction Type"], right_on = ["Strategy","BB Yellow Key","Transaction Type"], how = "left")
BIG_GROUP = pd.merge(BIG_GROUP, group_wgtpx, left_on=["Strategy","BB Yellow Key","Transaction Type"], right_on = ["Strategy","BB Yellow Key","Transaction Type"], how = "left")






Trade_Blotter_Data_allPM=BIG_GROUP


for PM in All_PM_names:   
    
    
    Trade_Blotter_Data=Trade_Blotter_Data_allPM[Trade_Blotter_Data_allPM["Business Unit"]==PM]
    
    
    if Trade_Blotter_Data.empty == True: 
       
        ###################################################
        # sending emails
        
        import win32com.client as win32
        outlook1 = win32.Dispatch('outlook.application')
        mail1 = outlook1.CreateItem(0)
        #mail.To = 'tyin@pag.com;antaifrenzy@gmail.com'
        mail1.To = PM_enail_mapping[PM]
                
        title_date=datetime.now(pytz.timezone('Asia/Shanghai')).date()-BDay(1)

############remove when running officially 
        title_date=title_date.strftime('%Y%m%d')
        sub1 = "[SIGN OFF REQUIRED] Trade Blotter " + title_date + " (" + PM + ")"  + " Overnight Trades"
                  
        mail1.Subject = sub1
        mail1.Body = 'Message body testing'        
        HtmlFile1 = open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel_notrades.htm', 'r')
        source_code1 = HtmlFile1.read() 
        HtmlFile1.close()
        #html=wb.to_html(classes='table table-striped')
        #html=firm_positions.to_html(classes='table table-striped')
        mail1.HTMLBody= source_code1
        mail1.Send()
        
    elif PM=="KYOSH":
        
        ### include the JPY notionals table for KYOSH
        
        BOOK=Trade_Blotter_Data["Strategy"]
        TICKER=Trade_Blotter_Data["BB Yellow Key"]
        BS = Trade_Blotter_Data["Transaction Type"]
        AMOUNT=Trade_Blotter_Data["Notional Quantity"]
        PRICE= Trade_Blotter_Data["WeightAverageTradePrice"]
        TRADEDATE=Trade_Blotter_Data["Trade Date"]
        UnderlyingCCY=Trade_Blotter_Data["Trade Currency"]
        Order_Total_Quantity=Trade_Blotter_Data["Order Total Quantity"]
        Trade_Book_FX_Rate = Trade_Blotter_Data["Trade/Book FX Rate"]
        Settle_Currency=Trade_Blotter_Data["Settle Currency"]
        
        Result = pd.DataFrame({"BOOK":BOOK,"TICKER":TICKER,"B/S":BS,"AMOUNT":AMOUNT,"PRICE":PRICE,
                               "TRADE DATE":TRADEDATE, "Underlying CCY":UnderlyingCCY,
                               "Order Total Quantity":Order_Total_Quantity,"Trade_Book_FX_Rate":Trade_Book_FX_Rate,
                               "Settle_Currency":Settle_Currency})
        
        import pdblp
        con = pdblp.BCon(debug=True, port=8194, timeout=60000)
        con.start()
        
          
        usdjpy = con.ref("USDJPY BGN Curncy", 'PX_LAST')
        usdjpy=usdjpy["value"][0]
        
        
        Ticker_list=pd.Series.tolist(TICKER)
    
        Ticker_list= list(set(Ticker_list))



    
        vwap_day=datetime.now(pytz.timezone('Asia/Shanghai')).date()-BDay(1)
        vwap_day=vwap_day.strftime('%Y%m%d')
        ############remove when running officially 
        #today=today.date()-timedelta(days=1)
        
 


        #vwap_day=
        px_list=con.bdh(Ticker_list, 'EQY_WEIGHTED_AVG_PX',vwap_day,vwap_day,ovrds=[('VWAP_START_TIME', '21:30:00'),('VWAP_END_TIME','4:00:00')])
        px_list=px_list.T
        px_list=px_list.reset_index()
        px_list.rename(columns={px_list.columns[2]:"VWAP"},inplace=True)
        
        Result1=Result.merge(px_list, left_on='TICKER', right_on='ticker',how='left')
        
        
        
        Result1["% COMPLETED"]=Result1["AMOUNT"]/Result1["Order Total Quantity"]
        Result1["% COMPLETED"]=Result1["% COMPLETED"].abs()
    
        Result1["% COMPLETED"]=Result1["% COMPLETED"].replace(np.inf,1.00000)
           
        
        Result1["PRICE"] = Result1["PRICE"].astype(np.float)

        
        
        
        Result1["Qty on Unfilled Balance"]=Result1["Order Total Quantity"]-Result1["AMOUNT"].abs()
        
        Result1["TRADE DATE"] = [today for i in range(len(Result1))]
        

        
        
        multiplier = con.ref(Ticker_list, 'PRICE_MULTIPLIER')
        multiplier=multiplier.fillna(1.00)
        
        multiplier.drop("field",axis=1, inplace=True)
        multiplier.rename(columns={"value":"Multiplier"},inplace=True)
        
        #Result1["Multiplier"]=multiplier["value"]
        
        
        Result1=Result1.merge(multiplier,left_on="TICKER",right_on="ticker",how="left")
        
        
        
        
        Result1["FX"]= [0 for i in range(len(Result1))]
        Result1["FX"]=Result1["Trade_Book_FX_Rate"]
        Result1["FX"][Result1["Settle_Currency"]=="USD"]=1
        Result1["$Notional"]=Result1["AMOUNT"]*Result1["PRICE"]/Result1["FX"]*Result1["Multiplier"]
        
        Result1["JPY Notional"] =  Result1["$Notional"] * usdjpy
        
        Result2=Result1[["BOOK","TICKER","B/S","AMOUNT","PRICE","VWAP","JPY Notional","$Notional","% COMPLETED","Qty on Unfilled Balance"]]
        
        
        Result2["direction"]=Result2["B/S"]
        Result2["direction"][Result2["B/S"]=="BuyToCover"]="Buy"
        
        sum_table= Result2["$Notional"].groupby(Result2["direction"]).sum()
        sum_table_df = sum_table.to_frame()
        sum_table_df=sum_table_df.reset_index()
        sum_table_df["direction"][sum_table_df["direction"]=="Buy"]="Buy/BuyToCover"
        total_g = sum_table_df["$Notional"].abs().sum()
        total_n = sum_table_df["$Notional"].sum()
        
        
        
        sum_table_df.loc[3]= ["Total(Gross)",total_g]
        sum_table_df.loc[4]= ["Total(Net)",total_n]
        
        Result2=Result2.drop("direction",axis=1)
        Result2.index+=1
        
      
        sum_table_df["JPY Notional"]=sum_table_df["$Notional"]*usdjpy
        
        sum_table_df=sum_table_df[["direction","JPY Notional","$Notional"]]
        






        words=pd.DataFrame(["please revert by email in case of any disagreement with the below. This blotter will be considered as signed off in the absence of reply.", "Note: we use VWAP full day"])
        
        Result3 = Result2.copy()
        sum_table_df3=sum_table_df.copy()
        
        with pd.ExcelWriter(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsx") as writer:  
            Result3.to_excel(writer, sheet_name='Sheet1',startrow=10,startcol=0, index=True)
            sum_table_df3.to_excel(writer, sheet_name='Sheet1',startrow=3,startcol=0, index=False)
            words.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=False,header=False)



        import xlsxwriter
        
        
        writer = pd.ExcelWriter(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsx")
        #df_new.to_excel(writer, index = False, sheet_name = 'File Name', header = False)
        
        Result3.to_excel(writer, sheet_name='Sheet1',startrow=10,startcol=0, index=True)
        sum_table_df3.to_excel(writer, sheet_name='Sheet1',startrow=3,startcol=0, index=False)
        words.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=False,header=False)
        
        
        pandaswb = writer.book
        pandaswb.filename = r"T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsm"
        pandaswb.add_vba_project(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\vbaProject_trade_blotter_kyosh.bin")
        #pandaswb.add_vba_project(r'C:\Users\tyin\Documents\vbaProject2.bin')
        
        writer.save()






    
        from win32com.client import Dispatch
        
        xl = Dispatch('Excel.Application')
        wb=xl.Workbooks.Open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsm')
        #xl.Visible = True -- optional
        xl.Application.Run("BORDER_FORMAT")
        

        xl.ActiveWorkbook.Save()
        
        wb.Close(True)
        
        ###################################################
        # sending emails
        
        import win32com.client as win32
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        #mail.To = 'tyin@pag.com;antaifrenzy@gmail.com'
        mail.To = PM_enail_mapping[PM]
                
        sub = "[SIGN OFF REQUIRED] Trade Blotter " + vwap_day + " (" + PM + ")"  + " Overnight Trades"
        
        
        mail.Subject = sub
        mail.Body = 'Message body testing'
        
        #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
        HtmlFile = open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\test file1.htm', 'r')
        source_code = HtmlFile.read() 
        HtmlFile.close()
        #html=wb.to_html(classes='table table-striped')
        #html=firm_positions.to_html(classes='table table-striped')
        mail.HTMLBody= source_code
        mail.Send()





    else:
        print("now is doing :  "+ PM)
        BOOK=Trade_Blotter_Data["Strategy"]
        TICKER=Trade_Blotter_Data["BB Yellow Key"]
        BS = Trade_Blotter_Data["Transaction Type"]
        AMOUNT=Trade_Blotter_Data["Notional Quantity"]
        PRICE= Trade_Blotter_Data["WeightAverageTradePrice"]
        TRADEDATE=Trade_Blotter_Data["Trade Date"]
        UnderlyingCCY=Trade_Blotter_Data["Trade Currency"]
        Order_Total_Quantity=Trade_Blotter_Data["Order Total Quantity"]
        Trade_Book_FX_Rate = Trade_Blotter_Data["Trade/Book FX Rate"]
        Settle_Currency=Trade_Blotter_Data["Settle Currency"]
        
        Result = pd.DataFrame({"BOOK":BOOK,"TICKER":TICKER,"B/S":BS,"AMOUNT":AMOUNT,"PRICE":PRICE,
                               "TRADE DATE":TRADEDATE, "Underlying CCY":UnderlyingCCY,
                               "Order Total Quantity":Order_Total_Quantity,"Trade_Book_FX_Rate":Trade_Book_FX_Rate,
                               "Settle_Currency":Settle_Currency})
        
        ## get the VWAP price from BERG
        
        
        
        import pdblp
        con = pdblp.BCon(debug=True, port=8194, timeout=60000)
        con.start()
        
        
        
        Ticker_list=pd.Series.tolist(TICKER)
        
        
        
        ###CANNOT CALCULATE DUPE TICKERS USING BERG API!!! need to amend here!
        
        Ticker_list= list(set(Ticker_list))
        
        
        
    
        vwap_day1=datetime.now(pytz.timezone('Asia/Shanghai')).date()-BDay(1)
        vwap_day1=vwap_day1.strftime('%Y%m%d')
        px_list=con.bdh(Ticker_list, 'EQY_WEIGHTED_AVG_PX',vwap_day1,vwap_day1,ovrds=[('VWAP_START_TIME', '21:30:00'),('VWAP_END_TIME','4:00:00')]) 
        px_list=px_list.T
        px_list=px_list.reset_index()
        #bypass exception
        if len(px_list) == 0:
            px_list['ticker'] = Ticker_list
            px_list['field'] = 'EQY_WEIGHTED_AVG_PX'
            px_list['VWAP'] = "NA"
        else:
            px_list.rename(columns={px_list.columns[2]:"VWAP"},inplace=True)

        Result1=Result.merge(px_list, left_on='TICKER', right_on='ticker',how='left')



        Result1["% COMPLETED"]=Result1["AMOUNT"]/Result1["Order Total Quantity"]
        Result1["% COMPLETED"]=Result1["% COMPLETED"].abs()

        Result1["% COMPLETED"]=Result1["% COMPLETED"].replace(np.inf,1.00000)


        Result1["PRICE"] = Result1["PRICE"].astype(np.float)




        Result1["Qty on Unfilled Balance"]=Result1["Order Total Quantity"]-Result1["AMOUNT"].abs()

        Result1["TRADE DATE"] = [today for i in range(len(Result1))]




        multiplier = con.ref(Ticker_list, 'PRICE_MULTIPLIER')
        multiplier=multiplier.fillna(1.00)

        multiplier.drop("field",axis=1, inplace=True)
        multiplier.rename(columns={"value":"Multiplier"},inplace=True)

        #Result1["Multiplier"]=multiplier["value"]


        Result1=Result1.merge(multiplier,left_on="TICKER",right_on="ticker",how="left")




        Result1["FX"]= [0 for i in range(len(Result1))]
        Result1["FX"]=Result1["Trade_Book_FX_Rate"]
        Result1["FX"][Result1["Settle_Currency"]=="USD"]=1
        Result1["$Notional"]=Result1["AMOUNT"]*Result1["PRICE"]/Result1["FX"]*Result1["Multiplier"]
        Result2=Result1[["BOOK","TICKER","B/S","AMOUNT","PRICE","VWAP","$Notional","% COMPLETED","Qty on Unfilled Balance"]]


        Result2["direction"]=Result2["B/S"]
        Result2["direction"][Result2["B/S"]=="BuyToCover"]="Buy"
        sum_table= Result2["$Notional"].groupby(Result2["direction"]).sum()
        sum_table_df = sum_table.to_frame()
        sum_table_df=sum_table_df.reset_index()
        sum_table_df["direction"][sum_table_df["direction"]=="Buy"]="Buy/BuyToCover"
        total_g = sum_table_df["$Notional"].abs().sum()
        total_n = sum_table_df["$Notional"].sum()



        sum_table_df.loc[3]= ["Total(Gross)",total_g]
        sum_table_df.loc[4]= ["Total(Net)",total_n]

        Result2=Result2.drop("direction",axis=1)
        Result2.index+=1
        #sum_table_df   the top small summary table !
        #Result2   the bottom big summary table !


        # next step to save these 2 dataframes into xlsx format


        words=pd.DataFrame(["please revert by email in case of any disagreement with the below. This blotter will be considered as signed off in the absence of reply.", "Note: we use VWAP full day"])

        Result3 = Result2.copy()
        sum_table_df3=sum_table_df.copy()

        with pd.ExcelWriter(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsx") as writer:
            Result3.to_excel(writer, sheet_name='Sheet1',startrow=10,startcol=0, index=True)
            sum_table_df3.to_excel(writer, sheet_name='Sheet1',startrow=3,startcol=0, index=False)
            words.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=False,header=False)

        ##################################STARTING FROM HERE!!


        #####################FORMATTING THE EXCEL
        #
        #import win32com.client as win32
        #excel = win32.gencache.EnsureDispatch('Excel.Application')
        #wb = excel.Workbooks.Open(r'C:\Users\tyin\Documents\df2excel.xlsx')
        #ws = wb.Worksheets("Sheet1")
        #ws.Columns.AutoFit()
        #wb.Save()
        #wb.Close()
        #
        #
        #
        #from openpyxl import load_workbook
        #workbook = load_workbook(filename=r'C:\Users\tyin\Documents\df2excel.xlsx')
        #from openpyxl.styles import Color, PatternFill, Font, Border
        #from openpyxl.styles import colors
        #from openpyxl.cell import Cell
        #ws = workbook.active
        #
        #count=0
        #
        #for col in ws.columns:
        #    count=count+1
        #    if count==1:
        #        print("skksksksk")
        #        continue
        #    else:
        #
        #        max_length = 0
        #        column = col[10].column # Get the column name
        #        # Since Openpyxl 2.6, the column name is  ".column_letter" as .column became the column number (1-based)
        #        for cell in col:
        #            try: # Necessary to avoid error on empty cells
        #                if len(str(cell.value)) > max_length:
        #                    max_length = len(cell.value)
        #            except:
        #                pass
        #        adjusted_width = (max_length + 2) * 1.2
        #        print(adjusted_width)
        #        ws.column_dimensions[column].width = adjusted_width
        #
        #
        #workbook.save(filename=r'C:\Users\tyin\Documents\df2excel.xlsx')


        #excel.Application.Quit()
        #
        #
        #from win32com.client.gencache import EnsureDispatch
        #from win32com.client import constants
        #yourExcelFile =r"C:\Users\tyin\Documents\test file1.xlsx"
        ##newFileName =r"C:\Users\tyin\Documents\0324testhtml.html"
        #newFileName ="0324testhtml"
        #
        #xl = EnsureDispatch('Excel.Application')
        #wb = xl.Workbooks.Open(yourExcelFile)
        #
        #
        #
        #wb.SaveAs(newFileName, constants.xlHtml)
        #xl.Workbooks.Close()
        #xl.Quit()
        ##del xl


        #######################
        ################ save as .xlsm file, starting with. xlsx




        import xlsxwriter


        writer = pd.ExcelWriter(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsx")
        #df_new.to_excel(writer, index = False, sheet_name = 'File Name', header = False)

        Result3.to_excel(writer, sheet_name='Sheet1',startrow=10,startcol=0, index=True)
        sum_table_df3.to_excel(writer, sheet_name='Sheet1',startrow=3,startcol=0, index=False)
        words.to_excel(writer, sheet_name='Sheet1',startrow=0,startcol=0, index=False,header=False)


        pandaswb = writer.book
        pandaswb.filename = r"T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsm"
        pandaswb.add_vba_project(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\vbaProject_trade_blotter_format.bin")
        #pandaswb.add_vba_project(r'C:\Users\tyin\Documents\vbaProject2.bin')

        writer.save()

        
# =============================================================================
#         
# import win32com.client as win32
# excel = win32.Dispatch("Excel.Application") # create an instance of Excel
# book = excel.Workbooks.Open(Filename=r'C:\Users\workbook1.xlsm')
# excel.Application.Run('workbook1.xlsm!Grey Scaling.OpenWorkbook') # This runs the macro that is on Sheet1 whose name is Grey Scalling
# book.Save()
# book.Close()
# excel.Application.Quit()
#         
#          
#         
#         
# =============================================================================
        
        
        ########## use python to run the VBA in that .xlsm file to generate the HTML file
        from win32com.client import Dispatch
        
        xl = Dispatch('Excel.Application')
        wb=xl.Workbooks.Open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\df2excel.xlsm')
        xl.Application.Run("BORDER_FORMAT")
        

        xl.ActiveWorkbook.Save()
        
        wb.Close(True)
        
        ###################################################
        # sending emails
        
        import win32com.client as win32
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        #mail.To = 'tyin@pag.com;antaifrenzy@gmail.com'
        mail.To = PM_enail_mapping[PM]
        mail.CC = PM_enail_mapping_CC_list
        sub = "[SIGN OFF REQUIRED] Trade Blotter " + vwap_day1 + " (" + PM + ")" + " Overnight Trades"
        
        
        mail.Subject = sub
        mail.Body = 'Message body testing'
        
        #HtmlFile = open(r'C:\Users\tyin\Documents\\'+newFileName+'.htm', 'r')
        HtmlFile = open(r'T:\Daily trades\Daily Re\Python_Trade_Blotter\test file1.htm', 'r')
        source_code = HtmlFile.read() 
        HtmlFile.close()
        #html=wb.to_html(classes='table table-striped')
        #html=firm_positions.to_html(classes='table table-striped')
        mail.HTMLBody= source_code
        mail.Send()












