
import pytz
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from os import listdir
from openpyxl import load_workbook

today=datetime.now(pytz.timezone('Asia/Shanghai'))

############remove when running officially 
today=today.date()
# today=today.date()-timedelta(days=1)

today=today.strftime('%Y%m%d')

# all_PM_tradeblotter_file_path=r"\\paghk.local\shares\Enfusion\Polymer\\"+today
all_PM_tradeblotter_file_path=r"//paghk.local/Polymer/HK/Department/All/Enfusion/Polymer/"+today
firm_positions_list_dir=listdir(all_PM_tradeblotter_file_path)

firm_positions_list_dir1 = firm_positions_list_dir[::-1]

for file_name in firm_positions_list_dir1:
    
    
    if "Trade Blotter - PM ALL (New)" in file_name:
        print(file_name)

        all_PM_tradeblotter_file_path=all_PM_tradeblotter_file_path+"\\"+file_name
        break

#Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path)
#load EOD blotter
Trade_Blotter_Data_allPM=pd.read_csv(all_PM_tradeblotter_file_path,engine='python')
#open loader
fn=r'T:\Daily trades\Daily Re\Python_Trade_Blotter\Tradingdata\Data Loader1.xlsx'
writer=pd.ExcelWriter(fn, engine='openpyxl')
book = load_workbook(fn)
writer.book = book
writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
sheet=book['Data']
for row in sheet['E5:AU9000']:
    for cell in row:
        cell.value=None
Trade_Blotter_Data_allPM.to_excel(writer,sheet_name='Data',header=True, index=False,startcol=4,startrow=0)
writer.save()
writer.close()


