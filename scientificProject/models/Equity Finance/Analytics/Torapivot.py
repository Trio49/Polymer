#read csv
import pandas as pd
tora_locates=pd.read_csv(r"P:\Operations\Polymer - Middle Office\Borrow analysis/Toradata.csv",engine='python')
#keep the data form State as Approved
tora_locates=tora_locates[tora_locates['State']=='Approved']
#keep the columns
tora_locates=tora_locates[['RecDate','BloombergCode2','HistPrice','MV','Currency','Broker','ApprovedQty','Rate',]]
#group by recdate, bloombergcode2, histprice, mv, currency,aggregate approvedqty and average rate
tora_locates=tora_locates.groupby(['RecDate','BloombergCode2','HistPrice','MV','Currency','Broker']).agg({'ApprovedQty':'sum','Rate':'mean'}).reset_index()
#to csv
# tora_locates.to_csv(r"P:\Operations\Polymer - Middle Office\Borrow analysis\Tora data iutput/Toradatasample.csv",index=False)
pivot_table = tora_locates.pivot_table(index=['RecDate','BloombergCode2', 'HistPrice','MV','Currency',], columns='Broker', values=['ApprovedQty', 'Rate'])
pivot_table.columns = pivot_table.columns.map('_'.join)
#for each row ,add 2 columns to mark the highest Rate and cheapest rate
pivot_table['Highest Rate']=pivot_table[['Rate_baml',
'Rate_gs', 'Rate_jpm', 'Rate_msdw', 'Rate_ubs']].max(axis=1)
pivot_table['Cheapest Rate']=pivot_table[['Rate_baml','Rate_gs', 'Rate_jpm', 'Rate_msdw', 'Rate_ubs']].min(axis=1)
#Mark the broker with highest rate and cheapest rate
pivot_table['Highest Rate Broker']=pivot_table[['Rate_baml', 'Rate_gs', 'Rate_jpm', 'Rate_msdw', 'Rate_ubs']].idxmax(axis=1)
pivot_table['Cheapest Rate Broker']=pivot_table[['Rate_baml','Rate_gs', 'Rate_jpm', 'Rate_msdw', 'Rate_ubs']].idxmin(axis=1)
#reset index
pivot_table=pivot_table.reset_index(drop=False)
#calculate the daily accrual
pivot_table['High Rate Accrual']=pivot_table['Highest Rate']*pivot_table['MV']/10000/365*30
pivot_table['Cheapest Rate Accrual']=pivot_table['Cheapest Rate']*pivot_table['MV']/10000/365*30
#add a rate saving column to show the spread between highest rate and cheapest rate
pivot_table['Rate Saving']=pivot_table['Highest Rate']-pivot_table['Cheapest Rate']
#add a daily accrual saving column to show the saving between highest rate and cheapest rate
pivot_table['Accrual Saving']=pivot_table['High Rate Accrual']-pivot_table['Cheapest Rate Accrual']
#add a column called "GC Rate check", if both highest rate and cheapest rate are higher than GC 50, then mark as "Include" otherwise "GC Rate"
pivot_table['GC Rate check']=pivot_table['Highest Rate'].apply(lambda x: 'Include' if x>0.5 else 'GC Rate')
#generate a new dataframe called "Cash_equity_allocation_saving" to show the saving of cash equity allocation, for AUD/JPY/HKD/SGD/USD
Cash_equity_allocation_saving=pivot_table[pivot_table['Currency'].isin(['AUD','JPY','HKD','SGD','USD'])]
#Group by Ticker, average the MV, sum the Accrual Saving
