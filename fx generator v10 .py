import pandas as pd
import numpy as np
import datetime
from pandas.tseries.offsets import BDay 
krw = r'T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\fundingkrw.xlsx'
twd = r'T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\fundingtwd.xlsx'
pbs = {'GSCO': "GS_PB", 'GSCO_FPI': "GS_FPI",'JPM': "JPM_FX", 'UBS': "UBS_FX",
           'MLCO': "BAML_FX", 'MSCO': "MS_FX"}
bunitmap=pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv",engine='python')
bookid=pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\Subbunit mapping.csv")
acc=pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\account mapping.csv")

def read_excel(krw, currency, f_date):
   dfk = pd.read_excel(krw, header=1)
   if dfk.empty==False:
    dfk = dfk.iloc[1:-1,:]
    dfk = dfk[dfk.columns[:-1]]
    #print(dfk)
    #dfk = dfk.unstack(['CustodianShortName', 'GSCO', 'MLCO', 'UBS'])
    dfk = dfk.set_index('CustodianShortName')
    dfk = dfk.stack()
    dfk = dfk.reset_index()
    
    dfk['FORWARD DATE'] = date
    if currency in ['KRW', 'krw']:
        dfk['FORWARD DATE'] = date + BDay(delta)
        dfk['FORWARD DATE']= dfk['FORWARD DATE'].apply(lambda x: datetime.date(x.year, x.month, x.day))
    dfk['Currency'] = currency
    
    dfk['RATE'] = 0
    dfk['USD Notional'] = np.NaN
    dfk['LOCAL NOTIONAL'] = -dfk[0]
    dfk['PM'] = dfk['CustodianShortName']
    dfk=dfk.merge(bunitmap, left_on='PM', right_on='Sub Business Unit',how='left') 
    dfk = dfk[dfk[0] != 0]
    dfk['PB'] = dfk['level_1'].apply(lambda x: pbs[x])
    dfk['SPOT'] = 'TRUE'
    dfk['Product']='SPOT'
    dfk['CCY Pair'] = 'USD'+dfk['Currency']
    dfk = dfk[['SPOT','CCY Pair', 'Currency','FORWARD DATE','PM',
               'PB', 'RATE','USD Notional','LOCAL NOTIONAL','Business Unit']]
    dfk_copy = dfk.copy()
    dfk_copy['SPOT'] = 'FALSE'
    dfk_copy['Product']='NDF'
    dfk_copy['LOCAL NOTIONAL'] = -dfk_copy['LOCAL NOTIONAL']
    
    dfk_copy['FORWARD DATE'] = datetime.datetime.strptime(f_date, '%Y%m%d').date()
    
    dfk = pd.concat([dfk, dfk_copy], axis=0)
    #dfk = dfk.reset_index()
    dfk = dfk.sort_index()
    dfk['Column1'] ='POLY_'+ dfk['PM'] +'_' + dfk['PB']
    return dfk

#dfk.to_excel('fx.xlsx',index=False)
fdate = input('please enter krw forward date(example:20210731):')
fdate2 = input('please enter twd forward date(example:20210731):')

#fdate = fdate[:4]+'/' + fdate[4:6] + '/' + fdate[-2:]
td = pd.datetime.today() - datetime.timedelta(days=0)
date = datetime.date(td.year, td.month, td.day)
delta = 1

df_twd = read_excel(twd, 'TWD', fdate2)
df_krw = read_excel(krw, 'KRW', fdate)
    
    
df = pd.concat([df_krw, df_twd], axis=0)
df['PM'] = np.where(df['SPOT'] == 'TRUE', df['PM'], 'EXT_FX')
df=df.sort_values(by=['PB','CCY Pair'],ascending=False)

#Mappbookid, account
df=df.merge(bookid, left_on='PM', right_on='Sub Business Unit',how='left')
# df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB', 
#         'USD Notional', 'LOCAL NOTIONAL','Account','Business Unit','Book ID']]
df['Account'] = np.where(
        df['PM'] == 'EXT_FX',
        'POLY_EXT_FX'+'_'+df['PB'],
        'POLY_'+df['Business Unit']+'_'+df['PB']
        
    )
df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB', 
        'USD Notional', 'LOCAL NOTIONAL','Account','Business Unit','Book ID']]

# df=df.merge(bookid, left_on='PM', right_on='Sub Business Unit',how='inner')
#map book ID and generate 

df['Product'] = np.where(df['SPOT'] == 'TRUE', 'SPOT', 'NDF')
pm=df['PM']




# for x in pm:
#     if x!=str('EXT_FX'):
#      df.loc[x,'Account'] ='POLY_'+ df.loc[x,'PM'].str[0:5] +'_' + df.loc[x,'PB']
#     else:
#      df.loc[x,'Account'] ='POLY_'+ df.loc[x,'PM'].astype(str)+'_' + df.loc[x,'PB']
# for i in range(len(raw_trades)):
#     x1=(raw_trades.loc[i,"GS Rate bps"]<=50) & (raw_trades.loc[i,"GS Rate bps"]>0)
#     x2=(raw_trades.loc[i,"MS Rate bps"]<=50) & (raw_trades.loc[i,"MS Rate bps"]>0)
#     x3=(raw_trades.loc[i,"UBS Rate bps"]<=50) & (raw_trades.loc[i,"UBS Rate bps"]>0)
#     x4=(raw_trades.loc[i,"BAML Rate bps"]<=50) & (raw_trades.loc[i,"BAML Rate bps"]>0)
#     x5=(raw_trades.loc[i,"JPM Rate bps"]<=50) & (raw_trades.loc[i,"JPM Rate bps"]>0)
#     x6=(raw_trades.loc[i,"NOMU Rate bps"]<=50) & (raw_trades.loc[i,"NOMU Rate bps"]>0)
#     if (x1 & x2 & x3 & x4 & x5& x6):
#         raw_trades.loc[i,"isGC"]=True
#     else:
#         raw_trades.loc[i,"isGC"]=False         
         
         
df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB', 
       'USD Notional', 'LOCAL NOTIONAL','Account','Book ID']]
df=df.merge(acc, left_on='Account', right_on='ACCOUNT ',how='left')
df['Account ID']=df['ID']
df['Rate']=str('Paste from recap')
df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB', 
       'USD Notional', 'LOCAL NOTIONAL','Rate','Book ID','Account','Account ID']]
#adding instruction matrix
# idk= input('Today is Monday? yes or no ')
instruction=input('Generate Instruction file? yes/no ')
if instruction=='yes':
    dfx=df
    dfx['Product'] = np.where(dfx['SPOT'] == 'TRUE', 'SPOT', 'NDF')
    dfx['Direction'] = np.where(dfx['LOCAL NOTIONAL'] >0 , 'Buy', 'Sell')
    dfx['VD']=dfx['FORWARD DATE']
    dfx=dfx[['CCY Pair', 'Product','Direction','VD',
                   'PB', 'PM','LOCAL NOTIONAL']]
    dfx=dfx.sort_values(by=['PB','CCY Pair'],ascending=False)
    #filter out NDFs for individual PMs
    dfx3=dfx.copy(deep=True)
    dfx3=dfx3.loc[dfx3['Product']=='SPOT']
    #fx reasury
    dfx1=dfx[['CCY Pair', 'Product','Direction','VD',
                   'PB', 'PM','LOCAL NOTIONAL']]
    
    #grouping
    dfx1=dfx1.groupby(['CCY Pair','PB','Product']).sum().reset_index()
    dfx1['Direction'] = np.where(dfx1['LOCAL NOTIONAL'] >0 , 'Buy', 'Sell')
    
    dfx1['VD'] = np.where(dfx1['CCY Pair'] == 'USDKRW',
                          datetime.date(int(fdate[:4]), int(fdate[4:6].lstrip('0')), int(fdate[-2:].lstrip('0'))),
                          datetime.date(int(fdate2[:4]), int(fdate2[4:6].lstrip('0')), int(fdate2[-2:].lstrip('0'))))
    #diffrentiate top net level
    dfx1['PM']=np.where(dfx1['Product']=='NDF','FX_Treasury','Net Fund level')
    # dfx1['Product']='NDF'
    dfx1=dfx1[['CCY Pair', 'Product','Direction','VD',
                   'PB', 'PM','LOCAL NOTIONAL']]
    dfx2=pd.concat([dfx3,dfx1],axis=0)
    dfx2=dfx2.sort_values(by=['PB','CCY Pair'],ascending=False)
    #adding 
    # save instruction per PB
    PB=dfx2['PB'].unique()
    
    with pd.ExcelWriter(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\Instruction.xlsx",engine='xlsxwriter') as writer:
     for n in PB:
       dfx2[dfx2['PB']==n].to_excel(writer,sheet_name=n,index=None)
       dfx2.to_excel('T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter/'+td.strftime('%Y%m%d')+'fxinstruction.xlsx',sheet_name='Instruction',index=False)

#generate account



#save file
df.to_excel('T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter/'+td.strftime('%Y%m%d')+'fx.xlsx',sheet_name='Uploader',index=False)
df.to_excel('T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\FX Uploader v2.xlsx',sheet_name='fundinginput',index=False)


#=================
