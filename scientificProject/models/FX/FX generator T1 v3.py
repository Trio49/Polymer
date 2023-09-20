import pandas as pd
import numpy as np
import datetime
from pandas.tseries.offsets import BDay 
krw = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw.xlsx'
twd = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd.xlsx'
krw_sub = r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingkrw_sub.xlsx'
twd_sub=r'P:\Operations\Polymer-FX\fx_project\funding\t1fundingtwd_sub.xlsx'
pbs = {'GSCO': "GS_PB", 'GSCO_FPI': "GS_FPI",'JPM': "JPM_FX", 'UBS': "UBS_FX",
           'MLCO': "BAML_FX", 'MSCO': "MS_FX"}
bunitmap=pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\bunitmap.csv",engine='python')
bookid=pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\Subbunit mapping.csv")
acc=pd.read_csv(r"T:\Daily trades\Daily Re\Python_Trade_Blotter\FX funding blotter\account mapping.csv")
# empty_series = pd.Series(dtype=float)
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
    
    dfk['FORWARD DATE'] = date+BDay(delta)
    if currency in ['TWD', 'twd']:
        dfk['FORWARD DATE'] = date + BDay(delta)
        dfk['FORWARD DATE']= dfk['FORWARD DATE'].apply(lambda x: datetime.date(x.year, x.month, x.day))
    if currency in ['KRW', 'krw']:
        dfk['FORWARD DATE'] = date + BDay(delta+1)
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
today = datetime.datetime.today()
td = today - datetime.timedelta(days=0)
date = datetime.date(td.year, td.month, td.day)
delta = 0

df_twd = read_excel(twd, 'TWD', fdate2)
df_krw = read_excel(krw, 'KRW', fdate)
df_twd_sub=  read_excel(twd_sub, 'TWD', fdate2) 
df_krw_sub=  read_excel(krw_sub, 'KRW', fdate) 
    
df = pd.concat([df_krw, df_twd], axis=0)
df_sub=pd.concat([df_krw_sub, df_twd_sub], axis=0)

def massage_fx_trades(df):
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
    df=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB', 
           'USD Notional', 'LOCAL NOTIONAL','Account','Book ID']]
    df=df.merge(acc, left_on='Account', right_on='ACCOUNT ',how='left')
    df['Account ID']=df['ID']
    df['Rate']=str('Paste from recap')
    FX_trades=df[['SPOT', 'CCY Pair', 'Currency', 'FORWARD DATE', 'PM', 'PB', 
           'USD Notional', 'LOCAL NOTIONAL','Rate','Book ID','Account','Account ID']]
    return FX_trades

fxt0 = massage_fx_trades(df)
fxt0_sub = massage_fx_trades(df_sub)

    
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
    
    # with pd.ExcelWriter(r"P:\Operations\Polymer-FX\fx_project\funding/Instruction.xlsx",engine='xlsxwriter') as writer:
      # for n in PB:
      #   dfx2[dfx2['PB']==n].to_excel(writer,sheet_name=n,index=None)
    dfx2['Product']=dfx2['Product'].str.replace('SPOT','ONSHORE')
    dfx2 = dfx2.loc[~dfx2['PM'].str.endswith('Net Fund level')]
    dfx2['LOCAL NOTIONAL']=dfx2['LOCAL NOTIONAL'].abs()
    def add_empty_row(group):
        return group.append(pd.Series(dtype='float64'), ignore_index=True)
    dfx2 = dfx2.groupby('PB').apply(add_empty_row)
    dfx2=dfx2.reset_index(drop=True)
    
    def format_float(x):
        return "{:,.2f}".format(x)
    dfx2['LOCAL NOTIONAL']=dfx2['LOCAL NOTIONAL'].apply(format_float)
    dfx2['LOCAL NOTIONAL']=dfx2['LOCAL NOTIONAL'].str.replace('nan','')
    dfx2['Traded Currency']=dfx2['CCY Pair'].str.slice(-3)
    dfx2=dfx2.rename(columns={'PM':'Account'})
    dfx2=dfx2[['CCY Pair', 'Product', 'Traded Currency','Direction', 'VD', 'PB', 'Account',
           'LOCAL NOTIONAL']]
    dfx2.to_excel(r'P:\Operations\Polymer-FX/fx_project\funding/'+td.strftime('%Y%m%d')+'_T1'+'fxinstruction.xlsx',sheet_name='Instruction',index=False)

#generate account



# # #save file
fxt0_sub.to_excel(r'P:\Operations\Polymer-FX\fx_project\funding/'+td.strftime('%Y%m%d')+'_T1'+'fx.xlsx',sheet_name='Uploader',index=False)
# df.to_excel(r'P:\Operations\Polymer-FX\fx_project\funding/FX Uploader v2.xlsx',sheet_name='fundinginput',index=False)


#=================
