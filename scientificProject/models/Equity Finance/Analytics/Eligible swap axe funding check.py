# this code is to check the eligible swap axe funding names
# read csv file

# !/usr/bin/env python
# coding: utf-8

# In[1]:


from datetime import datetime
from os import listdir
import numpy as np
import pandas as pd

today = datetime.today()  # -timedelta(days=1)
today_str = today.strftime(format='%Y%m%d')
# define path
filepath = r'P:\All\Enfusion\Polymer/' + today_str
# define the file directory
firm_positions_list_dir = listdir(filepath)
# rank the file by date
firm_positions_list_dir = firm_positions_list_dir[::-1]
for file_name in firm_positions_list_dir:
    # find the lastest file with the name "Firm_Flat_All_Pos"
    if "REPORT_Polymer_Firm_Flat_All_Pos" in file_name:
        filepath = filepath + "\\" + file_name
        print(filepath)
        break
# read the file
firm_positions = pd.read_csv(filepath)

# define the swap axe funding list
swap_axe_list = pd.read_csv(r'P:\All\FinanceTradingMO\Funding Axe check\Funding axe list.csv')
# add " Equity" to the Ticker column
swap_axe_list['Ticker'] = swap_axe_list['Ticker'] + ' Equity'
# filter the firm position by the swap axe list
firm_positions_filtered = firm_positions.loc[firm_positions['BB Yellow Key'].isin(swap_axe_list['Ticker'])]
# filter the position by is Financed equals false to find actual cash position
firm_positions_filtered = firm_positions_filtered.loc[firm_positions_filtered['Is Financed'] == False]
# filter out position equal 0 or smaller than 0
firm_positions_filtered = firm_positions_filtered[firm_positions_filtered['Position'] != 0]
firm_positions_filtered = firm_positions_filtered[firm_positions_filtered['Position'] > 0]
# keep the columns needed
firm_positions_filtered = firm_positions_filtered[
    ['BB Yellow Key', 'Position', 'Custodian Account Display Name', 'CustodianShortName','$ Gross Exposure']]
# remove CustodianShortName which has string Preallocation in it
# remove CustodianShortName which has string FPI in it
firm_positions_filtered = firm_positions_filtered[firm_positions_filtered['CustodianShortName'].str.contains('Preallocation') == False]
firm_positions_filtered = firm_positions_filtered[firm_positions_filtered['CustodianShortName'].str.contains('FPI') == False]
# sort the dataframe by CustodianShortName then by $Gross Exposure
firm_positions_filtered = firm_positions_filtered.sort_values(by=[ '$ Gross Exposure','CustodianShortName'], ascending=[False, False])
#group by BB yellow key, and sum the position, concat the CustodianShortName and Custodian Account Display Name
firm_positions_filtered_grouped = firm_positions_filtered.groupby(['BB Yellow Key']).agg(
    {'Position': 'sum', 'Custodian Account Display Name': lambda x: ', '.join(x),
        'CustodianShortName': lambda x: ', '.join(x), '$ Gross Exposure': 'sum'}).reset_index()
# add a column to count how many value Custodian Account Display Name has
firm_positions_filtered_grouped['Custodian Account Display Name count'] = firm_positions_filtered_grouped[
    'Custodian Account Display Name'].str.split(',').apply(lambda x: len(x))

# sort the dataframe by $Gross Exposure
firm_positions_filtered_grouped = firm_positions_filtered_grouped.sort_values(by=['$ Gross Exposure'], ascending=[False])
# remove the duplicate value under CustodianShortName
firm_positions_filtered_grouped['CustodianShortName'] = firm_positions_filtered_grouped['CustodianShortName'].str.split(',').apply(lambda x: ','.join(sorted(set(x), key=x.index)))
# compare against the Gross Exposure against Max in the swap axe list by ticker
firm_positions_filtered_grouped = pd.merge(firm_positions_filtered_grouped, swap_axe_list, left_on='BB Yellow Key', right_on='Ticker', how='left')
# add a column to compare the Gross Exposure against Max in the swap axe list by ticker
firm_positions_filtered_grouped['Gross Exposure vs Max'] = firm_positions_filtered_grouped['$ Gross Exposure'] - firm_positions_filtered_grouped['Max']
#if the Gross Exposure is bigger than Max, then the column will say "Over Max"
firm_positions_filtered_grouped['Max test']=''
firm_positions_filtered_grouped['Max test']=np.where(firm_positions_filtered_grouped['Gross Exposure vs Max']>0,'Over Max','less than Max')
#remove row where Max test is Over Max
firm_positions_filtered_grouped2 = firm_positions_filtered_grouped[firm_positions_filtered_grouped['Max test'] != 'Over Max']

#calculate potential saving
firm_positions_filtered_grouped2['Potential saving'] = firm_positions_filtered_grouped2['$ Gross Exposure'] * 0.0030/365*30

#add comma to the Gross Exposure and Max
firm_positions_filtered_grouped2['$ Gross Exposure'] = firm_positions_filtered_grouped2['$ Gross Exposure'].apply(lambda x: "{:,}".format(x))
firm_positions_filtered_grouped2['Max'] = firm_positions_filtered_grouped2['Max'].apply(lambda x: "{:,}".format(x))
#Save it to csv
firm_positions_filtered_grouped2.to_csv(r'P:\All\FinanceTradingMO\Funding Axe check\Eligible swap axe funding names.csv', index=False)


