#update existing OP coupon rate to add GC spread
import pandas as pd
#read GC_spread
GC_spread = pd.read_csv('P:\All\FinanceTradingMO\outperformance swap/GC_table.csv')
#Name the first column as PB
GC_spread.rename(columns={GC_spread.columns[0]:'PB'}, inplace=True)
def attach_GC_spread(row):
    #go thru Broker column to check which PB it is in GC_spread dataframe and check BBG code column to see if it contains CH or C1 OR c2 to determine which spread to add
    #add a new column in OP_booking called GC_spread
    #if the broker is not in GC_spread, then return 0
    if row['Prime'] in GC_spread['PB'].values:
        if 'CH' in row['Underlying BB Yellow Key']:
            return GC_spread.loc[GC_spread['PB'] == row['Prime'], 'CH'].values[0]
        elif 'C1' in row['Underlying BB Yellow Key'] or 'C2' in row['Underlying BB Yellow Key']:
            return GC_spread.loc[GC_spread['PB'] == row['Prime'], 'C1'].values[0]
        else:
            return 0

#read OP position
OP_position=pd.read_csv('P:\Operations\Polymer - Middle Office\PTH and FX booking\Archive/Existing OP.csv')
#remove the rows with no Underlying BB Yellow Key
OP_position=OP_position[OP_position['Underlying BB Yellow Key'].notnull()]
#apply attach_GC_spread function to OP_position to show the GC spread
OP_position['GC_spread']=OP_position.apply(attach_GC_spread, axis=1)
#Updated Rate = OP Rate + GC spread
OP_position['Updated Rate']=OP_position['Active Coupon Rate']+OP_position['GC_spread']/100000
#take the string after OUTPER_ in book name as the PM name
OP_position['PM_name']=OP_position['Book Name'].str.split('OUTPER_').str[1]
#save a new csv file
replace_dict = {'BAML':'MLCO', 'Goldman Sachs':'GSCO', 'JP Morgan':'JPM', 'UBS':'UBS'}
#replace the Counterparty Code with the replacement dictionary
OP_position['Counterparty Code'] = OP_position['Prime'].replace(replace_dict)
OP_position.to_csv('P:\Operations\Polymer - Middle Office\PTH and FX booking\Archive/Existing OP_updated.csv', index=False)

