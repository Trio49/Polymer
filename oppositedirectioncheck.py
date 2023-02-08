import pandas as pd
from collections import defaultdict


def main(pm_subset):
    trades = pd.read_excel(r'C:\Users\zzhang\Downloads\polymer_risk_jp_trade_since_20220301.xlsx')
    trades = trades[['pm', 'trade_date', 'bbg_yellow_key', 'quantity']]
    trades.dropna(inplace=True, subset=['bbg_yellow_key'])

    opp_trades_num = defaultdict(int)

    if pm_subset:
        trades = trades[trades['pm'].isin(pm_subset)]

    group_by = trades.groupby(['trade_date', 'bbg_yellow_key', 'pm'])

    df_grouped = pd.DataFrame()
    df_grouped['quantity'] = group_by['quantity'].sum().reset_index()['quantity']
    df_grouped['pm'] = group_by['quantity'].first().reset_index()['pm']
    df_grouped['bbg_yellow_key'] = group_by['quantity'].first().reset_index()['bbg_yellow_key']
    df_grouped['trade_date'] = group_by['quantity'].first().reset_index()['trade_date']

    # keep only duplicated records
    df_grouped = df_grouped[df_grouped.duplicated(subset=['trade_date', 'bbg_yellow_key'], keep=False)]

    unique_ticker_by_date = df_grouped.drop_duplicates(subset=['trade_date', 'bbg_yellow_key'])

    for index, row in unique_ticker_by_date.iterrows():
        tk = row['bbg_yellow_key']
        dt = row['trade_date']

        sub_group = df_grouped[(df_grouped['bbg_yellow_key'] == tk) & (df_grouped['trade_date'] == dt)]

        pm_list = sorted(list(sub_group['pm']))

        for pm1 in pm_list:
            for pm2 in pm_list:
                if pm1 != pm2:
                    q1 = sub_group[(sub_group['pm'] == pm1)]['quantity'].sum()
                    q2 = sub_group[(sub_group['pm'] == pm2)]['quantity'].sum()
                    if q1 * q2 < 0:
                        opp_trades_num[pm1 + pm2] += 1


    opp_trades_num = pd.DataFrame({'pair': list(opp_trades_num.keys()), 'value': list(opp_trades_num.values())})
    return opp_trades_num


if __name__ == '__main__':
    result = main(pm_subset=None)
print(result)
