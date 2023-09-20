import pandas as pd
#generate 10 buy and sell AAPL US equity trades
raw_trades= pd.read_excel(r"T:\Daily trades\Daily Re\SmartAllo\SmartAllocationUStrade.xlsx",sheet_name="trades")
#calculate the Moving average convergence divergence (MACD)
#MACD = 12-day EMA - 26-day EMA
#signal line = 9-day EMA of MACD
# calcualte the weighted average executed price for each ticker based on raw_trades
raw_trades["weighted_avg_price"]=raw_trades["Notional - Local"]/raw_trades["Execution Quantity"]
raw_trades["weighted_avg_price"]=raw_trades["weighted_avg_price"].round(2)
#calculate the MACD
