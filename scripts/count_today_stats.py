
import pandas as pd
import json
import os
from datetime import datetime

TRADES_PATH = "data/paper_trades.csv"
ORDERS_PATH = "data/active_orders.json"

def main():
    # 1. Pending
    try:
        with open(ORDERS_PATH, 'r') as f:
            pending = len(json.load(f))
    except:
        pending = 0
        
    print(f"PENDING_COUNT: {pending}")

    # 2. Closed Trades Today
    if os.path.exists(TRADES_PATH):
        df = pd.read_csv(TRADES_PATH)
        df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
        
        # Filter for today (Feb 6) in UTC
        today = pd.Timestamp.now(tz='UTC').floor('D')
        today_df = df[df['timestamp'] >= today]
        
        if today_df.empty:
            print("CLOSED_COUNT: 0")
            return

        # Simple logic: Group by Market, look for Flats
        closed_count = 0
        
        for mid, group in today_df.groupby('market_id'):
            buys = group[group['side'] == 'BUY']['shares'].sum()
            sells = group[group['side'] == 'SELL']['shares'].sum()
            net = buys - sells
            
            # If we traded both sides and are now flat -> Closed
            # (Or simplified: number of 'round trips'. But simple 'is flat now' check works for 'closed positions')
            has_buy = not group[group['side'] == 'BUY'].empty
            has_sell = not group[group['side'] == 'SELL'].empty
            
            if has_buy and has_sell and abs(net) < 0.1:
                closed_count += 1
                
        print(f"CLOSED_COUNT: {closed_count}")
    else:
        print("CLOSED_COUNT: 0")

if __name__ == "__main__":
    main()
