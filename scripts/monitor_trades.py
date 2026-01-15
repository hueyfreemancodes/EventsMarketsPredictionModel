import os
import time
import pandas as pd
import sys
from datetime import datetime

TRADE_LOG_PATH = "data/paper_trades.csv"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def load_data():
    if not os.path.exists(TRADE_LOG_PATH):
        return pd.DataFrame()
    try:
        df = pd.read_csv(TRADE_LOG_PATH)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        return pd.DataFrame()

def main():
    print("Starting Trade Monitor...")
    while True:
        try:
            df = load_data()
            clear_screen()
            print(f"=== Paper Trading Dashboard ===")
            print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
            
            if df.empty:
                print("\nWaiting for first trade...")
            else:
                # Basic Stats
                total_trades = len(df)
                buys = df[df['side'] == 'BUY']
                sells = df[df['side'] == 'SELL']
                
                # Estimate current PnL (Realized + Unrealized requires Mid Prices, which we don't have here)
                # We can show "Invested" vs "Returned"
                invested = buys['value'].sum()
                returned = sells['value'].sum()
                net_cash_flow = returned - invested
                
                print(f"\n--- Performance Summary ---")
                print(f"Total Trades: {total_trades}")
                print(f"Total Buys:   {len(buys)} (${invested:.2f})")
                print(f"Total Sells:  {len(sells)} (${returned:.2f})")
                print(f"Net Cash Flow: ${net_cash_flow:.2f}")
                
                print(f"\n--- Recent Trades (Last 10) ---")
                # Format for display
                display_df = df.tail(10).copy()
                display_df['time'] = display_df['timestamp'].dt.strftime('%H:%M:%S')
                print(display_df[['time', 'market_id', 'side', 'price', 'shares', 'value']].to_string(index=False))
                
            time.sleep(2)
        except KeyboardInterrupt:
            print("\nExiting Monitor.")
            sys.exit(0)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
