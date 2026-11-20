
import pandas as pd
import re
import os
from datetime import datetime, timedelta

LOG_PATH = "logs/bot.log"
TRADES_PATH = "data/paper_trades.csv"

def analyze_logs():
    print("--- LOG ANALYSIS (Last 12 Hours) ---")
    if not os.path.exists(LOG_PATH):
        print("Log file not found.")
        return

    submit_count = 0
    cancel_count = 0
    fill_count = 0
    
    # Simple regex for events
    # 2026-02-06 23:03:45,... [INFO] ...
    
    cutoff_time = datetime.now() - timedelta(hours=14) # Look back to ~6PM yesterday
    
    with open(LOG_PATH, 'r') as f:
        for line in f:
            # Extract timestamp
            # line format example: 2026-02-06 23:03:45,586
            try:
                ts_str = line.split(',')[0]
                ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
                if ts < cutoff_time:
                    continue
            except:
                continue

            if "SUBMIT" in line or "PLACING LIMIT" in line:
                submit_count += 1
            if "CANCEL" in line:
                cancel_count += 1
            if "FILLED" in line or "MATCHED" in line:
                fill_count += 1

    print(f"Orders Submitted: {submit_count}")
    print(f"Orders Cancelled: {cancel_count}")
    print(f"Orders Filled:    {fill_count}")
    
    if submit_count > 0:
        print(f"Fill Rate:        {(fill_count / submit_count) * 100:.2f}%")
    else:
        print("Fill Rate:        N/A (0 orders)")

def analyze_trades():
    print("\n--- TRADE ANALYSIS (Since Feb 6 6PM ET) ---")
    if not os.path.exists(TRADES_PATH):
        print("Trades file not found.")
        return

    try:
        df = pd.read_csv(TRADES_PATH)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Approximate 6PM ET yesterday in UTC is roughly 23:00 UTC Feb 6? 
        # Actually let's just look at 'today' Feb 7 and late Feb 6.
        # Current time is Feb 7 ~7:56 AM ET -> 12:56 UTC.
        # 6PM ET Feb 6 -> 23:00 UTC Feb 6.
        
        # Taking anything after 2026-02-06 22:00:00 UTC
        cutoff = pd.Timestamp("2026-02-06 22:00:00", tz="UTC").tz_convert(None) 
        # Note: df timestamp might be naive or aware.
        
        # Check if naive
        if df['timestamp'].dt.tz is None:
             # Assume UTC if naive, or match system. 
             # System seems to write UTC isoformat.
             pass
        else:
             df['timestamp'] = df['timestamp'].dt.tz_localize(None)

        recent_trades = df[df['timestamp'] > cutoff]
        
        if recent_trades.empty:
            print("No trades found since Feb 6 10:00 PM UTC.")
        else:
            print(f"Total Trades: {len(recent_trades)}")
            print(recent_trades[['market_id', 'side', 'price', 'shares', 'timestamp']].tail(10))
            
            # PnL estimate? Need closed loops.
            # Volume
            print(f"Volume Traded: ${recent_trades['value'].sum():.2f}")

    except Exception as e:
        print(f"Error reading trades: {e}")

if __name__ == "__main__":
    analyze_logs()
    analyze_trades()
