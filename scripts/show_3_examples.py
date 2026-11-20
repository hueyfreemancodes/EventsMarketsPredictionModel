
import json
import os
import pandas as pd
from datetime import datetime

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
METADATA_PATH = os.path.join(BASE_DIR, "scripts", "nba_game_markets.json")
RAW_METADATA_PATH = os.path.join(BASE_DIR, "polymarket_nba_markets_100639.json")
ACTIVE_ORDERS_PATH = os.path.join(BASE_DIR, "data", "active_orders.json")
TRADES_PATH = os.path.join(BASE_DIR, "data", "paper_trades.csv")

def load_metadata():
    mapping = {}
    # Load Raw
    if os.path.exists(RAW_METADATA_PATH):
        try:
            with open(RAW_METADATA_PATH, 'r') as f:
                for m in json.load(f):
                    mid = str(m.get('clob_token_id'))
                    mapping[mid] = {'title': m.get('title'), 'outcome': m.get('outcome_label')}
        except: pass
    # Load Active (Overwrite)
    if os.path.exists(METADATA_PATH):
        try:
            with open(METADATA_PATH, 'r') as f:
                for m in json.load(f):
                    mid = str(m.get('clob_token_id'))
                    mapping[mid] = {'title': m.get('title'), 'outcome': m.get('outcome_label')}
        except: pass
    return mapping

def main():
    metadata = load_metadata()
    print("--- 3 Recent Pending Orders (Feb 6) ---")
    if os.path.exists(ACTIVE_ORDERS_PATH):
        with open(ACTIVE_ORDERS_PATH, 'r') as f:
            orders = json.load(f)
        
        # Sort by timestamp desc
        sorted_orders = sorted(orders.items(), key=lambda x: x[1]['timestamp'], reverse=True)
        
        for i, (mid, order) in enumerate(sorted_orders[:3]):
            meta = metadata.get(str(mid), {})
            title = meta.get('title', 'Unknown Game')
            outcome = meta.get('outcome', 'Unknown Selection')
            print(f"{i+1}. {title} - {outcome} | {order['side']} @ ${order['price']} ({order['timestamp']})")
    
    print("\n--- 3 Recent Executed Trades (Feb 6) ---")
    if os.path.exists(TRADES_PATH):
        try:
            df = pd.read_csv(TRADES_PATH)
            # Filter for today (UTC or ET?) - just taking last 3
            if not df.empty:
                last_3 = df.tail(3)
                for i, row in last_3.iterrows():
                    mid = str(row['market_id'])
                    meta = metadata.get(mid, {})
                    title = meta.get('title', 'Unknown Game')
                    outcome = meta.get('outcome', 'Unknown Selection')
                    print(f"{i+1}. {title} - {outcome} | {row['side']} {row['shares']} shares @ ${row['price']} ({row['timestamp']})")
            else:
                print("No trades found in CSV.")
        except Exception as e:
            print(f"Error reading trades: {e}")

if __name__ == "__main__":
    main()
