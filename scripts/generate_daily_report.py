
import pandas as pd
import numpy as np

TRADES_PATH = "data/paper_trades.csv"

def generate_report():
    try:
        df = pd.read_csv(TRADES_PATH)
    except FileNotFoundError:
        print("No trade data found.")
        return

    if df.empty:
        print("Trade log is empty.")
        return

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Summary Stats
    total_trades = len(df)
    total_volume = df['value'].sum()
    unique_markets = df['market_id'].nunique()
    
    # PnL Calculation (FIFO)
    market_grps = df.groupby('market_id')
    realized_pnl = 0.0
    wins = 0
    losses = 0
    open_positions_count = 0
    
    for mid, group in market_grps:
        buys = group[group['side'] == 'BUY']
        sells = group[group['side'] == 'SELL']
        
        buy_qty = buys['shares'].sum()
        sell_qty = sells['shares'].sum()
        buy_val = buys['value'].sum()
        sell_val = sells['value'].sum()
        
        net_qty = buy_qty - sell_qty
        
        if abs(net_qty) < 0.001:
            # Closed Position
            pnl = sell_val - buy_val
            realized_pnl += pnl
            if pnl > 0: wins += 1
            else: losses += 1
        else:
            open_positions_count += 1
            
    win_rate = (wins / (wins + losses) * 100) if (wins + losses) > 0 else 0
    
    print("==========================================")
    print("       OVERNIGHT TRADING REPORT           ")
    print("==========================================")
    print(f"Total Trades Executed: {total_trades}")
    print(f"Total Volume Traded:   ${total_volume:,.2f}")
    print(f"Unique Markets Traded: {unique_markets}")
    print("------------------------------------------")
    print(f"Realized PnL:          ${realized_pnl:,.2f}")
    print(f"Win Rate:              {win_rate:.1f}% ({wins} W / {losses} L)")
    print(f"Open Positions:        {open_positions_count}")
    print("==========================================")

if __name__ == "__main__":
    generate_report()
