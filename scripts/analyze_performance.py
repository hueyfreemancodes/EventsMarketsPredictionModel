import pandas as pd
import numpy as np

LOG_PATH = "data/paper_trades.csv"

def analyze():
    print(f"Loading {LOG_PATH}...")
    try:
        df = pd.read_csv(LOG_PATH)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    except Exception as e:
        print(f"Error loading data: {e}")
        return

    if df.empty:
        print("No trades found.")
        return

    print(f"\n=== Overnight Session Report ===")
    print(f"Start: {df['timestamp'].min()}")
    print(f"End:   {df['timestamp'].max()}")
    duration = (df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 3600
    print(f"Duration: {duration:.2f} hours")

    # Metrics
    total_trades = len(df)
    
    # Calculate Realized PnL approx
    # (This is rough without matching exact exit to entry, but good for summary)
    buys = df[df['side'] == 'BUY']['value'].sum()
    sells = df[df['side'] == 'SELL']['value'].sum()
    net_flow = sells - buys
    
    print(f"\n--- Financials ---")
    print(f"Total Invested: ${buys:.2f}")
    print(f"Total Returned: ${sells:.2f}")
    print(f"Net Cash Flow:  ${net_flow:.2f}")
    print(f"(Note: Net Flow includes open positions cost. Use Dashboard for Equity)")

    
    # Detailed Trade Matching
    print(f"\n--- Detailed Trade History ---")
    trades_summary = []
    
    # Group by Market to match Buy/Sell pairs (FIFO)
    market_groups = df.groupby('market_id')
    
    total_realized_pnl = 0.0
    winning_trades = 0
    losing_trades = 0
    
    print(f"{'Time':<10} | {'Market':<15} | {'Side':<4} | {'Price':<6} | {'Shares':<8} | {'Value':<8} | {'PnL':<8}")
    print("-" * 80)
    
    for _, row in df.iterrows():
        # Print every trade
        time_str = row['timestamp'].strftime('%H:%M:%S')
        mid = row['market_id'][:15] + "..."
        pnl_str = "-"
        
        # Simple FIFO matching for realized PnL is complex in a flat loop
        # Instead, we just list them and calculate aggregate Realized PnL per market
        print(f"{time_str:<10} | {mid:<15} | {row['side']:<4} | {row['price']:<6.3f} | {row['shares']:<8.2f} | {row['value']:<8.2f} | {pnl_str:<8}")

    print("\n--- Market Level PnL (Session) ---")
    print(f"{'Market ID':<40} | {'Invested':<10} | {'Returned':<10} | {'Net PnL':<10} | {'ROI':<8}")
    print("-" * 90)
    
    export_rows = []
    
    for mid, group in market_groups:
        invested = group[group['side'] == 'BUY']['value'].sum()
        returned = group[group['side'] == 'SELL']['value'].sum()
        net_pnl = returned - invested
        
        # Only count as a "Trade Cycle" if we have both buy and sell (or at least closed some)
        # But for PnL, we include everything.
        
        roi = (net_pnl / invested) * 100 if invested > 0 else 0.0
        
        if invested > 0 and returned > 0: # Active cycle
            if net_pnl > 0: winning_trades += 1
            else: losing_trades += 1
            
        print(f"{mid:<40} | ${invested:<9.2f} | ${returned:<9.2f} | ${net_pnl:<9.2f} | {roi:+.1f}%")
        
        export_rows.append({
            'market_id': mid,
            'invested': invested,
            'returned': returned,
            'net_pnl': net_pnl,
            'roi_pct': roi
        })

    # Summary Stats
    cycles = winning_trades + losing_trades
    win_rate = (winning_trades / cycles) * 100 if cycles > 0 else 0.0
    
    print(f"\n=== Final Session Results ===")
    print(f"Total Invested:   ${buys:.2f}")
    print(f"Total Returned:   ${sells:.2f}")
    print(f"Net Session PnL:  ${net_flow:.2f}")
    print(f"Trade Cycles:     {cycles} ({winning_trades} W / {losing_trades} L)")
    print(f"Win Rate:         {win_rate:.1f}%")
    
    # Export
    pd.DataFrame(export_rows).to_csv("data/session_summary.csv", index=False)
    print(f"\nDetailed report saved to data/session_summary.csv")

if __name__ == "__main__":
    analyze()
