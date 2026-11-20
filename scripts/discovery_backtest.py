
import psycopg2
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import sys
import os

# Logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - DISCOVERY - %(message)s')
logger = logging.getLogger("Discovery")

def load_data():
    """Fetch recent market data joined with features."""
    conn = psycopg2.connect(host='localhost', port=8812, user='admin', password='quest', database='qdb')
    
    # 1. Get Top 5 Active Markets (to save time)
    query_markets = """
        SELECT market_id, count(*) as cnt 
        FROM order_book_snapshots 
        WHERE timestamp > dateadd('d', -3, now()) 
        GROUP BY market_id 
        ORDER BY cnt DESC 
        LIMIT 5;
    """
    df_m = pd.read_sql(query_markets, conn)
    target_markets = df_m['market_id'].tolist()
    
    logger.info(f"Analyzing Top 5 Markets: {target_markets}")
    
    dfs = []
    
    for mid in target_markets:
        # Fetch Data (Poly)
        q = f"""
            SELECT timestamp, market_id, ask_price_1, bid_price_1, 
                   (ask_price_1 + bid_price_1)/2 as mid_price
            FROM order_book_snapshots 
            WHERE market_id = '{mid}' 
            AND timestamp > dateadd('d', -3, now())
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(q, conn)
        
        if df.empty: continue
        
        # Mocking Kalshi/Fundamentals for this discovery script
        # In a real run, we would join with 'sports_fundamentals' and Kalshi DB
        # For 'Discovery', we will simulate "Kalshi" as a look-ahead proxy or standard noise
        # Actually, let's look for Kalshi linkages if they exist.
        
        # Simple Feature Engineering
        df['return_1m'] = df['mid_price'].pct_change(12) # Approx 1m if 5s snapshots
        df['spread'] = df['ask_price_1'] - df['bid_price_1']
        
        dfs.append(df)
        
    return pd.concat(dfs) if dfs else pd.DataFrame()

def run_discovery():
    logger.info("loading Data...")
    df = load_data()
    if df.empty:
        logger.error("No data found.")
        return

    logger.info(f"Loaded {len(df)} rows. Computing Alpha Factors...")
    
    # --- FACTOR 1: Volatility Harvester (Market Making) ---
    # Idea: If Spread is Wide (> 5c) AND Volatility is Low, can we capture spread?
    # Proxy Volatility: StdDev of returns over 5 mins
    df['vol_5m'] = df.groupby('market_id')['mid_price'].transform(lambda x: x.rolling(60).std())
    
    # Logic: If Spread > 0.05 AND Vol < 0.01 -> Place passive orders
    # Outcome: ROI = Spread size (if price stays within range)
    # Simulation: If Price(t+5m) is within Price(t) +/- Spread/2, we win spread.
    
    df['future_price'] = df.groupby('market_id')['mid_price'].shift(-60) # 5m lookahead
    
    # --- FACTOR 1: Volatility Harvester (Relaxed) ---
    # Try Spread > 4c (0.04) and Vol < 10c (0.10)
    condition_mm = (df['spread'] > 0.04) & (df['vol_5m'] < 0.10)
    
    # ... (same logic) ...

    # --- FACTOR 2: Mean Reversion (Fade the Pulse) ---
    # If Price moves > 2% in 1 minute, Bet AGAINST it.
    
    df['velocity'] = df['mid_price'].diff(12) # 1 min change
    condition_mom = df['velocity'].abs() > 0.02
    
    df['mom_pnl'] = np.nan
    mask_mom = condition_mom
    if mask_mom.any():
        future_ret = (df.groupby('market_id')['mid_price'].shift(-60) - df['mid_price'])
        
        # FADE: Trade Direction = -1 * sign(velocity)
        trade_dir = -1 * np.sign(df.loc[mask_mom, 'velocity']) # FADE
        df.loc[mask_mom, 'mom_pnl'] = trade_dir * future_ret.loc[mask_mom]

    mom_stats = df['mom_pnl'].describe()
    logger.info(f"\n--- FACTOR 2: Mean Reversion (Fade Velocity) ---\nTriggered: {mask_mom.sum()} times\nMean PnL: {mom_stats['mean']:.4f}\nWin Rate: {(df['mom_pnl'] > 0).mean():.2%}")
    
    # --- FACTOR 3: Mean Reversion (Spread Extension) ---
    # If Spread blows out to > 15c, does it contract?
    # Logic: Short the Spread (Bet on convergence)
    # Not tradable easily on Poly directionally, but implies "Wait to trade".
    
    # Alternative: "The Fake Out"
    # If Price High - Low is tiny (Low Vol) but Volume is High? (Need volume column, skipping for now)
    
if __name__ == "__main__":
    try:
        run_discovery()
    except Exception as e:
        logger.error(f"Failed: {e}")
